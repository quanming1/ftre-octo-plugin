"""
Octo Channel Plugin — Octo WebSocket Channel + 历史消息拉取。

Channel 负责：
  1. 启动 Node.js 桥接进程（octo-bridge.js）
  2. 连接桥接的本地 JSON WebSocket 接口
  3. 将 Octo 入站消息转换为 BusMessage 投递到 EventBus
  4. 将 AgentLoop 产生的回复通过 Octo API 发送回用户

桥接进程负责：
  - WuKongIM 二进制协议（CONNECT/CONNACK/RECV/RECVACK/PING/PONG）
  - DH 密钥交换（curve25519）+ AES-128-CBC 解密
  - 将解密后的消息以 JSON 格式转发到本地 WebSocket

历史消息拉取（参考 openclaw-channel-octo 的 getChannelMessages + historyPrefix）：
  被 @ 时调 API 拉取最近 N 条消息，按 last_bot_reply_seq 分段标注，
  存入用户消息 content 前缀，随消息持久化到 session DB。
"""

from __future__ import annotations

import asyncio
import json
import logging
import subprocess
from pathlib import Path
from typing import Any

import aiohttp

from ftre.channel.base import Channel

from _api import (
    OctoBotApi,
    CHANNEL_TYPE_DM,
    CHANNEL_TYPE_GROUP,
    CHANNEL_TYPE_THREAD,
    build_external_key,
    build_session_id,
    extract_parent_group_no,
    parse_session_id,
)
from _mention import (
    check_mentioned,
    get_cached_members,
    set_cached_members,
    build_uid_to_name_map,
    build_member_list_prefix,
)

logger = logging.getLogger("ftre.plugin.octo_channel")

# ─── 历史消息拉取与上下文注入 ──────────────────────────────────────────

# 拉取历史消息的默认条数
DEFAULT_HISTORY_LIMIT = 20

# 记录每个频道 bot 最后回复时的 message_seq
# 用于历史分段：<= cutoff 的为"已回答"，> cutoff 的为"新消息"
# 参考原始项目 lastBotReplySeqMap（inbound.ts:1433, 2891-2893）
_last_reply_seq: dict[str, int] = {}

# 待记录的入站 message_seq：{session_id: message_seq}
# _handle_message 存入（入站消息的 seq），send() 取出后调 record_bot_reply
_pending_inbound_seq: dict[str, int] = {}


def set_pending_inbound_seq(session_id: str, message_seq: int) -> None:
    """存储入站消息的 message_seq，供 send() 回复成功后记录分段点。"""
    _pending_inbound_seq[session_id] = message_seq


def take_pending_inbound_seq(session_id: str) -> int:
    """取出并删除入站消息的 message_seq。返回 0 表示没有记录。"""
    return _pending_inbound_seq.pop(session_id, 0)


def record_bot_reply(channel_id: str, message_seq: int, bot_id: str = "") -> None:
    """记录 bot 回复时的 message_seq，用于下次历史分段。

    在 send() 成功发送回复后调用。
    参考原始项目 inbound.ts:2888-2896。
    """
    if message_seq and message_seq > 0:
        key = f"{channel_id}:{bot_id}" if bot_id else channel_id
        existing = _last_reply_seq.get(key, 0)
        if message_seq > existing:
            _last_reply_seq[key] = message_seq
            logger.info(f"[octo] 记录 bot 回复 seq={message_seq} | key={key}")


async def fetch_and_build_history(
    api: Any,
    channel_id: str,
    channel_type: int,
    bot_uid: str,
    current_message_id: str,
    uid_to_name: dict[str, str],
    limit: int = DEFAULT_HISTORY_LIMIT,
    bot_id: str = "",
) -> str:
    """从 API 拉取频道历史消息，格式化为 Agent 可读的上下文前缀。

    流程（参考原始项目 inbound.ts:1932-2085）：
      1. POST /v1/bot/messages/sync 拉取最近 limit 条消息
      2. 过滤掉 bot 自己的消息和当前消息
      3. 只保留文本消息（type=1）
      4. 按 last_bot_reply_seq 分段：已回答 / 新消息
      5. 分别标注，格式化为 JSON

    返回空字符串表示无历史可注入。
    """
    messages = await api.get_channel_messages(
        channel_id=channel_id,
        channel_type=channel_type,
        limit=limit,
    )
    if not messages:
        logger.info(f"[octo] API 返回无历史消息: channel={channel_id}")
        return ""

    # 过滤：去掉 bot 自己的消息、当前消息、非文本消息
    # 参考 inbound.ts:1967-1979
    filtered: list[dict[str, Any]] = []
    for m in messages:
        if bot_uid and m.get("from_uid") == bot_uid:
            continue
        if current_message_id and str(m.get("message_id", "")) == current_message_id:
            continue
        if m.get("type") != 1:
            continue
        if not m.get("content"):
            continue
        filtered.append(m)

    if not filtered:
        logger.info(f"[octo] 历史消息过滤后为空: channel={channel_id}")
        return ""

    # 按 message_seq 升序排序
    filtered.sort(key=lambda m: m.get("message_seq", 0))

    # 分段：已回答 vs 新消息（参考 inbound.ts:2023-2024）
    cutoff_key = f"{channel_id}:{bot_id}" if bot_id else channel_id
    cutoff_seq = _last_reply_seq.get(cutoff_key, 0)
    answered = [m for m in filtered if m.get("message_seq", 0) <= cutoff_seq]
    new_msgs = [m for m in filtered if m.get("message_seq", 0) > cutoff_seq]

    logger.info(
        f"[octo] 历史分段: 已回答={len(answered)} 新消息={len(new_msgs)} "
        f"cutoff_seq={cutoff_seq} | channel={channel_id}"
    )

    def format_entries(items: list[dict[str, Any]]) -> str:
        formatted = []
        for m in items:
            uid = m["from_uid"]
            name = uid_to_name.get(uid, "")
            sender_label = f"{name}({uid})" if name else uid
            formatted.append({"sender": sender_label, "body": m["content"]})
        return json.dumps(formatted, ensure_ascii=False, indent=2)

    blocks: list[str] = []
    if answered:
        blocks.append(f"已经回答过，不要重复回答：\n```json\n{format_entries(answered)}\n```")
    if new_msgs:
        blocks.append(f"上次回复后的新消息，仅供参考，不要回答其中的问题：\n```json\n{format_entries(new_msgs)}\n```")

    if not blocks:
        return ""

    prefix = "\n\n".join(blocks)

    logger.info(
        f"[octo] 历史上下文已构建 | channel={channel_id} | "
        f"已回答={len(answered)} 新消息={len(new_msgs)} | 字符数={len(prefix)}"
    )
    return prefix


def build_sender_label(from_uid: str, uid_to_name: dict[str, str]) -> str:
    """构建发送者标签：'名称(uid)' 或纯 uid。"""
    name = uid_to_name.get(from_uid, "")
    return f"{name}({from_uid})" if name else from_uid


# ─── OctoChannel ───────────────────────────────────────────────────────


class OctoChannel(Channel):  # type: ignore[misc]
    """Octo WebSocket Channel（多 bot 支持）。

    负责：
      1. 启动 Node.js 桥接进程（octo-bridge.js）
      2. 连接桥接的本地 JSON WebSocket 接口
      3. 将 Octo 入站消息转换为 BusMessage 投递到 EventBus（携带 agent_id）
      4. 将 AgentLoop 产生的回复通过对应 bot 的 API 发送回用户

    配置格式（config.json plugins 数组）：
      {
        "name": "octo_channel",
        "config": {
          "api_url": "https://im.deepminer.com.cn/api",
          "bridge_port": 9876,
          "require_mention": true,
          "bots": [
            { "bot_token": "bf_xxx", "agent_id": "default", "bot_name": "Ftre" },
            { "bot_token": "bf_yyy", "agent_id": "coder",   "bot_name": "Coder" }
          ]
        }
      }
    """

    def __init__(
        self,
        config: dict[str, Any],
        bus: Any,
        session_manager: Any = None,
        channel_id: str = "octo",
        name: str = "Octo Channel",
    ) -> None:
        super().__init__(channel_id, name, bus)
        self.config: dict[str, Any] = config
        self.session_manager: Any = session_manager
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._session: aiohttp.ClientSession | None = None
        self._ws_task: asyncio.Task[Any] | None = None
        self._bridge_proc: subprocess.Popen[str] | None = None
        self._bridge_reader_task: asyncio.Task[Any] | None = None
        self.require_mention: bool = config.get("require_mention", True)

        # ─── 多 bot 配置 ──────────────────────────────────
        raw_bots = config.get("bots", [])
        if not isinstance(raw_bots, list):
            raw_bots = []

        # bot_id → { agent_id, bot_name, bot_token, bot_uid, api }
        # bot_id 用 bot_token 作为标识（唯一且不需要额外注册）
        self._bots: dict[str, dict[str, Any]] = {}
        # session_id → bot_id 映射（回复时查找用哪个 bot 发送）
        self._session_bots: dict[str, str] = {}

        api_url = config.get("api_url", "")
        for bc in raw_bots:
            token = bc.get("bot_token", "")
            if not token:
                continue
            bot_id = token  # 用 token 作唯一标识
            self._bots[bot_id] = {
                "agent_id": bc.get("agent_id", "default"),
                "bot_name": bc.get("bot_name", "Bot"),
                "bot_token": token,
                "bot_uid": "",
                "api": OctoBotApi(api_url, token),
            }

    async def start(self) -> None:
        """启动 Channel：注册所有 bot → 启动桥接进程 → 连接本地 JSON WS → 开启消息循环。"""
        bridge_port: int = self.config.get('bridge_port', 9876)
        plugin_dir = Path(__file__).resolve().parent
        bridge_path = plugin_dir / 'octo-bridge.js'
        api_url = self.config.get('api_url', '')

        # 注册所有 bot，获取各自的 robot_id
        for bot_id, bot_info in self._bots.items():
            try:
                credentials = await bot_info["api"].register_bot()
                bot_info["bot_uid"] = credentials.get("robot_id", "")
                logger.info(f"[octo] Bot 注册成功: bot_id={bot_id[:12]}... robot_id={bot_info['bot_uid']} agent_id={bot_info['agent_id']}")
            except Exception:
                logger.exception(f"[octo] Bot 注册失败: bot_id={bot_id[:12]}...")

        # 构建桥接进程参数
        bot_configs_for_bridge = [
            {"bot_token": bi["bot_token"], "bot_id": bi["bot_token"]}
            for bi in self._bots.values()
        ]

        bridge_args = [
            'node', str(bridge_path),
            '--api-url', api_url,
            '--port', str(bridge_port),
            '--bots', json.dumps(bot_configs_for_bridge),
        ]

        logger.info(f"[octo] 启动桥接进程: {bridge_path} 端口={bridge_port} bots={len(self._bots)}")
        self._bridge_proc = subprocess.Popen(
            bridge_args,
            cwd=str(plugin_dir),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        logger.info(f"[octo] 桥接进程已启动，pid={self._bridge_proc.pid}")

        # 后台异步读取桥接进程的 stdout 并输出到 ftre 日志
        self._bridge_reader_task = asyncio.create_task(self._read_bridge_output())

        # 等待桥接进程完成注册和 WuKongIM 连接
        await asyncio.sleep(3)

        # 检查桥接进程是否异常退出
        if self._bridge_proc.poll() is not None:
            logger.error(f"[octo] 桥接进程异常退出，exit_code={self._bridge_proc.returncode}")
            return

        # 连接桥接的本地 JSON WebSocket 服务
        ws_url = f'ws://127.0.0.1:{bridge_port}'
        logger.info(f"[octo] 正在连接桥接 WebSocket: {ws_url}")
        try:
            self._session = aiohttp.ClientSession()
            self._ws = await self._session.ws_connect(ws_url)
            logger.info(f"[octo] 桥接 WebSocket 连接成功: {ws_url}")
        except Exception as e:
            logger.error(f"[octo] 桥接 WebSocket 连接失败: {e}")
            return

        # 启动消息循环（后台协程，持续监听 Octo 消息）
        self._ws_task = asyncio.create_task(self._ws_loop())
        logger.info("[octo] 消息循环已启动")

    async def _read_bridge_output(self) -> None:
        """后台读取桥接进程的 stdout 并转发到 ftre 日志。"""
        if not self._bridge_proc or not self._bridge_proc.stdout:
            return
        loop = asyncio.get_event_loop()
        try:
            while True:
                line = await loop.run_in_executor(None, self._bridge_proc.stdout.readline)
                if not line:
                    break
                text = line.rstrip()
                if text:
                    logger.info(f"[octo-bridge] {text}")
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("[octo] 读取桥接输出时发生异常")

    async def _ws_loop(self) -> None:
        """从桥接 JSON WebSocket 接收消息并分发的循环。"""
        if self._ws is None:
            logger.warning("[octo] 消息循环退出: WebSocket 为空")
            return
        logger.info("[octo] 消息循环开始监听")
        try:
            async for msg in self._ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    try:
                        data = json.loads(msg.data)
                        msg_type = data.get('type')
                        logger.info(f"[octo] 收到桥接消息: type={msg_type}")
                        if msg_type == 'message':
                            await self._handle_message(data.get('data', {}))
                        else:
                            logger.info(f"[octo] 忽略未知消息类型: type={msg_type}")
                    except json.JSONDecodeError:
                        logger.warning(f"[octo] 无法解析 JSON 消息: {msg.data[:200]}")
                    except Exception:
                        logger.exception("[octo] 处理消息时发生异常")
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    logger.error(f"[octo] WebSocket 错误: {self._ws.exception()}")
                    break
                elif msg.type == aiohttp.WSMsgType.CLOSED:
                    logger.info("[octo] WebSocket 连接关闭")
                    break
        except asyncio.CancelledError:
            logger.info("[octo] 消息循环被取消")
        except Exception:
            logger.exception("[octo] 消息循环发生未预期的异常")

    async def _handle_message(self, msg: dict[str, Any]) -> None:
        """处理一条 WuKongIM 消息，转换为 BusMessage 投递到 EventBus。"""
        # ─── 识别来源 bot ──────────────────────────────────
        bot_id: str = msg.get("bot_id", "")
        bot_info = self._bots.get(bot_id)
        if bot_info is None:
            logger.warning(f"[octo] 未知 bot_id: {bot_id}")
            return

        bot_uid: str = bot_info["bot_uid"]
        bot_name: str = bot_info["bot_name"]
        agent_id: str = bot_info["agent_id"]
        bot_api: OctoBotApi = bot_info["api"]

        payload: dict[str, Any] = msg.get("payload", {})
        msg_type: Any = payload.get("type")
        from_uid: str = msg.get("from_uid", "")
        channel_id: str = msg.get("channel_id", "")
        channel_type: int = msg.get("channel_type", CHANNEL_TYPE_DM)
        message_id: str = str(msg.get("message_id", ""))
        content: str = payload.get("content", "")
        is_event: bool = bool((payload.get("event") or {}).get("type"))

        logger.info(
            f"[octo] 收到消息: 发送者={from_uid} 频道={channel_id} "
            f"频道类型={channel_type} 消息类型={msg_type} "
            f"bot_uid={bot_uid} agent_id={agent_id} "
            f"内容={content[:80]!r}"
        )

        # 过滤 bot 自己的消息（与 OpenClaw 保持一致，事件消息除外）
        if bot_uid and from_uid == bot_uid and not is_event:
            logger.info(f"[octo] 跳过自己的消息: from_uid={from_uid}")
            return

        # 群聊/讨论串 @ 检测门控：require_mention 为 True 时，只有被 @ 才回复
        is_group_or_thread = channel_type in (CHANNEL_TYPE_GROUP, CHANNEL_TYPE_THREAD)
        is_mentioned = False
        if is_group_or_thread and self.require_mention:
            is_mentioned = check_mentioned(payload, content, bot_uid, bot_name)
            if not is_mentioned:
                logger.info(
                    f"[octo] 群聊消息未 @ bot，跳过: "
                    f"发送者={from_uid} 频道={channel_id}"
                )
                # 非 @ 消息不投递给 Agent
                # 历史上下文在被 @ 时通过 API 拉取，不依赖内存缓存
                return

        # 非文本消息暂不处理（MVP 阶段只支持纯文本）
        if msg_type != 1:
            logger.info(f"[octo] 跳过非文本消息: type={msg_type}")
            return

        # 群聊/讨论串：刷新成员缓存（用于 @ 检测白名单 + Agent 上下文）
        if is_group_or_thread and channel_id:
            await self._refresh_member_cache_if_needed(channel_id, bot_api)

        # 私聊时 channel_id 为空，使用发送者 uid 作为回复目标
        if not channel_id:
            channel_type = CHANNEL_TYPE_DM
            channel_id = from_uid

        external_key = build_external_key(channel_type, channel_id, from_uid, bot_id)
        if self.session_manager is not None:
            session_id = await self.session_manager.get_or_create_external_session(
                channel_id=self.channel_id,
                external_key=external_key,
                title=f"Octo {channel_id}",
                external_data={
                    "channel_type": channel_type,
                    "channel_id": channel_id,
                    "from_uid": from_uid,
                    "bot_id": bot_id,
                },
            )
        else:
            session_id = build_session_id(channel_type, channel_id, from_uid, bot_id)
        logger.info(f"[octo] 消息投递: external_key={external_key} session_id={session_id}")

        # 构建上下文前缀（成员列表 + 历史消息 + 发送者标签）
        # 直接拼到 content 前缀，随用户消息一起持久化到 session DB
        if is_group_or_thread:
            parent_no = extract_parent_group_no(channel_id)
            members = get_cached_members(parent_no)
            uid_to_name = build_uid_to_name_map(members) if members else {}
            member_prefix = build_member_list_prefix(members) if members else ""
        else:
            # 私聊：无群成员列表，通过 API 获取发送者名称
            uid_to_name = {}
            member_prefix = ""
            try:
                user_info = await bot_api.get_user_info(from_uid)
                if user_info and user_info.get("name"):
                    uid_to_name[from_uid] = user_info["name"]
            except Exception:
                logger.debug(f"[octo] 获取用户信息失败: {from_uid}", exc_info=True)

        # 从 API 拉取历史消息并格式化（群聊和私聊都需要）
        #    补偿 agent 离线期间丢失的消息——session DB 里没有这些
        try:
            history_prefix = await fetch_and_build_history(
                api=bot_api,
                channel_id=channel_id,
                channel_type=channel_type,
                bot_uid=bot_uid,
                current_message_id=message_id,
                uid_to_name=uid_to_name,
                bot_id=bot_id,
            )
        except Exception:
            logger.warning(f"[octo] 拉取历史消息失败，跳过历史注入: channel={channel_id}", exc_info=True)
            history_prefix = ""

        # 拼接上下文前缀到 content，随用户消息持久化
        sender_label = build_sender_label(from_uid, uid_to_name)
        parts = []

        if member_prefix:
            parts.append(
                f'<OCTO_MEMBER_LIST desc="当前群聊的成员列表，用于 @ 人时查找 uid">\n'
                f'{member_prefix}\n'
                f'</OCTO_MEMBER_LIST>'
            )

        if history_prefix:
            parts.append(
                f'<OCTO_HISTORY desc="从 Octo API 拉取的频道历史消息，按上次回复分段标注。'
                f'已回答的消息不要重复回答，新消息仅供参考。当前消息只回答最后一条">\n'
                f'{history_prefix}\n'
                f'</OCTO_HISTORY>'
            )

        parts.append(
            f'<OCTO_CURRENT_MESSAGE desc="当前需要回复的消息">\n'
            f'[来自 {sender_label}]: {content}\n'
            f'</OCTO_CURRENT_MESSAGE>'
        )

        content = "\n\n".join(parts)

        # 存入站 message_seq，send() 回复成功后用于历史分段
        set_pending_inbound_seq(session_id, msg.get("message_seq", 0))

        # 记录 session → bot 映射（回复时查找用哪个 bot 发送）
        self._session_bots[session_id] = bot_id

        await self.receive(
            session_id=session_id,
            data={
                "session_id": session_id,
                "content": content,
                "from_uid": from_uid,
                "channel_id": channel_id,
                "channel_type": channel_type,
                "message_id": message_id,
                "octo_external_key": external_key,
            },
            metadata={
                "octo_message_id": message_id,
                "octo_external_key": external_key,
                "agent_id": agent_id,
            },
        )
        logger.info("[octo] 消息已投递到 EventBus")

    async def _refresh_member_cache_if_needed(self, group_no: str, bot_api: OctoBotApi) -> None:
        """检查成员缓存，若过期则异步刷新。

        Thread 的 channel_id 为复合格式 "groupNo____threadId"，
        需要提取父群号才能调 members API。
        刷新失败不阻塞消息处理（如 bot 不在群里返回 403 是正常情况）。
        """
        # Thread 的复合 ID 需要拆出纯 groupNo 才能调 members API
        parent_group_no = extract_parent_group_no(group_no)

        cached = get_cached_members(parent_group_no)
        if cached is not None:
            return

        logger.info(f"[octo] 成员缓存未命中，开始刷新: group={parent_group_no}")
        try:
            members = await bot_api.get_group_members(parent_group_no)
            set_cached_members(parent_group_no, members)
        except Exception:
            # bot 不在群里（403）是正常情况，用 WARNING 而非 ERROR
            logger.warning(f"[octo] 刷新成员缓存失败: group={parent_group_no}", exc_info=True)

    async def send(self, msg: Any) -> None:
        """将 AgentLoop 产生的回复发送回 Octo。"""
        if not hasattr(msg, 'data') or not isinstance(msg.data, dict):
            return

        event_type: str = msg.data.get("type", "")
        event_data: dict[str, Any] = msg.data.get("data", {})

        # 只发送完整的 assistant 回复，忽略流式增量
        if event_type not in ("assistant_message_complete",):
            return

        content: str = event_data.get("content", "")
        if not content:
            return

        session_id: str = msg.to_session or msg.from_session
        logger.info(f"[octo] 发送回复: session_id={session_id} 内容长度={len(content)}")

        # 查找此 session 对应的 bot
        bot_id = self._session_bots.get(session_id, "")
        bot_info = self._bots.get(bot_id)
        if bot_info is None:
            logger.warning(f"[octo] 找不到 session 对应的 bot: session_id={session_id}")
            return

        bot_api: OctoBotApi = bot_info["api"]

        # 尝试从 session_id 解析 channel_type 和 channel_id
        parsed = parse_session_id(session_id)
        if parsed is None and self.session_manager is not None:
            external = await self.session_manager.get_external_session(session_id)
            if external:
                data = external.get("external_data") or {}
                try:
                    parsed = (int(data["channel_type"]), str(data["channel_id"]), str(data.get("bot_id", "")))
                except (KeyError, TypeError, ValueError):
                    parsed = None
        if parsed is None:
            logger.warning(f"[octo] 无法解析 session_id: {session_id}")
            return

        channel_type, channel_id, _bot_id = parsed
        logger.info(f"[octo] 回复目标: channel_type={channel_type} channel_id={channel_id} agent_id={bot_info['agent_id']}")

        try:
            result = await bot_api.send_message(
                channel_id=channel_id,
                channel_type=channel_type,
                content=content,
            )
            logger.info(f"[octo] 回复发送成功: message_id={result.get('message_id')}")
            # 回复成功后记录入站消息的 message_seq，用于下次历史分段
            # 参考原始项目 inbound.ts:2888-2896
            inbound_seq = take_pending_inbound_seq(session_id)
            if inbound_seq:
                record_bot_reply(channel_id, inbound_seq, bot_id)
        except Exception:
            logger.exception("[octo] 回复发送失败")

    async def stop(self) -> None:
        """停止 Channel：断开 WebSocket、取消协程、关闭 HTTP session、杀掉桥接进程。"""
        logger.info("[octo] 正在停止 Channel...")

        if self._ws_task and not self._ws_task.done():
            self._ws_task.cancel()
            try:
                await self._ws_task
            except asyncio.CancelledError:
                pass

        if self._bridge_reader_task and not self._bridge_reader_task.done():
            self._bridge_reader_task.cancel()

        if self._ws and not self._ws.closed:
            await self._ws.close()
            logger.info("[octo] WebSocket 连接已关闭")

        if self._session:
            await self._session.close()

        # 关闭所有 bot 的 HTTP session
        for bot_info in self._bots.values():
            await bot_info["api"].close()

        if self._bridge_proc:
            self._bridge_proc.terminate()
            try:
                self._bridge_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._bridge_proc.kill()
            logger.info("[octo] 桥接进程已终止")

        logger.info("[octo] Channel 已停止")