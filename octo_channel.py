"""
Octo Channel Plugin for ftre

将 ftre agent 接入 Octo IM 平台（基于 WuKongIM 协议），作为群聊/私聊 bot 使用。

架构：
  Octo 服务器 ←(WuKongIM 二进制 WS)→ octo-bridge.js (Node.js 桥接)
       ←(JSON WS ws://127.0.0.1:9876)→ OctoChannel (Python) ←→ ftre EventBus ←→ AgentLoop

数据流：
  入站：Octo 用户发消息 → WuKongIM WS → 桥接解密 → JSON WS → _handle_message() → BusMessage → AgentLoop
  出站：AgentLoop 产生回复 → ChannelManager._dispatch_loop() → send() → Octo sendMessage API

依赖：
  - ftre Channel 基类：提供 receive()/send() 抽象，通过 EventBus 与 AgentLoop 通信
  - ftre Plugin 体系：setup() 中注册 Channel + Hook，Gateway 启动时自动加载
  - Node.js 桥接：处理 WuKongIM 二进制协议（DH 密钥交换 + AES-128-CBC 解密）
  - aiohttp：Python 端 WebSocket 客户端和 HTTP 客户端
"""

import asyncio
import json
import logging
import subprocess
from pathlib import Path

import aiohttp

from ftre.plugin import Plugin, BEFORE_AGENT_RUN
from ftre.channel.base import Channel

logger = logging.getLogger("ftre.plugin.octo_channel")

# Octo 消息通道类型常量
# 1=私聊(DM), 2=群聊(Group), 5=讨论串(Thread)
CHANNEL_TYPE_DM = 1
CHANNEL_TYPE_GROUP = 2
CHANNEL_TYPE_THREAD = 5


# ——————————————————————————————— Octo Bot API 客户端 ——————————————————————————


class OctoBotApi:
    """Octo Bot API HTTP 客户端。

    封装 Octo 平台的 REST API 调用，使用 bot_token 认证。
    主要接口：
      - POST /v1/bot/register   注册 bot，获取 robot_id / im_token / ws_url
      - POST /v1/bot/sendMessage 发送文本消息（注意：sendMessage 是驼峰命名）
    """

    def __init__(self, api_url: str, bot_token: str) -> None:
        self.api_url = api_url.rstrip("/")
        self.bot_token = bot_token
        self._session: aiohttp.ClientSession | None = None

    async def _ensure_session(self) -> aiohttp.ClientSession:
        """惰性创建 HTTP session，复用连接。"""
        if self._session is None:
            self._session = aiohttp.ClientSession(
                headers={
                    "Authorization": f"Bearer {self.bot_token}",
                    "Content-Type": "application/json",
                }
            )
        return self._session

    async def register_bot(self) -> dict:
        """注册 bot，获取连接凭证。

        POST /v1/bot/register
        请求体为空 JSON {}，认证使用 bot_token。

        返回字段：
          - robot_id:    bot 唯一标识
          - im_token:    WuKongIM 连接令牌
          - ws_url:      WebSocket 服务器地址
          - owner_uid:   bot 所有者用户 ID
          - api_url:     API 服务器地址
        """
        session = await self._ensure_session()
        async with session.post(
            f"{self.api_url}/v1/bot/register",
            json={},
        ) as resp:
            data = await resp.json()
            if resp.status != 200:
                logger.error(f"[octo] bot 注册失败，HTTP {resp.status}: {data}")
                raise RuntimeError(f"Bot 注册失败 ({resp.status}): {data}")
            logger.info(f"[octo] bot 注册成功: robot_id={data.get('robot_id')}")
            return data

    async def send_message(
        self,
        channel_id: str,
        channel_type: int,
        content: str,
    ) -> dict:
        """发送文本消息到指定频道。

        POST /v1/bot/sendMessage（注意：端点使用驼峰命名 sendMessage）

        参数：
          channel_id:   目标频道 ID。私聊时为对方 uid，群聊时为 group_no
          channel_type: 频道类型。1=私聊，2=群聊，5=讨论串
          content:      消息正文（纯文本）

        请求体格式：
          {
            "channel_id": "...",
            "channel_type": 1,
            "payload": {
              "type": 1,
              "content": "消息内容"
            }
          }
        """
        session = await self._ensure_session()
        payload = {
            "channel_id": channel_id,
            "channel_type": channel_type,
            "payload": {
                "type": 1,
                "content": content,
            },
        }
        logger.info(f"[octo] 发送消息: channel={channel_id} type={channel_type} 内容长度={len(content)}")
        async with session.post(
            f"{self.api_url}/v1/bot/sendMessage",
            json=payload,
        ) as resp:
            data = await resp.json()
            if resp.status != 200:
                logger.error(f"[octo] 消息发送失败，HTTP {resp.status}: {data}")
                raise RuntimeError(f"消息发送失败 ({resp.status}): {data}")
            logger.info(f"[octo] 消息发送成功: message_id={data.get('message_id')}")
            return data

    async def close(self) -> None:
        """关闭 HTTP session，释放连接。"""
        if self._session:
            await self._session.close()
            self._session = None


# ——————————————————————————————— Octo WebSocket Channel ——————————————————————


# session_id 和 external_key 的编码/解码工具函数
# session_id 格式: "octo_{channel_type}_{channel_id}"
#   - 私聊: "octo_1_{uid}"
#   - 群聊: "octo_2_{group_no}"
# external_key 格式: "octo:{channel_type}:{channel_id}"
#   - 用于跨组件传递 Octo 会话的唯一标识，避免与 session_id 混淆


def _build_external_key(channel_type: int, channel_id: str, from_uid: str) -> str:
    """构造 external_key 用于跨组件传递 Octo 会话标识。"""
    cid = channel_id if channel_id else from_uid
    return f"octo:{channel_type}:{cid}"


def _build_session_id(channel_type: int, channel_id: str, from_uid: str) -> str:
    """构造 session_id 用于 ftre 内部会话管理。

    格式: octo_{channel_type}_{channel_id}
    私聊时 channel_id 为空，则用 from_uid 替代。
    """
    cid = channel_id if channel_id else from_uid
    return f"octo_{channel_type}_{cid}"


def _parse_session_id(session_id: str) -> tuple[int, str] | None:
    """从 session_id 反向解析出 (channel_type, channel_id)。

    解析失败返回 None。"""
    parts = session_id.split("_", 2)
    if len(parts) < 3:
        return None
    try:
        channel_type = int(parts[1])
    except ValueError:
        return None
    return channel_type, parts[2]


class OctoChannel(Channel):
    """Octo WebSocket Channel。

    负责：
      1. 启动 Node.js 桥接进程（octo-bridge.js）
      2. 连接桥接的本地 JSON WebSocket 接口
      3. 将 Octo 入站消息转换为 BusMessage 投递到 EventBus
      4. 将 AgentLoop 产生的回复通过 Octo API 发送回用户

    桥接进程负责：
      - WuKongIM 二进制协议（CONNECT/CONNACK/RECV/RECVACK/PING/PONG）
      - DH 密钥交换（curve25519）+ AES-128-CBC 解密
      - 将解密后的消息以 JSON 格式转发到本地 WebSocket
    """

    def __init__(
        self,
        config: dict,
        bus,
        session_manager=None,
        channel_id: str = "octo",
        name: str = "Octo Channel",
    ) -> None:
        super().__init__(channel_id, name, bus)
        self.config = config
        self.session_manager = session_manager
        self.api = OctoBotApi(config.get("api_url", ""), config.get("bot_token", ""))
        self._ws = None
        self._session = None
        self._ws_task: asyncio.Task | None = None
        self._bridge_proc = None
        self._bridge_reader_task: asyncio.Task | None = None
        self._bot_uid = config.get("bot_id") or config.get("robot_id") or ""
        self._bot_name = config.get("bot_name") or config.get("bot_id") or ""
        # require_mention=True 时，群聊中只有被 @ 才回复（默认行为）
        # 设为 False 则群聊中所有消息都回复（类似免@）
        self.require_mention = config.get("require_mention", True)

    async def start(self) -> None:
        """启动 Channel：注册 bot → 启动桥接进程 → 连接本地 JSON WS → 开启消息循环。"""
        bridge_port = self.config.get('bridge_port', 9876)
        plugin_dir = Path(__file__).resolve().parent
        bridge_path = plugin_dir / 'octo-bridge.js'

        # 先注册 bot 获取 bot_uid，用于后续过滤自己的消息
        try:
            credentials = await self.api.register_bot()
            self._bot_uid = credentials.get("robot_id") or self._bot_uid
        except Exception:
            logger.exception("[octo] 启动前注册 bot 失败，继续尝试启动桥接")

        logger.info(f"[octo] 启动桥接进程: {bridge_path} 端口={bridge_port}")
        self._bridge_proc = subprocess.Popen(
            ['node', str(bridge_path),
             '--api-url', self.config['api_url'],
             '--bot-token', self.config['bot_token'],
             '--port', str(bridge_port)],
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
        """后台读取桥接进程的 stdout 并转发到 ftre 日志。

        这样可以在 ftre 的统一日志中看到桥接的运行状态，
        包括 WuKongIM 连接/断开、消息收发、错误信息等。
        """
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
        """从桥接 JSON WebSocket 接收消息并分发的循环。

        消息格式（来自桥接）：
          {"type": "message", "data": { ... WuKongIM 消息字段 ... }}

        处理流程：
          1. 解析 JSON
          2. 过滤 type 字段
          3. 调用 _handle_message() 处理消息
        """
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

    def _is_mentioned(self, payload: dict, content: str) -> bool:
        """检测 bot 是否在消息中被 @。

        检测顺序（按优先级）：
          1. mention.uids 包含 bot_uid → 被直接 @
          2. mention.ais=1 → @AI / @所有AI
          3. 文本兜底：消息内容中正则匹配 @bot名称

        返回 True 表示 bot 被提及，应回复。
        """
        mention = payload.get("mention") or {}

        # 1. 直接 @bot
        uids = mention.get("uids") or []
        if self._bot_uid and self._bot_uid in uids:
            logger.debug(f"[octo] 被直接 @: bot_uid={self._bot_uid}")
            return True

        # 2. @AI / @所有AI
        ais = mention.get("ais")
        if ais is True or ais == 1:
            logger.debug("[octo] 被 @AI 提及")
            return True

        # 3. 文本兜底：检查内容中是否包含 @bot名称
        #    注意：mention payload 通常由 Octo 服务端填充，这里作为兜底
        if content and self._bot_name:
            import re
            escaped = re.escape(self._bot_name)
            pattern = re.compile(rf"(?:^|\s)@{escaped}(?:\s|$)")
            if pattern.search(content):
                logger.debug(f"[octo] 文本兜底检测到 @{self._bot_name}")
                return True

        return False

    async def _handle_message(self, msg: dict) -> None:
        """处理一条 WuKongIM 消息，转换为 BusMessage 投递到 EventBus。

        WuKongIM 消息字段（桥接已解密）：
          - message_id:   消息唯一 ID
          - message_seq:  消息序号
          - from_uid:     发送者 UID
          - channel_id:   频道 ID（私聊时为空，群聊时为 group_no）
          - channel_type: 频道类型，1=私聊 2=群聊 5=Thread
          - timestamp:    消息时间戳
          - payload:      消息内容 {"type": 1, "content": "文本"}

        处理逻辑：
          1. 过滤自己的消息（避免 bot 回复自己的消息形成死循环）
          2. 过滤非文本消息（MVP 只处理 type=1 的文本消息）
          3. 私聊时用 from_uid 作为 channel_id 回复目标
          4. 通过 session_manager 获取或创建对应的 ftre session
          5. 调用 receive() 将消息投递到 EventBus
        """
        payload = msg.get("payload", {})
        msg_type = payload.get("type")
        from_uid = msg.get("from_uid", "")
        channel_id = msg.get("channel_id", "")
        channel_type = msg.get("channel_type", CHANNEL_TYPE_DM)
        message_id = str(msg.get("message_id", ""))
        content = payload.get("content", "")
        is_event = bool((payload.get("event") or {}).get("type"))

        logger.info(
            f"[octo] 收到消息: 发送者={from_uid} 频道={channel_id} "
            f"频道类型={channel_type} 消息类型={msg_type} "
            f"内容={content[:80]!r}"
        )

        # 过滤 bot 自己的消息（与 OpenClaw 保持一致，事件消息除外）
        if self._bot_uid and from_uid == self._bot_uid and not is_event:
            logger.info(f"[octo] 跳过自己的消息: from_uid={from_uid}")
            return

        # 群聊 @ 检测门控：require_mention 为 True 时，只有被 @ 才回复
        if channel_type == CHANNEL_TYPE_GROUP and self.require_mention:
            if not self._is_mentioned(payload, content):
                logger.info(
                    f"[octo] 群聊消息未 @ bot，跳过: "
                    f"发送者={from_uid} 频道={channel_id}"
                )
                return

        # 非文本消息暂不处理（MVP 阶段只支持纯文本）
        if msg_type != 1:
            logger.info(f"[octo] 跳过非文本消息: type={msg_type}")
            return

        # 私聊时 channel_id 为空，使用发送者 uid 作为回复目标
        if not channel_id:
            channel_type = CHANNEL_TYPE_DM
            channel_id = from_uid

        external_key = _build_external_key(channel_type, channel_id, from_uid)
        if self.session_manager is not None:
            session_id = await self.session_manager.get_or_create_external_session(
                channel_id=self.channel_id,
                external_key=external_key,
                title=f"Octo {channel_id}",
                external_data={
                    "channel_type": channel_type,
                    "channel_id": channel_id,
                    "from_uid": from_uid,
                },
            )
        else:
            session_id = _build_session_id(channel_type, channel_id, from_uid)
        logger.info(f"[octo] 消息投递: external_key={external_key} session_id={session_id}")

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
            metadata={"octo_message_id": message_id, "octo_external_key": external_key},
        )
        logger.info(f"[octo] 消息已投递到 EventBus")

    async def send(self, msg) -> None:
        """将 AgentLoop 产生的回复发送回 Octo。

        ChannelManager 通过 _dispatch_loop() 将 outbound BusMessage
        分发到对应 Channel 的 send() 方法。

        BusMessage.data 格式：
          {"type": "assistant_message_complete", "data": {"content": "回复内容"}}

        只处理 assistant_message_complete 类型的事件（流式增量 assistant_message 忽略）。
        """
        if not hasattr(msg, 'data') or not isinstance(msg.data, dict):
            return

        event_type = msg.data.get("type", "")
        event_data = msg.data.get("data", {})

        # 只发送完整的 assistant 回复，忽略流式增量
        if event_type not in ("assistant_message_complete",):
            return

        content = event_data.get("content", "")
        if not content:
            return

        session_id = msg.to_session or msg.from_session
        logger.info(f"[octo] 发送回复: session_id={session_id} 内容长度={len(content)}")

        # 尝试从 session_id 解析 channel_type 和 channel_id
        parsed = _parse_session_id(session_id)
        if parsed is None and self.session_manager is not None:
            # 如果 session_id 不是标准格式，尝试从 session_manager 获取 external_data
            external = await self.session_manager.get_external_session(session_id)
            if external:
                data = external.get("external_data") or {}
                try:
                    parsed = (int(data["channel_type"]), str(data["channel_id"]))
                except (KeyError, TypeError, ValueError):
                    parsed = None
        if parsed is None:
            logger.warning(f"[octo] 无法解析 session_id: {session_id}")
            return

        channel_type, channel_id = parsed
        logger.info(f"[octo] 回复目标: channel_type={channel_type} channel_id={channel_id}")

        try:
            result = await self.api.send_message(
                channel_id=channel_id,
                channel_type=channel_type,
                content=content,
            )
            logger.info(f"[octo] 回复发送成功: message_id={result.get('message_id')}")
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

        await self.api.close()

        if self._bridge_proc:
            self._bridge_proc.terminate()
            try:
                self._bridge_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._bridge_proc.kill()
            logger.info("[octo] 桥接进程已终止")

        logger.info("[octo] Channel 已停止")


# ——————————————————————————————— Plugin 入口 ——————————————————————————————————


class OctoChannelPlugin(Plugin):
    """Octo Channel Plugin。

    在 ftre Gateway 启动时自动加载，完成以下初始化：
      1. 创建 OctoChannel 实例并注册到 ChannelManager
      2. 注册 BEFORE_AGENT_RUN hook，在每次 Agent 运行前注入 Octo 平台提示

    配置来源：~/.ftre/config.json 中 plugins 数组的 config 字段
      {
        "name": "octo_channel",
        "config": {
          "bot_token": "bf_xxx",
          "api_url": "https://im.deepminer.com.cn/api",
          "bridge_port": 9876
        }
      }
    """

    name = "octo_channel"
    version = "1.0.0"

    def setup(self) -> None:
        """插件初始化：注册 Channel 和 Hook。"""
        config = self.api.config or {}
        logger.info(
            f"[octo] 插件初始化: api_url={config.get('api_url')} "
            f"bot_token={config.get('bot_token', '')[:8]}..."
        )

        channel = OctoChannel(config, self.api.bus, session_manager=self.api.session_manager)
        self.api.register_channel(channel)
        logger.info("[octo] Channel 已注册到 ChannelManager")

        self.api.register_hook(BEFORE_AGENT_RUN, self._on_agent_run)
        logger.info("[octo] before_agent_run Hook 已注册")

    def _on_agent_run(self, ctx):
        """BEFORE_AGENT_RUN Hook：在 Agent 每次运行前注入 Octo 平台提示。

        仅在 channel_id 为 "octo" 时生效，避免影响其他 channel 的会话。

        注入方式：
          - 如果 ctx.messages 是字符串（单条用户消息），包装为 list 并插入 system 消息
          - 如果 ctx.messages 是列表，追加到已有的 system 消息中，没有则插入一条新的
        """
        if ctx.channel_id != "octo":
            return ctx

        hint = (
            "你是 Octo IM 平台上的一个 bot。"
            "你通过频道接收用户消息并回复。"
        )

        if isinstance(ctx.messages, str):
            logger.info("[octo] Hook: messages 为字符串，包装为 list")
            ctx.messages = [
                {"role": "system", "content": hint},
                {"role": "user", "content": ctx.messages},
            ]
        elif isinstance(ctx.messages, list):
            for msg in ctx.messages:
                if isinstance(msg, dict) and msg.get("role") == "system":
                    if hint not in msg["content"]:
                        msg["content"] = f"{msg['content']}\n\n{hint}"
                    break
            else:
                ctx.messages.insert(0, {"role": "system", "content": hint})
            logger.info(f"[octo] Hook: 已注入 Octo 提示，消息数={len(ctx.messages)}")
        return ctx

    def teardown(self) -> None:
        pass