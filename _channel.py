"""
Octo Channel Plugin — Octo WebSocket Channel。

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

from __future__ import annotations

import asyncio
import json
import logging
import subprocess
from pathlib import Path
from typing import Any

import aiohttp

from ftre.channel.base import Channel

from _api import OctoBotApi
from _constants import (
    CHANNEL_TYPE_DM,
    CHANNEL_TYPE_GROUP,
    build_external_key,
    build_session_id,
    parse_session_id,
)
from _members import get_cached_members, set_cached_members
from _mention import check_mentioned

logger = logging.getLogger("ftre.plugin.octo_channel")


class OctoChannel(Channel):  # type: ignore[misc]
    """Octo WebSocket Channel。

    负责：
      1. 启动 Node.js 桥接进程（octo-bridge.js）
      2. 连接桥接的本地 JSON WebSocket 接口
      3. 将 Octo 入站消息转换为 BusMessage 投递到 EventBus
      4. 将 AgentLoop 产生的回复通过 Octo API 发送回用户
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
        self.api: OctoBotApi = OctoBotApi(config.get("api_url", ""), config.get("bot_token", ""))
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._session: aiohttp.ClientSession | None = None
        self._ws_task: asyncio.Task[Any] | None = None
        self._bridge_proc: subprocess.Popen[str] | None = None
        self._bridge_reader_task: asyncio.Task[Any] | None = None
        self._bot_uid: str = config.get("bot_id") or config.get("robot_id") or ""
        self._bot_name: str = config.get("bot_name") or config.get("bot_id") or ""
        # require_mention=True 时，群聊中只有被 @ 才回复（默认行为）
        # 设为 False 则群聊中所有消息都回复（类似免@）
        self.require_mention: bool = config.get("require_mention", True)

    async def start(self) -> None:
        """启动 Channel：注册 bot → 启动桥接进程 → 连接本地 JSON WS → 开启消息循环。"""
        bridge_port: int = self.config.get('bridge_port', 9876)
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
            f"内容={content[:80]!r}"
        )

        # 过滤 bot 自己的消息（与 OpenClaw 保持一致，事件消息除外）
        if self._bot_uid and from_uid == self._bot_uid and not is_event:
            logger.info(f"[octo] 跳过自己的消息: from_uid={from_uid}")
            return

        # 群聊 @ 检测门控：require_mention 为 True 时，只有被 @ 才回复
        if channel_type == CHANNEL_TYPE_GROUP and self.require_mention:
            if not check_mentioned(payload, content, self._bot_uid, self._bot_name):
                logger.info(
                    f"[octo] 群聊消息未 @ bot，跳过: "
                    f"发送者={from_uid} 频道={channel_id}"
                )
                return

        # 非文本消息暂不处理（MVP 阶段只支持纯文本）
        if msg_type != 1:
            logger.info(f"[octo] 跳过非文本消息: type={msg_type}")
            return

        # 群聊消息：刷新成员缓存（用于 @ 检测白名单 + Agent 上下文）
        if channel_type == CHANNEL_TYPE_GROUP and channel_id:
            await self._refresh_member_cache_if_needed(channel_id)

        # 私聊时 channel_id 为空，使用发送者 uid 作为回复目标
        if not channel_id:
            channel_type = CHANNEL_TYPE_DM
            channel_id = from_uid

        external_key = build_external_key(channel_type, channel_id, from_uid)
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
            session_id = build_session_id(channel_type, channel_id, from_uid)
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
        logger.info("[octo] 消息已投递到 EventBus")

    async def _refresh_member_cache_if_needed(self, group_no: str) -> None:
        """检查成员缓存，若过期则异步刷新。"""
        cached = get_cached_members(group_no)
        if cached is not None:
            return

        logger.info(f"[octo] 成员缓存未命中，开始刷新: group={group_no}")
        try:
            members = await self.api.get_group_members(group_no)
            set_cached_members(group_no, members)
        except Exception:
            logger.exception(f"[octo] 刷新成员缓存失败: group={group_no}")

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

        # 尝试从 session_id 解析 channel_type 和 channel_id
        parsed = parse_session_id(session_id)
        if parsed is None and self.session_manager is not None:
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