"""
Octo Channel Plugin for ftre

将 ftre agent 接入 Octo IM 平台，作为群聊 / 私聊 bot。
架构：Node.js 桥接（WuKongIM 二进制协议）←→ Python Channel（JSON WS）←→ ftre Bus ←→ AgentLoop
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

# Octo channel_type 常量
CHANNEL_TYPE_DM = 1
CHANNEL_TYPE_GROUP = 2
CHANNEL_TYPE_THREAD = 5


# ——————————————————————————————— Octo Bot API ——————————————————————————————————


class OctoBotApi:
    """Octo Bot API HTTP 客户端。"""

    def __init__(self, api_url: str, bot_token: str) -> None:
        self.api_url = api_url.rstrip("/")
        self.bot_token = bot_token
        self._session: aiohttp.ClientSession | None = None

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            self._session = aiohttp.ClientSession(
                headers={
                    "Authorization": f"Bearer {self.bot_token}",
                    "Content-Type": "application/json",
                }
            )
        return self._session

    async def register_bot(self) -> dict:
        """POST /v1/bot/register — 获取 robot_id / im_token / ws_url。"""
        session = await self._ensure_session()
        async with session.post(
            f"{self.api_url}/v1/bot/register",
            json={},
        ) as resp:
            data = await resp.json()
            if resp.status != 200:
                logger.error(f"[octo] register failed ({resp.status}): {data}")
                raise RuntimeError(f"Register bot failed ({resp.status}): {data}")
            logger.info(f"[octo] registered bot: {data.get('robot_id')}")
            return data

    async def send_message(
        self,
        channel_id: str,
        channel_type: int,
        content: str,
    ) -> dict:
        """POST /v1/bot/sendMessage — 发送文本消息。"""
        session = await self._ensure_session()
        payload = {
            "channel_id": channel_id,
            "channel_type": channel_type,
            "payload": {
                "type": 1,
                "content": content,
            },
        }
        logger.info(f"[octo] sendMessage: channel={channel_id} type={channel_type} len={len(content)}")
        async with session.post(
            f"{self.api_url}/v1/bot/sendMessage",
            json=payload,
        ) as resp:
            data = await resp.json()
            if resp.status != 200:
                logger.error(f"[octo] sendMessage failed ({resp.status}): {data}")
                raise RuntimeError(f"Send message failed ({resp.status}): {data}")
            logger.info(f"[octo] sendMessage OK: message_id={data.get('message_id')}")
            return data

    async def close(self) -> None:
        if self._session:
            await self._session.close()
            self._session = None


# ——————————————————————————————— Octo WebSocket Channel ——————————————————————


def _build_external_key(channel_type: int, channel_id: str, from_uid: str) -> str:
    cid = channel_id if channel_id else from_uid
    return f"octo:{channel_type}:{cid}"


def _build_session_id(channel_type: int, channel_id: str, from_uid: str) -> str:
    """格式: octo_{channel_type}_{channel_id}，DM 时 channel_id 用 from_uid"""
    cid = channel_id if channel_id else from_uid
    return f"octo_{channel_type}_{cid}"


def _parse_session_id(session_id: str) -> tuple[int, str] | None:
    """从 session_id 解析 (channel_type, channel_id)。"""
    parts = session_id.split("_", 2)
    if len(parts) < 3:
        return None
    try:
        channel_type = int(parts[1])
    except ValueError:
        return None
    return channel_type, parts[2]


class OctoChannel(Channel):
    """Octo WebSocket Channel — 通过 Node.js 桥接连接 WuKongIM 服务器。"""

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

    async def start(self) -> None:
        """启动 Node.js 桥接进程 → 连接本地 JSON WS → 启动消息循环。"""
        bridge_port = self.config.get('bridge_port', 9876)
        plugin_dir = Path(__file__).resolve().parent
        bridge_path = plugin_dir / 'octo-bridge.js'

        try:
            credentials = await self.api.register_bot()
            self._bot_uid = credentials.get("robot_id") or self._bot_uid
        except Exception:
            logger.exception("[octo] register bot failed before bridge start")

        logger.info(f"[octo] starting bridge: {bridge_path} port={bridge_port}")
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
        logger.info(f"[octo] bridge pid={self._bridge_proc.pid}")

        # 后台读取桥接进程日志输出（不阻塞）
        self._bridge_reader_task = asyncio.create_task(self._read_bridge_output())

        # 等待桥接启动
        await asyncio.sleep(3)

        # 检查进程是否还活着
        if self._bridge_proc.poll() is not None:
            logger.error(f"[octo] bridge process died immediately, exit code={self._bridge_proc.returncode}")
            return

        # 连本地 JSON WS
        ws_url = f'ws://127.0.0.1:{bridge_port}'
        logger.info(f"[octo] connecting to bridge WS: {ws_url}")
        try:
            self._session = aiohttp.ClientSession()
            self._ws = await self._session.ws_connect(ws_url)
            logger.info(f"[octo] bridge WS connected: {ws_url}")
        except Exception as e:
            logger.error(f"[octo] bridge WS connect failed: {e}")
            return

        # 启动消息循环（后台 task）
        self._ws_task = asyncio.create_task(self._ws_loop())
        logger.info("[octo] message loop started")

    async def _read_bridge_output(self) -> None:
        """后台读取桥接进程的 stdout，输出到 ftre 日志。"""
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
            logger.exception("[octo] error reading bridge output")

    async def _ws_loop(self) -> None:
        """WebSocket 消息循环。"""
        if self._ws is None:
            logger.warning("[octo] ws_loop: WS is None, exiting")
            return
        logger.info("[octo] ws_loop: entering message loop")
        try:
            async for msg in self._ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    try:
                        data = json.loads(msg.data)
                        msg_type = data.get('type')
                        logger.info(f"[octo] ws_loop: received type={msg_type}")
                        if msg_type == 'message':
                            await self._handle_message(data.get('data', {}))
                        else:
                            logger.info(f"[octo] ws_loop: ignoring type={msg_type}")
                    except json.JSONDecodeError:
                        logger.warning(f"[octo] ws_loop: bad JSON: {msg.data[:200]}")
                    except Exception:
                        logger.exception("[octo] ws_loop: error handling message")
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    logger.error(f"[octo] ws_loop: WS error: {self._ws.exception()}")
                    break
                elif msg.type == aiohttp.WSMsgType.CLOSED:
                    logger.info("[octo] ws_loop: WS closed")
                    break
        except asyncio.CancelledError:
            logger.info("[octo] ws_loop: cancelled")
        except Exception:
            logger.exception("[octo] ws_loop: unexpected error")

    async def _handle_message(self, msg: dict) -> None:
        """处理 WuKongIM 消息 → 投递到 Bus。"""
        payload = msg.get("payload", {})
        msg_type = payload.get("type")
        from_uid = msg.get("from_uid", "")
        channel_id = msg.get("channel_id", "")
        channel_type = msg.get("channel_type", CHANNEL_TYPE_DM)
        message_id = str(msg.get("message_id", ""))
        content = payload.get("content", "")
        is_event = bool((payload.get("event") or {}).get("type"))

        logger.info(
            f"[octo] _handle_message: from={from_uid} channel={channel_id} "
            f"ch_type={channel_type} payload_type={msg_type} "
            f"content={content[:80]!r}"
        )

        # Match OpenClaw's loop guard: skip the bot's own non-event messages.
        if self._bot_uid and from_uid == self._bot_uid and not is_event:
            logger.info(f"[octo] _handle_message: skipping self message from={from_uid}")
            return

        if msg_type != 1:  # 非文本消息
            logger.info(f"[octo] _handle_message: skipping non-text type={msg_type}")
            return

        # DM: channel_id 为空，用 from_uid 作为 reply target
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
        logger.info(f"[octo] _handle_message: external_key={external_key} session_id={session_id}")

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
        logger.info(f"[octo] _handle_message: receive() called successfully")

    async def send(self, msg) -> None:
        """推送 outbound 消息到 Octo。"""
        if not hasattr(msg, 'data') or not isinstance(msg.data, dict):
            return

        event_type = msg.data.get("type", "")
        event_data = msg.data.get("data", {})

        # 只发送完整的 assistant 回复
        if event_type not in ("assistant_message_complete",):
            return

        content = event_data.get("content", "")
        if not content:
            return

        session_id = msg.to_session or msg.from_session
        logger.info(f"[octo] send: session_id={session_id} content_len={len(content)}")

        parsed = _parse_session_id(session_id)
        if parsed is None and self.session_manager is not None:
            external = await self.session_manager.get_external_session(session_id)
            if external:
                data = external.get("external_data") or {}
                try:
                    parsed = (int(data["channel_type"]), str(data["channel_id"]))
                except (KeyError, TypeError, ValueError):
                    parsed = None
        if parsed is None:
            logger.warning(f"[octo] send: cannot parse session_id: {session_id}")
            return

        channel_type, channel_id = parsed
        logger.info(f"[octo] send: parsed ch_type={channel_type} ch_id={channel_id}")

        try:
            result = await self.api.send_message(
                channel_id=channel_id,
                channel_type=channel_type,
                content=content,
            )
            logger.info(f"[octo] send: OK message_id={result.get('message_id')}")
        except Exception:
            logger.exception("[octo] send: failed")

    async def stop(self) -> None:
        """断开 WS 连接，取消消息循环，关闭 HTTP session，杀掉桥接进程。"""
        logger.info("[octo] stop: shutting down...")

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
            logger.info("[octo] stop: WS closed")

        if self._session:
            await self._session.close()

        await self.api.close()

        if self._bridge_proc:
            self._bridge_proc.terminate()
            try:
                self._bridge_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._bridge_proc.kill()
            logger.info("[octo] stop: bridge process killed")

        logger.info("[octo] stop: done")


# ——————————————————————————————— Plugin Entry ——————————————————————————————————


class OctoChannelPlugin(Plugin):
    """Octo Channel Plugin。"""

    name = "octo_channel"
    version = "1.0.0"

    def setup(self) -> None:
        config = self.api.config or {}
        logger.info(f"[octo] setup: api_url={config.get('api_url')} bot_token={config.get('bot_token', '')[:8]}...")

        channel = OctoChannel(config, self.api.bus, session_manager=self.api.session_manager)
        self.api.register_channel(channel)
        logger.info("[octo] setup: channel registered")

        self.api.register_hook(BEFORE_AGENT_RUN, self._on_agent_run)
        logger.info("[octo] setup: before_agent_run hook registered")

    def _on_agent_run(self, ctx):
        """BEFORE_AGENT_RUN hook: 注入 Octo 平台提示。"""
        if ctx.channel_id != "octo":
            return ctx

        hint = (
            "You are a bot on the Octo IM platform. "
            "You receive messages from users and reply via the channel."
        )

        # messages 可能是 str 或 list[dict]
        if isinstance(ctx.messages, str):
            logger.info("[octo] hook: messages is str, wrapping into list")
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
            logger.info(f"[octo] hook: injected into {len(ctx.messages)} messages")
        return ctx

    def teardown(self) -> None:
        pass
