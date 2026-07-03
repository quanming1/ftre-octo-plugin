"""
Octo Channel Plugin — 常量 + Octo Bot API HTTP 客户端。

常量定义：
  - CHANNEL_TYPE_DM/GROUP/THREAD: 频道类型
  - session_id 格式: "octo_{channel_type}_{channel_id}"
  - external_key 格式: "octo:{channel_type}:{channel_id}"

API 接口：
  - POST /v1/bot/register    注册 bot，获取 robot_id / im_token / ws_url
  - POST /v1/bot/sendMessage  发送文本消息（注意：sendMessage 是驼峰命名）
  - GET  /v1/bot/groups       获取 bot 加入的群列表
  - GET  /v1/bot/groups/{no}   获取群信息
  - GET  /v1/bot/groups/{no}/members  获取群成员
  - GET  /v1/bot/space/members 搜索空间成员
  - POST /v1/bot/messages/sync 获取频道历史消息
"""

import base64
import json
import logging
import uuid
from typing import Any

import aiohttp

logger = logging.getLogger("ftre.plugin.octo_channel")

# ─── 常量 ──────────────────────────────────────────────────────────────

# Octo 消息通道类型常量
# 1=私聊(DM), 2=群聊(Group), 5=讨论串(Thread)
CHANNEL_TYPE_DM = 1
CHANNEL_TYPE_GROUP = 2
CHANNEL_TYPE_THREAD = 5


def build_external_key(channel_type: int, channel_id: str, from_uid: str, bot_id: str = "") -> str:
    """构造 external_key 用于跨组件传递 Octo 会话标识。

    external_key 格式: "octo:{channel_type}:{channel_id}:{bot_id}"
    私聊时 channel_id 为空，则用 from_uid 替代。
    bot_id 区分同一群内不同 bot 的 session。
    """
    cid = channel_id if channel_id else from_uid
    return f"octo:{channel_type}:{cid}:{bot_id}"


def build_session_id(channel_type: int, channel_id: str, from_uid: str, bot_id: str = "") -> str:
    """构造 session_id 用于 ftre 内部会话管理。

    session_id 格式: "octo_{channel_type}_{channel_id}_{bot_id}"
    私聊时 channel_id 为空，则用 from_uid 替代。
    bot_id 区分同一群内不同 bot 的 session。
    """
    cid = channel_id if channel_id else from_uid
    return f"octo_{channel_type}_{cid}_{bot_id}"


def parse_session_id(session_id: str) -> tuple[int, str, str] | None:
    """从 session_id 反向解析出 (channel_type, channel_id, bot_id)。

    解析失败返回 None。
    """
    parts = session_id.split("_", 3)
    if len(parts) < 4:
        return None
    try:
        channel_type = int(parts[1])
    except ValueError:
        return None
    return channel_type, parts[2], parts[3]


def extract_parent_group_no(channel_id: str) -> str:
    """从复合 channel_id 中提取父群号。

    Thread 的 channel_id 格式为 "groupNo____threadId"（4 个下划线）。
    普通群聊直接返回原值。
    """
    if "____" in channel_id:
        return channel_id.split("____", 1)[0]
    return channel_id


# ─── OctoBotApi ────────────────────────────────────────────────────────


class OctoBotApi:
    """Octo Bot API HTTP 客户端。

    封装 Octo 平台的 REST API 调用，使用 bot_token 认证。
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

    async def register_bot(self) -> dict[str, Any]:
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
            data: dict[str, Any] = await resp.json()
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
    ) -> dict[str, Any]:
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
        # client_msg_no: WuKongIM 服务端据此去重，重试不会产生重复消息
        payload = {
            "channel_id": channel_id,
            "channel_type": channel_type,
            "payload": {
                "type": 1,
                "content": content,
            },
            "client_msg_no": str(uuid.uuid4()),
        }
        logger.info(f"[octo] 发送消息: channel={channel_id} type={channel_type} 内容长度={len(content)}")
        async with session.post(
            f"{self.api_url}/v1/bot/sendMessage",
            json=payload,
        ) as resp:
            data: dict[str, Any] = await resp.json()
            if resp.status != 200:
                logger.error(f"[octo] 消息发送失败，HTTP {resp.status}: {data}")
                raise RuntimeError(f"消息发送失败 ({resp.status}): {data}")
            logger.info(f"[octo] 消息发送成功: message_id={data.get('message_id')}")
            return data

    async def get_group_members(self, group_no: str) -> list[dict[str, Any]]:
        """获取群成员列表。

        GET /v1/bot/groups/{groupNo}/members

        返回成员列表，每个成员包含：
          - uid:   用户唯一标识
          - name:  用户显示名称
          - role:  角色（admin/member）
          - robot: 是否为机器人（1=True, 0=False）

        用于：
          - @ 检测门控的 human-only 白名单
          - 向 agent 展示群成员信息
        """
        session = await self._ensure_session()
        url = f"{self.api_url}/v1/bot/groups/{group_no}/members"
        logger.debug(f"[octo] 获取群成员: group_no={group_no}")
        async with session.get(url) as resp:
            data: dict[str, Any] = await resp.json()
            if resp.status != 200:
                logger.error(f"[octo] 获取群成员失败，HTTP {resp.status}: {data}")
                raise RuntimeError(f"获取群成员失败 ({resp.status}): {data}")
            # 标准化：兼容 members 字段或直接数组两种返回格式
            raw: Any = data.get("members") if isinstance(data, dict) else data
            if isinstance(raw, list):
                members: list[dict[str, Any]] = raw
            else:
                members = []
            logger.debug(f"[octo] 群成员获取成功: {len(members)} 人")
            return members

    async def list_groups(self) -> list[dict[str, Any]]:
        """获取 bot 加入的群列表。

        GET /v1/bot/groups

        返回群列表，每个群包含 group_no 和 name。
        """
        session = await self._ensure_session()
        url = f"{self.api_url}/v1/bot/groups"
        logger.debug("[octo] 获取群列表")
        async with session.get(url) as resp:
            data: Any = await resp.json()
            if resp.status != 200:
                logger.warning(f"[octo] 获取群列表失败，HTTP {resp.status}")
                return []
            return data if isinstance(data, list) else []

    async def get_group_info(self, group_no: str) -> dict[str, Any]:
        """获取群信息。

        GET /v1/bot/groups/{groupNo}

        返回群信息，包含 group_no、name、member_count 等。
        """
        session = await self._ensure_session()
        url = f"{self.api_url}/v1/bot/groups/{group_no}"
        logger.debug(f"[octo] 获取群信息: group_no={group_no}")
        async with session.get(url) as resp:
            data: dict[str, Any] = await resp.json()
            if resp.status != 200:
                raise RuntimeError(f"获取群信息失败 ({resp.status}): {data}")
            return data

    async def search_space_members(self, keyword: str = "", limit: int = 20) -> list[dict[str, Any]]:
        """搜索空间成员。

        GET /v1/bot/space/members?keyword=...&limit=...

        返回成员列表，包含 uid、name、robot。
        """
        session = await self._ensure_session()
        params = {}
        if keyword:
            params["keyword"] = keyword
        if limit:
            params["limit"] = str(limit)
        url = f"{self.api_url}/v1/bot/space/members"
        logger.debug(f"[octo] 搜索成员: keyword={keyword}")
        async with session.get(url, params=params) as resp:
            data: Any = await resp.json()
            if resp.status != 200:
                logger.warning(f"[octo] 搜索成员失败，HTTP {resp.status}")
                return []
            return data if isinstance(data, list) else []

    async def get_channel_messages(
        self,
        channel_id: str,
        channel_type: int,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        """获取频道历史消息（用于注入上下文）。

        POST /v1/bot/messages/sync

        参数：
          channel_id:   频道 ID
          channel_type: 频道类型（1=私聊 2=群聊 5=讨论串）
          limit:        拉取条数，默认 20

        返回消息列表，每条包含：
          - from_uid:    发送者 UID
          - message_id:  消息 ID
          - message_seq: 消息序号
          - content:     文本内容（从 base64 payload 解码）
          - type:        消息类型
          - timestamp:   时间戳（秒）

        API 返回的 payload 是 base64 编码的 JSON 字符串，需解码。
        """
        session = await self._ensure_session()
        url = f"{self.api_url}/v1/bot/messages/sync"
        logger.debug(f"[octo] 拉取频道历史: channel={channel_id} type={channel_type} limit={limit}")

        async with session.post(
            url,
            json={
                "channel_id": channel_id,
                "channel_type": channel_type,
                "limit": limit,
                "start_message_seq": 0,
                "end_message_seq": 0,
                "pull_mode": 1,  # 1 = 向上拉（更新消息）
            },
        ) as resp:
            if resp.status != 200:
                logger.warning(f"[octo] 拉取历史消息失败，HTTP {resp.status}")
                return []

            data: dict[str, Any] = await resp.json()
            raw_messages = data.get("messages") if isinstance(data, dict) else []
            if not isinstance(raw_messages, list):
                return []

            # 解码 base64 payload
            messages: list[dict[str, Any]] = []
            for m in raw_messages:
                payload: dict[str, Any] = {}
                raw_payload = m.get("payload")
                if raw_payload:
                    try:
                        if isinstance(raw_payload, str):
                            decoded = base64.b64decode(raw_payload).decode("utf-8")
                            payload = json.loads(decoded)
                        elif isinstance(raw_payload, dict):
                            payload = raw_payload
                    except Exception:
                        logger.debug(f"[octo] payload 解码失败: message_id={m.get('message_id')}")

                messages.append({
                    "from_uid": m.get("from_uid", ""),
                    "message_id": str(m.get("message_id", "")),
                    "message_seq": m.get("message_seq", 0),
                    "type": payload.get("type"),
                    "content": payload.get("content", ""),
                    "timestamp": m.get("timestamp", 0),
                })

            logger.info(f"[octo] 历史消息拉取成功: {len(messages)} 条")
            return messages

    async def close(self) -> None:
        """关闭 HTTP session，释放连接。"""
        if self._session:
            await self._session.close()
            self._session = None