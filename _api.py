"""
Octo Channel Plugin — Octo Bot API HTTP 客户端。

封装 Octo 平台的 REST API 调用，使用 bot_token 认证。
主要接口：
  - POST /v1/bot/register   注册 bot，获取 robot_id / im_token / ws_url
  - POST /v1/bot/sendMessage 发送文本消息（注意：sendMessage 是驼峰命名）
"""

import logging
from typing import Any

import aiohttp

logger = logging.getLogger("ftre.plugin.octo_channel")


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

    async def close(self) -> None:
        """关闭 HTTP session，释放连接。"""
        if self._session:
            await self._session.close()
            self._session = None