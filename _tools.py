"""
Octo Channel Plugin — Agent 工具。

两个工具：
  - octo_management: 查询群信息和成员
  - octo_reply: 发送回复消息到当前频道
"""

import json
import logging
import re
from typing import Any

from ftre_agent_core.tool import Tool, ToolParameter, Injected

logger = logging.getLogger("ftre.plugin.octo_channel")


def _make_success(data: Any) -> str:
    """格式化成功结果为 JSON 字符串。"""
    return json.dumps({"success": True, "data": data}, ensure_ascii=False, indent=2)


def _make_error(msg: str) -> str:
    """格式化错误结果为 JSON 字符串。"""
    return json.dumps({"success": False, "error": msg}, ensure_ascii=False)


async def _execute_action(api: Any, action: str, args: dict[str, Any]) -> str:
    """执行单个管理操作，返回 JSON 字符串结果。

    参数：
      api:     OctoBotApi 实例
      action:  操作名称
      args:    工具调用参数
    """
    try:
        if action == "list-groups":
            groups = await api.list_groups()
            return _make_success({
                "count": len(groups),
                "groups": [
                    {"group_no": g.get("group_no", ""), "name": g.get("name", "")}
                    for g in groups
                ],
            })

        elif action == "group-info":
            group_id = args.get("groupId", "")
            if not group_id:
                return _make_error("groupId 是必填参数")
            info = await api.get_group_info(group_id)
            return _make_success(info)

        elif action == "group-members":
            group_id = args.get("groupId", "")
            if not group_id:
                return _make_error("groupId 是必填参数")
            members = await api.get_group_members(group_id)
            return _make_success({
                "count": len(members),
                "members": [
                    {
                        "uid": m.get("uid", ""),
                        "name": m.get("name", ""),
                        "role": m.get("role", ""),
                        "robot": m.get("robot", 0),
                    }
                    for m in members
                ],
            })

        elif action == "search-members":
            keyword = args.get("keyword", "")
            members = await api.search_space_members(keyword=keyword)
            return _make_success({
                "count": len(members),
                "members": [
                    {
                        "uid": m.get("uid", ""),
                        "name": m.get("name", ""),
                        "robot": m.get("robot", 0),
                    }
                    for m in members
                ],
            })

        else:
            return _make_error(f"未知操作: {action}。支持的操作: list-groups, group-info, group-members, search-members")

    except Exception as e:
        logger.exception(f"[octo] 管理工具执行失败: action={action}")
        return _make_error(str(e))


def create_octo_management_tool(api: Any) -> Tool:
    """创建 Octo 管理工具。

    参数：
      api: OctoBotApi 实例（已配置好 api_url 和 bot_token）

    返回一个 Tool 对象，注册到 ftre 的 tool_registry 后 Agent 即可调用。
    """
    async def _run(action: str, groupId: str = "", keyword: str = "") -> str:
        """Octo 群组管理工具。

        可执行的操作：
        - list-groups: 列出 bot 加入的所有群（无需参数）
        - group-info: 查看指定群的信息（需 groupId）
        - group-members: 查看指定群的成员列表（需 groupId）
        - search-members: 按关键词搜索空间成员（可选 keyword）
        """
        args: dict[str, Any] = {"groupId": groupId, "keyword": keyword}
        return await _execute_action(api, action, args)

    return Tool(
        name="octo_management",
        description=(
            "管理 Octo 群组：列出群、查看群信息、查看群成员、搜索成员。"
            "action 可选值：list-groups（列出 bot 加入的群）、"
            "group-info（查看群信息，需 groupId）、"
            "group-members（查看群成员，需 groupId）、"
            "search-members（搜索空间成员，可选 keyword）。"
        ),
        parameters=[
            ToolParameter(
                name="action",
                type="string",
                description="要执行的操作",
                required=True,
                enum=["list-groups", "group-info", "group-members", "search-members"],
            ),
            ToolParameter(
                name="groupId",
                type="string",
                description="群 ID (group_no)。group-info 和 group-members 必填，其他操作不需要",
                required=False,
            ),
            ToolParameter(
                name="keyword",
                type="string",
                description="搜索关键词。仅 search-members 使用，模糊匹配用户名",
                required=False,
            ),
        ],
        func=_run,
    )


def create_octo_reply_tool(
    api: Any,
    bot_id: str,
    session_bots: dict[str, str],
    session_manager: Any = None,
) -> Tool:
    """创建 Octo 回复工具。

    参数：
      api:             OctoBotApi 实例（已绑定当前 bot 的 token）
      bot_id:          当前 bot 的 ID
      session_bots:    session_id → bot_id 映射（OctoChannel._session_bots）
      session_manager: SessionManager（用于从 external_session 取 channel 信息）
    """

    async def _reply(
        content: str,
        session_id: str = Injected("session_id"),
    ) -> str:
        """发送回复消息到当前 Octo 频道。

        这是唯一的回复方式——普通文本输出不会自动发送给用户。
        支持在 content 中使用 @[uid:名称] 格式 @ 群成员。
        """
        if not content or not content.strip():
            return _make_error("content 不能为空")

        # 从 session_id 解析 channel_type 和 channel_id
        from _api import parse_session_id
        from _channel import take_pending_inbound_seq, record_bot_reply

        parsed = parse_session_id(session_id)
        if parsed is None and session_manager is not None:
            external = await session_manager.get_external_session(session_id)
            if external:
                data = external.get("external_data") or {}
                try:
                    parsed = (int(data["channel_type"]), str(data["channel_id"]), str(data.get("bot_id", "")))
                except (KeyError, TypeError, ValueError):
                    parsed = None
        if parsed is None:
            return _make_error(f"无法解析 session_id: {session_id}")

        channel_type, channel_id, _ = parsed

        # 解析 @[uid:name] → @name + mention_uids
        mention_uids: list[str] = []

        def _replace_mention(m: re.Match) -> str:
            uid = m.group(1)
            name = m.group(2)
            if uid not in mention_uids:
                mention_uids.append(uid)
            return f"@{name}"

        clean_content = re.sub(r"@\[([a-f0-9]{32}):([^\]]+)\]", _replace_mention, content)

        try:
            result = await api.send_message(
                channel_id=channel_id,
                channel_type=channel_type,
                content=clean_content,
                mention_uids=mention_uids if mention_uids else None,
            )
            logger.info(f"[octo] 回复发送成功: message_id={result.get('message_id')}")

            # 记录入站消息 seq，用于下次历史分段
            inbound_seq = take_pending_inbound_seq(session_id)
            if inbound_seq:
                record_bot_reply(channel_id, inbound_seq, bot_id)

            return _make_success({"message_id": result.get("message_id")})
        except Exception as e:
            logger.exception("[octo] 回复发送失败")
            return _make_error(str(e))

    return Tool(
        name="octo_reply",
        description=(
            "发送回复消息到当前 Octo 频道。"
            "这是你唯一的回复方式——普通文本输出不会自动发送给用户。"
            "在 content 中可以使用 @[uid:名称] 格式来 @ 群成员。"
            "可以多次调用来发送多条消息。"
        ),
        parameters=[
            ToolParameter(
                name="content",
                type="string",
                description="要发送的消息内容。支持 @[uid:名称] 格式 @ 群成员。",
                required=True,
            ),
        ],
        func=_reply,
    )