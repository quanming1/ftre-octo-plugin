"""
Octo Channel Plugin — Agent 管理工具。

注册 octo_management Tool，让 Agent 能主动查询 Octo 群组信息、成员和历史消息。

支持的 action：
  - list-groups:     列出 bot 加入的群
  - group-info:      查看群信息
  - group-members:   查看群成员列表
  - search-members:  按关键词搜索空间成员
  - fetch-history:   按需拉取当前频道的更多历史消息（支持分页）
"""

import json
import logging
from typing import Any

from ftre_agent_core.tool import Tool, ToolParameter, Injected

logger = logging.getLogger("ftre.plugin.octo_channel")


def _make_success(data: Any) -> str:
    """格式化成功结果为 JSON 字符串。"""
    return json.dumps({"success": True, "data": data}, ensure_ascii=False, indent=2)


def _make_error(msg: str) -> str:
    """格式化错误结果为 JSON 字符串。"""
    return json.dumps({"success": False, "error": msg}, ensure_ascii=False)


async def _execute_action(
    api: Any,
    action: str,
    args: dict[str, Any],
    channel: Any = None,
) -> str:
    """执行单个管理操作，返回 JSON 字符串结果。"""
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

        elif action == "fetch-history":
            if channel is None:
                return _make_error("channel 实例不可用，无法拉取历史")

            session_id = args.get("_session_id", "")
            session_manager = args.get("_session_manager")
            limit = max(1, min(int(args.get("limit", 50)), 200))
            before_seq = int(args.get("beforeSeq", 0))

            if not session_id or not session_manager:
                return _make_error("session 信息不可用")

            # 从 external_session 获取频道信息
            external = await session_manager.get_external_session(session_id)
            if not external:
                return _make_error("当前 session 不是 Octo 频道会话")

            ext_data = external.get("external_data") or {}
            ch_id = str(ext_data.get("channel_id", ""))
            ch_type = int(ext_data.get("channel_type", 0))
            bot_id = str(ext_data.get("bot_id", ""))

            if not ch_id or not ch_type:
                return _make_error("无法从 session 元数据中获取频道信息")

            # 找到对应的 bot api
            bot_info = channel._bots.get(bot_id) if bot_id else None
            if not bot_info and channel._bots:
                bot_info = next(iter(channel._bots.values()))
            if not bot_info:
                return _make_error("找不到对应的 bot 连接")

            bot_api = bot_info["api"]
            bot_uid = bot_info["bot_uid"]

            # 拉取历史
            messages = await bot_api.get_channel_messages(
                channel_id=ch_id,
                channel_type=ch_type,
                limit=limit,
            )
            if not messages:
                return _make_success({"count": 0, "messages": [], "has_more": False})

            # 过滤 bot 自己的消息
            filtered = [m for m in messages if m.get("from_uid") != bot_uid]

            # before_seq 向前翻页
            if before_seq > 0:
                filtered = [m for m in filtered if m.get("message_seq", 0) < before_seq]

            # 按 seq 降序（最新在前）
            filtered.sort(key=lambda m: m.get("message_seq", 0), reverse=True)

            # 只保留文本消息
            filtered = [m for m in filtered if m.get("type") == 1 and m.get("content")]

            # 构建 uid → name 映射
            uid_to_name: dict[str, str] = {}
            try:
                from _mention import get_cached_members, extract_parent_group_no
                from _channel import build_uid_to_name_map
                if ch_type in (2, 5):
                    parent_no = extract_parent_group_no(ch_id)
                    members = get_cached_members(parent_no)
                    if members:
                        uid_to_name = build_uid_to_name_map(members)
            except Exception:
                pass

            formatted = []
            for m in filtered:
                uid = m.get("from_uid", "")
                name = uid_to_name.get(uid, "")
                sender = f"{name}({uid})" if name else uid
                formatted.append({
                    "seq": m.get("message_seq", 0),
                    "sender": sender,
                    "content": m.get("content", ""),
                    "timestamp": m.get("timestamp", 0),
                })

            min_seq = min((m["seq"] for m in formatted), default=0)
            has_more = len(filtered) >= limit

            result = {
                "count": len(formatted),
                "messages": formatted,
                "oldest_seq": min_seq,
                "has_more": has_more,
                "hint": (
                    f"如需继续向前翻页，传入 beforeSeq={min_seq}"
                    if has_more else "已无更多历史消息"
                ),
            }

            logger.info(
                f"[octo] Agent 按需拉取历史: channel={ch_id} limit={limit} "
                f"before_seq={before_seq} → {len(formatted)} 条, has_more={has_more}"
            )
            return _make_success(result)

        else:
            return _make_error(
                f"未知操作: {action}。支持的操作: "
                f"list-groups, group-info, group-members, search-members, fetch-history"
            )

    except Exception as e:
        logger.exception(f"[octo] 管理工具执行失败: action={action}")
        return _make_error(str(e))


def create_octo_management_tool(api: Any, channel: Any = None) -> Tool:
    """创建 Octo 管理工具。

    参数：
      api:      OctoBotApi 实例（已配置好 api_url 和 bot_token）
      channel:  OctoChannel 实例（fetch-history 需要；其他 action 不需要）

    返回一个 Tool 对象，注册到 ftre 的 tool_registry 后 Agent 即可调用。
    """
    async def _run(
        action: str,
        groupId: str = "",
        keyword: str = "",
        limit: int = 50,
        beforeSeq: int = 0,
        session_id: str = Injected("session_id"),
        session_manager=Injected("session_manager"),
    ) -> str:
        """Octo 平台管理工具。

        可执行的操作：
        - list-groups: 列出 bot 加入的所有群（无需参数）
        - group-info: 查看指定群的信息（需 groupId）
        - group-members: 查看指定群的成员列表（需 groupId）
        - search-members: 搜索空间成员（可选 keyword 模糊匹配用户名）
        - fetch-history: 拉取当前频道的更多历史消息（可选 limit 默认 50、beforeSeq 分页）
        """
        args: dict[str, Any] = {
            "groupId": groupId,
            "keyword": keyword,
            "limit": limit,
            "beforeSeq": beforeSeq,
            "_session_id": session_id,
            "_session_manager": session_manager,
        }
        return await _execute_action(api, action, args, channel=channel)

    return Tool(
        name="octo_management",
        description=(
            "管理 Octo 平台：查询群组信息、成员、以及拉取频道历史消息。\n"
            "action 可选值：\n"
            "- list-groups：列出 bot 加入的所有群\n"
            "- group-info：查看群信息（需 groupId）\n"
            "- group-members：查看群成员列表（需 groupId）\n"
            "- search-members：搜索空间成员（可选 keyword）\n"
            "- fetch-history：拉取当前频道的更多历史聊天记录。"
            "当已有上下文中的历史消息不够、需要查看更多之前的对话时使用。"
            "默认拉取 50 条最新消息；传入 beforeSeq（上次结果中的 oldest_seq）可继续向前翻页。"
            "返回的消息按时间倒序排列（最新在前），每条包含 seq、sender、content、timestamp。"
        ),
        parameters=[
            ToolParameter(
                name="action",
                type="string",
                description="要执行的操作",
                required=True,
                enum=["list-groups", "group-info", "group-members", "search-members", "fetch-history"],
            ),
            ToolParameter(
                name="groupId",
                type="string",
                description="群 ID (group_no)。group-info 和 group-members 必填",
                required=False,
            ),
            ToolParameter(
                name="keyword",
                type="string",
                description="搜索关键词。仅 search-members 使用",
                required=False,
            ),
            ToolParameter(
                name="limit",
                type="number",
                description="拉取条数，仅 fetch-history 使用，默认 50，最大 200",
                required=False,
            ),
            ToolParameter(
                name="beforeSeq",
                type="number",
                description="分页游标，仅 fetch-history 使用。传 0 从最新开始拉；传上次结果的 oldest_seq 继续向前翻页",
                required=False,
            ),
        ],
        func=_run,
    )
