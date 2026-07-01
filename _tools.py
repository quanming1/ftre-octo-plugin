"""
Octo Channel Plugin — Agent 管理工具。

注册 octo_management Tool，让 Agent 能主动查询 Octo 群组信息和成员。

MVP 实现的 4 个操作：
  - list-groups:    列出 bot 加入的群
  - group-info:     查看群信息
  - group-members:  查看群成员列表
  - search-members: 按关键词搜索空间成员

设计参考：openclaw-channel-octo 的 createOctoManagementTools (agent-tools.ts:552-716)。
"""

import json
import logging
from typing import Any

from ftre_agent_core.tool import Tool, ToolParameter

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