"""
Octo Channel Plugin — 群成员缓存与格式化。

提供：
  - 成员缓存（5 分钟 TTL），避免每条消息都调 API
  - build_member_list_prefix()：将成员列表格式化为 Agent 可读的上下文
  - 供 _channel.py 刷新缓存，供 _plugin.py 注入 system prompt
"""

import logging
import time
from typing import Any

logger = logging.getLogger("ftre.plugin.octo_channel")

# 成员缓存：{group_no: (members, expiry_timestamp)}
# TTL 5 分钟，缓存失效后下一条消息触发刷新
_CACHE_TTL_SEC = 5 * 60
_member_cache: dict[str, tuple[list[dict[str, Any]], float]] = {}


def get_cached_members(group_no: str) -> list[dict[str, Any]] | None:
    """获取缓存的群成员列表，若缓存过期或不存在返回 None。"""
    entry = _member_cache.get(group_no)
    if entry is None:
        return None
    members, expiry = entry
    if time.time() > expiry:
        logger.debug(f"[octo] 成员缓存过期: group={group_no}")
        return None
    return members


def set_cached_members(group_no: str, members: list[dict[str, Any]]) -> None:
    """更新成员缓存。"""
    _member_cache[group_no] = (members, time.time() + _CACHE_TTL_SEC)
    logger.info(f"[octo] 成员缓存已更新: group={group_no} 成员数={len(members)}")


def build_member_list_prefix(members: list[dict[str, Any]]) -> str:
    """将群成员列表格式化为 Agent 上下文前缀。

    - ≤10 人：列出所有成员（名称 + UID）
    - >10 人：提示人数太多，告知 Agent 可以调用工具查询

    返回空字符串表示无成员数据（不注入）。
    """
    if not members:
        return ""

    if len(members) <= 10:
        lines = [f"  {m.get('name', '?')} (uid={m.get('uid', '?')})" for m in members]
        member_text = "\n".join(lines)
        return (
            f"[本群成员]\n"
            f"{member_text}\n"
            f"\n"
            f"回复中提到某人时，可以用 @[uid:名称] 格式来 @ 对方。\n"
            f"例如：@[{members[0].get('uid', 'uid')}:{members[0].get('name', 'name')}]。\n"
            f"注意：不要编造 uid，必须从上方的成员列表中复制。\n\n"
        )

    return (
        f"[本群信息] 共有 {len(members)} 名成员（人数较多，未全部列出）。\n"
        f"如需 @ 某人，请从上方成员列表中查找其 uid，或用管理工具查询。\n\n"
    )


def build_uid_to_name_map(members: list[dict[str, Any]]) -> dict[str, str]:
    """从成员列表构建 uid → name 映射表。

    用于历史消息中的发送者标签和当前消息的发送者标注。
    """
    result: dict[str, str] = {}
    for m in members:
        uid = m.get("uid", "")
        name = m.get("name", "")
        if uid and name:
            result[uid] = name
    return result