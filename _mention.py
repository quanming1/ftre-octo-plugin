"""
Octo Channel Plugin — @ 检测 + 群成员缓存与格式化。

@ 检测（check_mentioned）：
  1. mention.uids 包含 bot_uid → 被直接 @
  2. mention.ais=1 → @AI / @所有AI（但 @所有人 广播时 ais=1 被抑制）
  3. 文本兜底：消息内容中正则匹配 @bot名称

广播抑制（参考 openclaw-channel-octo inbound.ts:1803-1816）：
  Octo 服务端将 @所有人 重写为 {all:1, ais:1}，
  如果不抑制，bot 会回复每条 @所有人 消息造成群刷屏。
  规则：当 all=1 或 humans=1 时，ais=1 不触发回复。
  纯 {ais:1}（无 all、无 humans）是故意 @AI，正常触发。
  直接 @bot uid 始终触发，不受广播抑制影响。

群成员缓存（5 分钟 TTL），避免每条消息都调 API。
"""

import logging
import re
import time
from typing import Any

logger = logging.getLogger("ftre.plugin.octo_channel")

# ─── @ 检测 ────────────────────────────────────────────────────────────


def check_mentioned(
    payload: dict[str, Any],
    content: str,
    bot_uid: str,
    bot_name: str,
) -> bool:
    """检测 bot 是否在消息中被 @。

    参数：
      payload:   WuKongIM 消息 payload（含 mention 字段）
      content:   消息文本内容
      bot_uid:   bot 自己的 UID
      bot_name:  bot 的名称（用于文本兜底）

    返回 True 表示 bot 被提及，应回复。
    """
    mention = payload.get("mention") or {}

    # 1. 直接 @bot — 始终触发，不受广播抑制影响
    uids = mention.get("uids") or []
    if bot_uid and bot_uid in uids:
        logger.debug(f"[octo] 被直接 @: bot_uid={bot_uid}")
        return True

    # 2. @AI / @所有AI — 但需要广播抑制
    ais = mention.get("ais")
    is_ais = ais is True or ais == 1

    # 广播抑制：@所有人 (all=1) 或 @所有人(humans=1) 时，
    # 服务端会同时设 ais=1，但这是广播不是 @AI，不应触发 bot 回复
    all_raw = mention.get("all")
    is_broadcast = all_raw is True or all_raw == 1
    # humans=1 也是 @所有人（Plan X），同样抑制
    humans_raw = mention.get("humans")
    is_humans = humans_raw is True or humans_raw == 1

    if is_ais and not (is_broadcast or is_humans):
        logger.debug("[octo] 被 @AI 提及")
        return True

    if is_ais and (is_broadcast or is_humans):
        logger.info("[octo] @所有人 广播，抑制 @AI 触发")
        # 广播时不因 ais 触发，但仍可能因文本兜底或直接 @uid 触发
        # 不 return False，继续检查文本兜底

    # 3. 文本兜底：检查内容中是否包含 @bot名称
    #    注意：mention payload 通常由 Octo 服务端填充，这里作为兜底
    if content and bot_name:
        escaped = re.escape(bot_name)
        pattern = re.compile(rf"(?:^|\s)@{escaped}(?:\s|$)")
        if pattern.search(content):
            logger.debug(f"[octo] 文本兜底检测到 @{bot_name}")
            return True

    return False


# ─── 群成员缓存与格式化 ────────────────────────────────────────────────

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
            f"{member_text}\n\n"
            f"群聊中 @ 某人时，必须用 @[uid:displayName] 格式，其中 uid 是成员的 32 位十六进制 ID。\n"
            f"方括号和冒号缺一不可，uid 和 displayName 之间只有一个冒号。\n"
            f"不要用 bot_id 或用户名（如 somebody_bot），不要写裸 uid 不加方括号。\n"
            f"示例：@[{members[0].get('uid', 'uid')}:{members[0].get('name', 'name')}]\n"
            f"必须从上方成员列表中复制 uid，不要编造。"
        )

    return (
        f"共有 {len(members)} 名成员（人数较多，未全部列出）。\n"
        f"如需 @ 某人，先用管理工具查询其 uid 和名称，再用 @[uid:displayName] 格式 @。"
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