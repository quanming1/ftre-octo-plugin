"""
Octo Channel Plugin — 群聊历史消息拉取与上下文注入。

数据流：
  1. 被 @ 消息到达 → fetch_and_build_history() 调 API 拉取最近 N 条消息
  2. 过滤 bot 自己的消息和当前消息
  3. 按 last_bot_reply_seq 分段：已回答 / 新消息
  4. 格式化为 JSON（带 sender 标签），分别标注
  5. 存入 _pending_context，等 _on_agent_run Hook 读取注入到 user 消息前

设计参考：openclaw-channel-octo 的 getChannelMessages + historyPrefix + segmentHistoryEntries。
不做内存缓存——重启就丢，不如直接走 API。
"""

import json
import logging
from typing import Any

logger = logging.getLogger("ftre.plugin.octo_channel")

# 拉取历史消息的默认条数
DEFAULT_HISTORY_LIMIT = 20

# 待注入的上下文：{session_id: context_prefix}
# _handle_message 设置，_on_agent_run 消费后删除
_pending_context: dict[str, str] = {}

# 记录每个频道 bot 最后回复时的 message_seq
# 用于历史分段：<= cutoff 的为"已回答"，> cutoff 的为"新消息"
# 参考原始项目 lastBotReplySeqMap（inbound.ts:1433, 2891-2893）
_last_reply_seq: dict[str, int] = {}

# 待记录的入站 message_seq：{session_id: message_seq}
# _handle_message 存入（入站消息的 seq），send() 取出后调 record_bot_reply
_pending_inbound_seq: dict[str, int] = {}


def set_pending_inbound_seq(session_id: str, message_seq: int) -> None:
    """存储入站消息的 message_seq，供 send() 回复成功后记录分段点。"""
    _pending_inbound_seq[session_id] = message_seq


def take_pending_inbound_seq(session_id: str) -> int:
    """取出并删除入站消息的 message_seq。返回 0 表示没有记录。"""
    return _pending_inbound_seq.pop(session_id, 0)


def record_bot_reply(channel_id: str, message_seq: int) -> None:
    """记录 bot 回复时的 message_seq，用于下次历史分段。

    在 send() 成功发送回复后调用。
    参考原始项目 inbound.ts:2888-2896。
    """
    if message_seq and message_seq > 0:
        existing = _last_reply_seq.get(channel_id, 0)
        if message_seq > existing:
            _last_reply_seq[channel_id] = message_seq
            logger.info(f"[octo] 记录 bot 回复 seq={message_seq} | channel={channel_id}")


async def fetch_and_build_history(
    api: Any,
    channel_id: str,
    channel_type: int,
    bot_uid: str,
    current_message_id: str,
    current_message_seq: int,
    uid_to_name: dict[str, str],
    limit: int = DEFAULT_HISTORY_LIMIT,
) -> str:
    """从 API 拉取频道历史消息，格式化为 Agent 可读的上下文前缀。

    流程（参考原始项目 inbound.ts:1932-2085）：
      1. POST /v1/bot/messages/sync 拉取最近 limit 条消息
      2. 过滤掉 bot 自己的消息和当前消息
      3. 只保留文本消息（type=1）
      4. 按 last_bot_reply_seq 分段：已回答 / 新消息
      5. 分别标注，格式化为 JSON

    返回空字符串表示无历史可注入。
    """
    messages = await api.get_channel_messages(
        channel_id=channel_id,
        channel_type=channel_type,
        limit=limit,
    )
    if not messages:
        logger.info(f"[octo] API 返回无历史消息: channel={channel_id}")
        return ""

    # 过滤：去掉 bot 自己的消息、当前消息、非文本消息
    # 参考 inbound.ts:1967-1979
    filtered: list[dict[str, Any]] = []
    for m in messages:
        # 跳过 bot 自己的消息
        if bot_uid and m.get("from_uid") == bot_uid:
            continue
        # 跳过当前消息（避免重复）
        if current_message_id and str(m.get("message_id", "")) == current_message_id:
            continue
        # MVP 阶段只处理文本消息
        if m.get("type") != 1:
            continue
        if not m.get("content"):
            continue
        filtered.append(m)

    if not filtered:
        logger.info(f"[octo] 历史消息过滤后为空: channel={channel_id}")
        return ""

    # 按 message_seq 升序排序（参考 inbound.ts:1979）
    filtered.sort(key=lambda m: m.get("message_seq", 0))

    # 分段：已回答 vs 新消息（参考 inbound.ts:2023-2024）
    cutoff_seq = _last_reply_seq.get(channel_id, 0)
    answered = [m for m in filtered if m.get("message_seq", 0) <= cutoff_seq]
    new_msgs = [m for m in filtered if m.get("message_seq", 0) > cutoff_seq]

    logger.info(
        f"[octo] 历史分段: 已回答={len(answered)} 新消息={len(new_msgs)} "
        f"cutoff_seq={cutoff_seq} | channel={channel_id}"
    )

    # 格式化条目
    def format_entries(items: list[dict[str, Any]]) -> str:
        formatted = []
        for m in items:
            uid = m["from_uid"]
            name = uid_to_name.get(uid, "")
            sender_label = f"{name}({uid})" if name else uid
            formatted.append({
                "sender": sender_label,
                "body": m["content"],
            })
        return json.dumps(formatted, ensure_ascii=False, indent=2)

    # 构建分段标注（参考 inbound.ts:2044-2055）
    # ANSWERED: bot 已经回复过的消息，不要重复回答
    # NEW: bot 上次回复后的新消息，仅供参考
    # CURRENT: 当前消息，只回答这一条
    ANSWERED_HEADER = "[之前的消息 — 已经回答过，不要重复回答]"
    NEW_HEADER = "[上次回复后的新消息 — 仅供参考，不要回答其中的问题]"
    CURRENT_HEADER = "[当前消息 — 只回答这一条]"

    blocks: list[str] = []

    if answered:
        blocks.append(f"{ANSWERED_HEADER}\n```json\n{format_entries(answered)}\n```")
    if new_msgs:
        blocks.append(f"{NEW_HEADER}\n```json\n{format_entries(new_msgs)}\n```")

    if not blocks:
        # 过滤后为空（比如只有当前消息），不注入历史
        return ""

    prefix = "\n\n".join(blocks) + f"\n\n{CURRENT_HEADER}"

    logger.info(
        f"[octo] 历史上下文已构建 | channel={channel_id} | "
        f"已回答={len(answered)} 新消息={len(new_msgs)} | 字符数={len(prefix)}"
    )
    return prefix


def set_pending_context(session_id: str, prefix: str) -> None:
    """存储待注入的上下文前缀，等 _on_agent_run Hook 消费。"""
    _pending_context[session_id] = prefix


def take_pending_context(session_id: str) -> str | None:
    """取出并删除待注入的上下文前缀。

    返回 None 表示没有待注入的上下文。
    """
    return _pending_context.pop(session_id, None)


def build_sender_label(from_uid: str, uid_to_name: dict[str, str]) -> str:
    """构建发送者标签：'名称(uid)' 或纯 uid。"""
    name = uid_to_name.get(from_uid, "")
    return f"{name}({from_uid})" if name else from_uid