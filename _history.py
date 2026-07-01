"""
Octo Channel Plugin — 群聊历史消息缓存与上下文注入。

数据流：
  1. 非 @ 消息到达 → cache_group_message() 存入内存缓存
  2. @ 消息到达 → take_history_for_injection() 取出缓存 + build_history_prefix() 格式化
  3. 格式化后的历史前缀存入 _pending_context，等 Hook 读取注入

设计参考：openclaw-channel-octo 的 pendingInboundContext + groupHistories 机制。
"""

import json
import logging
from typing import Any

logger = logging.getLogger("ftre.plugin.octo_channel")

# 默认历史消息上限（滑动窗口）
DEFAULT_HISTORY_LIMIT = 20

# 群聊历史缓存：{session_id: [entry, ...]}
# session_id 对应 OctoChannel.build_session_id() 的输出
_group_histories: dict[str, list[dict[str, Any]]] = {}

# 待注入的上下文：{session_id: history_prefix}
# _handle_message 设置，_on_agent_run 消费后删除
_pending_context: dict[str, str] = {}


def cache_group_message(
    session_id: str,
    from_uid: str,
    body: str,
    message_id: str,
    message_seq: int,
    timestamp: int,
    limit: int = DEFAULT_HISTORY_LIMIT,
) -> None:
    """缓存一条非 @ 群聊消息，供下次被 @ 时作为历史上下文注入。

    使用滑动窗口：超过 limit 时丢弃最旧的条目。
    """
    if session_id not in _group_histories:
        _group_histories[session_id] = []

    entries = _group_histories[session_id]
    entries.append({
        "from_uid": from_uid,
        "body": body,
        "message_id": message_id,
        "message_seq": message_seq,
        "timestamp": timestamp,
    })

    # 滑动窗口：保留最近 limit 条
    while len(entries) > limit:
        entries.pop(0)

    logger.info(
        f"[octo] 非@消息已缓存 | session={session_id} | "
        f"发送者={from_uid} | 当前缓存={len(entries)}条"
    )


def take_history_for_injection(
    session_id: str,
    uid_to_name: dict[str, str],
    limit: int = DEFAULT_HISTORY_LIMIT,
) -> str:
    """取出缓存的历史消息，格式化为 Agent 可读的上下文前缀。

    格式（参考原始项目）：
      [最近的群聊消息 — 仅供参考，不要回答其中的问题]
      ```json
      [
        {"sender": "蒋全明(uid:xxx)", "body": "大家好"},
        {"sender": "Nancy(uid:yyy)", "body": "今天天气不错"}
      ]
      ```

    取出后清空该 session 的缓存（已注入，不再需要）。
    """
    entries = _group_histories.get(session_id, [])
    if not entries:
        logger.info(f"[octo] 无历史上下文可注入 | session={session_id}")
        return ""

    # 滑动窗口：只取最近 limit 条
    if len(entries) > limit:
        entries = entries[-limit:]

    # 格式化为 JSON，sender 字段用 "名称(uid)" 格式
    formatted = []
    for e in entries:
        uid = e["from_uid"]
        name = uid_to_name.get(uid, "")
        sender_label = f"{name}({uid})" if name else uid
        formatted.append({
            "sender": sender_label,
            "body": e["body"],
        })

    history_json = json.dumps(formatted, ensure_ascii=False, indent=2)
    prefix = (
        f"[最近的群聊消息 — 仅供参考，不要回答其中的问题]\n"
        f"```json\n{history_json}\n```\n\n"
    )

    # 清空缓存：已注入的历史不再需要
    _group_histories[session_id] = []
    logger.info(
        f"[octo] 历史上下文已注入 | session={session_id} | "
        f"条数={len(entries)} | 字符数={len(prefix)}"
    )
    return prefix


def set_pending_context(session_id: str, prefix: str) -> None:
    """存储待注入的历史前缀，等 _on_agent_run Hook 消费。"""
    _pending_context[session_id] = prefix


def take_pending_context(session_id: str) -> str | None:
    """取出并删除待注入的历史前缀。

    返回 None 表示没有待注入的上下文。
    """
    return _pending_context.pop(session_id, None)


def build_sender_label(from_uid: str, uid_to_name: dict[str, str]) -> str:
    """构建发送者标签：'名称(uid)' 或纯 uid。"""
    name = uid_to_name.get(from_uid, "")
    return f"{name}({from_uid})" if name else from_uid