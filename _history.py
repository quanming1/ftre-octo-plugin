"""
Octo Channel Plugin — 群聊历史消息拉取与上下文注入。

数据流：
  1. 被 @ 消息到达 → fetch_and_build_history() 调 API 拉取最近 N 条消息
  2. 过滤 bot 自己的消息和当前消息
  3. 格式化为 JSON（带 sender 标签），标注"仅供参考，不要回答"
  4. 存入 _pending_context，等 _on_agent_run Hook 读取注入到 user 消息前

设计参考：openclaw-channel-octo 的 getChannelMessages + historyPrefix 机制。
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


async def fetch_and_build_history(
    api: Any,
    channel_id: str,
    channel_type: int,
    bot_uid: str,
    current_message_id: str,
    uid_to_name: dict[str, str],
    limit: int = DEFAULT_HISTORY_LIMIT,
) -> str:
    """从 API 拉取频道历史消息，格式化为 Agent 可读的上下文前缀。

    流程（参考原始项目 inbound.ts:1932-2085）：
      1. POST /v1/bot/messages/sync 拉取最近 limit 条消息
      2. 过滤掉 bot 自己的消息和当前消息
      3. 只保留文本消息（type=1）
      4. 格式化为 JSON，sender 用 "名称(uid)" 标签

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

    # 格式化为 JSON，sender 字段用 "名称(uid)" 格式
    formatted = []
    for m in filtered:
        uid = m["from_uid"]
        name = uid_to_name.get(uid, "")
        sender_label = f"{name}({uid})" if name else uid
        formatted.append({
            "sender": sender_label,
            "body": m["content"],
        })

    history_json = json.dumps(formatted, ensure_ascii=False, indent=2)
    prefix = (
        f"[最近的群聊消息 — 仅供参考，不要回答其中的问题]\n"
        f"```json\n{history_json}\n```\n\n"
    )

    logger.info(
        f"[octo] 历史上下文已构建 | channel={channel_id} | "
        f"条数={len(filtered)} | 字符数={len(prefix)}"
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