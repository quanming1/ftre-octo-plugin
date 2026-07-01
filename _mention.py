"""
Octo Channel Plugin — @ 检测与免@ 偏好工具。

检测 bot 是否在群聊消息中被 @，支持三种检测方式：
  1. mention.uids 包含 bot_uid → 被直接 @
  2. mention.ais=1 → @AI / @所有AI（但 @所有人 广播时 ais=1 被抑制）
  3. 文本兜底：消息内容中正则匹配 @bot名称

广播抑制（参考 openclaw-channel-octo inbound.ts:1803-1816）：
  Octo 服务端将 @所有人 重写为 {all:1, ais:1}，
  如果不抑制，bot 会回复每条 @所有人 消息造成群刷屏。
  规则：当 all=1 或 humans=1 时，ais=1 不触发回复。
  纯 {ais:1}（无 all、无 humans）是故意 @AI，正常触发。
  直接 @bot uid 始终触发，不受广播抑制影响。
"""

import logging
import re
from typing import Any

logger = logging.getLogger("ftre.plugin.octo_channel")


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