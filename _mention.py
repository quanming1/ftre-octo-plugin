"""
Octo Channel Plugin — @ 检测与免@ 偏好工具。

检测 bot 是否在群聊消息中被 @，支持三种检测方式：
  1. mention.uids 包含 bot_uid → 被直接 @
  2. mention.ais=1 → @AI / @所有AI
  3. 文本兜底：消息内容中正则匹配 @bot名称
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

    # 1. 直接 @bot
    uids = mention.get("uids") or []
    if bot_uid and bot_uid in uids:
        logger.debug(f"[octo] 被直接 @: bot_uid={bot_uid}")
        return True

    # 2. @AI / @所有AI
    ais = mention.get("ais")
    if ais is True or ais == 1:
        logger.debug("[octo] 被 @AI 提及")
        return True

    # 3. 文本兜底：检查内容中是否包含 @bot名称
    #    注意：mention payload 通常由 Octo 服务端填充，这里作为兜底
    if content and bot_name:
        escaped = re.escape(bot_name)
        pattern = re.compile(rf"(?:^|\s)@{escaped}(?:\s|$)")
        if pattern.search(content):
            logger.debug(f"[octo] 文本兜底检测到 @{bot_name}")
            return True

    return False