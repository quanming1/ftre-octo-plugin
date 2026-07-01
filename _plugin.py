"""
Octo Channel Plugin — Plugin 入口。

在 ftre Gateway 启动时自动加载，完成以下初始化：
  1. 创建 OctoChannel 实例并注册到 ChannelManager
  2. 注册 BEFORE_AGENT_RUN hook，在每次 Agent 运行前注入 Octo 平台提示

配置来源：~/.ftre/config.json 中 plugins 数组的 config 字段
  {
    "name": "octo_channel",
    "config": {
      "bot_token": "bf_xxx",
      "api_url": "https://im.deepminer.com.cn/api",
      "bridge_port": 9876
    }
  }
"""

import logging
from typing import Any

from ftre.plugin import Plugin, BEFORE_AGENT_RUN

from _channel import OctoChannel
from _constants import CHANNEL_TYPE_GROUP, parse_session_id
from _members import get_cached_members, build_member_list_prefix

logger = logging.getLogger("ftre.plugin.octo_channel")


class OctoChannelPlugin(Plugin):  # type: ignore[misc]
    """Octo Channel Plugin。

    在 ftre Gateway 启动时自动加载，完成以下初始化：
      1. 创建 OctoChannel 实例并注册到 ChannelManager
      2. 注册 BEFORE_AGENT_RUN hook，在每次 Agent 运行前注入 Octo 平台提示
    """

    name = "octo_channel"
    version = "1.0.0"

    def setup(self) -> None:
        """插件初始化：注册 Channel 和 Hook。"""
        config = self.api.config or {}
        logger.info(
            f"[octo] 插件初始化: api_url={config.get('api_url')} "
            f"bot_token={config.get('bot_token', '')[:8]}..."
        )

        channel = OctoChannel(config, self.api.bus, session_manager=self.api.session_manager)
        self.api.register_channel(channel)
        logger.info("[octo] Channel 已注册到 ChannelManager")

        self.api.register_hook(BEFORE_AGENT_RUN, self._on_agent_run)
        logger.info("[octo] before_agent_run Hook 已注册")

    def _on_agent_run(self, ctx: Any) -> Any:
        """BEFORE_AGENT_RUN Hook：注入 Octo 平台提示和群成员信息。

        仅在 channel_id 为 "octo" 时生效。

        注入方式：
          - ctx.messages 是字符串时 → 包装为 list 并插入 system 消息
          - ctx.messages 是列表时 → 追加到已有 system 消息，没有则插入
          - 群聊时额外注入成员列表（从缓存读取）
        """
        if ctx.channel_id != "octo":
            return ctx

        # 解析 session_id 判断是否为群聊
        parsed = parse_session_id(ctx.session_id)
        is_group = parsed and parsed[0] == CHANNEL_TYPE_GROUP

        # 构建 system hint
        hint = (
            "你是 Octo IM 平台上的一个 bot。"
            "你通过频道接收用户消息并回复。"
        )

        # 群聊：注入成员列表
        if is_group and parsed:
            _, group_no = parsed
            members = get_cached_members(group_no)
            if members:
                member_prefix = build_member_list_prefix(members)
                if member_prefix:
                    hint = f"{member_prefix}{hint}"
                    logger.info(f"[octo] Hook: 群成员列表已注入，{len(members)} 人")
            else:
                logger.info("[octo] Hook: 成员缓存未命中，跳过成员列表注入")

        if isinstance(ctx.messages, str):
            logger.info("[octo] Hook: messages 为字符串，包装为 list")
            ctx.messages = [
                {"role": "system", "content": hint},
                {"role": "user", "content": ctx.messages},
            ]
        elif isinstance(ctx.messages, list):
            for msg in ctx.messages:
                if isinstance(msg, dict) and msg.get("role") == "system":
                    if hint not in msg["content"]:
                        msg["content"] = f"{msg['content']}\n\n{hint}"
                    break
            else:
                ctx.messages.insert(0, {"role": "system", "content": hint})
            logger.info(f"[octo] Hook: 已注入 Octo 提示，消息数={len(ctx.messages)}")
        return ctx

    def teardown(self) -> None:
        pass