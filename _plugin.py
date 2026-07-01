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

from ftre.plugin import Plugin, BEFORE_AGENT_RUN

from _channel import OctoChannel

logger = logging.getLogger("ftre.plugin.octo_channel")


class OctoChannelPlugin(Plugin):
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

    def _on_agent_run(self, ctx):
        """BEFORE_AGENT_RUN Hook：在 Agent 每次运行前注入 Octo 平台提示。

        仅在 channel_id 为 "octo" 时生效，避免影响其他 channel 的会话。

        注入方式：
          - 如果 ctx.messages 是字符串（单条用户消息），包装为 list 并插入 system 消息
          - 如果 ctx.messages 是列表，追加到已有的 system 消息中，没有则插入一条新的
        """
        if ctx.channel_id != "octo":
            return ctx

        hint = (
            "你是 Octo IM 平台上的一个 bot。"
            "你通过频道接收用户消息并回复。"
        )

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