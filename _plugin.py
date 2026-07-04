"""
Octo Channel Plugin — Plugin 入口。

在 ftre Gateway 启动时自动加载，完成以下初始化：
  1. 创建 OctoChannel 实例并注册到 ChannelManager
  2. 注册 BEFORE_AGENT_RUN hook，在每次 Agent 运行前注入 Octo 平台提示

配置来源：~/.ftre/config.json 中 plugins 数组的 config 字段
  {
    "name": "octo_channel",
    "config": {
      "api_url": "https://im.deepminer.com.cn/api",
      "bridge_port": 9876,
      "bots": [
        { "bot_token": "bf_xxx", "agent_id": "octo", "bot_name": "Octo" }
      ]
    }
  }
"""

import logging
from typing import Any

from ftre.plugin import Plugin, BEFORE_AGENT_RUN

from _channel import OctoChannel
from _tools import create_octo_management_tool

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
        self._channel: OctoChannel
        config = self.api.config or {}
        bots = config.get("bots", [])
        logger.info(
            f"[octo] 插件初始化: api_url={config.get('api_url')} "
            f"bots={len(bots)}"
        )

        channel = OctoChannel(config, self.api.bus, session_manager=self.api.session_manager)
        self.api.register_channel(channel)
        self._channel = channel
        logger.info("[octo] Channel 已注册到 ChannelManager")

        self.api.register_hook(BEFORE_AGENT_RUN, self._on_agent_run)
        logger.info("[octo] before_agent_run Hook 已注册")

    def _on_agent_run(self, ctx: Any) -> Any:
        """BEFORE_AGENT_RUN Hook：注入 Octo 平台身份提示 + 注册私有工具。

        成员列表和历史消息已在 _handle_message 中拼接到 content 前缀，
        随用户消息持久化到 session DB，这里不再处理 user 消息。
        """
        if ctx.channel_id != "octo":
            return ctx

        # 注册 Octo 管理工具为当前 agent 的私有工具
        if self._channel._bots and "octo_management" not in ctx.agent_tool_registry.names:
            first_api = next(iter(self._channel._bots.values()))["api"]
            ctx.agent_tool_registry.register(create_octo_management_tool(first_api))

        # system prompt: bot 身份提示
        system_hint = (
            "<OCTO_IDENTITY desc=\"你是 Octo IM 平台上的 bot，以下是你的身份信息\">\n"
            "你是 Octo IM 平台上的一个 bot。"
            "你通过频道接收用户消息并回复。"
            "\n</OCTO_IDENTITY>"
        )

        if isinstance(ctx.messages, list):
            for msg in ctx.messages:
                if isinstance(msg, dict) and msg.get("role") == "system":
                    if system_hint not in msg["content"]:
                        msg["content"] = f"{system_hint}\n\n{msg['content']}"
                    break
            else:
                ctx.messages.insert(0, {"role": "system", "content": system_hint})

            logger.info(f"[octo] Hook: 已注入 Octo 身份提示，消息数={len(ctx.messages)}")
        return ctx

    def teardown(self) -> None:
        pass