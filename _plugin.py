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
from _history import take_pending_context
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
        config = self.api.config or {}
        logger.info(
            f"[octo] 插件初始化: api_url={config.get('api_url')} "
            f"bot_token={config.get('bot_token', '')[:8]}..."
        )

        channel = OctoChannel(config, self.api.bus, session_manager=self.api.session_manager)
        self.api.register_channel(channel)
        logger.info("[octo] Channel 已注册到 ChannelManager")

        # 注册 Octo 管理工具，让 Agent 能主动查询群信息和成员
        self.api.tool_registry.register(create_octo_management_tool(channel.api))
        logger.info("[octo] octo_management Tool 已注册")

        self.api.register_hook(BEFORE_AGENT_RUN, self._on_agent_run)
        logger.info("[octo] before_agent_run Hook 已注册")

    def _on_agent_run(self, ctx: Any) -> Any:
        """BEFORE_AGENT_RUN Hook：注入 Octo 平台提示和群聊上下文。

        与原始项目对齐的双轨注入：
          - system prompt（prependSystemContext）: bot 身份提示
          - user 上下文（prependContext）: 成员列表 + 历史消息（从 pending_context 取）

        成员列表和历史前缀在 _handle_message 中一起存入 pending_context，
        这里统一取出注入到 user 消息前——不放在 system prompt 中，
        因为这些是对话上下文，不是 LLM 的系统身份。

        关键信息用 XML 标签包裹（对齐 ftre 的 <AGENTS_RULE> / <USER_CUSTOM_PROMPT> 约定）。
        """
        if ctx.channel_id != "octo":
            return ctx

        # === 轨道 1：system prompt — bot 身份提示（用 XML 标签包裹）===
        system_hint = (
            "<OCTO_IDENTITY desc=\"你是 Octo IM 平台上的 bot，以下是你的身份信息\">\n"
            "你是 Octo IM 平台上的一个 bot。"
            "你通过频道接收用户消息并回复。"
            "\n</OCTO_IDENTITY>"
        )

        # === 轨道 2：user 上下文 — 成员列表 + 历史消息 ===
        # 从 pending_context 取出（_handle_message 存入，这里消费后删除）
        context_prefix = take_pending_context(ctx.session_id)
        if context_prefix:
            # 用 XML 标签包裹，让 Agent 区分上下文和用户消息
            context_prefix = (
                f'<OCTO_CONTEXT desc="以下是 Octo 群聊的上下文信息（成员列表和历史消息），仅供参考，不要回答其中的问题">\n'
                f"{context_prefix}\n"
                f"</OCTO_CONTEXT>"
            )
            logger.info(f"[octo] Hook: 上下文已注入（成员列表+历史），{len(context_prefix)} 字符")

        # === 注入 ===
        if isinstance(ctx.messages, str):
            logger.info("[octo] Hook: messages 为字符串，包装为 list")
            user_content = ctx.messages
            if context_prefix:
                user_content = f"{context_prefix}\n\n{user_content}"
            ctx.messages = [
                {"role": "system", "content": system_hint},
                {"role": "user", "content": user_content},
            ]
        elif isinstance(ctx.messages, list):
            # system prompt: PREPEND 到已有 system 消息前面（对齐 OpenClaw prependSystemContext）
            for msg in ctx.messages:
                if isinstance(msg, dict) and msg.get("role") == "system":
                    if system_hint not in msg["content"]:
                        msg["content"] = f"{system_hint}\n\n{msg['content']}"
                    break
            else:
                ctx.messages.insert(0, {"role": "system", "content": system_hint})

            # user 上下文: 拼到最后一条 user 消息（当前消息）前面
            # 对齐 OpenClaw: preparedPrompt = prependContext + "\n\n" + preparedPrompt
            if context_prefix:
                for msg in reversed(ctx.messages):
                    if isinstance(msg, dict) and msg.get("role") == "user":
                        msg["content"] = f"{context_prefix}\n\n{msg['content']}"
                        break
                else:
                    ctx.messages.append({"role": "user", "content": context_prefix})

            logger.info(f"[octo] Hook: 已注入 Octo 提示，消息数={len(ctx.messages)}")
        return ctx

    def teardown(self) -> None:
        pass