"""
Octo Channel Plugin for ftre — 公开门面。

将 ftre agent 接入 Octo IM 平台（基于 WuKongIM 协议），作为群聊/私聊 bot 使用。

架构：
  Octo 服务器 ←(WuKongIM 二进制 WS)→ octo-bridge.js (Node.js 桥接)
       ←(JSON WS ws://127.0.0.1:9876)→ OctoChannel (Python) ←→ ftre EventBus ←→ AgentLoop

数据流：
  入站：Octo 用户发消息 → WuKongIM WS → 桥接解密 → JSON WS → _handle_message() → BusMessage → AgentLoop
  出站：AgentLoop 产生回复 → ChannelManager._dispatch_loop() → send() → Octo sendMessage API

内部模块（_ 前缀表示私有，不直接暴露）：
  - _api.py:       常量 + OctoBotApi HTTP 客户端 + session_id 编解码
  - _mention.py:   @ 检测 + 群成员缓存与格式化
  - _channel.py:   OctoChannel 类 + 历史消息拉取与上下文注入
  - _tools.py:     octo_management Agent 工具
  - _plugin.py:    OctoChannelPlugin 入口
"""

from _plugin import OctoChannelPlugin  # noqa: E402, F401

__all__ = ["OctoChannelPlugin"]