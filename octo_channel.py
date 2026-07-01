"""
Octo Channel Plugin for ftre — 公开门面。

将 ftre agent 接入 Octo IM 平台（基于 WuKongIM 协议），作为群聊/私聊 bot 使用。

架构：
  Octo 服务器 ←(WuKongIM 二进制 WS)→ octo-bridge.js (Node.js 桥接)
       ←(JSON WS ws://127.0.0.1:9876)→ OctoChannel (Python) ←→ ftre EventBus ←→ AgentLoop

数据流：
  入站：Octo 用户发消息 → WuKongIM WS → 桥接解密 → JSON WS → _handle_message() → BusMessage → AgentLoop
  出站：AgentLoop 产生回复 → ChannelManager._dispatch_loop() → send() → Octo sendMessage API

依赖：
  - ftre Channel 基类：提供 receive()/send() 抽象，通过 EventBus 与 AgentLoop 通信
  - ftre Plugin 体系：setup() 中注册 Channel + Hook，Gateway 启动时自动加载
  - Node.js 桥接：处理 WuKongIM 二进制协议（DH 密钥交换 + AES-128-CBC 解密）
  - aiohttp：Python 端 WebSocket 客户端和 HTTP 客户端

内部模块（_ 前缀表示私有，不直接暴露）：
  - _api.py:       OctoBotApi HTTP 客户端
  - _channel.py:   OctoChannel 类（WS 连接、消息收发、session 映射）
  - _plugin.py:    OctoChannelPlugin 入口
  - _mention.py:   @ 检测 + 后续免@ 偏好
  - _constants.py: 常量 + session_id 编解码工具函数
"""

# 从各内部模块 re-export 所有公开 API
from _api import OctoBotApi  # noqa: E402, F401
from _channel import OctoChannel  # noqa: E402, F401
from _plugin import OctoChannelPlugin  # noqa: E402, F401
from _constants import (  # noqa: E402, F401
    CHANNEL_TYPE_DM,
    CHANNEL_TYPE_GROUP,
    CHANNEL_TYPE_THREAD,
    build_external_key,
    build_session_id,
    extract_parent_group_no,
    parse_session_id,
    _build_external_key,
    _build_session_id,
    _parse_session_id,
)
from _members import (  # noqa: E402, F401
    get_cached_members,
    set_cached_members,
    build_member_list_prefix,
)

# 桥接进程和测试需要直接引用这些模块
import aiohttp  # noqa: E402
import subprocess  # noqa: E402

__all__ = [
    "OctoBotApi",
    "OctoChannel",
    "OctoChannelPlugin",
    "CHANNEL_TYPE_DM",
    "CHANNEL_TYPE_GROUP",
    "CHANNEL_TYPE_THREAD",
    "build_external_key",
    "build_session_id",
    "parse_session_id",
    "_build_external_key",
    "_build_session_id",
    "_parse_session_id",
    "aiohttp",
    "subprocess",
]