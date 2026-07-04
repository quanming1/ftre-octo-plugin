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
  - _api.py:       常量 + OctoBotApi HTTP 客户端 + session_id 编解码
  - _mention.py:   @ 检测 + 群成员缓存与格式化
  - _channel.py:   OctoChannel 类 + 历史消息拉取与上下文注入
  - _tools.py:     octo_management Agent 工具
  - _plugin.py:    OctoChannelPlugin 入口
"""

# 从各内部模块 re-export 所有公开 API
from _api import (  # noqa: E402, F401
    OctoBotApi,
    CHANNEL_TYPE_DM,
    CHANNEL_TYPE_GROUP,
    CHANNEL_TYPE_THREAD,
    build_external_key,
    build_session_id,
    extract_parent_group_no,
    parse_session_id,
)
from _mention import (  # noqa: E402, F401
    check_mentioned,
    get_cached_members,
    set_cached_members,
    build_member_list_prefix,
    build_uid_to_name_map,
)
from _channel import (  # noqa: E402, F401
    OctoChannel,
    fetch_and_build_history,
    record_bot_reply,
    set_pending_inbound_seq,
    take_pending_inbound_seq,
    build_sender_label,
)
from _tools import create_octo_management_tool  # noqa: E402, F401
from _plugin import OctoChannelPlugin  # noqa: E402, F401

# 桥接进程和测试需要直接引用这些模块
import aiohttp  # noqa: E402
import subprocess  # noqa: E402

__all__ = [
    "OctoBotApi",
    "OctoChannel",
    "OctoChannelPlugin",
    "create_octo_management_tool",
    "CHANNEL_TYPE_DM",
    "CHANNEL_TYPE_GROUP",
    "CHANNEL_TYPE_THREAD",
    "build_external_key",
    "build_session_id",
    "parse_session_id",
    "extract_parent_group_no",
    "aiohttp",
    "subprocess",
]