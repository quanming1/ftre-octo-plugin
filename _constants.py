"""
Octo Channel Plugin — 常量与 session_id 编解码工具函数。

- session_id 格式: "octo_{channel_type}_{channel_id}"
  - 私聊: "octo_1_{uid}"
  - 群聊: "octo_2_{group_no}"
- external_key 格式: "octo:{channel_type}:{channel_id}"
  - 用于跨组件传递 Octo 会话的唯一标识，避免与 session_id 混淆
"""

# Octo 消息通道类型常量
# 1=私聊(DM), 2=群聊(Group), 5=讨论串(Thread)
CHANNEL_TYPE_DM = 1
CHANNEL_TYPE_GROUP = 2
CHANNEL_TYPE_THREAD = 5


def build_external_key(channel_type: int, channel_id: str, from_uid: str) -> str:
    """构造 external_key 用于跨组件传递 Octo 会话标识。

    external_key 格式: "octo:{channel_type}:{channel_id}"
    私聊时 channel_id 为空，则用 from_uid 替代。
    """
    cid = channel_id if channel_id else from_uid
    return f"octo:{channel_type}:{cid}"


def build_session_id(channel_type: int, channel_id: str, from_uid: str) -> str:
    """构造 session_id 用于 ftre 内部会话管理。

    session_id 格式: "octo_{channel_type}_{channel_id}"
    私聊时 channel_id 为空，则用 from_uid 替代。
    """
    cid = channel_id if channel_id else from_uid
    return f"octo_{channel_type}_{cid}"


def parse_session_id(session_id: str) -> tuple[int, str] | None:
    """从 session_id 反向解析出 (channel_type, channel_id)。

    解析失败返回 None。
    """
    parts = session_id.split("_", 2)
    if len(parts) < 3:
        return None
    try:
        channel_type = int(parts[1])
    except ValueError:
        return None
    return channel_type, parts[2]


# 保持向后兼容的旧名称（去掉下划线前缀）
_build_external_key = build_external_key
_build_session_id = build_session_id
_parse_session_id = parse_session_id