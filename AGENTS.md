# AGENTS.md — ftre Octo Plugin

AI agent 在操作本项目时的必要信息。**每次操作前请先阅读 [README.md](./README.md)** 了解完整架构和配置。

## 快速事实

| 项目 | 值 |
|------|-----|
| 仓库 | `quanming1/ftre-octo-plugin` |
| 本地路径 | `C:\Users\蒋全明\.ftre\plugins\octo-plugin` |
| 参考项目 | [Mininglamp-OSS/openclaw-channel-octo](https://github.com/Mininglamp-OSS/openclaw-channel-octo) |
| 后端项目 | `E:\ftre`（ftre Gateway） |
| 测试文件 | `E:\ftre\tests\test_octo_channel.py` |
| Shim 文件 | `C:\Users\蒋全明\.ftre\plugins\octo_channel.py` |
| 配置 | `C:\Users\蒋全明\.ftre\config.json`（plugins 数组中） |
| Node 桥接 | `octo-bridge.js`，处理 WuKongIM 二进制协议 |
| Python 版本 | 3.12 |
| 测试数量 | 40 |
| 代码检查 | mypy --strict + ruff + bandit + vulture 全过 |

## 运行测试

```powershell
cd E:\ftre
$env:PYTHONPATH = "$env:USERPROFILE\.ftre\plugins\octo-plugin"
python -m pytest tests\test_octo_channel.py -v
```

## 代码检查

```powershell
cd $env:USERPROFILE\.ftre\plugins\octo-plugin
mypy --strict --ignore-missing-imports .
ruff check .
bandit -r . -ll
vulture . --min-confidence 80
```

## 文件职责速查

| 文件 | 行数 | 职责 | 修改场景 |
|------|------|------|----------|
| `_api.py` | ~307 | 常量 + OctoBotApi HTTP 客户端 + session_id 编解码 | 新增 Octo API 端点或会话格式变更 |
| `_mention.py` | ~131 | @ 检测（含广播抑制）+ 群成员缓存与格式化 | 改 @ 检测逻辑或成员列表展示 |
| `_channel.py` | ~529 | OctoChannel 类 + 历史消息拉取与上下文注入 | 改消息收发/WS/历史/桥接管理 |
| `_tools.py` | ~135 | octo_management Agent 工具 | 新增管理操作 |
| `_plugin.py` | ~135 | OctoChannelPlugin 入口 + Hook + 安全策略 | 改注入逻辑或注册新 Hook |
| `octo_channel.py` | ~67 | 公开门面 re-export | 新增模块时同步导出 |
| `octo-bridge.js` | ~343 | Node.js WuKongIM 协议桥接 | 改 WuKongIM 协议处理 |

## 关键架构决策

1. **桥接架构**: Python 不直接处理 WuKongIM 二进制协议，由 Node.js 桥接先解密再通过本地 JSON WS 传给 Python
2. **Shim 加载**: ftre 扫描 `~/.ftre/plugins/*.py`，需要顶层的 `octo_channel.py` 作为 shim，实际项目在子目录 `octo-plugin/`。shim 在加载前将插件目录加入 `sys.path`
3. **session_id 编码**: `octo_{channel_type}_{channel_id}`，私聊时 channel_id 为空就用 from_uid
4. **WuKongIM 解密**: RECV 包密文是 base64 编码的 AES-128-CBC，需先 `Buffer→UTF-8→base64 decode→AES decrypt`
5. **`_` 前缀**: 表示内部模块，不对外暴露，只有 `octo_channel.py` 是公开 API

## 核心功能对齐原始项目

### @ 检测门控
- 群聊（type=2）和讨论串（type=5）都覆盖
- 三层检测：uids 直接 @ → ais=1（@AI）→ 文本正则兜底
- **广播抑制**：`all=1` 或 `humans=1` 时抑制 `ais=1`，防止 @所有人 刷屏
- 直接 @bot uid 始终触发，不受广播抑制影响

### 历史消息注入
- 被 @ 时调 `POST /v1/bot/messages/sync` 拉取最近 20 条消息
- 过滤 bot 自己的消息、当前消息、非文本消息
- 按 `_last_reply_seq` 分段：已回答（不要重复回答）/ 新消息（仅供参考）
- 回复成功后记录 `message_seq` 作为下次分段点

### 双轨注入（对齐 OpenClaw prependContext / prependSystemContext）
- **system prompt**（`<OCTO_IDENTITY>` 标签）：bot 身份提示，PREPEND 到已有 system 消息前
- **user 上下文**（`<OCTO_CONTEXT>` 标签）：成员列表 + 历史消息，拼到最后一条 user 消息前
- **安全策略**（`<OCTO_SAFETY>` 标签）：临时 hardcode，只响应特定用户
- 分隔符在注入点显式管理（`\n\n`），数据本身不带尾部换行

### Agent 管理工具
`octo_management` Tool 注册到 ftre tool_registry，Agent 可主动调用：

| action | API | 用途 |
|--------|-----|------|
| `list-groups` | `GET /v1/bot/groups` | 列出 bot 加入的群 |
| `group-info` | `GET /v1/bot/groups/{groupNo}` | 查看群信息 |
| `group-members` | `GET /v1/bot/groups/{groupNo}/members` | 查看群成员 |
| `search-members` | `GET /v1/bot/space/members` | 搜索空间成员 |

### 出站消息
- sendMessage 附带 `client_msg_no`（UUID）做幂等去重
- 空回复不发送（`if not content: return`）

## Git 约定

- **禁止私自 commit/push**: 除非用户明确要求，否则只改代码不提交
- Commit message 用中文
- 本仓库独立于 ftre 主仓库

## Bot 信息（当前配置）

| 字段 | 值 |
|------|-----|
| Bot 名称 | ftre开发 |
| Bot ID | 27hzdeigbfkcaf10dbd_bot |
| API Server | https://im.deepminer.com.cn/api |
| WS Server | wss://im.deepminer.com.cn/ws |

## API 速查

| 端点 | 方法 | 用途 |
|------|------|------|
| `/v1/bot/register` | POST | 注册 bot，获取 robot_id / im_token / ws_url |
| `/v1/bot/sendMessage` | POST | 发送消息（注意驼峰命名），附带 client_msg_no 幂等去重 |
| `/v1/bot/messages/sync` | POST | 获取频道历史消息（payload 为 base64 编码 JSON） |
| `/v1/bot/groups` | GET | 获取 bot 加入的群列表 |
| `/v1/bot/groups/{groupNo}` | GET | 获取群信息 |
| `/v1/bot/groups/{groupNo}/members` | GET | 获取群成员列表 |
| `/v1/bot/space/members` | GET | 搜索空间成员（keyword 参数） |

sendMessage 请求体格式:
```json
{
  "channel_id": "...",
  "channel_type": 1,
  "payload": { "type": 1, "content": "..." },
  "client_msg_no": "uuid-v4"
}
```

channel_type: 1=私聊, 2=群聊, 5=讨论串

Thread 的 channel_id 是复合格式 `groupNo____threadId`（4 个下划线），
调 members API 需用 `extract_parent_group_no()` 提取父群号。