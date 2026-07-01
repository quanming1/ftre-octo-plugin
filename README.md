# ftre Octo Plugin

Octo IM 频道插件，将 ftre agent 接入 Octo 平台（基于 WuKongIM 协议），
作为群聊/私聊 bot 使用。

> **参考项目**: 本插件桥接协议和 API 设计大量参考了 OpenClaw 的 Octo Channel 插件
> [Mininglamp-OSS/openclaw-channel-octo](https://github.com/Mininglamp-OSS/openclaw-channel-octo)，
> 特此致谢。

## 架构

```text
Octo 服务器 ←(WuKongIM 二进制 WS)→ octo-bridge.js (Node.js 桥接)
     ←(JSON WS ws://127.0.0.1:9876)→ OctoChannel (Python) ←→ ftre EventBus ←→ AgentLoop
```

- **octo-bridge.js**：处理 WuKongIM 二进制协议（DH 密钥交换 + AES-128-CBC 解密），
  将解密后的消息以 JSON 格式转发到本地 WebSocket。
- **OctoChannel (Python)**：连接桥接的本地 WS，将入站消息投递到 ftre EventBus，
  将 AgentLoop 的回复通过 Octo HTTP API 发回用户。

## 文件结构

```
octo-plugin/
  octo_channel.py   ← 公开门面，re-export 所有 API
  _constants.py     ← 常量 + session_id 编解码工具函数
  _api.py           ← OctoBotApi HTTP 客户端
  _mention.py       ← @ 检测 + 免@ 偏好
  _channel.py       ← OctoChannel 类（WS 连接、消息收发、session 映射）
  _plugin.py        ← OctoChannelPlugin 入口（setup/hook）
  octo-bridge.js    ← Node.js 桥接
  package.json
```

`_` 前缀表示内部模块，`octo_channel.py` 作为唯一公开接口 re-export 所有符号。
ftre 插件扫描器通过 `~/.ftre/plugins/octo_channel.py`（shim）加载本项目。

## 安装

```powershell
cd $env:USERPROFILE\.ftre\plugins\octo-plugin
npm install
```

## 配置

`~/.ftre/config.json` 中 plugins 数组：

```json
{
  "plugins": [
    {
      "name": "octo_channel",
      "enabled": true,
      "config": {
        "bot_token": "bf_xxx",
        "api_url": "https://im.deepminer.com.cn/api",
        "bridge_port": 9876,
        "require_mention": true
      }
    }
  ]
}
```

### 配置项

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `bot_token` | string | 必填 | Octo Bot Token（`bf_` 开头） |
| `api_url` | string | 必填 | Octo API 服务器地址 |
| `bridge_port` | int | 9876 | 桥接本地 WebSocket 端口 |
| `require_mention` | bool | true | 群聊中是否必须 @ 才回复。设为 false 则所有消息都回复 |
| `bot_id` | string | 自动获取 | bot 的 UID（从 register API 自动获取，也可手动指定） |
| `bot_name` | string | 同 bot_id | bot 名称，用于 @ 检测的文本兜底 |

## 群聊 @ 检测

当 `require_mention` 为 true（默认）时，bot 在群聊中只在被 @ 时回复。
检测逻辑按优先级：

1. **mention.uids** 包含 bot_uid → 被直接 @
2. **mention.ais=1** → @AI / @所有AI
3. **文本兜底**：消息内容正则匹配 `@bot名称`（防止旧客户端不发 mention payload）

私聊消息始终回复，不受 `require_mention` 影响。

## 运行时说明

- 插件启动时调用 `POST /v1/bot/register` 获取 `robot_id`，用于过滤自己发出的消息，
  防止 bot 自回复死循环。
- Octo 会话通过 `SessionManager.get_or_create_external_session()` 映射到 ftre session。
  session_id 格式：`octo_{channel_type}_{channel_id}`。
- 桥接进程从本目录启动，Node 依赖从 `octo-plugin/node_modules` 解析。

## 验证

```powershell
# 语法检查
node --check .\octo-bridge.js
python -c "import ast; ast.parse(open('octo_channel.py').read()); print('OK')"

# 运行测试（从 ftre 仓库）
cd E:\ftre
$env:PYTHONPATH = "$env:USERPROFILE\.ftre\plugins\octo-plugin"
python -m pytest tests\test_octo_channel.py -v
```