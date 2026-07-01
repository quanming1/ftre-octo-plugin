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

## 运行测试

```powershell
cd E:\ftre
$env:PYTHONPATH = "$env:USERPROFILE\.ftre\plugins\octo-plugin"
python -m pytest tests\test_octo_channel.py -v
```

## 文件职责速查

| 文件 | 职责 | 修改场景 |
|------|------|----------|
| `_constants.py` | 常量 + session_id 编解码 | 新增 channel_type 或会话格式变更 |
| `_api.py` | OctoBotApi HTTP 客户端 | 新增 Octo API 端点 |
| `_mention.py` | @ 检测 + 免@ 偏好 | 改 @ 检测逻辑或加免@ 功能 |
| `_members.py` | 成员缓存 + 格式化 | 改成员列表展示方式 |
| `_channel.py` | OctoChannel 类 | 改消息收发/WS 连接/桥接管理 |
| `_plugin.py` | OctoChannelPlugin 入口 + Hook | 改 system prompt 注入或注册新 Hook |
| `octo_channel.py` | 公开门面 re-export | 新增模块时同步导出 |
| `octo-bridge.js` | Node.js WuKongIM 协议桥接 | 改 WuKongIM 协议处理 |

## 关键架构决策

1. **桥接架构**: Python 不直接处理 WuKongIM 二进制协议，由 Node.js 桥接先解密再通过本地 JSON WS 传给 Python
2. **Shim 加载**: ftre 扫描 `~/.ftre/plugins/*.py`，所以需要顶层的 `octo_channel.py` 作为 shim，实际项目在子目录 `octo-plugin/`
3. **session_id 编码**: `octo_{channel_type}_{channel_id}`，私聊时 channel_id 为空就用 from_uid
4. **WuKongIM 解密**: RECV 包密文是 base64 编码的 AES-128-CBC，需先 `Buffer→UTF-8→base64 decode→AES decrypt`
5. **`_` 前缀**: 表示内部模块，不对外暴露，只有 `octo_channel.py` 是公开 API

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
| `/v1/bot/sendMessage` | POST | 发送消息（注意驼峰命名） |
| `/v1/bot/groups/{groupNo}/members` | GET | 获取群成员列表 |

sendMessage 请求体格式:
```json
{
  "channel_id": "...",
  "channel_type": 1,
  "payload": { "type": 1, "content": "..." }
}
```

channel_type: 1=私聊, 2=群聊, 5=讨论串