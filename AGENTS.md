# AGENTS.md — ftre Octo Plugin

AI agent 在操作本项目时的必要信息。**每次操作前请先阅读 [README.md](./README.md)** 了解完整架构和配置。

<project>
本地路径：C:\Users\蒋全明\.ftre\plugins\octo_plugin\
后端项目：E:\ftre（ftre Gateway，shim 通过 ~/.ftre/plugins/octo_channel.py 被扫描加载）
定位：ftre 生态外部插件（独立 git 仓库，独立管理）——Octo IM 消息通道
技术栈：Python 3.12 + Node.js（octo-bridge.js 处理 WuKongIM 二进制协议）
代码检查：mypy --strict + ruff + bandit + vulture 全过

MANDATORY 首次进入本仓库先读 3 份文档，之后每次 commit 前重读第 1 份：
1. docs/COMMIT.md — 提交规范唯一完整定义（type/scope/hook 机制）
2. docs/PROCESS.md — PRD 驱动开发流程（六步闭环）
3. docs/TODO.yaml — 阶段 id 唯一事实源（commit scope 校验依据）
</project>

<git_flow MANDATORY>

<basic_discipline>
- NEVER 私自 commit / push：除非用户明确要求（"commit"、"push"、"提交"），否则只改代码不提交
- 回滚需确认：回滚前告知内容/范围/影响，得到确认后再执行
- ALWAYS push 前先 commit
</basic_discipline>

<branch_model>
master（仅发布，永不直接提交）← develop（默认基底）← feature/&lt;阶段id&gt;-&lt;name&gt; / prd-update / todos-update / release/&lt;ver&gt; / hotfix/&lt;name&gt;

- 默认工作分支是 develop
- NEVER 直接提交 master；NEVER 直接 commit 到 develop——develop 只接受 `feature/*` → `git merge --no-ff` 合入
- MANDATORY feat/fix 分支名必须关联 TODO 阶段 id（如 feature/A2-octo-channel），提交 scope 与分支名阶段 id 必须一致（commit-msg hook 强制）
</branch_model>

<commit_format>
`&lt;type&gt;(&lt;scope&gt;): &lt;subject&gt;`，subject 中文
- type 白名单：feat / fix / prd / todos / docs / refactor / test / style / chore / perf
- feat/fix/prd/todos 的 scope 必须是 docs/TODO.yaml 中真实存在的阶段 id
- 其他 type 的 scope 用 .githooks/.scopes 白名单模块名（channel/tool/management/config/docs）
- 一条提交只做一件事；NEVER 写 fix stuff / update / misc 这类无意义 message
</commit_format>

<merge_and_hooks>
- feature/* → develop 用 --no-ff；develop → master 走 release/*；NEVER rebase 已推送历史
- 本地强制：.githooks/commit-msg（提交校验）+ .githooks/pre-push（master 保护 + develop merge-only）
- merge:/revert: 开头系统提交跳过
- MANDATORY 首次在本仓库提交前，先完整阅读 docs/COMMIT.md（提交规范唯一完整定义，含 type/scope 规则与常见错误速查）
- 标准流程：checkout develop → checkout -b feature/&lt;阶段id&gt;-&lt;task&gt; → 开发+测试 → commit → merge --no-ff → push develop
</merge_and_hooks>

</git_flow>

<prd_driven MANDATORY>
- MANDATORY 首次在本仓库开工前，先完整阅读 docs/PROCESS.md（PRD 驱动流程六步闭环）
- ALWAYS 先 PRD 后开发：TODO 阶段开工前先在 docs/prd/ 建 PRD（从 PRD-TEMPLATE.md 复制）并定稿 approved
- PRD 是唯一依据：需求/实现/测试/验收全部对照 PRD；验收按 PRD「验收标准」逐条核对
- 阶段 id 与状态见 docs/TODO.yaml（commit scope 的唯一事实源）
</prd_driven>

## 快速事实

| 项目 | 值 |
|------|-----|
| 仓库 | `quanming1/ftre-octo-plugin` |
| 本地路径 | `C:\Users\蒋全明\.ftre\plugins\octo_plugin` |
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
$env:PYTHONPATH = "$env:USERPROFILE\.ftre\plugins\octo_plugin"
python -m pytest tests\test_octo_channel.py -v
```

## 代码检查

```powershell
cd $env:USERPROFILE\.ftre\plugins\octo_plugin
mypy --strict --ignore-missing-imports .
ruff check .
bandit -r . -ll
vulture . --min-confidence 80
```

## 文件职责速查

| 文件 | 行数 | 职责 | 修改场景 |
|------|------|------|----------|
| `_api.py` | ~335 | 常量 + OctoBotApi HTTP 客户端 + session_id 编解码 | 新增 Octo API 端点或会话格式变更 |
| `_mention.py` | ~152 | @ 检测（含广播抑制）+ 群成员缓存与格式化 | 改 @ 检测逻辑或成员列表展示 |
| `_channel.py` | ~740 | OctoChannel 类 + 历史消息拉取与上下文注入 + GROUP.md 缓存 | 改消息收发/WS/历史/桥接管理/GROUP.md |
| `_tools.py` | ~305 | octo_management Agent 工具（含 fetch-history） | 新增管理操作 |
| `_plugin.py` | ~95 | OctoChannelPlugin 入口 + Hook + GROUP.md 注入 | 改注入逻辑或注册新 Hook |
| `octo_channel.py` | ~67 | 公开门面 re-export | 新增模块时同步导出 |
| `octo-bridge.js` | ~343 | Node.js WuKongIM 协议桥接 | 改 WuKongIM 协议处理 |

## 关键架构决策

1. **桥接架构**: Python 不直接处理 WuKongIM 二进制协议，由 Node.js 桥接先解密再通过本地 JSON WS 传给 Python
2. **Shim 加载**: ftre 扫描 `~/.ftre/plugins/*.py`，需要顶层的 `octo_channel.py` 作为 shim，实际项目在子目录 `octo_plugin/`。shim 在加载前将插件目录加入 `sys.path`
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

### GROUP.md 注入
- 群聊消息到达时 fire-and-forget 调 `GET /v1/bot/groups/{groupNo}/md` 拉取 GROUP.md
- 内存缓存（`_group_md_cache`）+ 已检查集合（`_group_md_checked`），每个群只拉一次
- `BEFORE_AGENT_RUN` hook 里从 session 元数据解析频道 → 读缓存 → 注入 `<OCTO_GROUP_MD>` 到 system prompt
- 私聊不拉 GROUP.md

### 按需历史拉取
- `octo_management` Tool 新增 `fetch-history` action，Agent 主动决定何时拉取更多历史
- 支持 `limit`（默认 50，最大 200）和 `beforeSeq` 分页游标
- 工具自动从 session 元数据解析频道信息，Agent 不需要知道 channel_id

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
| `fetch-history` | `POST /v1/bot/messages/sync`（按需） | 拉取频道更多历史消息（limit + beforeSeq 分页） |

### 出站消息
- sendMessage 附带 `client_msg_no`（UUID）做幂等去重
- 空回复不发送（`if not content: return`）

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
| `/v1/bot/groups/{groupNo}/md` | GET | 获取群 GROUP.md（群规则/话题设定等） |
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

<anti_lazy>
- NEVER 用空函数、TODO、placeholder 假装完成
- NEVER 重复性任务做几个就声称全部完成——逐个执行，验证全部
- NEVER 跳过失败的步骤——修复后重新验证
- 同一问题反复改不好就停下：回到初始假设、复现路径和失败证据重新判断，换方向
- 收尾前通读改过的文件：确认连贯、无语法错误、无残留调试代码
- 违反以上任何一条：下一轮立即自纠
</anti_lazy>
