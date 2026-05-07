# Feishu Local Agent Bridge

把飞书 / Lark 开放平台机器人接到本机查询能力的小型桥接服务。

它通过 `lark-cli event consume` 长连接接收机器人消息，在本地按白名单路由到只读工具或 Codex skill，再用 `lark-cli im +messages-send` 把查询结果回复到飞书。

> 设计目标：让团队成员在飞书里问机器人，本机负责执行数据地图、血缘、元数据等查询；默认不提供任意 shell，也不开放写操作。

## 功能特性

- 不需要公网回调地址：复用 `lark-cli` 长连接消费飞书事件。
- 支持私聊机器人、群聊 `@机器人` 触发。
- 默认查询优先：支持表元数据、raw/Hive 接入检查、raw 到模型表血缘查询。
- 工具白名单：本地脚本、OpenCLI adapter、Codex skill 都可以封装成只读工具。
- 基础安全：参数校验、限流、超时、敏感信息脱敏、审计日志。
- 支持 macOS LaunchAgent 常驻运行。

## 项目结构

```text
feishu-local-agent-bridge/
├── bridge.py                                      # 主进程：收消息、路由、调用工具、发回复
├── config.example.yaml                           # 可提交的配置模板
├── config.yaml                                   # 本地真实配置，已被 .gitignore 忽略
├── launchd/com.example.feishu-local-agent-bridge.plist.example
├── requirements.txt
├── tests/test_bridge.py
└── tools/__init__.py
```

## 前置条件

- Python 3.9+
- Node.js / npm
- 已安装并授权 `lark-cli`
- 可选：`opencli` 以及你自己的只读查询 CLI / Codex skill

安装飞书 CLI：

```bash
npm install -g @larksuite/cli
lark-cli config init --new
lark-cli doctor
```

安装 Python 依赖：

```bash
python3 -m pip install -r requirements.txt
```

## 创建飞书机器人

在飞书开放平台里准备一个内部应用：

1. 创建或打开一个企业自建应用。
2. 开启「机器人」能力。
3. 在「事件订阅」里启用：

```text
im.message.receive_v1
```

4. 按 `lark-cli event schema im.message.receive_v1` 的提示补齐接收消息权限。
5. 如果希望机器人身份回复，补齐发送消息相关权限；否则可以在配置中使用 `send_as: user`。
6. 发布 / 安装应用到当前企业。

本机检查事件是否可用：

```bash
lark-cli event list
lark-cli event schema im.message.receive_v1
```

## 安装与配置

克隆项目后进入目录：

```bash
cd feishu-local-agent-bridge
```

复制配置模板：

```bash
cp config.example.yaml config.yaml
```

编辑 `config.yaml`，最小配置如下：

```yaml
feishu:
  event_key: im.message.receive_v1
  event_as: bot
  send_as: bot
  bot_name: YourBotName

security:
  trigger: mention_or_dm
  allow_all_group_users: true
  max_reply_chars: 3000
  command_timeout_seconds: 120

tools:
  source_raw_check:
    enabled: true
    readonly: true
    opencli_site: data-map
    # Optional for task-detail verification. Keep empty unless your platform provides these APIs.
    task_detail_url_template: ""
    task_detail_referer_template: ""
  raw_model_lineage:
    enabled: false
    readonly: true
    script: /absolute/path/to/explore_raw_model_lineage.py
  table_metadata:
    enabled: true
    readonly: true
    opencli_site: data-map

output:
  dir: ./outputs

logging:
  dir: ./logs
```

注意：

- `config.yaml` 是本地私有文件，不要提交到 Git。
- Cookie、token、App Secret、企业平台登录态只允许写在本地配置或本机凭证里，不要写进 README / 代码 / 测试。
- 如果你没有企业数据平台工具，可以先关闭对应工具，只保留帮助和基础路由测试。

## 本地运行

预检配置：

```bash
python3 bridge.py --config config.yaml --check
```

前台启动：

```bash
python3 bridge.py --config config.yaml
```

本地模拟一条飞书事件，不真正发送回复：

```bash
python3 bridge.py --config config.yaml --dry-run --once-event-json \
'{"event_id":"ev_test","chat_id":"oc_test","chat_type":"p2p","sender_id":"ou_test","content":"help"}'
```

## macOS 常驻运行

复制 LaunchAgent 模板：

```bash
cp launchd/com.example.feishu-local-agent-bridge.plist.example \
  ~/Library/LaunchAgents/com.example.feishu-local-agent-bridge.plist
```

把模板里的 `/ABSOLUTE/PATH/TO/feishu-local-agent-bridge` 替换为你的项目绝对路径，然后加载：

```bash
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.example.feishu-local-agent-bridge.plist
launchctl kickstart -k gui/$(id -u)/com.example.feishu-local-agent-bridge
```

查看状态：

```bash
launchctl print gui/$(id -u)/com.example.feishu-local-agent-bridge
lark-cli event status
```

查看日志：

```bash
tail -f ./logs/launchd.err.log
tail -f ./logs/bridge-$(date +%Y%m%d).jsonl
```

停止服务：

```bash
launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/com.example.feishu-local-agent-bridge.plist
lark-cli event stop
```

## 飞书使用示例

私聊机器人：

```text
帮助
source_db.source_table 查询是否有接入 hive 表
raw_xxx_full_1d 对应模型表
查表 db.table 元数据
```

群聊里需要 `@机器人`：

```text
@YourBotName source_db.source_table 查询是否有接入 hive 表
```

## 内置工具路由

- `source_raw_check`：根据源库表查询是否存在 raw/Hive 接入；会尝试搜索数据地图与任务详情，并校验 reader 源表和 writer 目标表。
- `raw_model_lineage`：封装本地 raw 到模型表血缘查询脚本，例如 Codex skill 里的 `explore_raw_model_lineage.py`。
- `table_metadata`：封装 OpenCLI 数据地图表搜索、表详情、字段查询。

如果工具依赖企业平台登录态，请确保本机 CLI 或 cookie 文件可用；本项目不会内置任何真实登录态。

## 扩展新的本地 skill

推荐模式是“飞书只接路由，本机工具做查询”：

1. 在 `bridge.py` 中新增一个 Tool 类。
2. 在 Tool 内校验参数，例如表名只允许字母、数字、下划线、点号。
3. 用 `subprocess.run([...], shell=False)` 调用本地 skill 脚本。
4. 在 `config.yaml` 的 `tools:` 下增加开关和脚本路径。
5. 在路由器里增加关键词识别。
6. 给正常输入、非法输入、超时、无结果都补单测。
7. 重启 bridge。

不要把聊天文本直接拼到 shell 命令里，也不要把写操作类 skill 直接暴露给群聊。

## 测试

```bash
python3 -m py_compile bridge.py tools/__init__.py
python3 -m unittest discover -s tests -v
```

## 发布前检查

`.gitignore` 默认排除了：

- `config.yaml`
- `logs/`
- `outputs/`
- `*.pid`
- `.env`
- cookie / token / config JSON 文件

发布前建议再扫一遍敏感信息：

```bash
grep -RInE "app_secret|DP_SESSION_ID|TGC|dunCookie|access_token|refresh_token|cookie|ou_|oc_" . \
  --exclude-dir=.git --exclude=README.md
```

如果你曾经把 App Secret、Cookie、Token 粘贴到聊天或截图里，建议在开放平台或对应系统里重新生成 / 轮换一次。

## 常见问题

### `lark-cli doctor` 提示 not configured

执行：

```bash
lark-cli config init --new
```

按终端输出的授权链接完成登录，再运行：

```bash
lark-cli doctor
```

### 飞书里发消息没有响应

依次检查：

```bash
lark-cli event status
lark-cli event list
python3 bridge.py --config config.yaml --check
tail -f ./logs/launchd.err.log
```

常见原因：应用没有发布、机器人能力未开启、事件 `im.message.receive_v1` 未订阅、群聊没有 `@机器人`、`bot_name` 配置和实际机器人名不一致。

### 群聊要不要限制人员

默认配置允许群内所有人发起查询，但只能调用工具白名单。你可以通过 `allowed_chat_ids`、`blocked_open_ids` 和限流参数逐步收紧。

### 能不能让机器人调用本地 Codex skill

可以。建议只暴露只读 skill，并用 Tool 适配器固定参数、固定入口、固定超时；不要让飞书用户直接选择任意 skill 或执行任意 shell。
