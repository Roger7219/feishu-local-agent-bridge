# Feishu Local Agent Bridge

一个极简的飞书 / Lark 本地 Agent 桥接框架。

它通过 `lark-cli event consume` 接收飞书机器人消息，把消息文本转发给你配置的本地 Agent 命令，再通过 `lark-cli im +messages-send` 把本地 Agent 的结果回复到飞书。

> 这个仓库只提供“飞书消息 ↔ 本地 Agent”的通用连接能力，不内置任何业务查询路由、私有工具、Cookie 或公司内部适配器。

## 功能特性

- 不需要公网回调地址：使用 `lark-cli` 的事件长连接。
- 支持私聊机器人，以及群聊中 `@机器人` 触发。
- 只调用一个可配置的本地 Agent 命令。
- 用户消息作为普通 subprocess 参数传入，不使用 `shell=True`。
- 支持限流、超时、长结果落盘、JSONL 审计日志。
- 提供 macOS LaunchAgent 模板，方便常驻运行。

## 项目结构

```text
feishu-local-agent-bridge/
├── bridge.py                                      # 主进程：收消息、调用本地 Agent、回复飞书
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

安装飞书 / Lark CLI：

```bash
npm install -g @larksuite/cli
lark-cli config init --new
lark-cli doctor
```

安装 Python 依赖：

```bash
python3 -m pip install -r requirements.txt
```

## 创建并配置飞书机器人

在飞书开放平台中准备一个企业自建应用：

1. 创建或打开一个内部应用。
2. 开启「机器人」能力。
3. 在「事件订阅」中启用：

```text
im.message.receive_v1
```

4. 按下面命令提示补齐接收消息所需权限：

```bash
lark-cli event schema im.message.receive_v1
```

5. 如果希望用机器人身份回复，请补齐发送消息权限；如果不使用机器人身份回复，可以在 `config.yaml` 里设置 `send_as: user`。
6. 发布 / 安装应用到当前企业。

检查本机是否能看到事件：

```bash
lark-cli event list
lark-cli event schema im.message.receive_v1
```

## 配置

复制配置模板：

```bash
cp config.example.yaml config.yaml
```

编辑 `config.yaml`：

```yaml
feishu:
  event_key: im.message.receive_v1
  event_as: bot
  send_as: bot
  bot_name: YourBotName

security:
  max_reply_chars: 3000
  command_timeout_seconds: 120
  rate_limit_per_user_per_minute: 5
  rate_limit_per_user_per_hour: 50
  rate_limit_per_chat_per_minute: 20
  block_write_intents: false

agent:
  enabled: true
  command:
    - python3
    - /absolute/path/to/your_agent.py
    - --query
    - "{query}"
  cwd: .
  env: {}

output:
  dir: ./outputs

logging:
  dir: ./logs
```

`agent.command` 是唯一的本地 Agent 接入点。bridge 会在执行前替换这些占位符：

- `{query}`：飞书消息文本，已去掉机器人 mention。
- `{chat_id}`：飞书 chat ID。
- `{sender_id}`：发送人 open ID。
- `{message_id}`：消息 ID / 事件 ID。

请保持 `command` 为 YAML 数组，不要写成一整段 shell 字符串。

## 本地 Agent 输出约定

你的本地 Agent 可以直接输出纯文本：

```text
hello from local agent
```

也可以输出 JSON：

```json
{"reply":"hello from local agent"}
```

bridge 会优先读取这些字符串字段作为回复内容：

- `reply`
- `text`
- `message`
- `output`
- `result`

一个最小 Agent 示例：

```python
#!/usr/bin/env python3
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--query", required=True)
args = parser.parse_args()

print(f"你说的是：{args.query}")
```

## 本地运行

预检配置：

```bash
python3 bridge.py --config config.yaml --check
```

前台启动：

```bash
python3 bridge.py --config config.yaml
```

模拟一条飞书事件，不真正发消息到飞书：

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

把模板里的 `/ABSOLUTE/PATH/TO/feishu-local-agent-bridge` 替换成你的项目绝对路径，然后加载：

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

## 飞书使用方式

私聊机器人：

```text
help
帮我总结这段内容
查询我的本地 Agent 能力
```

群聊中需要 `@机器人`：

```text
@YourBotName 帮我总结这段内容
```

群聊中没有 mention 机器人时，bridge 默认不会响应。

## 安全说明

- bridge 不会把聊天文本当作 shell 命令执行。
- 本地 Agent 命令只由机器主人在 `config.yaml` 中配置。
- 真实密钥应放在本地配置、Keychain 或环境变量中，不要提交到 Git。
- `config.yaml`、日志、输出目录、PID 文件、疑似密钥文件均已被 `.gitignore` 忽略。
- 如果希望在调用本地 Agent 前先拦截疑似写操作，可以设置：

```yaml
security:
  block_write_intents: true
```

## 测试

```bash
python3 -m py_compile bridge.py tools/__init__.py
python3 -m unittest discover -s tests -v
```

## 发布前检查

公开或 fork 前建议执行：

```bash
git status --short
grep -RInE "app_secret|client_secret|access_token|refresh_token|authorization|cookie|password" . \
  --exclude-dir=.git --exclude=README.md
```

如果真实密钥曾经被提交过，请先轮换密钥，并重写 Git 历史后再公开仓库。

## 常见问题

### `lark-cli doctor` 提示 not configured

执行：

```bash
lark-cli config init --new
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

常见原因：应用没有发布、机器人能力未开启、没有订阅 `im.message.receive_v1`、群聊里没有 `@机器人`、`bot_name` 和实际机器人名称不一致。

### 可以接入 Codex 或其他本地 Agent 吗？

可以。把本地入口命令配置到 `agent.command` 即可：

```yaml
agent:
  command:
    - python3
    - /path/to/my_codex_wrapper.py
    - --query
    - "{query}"
```

bridge 只负责飞书消息收发、限流、日志和 subprocess 调用；具体怎么回答，由你的本地 Agent 决定。
