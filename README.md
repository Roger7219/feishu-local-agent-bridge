# Feishu Local Agent Bridge

A minimal bridge that connects a Feishu/Lark bot to a local agent process.

The bridge receives bot messages through `lark-cli event consume`, forwards the text to a configured local command, and sends the command output back with `lark-cli im +messages-send`.

> This repository is intentionally business-neutral. It does not include built-in data-platform routes, private tools, cookies, or company-specific adapters.

## Features

- No public callback URL required; uses the `lark-cli` event long connection.
- Supports private chats and group messages that mention the bot.
- Calls one configurable local agent command.
- Passes chat text as a plain subprocess argument; never uses `shell=True`.
- Supports rate limiting, timeouts, long-output spooling, and JSONL audit logs.
- Includes a macOS LaunchAgent template for daemon mode.

## Project Layout

```text
feishu-local-agent-bridge/
├── bridge.py                                      # Main process: receive, route, call local agent, reply
├── config.example.yaml                           # Safe config template
├── config.yaml                                   # Local private config, ignored by Git
├── launchd/com.example.feishu-local-agent-bridge.plist.example
├── requirements.txt
├── tests/test_bridge.py
└── tools/__init__.py
```

## Prerequisites

- Python 3.9+
- Node.js / npm
- `lark-cli` installed and configured

Install Feishu/Lark CLI:

```bash
npm install -g @larksuite/cli
lark-cli config init --new
lark-cli doctor
```

Install Python dependency:

```bash
python3 -m pip install -r requirements.txt
```

## Create And Configure The Feishu Bot

In Feishu Open Platform:

1. Create or open an internal app.
2. Enable the Bot capability.
3. Enable event subscription for:

```text
im.message.receive_v1
```

4. Grant the scopes required by:

```bash
lark-cli event schema im.message.receive_v1
```

5. Grant send-message permission if you want bot replies. Otherwise set `send_as: user` in `config.yaml`.
6. Publish/install the app.

Check local event availability:

```bash
lark-cli event list
lark-cli event schema im.message.receive_v1
```

## Configure

Copy the config template:

```bash
cp config.example.yaml config.yaml
```

Edit `config.yaml`:

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

`agent.command` is the only integration point. The bridge replaces these placeholders before running the command:

- `{query}`: incoming chat text after removing the bot mention.
- `{chat_id}`: Feishu chat ID.
- `{sender_id}`: Feishu sender open ID.
- `{message_id}`: message/event ID.

Keep the command as a YAML list. Do not wrap the whole command in a shell string.

## Local Agent Contract

Your local agent can output plain text:

```text
hello from local agent
```

Or JSON with one of these string fields:

```json
{"reply":"hello from local agent"}
```

Supported reply keys are `reply`, `text`, `message`, `output`, and `result`.

A tiny example agent:

```python
#!/usr/bin/env python3
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--query", required=True)
args = parser.parse_args()

print(f"You said: {args.query}")
```

## Run Locally

Preflight check:

```bash
python3 bridge.py --config config.yaml --check
```

Start foreground:

```bash
python3 bridge.py --config config.yaml
```

Simulate one event without sending to Feishu:

```bash
python3 bridge.py --config config.yaml --dry-run --once-event-json \
'{"event_id":"ev_test","chat_id":"oc_test","chat_type":"p2p","sender_id":"ou_test","content":"help"}'
```

## macOS LaunchAgent

Copy and edit the plist example:

```bash
cp launchd/com.example.feishu-local-agent-bridge.plist.example \
  ~/Library/LaunchAgents/com.example.feishu-local-agent-bridge.plist
```

Replace `/ABSOLUTE/PATH/TO/feishu-local-agent-bridge` with your real project path, then load it:

```bash
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.example.feishu-local-agent-bridge.plist
launchctl kickstart -k gui/$(id -u)/com.example.feishu-local-agent-bridge
```

Check status:

```bash
launchctl print gui/$(id -u)/com.example.feishu-local-agent-bridge
lark-cli event status
```

View logs:

```bash
tail -f ./logs/launchd.err.log
tail -f ./logs/bridge-$(date +%Y%m%d).jsonl
```

Stop:

```bash
launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/com.example.feishu-local-agent-bridge.plist
lark-cli event stop
```

## Chat Usage

Private chat:

```text
help
summarize this note
query something from my local agent
```

Group chat:

```text
@YourBotName summarize this note
```

Group messages are ignored unless the bot is mentioned.

## Safety Notes

- The bridge never executes chat text as a shell command.
- The local agent command is configured by the machine owner in `config.yaml`.
- Real secrets belong in local config, keychains, or environment variables; do not commit them.
- `config.yaml`, logs, outputs, PID files, and secret-like files are ignored by Git.
- Set `security.block_write_intents: true` if you want the bridge to reject write-like prompts before calling the local agent.

## Tests

```bash
python3 -m py_compile bridge.py tools/__init__.py
python3 -m unittest discover -s tests -v
```

## Publish Checklist

Before publishing or forking, run:

```bash
git status --short
grep -RInE "app_secret|client_secret|access_token|refresh_token|authorization|cookie|password" . \
  --exclude-dir=.git --exclude=README.md
```

If any real secret was ever committed, rotate that credential and rewrite Git history before making the repository public.

## Troubleshooting

### `lark-cli doctor` says not configured

Run:

```bash
lark-cli config init --new
lark-cli doctor
```

### Bot receives no messages

Check:

```bash
lark-cli event status
lark-cli event list
python3 bridge.py --config config.yaml --check
tail -f ./logs/launchd.err.log
```

Common causes: the app is not published, Bot capability is disabled, `im.message.receive_v1` is not subscribed, the group message did not mention the bot, or `bot_name` does not match the actual bot name.

### Can this call Codex or another local agent?

Yes. Put the local entrypoint in `agent.command`, for example:

```yaml
agent:
  command:
    - python3
    - /path/to/my_codex_wrapper.py
    - --query
    - "{query}"
```

The bridge only handles Feishu I/O, rate limits, logs, and subprocess wiring. Your local agent decides how to answer.
