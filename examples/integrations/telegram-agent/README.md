# Telegram Agent

This example is one conversational Telegram bot. The `bot/` package runs locally with deterministic mock responses; the repository root adds signed standing webhook ingress for deployment. Both entrypoints use the same chat flow.

## Scaffold And Run

```sh
swarm new webhook-responder --output telegram-agent
cd telegram-agent
cd bot
swarm verify --config ./swarm.yaml --contracts .
swarm serve --config ./swarm.yaml --contracts .
```

In another terminal, run the checked smoke scenario:

```sh
cd telegram-agent/bot
swarm test --config ./swarm.yaml --contracts . ./tests/smoke.yaml
```

The run uses `mock_only`, needs no LLM or Telegram credentials, and sends no network request. The mock reply is deterministic: `Mock turn 1: hello from Telegram`.

## Conversation Continuity

The agent declares `memory: true`. Repeated messages for one `conversation_reference` continue the same conversation, while another reference starts at turn one. Restarting `swarm serve` with the same selected store reloads the prior conversation.

Expected replies for messages A1, A2, B1, restart, A3 are:

```text
Mock turn 1: hello
Mock turn 2: again
Mock turn 1: hello
Mock turn 3: after restart
```

## Go Live

The root package already contains the standing Telegram ingress wrapper. Return to the scaffold root, store the two Telegram secrets, and expose the Anthropic key:

```sh
printf '%s' "$WEBHOOK_SIGNING_SECRET" | swarm secrets set webhook_signing.telegram --stdin
printf '%s' "$TELEGRAM_BOT_TOKEN" | swarm secrets set telegram_bot_token --stdin
export ANTHROPIC_API_KEY
swarm verify --config ./swarm.live.yaml --contracts .
swarm serve --config ./swarm.live.yaml --contracts . --dev --expose
```

Read the ready output for the public `/webhooks/chat/telegram` URL, then register that exact URL and the same signing secret with Telegram's `setWebhook` API. Telegram POSTs are rejected unless `X-Telegram-Bot-Api-Secret-Token` matches `webhook_signing.telegram`.

The live example teaches `anthropic` because it requires one environment variable and no local model process. `llm.backend` is swappable; for example, `claude_cli` can use the same contracts when its own runtime prerequisites are configured.
