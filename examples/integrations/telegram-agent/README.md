# Telegram Agent

This example is one conversational Telegram bot. The `bot/` package runs locally with deterministic mock responses. The repository root adds signed standing webhook ingress for deployment. Both entrypoints use the same chat flow.

## Scaffold And Run

```sh
swarm new webhook-responder --output telegram-agent
cd telegram-agent/bot
swarm verify --config ./swarm.yaml --contracts .
swarm serve --config ./swarm.yaml --contracts .
```

In another terminal:

```sh
cd telegram-agent/bot
swarm test --config ./swarm.yaml --contracts . ./tests/smoke.yaml
```

The checked source uses `mock_only`, needs no LLM or Telegram credentials, and sends no network request. Its first reply is:

```text
Mock turn 1: hello from Telegram
```

## Conversation Continuity

The agent declares `memory: true`. Repeated messages for one `conversation_reference` continue the same conversation, while another reference starts at turn one. Restarting `swarm serve` with the same store reloads the conversation.

For messages A1, A2, B1, restart, A3, the replies are:

```text
Mock turn 1: hello
Mock turn 2: again
Mock turn 1: hello
Mock turn 3: after restart
```

## Go Live

Live graduation is one explicit source edit. In `bot/flows/telegram-chat/agents.yaml`, change `phrase-bot` from:

```yaml
phrase-bot:
  role: phrase_bot
  intent: prompts/phrase-bot.md
  model: regular
  memory: true
  subscriptions: [inbound.telegram.text_message]
  emit_events: [telegram.reply_requested]
  mock:
    kind: python
    module: mocks/phrase-bot.py
```

to:

```yaml
phrase-bot:
  role: phrase_bot
  intent: prompts/phrase-bot.md
  model: regular
  memory: true
  subscriptions: [inbound.telegram.text_message]
  emit_events: [telegram.reply_requested]
```

Removing that exact `mock:` block changes the agent from mock-authored to live-authored. Changing only `llm.backend` does not override an authored mock.

Return to the scaffold root and store the three live credentials:

```sh
cd ../
printf '%s' "$WEBHOOK_SIGNING_SECRET" | swarm secrets set webhook_signing.telegram --stdin
printf '%s' "$TELEGRAM_BOT_TOKEN" | swarm secrets set telegram_bot_token --stdin
printf '%s' "$ANTHROPIC_API_KEY" | swarm secrets set ANTHROPIC_API_KEY --stdin
swarm verify --config ./swarm.live.yaml --contracts .
swarm serve --config ./swarm.live.yaml --contracts . --dev --expose
```

Read the ready output for the public `/webhooks/chat/telegram` URL. Register that exact URL and the same signing secret with Telegram's `setWebhook` API. Telegram POSTs are rejected unless `X-Telegram-Bot-Api-Secret-Token` matches `webhook_signing.telegram`.

After the mock block is removed, `llm.backend` selects the live provider. This example uses `anthropic`; another supported live backend can use the same contracts when its own credential and runtime prerequisites are configured.
