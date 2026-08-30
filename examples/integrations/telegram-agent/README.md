# Telegram Agent

This example is one conversational Telegram bot. The selected root contains the signed standing ingress and its `bot/telegram-chat` child flow. The mock configuration runs the complete flow tree locally with deterministic responses.

## Scaffold And Run

```sh
swarm new webhook-responder --output telegram-agent
cd telegram-agent
swarm verify . --config ./swarm.yaml
swarm serve . --config ./swarm.yaml
```

In another terminal:

```sh
cd telegram-agent
swarm test . tests/smoke.yaml --config ./swarm.yaml
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

Live graduation is one explicit source edit. In `bot/telegram-chat/agents.yaml`, change `phrase-bot` from:

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

Store the three live credentials at the scaffold root:

```sh
printf '%s' "$WEBHOOK_SIGNING_SECRET" | swarm secrets set webhook_signing.telegram --stdin
printf '%s' "$TELEGRAM_BOT_TOKEN" | swarm secrets set telegram_bot_token --stdin
printf '%s' "$ANTHROPIC_API_KEY" | swarm secrets set ANTHROPIC_API_KEY --stdin
swarm verify . --config ./swarm.live.yaml
swarm serve . --config ./swarm.live.yaml --dev --expose
```

Read the ready output for the public `/webhooks/chat/telegram` URL. Register that exact URL and the same signing secret with Telegram's `setWebhook` API. Telegram POSTs are rejected unless `X-Telegram-Bot-Api-Secret-Token` matches `webhook_signing.telegram`.

After the mock block is removed, `llm.backend` selects the live provider. This example uses `anthropic`; another supported live backend can use the same contracts when its own credential and runtime prerequisites are configured.
