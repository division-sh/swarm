# Telegram Agent

This example is one conversational Telegram bot. The selected root contains signed standing ingress and its `telegram-chat` child flow. Authored doubles stay in the source: the command chooses live or mock execution.

## Scaffold And Test

```sh
swarm new webhook-responder --output telegram-agent
cd telegram-agent
swarm verify .
swarm test . tests/smoke.yaml
```

`verify` checks structural validity, not deployment readiness. `test` creates a fresh private mock runtime and removes it after the scenario finishes. It needs no separate server, LLM credentials, Telegram credentials, Claude executable or Docker. Its private session has no webhook ingress and cannot connect to an existing server. Repeating the command starts a new store, not a retained conversation.

The source's mock reply begins:

```text
Mock turn 1: hello from Telegram
```

## Serve Live

No source edit, mock-block removal or separate graduation configuration is required. `serve` and `serve --dev` select live execution even when the source contains doubles. Configure the selected live provider's actual prerequisites before serving.

For example, to use the Anthropic API backend:

```sh
printf '%s' "$WEBHOOK_SIGNING_SECRET" | swarm secrets set webhook_signing.telegram --stdin
printf '%s' "$TELEGRAM_BOT_TOKEN" | swarm secrets set telegram_bot_token --stdin
printf '%s' "$ANTHROPIC_API_KEY" | swarm secrets set ANTHROPIC_API_KEY --stdin
swarm serve . --backend anthropic --dev --expose
```

Read the ready output for the public `/webhooks/chat/telegram` URL. Register that exact URL and the same signing secret with Telegram's `setWebhook` API. Telegram POSTs are rejected unless `X-Telegram-Bot-Api-Secret-Token` matches `webhook_signing.telegram`.

The default Claude CLI backend instead requires its own credentials, executable and supported workspace setup. Authored doubles do not waive those live requirements.

## Conversation Continuity

The agent declares `memory: true`. Repeated live messages for one `conversation_reference` continue the same conversation; another reference starts a separate conversation. Plain `swarm serve` uses retained state across restart. `serve --dev` deliberately starts a fresh scratch epoch. Public `test` always uses a fresh isolated store and is not a restart test.
