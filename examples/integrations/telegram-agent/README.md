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

The smoke test enters the declared public root input. An explicit connection delivers it to the private `telegram-chat` flow; live webhooks enter through the separate signed ingress connection.

The source's mock reply begins:

```text
Mock turn 1: hello from Telegram
```

## Serve Live

No source edit, mock-block removal or separate graduation configuration is required. `serve` and `serve --dev` select live execution even when the source contains doubles. Configure the selected live provider's actual prerequisites before serving.

An absent webhook signing credential leaves the declared Telegram ingress dormant. Swarm reports `DORMANT ingress` with the exact missing key and provisioning command; it publishes no Telegram route and creates no standing generation solely for that declaration. Other admissible work can still serve. This does not waive credentials required by the live model, outgoing Telegram replies, or another active capability. An empty or whitespace credential, a corrupt credential store, or a read failure is an error, not dormancy.

For example, to use the Anthropic API backend:

```sh
printf '%s' "$TELEGRAM_BOT_TOKEN" | swarm secrets set telegram_bot_token --stdin
printf '%s' "$ANTHROPIC_API_KEY" | swarm secrets set ANTHROPIC_API_KEY --stdin
swarm serve . --backend anthropic --dev --expose
```

To enable the dormant ingress, provision its signing credential and restart the server:

```sh
printf '%s' "$WEBHOOK_SIGNING_SECRET" | swarm secrets set webhook_signing.telegram --stdin
```

Setting a secret or reading status does not activate a running dormant declaration. Stop and repeat the serve command. Read the ready output for the public `/webhooks/chat/telegram` URL, then register that exact URL and the same signing secret with Telegram's `setWebhook` API. Telegram POSTs are rejected unless `X-Telegram-Bot-Api-Secret-Token` matches the admitted signing credential.

For a source that also declares the Telegram channel onboarding adapter, `swarm channel connect telegram` is the explicit in-process credential-admission and identity-ceremony path. It discovers the declaration before ingress is enabled. A stale previously learned credential instead reports `RECOVERY REQUIRED`; resume the exact reported operation with fresh credentials and complete the fresh ceremony. Neither restart nor a populated source-default key substitutes for that learned authority.

The default Claude CLI backend instead requires its own credentials, executable and supported workspace setup. Authored doubles do not waive those live requirements.

## Conversation Continuity

The agent declares `memory: true`. Repeated live messages for one `conversation_reference` continue the same conversation; another reference starts a separate conversation. Plain `swarm serve` uses retained state across restart. `serve --dev` deliberately starts a fresh scratch epoch. Public `test` always uses a fresh isolated store and is not a restart test.

In retained serve, losing the ingress credential makes the existing nonterminal standing generation non-executable without deleting its history or operator pause. Reprovisioning and restarting reuses that generation; it does not replay work quiesced during dormancy. A terminal generation still requires the existing explicit reset remedy.
