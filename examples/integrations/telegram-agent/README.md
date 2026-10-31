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

The smoke test enters the declared public root input. An explicit connection delivers it to the private `telegram-chat` flow. The root also holds the signed Telegram provider and its retirement gate; live webhooks use that same connection.

The source's mock reply begins:

```text
Mock turn 1: hello from Telegram
```

## Serve Live

Set `operator_account` in `policy.yaml` to the Telegram bot account selected by
`swarm channel connect telegram`. The checked-in `420079` is the example peer's
bot ID, not a fallback to the first registered account. Each receiving flow
selects its own account explicitly; customer identities do not choose it.

No source edit, mock-block removal or separate graduation configuration is required. `serve` and `serve --dev` select live execution even when the source contains doubles. Configure the selected live provider's actual prerequisites before serving.

Connect works while serve runs. Registration stores the bot credentials and its
private webhook signing key through the channel owner. Serve admits that exact
registration only for its sole current flow holder; it does not select the first
linked bot or trust an account supplied in a webhook payload. No manual signing
secret or `setWebhook` call is needed for this registered-account journey.

For example, to use the Anthropic API backend:

```sh
swarm channel connect telegram
printf '%s' "$ANTHROPIC_API_KEY" | swarm secrets set ANTHROPIC_API_KEY --stdin
swarm serve . --backend anthropic --expose
```

Enter the bot token at the hidden connect prompt and confirm the displayed bot
account. Before serving, put that account in `policy.yaml` as described above.
Operator verification is separate and only applies when the deployment declares
an HITL operator; customers are never asked to authorize the connection.

A Telegram bot connected or newly held while serve runs receives after a
serve restart in v0.7. Connecting or assigning an account while serve is
running does not refresh that process's credential admission.
Reading status, replacing a secret or rotating a provider registration cannot
promote changed credentials. The exact operation, receipts and sealed values
must remain current.

An unheld Telegram bot stays idle without a webhook. Its messages are not
received or counted as `account_unclaimed`; a source-less receiver to count them
is not implemented. This is distinct from the pending deployment-HITL inbox
transport.

The default Claude CLI backend instead requires its own credentials, executable and supported workspace setup. Authored doubles do not waive those live requirements.

## Conversation Continuity

The agent declares `memory: true`. Repeated live messages for one `conversation_reference` continue the same conversation; another reference starts a separate conversation. Plain `swarm serve` uses retained state across restart. `serve --dev` deliberately starts a fresh scratch epoch. Public `test` always uses a fresh isolated store and is not a restart test.

In retained serve, losing the ingress credential makes the existing nonterminal standing generation non-executable without deleting its history or operator pause. Reprovisioning and restarting reuses that generation; it does not replay work quiesced during dormancy. A terminal generation still requires the existing explicit reset remedy.
