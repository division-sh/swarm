# WhatsApp Physical Qualification Runbook

This is the pending P01-P09 acceptance plan for #2577/#2614, not a completed
receipt or a claim of supported WhatsApp interoperability. Account-free SDK
peers and served tests remain separate evidence. Use the exact final candidate
after its compiler prerequisite has independently qualified.

## People, Devices And Consent

- A disposable, already registered mobile SIM/eSIM WhatsApp account for Swarm.
  Do not use a personal or production business account. The phone retains the
  primary registration; Swarm links as a companion, not by receiving an SMS code.
- An independent operator account, two customer accounts, an unauthorized
  participant, and a disposable group. State any role sharing explicitly; the
  operator-versus-customer negative must use distinct identities.
- A second screen to scan the Linked Devices QR. Record phone OS, WhatsApp
  version, number/account type, UTC date and region privately, without the number.
- Consent to an unofficial client and possible account restriction, v1
  unencrypted session files in a restrictive bridge-owned directory, controlled
  network loss, graceful/crash restart, reset, unbind and explicit logout/re-pair.
- Reserve about 90 minutes and a next-day 15-20 minute continuity check. No
  verification code, API token, QR, pairing key or session file goes into chat,
  GitHub, a screen recording or an online QR renderer.

F operates Swarm and collects redacted machine evidence; the user performs the
phone actions. Each destructive action needs the user's immediate confirmation.
Do not start a destructive step while the previous effect outcome is uncertain.

## Exact Setup

Record Swarm commit, binary SHA256, actual `go version`, source artifact hash,
selected backend, schema identity, and SDK replacement
`740de549008b7347aff194b4d2dc9c036602f582`. Keep the full private coordinates in
restricted local evidence; publish only stable aliases and safe receipt IDs.

Use one Swarm installation, one persistent disposable selected store and its
ordinary authenticated control API. Bind the API to loopback or an explicitly
secured operator route. Use `--api-token-file`, not a token in command arguments
or an invented environment variable. There is no WhatsApp webhook, managed
tunnel, provider token, sidecar or separate bridge deployment.

Admit the unchanged public source before connecting. The existing
`internal/serveapp/testdata/whatsapp-session-reply` source is an authored-reply
control; `whatsapp-session-input` and `whatsapp-session-notice` provide the
card/input and notice journeys. Do not change declarations after admission or
hand-copy session/binding rows between sources. Use fresh public connect or
source admission for each actual target. A bare echo is not evidence of separate
agent memory; P03 must separately inspect the declared conversation histories.

For initial native pairing, F uses the ordinary authenticated public
`channel.onboarding_start`/`channel.onboarding_get`/`channel.onboarding_retry`
responses and retains the returned operation ID privately. The human CLI may
stop at a pending pairing phase; these examples do not claim it renders a QR.
Do not invent an operation ID or read it from private persistence.

Public control examples, with private values supplied locally:

```sh
swarm channel connect whatsapp --source <admitted-directory> \
  --api-server <operator-origin> --api-token-file <private-token-file>
swarm channel list --api-server <operator-origin> \
  --api-token-file <private-token-file> --json
swarm channel resume <operation-id> --api-server <operator-origin> \
  --api-token-file <private-token-file>
swarm channel logout <operation-id> --api-server <operator-origin> \
  --api-token-file <private-token-file> --json
```

Pairing material comes only from authenticated `channel.onboarding_get` for the
exact current operation. F displays it locally through a trusted offline QR
renderer without recording its bytes; the renderer grants no account or
principal authority. Expired/replaced/canceled material is discarded. Do not
invent a provider credential, caller principal, receipt or activation to advance
the ceremony. Pairing, Connected, principal claim, human confirmation and READY
are separately observed facts.

## Finite Journey Matrix

| ID | Phone action and supported-path check | Required evidence |
| --- | --- | --- |
| P01 | Start declared unpaired connection. Let a QR expire, obtain the current replacement through authorized readback, and link it in Linked Devices. | Expired/foreign readback refused; exact current account paired; actual Connected observed; pairing alone is not READY or human identity. |
| P02 | Independent operator sends the exact live challenge and completes the existing confirmation. A customer sends ordinary text during setup and cannot claim controls. | Exact operator binding and confirmation receipt; no first-sender auto-bind; subsequent valid claim progresses; READY names the admitted target and live health. |
| P03 | Two customers alternate uniquely labeled messages to the same linked account, then one sends a genuine reply to an earlier message. | Separate conversation-scoped histories/receipts, correct destination for every response, original reply reference retained, no cross-customer memory or reply. A forward is not a genuine reply. |
| P04 | Operator sends `/inbox` before relying on a previous delivered card. Exercise a text card action, required input/Skip, and a quoted `Acknowledge` notice reply. | English public mailbox/card state matches the actual messages; processing, not only transport receipt, completes exactly once; optional unsupported controls return typed refusal, not fabricated buttons/menu/ACK. |
| P05 | Explicitly select the disposable group as delivery destination and read its existing audience disclosure. Other member tries a quoted action; operator tries an obsolete or foreign quote. | Confirmed shared-audience disclosure; unauthorized/stale/foreign mutations refused with unchanged authoritative state. Group visibility is not operator permission. |
| P06 | Complete graceful restart, then actual process death and restart at an agreed launched/unlaunched boundary. Repeat Inbox/readback and customer input. | Pairing and immutable receipts retained; current occurrence re-admitted; exact prior outcome evidence survives; no duplicate effect or automatic resend. |
| P07 | Lose the actual connection and exercise authorized explicit recovery. Include permanent unlink only on explicit consent. | Honest disconnected/not-ready and actionable fallback; enabled responsibility retained; uncertain effect not replayed; recovery requires fresh admitted authority. |
| P08 | Unbind, explicitly reconnect using its retained fence, run both public reset modes/source retirement, then explicitly logout and re-pair. | Nondestructive actions fence/join but retain pairing/history; fresh claim/confirmation after unbind; explicit logout alone unlinks/deletes exact private state; no surviving executable authority or successor deletion. |
| P09 | Inspect installation, final artifact/binary/SDK pin and next-day continued operation. | One installation, no extra runtime service, full private provenance with redacted public summary, actual phone continuity and final readiness. |

Use only supported public controls and ordinary real messages. Raw SQL, fake
subscriber rows, internally fabricated mutation, raw webhook substitution and
test peers cannot complete a physical row. Existing both-store automated proofs
remain required; running one real number against one backend does not grant
unrun backend proof credit. Do not link competing connections to the same account.

## Receipt And Stop Rules

For each row record candidate/binary/source/pin, backend, UTC start/end, actor
aliases, exact action, safe operation/effect/receipt IDs, expected and observed
authoritative state, result and the private evidence location. Mark each row
PASS, FAIL or NOT RUN; never turn a healthy repeat into causal closure.

On failure preserve the original terminal result, phase and before-cancel
diagnostics without message bodies, phone numbers, QR or session bytes. Stop the
affected journey; do not retry, raise a deadline, clear a row, delete history or
adopt replacement authority to obtain green. The open #2353 PostgreSQL 5s resume
entry observation remains separate and needs causal resolution or explicit
LEAD/user landing-risk disposition. Physical success does not waive it.

After the session, perform the agreed explicit logout, verify Linked Devices and
public retirement/readback, and remove disposable local evidence only after all
work joins and the useful redacted receipts are retained. No production account
or source is an acceptable cleanup target.
