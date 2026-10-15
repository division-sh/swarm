# WhatsApp Physical Qualification Runbook

**NOT READY: preparation checkpoint, not the user's final sitting packet.**
The current publishing base is master `7956d66def`, including #2691/#2692.
The bounded P01 readback repair is signed `6b2bf8e0a7` under
[#2577/6198923816](https://github.com/division-sh/swarm/issues/2577#issuecomment-6198923816).
The final issue
comment must bind the complete commit/tree, supplied binary SHA256, actual Go,
SDK pin, clean preflight, hosted run/attempt/selection and reviewer-g approval.
Do not ask the user to sit before those non-user holds are closed.

The first live unpaired rehearsal rendered an authenticated current QR through
an offline terminal tool, but its later expiry readback failed with
`unclassified_runtime_error`. That original RED is retained under
[#2577/6198554641](https://github.com/division-sh/swarm/issues/2577#issuecomment-6198554641),
not relabeled as a pass. At `6b2bf8e0a7`, all four deterministic SQLite/PostgreSQL
cuts pass under race x3, with strict execution-health and scope negatives. The
real pinned binary rendered a current QR in a private terminal, then a single
read-only check after the normal unscanned window returned qr_expired, no code,
paired=false, connected=false and the same operation. Public list remained
unbound/not READY; shutdown joined with exit zero. This is agent-feasible
rehearsal, not physical P01 or interoperability. No account or phone has paired.
Fresh final-head preflight and normal hosted qualification remain required;
the first repair-head preflight correctly caught the changed startup consumer
fingerprint, which is refreshed with explicit owner/caller/proof accounting.
Reviewer-g then found a duplicate-disconnect lifetime gap at `854bef93bd`
([6199462678](https://github.com/division-sh/swarm/issues/2577#issuecomment-6199462678)).
The successor must pass its both-store first/second-event and irreversible
retirement controls, fresh affected proofs/preflight and a new exact-head QR
rehearsal. Earlier passing single-disconnect receipts do not approve it.
The deterministic notice-producer/source-switch packet is also awaiting the
bounded runbook review requested in
[#2577/6198521524](https://github.com/division-sh/swarm/issues/2577#issuecomment-6198521524).
No undeclared model credentials, fake receipts or hidden operator setup may
stand in for that packet. These gaps must be removed before final readiness.

This is the pending P01-P09 acceptance plan for #2577/#2614, not a completed
receipt or a claim of supported WhatsApp interoperability. Account-free SDK
peers and served tests remain separate evidence. The Go 1.26 prerequisite
(#2622) has merged; use the exact reviewed final provider candidate, not an old
draft or an account-free test binary.

## People, Devices And Consent

The amended [user condition](https://github.com/division-sh/swarm/issues/2577#issuecomment-6198061713)
bounds G's rehearsal to exact-head pinned-SDK/fake-peer proofs and this runbook
through actual terminal QR rendering and the pairing wait. A separate rehearsal
phone/account/operator is optional, not a preparation prerequisite. Unless the
user supplies one, the user's device is the first real device; G is on live
standby during the sitting. No fake-peer receipt is phone interoperability.

- A disposable, already registered mobile SIM/eSIM WhatsApp account for Swarm.
  Do not use a personal or production business account. The phone retains the
  primary registration; Swarm links as a companion, not by receiving an SMS code.
- An independent operator account, two customer accounts, an unauthorized
  participant, and a disposable group. State any role sharing explicitly; the
  operator-versus-customer negative must use distinct identities.
- A second screen to scan the Linked Devices QR. Record phone OS, WhatsApp
  version, number/account type, UTC date and region privately, without the number.
- Consent to an unofficial client and possible account restriction, v1
  unencrypted session files in a restrictive provider-owned directory, controlled
  network loss, graceful/crash restart, reset, unbind and explicit logout/re-pair.
- Reserve about 90 minutes and a next-day 15-20 minute continuity check. No
  verification code, API token, QR, pairing key or session file goes into chat,
  GitHub, a screen recording or an online QR renderer.

G now owns the implementation continuation and redacted machine-evidence
collection under [F's handoff](https://github.com/division-sh/swarm/issues/2577#issuecomment-6188033199)
and [reviewer-f's confirmation](https://github.com/division-sh/swarm/issues/2577#issuecomment-6188070641).
The user performs the phone actions. This transfer does not authorize the
physical session or publication of #2614. Each destructive action needs the
user's immediate confirmation. Do not start a destructive step while the
previous effect outcome is uncertain. F's source-pinned receipts and private
evidence remain in their original locations; ownership transfer does not rebind
those receipts to G's current candidate.

## Exact Setup

Before the user arrives, G supplies the final pinned binary and prepares the
private deployment, token file and offline renderer. Use two terminals on the
same host. The following exact preparation was exercised on vemew; the final
issue comment must bind it to the then-final binary and candidate, not reuse a
provisional hash as new evidence:

```sh
REPO=/home/youmew/dev/wt/agent-g/main
QUAL=/home/youmew/.local/state/agent-g-2577-readiness
BIN="$QUAL/swarm"
API=http://127.0.0.1:19381
SOURCE="$REPO/internal/serveapp/testdata/whatsapp-session-reply"
umask 077
set +x
install -d -m 700 "$QUAL/deployment/state" "$QUAL/home" "$QUAL/xdg"
```

The explicitly selected private state base MUST already exist, be owned by the
operator and have mode 0700. The first rehearsal omitted it: the directory owner
correctly refused asynchronous session construction with a missing base. The
missing/prepared-base race control passed, and ordinary restart after the setup
correction progressed the same retained operation; the original failure stays
recorded in [6198462840](https://github.com/division-sh/swarm/issues/2577#issuecomment-6198462840).
Do not weaken permission/symlink checks, delete retained history, or invent a
new operation to conceal that failure.

The prepared `$QUAL/deployment.yaml` for the model-free authored-reply source is:

```yaml
runtime:
  recovery_on_startup: true
workspace:
  backend: host
  allow_exec_on_host: true
store:
  backend: sqlite
  sqlite:
    path: /home/youmew/.local/state/agent-g-2577-readiness/deployment/store.sqlite
llm:
  backend: anthropic
```

The reply source has no agent and makes no paid model call. This config alone
does not complete the separate notice-producer setup. G creates the LOCAL
control token once before the sitting, without displaying it:

```sh
openssl rand -hex -out "$QUAL/api-token" 32
chmod 600 "$QUAL/api-token" "$QUAL/deployment.yaml"
```

That is a Swarm control credential, not a WhatsApp account/provider token.
Never overwrite it during a live session or ordinary restart. In Terminal A:

```sh
env -i PATH="$PATH" HOME="$QUAL/home" XDG_CONFIG_HOME="$QUAL/xdg" \
  "$BIN" --swarm-dir "$QUAL/deployment/state" serve "$SOURCE" \
  --config "$QUAL/deployment.yaml" --api-token-file "$QUAL/api-token" \
  --api-listen-addr 127.0.0.1:19381 --mcp-listen-addr 127.0.0.1:19382 \
  --workspace-backend host --no-color --verbose
```

Expect all 22 phases through `ready`, the exact SQLite path, loopback listeners
and `DORMANT` / `session admission required`. This is healthy unpaired serving,
not a READY channel. Do not use `--dev`: physical pairing must persist. Do not
add `--no-feed` without `--dev`; serve correctly refuses that combination.
Stop on a boot failure. In Terminal B define the authenticated public command:

```sh
channel() {
  env -i PATH="$PATH" HOME="$QUAL/home" XDG_CONFIG_HOME="$QUAL/xdg" \
    "$BIN" --config "$QUAL/deployment.yaml" \
    --swarm-dir "$QUAL/deployment/state" channel "$@" \
    --api-server "$API" --api-token-file "$QUAL/api-token"
}
START=$(channel connect whatsapp --source "$SOURCE" --no-save --json)
OP=$(printf '%s' "$START" | jq -er '.operation.operation_id')
unset START
```

The first result may be `activating_provider` / `preparing`, not READY. After
that preparation step, explicitly continue the SAME operation once; do not
introduce a polling or reconnect loop:

```sh
PAIR=$(channel resume "$OP" --json)
printf '%s' "$PAIR" | jq -e '.pairing.status == "awaiting_qr" and
  (.pairing.code|length>0) and .pairing.paired == false and
  (.readiness.ready != true)' >/dev/null
EXPIRES=$(printf '%s' "$PAIR" | jq -er '.pairing.expires_at')
test "$(date -u -d "$EXPIRES" +%s)" -gt "$(date -u +%s)"
```

G prepares Debian `qrencode=4.1.1-2` and `libqrencode4=4.1.1-2` offline tooling
under `$QUAL/qr-tool/root`, not in Swarm's repository or dependencies. The
executable SHA256 is
`1046d4adc8878d6352704e8ae18a2d6822763eb1db639b58e3770829081f9fd1`;
the library SHA256 is
`13f66b57154db818c5f15b268a73b8e2f97146850315d0c1fde2141eb20cf4b5`.
In Terminal B, with recording disabled and enough columns to avoid wrapping:

```sh
printf 'Scan in WhatsApp > Linked Devices > Link a device. Not READY yet.\n'
printf '%s' "$PAIR" | jq -jr '.pairing.code' |
  env LD_LIBRARY_PATH="$QUAL/qr-tool/root/usr/lib/x86_64-linux-gnu" \
  "$QUAL/qr-tool/root/usr/bin/qrencode" --type UTF8 --margin 4
unset PAIR
```

Expect a square black/white QR with three large corner finder marks and a clear
margin. The code travels through stdin, not process arguments, files or a web
renderer. No QR bytes or terminal recording belong in the receipt. G's
unattended rehearsal used an owned PTY with output/recording discarded; the
user sees Terminal B directly. The unscanned-expiry control currently exposes
the OPEN readback failure above: do not claim successful expiry/renewal yet.

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

For initial native pairing, use the ordinary authenticated public CLI or
`channel.onboarding_start`/`channel.onboarding_get`/`channel.onboarding_retry`
responses and retain the returned operation ID privately. Human/JSON CLI output
can disclose the current pairing material and expiry; it does not render a QR
bitmap. Quiet output contains only the operation ID. Preparing, awaiting QR and
paired progress remain nonterminal and not READY. Advance once with explicit
`resume`; do not add automatic polling or reconnect. Do not invent an operation
ID or read it from private persistence.

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

Pairing material comes only from authenticated current-operation readback,
including the public Start/Get/Retry responses or human/JSON CLI output above.
G displays it locally through the pinned offline terminal renderer above without
recording its bytes; the renderer grants no account or principal authority.
Expired/replaced/canceled material is discarded. Do not
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
adopt replacement authority to obtain green. The served paired-PostgreSQL
five-second pre-handshake timeout is an **ACCEPTED RESIDUAL, not a landing hold**,
under the [user-ratified disposition](https://github.com/division-sh/swarm/issues/2577#issuecomment-6198119875).
Its signature is failure to observe the controlled peer's SDK frame within the
original window; the historical focused group was RED178.976s with no failing
stack retained. Synchronous private initialization/durability remains an
unconfirmed hypothesis. Later healthy samples do not establish causal closure.
Any recurrence of this exact signature, local or hosted on any branch, reopens
#2577 and blocks the NEXT RELEASE until a bounded causal RED-to-GREEN repair
lands. No timeout increase, retry-to-green, skip or diagnostic-budget reset is
authorized. A different phase/stack or SQLite manifestation is a separate
finding, including the currently OPEN real unpaired-QR readback failure. The
physical acceptance and human-written upstream SDK PR remain separate user
requirements; neither is claimed complete by this runbook.

After the session, perform the agreed explicit logout, verify Linked Devices and
public retirement/readback, and remove disposable local evidence only after all
work joins and the useful redacted receipts are retained. No production account
or source is an acceptable cleanup target.
