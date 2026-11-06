# WhatsApp Physical Qualification

## Scope And Current Evidence

This guide supersedes its old #2614 source-pin instruction. Use the
[phone-only sitting runbook](https://github.com/division-sh/swarm/issues/2577#issuecomment-6454704112)
with the current command and limitation corrections below, not the older
6243293343 packet. Steps 1-3 and 7-10 are retained. Steps 4-6 and all their
repeats remain deferred under
[6517309283](https://github.com/division-sh/swarm/issues/2577#issuecomment-6517309283):
no deployment-HITL Inbox, group confirmation, or channel notice/ACK credit.

[Historical automated evidence](https://github.com/division-sh/swarm/issues/2577#issuecomment-6641724024)
is pinned to clean master `5ca103d649d672ae76fea75f69d50c20d18bb954`,
Go 1.26.8 linux/amd64 and WhatsApp fork `740de549008b`. On server2, all 47
retained roots passed: 453 roots / 2,536 terminal leaves overall, zero failed,
skipped or incomplete. The current emitted plan maps all 47 roots; this does
not claim the complete units, all master tests, or any physical acceptance.
Older failures and receipts retain their original source and scope.

The November 5 sitting stopped at an incoming callback during the unapproved
linked-account preview; see the
[source/stop record](https://github.com/division-sh/swarm/issues/2577#issuecomment-6667466070).
No operator/business/reset/next-day physical credit follows from the linked
preview. Read the [transition coverage map](issue-2577-sitting-transition-coverage.md)
with its exact-head qualification packet; a green root count alone is not READY.

The [retained-preview decision](https://github.com/division-sh/swarm/issues/2577#issuecomment-6673738657)
requires the next explicit same-account connect to restore an unapproved preview
without another QR or implicit approval. The [approval bound](https://github.com/division-sh/swarm/issues/2577#issuecomment-6673799042)
requires the existing affirmative answer to be carried on retry; resume alone is
not that answer. These source repairs and their merged-head preparation must be
cleared before invitation. Preserve the retained pairing; neither reset mode nor
an unapproved logout is a qualified substitute for this re-entry.

The current-source binary passed CLI help, portable verification of the existing
authored-reply fixture, and public scaffold generation without starting a QR
operation. This is not a rehearsal with the private device fixture. A development
binary's unknown version/commit metadata must not be reported as release metadata.
Record the actual source, binary checksum, SDK pin and command transcript before
invitation; a later build does not inherit these executed receipts by ancestry.

## Before Invitation

**NOT YET SITTING READY:** approved private fixture input, consent and exact
staging/rehearsal remain required. The operator performs all terminal work; the
user types no Swarm commands or IDs. No live debugging is allowed in the sitting.

- Obtain the privately approved disposable linked account, independent operator,
  and two customer identities. Do not infer or adopt an existing session.
- Confirm consent for the unofficial client, private unencrypted session state,
  planned disconnect/crash, both resets and explicit logout. Never use a
  production account as a cleanup target.
- Stage the exact deployment, source, account selection, API token file and
  authenticated RPC helper. Keep HOME/config/state isolated and token mode 0600.
  No test-only environment variables belong in the device commands.
- Verify the intended account in source `policy.operator_account` and, where
  selected, deployment `channels.hitl.account`. Record the exact source bundle,
  holder/interface and operation revisions from public readback, never a first
  account or guessed target. Rehearse the complete private command sequence.
- Separate pairing, operator identity and business activation. With the exact
  configured deployment HITL account, `channel connect whatsapp` itself owns the
  account preview/approval, operator challenge and claimant confirmation. Do not
  insert an obsolete second runtime enrollment. Serve then adopts the registered
  account for the prepared authored source; pairing alone grants no operator role.
- Complete the sender observation below before claiming ceremony conformance.
  A synthetic peer or successful QR scan cannot establish that fact.
- Check the quiet-host envelope, exact owned PID/socket, safe stop/join and
  cleanup privileges before invitation. A missing prerequisite means NOT READY.

Publish **SITTING READY** on #2577 only after the retained automated checks and
preparation gates are satisfied. Keep actual device and next-day results NOT RUN
until observed. The sitting budget remains 16 minutes plus at most 4 next day,
20 minutes total; unfinished cells at either hard stop remain NOT RUN.

## Delivered Identity And Beta Limits

[The identity decision](https://github.com/division-sh/swarm/issues/2577#issuecomment-6596896164)
uses the application's ceremony: bind the delivered `external_account` to a role
once and route by that opaque ID, not by a phone number. It authorizes no new
platform field, phone lookup or normalization. Stability is **UNKNOWN** until the
sanctioned observation establishes it for the approved sender and supported cuts.

Current `internal/sessionprovider/incoming.go` takes `Info.Sender.ToNonAD()` at
line 52 and writes that exact value as `external_account` at line 74. Lines
101-103 accept both phone-number and LID servers; this path does not resolve
`SenderAlt`. Source inspection alone does not prove cross-kind ID stability.

For the same approved sender, retain privately the exact delivered ID, event
kind and source/message reference for supported ordinary text, quoted reply,
edit and revoke, including the planned reconnect comparison. Compare exact
values before a ceremony-conformance claim; do not merge or normalize PN/LID
forms. Unsupported kinds are NOT RUN, not added support. If a mismatch is
observed, stop the claim and return the evidence to the user on #2577; do not
build a mapping or change identity policy during the sitting. Publish only the
outcome and supported-kind coverage, never raw IDs. This adds no group, media,
Inbox or next-day qualification and no permission to expose a phone session.

[The beta takeover decision](https://github.com/division-sh/swarm/issues/2577#issuecomment-6596893781)
requires no platform takeover mechanism. Linked-device/self echoes are discarded
(`incoming.go:89`); flows receive no event reporting the owner's typing. The
owner can read chats in their own app, but manually replying there does not
automatically pause the receptionist. Intervention is through the integrating
application's owner-agent relay. This is a limitation to disclose, not evidence
that this starter implements or qualifies that escalation path.

## Current Command Forms

All variables and the existing authenticated `p_rpc` wrapper must be privately
bound and rehearsed before invitation, after changing into the deployment
directory. These are individual step forms, not an unattended script. Each
request JSON freezes the exact observed source/interface/operation/revision and
a distinct intended idempotency key; no mutable or guessed selector is allowed.

```sh
# Deployment registration, with serve stopped for this sitting's first pairing.
"$BIN" --config "$CONFIG" --swarm-dir "$STATE" channel connect whatsapp --no-save
"$BIN" --config "$CONFIG" --swarm-dir "$STATE" serve "$SOURCE" \
  --api-token-file "$TOKEN" --workspace-backend host --no-color --verbose
"$BIN" --config "$CONFIG" --swarm-dir "$STATE" channel list --json
"$BIN" --config "$CONFIG" --swarm-dir "$STATE" channel status whatsapp "$ACCOUNT" --json

# Read back the exact operation retained by deployment connect when needed.
p_rpc channel.onboarding_get "$OPERATION_REQUEST_JSON"

# Exact public readbacks and the separately consented unbind/reset/logout cuts.
p_rpc run.list "$EXACT_BUNDLE_FILTER_JSON"
p_rpc event.list "$EXACT_RUN_FILTER_JSON"
p_rpc channel.unbind "$UNBIND_REQUEST_JSON"
p_rpc runtime.nuke "$RESET_PREVIEW_JSON"
p_rpc runtime.nuke "$RESET_APPLY_JSON"
"$BIN" --config "$CONFIG" --swarm-dir "$STATE" channel logout whatsapp "$ACCOUNT" --json
```

Prepare separate preview/apply bodies for retained-source and cleared-source
resets; do not reuse a destructive request or treat a preview as an apply.
There is no `swarm reset` command. Resume takes the exact durable operation ID,
not an account ID. Fresh deployment connect has no account argument; selecting
an existing registration uses the exact observed positional account.

Stop only the retained owned serve PID with TERM and checked wait. The planned
crash/socket cuts require the already verified exact PID/socket and consent;
never use broad process matches, a host-wide firewall or private-row repair.
Any wrong audience, missing output, refusal, timeout, uncertain/pending teardown,
duplicate effect, lost ownership or failed state assertion is **FAIL -> STOP**.
Retain private evidence and mark later cells NOT RUN. No config/code edits,
extra resume, retry-to-green, session deletion or debugging during the sitting.
Retain pairing unless an explicitly requested public logout completes; never
fabricate cleanup success.

## Retained Residuals

The paired PostgreSQL resume timeout remains the explicitly accepted residual
under [6198119875](https://github.com/division-sh/swarm/issues/2577#issuecomment-6198119875).
A recurrence reopens #2577 and blocks the next release; do not raise the bound,
retry or skip it. The current passing 0.31s PostgreSQL control does not explain
the historical failure. Keep this separate from retained pre-QR failures.

Under the [user amendment](https://github.com/division-sh/swarm/issues/2577#issuecomment-6223240045),
the human-written upstream SDK PR is a user-owned follow-up due 2026-11-28, not
a #2614 landing condition. Preserve the single pinned-fork replace line and its
exit-condition comment until both fixes are upstream. No vendoring is authorized.
#2577 remains open for the upstream follow-up and replace-line exit.

After the sitting, refresh #2852's final release inventory with the actual
observations and remaining limitations, not a physical PASS inferred from mocks.
