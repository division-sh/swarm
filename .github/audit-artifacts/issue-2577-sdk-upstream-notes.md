# SDK Upstream Diff And Reproducer Notes

Technical evidence for the user's human-written upstream PR and Matrix
coordination. This is not that PR, not its proposed human description and not a
new SDK implementation or vendored copy. The user owns submission. Both fixes
must be represented; Swarm's authorized fork is not the upstream merge condition.

## Exact Reviewed Diff

Base: `tulir/whatsmeow@68df7a5ec7b5`.
Signed fork: `division-sh/whatsmeow@740de549008b7347aff194b4d2dc9c036602f582`.
Topic branch: `agent-f/2577-response-waiter`.

[Complete base-to-fork diff](https://github.com/division-sh/whatsmeow/compare/68df7a5ec7b5...740de549008b7347aff194b4d2dc9c036602f582):

| Commit | Production ownership repair | Permanent SDK proof |
| --- | --- | --- |
| `01ff3e9474e854b9f51787448e6e87d3c4b05024` | `socket/framesocket.go` and `socket/noisesocket.go`: private synchronized callback, exported `SetOnDisconnect`/`GetOnDisconnect`, exact callback selected once outside callback execution. | `socket/disconnect_test.go`: suppression before selection, selected callback surviving late suppression, enabled remote close, at-most-once close, reentrant stop, concurrent initialize/clear/close, actual remote read pump. |
| `63a587ffb4634a3b682fb5e5e7047cf8f1ef8001` | No production change; introduce deterministic response-waiter regression first. | `request_test.go`: receiver-selection interleaving and complete ownership matrix. |
| `740de549008b7347aff194b4d2dc9c036602f582` | `request.go`, six additions/two deletions: cancellation removes/closes only its exact still-registered waiter under the existing lock. | Same unchanged regressions turn green; stale cancellation cannot remove a replacement, selected response survives late cancellation/disconnect. |

The callback accessor replaces the unsafe exported `OnDisconnect` field. Callers
assign through the setter; the snapshot accessor must not be mistaken for holding
the callback lock. A selected callback remains selected even if another caller
suppresses later selections. Invoke it without holding the new mutex so reentrant
stop works. This is an intentional exported API change, not a compatibility alias.

For responses, removal from the waiter map transfers ownership to delivery or
disconnect cleanup. Cancellation can no longer close that transferred channel or
delete a newly registered waiter with the same request ID. Preserve existing
result/error/cancellation behavior; do not add retries, deadlines or detector
suppression. The second repair adds no exported API or wire-format change.

## How The Deterministic Reproducer Works

`TestResponseCancelAfterReceiverSelection` uses a context whose `Done` method
blocks select-operand evaluation after `receiveResponse` has removed its waiter.
While delivery is paused, cancel the original request, then release the receiver.
The old implementation closes the removed channel, so delivery panics with
`send on closed channel`; the new exact-owner check leaves delivery's channel
alone and the original response arrives. The test joins the receiver and checks
the response, not only absence of a panic.

Additional cases: cancellation-first, response-first, repeated cancellation,
stale cancellation preserving a replacement, canceled old waiter preserving a
replacement, disconnect-then-cancel, a full waiter cleared once, and selected
waiter versus replacement with/without disconnect. Callback tests include the
real WebSocket read pump, not only direct callback assignment.

SDK command at the reviewed fork, actual Go1.26.8:

```sh
GOWORK=off GOTOOLCHAIN=go1.26.8 go test -race . ./socket \
  -run 'Test(Response|FrameSocketDisconnect)' -count=1
```

Use an isolated SDK worktree for historical comparison, with the preserved
reproducer appropriate to that API. Never edit the module cache, suppress race
reports, skip tests or copy SDK production sources into Swarm. Old callback
fixtures using the unsafe field and new setter fixtures are distinct API
contexts; failure to compile a new setter fixture on the old API is not a race
reproduction. The response regression itself is SDK-API neutral and can run on
the old base with its unchanged test source.

## Retained RED And GREEN Evidence

These are historical, pinned receipts, not newly rerun or physical phone evidence:

- Reviewer-f accepted the exact callback fork in #2577 comment6074737709 after
  the original shutdown/logout/pairing failures turned green.
- Deterministic old response pin: RED0.015s; old ownership matrix RED0.017s.
  New owner matrix PASS1.028s; callback matrix PASS1.041s on Go1.26.8.
- Swarm actual TLS/Noise result/error/cancellation and deterministic handoff:
  both-store GREEN25.929s; the same old-pin handoff RED10.506s.
- Send/logout/remote-close/lost-result/no-replay/drain PASS25.359s;
  shutdown/pairing/direct-QR PASS12.599s. These controlled peers establish
  lifecycle integration, not WhatsApp-service interoperability.
- Reviewer-f accepted the response ownership repair at `c5663f064`, #2577
  comment6103030735, after independently checking old RED versus new GREEN.

Original artifacts and technical notes are retained privately under
`/home/youmew/dev/swarm/agent-f-2577-prework/`. Use their original pins and hashes;
do not substitute current-head timing or claim a fresh native union receipt.

## Exclusions And Exit Condition

No SDK QR-buffer patch is included. Swarm uses direct public QR/ADV events under
its own bounded joined lifecycle owner; the unused helper's diagnostic and old
RED evidence are retained separately. Swarm callbacks, capture persistence,
business admission, journals and the private-store fence are not upstream SDK
changes. No migration, backfill, vendoring, copied SDK implementation, timeout
increase, uncertain resend or test weakening belongs in this diff.

After the user opens the upstream PR, put its real URL into the one Swarm
`replace` comment. Remove the replacement only after an exact upstream re-pin
contains both reviewed repairs and the SDK/Swarm adverse race matrices pass.
An upstream PR covering only one fix, a fork-only approval or a healthy physical
sample does not satisfy that exit condition.
