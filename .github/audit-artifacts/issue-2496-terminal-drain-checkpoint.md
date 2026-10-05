# Retained-Refresh Terminal Shutdown Checkpoint

Issue #2496 / PR #2525. Local source `7b715426e`, following `272d5feb7`.
No push or rebase. This is additive WIP evidence, not final class closure or
merge approval. The complete managed default, refreshed describe provenance
after this spec edit and exact-head CI remain outstanding.

Binding split ruling: issue comment5970914706; LEAD escalation on #2411 at
5970920278. Terminal caller census addendum: issue comment5971150369.
The same-attempt readiness-grant handoff is **not implemented**. Its phase,
request/lost-ack and cleanup rights remain exactly as before, pending explicit
user/LEAD ratification with B coordination.

## Concept And Consumer Census

The changed concept is process-owned terminal disposition of an incomplete
source-set refresh's admission wait, not attachment/grant transfer. Ordinary
incomplete refreshes stay fenced and retryable. A separate terminal-drain
signal does not complete a refresh or authorize any durable write.

| Owner / consumer | Disposition and proof |
| --- | --- |
| `RuntimeContextManager` / `PreparedRuntimeSourceSetTransition` | Existing `sourceSetMu` serializes terminal withdrawal against held preparation, Commit and resume. Pending refresh authority stays retained; terminal retry refuses. The aggregate fences the whole runtime/standing set before releasing drain waiters. Core whole-set/preparation test plus actual native aggregate shutdown/restart matrix. |
| `runtimeSourceSetTransitionAdmission` | Owns distinct completion and terminal-drain signals. Only aggregate `DeactivateAllWithOptions` emits terminal drain. `Done` stays open; post-Commit Abort cannot restore visibility. No durable disposition or second grant ledger. |
| Individual `DeactivateBundleHashWithOptions` | Serializes withdrawal with preparation and refuses a pending whole-set transition until aggregate terminal disposition. It cannot release the shared gate. Core refusal and whole-set tests. |
| Manager `waitForSourceSetTransition` -> delivery admission | A terminal wake returns an explicit process-local error, not success. Pending source-set conflict checks remain active. Existing real EventBus cancellation/dequeued-carrier control remains passing. |
| Manager `waitForSourceSetTransition` -> `releaseLoop` | Only settlement recognizes terminal drain. It retains exact-token rechecks and all native grant checks; it can attempt finalization, not admit execution. Errors enter the existing owning Manager join, not only a log. Native current-grant retirement/restart and exact injected self-release failure controls. |
| Manager / Runtime individual shutdown | One Manager-owned `CheckShutdownAdmission` rejects individual joins before they can wait behind the unresolved shared fence. Native test calls both entrances and requires the exact pending refusal, then uses the real aggregate owner. No second shutdown executor or detached cleanup. |
| Runtime stop / Manager shutdown transition | Existing reserved watcher/executor, accepted work leases and runtime joins remain the owners. Attempt cleanup runs only through existing acknowledgment rules. Native proof verifies exact retired ordinal/plan/phase before releasing process capability, then a new monotonic attachment attempt on restart without repeated construction/creation. |
| Serve `ShutdownProcessWithOptions` | Delete shutdown-only `settlePendingSourceSetTransitionLocked`: terminal shutdown no longer resumes Commit or republishes primary execution. Both registered normal and reset paths delegate to aggregate retirement. Selected-owner retirement order remains unchanged. Terminal possession errors remain explicit. A poisoned `CurrentSourceSet` regression and selected-failure/projection-join controls prove these consumers. |
| `attachPrimaryRuntime` | Kept for ordinary committed startup/reset publication, not terminal shutdown. No restored primary visibility in the terminal owner. |
| Selected-store lifecycle/readiness/request owners | Unchanged. Exact grants, attempt identity, phase and original request evidence still govern mutation/cleanup. The actual adjacent survivor refusal is not fixed by a terminal wake. |

The two Manager wait consumers are individually enumerated; the terminal
signal has one production producer. This does not claim that introducing a
shared signal proves all durable source-set survivor behavior.

## Proof Boundaries

Raw managed receipts are compressed beside this file; uncompressed SHA256s,
package results, named roots and every native backend/field repetition are in
`issue-2496-terminal-drain-qualification.json`. No skips, filtered default
failure, timeout inflation or capacity bypass is qualification credit.

Final source7b715426e: composed race/count3 gives48 root passes, zero failures
or skips, including12 native shutdown/backend/field cells. Runtime220.195s,
Manager7.426s, serveapp2.950s and partition1.381s. Complete Manager package
through `swarm-test`:322 roots PASS, zero FAIL/SKIP,18.168s. Earlier initial
and strengthened matrix receipts are separately source-attributed, not folded
into this head's result. The initial runner invocation omitted its required
`--` separator and exited2 before executing tests; its usage receipt is kept.

The native terminal matrix uses real constructor/headers, eight actors under
two keyed parents, real native SQLite/PostgreSQL lifecycle/readiness owners,
real Runtime/Manager loops and the real aggregate terminal owner. Failed
Commit is deliberately retained **before changing the grant**. It proves
authorized terminal draining and current-rights cleanup independently of the
held survivor amendment. It is not public-launcher/provider qualification and
does not prove post-successor-grant cleanup rights.

Per native cell: retain the failed refresh; reject Manager and individual
Runtime shutdown; post-Commit Abort leaves visibility withdrawn; aggregate
shutdown joins exact resources and acknowledges predecessor retirement;
terminal refresh retry and runtime selection remain unavailable; restart
uses a higher attachment ordinal with the same plan and unchanged creation
timestamp, construction fields/configuration/stage/creation time.

Existing cancellation-only control still retains a dequeued delivery and its
route until the aggregate gate settles. The core aggregate test holds an
accepted request through whole-set fencing and drain wake, requires `Done`
to stay open, rejects retry, and joins the request rather than dropping it.
The self-release failure test preserves the exact injected error in the
Manager work join. Serve regressions prohibit source-set reads/resume in
terminal shutdown, preserve selected cleanup errors, and release constructed
projections only after successful join.

Historical native adjacent-grant refusal and ten-minute cleanup deadlock
receipts remain in `issue-2496-c22-native-*.log.gz`. The grant refusal is still
a live same-PR obligation. The terminal matrix is **not** a rewritten passing
version of that survivor oracle; its separate root and all four mandatory
backend/field leaves are registered in the proof plan/partition test.

## Remaining Closure

The chosen #2496 construction/readiness working class remains open. User/LEAD
must ratify the exact survivor attempt/grant contract with B before such edits.
Afterward: actual changed-source-set native multi-actor survivor proof,
per-actor failure/rollback/unknown-ack/late-callback/cleanup proof, normal and
selected journeys, fresh describe and full managed default, then the required
conflict-only rebase/batched push and exact-head CI.

No new framework, vendor package, compatibility owner, predecessor-write
permission, replay capability or durable disposition was added. Parents
#2411/#2250 remain open; #2497 and A/B ownership remain separate. This bounded
repair is promoted inside #2496, not deferred as a follow-up. The underlying
attempt-versus-generation smell stays in the explicit LEAD/B amendment; no
unapproved estimate or closure claim is substituted for that decision.

Measured on source7b715426e versus e63:232 non-test Go paths,8430 additions/
7983 deletions, net+447, including moved/generated/test-support source. The
readiness core stays387 lines. Complexity >=30 gates pass (cognitive573->570,
cyclomatic264->260); disclose >=50 increases (cognitive193->194, cyclomatic
53->55), not uniform improvement. The new invariant is explicit aggregate-only
terminal disposition of a refresh gate without conferring execution authority
or dropping exact accepted work. Measurement is not semantic closure.
