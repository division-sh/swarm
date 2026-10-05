# C22: Native Readiness Survivor Rebind Needs A Disposition

Issue #2496 / PR #2525. Local batch above `1df3a2366`, base `e63f4bdb1`;
not pushed, not qualified, no merge approval. Preserve the C22 approval in
issue comment5969425820 and the frontier-consumer addendum5969942426.

## Measured Counterexample

Complete normal construction attaches eight exact keyless actors under two
keyed parents. The aggregate source-set transition retains those exact actors,
issues an admitted adjacent generation, then calls the existing
`PreparedDurableTopologySourceSetRebind.Commit`. The selected store refuses:
`agent_topology_authority_conflict: readiness_activation_attempt_not_current`.

The readiness row still binds its accepted activation attempt to the predecessor
grant. `agentpersistence.authorizeFlowReadinessMutation` requires that grant to
equal the successor lifecycle request's grant, even for `source_set_rebind`.
The Manager-only survivor control uses a persistence probe and cannot prove
this native selected-store transition.

Native aggregate receipt:
`/tmp/agent-e-2496-c22-native-source-set-admitted-fixture.log`.
SQLite fails at the rebind, then cleanup waits on the retained source-set fence.
The unchanged default Go test timeout terminates this diagnostic at ten minutes;
this is RED, not a passing cancellation/cleanup proof. No timeout was increased.
The stack shows `releaseLoop -> waitForSourceSetTransition -> admission.Done`,
with runtime retirement waiting for outstanding execution leases. The failed
aggregate transition deliberately keeps its admission fence for retry; Abort
after Commit does not settle it.

A separate stopped-cell native control isolates the durable admission boundary
without live-loop shutdown. Both SQLite and PostgreSQL produce the identical
named refusal in `TestAgentLifecycleReadinessSourceSetRebindBothStores`.
It uses constructor-owned header/entry/receipt preparation, exact stopped-cell
admission and a real admitted adjacent grant. It does not claim a complete
source-set mutation, executable actor journey or aggregate cleanup proof.
Package RED1.804s; both cells fail in the lifecycle transaction.
Receipt: `/tmp/agent-e-2496-c22-native-readiness-adjacent-binding.log`.

## Boundary And Requested Ruling

This is another consumer of the approved construction/readiness authority, but
its repair must settle the exact grant/attempt contract before coding. C22
requires real source-rebind preservation and forbids redefining generation or
lifecycle semantics; B's generation authority remains separate.

Recommended bounded direction for independent disposition:

1. Existing accepted readiness resources survive a same-process aggregate
   refresh without reconstruction, epoch changes or attachment replay. Permit
   one exact store-owned handoff of the accepted attempt's grant only when the
   native transaction proves the predecessor/successor process, boot, runtime,
   source and adjacent-generation relationship, unchanged plan/attempt and
   current committed source-set successor. The same lifecycle transaction must
   bind the actor; failures roll back both pieces. Exact replay is idempotent.
2. Preserve the settled retry rule: resource cleanup requires acknowledged
   predecessor abandonment and a new attempt at `planned`. Do not use this
   no-cleanup handoff to resume abandoned attempts or admit foreign generations.
3. Define failed aggregate refresh retirement explicitly: retain the fence and
   owner for retry during normal operation, but an owned terminal shutdown must
   settle the fence/accepted work and join its loops without restoring visibility
   or granting stale execution. The current indefinite self-finalizer wait is
   not cleanup evidence. No detached cleanup, timeout inflation or admission
   bypass is proposed.

Reviewer-e: rule whether this exact no-cleanup handoff and terminal disposition
complete the existing C22/R5.1 contract, or need a LEAD cross-record with B's
authority. No changes to these production owners have been made on assumption.

## Other Work And Honest Closure

The four complete packages (`manager`, `runforkadmission`, `runforkreadiness`,
`runforkexecution`) pass their composed normal tests. Selected execution
125.216s; its real HTTP/activity cases also pass on both stores. The complete
constructed actor projection and real nested selected execution passed three
race repetitions before the later admission-fixture correction; final composed
race proof is still required. Native construction/startup/restart passed before
adding this aggregate rebind step, not after it.

Handler native INSERT contention and equal/conflict readback, held raw writer,
and unchanged native COMMIT contention pass three race repetitions. Preserve
the original managed-default RED; this is not a full-qualification waiver.

Canonical consumers now distinguish all known constructor-backed attachments
from actual occurrence-bound dynamic route proof. A validated keyless instance
at its authored static coordinate is static topology; a concrete coordinate
changed by keyed construction requires the existing dynamic proof. Idle known
siblings cannot invent frontier work. Real activity positives and historical
refusal controls remain exact. No replay expansion or inferred ancestry grant.

Chosen-class closure remains unproven. Track this under C22 on #2496/#2525,
with #2411/#2250 open and the existing canonical readiness/lifecycle watchlist;
do not split to #2497 or declare B/G blocking on unrelated work. The current
gate needs this concrete additional disposition before grant/fence edits.
No new issue or legacy compatibility path is proposed. Default-suite/exact-head
CI qualification and the final conflict-only rebase remain outstanding.
