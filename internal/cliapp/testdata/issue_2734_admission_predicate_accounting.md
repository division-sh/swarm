# #2734 Native Cleanup / Startup Predicate Accounting

The approved native termination contract is
https://github.com/division-sh/swarm/issues/2734#issuecomment-6245964485.
The one-writer finalizer handoff is
https://github.com/division-sh/swarm/issues/2734#issuecomment-6246277793.

Exact-head preflight at 8e7f91f06f rejected the changed PostgreSQL transaction
body for A19/A28 (P18/P24). The correction records that one changed body and
the four declarations of its new closed cancellation helper. No previous
owner, audit row, proof family, refusal or scanner is removed or exempted.

| Owner | Callers / effect | Existing-contract proof |
| --- | --- | --- |
| `Backend.runTransactionOutcome` | `RunTransactionWithOptionsOutcome` supplies the admitted write/drain arm; `RunTransactionOutcome` and `RunTransaction` forward through it. `RunReadTransaction` supplies repeatable-read/read-only options and `drain=false`. The inspection helpers consume that unchanged read-only entry. | `TestPostgresTransactionCancellationRollbackOrders` tests both modes, cancellation/deadline, rollback orders and independent callback failures. `TestPostgresTransactionPanicAndFailureExits` preserves domain, rollback and uncertain-COMMIT failures. |
| `cleanTransactionCancellation.Error` / `Unwrap` | The private native owner constructs this error only before any attempt or after positively joined pre-COMMIT rollback and connection cleanup. Its public cause is preserved, never replaced with an acknowledgement. | `TestFaultMatrixCancellationAndCommitAcknowledgement`, plus the actual both-store shutdown attachment cuts. Independent error joins, acknowledged outcomes and crossed backend receipts cannot consume it. |
| `CleanCancellationBeforeCommit` | The native finalizer refuses a callback's foreign receipt; mutationprotocol reads only the exact native dialect's direct sealed outcome. It is not a schema, source, ownership or startup predicate. No general error-tree search confers authority. | Same native fault matrix and foreign-callback negatives in the native transaction exit roots. |
| `onlyCallerCancellation` | Only the native write termination arm uses this exact cancellation-leaf/join check. Any typed domain error, independent failure, timeout or SQL handle error prevents the outcome. | Native transaction rollback/fault roots, including the read-only and deadline cells. |

A19/P18 and A28/P24 stay unchanged: `refuseInspectionMutation`, backend validity,
the caller checks, read-only options and consistent inspection snapshot binding
still run through their existing owners. The new successful-cleanup outcome
requires `drain=true`; it never certifies a read-only inspection refusal as
successful admission. Inspection does not gain execution permission, a retry,
repair, alternate schema or a fallback on absent evidence. Verification does
not import a new runtime-only bundle predicate.

Read controls: `TestInspectionSnapshotBindsReadersAndRejectsEscapes`,
`TestInspectionNativeReadCancellationJoinsTransactionAndDisposes`, and
`TestPostgresTransactionSoleErrorIdentity`. The first two retain real native
snapshot/cancellation/cleanup ownership; the last preserves independent error
identity. Read-only cancellation/deadline controls in the native race matrix
continue to require the original causes and no clean write-cancellation fact.

The normal census and all existing `TestAdmissionPredicateRatchetRejects*`
hostiles remain. Under the existing uncovered-refusal root, four additional
named reaches mutate each exact helper declaration and reject both body changes
and deletion. Cost: zero new roots. Regeneration only records the measured
bodies; it supplies no execution credit and cannot erase the retained preflight
RED, incomplete aggregate controls or the original hosted shutdown RED.
