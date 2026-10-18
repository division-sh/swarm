# 2614: bounded rebind transaction map

DIAGNOSTIC ONLY. Frozen product base f2e0d031a0; first source-map revision for
reviewer-g 6252997547. No production repair or runtime execution in this packet.

## Scope and notation

Observed cut: original connection has completed, replacement rebind has paired,
and the explicit Retry is at pairing_checkpoint. Neither unbind nor either reset
branch has executed. The two failures have save_proof=true. That association does
not identify the contending writer. Ordinary reads below are not row locks.

P = operator_principals row; B = same visible operator_channel_bindings row;
O = operator_channel_operations row; C = channel_onboarding_operations row;
A = connected_channel_activations row. Different ceremonies have different C/O
rows, but use the same P and B. U = explicit FOR UPDATE (tuple update lock);
K = implicit FK parent key-share; W = UPDATE's tuple lock (normally NO KEY UPDATE
for unchanged keys, UPDATE for a changed referenced unique key). SQL waits for
another row writer can appear in Detail as ShareLock on transactionid, not as a
tuple lock. Table AccessShare/RowShare/RowExclusive locks are also acquired;
these ordinary table modes do not conflict with one another in this workload.

The DDL owner is platform-spec.yaml, not a handwritten schema. Relevant declared
FKs are principal_id -> operator_principals for O, B, C and activation;
B.operation_id -> O.operation_id; activation.operation_id -> C.operation_id.
Inserts can therefore acquire K locks on those referenced tuples. Unique/partial
indexes can additionally wait on an inserting transaction. No custom trigger on
P/B/O/C is specified by their DDL; PostgreSQL's FK constraint triggers matter.
No observation of the live database's catalog/trigger definitions is claimed.
Lock-mode compatibility follows PostgreSQL's [row-lock table](https://www.postgresql.org/docs/current/explicit-locking.html#LOCKING-ROWS);
the implicit FK lookup is implemented as FOR KEY SHARE in PostgreSQL 16's
[RI_FKey_check](https://github.com/postgres/postgres/blob/REL_16_STABLE/src/backend/utils/adt/ri_triggers.c).

## Finite source map

All paths below are original owners, not replacement SQL. Paths are relative to
internal/store/internal/backend unless prefixed otherwise. The order is between
statements; a multi-table FOR UPDATE statement does not promise join-row order.

| Owner / original sites | Transaction acquisition sequence | Reachability at this cut |
| --- | --- | --- |
| operatorchannel.beginBinding, owner.go:229-294 | principal read; request-key O(U, if present); session C(U) via requireSessionParent; B(U); unresolved bound O(U, ORDER BY operation_id); new O insert -> P(K), unique request/challenge checks | Direct advanceIdentity -> Service.Begin from pairing-checkpoint Retry. Replay exits before later acquisitions. Session parent is the replacement C, not the completed predecessor. |
| channelonboarding.advance, owner.go:296-410 | C(U); optional A(U) for coordinate rebind; C(W); optional A(W) | Both bindCurrentCandidate coordinate rebinding and RuntimeConnection.CheckpointPairing account checkpoint use this owner, as separate transactions. Account-only checkpoint does not insert O or mutate P/B. |
| channelonboarding.sessionStandingBindingCurrent, session_standing.go:34-83 | predecessor C(U); principal-singleton advisory ExclusiveLock; P(U); B(U); same B(U) through RequireOnboardingBinding; bound child O read without lock; optional A(U) | Runtime.observeStandingSessionBinding / validateStandingSessionBinding can observe the still-current first connection during replacement pairing. Parent validity and exact binding checks still run. |
| channelonboarding.RequireSessionBusinessInputTx, session_publication.go:25-49, under eventpersistence native publication | source C(U); singleton advisory ExclusiveLock; P(U); B(U); A(U); caller's publication writes and their FKs | Native business publication is a possible concurrent producer from the prior live connection, not proven present in either failed invocation. |
| operatorchannel.completeProof, owner.go:822-841 | exact O(W) for pending/failed/active proof status; commit | Background RecoverProofResponsibilities -> materializeProof after predecessor confirmation, plus synchronous materialization/replay. File proof Get/Put is outside this transaction. This single-row completion alone has no later B/P acquisition, so it does not establish a reverse pair. |
| operatorchannel.confirmBinding, owner.go:406-554 | O(U); nonterminal parent C(U); singleton advisory ExclusiveLock; P(U); B(U); B upsert/W -> FK O/P(K) if checked; O(W); delivery binding consequences | Initial confirmation has returned; replacement confirmation is later than the failed cut. A terminal bound replay reads B without FOR UPDATE and returns early: do not invent O->B(U) for that branch. Other concurrent confirmations are not established. |
| operatorchannel.expireBinding / settleSessionClaim, owner.go:342-388 and session_claim.go:22-90 | expire: O(U), O(W) only if expiring, no parent lock; session claim: receipt read/replay; C(U), matching O(U), receipt insert/update and O(W) | Recovery/claim callback can run; the second fixture claim is sent only after the failed pairing checkpoint. Prior claim replay does not imply a new binding mutation. |
| channelonboarding.reconcileBinding, binding.go:29-76 | C(U); B(U); bound child O read without lock; C(W) if changed | Onboarding recovery/reconciliation can overlap; RequireOnboardingBinding intentionally does NOT lock the immutable bound child. |
| channelonboarding.publishActivation, owner.go:449 onward | C(U); singleton advisory ExclusiveLock; P(U); B(U) through exact binding owner; prior A(U); activation insert/update and FK C/P(K) | Predecessor activation publication has completed before the replacement pairing cut; replacement publication is later. Retained work is a candidate only if actually observed. |
| channeldelivery native/render/response/currentness owners | singleton advisory ExclusiveLock then P(U); exact native-setting/qualification joins can lock B, C, A and settings in one FOR UPDATE OF statement; plan/render/receipt W/FKs follow | Channel reconciliation for the prior activation may run independently. native_setting.go:103, native_qualification.go:67, render.go:33, response.go:25/193/331, recovery.go:63. Do not infer within-join acquisition order from the FOR UPDATE OF list. |
| provider account native/SDK writes | RuntimeConnection.CheckpointPairing -> selected advance above. SDK private session/device persistence is sessionpersistence.openDatabase's SQLite container (session.go:73), not this selected PostgreSQL database | Account milestone writer is covered by C(U)/C(W), not presumed to take a PostgreSQL SDK device lock. Lifecycle/occurrence gates are Go locks, not PG wait-for edges. |

Postgres RunTransaction/RunTransactionOutcome (postgres/transaction.go) retains
its existing connection admission, isolation, rollback/commit and cleanup. No
global SQL serialization is assumed. session_resume and error wrappers alone
do not reveal which native transaction returned 40P01.

## Source-proven candidate inversion, not an attributed hosted cycle

beginBinding can hold B(U) while its new O INSERT requests P(K) through the FK.
sessionStandingBindingCurrent can hold P(U) while requesting the same B(U).
P(U) conflicts with P(K); B(U) conflicts with B(U). The completed predecessor C
and replacement C differ, so those parent locks do not prevent this overlap.
This is a reachable source-level cycle candidate. Proof completion alone is not
its second side; saved-proof and wrapper counts do not select a competing owner.
Other principal-first channel transactions can supply the same second side.

An alternate C/B inversion may involve different parent-scoped owners or the
multi-table native qualification statements; do not choose one without evidence.
Later explicit unbind/reset/logout/teardown actions are excluded from this cut.

## Prepared capture and remaining owner handoff

The signed parser delta reads Detail only for concrete *pq.Error / 40P01 and
accepts a complete, bounded transactionid wait cycle. It emits fresh p1..p8 and
x1..x8 labels, finite lock modes, or cycle_unparsed. Detail/Where/InternalQuery,
SQL, values, and raw process/transaction numbers are never exported. Existing
Retry and PublicLogout/site capture remain. A recognized graph is explicitly
cycle_observed_owners_unmapped, never evidence of a selected owner.

The parser alone cannot correlate both participants. Do not dispatch this
preparation as if it were complete. Required named owner hunks for review:

1. Diagnostic-only observer at the existing closed PostgreSQL transaction
   callback boundaries in operatorchannel.postgresRunner.mutate/mutateOutcome,
   channelonboarding.postgresRunner.mutate/mutateOutcome, and the selected
   channel-delivery/publication callbacks. Obtain pg_backend_pid() on that SAME
   sql.Tx, once, with no externally supplied SQL or transaction/DSN export.
2. Finite BEFORE/AFTER labels at the original acquisitions above: session parent,
   principal fence, binding, unresolved O set, new O insert/FK, account checkpoint,
   native qualification join. Capture the failing callback's cycle before its
   original rollback removes the victim's wait. A bounded in-process registry
   can map Detail participants to live native owner/site labels there; use
   per-observation aliases only. Missed owner/participant stays INCOMPLETE.
3. No new product authority/fault port is included. Existing callback-lock tests
   have a test-only driver interception seam, but a saved-proof SESSION fixture
   also needs genuine admitted account evidence; do not fabricate that authority
   merely to build a SQL cycle. If that complete seam is handed off instead,
   prefer one deterministic two-owner control with original acquisitions.

No post-abort pg_locks snapshot is proposed as cycle evidence. If owner callbacks
cannot map both participants, return that gap before proposing runner-owned
server logging/live observation. No shared PG setting or timeout is changed.

After actual participants are established, propose the smallest common-order
change inside those owners, with native pre-fix RED/post-fix GREEN and exact
rollback/replay/currentness plus both-store regression controls. No generic
retry, sleep, broader mutex, deadline change or fixture wait is authorized.
