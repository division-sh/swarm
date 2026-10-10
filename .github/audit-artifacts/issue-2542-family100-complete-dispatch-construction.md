# Cohort 100: shared complete-event dispatch construction

Governing context: approved #2542/#2151 broad fixture-authority migration;
selected_runtime_store_construction, raw_sql_policy, backend-neutral mutation
boundary and platform_pipeline_receipt_rows in platform-spec.yaml. Existing
decisionpersistence creation/decision and pipelinepersistence routing owners apply.
No production policy, new domain owner or public raw port is introduced.

The complete-event fixture is shared by recovery, manager backlog, normal handoff,
publication election, paused/stop control, standing recovery, origin cancellation,
decision continuation and corrupt-scope proofs. Its only surviving database use
was the two-write decision seed. The fixture now carries no database/dialect field;
PostgreSQL consumes StartPostgresRuntimeStore, SQLite its existing native constructor.
No caller obtains a pool, reconstructs a coordinator or repeats pool cleanup.

Decision setup now consumes existing Card/Anchor/FreezeSnapshot/CreateDecisionCard
and DecisionCardDomain.ApplyDecisionForTest. The latter owns the decided transition,
change log and pending route obligation in one original transaction. The old empty
anchor/snapshot and invented card/schema/source hashes are invalid and deleted.
The setup retains exact event/run, approve verdict, test principal, mock mode,
event-created clock and pending/attempt-zero routing posture. This is a domain
fixture for continuation proofs, not a claim of public human-operation admission.

Systematic consumers: insertDecisionObligation and all four direct For callers
(target/foreign continuation, sustained recovery and later-run scan) use this
single recipe. Constructor callers need no forwarding change. Existing seven
dispatch-seed recipes are updated rather than duplicating the shared constructor.
One new exact recipe and hostile owner/key/clock/hash/verdict controls cover the
decision seed; the constructor oracle permits only native constructor/field removal.

Focused both-store race proof: original recovery surfaces, manager backlog, normal
atomic handoff, exact target/foreign continue, sustained global decision re-entry,
later-run non-starvation, acknowledged decision before corrupt-scope quarantine,
and the new native decision-owner control. The new control requires two acknowledged
original writes, no active work, exact decoded card/result equality, generated
hashes and admitted source, pending routing, foreign-key refusal, complete event
readback, actual decision claim and completed settlement. Existing tests retain
their temporal cuts, no-mutation/no-dispatch, replay and receipt assertions.

Reliability/proof value: retires a shared hand-built invalid card and non-atomic
two-autocommit protocol, plus exposed selected construction and duplicate pool
cleanup. No measured flake-rate improvement is claimed. Parent completion remains
open; no new architecture/framework/compatibility/watchlist split is required.
