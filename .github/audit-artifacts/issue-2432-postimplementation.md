# Post-Implementation Proof Audit: #2432

Current status and final-head proof: [cycle 4](issue-2441-cycle4-postimplementation.md).
The checkpoint status below is historical; the diagnostic/provenance table
remains this issue's separate manifestation inventory.

Cycle-3 combined status is in `issue-2321-pr2441-review3-proof.md` and the
separate #2442 proof/counterexample artifacts. No diagnostic production change
in that pass. Combined PR remains not review-ready because native PostgreSQL
query-cancellation provenance is not closed; prior dual-store diagnostic proof
does not waive the integration failure.

Current review-cycle-2 fixture and dual-store proof accounting is in
`issue-2321-pr2441-review2-proof.md`. Every independent/reopened SQLite handle now
has typed payload admission, with forced winner controls. Combined qualification
is blocked; historical passes below do not establish final-head merge readiness.

Rebased typed-payload admission and dual-store fork proof:
`issue-2321-pr2441-rebase-accounting.md`. This preserves the existing diagnostic
owner and exact receipt validation; combined qualification still awaits LSF-030.

## PR #2441 Review Pass 1 Repair

**Current combined result: not review-ready.** New LSF-030 blocks qualification;
see `issue-2321-pr2441-review1-escalation.md` and #2321 comment5609541948. It is a
separate coordinator retirement race, not a diagnostic regression or a repaired
historical PostgreSQL anomaly. No production fix is silently absorbed here.

The table description and runtime-log encoding summary incorrectly contradicted
the integrated settlement contract. They now reference the canonical
`agent_lifecycle_authority.diagnostic_projection.projection` and
`recovery.diagnostic_settlement` rules: outbox occurrence UUID is not event UUID;
event ID is UUIDv5/OID of `swarm:lifecycle-diagnostic:` plus outbox_id. Immutable
actor/parent modes remain producer provenance. First logger root posture owns
non-executable diagnostic mode; receipt replay cannot adopt a later caller's mode.
No runtime behavior or settlement owner was changed to match the obsolete prose.

| Review manifestation | Status | Exact proof |
| --- | --- | --- |
| Encoding/table summaries assign mode to producer provenance | reproduced and fixed | Spec summaries reconciled; TestLifecycleDiagnosticSettlementIdentityAndModeOnBothStores covers mock enqueue/live first logger/mock replay and the converse on SQLite and PostgreSQL, retaining one event and unchanged receipt. |
| Table summary aliases occurrence UUID and derived event UUID | reproduced and fixed | Same real-store test derives UUIDv5/OID explicitly, reads the exact event, rejects occurrence-ID equality, and proves no event exists at outbox_id. |

Both-store new matrix passes (1.434s); settlement/atomic controls pass three
repetitions (17.323s); API-spec tests pass (1.499s). Source search checked all current
outbox/encoding/projection summaries. Watchlist b1eff4d already records this
integration defect; no additional writer, issue or architecture is introduced.
The additional identity/mode race matrix passes three repetitions (49.212s).
Repaired-head full-profile qualification failed on LSF-030. The earlier
closure statement is superseded until those checks complete, not retroactively
credited from the preceding successful full-suite receipt.

## Previous Qualified Receipt

**Historical qualification preceding review pass 1.** B's PR #2440 merged as
47c0e70d8ef89a2c99f0be4af632f24692121b83. The serialized integration ruling
https://github.com/division-sh/swarm/issues/2432#issuecomment-5604559542 supersedes
the historical reset freeze recorded in issue-2432-reset-integration-escalation.md.
That failing probe remains historical evidence, not the current runtime contract.

Agent-g; one combined #2321/#2432/#2439 PR, separate repair commits/proof tables.
The branch was carefully rebased with merge resolutions reapplied; B's single
eventpersistence settlement owner replaces G's agentpersistence projector. Final
full-profile swarm-test passed at qualifying HEAD 1f5680f25bdb7fc988ac5e74a9c8bb27c1698336,
code/test tree be9e98fe3fb2504b68dcefa487eaec66a222a519. Subsequent changes are
audit documents and two gofmt alignment corrections in serveapp/main.go; comparing
that file against gofmt of the qualified source is byte-identical. No executable
tokens, tests or spec changed. The initial CI static-format failure remains a
failed receipt. Lead disposition
issuecomment-5607051620 records LSF-028/029 as explicit #2353 residual risks,
not an indefinite historical-evidence blocker or a claim that they were fixed.
Live/restart at runtime head 5426deb7b passed on both stores with distinct failed-attempt
accounting in issue-2321-postimplementation.md; no old delivery was replayed.
Approved original/amended gate:
https://github.com/division-sh/swarm/issues/2432#issuecomment-5601046597.

## Class And Governing Contract

Concepts changed: durable lifecycle diagnostic occurrence identity, atomic log/ack
projection, immutable execution/provenance capture, causal versus observational
fork validation, destructive diagnostic lifetime and auxiliary-error disposition.
Chosen class: lifecycle diagnostic outbox projection convergence and provenance.
Immediate parent: durable operation-outcome-to-diagnostic projection. Broader
parent #2250 remains open; #2321 is the shared integration vehicle, not a claim
to eliminate all startup/lifecycle/transaction architecture debt. Original shutdown
symptom was an entry point; the full class is covered, not just concurrent logging.

Exact authoritative platform-spec.yaml sections updated: agent_lifecycle_authority
public_restart.diagnostic, recovery.rule, transitions.terminal_flow; platform_tables
agent_lifecycle_diagnostic_outbox and diagnostics_encoding.runtime_log_encoding;
diagnostic_direct platform.runtime_log admission; run_model.fork selected_contract
typed-lineage/platform-event policies and matching runtime.run_fork.selected_contract_execution
policies, including destructive discard. Required new outbox facts have no legacy
default. Old selected stores are unsupported; no backfill, conversion or migration.

Closure commitment and achieved implementation claim: **failure class eliminated**
for this entire diagnostic class. No currently known same-class child remains;
the complete named proofs and full suite passed. Independent merge review is
still required, not replaced by shared ownership. The #2439 failure was discovered
by F4, explicitly tracked and independently approved, and repaired separately in
this PR. It was not silently absorbed or disguised as a diagnostic retry.

## Canonical Owners And Exhaustive Consumption

| Owner / all known consumers | After-change disposition and execution evidence |
| --- | --- |
| Manager lifecycle coordinator, selected materialization options, startup grant | Existing transition owner remains. All coordinator commit entrances seal explicit normal/selected origin; accepted inbound event is distinct from management observation. Static materialization, fresh spawn/adoption, start/restart/reconfigure, takeover/source retirement, terminal/self-release/shutdown/reset use the same grant-bound transition. Lifecycle coordinator/effect/source-set parity tests and actual selected execution exercise these entrances. |
| agentpersistence lifecycle transition/evidence writers on both stores | Only outbox producers; capture required actor/event mode and typed origin with durable operation result. Existing event and selected-fork validators reject foreign origin before enqueue. F6 and malformed provenance controls below. |
| EventPostgresOwner/EventSQLiteOwner.PersistLifecycleDiagnostic | Sole writable projection owner inherited from merged #2440. Locks the exact occurrence and protects an existing run; validates immutable live history and atomically commits canonical event plus complete receipt. Pending cleanup history becomes standalone without restoring a run; acknowledged history replays its immutable receipt without requiring a surviving event/run. Both stores, independent handles, failure, reopen and admitted reset controls execute this path. |
| runtime.EncodeLifecycleDiagnosticLog and EventOwner.ValidateRuntimeLogRecordTx | One canonical lifecycle payload encoder shared by RuntimeLogger and persistence; ordinary logs retain their existing encoder. Event ID is UUID SHA1/OID of swarm:lifecycle-diagnostic: plus outbox ID. Immutable producer lineage controls causal parent; first logger posture controls non-executable diagnostic mode and the receipt freezes that choice. Exact observation validation uses canonical event bytes, not PostgreSQL JSONB receipt formatting. No-delivery settlement and zero deliveries are required. |
| Manager projectLifecycleDiagnostics and persistent composition/forwarders | Thin pending-list/batch consumer delegates to the required RuntimeLogger through existing bus hooks. Six sites are executable registration/adoption, teardown, hydration, loop start, loop finalizer and launchTerminalFlowCompletion. Missing bus/logger refuses without acknowledging. Main and selected-runtime logger hooks consume the same atomic owner. No generic log-then-mark survives; the test-only batch helper invokes the actual RuntimeLogger. |
| Auxiliary failure/retirement owner | Immediate auxiliary failures go to ProcessLog; hydration returns errors. Terminal completion clears successful lifecycle state independently, retains genuine diagnostic failure for shutdown, and joins owned work. It never repeats lifecycle mutation to repair a diagnostic. Manager tests and original terminal conformance path exercise this separation. |
| RunForkOwner.ValidateLifecycleDiagnosticOriginTx | Existing selected execution/binding owner validates exact execution, generation, fingerprints, run/source/parent and binding. Projection uses historical mode after close rather than requiring a currently executing manager. Typed payload copies alone do not authorize history. |
| EventOwner.LifecycleObservationIDsTx; PostgreSQL and SQLite final fork validators | Only exact acknowledged canonical observations are excluded from final stray count. Removed both payload-tag root exceptions. Observations never enter recursive causal roots or satisfy delivery/effect/activation gates. Actual ExecuteSelectedContractRunFork plus hostile final-activation variants run both stores. |
| Selected runtime container, ordinary causal EventBus/LLM/session/tool/activity logs | Accepted parent chains remain authoritative, not lifecycle observations. Selected dispatch/materialize/start/finalization and contract-swap execution use the same origin capture and validators. Existing successful execution, provider failure cleanup and uncaused-log rejection controls remain. No generic exemption for parentless error logs. |
| RunFork selected destructive discard on both stores | Existing deletion transaction removes pending/projected diagnostic rows with its event set; ordinary execution Close retains them. Rollback preserves the complete prior pair, concurrent projection/discard uses the existing selected-store transaction boundaries, no resurrection. No extra outbox/cleanup framework. |
| Operator runtime-log/trace/CLI readers and read-only pending enumeration | Continue consuming canonical persisted facts; never acknowledge. Exact run/artifact/provenance is from the immutable occurrence; diagnostic execution mode is the retained first-settlement receipt, not retry caller or current agent. |
| SQLite transaction and bootstrap owners | Separate #2439 class/proof table. F4 no longer returns an open physical transaction to the pool. PostgreSQL-specific broader cleanup remains explicitly split to #2250. |

Old paths invalid/deleted: public MarkAgentLifecycleDiagnosticProjected and its
list/log/mark protocol; manager call to optional source-bound LogRuntime for lifecycle
projection; projection-caller-derived lifecycle run/source/lineage; payload-tag selected
tree roots; destructive-discard pending diagnostic survivor. Read-only pending list and ordinary immediate RuntimeLogger remain legitimate. The old
ProjectAgentLifecycleDiagnostics facade and CommitRuntimeLogRecordTx ports are deleted;
only PersistLifecycleDiagnostic writes a diagnostic receipt. Admitted reset preserves
historical snapshots/receipts; it is not selected-fork discard.
No process-local mutex is claimed to solve cross-manager concurrency.

## Manifestation Coverage

All persistence rows below execute SQLite and host PostgreSQL unless explicitly
identified as a unit/structural supplement. Names describe actual tests, not the
earlier audit's proposed names. Execution logs and final suite must be bound below.

| Row / manifestation | Classification | Exact proof |
| --- | --- | --- |
| F1 competing projector calls | reproduced and fixed | TestLifecycleDiagnosticAtomicProjection/concurrent, eight callers, exact event/ack cardinality; race repeats. |
| F2 independent contexts/handles | reproduced and fixed | TestLifecycleDiagnosticIndependentHandlesAndReopen uses two physical pools, same slug/different run/route, concurrent projection and reopen. DisjointSourcesIgnoreAmbientAuthority adds two actual artifact hashes, same slug, exact persisted run/source, unrelated caller context. |
| F3 event persistence failure | reproduced and fixed | AtomicProjection/before_insert, genuine trigger error, pending one/event zero, explicit retry one canonical log. |
| F4 ack/commit/unknown return | reproduced and fixed | AtomicProjection/ack_rollback and /before_commit (real deferred constraint); IndependentHandlesAndReopen loses a committed response then retries from a new connection. #2439 covers physical COMMIT failure and committed-but-error disposition. |
| F5 stale/corrupt/repeated acknowledgement | reproduced and fixed | AtomicProjection/missing_ack, payload_conflict, missing_history, timestamp_conflict and ForkLifetime/ack_event_conflict, ack_without_event; live fork activation validates the full canonical event, while acknowledged historical replay validates the receipt without recreating an event. No blanket zero-row acknowledgement. |
| F6 normal historical provenance | reproduced and fixed | AtomicProjection/foreign_context, mock_mode, deleted_actor, changed_actor; DisjointSourcesIgnoreAmbientAuthority; original actor not required at projection. |
| F6 causal actor/event modes | execution-proven through the same corrected path | TestLifecycleDiagnosticCausalOrigin valid_mode_difference, missing_parent, foreign_parent; exact accepted parent and actor/causal-mode provenance; diagnostic event mode follows the first logger posture. Rejected enqueue leaves zero operation. |
| F6 selected historical provenance | execution-proven through the same corrected path | ForkLifetime/delayed_close and causal_delayed_close: real issuance/claim/enqueue then quiesce/close before projection; historical_provenance_conflict refuses corrupted execution. |
| F6 selected malformed/foreign coordinates | execution-proven through the same corrected path | ForkLifetime/malformed_provenance: source/fork run, execution/generation/binding, actor census/admission/container/config fingerprint, artifact, actor/event mode, absent owner, impossible/missing causal parent. Matching copied outbox and operation payload still refuses; restored actual authority succeeds. |
| F7 supported selected success | execution-proven through the same corrected path | TestLifecycleDiagnosticForkActivationBothStores/clean executes actual source materialization, selected runtime/agent/start/work/quiesce and final activation; acknowledged observations exist and pending=0 at validation. Existing served/standalone selected-fork and replay-ready contract-swap tests retain their specific surface credit. |
| F7 tags/children/corruption cannot authorize | reproduced and fixed | Same actual fork execution with forged_tags, observation_child, corrupt_ack; activation rejects. Existing TestSelectedContractActivationRejectsUncausedForkLocalRuntimeLogDiagnostic and tool-executor variant retain fail-closed controls. |
| F7 cleanup and projection race | reproduced and fixed | ForkLifetime/discard_pending, discard_projected, causal_discard, discard_rollback and discard_race. Deterministic event-admission barrier; PG serializable discard conflict retains full pair before explicit caller retry. No production retry added. |
| F7 failed selected execution cleanup | execution-proven through the same corrected path | TestExecuteSelectedContractRunForkProviderFailurePreservesEvidenceThroughCleanup and TestExecuteSelectedContractRunForkCleansUpBeforeActivationOnPublishFailure retain failure and prohibit false activation; actual selected runtime is not replaced with a seeded-tag-only proof. |
| F7 admitted reset/history | reproduced and fixed | Merged TestLifecycleDiagnosticResetHistoryOnBothStores pending/projected reset families and TestLifecycleDiagnosticSettlementOnBothStores exact receipt replay controls execute the combined typed-provenance path; no current-run check is reinstated for acknowledged history. |
| F8 cancellation/hydration/shutdown | execution-proven through the same corrected path | AtomicProjection/cancelled; TestLifecycleDiagnosticFailureDoesNotChangeCompletedRetirement; original conformance template terminalization and compiled retained lifecycle tests. Genuine auxiliary failure remains observable, successful retirement is not rolled back. |
| F9 batch boundaries/prefix | execution-proven through the same corrected path | TestLifecycleDiagnosticBatchCardinality 0/1/100/101 and AtomicProjection/batch_prefix; exact prefix/event/ack/pending counts, no skip-and-success. |
| F10 nil/no-op optional logger | reproduced and fixed | TestLifecycleDiagnosticProjectionRefusesMissingLogger and merged TestLifecycleDiagnosticProjectionDrainsBeyondOneBatchAndFailsClosed; the atomic RuntimeLogger sink is required, no optional callback can acknowledge. |
| F11 supported lifecycle/idempotency | execution-proven through the same corrected path | Original TestHandleEmitTool_TemplateAgentEmissionReachesSameInstanceNodeAndTerminalizesEntity, TestServedParityHarnessAgentRestartLifecycle and unchanged lifecycle/effect authority suites; retained T/L/H proof remains separately credited under #2321. |
| #2439 shared connection termination | split / escalated as separate class | Approved issue #2439, repaired in separate commit and exact T1-T14 table within same PR. |
| Broader orchestration/PG unwind and onboarding | split / escalated as separate class | #2250 and F-owned #2319 remain open. No closure credit borrowed from diagnostic projection. |

## Verification And Tracking

Post-rebase checkpoint: TestLifecycleDiagnosticForkLifetime and actual
TestLifecycleDiagnosticForkActivationBothStores pass on SQLite/PostgreSQL (13.299s
and 14.816s). Managed diagnostic race count=3 passed (store 656.324s, actual
catalog/fork 116.361s). The integrated swarm-test at 5426deb7b passed
runtimepersistence (558.061s), catalog/fork (494.842s) and releasee2e (841.922s),
but FAILED one unrelated rate-limit timing assertion. ae8bbb782 repairs only that
test oracle. That failed run is not an overall green receipt. The subsequent
repaired-head full-profile qualification passed, as recorded below.
SQLite exit/bootstrap race checks pass (16.971s/13.910s). Persistence inventory is
regenerated with only exact moved/removed findings classified. API spec checks pass.


Executed focused complete diagnostic/read-consumer selection:
`go test ./internal/store/internal/runtimepersistence -run
'TestSQLiteStandaloneSelectedReadsBypassMutationAdmission|TestStandaloneSelectedReadAccessModeGuard|TestPendingAgentDeliveryRetryEligibilityPreservesSubsecondStoreParity|TestSQLiteStandingServiceOperatorLifecycle|TestLifecycleDiagnostic'
-count=1 -timeout=180s` PASS 26.206s. Separate disjoint-source race count=3 PASS
21.868s; malformed provenance matrix PASS 2.105s. Prior race concurrency/discard
count=3 PASS and actual dual-store fork activation PASS are implementation
checkpoints, not substitutes for the following full-suite receipt.

Final qualification: `SWARM_TEST_PROOF_PROFILE=full go run ./cmd/swarm-test --
-count=1 -timeout=30m ./...` **PASS, exit 0** at 1f5680f25. Runtimepersistence
552.917s, actual catalog/fork 503.274s, releasee2e 840.195s, serveapp 502.424s;
all remaining packages passed. Log /tmp/agent-g-final-bounded-repairs-full.log,
SHA256 1e449b463692002e4457cfb68cdee552a3a29edbad738d679fe6ec8234307db0.
Separate #2439 T7 read cancellation and physical retirement controls pass race
count=50; separate #2321 failure evidence controls pass race count=3. These are
the final integrated boundaries, not a claim that earlier failed receipts passed.

Watchlist: existing #2432 nodes/refinements 12e94ca, 7a23362 and 5621eed; #2439
refinement 823590f. No further node, issue, POTENTIAL_ISSUES entry or architecture
framework. Parent action remains bounded promotion now and #2250 open. Remaining
parent effort is multiple owner-specific orchestration passes, low confidence in
a numeric tail; no same-class diagnostic child planned. The bounded repair has
high ROI: one atomic persisted decision replaces duplicate/conflicting log-then-ack,
and exact durable observation evidence replaces six spoofable detail fields.
Effort spent includes fault and full-path proof, not a generic lifecycle rewrite.

General non-closure note: diagnostic persistence adds visible runtime_log events.
Tests asserting business event cardinality now count the exact semantic event name
instead of mistaking all diagnostic observations for business outputs. No production
event filtering or public result is weakened.
