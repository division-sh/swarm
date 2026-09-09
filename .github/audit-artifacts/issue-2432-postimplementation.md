# Post-Implementation Proof Audit: #2432

**Closure withheld:** admitted reset integration fails on both stores at 34d86df18.
See issue-2432-reset-integration-escalation.md and its executable 12/12 failing
probe. Generic reset/whole-parent cleanup alignment and zero-tail claims below
are withdrawn pending reconciliation with the authoritative retained-audit policy
and B's overlapping #2440 owner. This is not a final PR proof audit.

Agent-g; candidate audit for the combined #2321/#2432/#2439 PR. Runtime repair
commit 3dbd7a944; SQLite connection repair separately eb4c8c613. Final integrated
suite/head binding is still pending; do not treat this candidate as review approval.
Approved pre-audit and additive provenance/cleanup gate:
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

Closure commitment: eliminate this entire diagnostic class. No currently known
same-class child remains after the named proofs pass. Confidence is conditional on
final full-suite/review, not proof-by-shared-owner. The #2439 failure was discovered
by F4, explicitly tracked and independently approved, and repaired separately in
this PR. It was not silently absorbed or disguised as a diagnostic retry.

## Canonical Owners And Exhaustive Consumption

| Owner / all known consumers | After-change disposition and execution evidence |
| --- | --- |
| Manager lifecycle coordinator, selected materialization options, startup grant | Existing transition owner remains. All coordinator commit entrances seal explicit normal/selected origin; accepted inbound event is distinct from management observation. Static materialization, fresh spawn/adoption, start/restart/reconfigure, takeover/source retirement, terminal/self-release/shutdown/reset use the same grant-bound transition. Lifecycle coordinator/effect/source-set parity tests and actual selected execution exercise these entrances. |
| agentpersistence lifecycle transition/evidence writers on both stores | Only outbox producers; capture required actor/event mode and typed origin with durable operation result. Existing event and selected-fork validators reject foreign origin before enqueue. F6 and malformed provenance controls below. |
| AgentPostgresOwner/AgentSQLiteOwner.ProjectAgentLifecycleDiagnostics | Sole writable projection owner, selected-store-wide rather than per manager/source. Existing author-activity/revision transaction locks run before occurrence; validates immutable history, commits canonical event, validates settlement, and acknowledges atomically. Both backends, independent handles, concurrency, failure, reopen and batches execute this path. |
| runtime.EncodeRuntimeLogRecord and EventOwner.CommitRuntimeLogRecordTx/ValidateRuntimeLogRecordTx | Existing canonical encoding and named diagnostic_direct persistence. Ordinary RuntimeLogger also consumes the encoder. Exact outbox UUID/time/run/mode/parent and full payload are validated; no duplicate logger schema or generic new log sink. Canonical no_subscriber_by_design and zero deliveries are required. |
| Manager projectLifecycleDiagnostics and persistent composition/forwarders | Thin bounded batch consumer; no logger/context interpretation or standalone mark. Spawn, executable adoption, startup hydration/recovery, loop start/finalizer and terminal completion all call it. Optional bus/logger is no longer required. Persistent constructor roles require the store capability; nil-bus proof executes projection. In-memory manager remains a different nonpersistent configuration. |
| Auxiliary failure/retirement owner | Immediate auxiliary failures go to ProcessLog; hydration returns errors. Terminal completion clears successful lifecycle state independently, retains genuine diagnostic failure for shutdown, and joins owned work. It never repeats lifecycle mutation to repair a diagnostic. Manager tests and original terminal conformance path exercise this separation. |
| RunForkOwner.ValidateLifecycleDiagnosticOriginTx | Existing selected execution/binding owner validates exact execution, generation, fingerprints, run/source/parent and binding. Projection uses historical mode after close rather than requiring a currently executing manager. Typed payload copies alone do not authorize history. |
| AgentOwner.LifecycleObservationIDsTx; PostgreSQL and SQLite final fork validators | Only exact acknowledged canonical observations are excluded from final stray count. Removed both payload-tag root exceptions. Observations never enter recursive causal roots or satisfy delivery/effect/activation gates. Actual ExecuteSelectedContractRunFork plus hostile final-activation variants run both stores. |
| Selected runtime container, ordinary causal EventBus/LLM/session/tool/activity logs | Accepted parent chains remain authoritative, not lifecycle observations. Selected dispatch/materialize/start/finalization and contract-swap execution use the same origin capture and validators. Existing successful execution, provider failure cleanup and uncaused-log rejection controls remain. No generic exemption for parentless error logs. |
| RunFork selected destructive discard on both stores | Existing deletion transaction removes pending/projected diagnostic rows with its event set; ordinary execution Close retains them. Rollback preserves the complete prior pair, concurrent projection/discard joins at run ownership, no resurrection. No extra outbox/cleanup framework. |
| Operator runtime-log/trace/CLI readers and read-only pending enumeration | Continue consuming canonical persisted facts; never acknowledge. Exact run/artifact/mode readback is from persisted occurrence, not caller or current agent. |
| SQLite transaction and bootstrap owners | Separate #2439 class/proof table. F4 no longer returns an open physical transaction to the pool. PostgreSQL-specific broader cleanup remains explicitly split to #2250. |

Old paths invalid/deleted: public MarkAgentLifecycleDiagnosticProjected and its
list/log/mark protocol; manager call to optional source-bound LogRuntime for lifecycle
projection; caller-derived lifecycle run/source/mode/lineage; payload-tag selected
tree roots; destructive-discard pending diagnostic survivor. Read-only pending list
and ordinary immediate RuntimeLogger remain legitimate and non-acknowledging.
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
| F5 stale/corrupt/repeated acknowledgement | reproduced and fixed | AtomicProjection/missing_ack, payload_conflict, missing_history, timestamp_conflict and ForkLifetime/ack_event_conflict, ack_without_event; exact replay/concurrent path validates existing full canonical record, no blanket zero-row success. |
| F6 normal historical provenance | reproduced and fixed | AtomicProjection/foreign_context, mock_mode, deleted_actor, changed_actor; DisjointSourcesIgnoreAmbientAuthority; original actor not required at projection. |
| F6 causal actor/event modes | execution-proven through the same corrected path | TestLifecycleDiagnosticCausalOrigin valid_mode_difference, missing_parent, foreign_parent; exact persisted parent/mode; rejected enqueue leaves zero operation. |
| F6 selected historical provenance | execution-proven through the same corrected path | ForkLifetime/delayed_close and causal_delayed_close: real issuance/claim/enqueue then quiesce/close before projection; historical_provenance_conflict refuses corrupted execution. |
| F6 selected malformed/foreign coordinates | execution-proven through the same corrected path | ForkLifetime/malformed_provenance: source/fork run, execution/generation/binding, actor census/admission/container/config fingerprint, artifact, actor/event mode, absent owner, impossible/missing causal parent. Matching copied outbox and operation payload still refuses; restored actual authority succeeds. |
| F7 supported selected success | execution-proven through the same corrected path | TestLifecycleDiagnosticForkActivationBothStores/clean executes actual source materialization, selected runtime/agent/start/work/quiesce and final activation; acknowledged observations exist and pending=0 at validation. Existing served/standalone selected-fork and replay-ready contract-swap tests retain their specific surface credit. |
| F7 tags/children/corruption cannot authorize | reproduced and fixed | Same actual fork execution with forged_tags, observation_child, corrupt_ack; activation rejects. Existing TestSelectedContractActivationRejectsUncausedForkLocalRuntimeLogDiagnostic and tool-executor variant retain fail-closed controls. |
| F7 cleanup and projection race | reproduced and fixed | ForkLifetime/discard_pending, discard_projected, causal_discard, discard_rollback and discard_race. Deterministic event-admission barrier; PG serializable discard conflict retains full pair before explicit caller retry. No production retry added. |
| F7 failed selected execution cleanup | execution-proven through the same corrected path | TestExecuteSelectedContractRunForkProviderFailurePreservesEvidenceThroughCleanup and TestExecuteSelectedContractRunForkCleansUpBeforeActivationOnPublishFailure retain failure and prohibit false activation; actual selected runtime is not replaced with a seeded-tag-only proof. |
| F8 cancellation/hydration/shutdown | execution-proven through the same corrected path | AtomicProjection/cancelled; TestLifecycleDiagnosticFailureDoesNotChangeCompletedRetirement; original conformance template terminalization and compiled retained lifecycle tests. Genuine auxiliary failure remains observable, successful retirement is not rolled back. |
| F9 batch boundaries/prefix | execution-proven through the same corrected path | TestLifecycleDiagnosticBatchCardinality 0/1/100/101 and AtomicProjection/batch_prefix; exact prefix/event/ack/pending counts, no skip-and-success. |
| F10 nil/no-op optional logger | reproduced and fixed | TestLifecycleDiagnosticProjectionUsesPersistenceWithoutOptionalBus; canonical Tx event sink required, no optional callback can acknowledge. |
| F11 supported lifecycle/idempotency | execution-proven through the same corrected path | Original TestHandleEmitTool_TemplateAgentEmissionReachesSameInstanceNodeAndTerminalizesEntity, TestServedParityHarnessAgentRestartLifecycle and unchanged lifecycle/effect authority suites; retained T/L/H proof remains separately credited under #2321. |
| #2439 shared connection termination | split / escalated as separate class | Approved issue #2439, repaired in separate commit and exact T1-T14 table within same PR. |
| Broader orchestration/PG unwind and onboarding | split / escalated as separate class | #2250 and F-owned #2319 remain open. No closure credit borrowed from diagnostic projection. |

## Verification And Tracking

Executed focused complete diagnostic/read-consumer selection:
`go test ./internal/store/internal/runtimepersistence -run
'TestSQLiteStandaloneSelectedReadsBypassMutationAdmission|TestStandaloneSelectedReadAccessModeGuard|TestPendingAgentDeliveryRetryEligibilityPreservesSubsecondStoreParity|TestSQLiteStandingServiceOperatorLifecycle|TestLifecycleDiagnostic'
-count=1 -timeout=180s` PASS 26.206s. Separate disjoint-source race count=3 PASS
21.868s; malformed provenance matrix PASS 2.105s. Prior race concurrency/discard
count=3 PASS and actual dual-store fork activation PASS are implementation
checkpoints; final full-suite head binding remains required, not assumed.

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
