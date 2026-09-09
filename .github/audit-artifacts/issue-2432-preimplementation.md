Current status: approved by independent gate cycle 2, https://github.com/division-sh/swarm/issues/2432#issuecomment-5601046597. The historical freeze/separate-PR wording below is superseded. Implement in separate commits and proof tables in the combined #2321 PR. Read issue-2432-integration-gate.md for the additional binding destructive-discard lifetime rule.

# Pre-Implementation Coverage Audit

Issue: #2432. Agent: agent-g. Phase: pre-audit only; independent approval pending.
Cycle-1 outcome is **insufficient; widen class**. The additive
`issue-2432-fork-provenance-amendment.md` supersedes the selected-fork exclusions,
universally unparented provenance proposal, F6/F7 and related stop condition below.
Read both artifacts together; neither authorizes production implementation.
Audited fresh `origin/master` at `496498457` on 2026-09-08. #2321 remains at
`631af39ca` in its separate worktree; no runtime repair is included here.

## Classification and Governing Context

- Category: failure-class, semantic-drift, dual-selected-store parity.
- Observed symptom: template-agent terminalization occasionally fails shutdown
  with `lifecycle diagnostic projection conflict` after duplicate log emission.
- Chosen class: lifecycle diagnostic outbox projection convergence and provenance.
- Immediate parent: durable operation-outcome-to-diagnostic projection.
- Broadest plausible parent: explicit ownership of multi-phase runtime transactions
  and lifecycle state machines, tracked by open #2250.
- Framing: #2432 is broad enough. Concurrency, sequential retry after failed ack,
  missing persistence, historical attribution and auxiliary failure disposition
  belong to this same class, not separate children. The failing helper and
  conformance fixture are entry points, not the audit boundary.
- Intended closure: failure class eliminated for this complete diagnostic class,
  not just the shutdown manifestation. #2250 is not claimed closed.

Read the complete issue body and independent sibling reproduction comment
https://github.com/division-sh/swarm/issues/2432#issuecomment-5588209972;
re-read IMPLEMENTER_GUIDELINES.md and SEMANTIC_DRIFT.md in swarm-docs.
Binding exact authoritative platform-spec.yaml sections:

| Reference | Binding constraint |
|---|---|
| `agent_lifecycle_authority.public_restart.diagnostic` and `.idempotency` | Durable transition/outbox are lifecycle success; projection is not another transition. |
| `agent_lifecycle_authority.recovery.rule` | Pending diagnostics project through canonical log ownership; failure retains pending evidence and never repeats lifecycle mutation. |
| `agent_lifecycle_authority.transitions.terminal_flow` | Committed lifecycle facts and auxiliary errors are distinct; joins cannot hold dependencies needed by accepted work. |
| `agent_lifecycle_authority.destructive_cleanup_quiescence` | Shutdown precedes reset/nuke/fork discard. |
| `platform_tables` table `agent_lifecycle_diagnostic_outbox` (13976), `agent_lifecycle_operations`, `agent_lifecycle_transition_facts` | Exact run-scoped identity is a historical snapshot, not a current-agent selector. |
| `platform_tables.diagnostics_encoding.runtime_log_encoding` (16045) | Existing events table, canonical payload, global scope, structured details; no diagnostic table. |
| `contract_formats` event admission `diagnostic_direct` (1032), `structural_subtypes.platform.runtime_log` | Named commit only, platform/runtime producer, exact existing run, non-executable evidence. |
| `platform_events.catalog["platform.runtime_log"]` | Existing diagnostic-direct subtype, not routed lifecycle work. |
| `runtime.run_fork.selected_contract_execution.fork_local_runtime_platform_event_lineage_policy` and `.fork_local_runtime_typed_lineage`; corresponding fork policy at 16735/17038 | Causal fork output requires persisted parent lineage. A lifecycle snapshot cannot fabricate a parent or become selected-fork activation evidence. |

These sections govern, but do not currently specify an atomic projection/ack or
stored execution mode for the diagnostic. The proposed delta below resolves those
gaps explicitly; it is submitted for gate approval, not treated as already ratified.

## Execution Path and Gate Classification

1. Public/API auth, restart idempotency admission, runtime startup admission or
   internal termination authorizes a lifecycle request. **Different concept**:
   existing API idempotency, process/topology authority and exact generation checks
   in manager lifecycle coordinator and agentpersistence remain unchanged.
2. Coordinator serializes exact identity, validates expected epoch/generation,
   applies lifecycle/session/provider-drain consequences, commits immutable outcome,
   transition fact and outbox. **Same class** at the diagnostic snapshot writer;
   **different concept** for transition/session/effect decisions themselves, proven
   by existing LifecycleAndExternalEffectAuthority and LifecycleSubordinate tests.
   A successful durable commit is required before a pending diagnostic exists.
3. Spawn/adoption, loop start/finalization, hydration or terminal completion invokes
   the projector. **Same class**: all invocation contexts must converge on persisted
   row ownership regardless of ambient run/source/mode.
4. Today: global pending list -> EventBus.LogRuntime -> RuntimeLogger context
   interpretation -> named durable runtime-log commit -> standalone ack. **Same
   class**: this split decision is removed for lifecycle diagnostics.
5. Replacement: selected-store projection locks/reloads the exact row, validates
   stored snapshot and existing run, invokes canonical encoding/named event commit
   with the same transaction, acknowledges, then commits. **Same class**.
6. Public log/trace/CLI readers consume persisted canonical event rows. **Same
   class** for readback proof; **different concept** for filtering/presentation.
   Optional transcript/terminal presentation is not durable acknowledgement.
7. Retirement joins and shutdown/reset completion consume actual lifecycle failure
   separately from auxiliary projection failure. **Same class** for distinguishing
   the diagnostic error; **different concept** for existing worklifetime joins and
   destructive cleanup ordering, retained unchanged and regression-tested.
8. #2321 combined acceptance and F-owned #2319 webhook activation are **explicitly
   split/tracked separately**. Neither changes diagnostic semantics or receives
   closure credit from this repair.

## Exhaustive Owner and Consumer Census

Census commands: `rg -n 'projectLifecycleDiagnostics|ListPendingAgentLifecycleDiagnostics|MarkAgentLifecycleDiagnosticProjected' internal --glob '*.go'`;
`rg -n 'agent_lifecycle_diagnostic_outbox' internal platform-spec.yaml`;
`rg -n 'PersistRuntimeLog|CommitRuntimeLogEvent|RuntimeLogLineageParentEventID' internal --glob '*.go'`.
Generated facade entries are forwarding consumers, not independent semantics.
Labels below are the planned dispositions, not claims of implemented migration.

| Owner / consumer seam | Classification and exact disposition |
|---|---|
| `agentpersistence/lifecycle.go`: insertPostgresLifecycleEvidence / insertSQLiteLifecycleEvidenceTx | **Moved to canonical owner in this work** for typed diagnostic provenance capture. These are the only two outbox insert interpreters found. Existing operation and transition outcome remain authoritative. |
| Same file: CommitAgentLifecycleTransitionTx / commitPostgresAgentLifecycleTransitionTx / commitSQLiteAgentLifecycleTransitionTx | **Already consumes canonical owner**: spawn, hydrate/materialize, start, restart, reconfigure, source rebind/retire, takeover, teardown, self-release, shutdown/reset all enqueue through these writers. No separate producer-specific projector is needed. |
| `manager/agent_manager.go`: adoptPersistedAgentExecutable (479) | **Moved to canonical owner in this work**: invocation delegates atomic projection; no caller-context attribution, no ignored persistence error. Lifecycle-only adoption does not become executable. |
| Same file: fresh spawn (703) | **Moved to canonical owner in this work**, same named projection request after commit, independent proof for its error exit. |
| `manager/runtime.go`: HydrateForStartup (1014), Recover / RecoverWithStartupReplayDiagnostics wrappers | **Moved to canonical owner in this work**; exact pending snapshot projection, no repeated transition, retained genuine error before startup reports success. |
| Same file: launchExecutionLoop start (1664) | **Moved to canonical owner in this work**; projection has no dependence on loop parent authority. |
| Same file: launchExecutionLoop finalizer (1681) | **Moved to canonical owner in this work**; background caller must not manufacture live mode/run. |
| `manager/terminal_retirement.go`: completeTerminalRetirements / launchTerminalFlowCompletion | **Moved to canonical owner in this work**; lifecycle retirement completion/terminal-set clearing depends on retirement success, not log availability. Actual auxiliary errors remain observable. |
| `manager/runtime.go`: projectLifecycleDiagnostics | **Moved to canonical owner in this work**; thin bounded batch driver of selected-store projection, not payload/context/log/ack interpreter. |
| `manager/types.go`: AgentLifecycleDiagnosticPersistence; PersistenceRoles.LifecycleDiagnostics; composition/generated facade forwarders | **Moved to canonical owner in this work**; replace writable list+mark protocol with one named projection capability. Constructor requires it for persistent lifecycle operation. No nil-log acknowledgement path. |
| `agentpersistence/lifecycle.go`: both list/mark implementations and requireSingleLifecycleDiagnosticProjection | **Moved to canonical owner in this work**; old public Mark method and writable protocol are removed. Read-only test/inspection enumeration may remain only if consumed, never an alternative ack path. |
| `runtime/diagnostics.go`: RuntimeLogger, canonical payload construction; `runtime_log_payload.go` decoder | **Moved to canonical owner in this work** for extraction/reuse of one explicit-facts record encoder. Existing ordinary Log path keeps its caller-lineage contract; lifecycle uses typed row facts instead. No copied payload schema in agentpersistence. |
| `eventpersistence/runtime_log_persistence.go`: runtimeLogEvent / PersistRuntimeLog; `event_commit.go`: commitRuntimeLogEvent | **Moved to canonical owner in this work** for transaction-local named runtime-log commit consumed by agent owner. Existing named constructor/admission/settlement stays authoritative. |
| `eventpersistence/owner.go`, backend postgres/sqlite transaction owners, authoractivity and runforkrevision | **Already consumes canonical owner**; existing transaction/story/revision composition reused, explicitly passed SQL transaction, no nested public PersistRuntimeLog transaction. |
| `bus/eventbus_routing.go`: LogRuntime and logger adapters | **Different semantic concept, with proof**: ordinary immediate diagnostic requests retain source/context checks. Lifecycle projection stops calling this optional, source-bound bus hook. RuntimeLogger payload and no-op/failure tests protect ordinary behavior. |
| Runtime tools/LLM log producers, runfork logger hooks and selected-fork lineage validators | **Different semantic concept, with proof**: causal execution diagnostics have persisted parent requirements; outbox lifecycle snapshots do not confer selected-fork output authority. Existing selected-fork causal/uncaused rejection tests retained. |
| `eventpersistence/*inbound_publication.go` and agentpersistence directive_operations | **Different semantic concept, with proof**: inbound publication and directive outcome are named atomic domain operations, not lifecycle log projectors. Existing named-commit/admission parity tests and source census protect their owners. |
| RuntimeLog readers: operator logs/trace, CLI projectRuntimeLogEntry, turn recorder/replay readers | **Already consumes canonical owner**: canonical payload/event readback, no outbox acknowledgement. Run/source filters must display stored subject; no UI-specific identity fix. |
| selected-store bootstrap/reset/nuke/preservation table registry | **Different semantic concept, with proof**: aggregate deletion of admitted current shape remains existing cleanup authority. No export, migration, backfill, fallback reader or synthetic missing-run recreation is added. |

The chosen store owner is a real semantic owner: it already creates durable
lifecycle outcome/outbox and can bind the existing event owner transaction port.
It is not merely the first manager helper found. No currently known lifecycle
diagnostic producer/consumer is left bypassing or split from this chosen class.

## Proposed Bounded Design / Spec Delta

### Durable Projection

Keep the existing outbox and events tables. Replace the manager's list -> log ->
mark sequence with `ProjectAgentLifecycleDiagnostics(ctx, limit)` on the existing
selected agent persistence owner. Internal exact-row projection returns inserted
or already-projected only after verifying persisted truth. The caller supplies no
payload, run, source, execution mode or acknowledgement timestamp authority.

One row is the atomic unit: lock/reload row -> validate snapshot and existing run
-> canonical runtime-log record -> named event commit -> set projected_at -> commit.
PostgreSQL uses row-level locking/revalidation across independent connections;
SQLite uses the existing transaction/mutation owner and busy/cancellation budget.
Do not hold a list cursor across nested writes. Do not use SKIP LOCKED to report a
competing in-flight row as completed. A batch is a bounded ordered set of up to 100
row identities; an earlier committed prefix survives a later row failure. Further
batches consume remaining pending rows, including concurrent enqueue, using the
existing caller lifetime. There is no scheduler or background recovery framework.

Existing agent owner binding to a transaction-local event committer is already used
by directive operations. Add the analogous narrow runtime-log Tx port; extract the
body of commitRuntimeLogEvent so both ordinary and lifecycle log persistence use
its exact validation/append/settlement path. Pass the same *sql.Tx, authoractivity
mutation and revision effects, and finalize once. Both backends currently open a
new transaction for public PersistRuntimeLog, so calling it inside another write
would NOT be atomic and is explicitly forbidden.

Use the outbox UUID as the stable diagnostic event ID, and outbox created_at as the
event timestamp. Canonical collision checking must reject a different event or
payload at that ID, including when projected_at is already set. Exact concurrent
replay observes one log and successful acknowledged state; missing row, mismatched
operation/identity, missing acknowledged event or conflicting event is corruption,
not blanket zero-row success. The ordinary runtime-log caller continues to receive
new admission identity; only this existing durable occurrence fixes its ID/time.

An error/ambiguous return is not proof of rollback. Reopen/retry observes either
both committed facts or neither. A failure before commit leaves the row pending and
no new event. No idempotency table or separate sink/ack recovery protocol is needed.

### Exact Provenance

Persist the missing fact at its writer, not by guessing at retry time. Proposed
minimal DDL delta: add required `execution_mode TEXT NOT NULL CHECK
(execution_mode IN ('live','mock'))` to agent_lifecycle_diagnostic_outbox on both
stores, without a default. Capture it from the exact canonical persisted agent
descriptor in the SAME lifecycle transaction after cell application and before
evidence commit, using the existing descriptor decoder. Candidate creation uses
the newly persisted candidate; mode-preserving transitions use that exact row.
Missing/malformed descriptor is a write failure, not a reason to infer mode from
backend, source mock presence, current manager posture or loop run_mode.

Keep existing typed transition result as outbox payload. Decode and validate it,
cross-check operation_id and the complete identity against outbox columns and
stored immutable operation/transition evidence. Its ProcessBinding.BundleHash
already persists exact artifact identity; do not add a second source selector.
Current master source facts are artifact-hash facts, not the retired source-path
model. Do not reconstruct from the currently loaded bundle or current agent.
Existing run owner establishes the exact run is present. Terminal runs are legal;
missing runs fail without recreation. Deleted/reconfigured current agent rows are
not needed at projection time. Preserve the recorded process binding as historical
evidence even after process takeover; it does not authorize execution.

Extract one explicit-facts record encoder from RuntimeLogger. Lifecycle calls it
with row run/identity, stored mode, stable occurrence ID/time and structured
transition details including recorded artifact/process binding. No incoming
context value enters those facts; caller context bounds cancellation only.
Lifecycle transition diagnostics are intentionally run-scoped, unparented operation
observations: no stored causal event exists in this outbox, so parent/subject/handler
are explicitly absent. Do not invent a parent from operation UUID, transition UUID,
caller event or a 'latest' event. They remain non-routed/non-executable with
no_subscriber_by_design and cannot count as causal selected-fork activation/output.
Retain selected-fork causal diagnostic admission/rejection unchanged and prove this
observational distinction. If that distinction contradicts an exact contract during
implementation, stop for a gate repair, not a context bypass.

Promote the approved delta into authoritative platform-spec.yaml in the repair PR:
outbox column/capture and snapshot rules; public_restart.diagnostic atomic projection
semantics; recovery retry semantics; transitions.terminal_flow auxiliary errors;
runtime_log_encoding and diagnostic-direct explicit identity/non-executable rules.
Update generated schema/owner inventories as required. Old selected stores are
unsupported; no migration, export, backfill, column default or compatibility reader.

### Lifetime, Presentation and Failure Disposition

Durable projection does not call optional EventBus logger, stdout or transcript
recorder. Missing bound event persistence fails construction/projection without
acknowledgement. Optional presentation is not the projection transaction and has no
exactly-once guarantee. Existing durable log readers provide canonical observation;
no new lifecycle presentation framework is proposed.

Keep existing manager invocation/lifetime owners; no detached worker, global
manager mutex or lock held while joining provider/loop work. Projection transaction
holds only DB dependencies and canonical local encoding, never EventBus delivery,
external calls or callbacks that start their own store transaction. Owned work
joins before store teardown; reset/nuke cannot delete the store during projection.

Public restart/spawn/adoption success remains the durable lifecycle outcome, not
immediate log success. Replace ignored opportunistic projection errors with the
existing process diagnostic failure reporting path (not recursively the failing
durable sink); preserve pending rows and original error cause. Startup/shutdown
return genuine projection errors with explicit diagnostic-projection attribution.
Exact already-projected replay is success, never terminal-retirement conflict.

In terminal completion, finish/clear the actual retirement set after retirement
success independently of a later diagnostic failure. Preserve and report the latter
as auxiliary projection failure, not as failed lifecycle commit or reason to retain
executable retirement authority. Real retirement/lease/panic failures retain existing
error semantics. A reported shutdown diagnostic error does not erase an earlier
successful durable transition and must not replay it on restart. No error suppression
or 'ignore every conflict' policy is permitted.

## Manifestation-Level Proof Plan

All `TestLifecycleDiagnosticProjectionParity/<case>` entries below are NEW planned
subtests, executed against real SQLite and host PostgreSQL, reading canonical events,
outbox, operations, transition facts, agents and sessions. Deterministic barriers and
fault seams supplement real stores; they cannot replace them. Existing tests below
are regression proof, not claimed proof of the unimplemented fix.

| Family | Exact planned proof and invariants |
|---|---|
| F1 concurrent callers, one manager | `.../same_manager_concurrent`: barrier after candidate discovery, both callers execute real Tx; one event, one ack, both succeed, no terminal error. Retain the original projector probe with inverted closure assertions and race-enabled manager test. |
| F2 independent managers/contexts | `.../two_managers_shared_store` and `/disjoint_sources_same_slug`: independent manager/store handles, same row plus two run/artifact rows; exact event counts and stored run/mode/artifact, no source-bound bus dependency. |
| F3 log/validation failure | `.../before_event_insert` and `/invalid_payload`: injected named-writer failure leaves pending/event count zero, original failure returned; retry yields one event and unchanged operation/generation/session counts. |
| F4 ack/commit/ambiguous-return/reopen | `.../after_event_before_ack`, `/before_commit`, `/after_commit_return_error`, `/reopen_retry`: real transaction rollback and committed-but-error control; readback proves both-or-neither and retry one canonical event. No process sleep as a failure oracle. |
| F5 exact replay vs corruption | `.../already_projected`, `/missing_row`, `/wrong_operation_identity`, `/conflicting_event_id`, `/ack_without_event`: no blanket zero-row success; mismatches fail without writes. Competing ack after first commit validates the exact event. |
| F6 stored identity / ambient contamination | `.../persisted_provenance`: same slug across runs/artifacts, live/mock rows, unrelated caller run/source/parent/handler/recorder and no values; exact durable Event.RunID/ExecutionMode, source binding, actor coordinate and absent source_event_id. Mutated payload identity/mode/source fails before ack. |
| F7 terminal/historical/fork observations | `.../terminal_and_deleted_agent`, `/reconfigured_successor`, `/missing_run`, `/fork_observation_not_activation`: every supported run status, actor deletion and mode-changing successor do not rewrite old facts; no fabricated run or selected-fork causal credit. Preserve `Test*RuntimeLogAdmissionPreservesEveryRunStatus`, `Test*RunScopedRuntimeLogRequiresExistingRun`, and selected-fork causal/uncaused rejection tests. |
| F8 lifetime and failure policy | `.../cancel_before_commit`, `/shutdown_join`, `/reset_order`, `/startup_retry`; manager `TestLifecycleDiagnosticFailureDoesNotRetainTerminalAuthority`: barriers prove no store close before join, cancelled transaction retry safe, true retirement errors preserved, auxiliary failure not an executable retirement or repeated lifecycle transition. Exercise both Manager run modes and startup recovery enabled/disabled projection entrances actually reached. |
| F9 cardinality and partial batch | `.../batch_0`, `/batch_1`, `/batch_100`, `/batch_101`, `/concurrent_enqueue`, `/middle_row_failure`: exact prefix acknowledged, failed row and suffix pending, retry no duplicates, later enqueue included without cursor/write deadlock. |
| F10 no persistence / no-op presentation | `.../missing_log_owner`: required binding refusal, no ack; optional EventBus/no-op logger irrelevant to durable success. Recorder/console disabled or failing cannot fabricate persistence. Keep ordinary RuntimeLogger nil/persistence/error tests unchanged in contract. |
| F11 supported regression | Run unchanged `TestHandleEmitTool_TemplateAgentEmissionReachesSameInstanceNodeAndTerminalizesEntity` complete SQLite/PG matrix (omitted/literal ID x 1/2/3 children), `TestLifecycleAndExternalEffectAuthority(SQLite|Postgres)`, `TestLifecycleSubordinateTransaction(SQLite|Postgres)`; compiled public restart same-key/API-completion retry readback and retained graceful/forced restart through existing releasee2e harness. #2321 retained H proof is credited as H, never public L or T. |

Add public regression `TestLifecycleDiagnosticPublicRestartParity` using the
existing compiled served harness: same idempotency key returns identical lifecycle
outcome, exact internal log readback after restart, unchanged generation/session
cardinality; persistent pending diagnostic retries after service restart. Both
SQLite and PostgreSQL are required. Use fresh stores only.

Generic failing proof: the committed #2321 probe artifact at 631af39ca plus lead's
independent same-manager/cross-manager/sequential/ambient-context probes. Planned
generic failure injection runs one durable occurrence through every transaction
cut, retry/reopen and competing owner, rather than patching conformance timing.

Required final verification: focused manager race tests; canonical runtime-log,
named diagnostic-direct/route-settlement, lifecycle/session and selected-fork
lineage parity; compiled supported F11; schema/spec/persistence-inventory checks;
full suite `SWARM_TEST_PROOF_PROFILE=full go run ./cmd/swarm-test -- -timeout=30m ./...`.
Small isolated tests use ordinary go test. No suite-pass or runtime-closure claim
is made by this audit. Do not rerun until a flaky green result and call that closure.

Audit execution evidence on 496498457: `go test ./internal/runtime -run
'^TestRuntimeLogger' -count=1` passed (2.031s); `git diff --check` passed.
This is existing codec/context regression evidence, not projection closure. Lead's
real SQLite/PostgreSQL lifecycle authority results are recorded evidence on #2432,
not claimed as a fresh backend rerun by this audit.

## Parent Probe, Tracker, Watchlist and Feasibility

Parent sibling probe: ordinary RuntimeLogger has no durable enqueue/ack lifecycle;
inbound and directive named operations already couple their authoritative writes in
selected-store transactions; authoractivity also finalizes within the lifecycle
transaction. Their source/lineage contracts remain separate and receive regression
proof, not a logger rewrite. The concrete live gap found is this lifecycle outbox
bridge. Delete that bridge's split writer, not all runtime diagnostic producers.

Tracker decision: #2432 remains correct as written; this artifact supplies the exact
owner/spec decisions requiring approval. #2321 stays approved but acceptance-blocked
on #2432 and separately #2319. Closed #1927 stays closed. #2250 remains the broader
architecture tracker; no new child or POTENTIAL_ISSUES entry is required.

Watchlist-backed promotion check: consumed swarm-docs@12e94ca, existing
`runtime-operations.shutdown_and_runtime_lifecycle`. It explicitly covers
cross-manager convergence, sequential failed-ack retry, stored identity/mode,
terminal history, missing logger and durable-vs-presentation separation. All are
absorbed here; it does not justify absorbing unrelated shutdown/provider/startup
owners. No additional watchlist edit is needed before this gate. Preserve concurrent
docs changes; this commit is published on `review/lifecycle-diagnostic-projection`.

Parent action: close this complete bounded child now, keep #2250's explicitly
tracked decomposition open. Remaining known same-class child tail after this PR:
zero, moderate confidence pending fault/parity proof. Broader #2250 tail groups:
startup/composition, delivery lifetimes, connect/readiness and engine/inbound phase
ownership; the parent's census is still needed, so no honest numeric child count
or closure date is asserted. This audit does not re-open historical #1927.

Architecture feedback: a context-based logger plus void-like optional presentation
was used as the durable completion port. The long-run better direction is explicit
stored diagnostic facts into the existing codec and one named transactional writer.
Tracking decision: implement this concrete correction in #2432; broader phase debt
remains #2250 and the existing watchlist, not a new logging architecture. Estimate:
2-4 engineering days including dual-store fault and served proof, moderate confidence;
high ROI because it removes duplicate logs, misleading shutdown failure and wrong-run
attribution together, with no framework or compatibility cost.

Closure feasibility: yes, one repair PR appears feasible using existing agent/event
owners and one additive required outbox fact. Fixing only the manager helper would
leave backend ack and historical mode interpretation live; this plan removes both.
This PR commits to eliminate its chosen class entirely. It is not a first-slice claim.

Stop conditions: independent gate not recorded; a selected-store transaction cannot
compose existing named event persistence; historical mode capture requires a new
execution selector; a supported fork/lifecycle contract requires causal provenance
that the approved operation observation cannot represent; cleanup requires resurrecting
missing runs; or implementation uncovers another live same-concept projector outside
this census. Report and repair the gate rather than adding a framework or compatibility.

**Gate request:** approve this bounded projection/provenance repair, specifically the
required mode snapshot, transaction-local named log writer, unparented observational
semantics, and auxiliary failure disposition. No production implementation starts
until the independent outcome is explicitly posted on #2432.
