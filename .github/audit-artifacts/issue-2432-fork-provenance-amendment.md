# Pre-Implementation Coverage Audit: Additive Fork Provenance Amendment

Issue #2432; agent-g; cycle-1 repair, independent re-gate requested.
Production remains frozen. Freshly fetched origin/master is still `496498457`.
This amends audit `5e303a531`, not a restart of its eleven-family census or #2321's
approved design. Binding review:
https://github.com/division-sh/swarm/issues/2432#issuecomment-5588937989.
The complete updated issue and paired probe were read, as were the implementer and
semantic-drift guidelines. Watchlist refinement `swarm-docs@7a23362` is consumed.

## Correction and Scope

The previous selected-fork exclusion was wrong. Its manager writes the SAME durable
lifecycle/outbox, and both final activation validators interpret those diagnostics.
The previous universal removal of causal provenance, blanket different-concept
classification of fork validators, F7 shorthand and deferred contradiction stop
condition are superseded here. The contradiction is resolved in this proposed
contract now, not deferred to implementation.

Chosen class, parent chain, atomic event+ack design, mode snapshot, stable outbox
occurrence identity, existing codec/Tx owners, auxiliary-error separation and
F1-F5/F8-F11 remain. This is complete-class closure, not a first slice. No new
issue, logging/lifecycle framework, outbox, permission, compatibility or replay
policy is proposed. Both fork validators and the typed-root exception are absorbed
into #2432, not parked in #2250.

## Added Consumer and Producer Census

| Exact seam | Classification and disposition |
|---|---|
| `runforkexecution/runtime_container.go`: issue/claim runtime execution, Publish, selectedContractRuntimeContainerLineageContext | **Moved to canonical owner in this work** for immutable diagnostic-origin input. The existing selected runtime owner supplies exact issued execution/binding/generation facts, not a logger context flag. Both normal selected execution and historical contract-swap executor use this container. |
| `runforkexecution/agent_runtime_materialization.go`: startSelectedContractAgentRuntime, issueSelectedContractAgentRuntimeGenerationGrant, selectedContractManagerOptions | **Moved to canonical owner in this work**. Both zero-agent and concrete-agent managers keep the shared diagnostic role. MaterializeAdmittedAgent, Run, shutdown, failed preflight/materialization cleanup, quiescence and generation retirement are explicitly in scope. |
| `startupownership/process_capability.go`: generationGrant.CommitAgentLifecycleTransition | **Already consumes canonical lifecycle owner**, with **moved** diagnostic-origin carriage/validation. Exact process binding remains grant-owned; diagnostic provenance cannot replace it or authorize another lifecycle mutation. |
| `manager/lifecycle_coordinator.go`: replacement/materialization/start/retirement and terminal-flow entrances | **Moved to canonical owner in this work** for immutable origin request. Lifecycle ownership and exact topology validation remain unchanged. An accepted event-origin request is distinct from an unparented management observation. |
| `agentpersistence/lifecycle.go`: both transition/evidence writers | **Moved to canonical owner in this work** to validate and seal origin with outcome, outbox and mode. This covers normal and selected-fork actors, including dynamically materialized flow actors, not only initial records. |
| `runforkpersistence/selected_contract_runtime_execution.go`, selected binding/lineage owners | **Already consumes canonical owner**, extended only with a transaction-local validation port for diagnostic origin. Owns the exact execution ID, fork/source run, binding and generation relation; no copied authority interpreter in manager. |
| PostgreSQL `ensureRunForkSelectedContractExecutionForkState` in run_fork_selected_contract_execution_mutation.go | **Moved to canonical owner in this work**. Remove the unparented payload-tag UNION and consume exact durable observation validation outside causal tree recursion. |
| SQLite `ensureSQLiteRunForkSelectedContractExecutionForkState` in run_fork_selected_contract_sqlite.go | **Moved to canonical owner in this work**, same semantic classification with SQLite persistence proof, not SQL inspection credit. |
| RuntimeLogger/log adapters and selected receiver/MCP/tool contexts | **Same concept** when carrying lifecycle facts to the old projector or when supplying final validator tags; that authority path is removed. **Different concept, with proof** for ordinary selected-work causal logging: real accepted event -> verified persisted parent -> named log commit remains. |
| Public Execute/ActivateRunFork, served composition and catalog fork callers | **Already consume canonical selected runtime and validators**; extend successful execution+activation and negative proof through these entrances, not only seeded-event store tests. |

Searches additionally covered all production `LogRuntime`, `logRunRuntime`,
`logPublisherRuntime`, `logSessionRuntime`, EventBus `logRuntime` calls and all
`WithRuntimeLineage`/`RuntimeLineage` constructors in the selected container's
reachable owner families. The existing root exception is generic across ALL
platform.runtime_log records carrying the six matching details, not lifecycle-only.
Its live producer dispositions are:

| Producer family reachable through selected logger | Required disposition when root exception is deleted |
|---|---|
| Lifecycle materialize/start/finalize/retire/hydrate projector | The new immutable lifecycle provenance/observation relation below; no tag authority. Includes failure cleanup after a partial materialization. |
| Container `selected_contract_event_dispatched` (runtime_container.go:437) | Explicit committed fork EventID already supplied; preserve exact causal parent and test successful final activation. |
| EventBus publication/delivery/claim diagnostics (`eventbus_publish`, `eventbus_routing`, outbox delivery, sweeper event diagnostics) | Existing exact event subject/lineage; verify committed same-run parent. A pre-commit failure or nonexistent subject is not silently promoted by tags. |
| Manager accepted delivery, quarantine, panic, dead-letter and receiver-refusal diagnostics | Event-bearing calls retain exact event lineage. No-event failures (readiness retry, release-loop failure, orphan/reset diagnostics) have no lifecycle-outbox occurrence merely because their component is manager; they remain ordinary observations, cannot be exempted by tags and cannot authorize successful fork activation. Normal non-fork behavior remains unchanged. |
| LLM API/CLI/OpenAI/mock runtime, session adoption/rotation/watchdog | Managed session work runs under the accepted turn event; retain persisted subject and mode through session callbacks. Fork-chat is a different owner, not this container. No parentless session log receives a lifecycle exemption. |
| Pipeline handler/engine, activity, compute replay; tools and MCP | Preserve existing accepted-event or exact activity-request subject and canonical causal lineage. No causal parent from a component/action string. Existing activity-lineage owner remains binding. |
| Timer reconcile/register failure, readiness retry and EventBus no-event sweep failure | Error-only observations without exact causal evidence remain non-causal and non-exempt from selected activation. Do not hide or convert them to lifecycle observations. When produced under a genuine accepted event, preserve that parent. |
| Tests manually constructing the six details | Update the paired test: matching tags alone must now reject, exactly like absent/foreign tags. Add separate positive backed by actual lifecycle enqueue/projection. |

No second legitimate success-path producer requiring a new parentless exemption
was identified by this sweep. The error-only rows above are deliberately fail-closed,
not promised successful activation after a missing authority/dependency. Their tags
are not preserved as a compatibility seam. A newly proven supported success requiring
another noncausal domain exemption is a stop/re-gate condition, not an implicit #2250
deferral. No such additional class is claimed discovered or silently split here.

## Immutable Enqueue-Time Provenance

Proposed typed origin has two explicit dimensions: execution owner (`normal` or
`selected_contract_fork`) and causality (`observation` or `accepted_event`). It is
captured in the ORIGINAL lifecycle transaction, never at projection/retry. There is
no default for an unknown origin and no failed-causal-validation fallback to observation.

| Fact | Canonical owner and capture rule |
|---|---|
| operation/outbox/transition IDs, complete concrete agent identity, phase/generation/config revision | Existing lifecycle coordinator and selected-store outcome/evidence writer; exact equality with the recorded operation, transition and outbox columns. Stable projected EventID remains outbox UUID. |
| process/grant/runtime/artifact binding | generationGrant supplies ProcessExecutionBinding; selected store validates existing topology/process ownership in the transition. Persisted result retains it through cleanup. No read of the successor agent during projection. |
| actor execution mode | Exact admitted persisted descriptor in the original lifecycle transaction, including terminal transitions. Retain this as a typed provenance fact, not AgentRunMode or ambient posture. |
| selected fork execution identity | Existing selected runtime container supplies execution ID, generation, binding ID/identity and fork/source run coordinates through its manager's typed construction options. The selected-store runfork owner validates those exact facts against its durable execution and binding inside the lifecycle transaction. It must agree with concrete run, artifact and granted runtime generation. Grant ID alone does NOT identify a selected execution. |
| observation origin | Explicit management/materialization/start/cleanup request; parent, subject and handler absent. No fake event is manufactured for startup or shutdown. For a selected manager this retains the validated selected execution identity, even when the logger context later disappears. |
| accepted-event origin | Exact event supplied by the current owned delivery/flow mutation entrance, not arbitrary `RuntimeLineage` context. In the transaction, existing event/selected-lineage owners verify persistent event identity, same run, execution mode and the selected execution relation where applicable. Preserve its exact parent/subject rather than recomputing from an operation UUID or selecting a latest event. |

Store the closed provenance value as required JSONB/JSON on the existing outbox
(`provenance`, no default), sealed with the immutable lifecycle operation result in
the same transaction. Include the explicit requested origin in existing operation
identity/conflict checking so replay cannot alter it. The earlier required
`execution_mode` column denotes the diagnostic EVENT mode: actor descriptor mode
for an observation, exact accepted-event mode for a causal diagnostic. Keep actor
mode in the typed provenance when the two are distinct; do not collapse causal mode
and configured actor mode or reject a valid relation using manager container posture.
Canonical owners validate each fact against its own source, not against payload tags.

The selected origin validation port is narrowly transaction-local on the existing
runfork owner, bound through store composition; agentpersistence must not import
runforkpersistence back into an existing dependency cycle. This is not an additional
semantic owner. Existing process/topology and selected execution authorizations
still precede mutation. Invalid/foreign/missing origin rolls back the entire enqueue;
logging cannot create permission for materialization or provider work.

Once committed, provenance is HISTORICAL evidence. Projection and activation compare
the immutable occurrence with retained operation/binding/execution identity; they do
not demand a still-live heartbeat/lease, current agent, current context, or current
generation. Closed execution or retired grant is expected after successful cleanup.
Missing/corrupt historical evidence fails closed without reselection or resurrection.
Current-state lease checks apply at enqueue, not as invented retry requirements.

Ordinary causal tools/LLM/container logs do not need a lifecycle outbox. Their current
canonical source_event_id remains authoritative; typed diagnostic details may remain
presentation but never constitute another selected-tree root. This amendment does
not turn all logs into durable lifecycle operations.

## Validator / Spec Reconciliation

One read-only exact lifecycle-diagnostic validation operation on the EXISTING agent
persistence owner is consumed by projection replay and both fork validators. It
loads the specific durable outbox/operation/transition occurrence, validates typed
provenance through existing owners, and checks the exact canonical event ID, run,
producer/class/scope, mode, timestamp, payload, parent and no-delivery settlement.
For an already projected event it requires the corresponding committed ack; payload
outbox_id or runtime_lineage_* strings alone prove nothing. A pending unprojected
row is not a projected event. Missing, altered, foreign or multiply bound evidence
is corruption, not an observation exemption.

Both activation validators make two SEPARATE decisions within the existing activation
transaction and fork-run lock:

1. **Causal selected tree:** existing selected execution events and permitted causal
   descendants only. Delete the SQL UNION making matching typed parentless logs roots.
   The observation set never enters recursion. Exact persisted selected-work causal
   diagnostics remain eligible through their real parent chain.
2. **Permitted noncausal lifecycle observations:** exact event IDs returned by the
   lifecycle occurrence validator for this fork run. Exclude these records only from
   the final stray-event count; never from any execution/delivery/effect gate. A child
   whose only parent is an observation remains outside the selected tree and blocks
   activation. An observation alone cannot supply selected execution lineage, completed
   deliveries, settled effects, readiness or source-freeze permission.

For causal lifecycle diagnostics the real parent relation is checked; invalid causal
rows cannot be rescued by the observation path. The earlier gates for allowed source
events, exact recipient identities, selected activity provenance, completed deliveries,
quiescence and source state remain intact. Valid unparented lifecycle observations
therefore **do not prevent otherwise valid activation**, but **grant zero causal credit**.

There must be no read/validate/exempt race: projection and activation use the existing
fork/run transaction serialization. PostgreSQL RequirePresent in a write transaction
locks the run FOR UPDATE; obtain the same run lock before outbox lock/validation,
and keep consistent ordering with activation. SQLite uses its existing write owner.
Observe candidates and validate/exempt within that boundary, not a process cache or
an unlocked precomputed list. Include concurrent projection/activation in F7 proof.

Authoritative spec delta, to be promoted WITH implementation after approval:

- `agent_lifecycle_authority.public_restart.diagnostic`, `.recovery.rule`,
  `.transitions.terminal_flow`, and the outbox table: immutable typed provenance,
  causal versus observational occurrence, mode semantics and atomic projection/ack.
- `platform_tables.diagnostics_encoding.runtime_log_encoding` and diagnostic-direct
  subtype: exact occurrence-backed observation is non-routed evidence; a causal
  diagnostic retains its verified parent. No payload detail authorizes either.
- `run_model.fork.selected_contract_fork_local_runtime_typed_lineage`,
  `.selected_contract_fork_local_runtime_platform_event_lineage_policy`, and
  `.semantics.3g1b_selected_contract_fork_local_runtime_typed_lineage` /
  `.3g2_selected_contract_fork_local_runtime_platform_event_lineage_policy`:
  explicitly separate permitted historical lifecycle observations from causal selected
  output. Only the latter can participate in the selected tree; tag-based roots are
  forbidden. Exact observation evidence can prevent a false stray-event rejection,
  never satisfy execution/activation obligations.
- Mirror the same constraint in
  `runtime.run_fork.selected_contract_execution.fork_local_runtime_typed_lineage`
  and `.fork_local_runtime_platform_event_lineage_policy` so governing clauses agree.

No table, event-catalog member, broad platform allowlist, generic logging framework,
migration, backfill, compatibility tags, or new execution/replay permission is added.
Old stores without required snapshot facts remain unsupported. This proposed spec
delta resolves the observed code/spec contradiction; it does not ratify current tags.

## Expanded F6 / F7 Proof Rows

These supplement, not replace, the original families. New names below are planned
tests, not claims of tests already present or passing. Every row runs on real SQLite
and PostgreSQL; deterministic injection is additional to real backend readback.

| Row | Exact proof / required result |
|---|---|
| F6a normal observation | `TestLifecycleDiagnosticProjectionParity/normal_observation_provenance`: absent and poisoned caller context, exact stored run/actor/artifact/mode, no invented parent; one event/ack. |
| F6b causal selected work | `.../selected_causal_provenance`: actual admitted selected turn and lifecycle entrance plus ordinary tool/activity/dispatch log; exact persisted parent and mode preserved, canonical final selected validation succeeds. |
| F6c immutable selected observation | `.../selected_observation_snapshot`: real generation grant and materialization enqueue; validate execution/binding/actor identity from durable owners; no causal parent, no effect or delivery authorization minted. |
| F6d delayed / reopened projection | `.../selected_delayed_projection` and `/selected_reopen_projection`: defer projection, stop original manager/retire grant/close selected execution, reopen same store and project through another manager with absent or foreign run/source/mode/lineage context. Exact stored provenance and event cardinality survive, no new transitions or effects. |
| F6e foreign / malformed | `.../selected_provenance_rejection`: missing/foreign parent, wrong lineage run/artifact/event mode/actor mode/execution ID/generation/binding, mismatched outbox/operation/transition, missing historical owner record. Refuse at enqueue or projection before ack as appropriate; unchanged canonical rows and source state. No invalid-causal-to-observation fallback. |
| F7a complete supported success | `TestLifecycleDiagnosticSelectedForkSupportedParity`: existing admitted selected-fork path -> binding -> runtime issuance -> real materialize/start -> selected work/deliveries -> stop/quiesce -> projection -> final activation. Assert Activated=true and source_frozen/source_run_status under existing policy; exact operations/outbox/log/effect counts. Extend both direct runtime execution and served operator entrance; do not bypass earlier gates with only seeded events. |
| F7b observation is harmless, not authority | Same supported path with noncausal lifecycle rows before final validation succeeds; separate `/observation_without_selected_execution` and `/child_of_observation` reject with zero activation/source mutation. Do not put the observation ID in recursive roots. |
| F7c tags do not authorize | Adapt lead paired probe for both stores: exact-looking details without a real acknowledged occurrence, omitted fields, foreign run, copied real occurrence with a different EventID, and changed payload all reject. Positive uses actual lifecycle enqueue/projection, not the seeded tags. |
| F7d pending versus acknowledged | `/projection_activation_race`: barrier around projection commit and activation run lock; either full event+ack is observed or no event is observed. No transient stray-event failure due to reading halves, no lost diagnostic, no duplicate event on retry. |
| F7e cleanup/failure/retained history | `/partial_materialization_cleanup`, `/preflight_failure_cleanup`, `/provider_failure_cleanup`, `/historical_contract_swap`: preserve durable successful prefix and real failure, no false activation, delayed valid diagnostics remain projectable and never resurrect an actor. Cover both executor owners. |
| F7f ordinary no-parent rejection | Preserve `TestSelectedContractActivationRejectsUncausedForkLocalRuntimeLogDiagnostic` and tool-executor counterpart; add same cases with forged typed tags. No-event timer/readiness/sweep error logs gain no lifecycle exemption. Genuine causal session/activity/LLM/manager diagnostics keep their existing parent requirements. |

Existing regression entrances include
`TestExecuteSelectedContractRunForkMaterializesAndExecutesForkLocalAgentRuntime`,
`TestActivateSelectedContractRunForkExecutesReplayReadyContractSwapThroughSelectedRecipients`,
`TestExecuteSelectedContractRunForkProviderFailurePreservesEvidenceThroughCleanup`,
and served run_fork_runtime tests. Their current store coverage must be extended
where necessary, not relabelled as dual-store. Preserve source snapshots on all
negative exits; on positive activation assert only the already-authorized source
freeze behavior changes source state. Preserve distinct T/L/H credit in #2321.

## Evidence, Tracking and Gate Request

Lead's paired PostgreSQL named-commit/final-activation probe is accepted as reproduced
baseline evidence (exact tags accept; absent/foreign reject), NOT fix proof or SQLite
execution proof. Independent source inspection confirms matching SQLite interpretation.
Fresh focused `go test ./internal/runtime/correlation ./internal/runtime/core/eventreceiver
-count=1` passed (0.008s / 0.003s). No new runtime code, backend closure test, whole-suite,
served fork or live-provider result is claimed in this amendment.

Tracker decision: current #2432 body already records the corrected class and gate;
this amendment completes the specified owner/spec/proof repair. Consume existing
watchlist `7a23362` mappings to shutdown_and_runtime_lifecycle AND
timestamp_fork_replay_resume_ownership. It names the missing selected-fork producers,
both validators and tag-root contradiction; no additional edit/new issue is needed.
#2250 remains broader phase debt, not an unnamed parking place for any omitted
producer. #1927/#2418 remain closed. #2321 stays approved and acceptance-blocked
on the separately gated repair and F-owned #2319.

Parent promotion decision: absorb the complete fork interpretation into this same
class now; no first-slice exemption. Remaining known same-class child tail is zero,
moderate confidence pending supported fork and fault execution. Estimated repair
effort is now 3-5 engineering days including proof (previously 2-4); the increase is
for durable origin capture and dual-validator successful-path coverage, not a new
architecture. ROI remains high: one owner-backed relation replaces payload authority
while atomic projection closes duplicates and false shutdown conflicts.

Request one focused independent approval of this ADDITIVE amendment together with
the unchanged atomic/F1-F11 audit. Production diagnostic work remains frozen until
the gate is explicitly recorded. If another supported producer requires separate
noncausal authority, stop and name the concrete class/tracker decision rather than
silently retaining the old tag exception or expanding the logging model.
