# Pre-Implementation Coverage Audit: Standing Operator Composition Amendment

Agent-g, #2498 / #2407 R1.1, 2026-09-30.
Binding ruling: https://github.com/division-sh/swarm/issues/2498#issuecomment-5905422933.
Current independent outcome: **approved for coding, not merge**, superseding the
rejection below: https://github.com/division-sh/swarm/issues/2498#issuecomment-5906100464.
The complete matrix and fail-closed no-op condition are binding. This remains
the pre-implementation artifact, not a final runtime proof audit.
Audit branch baseline: 73c35cc3e; production census: origin/master@4fccc57ec73ce54827167dddc5bbafac41ae66d2
and the unchanged standing controller/context-manager boundaries at audit time.
The approved implementation and measured receipts are tracked separately in
`issue-2498-implementation-progress.md`; no merge or parent closure is claimed.

## Class, Parent And Closure Decision

Category: high-risk lifecycle semantic drift / supported-surface and backend parity.
Observed entry point: compiled standing suspend succeeds, ordinary continue
correctly refuses, then preserved-suspension reset fails before its transaction
because suspension already retired the execution child. The endpoint and helper
error are entry points, not the audit boundary.

Chosen added working class: **standing operator transition composition when
durable desired state and process-child presence differ**. It covers all three
commands, their true no-ops, exact current versus historical identity, source
selection, child absence/presence, teardown/publication and failure compensation.
Immediate parent: standing desired-state / executable occurrence composition.
Broader runtime parent: generation-scoped executable lifecycle composition, #2250.
Coverage parent: #2407 R1.1. The previous reset-only framing was too narrow, not an
acceptable first slice; P16a-P16e in the stop artifact are superseded by the full
matrix below. This amendment is additive to P01-P16, M01-M23, RB1-RB4 and U1-U5.

Parent action: absorb the complete finite standing operator state product now
within #2498; do not split repeat suspend/resume or valid terminal/invalid reset
into follow-ups. Leave #2250's general phase orchestration and #2407's other
acceptance rows open. No previously closed issue is reopened or superseded.
This PR aims to eliminate every chosen class entirely. Intended final closure:
**failure class eliminated**, only after all named proofs and default qualification.
Current level: audited class expansion plus baseline controls/counterexamples;
no process repair, final-head audit, PR readiness, corpus or parent closure.

## Binding Governing Context

Re-read the issue body/thread, the prior corpus/readback/pause approvals, latest
independent rejection, and IMPLEMENTER_GUIDELINES.md / SEMANTIC_DRIFT.md.
Exact authoritative paths re-read:

- `flow_model.flow_instance_authoring.standing_activation_model.canonical_owners`,
  `.lifecycle_authority`, `.process_routing`, `.fail_closed`;
- `platform_tables.tables.standing_services`, `.standing_service_generations`,
  `.standing_service_journal`, `.run_control_state` and run lifecycle/source;
- `process_local_work_lifetime_authority.standing_lifecycle.{standing_recovery,standing_service}`
  and `.shutdown_and_store_release`;
- `api_specification.method_catalog["standing.suspend"]`, `["standing.resume"]`,
  `["standing.reset"]`, API idempotency and `run.pause` / `run.continue`;
- `cli_specification.command_catalog.standing_{suspend,resume,reset}`;
- exact provider publication, immutable source and selected execution-posture
  contracts used by the existing standing writer/activation consumers.

The lifecycle table already requires suspended reset, terminal-declared reset
under the latest declaration source, and explicit typed invalid-current reset
remediation. A validated `invalid_current` disposition is not a reader error:
its exact pointer/run/generation/identity relations are proved. Broken identity,
unreadable/missing relation, unknown enum or unavailable source is not reset
permission. Non-executable generations must not acquire executable children.
The blanket process-routing/drain/rollback wording needs the clarification
proposed below, not removal of the standing owner or weakening of reset validation.
No new standing product policy is proposed.

## Complete Durable State x Child-Presence Admission Matrix

`D` is produced by the canonical selected-store reader/writer, not cached API
output, status strings, reason/actor tags or a child-presence boolean. `C` is
observed and retained by RuntimeContextManager, bound to the selected loaded
runtime/source and exact service/run/generation/process occurrence. An exact
child below means one published, admissible child, not a stale/fenced/prepared one.
Matrix decisions are revalidated at the actual desired-state transaction.

| D and C before the command | Fresh suspend | Fresh resume | Fresh reset |
|---|---|---|---|
| active_intrinsic + exact child | Close ingress, exclude recovery, fence/park/drain, persist suspension, retire exactly that child | True durable no-op; preserve exact child, schedules, publication sequence and ingress; do not prepare/republish | Same drain; retire N, create N+1 under latest declaration source, retire captured child; materialize/publish only admitted executable N+1 |
| active_intrinsic + no child | Refuse inconsistent executable composition | Refuse; an already-active no-op is not permission to invent/reconstruct a missing child | Refuse inconsistent composition; no blanket missing-child exception or implicit source/startup repair |
| active_intrinsic + stale/foreign/multiple child | Refuse before durable mutation; preserve/fence through existing failure owner | Refuse, no fake no-op success or replacing the wrong child | Refuse, no retiring an unrelated child or advancing a pointer |
| suspended + no child | True new-key no-op; retain N, override, journal/domain facts and closed ingress | Clear override on exact N; prepare one fresh child, activate and publish before ingress reopen | Retire N/create N+1 atomically, preserve suspension; remain child-free and suppressed |
| suspended + any live child | Refuse inconsistent non-executable composition, not use repeat suspend as cleanup | Refuse; do not retire/reuse that child as an authorized resume | Refuse; no generic drain used to hide unexpected live execution |
| terminal_declared + no child, override none | Refuse with existing reset remediation; never change terminal N to paused | Refuse; never resume terminal N | Existing writer validates terminal predecessor/latest declaration; create and publish executable N+1, never reconstruct N |
| terminal_declared + no child, retained suspended override | Refuse terminal lifecycle transition | Refuse terminal resume | Existing writer admits reset; N+1 remains suspended and child-free until standing.resume |
| terminal_declared + any live child | Refuse inconsistent terminal execution | Refuse | Refuse; ordinary reset is not authority to repair an unproved live terminal child |
| validated exact-current invalid_current + declaration + no child | Refuse; suspended-looking columns are not proof of valid no-op | Refuse; active-looking columns are not executable/no-op authority | Reset only through existing exact relation/source/lifecycle remediation checks; next state follows retained override, then conditionally publish |
| invalid_current + live/stale/foreign child | Refuse, preserve fail-closed process ownership | Refuse | Refuse; invalid durable remediation is not permission to consume an unrelated/live non-executable child |
| orphaned + no declaration + no child | Refuse until declaration restore | Refuse until declaration restore | Refuse until declaration restore; do not supply a primary-context declaration |
| terminal_orphaned / undeclared invalid_current + no child | Refuse | Refuse | Restore declaration first through complete-set reconciliation; terminal N remains terminal_declared, then explicit reset; invalid orphan follows existing restore/remediation sequence |
| any undeclared/orphaned state + live child | Refuse inconsistent visibility; do not invent a declaration | Refuse | Refuse; existing removal/restore owner remains authoritative |
| unknown/unloaded/foreign service or ambiguous loaded declaration | Refuse at source/operator selection; no durable side effect | Refuse | Refuse |
| unprovable/broken pointer, missing run/generation relation, malformed identity/source/lifecycle/desired-state product | Reader/owner error, fail closed; not a no-op | Reader/owner error | Fail closed, not typed invalid-current reset permission |
| historical N != current pointer | Not current command authority | Not current command authority | Commands select service's exact current N; historical run facts cannot authorize current mutation or child publication |
| exact child already fenced, prepared but unpublished, retiring, or owned by another in-progress transition | Existing serialized operation/compensation retains it; competing command cannot infer live/absent from a map gap | No second publication or unrelated reopen | No new transition until existing owned completion/cleanup settles; stale coordinate refuses |

Active intrinsic includes a run-scoped generic pause; P01-P16 still governs new
dispatch. The child is not retired by generic pause and canonical standing
reconciliation may normalize it without changing N. This is distinct from a
persistent standing suspension. The same steady matrix applies after retained
startup and after destructive-reset reconstruction. It never admits work under
an absent child or treats "loaded declaration" as executable publication.

## Full Public Path And Gate Classification

| Ordered gate | Proof/owner and classification |
|---|---|
| Admitted source, listener, bearer auth, write:runs, compiled CLI relative operands | Different source/auth/invocation concept; existing H/golden/standing supported harness and CLI tests retain their own credit |
| API idempotency request admission | Different request-replay concept; fresh distinct keys reach the controller; replay of an old key returns its original result without another process transition |
| Serve supervisor reset fence and serialized standing controller | Same chosen composition class; retain loaded-context/operator lease and reject reset/shutdown overlap |
| AcquireStandingService | Same class at exact loaded declaration/source selection; no acquisition of a fenced execution child and no primary-runtime fallback |
| Gateway closure and pipeline parent exclusion | Same class; drain admitted ingress and exclude recovery before desired-state mutation on child and no-child paths |
| Exact durable observation plus process-child capture | Same class; names D and C without authorizing mutation from an earlier snapshot |
| Actual selected-store standing operation | Same class; locks parent before run, proves exact pointer/relation/source/posture and expected predecessor, decides no-op versus applied result |
| Captured child/scheduler teardown or no-child retained barrier | Same class; drain/join only real captured child, never fabricated Wait/Retire/Restore; cancellation cannot discard ownership |
| Committed successor / same-N resume activation | Same class for conditional child creation/publication; canonical activation/readiness/timer owners remain separate consumers, not reimplemented |
| Ingress reopen or compensation | Same class; reopen only admitted executable publication; failed no-child operation retains prior suppression, failed active operation restores only captured still-authorized predecessor |
| Public continue refusal / subsequent event execution / retained restart | Same class proof of command outcome and exact execution permission, with real serving generations on both stores |

Before the failing reset is reachable, initial source admission, standing
reconciliation/materialization, publication, CLI/auth, suspend drain and durable
suspension already succeed. Fresh repeat suspend/resume must reach the controller
with a new request key; keyed replay bypasses it and cannot prove a true no-op.
Terminal/revised-source and typed-invalid no-child cases must reach readiness
under their existing service-local exclusion, not by ignoring a boot refusal.

## Existing-Owner Implementation Proposal For The Re-Gate

1. Keep one serve composition path for the three standing commands. It consumes
   selected-store standing semantics and the existing context-manager occurrence
   owner; it does not interpret EffectiveState/reason strings independently.
   Extend existing operation/reconciliation values only with the minimum typed
   exact-current precondition and acknowledged changed/no-change evidence needed
   to preserve those facts across the process barrier. No new planner, persisted
   record, generic transition abstraction or parallel interpreter is proposed.
2. The exact selected-store observation determines what resources to capture,
   not mutation permission. The actual writer re-reads and validates the complete
   current product and expected service/run/generation/source before any no-op or
   write. A changed predecessor returns conflict/refusal with no mutation, not an
   application retry. Existing canonical classifier/reader errors fail closed.
   Harden early no-op branches to consume that proof rather than accepting two
   matching override/effective-state columns despite an invalid current product.
3. Existing RuntimeContextManager records exact present versus absent resources
   and prior suppression under its serialization. Existing serve transition owns
   pipeline exclusion independently of an optional real captured occurrence.
   No-child is a typed observation, not an empty successful execution transition.
   Leave low-level BeginStandingServiceTransition's no-occurrence rejection
   meaningful for paths requesting an actual child drain. No synthetic child.
4. Keep the pipeline parent exclusion and inbound gate through the transaction
   and captured predecessor disposition, including valid no-child mutations and
   no-ops. Never call Done early because there is no child. No database/run lock
   is held while awaiting ingress, child, callback or scheduler drain. Preserve
   parent-before-run and run-before-delivery ordering and existing successor
   activation/publication ordering; do not add a reentrant exclusion framework.
5. Active resume no-op preserves the actual exact child and never calls successor
   preparation/publication. Repeat suspended suspend is no-op without a drain.
   Suspended resume creates one child for N; executable reset creates one child
   for N+1; preserved-suspension reset creates none. Every publication uses the
   existing PreparedStandingServicePublication and canonical activation path.
6. Compensation is phase-aware in the existing retained transition: rollback of
   an already-suppressed/no-child command must not reopen ingress or call child
   restoration. Active rollback restores only the exact unretired child and
   parked schedules actually captured, and only if current durable authority
   still permits it; terminalization or a changed source/generation cannot reopen
   a stale child. Failure retains barriers and errors for joined cleanup.
7. Acknowledged commit plus cleanup/publication/reopen error keeps committed
   identity/outcome evidence; it cannot pretend the write rolled back, advance N
   again or restore predecessor authority. Use existing mutation acknowledgement
   and continuation/timer cleanup owners, not error suppression or a new retry
   loop. Same-key replay and fresh retry are separate proof cases.

This consumes two real authorities: durable command validity is owned by the
existing standing writer/reader, exact process resource disposition by the
existing context manager. A child query, API cache or string comparison is not
a third owner. One helper extraction is acceptable only inside these owners;
"shared helper" is not closure evidence.

## Exhaustive Systematic-Consumption Census

| Owner / every known relevant seam | Disposition and named execution proof |
|---|---|
| CLI standing.go -> apiv1 executeStandingServiceOperation / shared API idempotency | Already routes all suspend/resume/reset to the canonical serve controller; public new-key/replay rows S02/S04/S14, unknown identity S10 |
| Production mount in serve main.go, serveStandingServiceController all three entrances | Moved to the composed durable/process result; S01-S17, not reset-only treatment |
| admitProcessTransition, processLifecycleSupervisor reset/stop fence, controller mutex and RuntimeContextUse | Already own process/context admission; preserve and prove S16/S17 against reset/shutdown/two contexts |
| AcquireStandingService | Already exact loaded operator selection; all three production callers are this serve controller; S10/S17 prove ambiguous/foreign/unloaded refusal and isolation |
| closeAndDrain / serveStandingServiceTransition Wait, Restore, Retire | Moved to exact captured-child versus legitimate no-child disposition while retaining pipeline exclusion; S01/S02/S05/S07/S08/S11-S13/S16 |
| restoreAdmission / failClosedAfterReopen | Moved to restore only captured prior authority; S11-S13 include no-child rollback and active compensation; no unconditional gate reopen |
| publishActiveService -> PrepareStandingServicePublication -> Prepared.Publish/Discard | Already exact activation/new-child owner; call only on owner-acknowledged admitted publication, not active no-op; S03/S04/S06-S09/S13 |
| BeginStandingServiceTransition -> scheduler.ParkOccurrence -> actual Wait/Retire/Restore | Already sole exact process drain owner; only production caller closeAndDrain. S01/S11/S15 preserve actual held-work and exact scheduler restore; its absence guard is not globally weakened |
| newStandingOccurrencesLocked via Register / PublishResetRuntimeContexts | Already startup/reset child creation consumes suppression and exact target identity; S05/S07/S08/S16, no executable child for non-executable D |
| SuppressStandingServiceTargets / capability refresh / ingress selection | Already process visibility owner, called by non-executable startup and failed reopen; S05/S07/S08/S10-S13/S16 |
| RestoreStandingServiceTargets, WaitStandingServiceOccurrence, RetireStandingServiceOccurrence, unprepared PublishStandingServiceTargets | No production caller found in the repo census; tests are not a second live interpreter. Do not newly use them as an absence workaround. Retain/delete only if demonstrably superseded, not unrelated cleanup credit |
| StandingServicePersistence -> workflowInstanceStore -> PipelineCoordinator -> SQLite/Postgres standingServiceAdapter | Already one selected-store command owner; tighten exact transaction preconditions/typed no-op result consumption for all commands; S01-S14/S17 and existing dual-store source/invalid controls |
| adapter loadStandingServiceTx / readStandingRestartDispositionTx / requireStandingRunSourceTx / admitStandingServiceRunTx | Already parent/run/source/posture owner; validate before no-op/write, not only after mutation; S07-S12/S17 |
| standingdisposition.ReadByRun -> sole ClassifyStandingRestart | Already canonical complete-fact owner; no alternate serve table/classifier. Committed model is in pipeline; unchanged model moves to existing runlifecycle in the approved local candidate to remove the package cycle, without aliases/dual ownership |
| ReconcileStandingServiceSet / single reconcile / orphan / reset-required declaration restore | Already canonical declaration and non-executable-state producer; S05/S07-S09/S17, retain terminal N source and latest declaration source separately |
| PublishStandingService and PipelineCoordinator.ActivateStandingTarget | Already canonical publication-sequence/materialization writer; S03/S04/S06-S09 prove exactly-once activation and no no-op publication churn |
| Runtime PrepareStandingTargets / EnsureStandingServiceTargets / serveRuntimeContextStandingTargets / startupStandingWorkOwner | Already consume executable disposition and retain declaration for operators; S05/S07-S09/S16 prove retained startup and reconstruction ordering |
| InboundGateway Close/Wait/ReopenStandingServiceAdmission, resolved admission/catalog | Already gateway barrier and exact provider authority; S01-S05/S11-S13 public ingress plus resource-level gates; no duplicate alias registry |
| eventpersistence inbound_publication and sqlite_inbound_publication exact standing parent/publication/generation guards | Already consume canonical active publication and parent-before-run relation; S02/S03/S05-S13 prove unavailable no-child states and post-publication ingress, never authorize commands from publication alone |
| operatorsurface run API projections and runlifecycle run-origin relation validation | Already observation/identity validation, not standing command permission; S05/S07-S10/S14 preserve exact public origins and historical generation facts |
| runbundle owner and active_run_quiescence exact-current standing exclusion | Different generic non-standing availability/cleanup projection, not no-child command permission; existing standing exclusion/historical controls and S07/S09/S16 preserve boundary |
| Manager readiness, workflow-timer restore, EventBus standing recovery and continuation classification | Already consume standing restart/occurrence owners; additive negative no-work checks S02/S05/S07/S08/S10; original P/M/U tests remain mandatory |
| Generic run.pause/continue, pipeline/delivery claim, continuation/wake and global ingress | Already in approved pause class, not alternative standing commands; P01-P16/S05/S09/S17 preserve refusal, claim fencing and lawful release |
| Bundle removal, global shutdown/reset, run.fork, other selected-source/aggregate cleanup | Different semantic concepts, with explicit existing bundle/reset/shutdown/fork suites; S16/S17 exercise intersecting operator fencing only. Broader orchestration remains #2250, unsupported fork tail #642 |
| platformschema bootstrap and destructivereset cleanup catalog -> adminpersistence destructive reset | Different schema initialization/server-wide destructive deletion, not command permission; these are the only other standing-table write families found. Existing reset/cleanup catalog tests and S16 preserve exclusion and reconstruction |

Census: non-test symbol callers plus SQL standing_services/generations/journal
writers, process standing-map/NewStanding publication and deletion sites, gateway
admission, all command bindings, reset/startup and canonical disposition readers.
The six facade methods delegate to the same selected adapter; they are not
independent backend interpretations. The existing served suspend -> resume ->
reset test misses absent-child reset and new-key no-ops. The generic-continue
counterexample and no-child low-level probe remain baseline evidence, not proof
that every command must enter a child drain.

Old interpretations invalidated: unconditional child-drain permission for every
suspend/reset; unconditional new-child publication on resume; unconditional
precommit ingress reopen; two-column no-op acceptance without exact current
proof; treating a declared but non-executable generation as missing operator
authority. Existing low-level child absence errors, source/identity errors,
exclusive command serialization, real child joins and non-executable suppression
remain authoritative. No compatibility reader or second owner survives.

## Manifestations And Exact Planned Proof

All persistent/serve rows run on SQLite and PostgreSQL. Proposed test names below
are implementation obligations, not tests already added or green. Public rows
use existing real listener/H retained child generations plus compiled CLI and
authenticated /v1/rpc; H is internal mock lifecycle proof, not paid/live-provider
credit. Additional process-identity/scheduler/barrier observations are named
supplemental owner-level assertions, never invented public read endpoints.

| Row | Manifestation / current evidence | Required proof and exact oracle |
|---|---|---|
| S01 | Active child suspend/reset; existing selected/served control covers normal chain only | Extend TestStandingOperatorTransitionMatrixPublicBothStores: active suspend and active reset with a held admitted work lease; no SQL mutation until actual drain; exact N retirement/successor, no predecessor timers/agents/ingress |
| S02 | Fresh repeat suspend without child; independently reproduced both-store process failure, not G repair | Same public matrix, suspended/repeat_suspend: use a new key, not replay; success with same N, override, journal, publication and domain facts; no child, no gate reopen, signed ingress refuses |
| S03 | Suspended resume -> one exact child | Same public matrix, suspended/resume plus existing TestStandingPauseAuthorityPublicBothStores: CLI/RPC resume N, activate once, publish before reopen, real resulting turn/delivery/card settlement |
| S04 | Fresh repeat active resume; independently reproduced both-store preparation refusal | Same public matrix, active/repeat_resume after successful resume, new key: same child identity, N, routes/timers/publication sequence and domain facts; no new preparation, creation effect or ingress outage |
| S05 | Suspended reset twice and retained restart -> reset; public reset failure already executed | TestStandingResetSuspendedPublicRestartBothStores: N -> N+1 -> N+2 under distinct keys, each suspended/no-child; restart while suspended then another reset; generic continue CLI/RPC refuses every current successor; only standing.resume restores execution |
| S06 | Active reset successor publication | Public matrix active/reset: exactly N+1 plus one executable child, correct run/source, no retired-N projection; same-key replay does not create N+2 |
| S07 | Terminal declared reset without child, current/revised declaration source; public failure inferred, not executed | TestStandingResetTerminalDeclaredPublicRestartBothStores: real terminal checkpoint, joined stop, optional admitted source revision on cold restart, terminal N remains old-source/no-child; public reset creates N+1 under latest declaration, old evidence intact; retained suspended override variant creates no child |
| S08 | Validated invalid-current reset without child; public failure inferred, store controls pass | TestStandingResetValidatedInvalidPublicBothStores: controlled existing selected-store test setup produces provable typed invalid_current (not broken relation), retained boot excludes execution; public reset applies existing remediation/latest source, exact N+1 and retained override; separately corrupt relation must refuse |
| S09 | Orphan/terminal orphan/invalid orphan restoration | Public matrix orphan/restore: declaration absent refuses all three; restore by admitted cold declaration reconciliation, no live source replacement; terminal N remains terminal until reset, invalid orphan follows existing restore-then-reset; retained override governs child |
| S10 | Unknown/unloaded/foreign/ambiguous identity; broken or historical relation; active-missing and non-executable-live child | TestStandingOperatorInvalidCompositionPublicBothStores: table for each identity/relation/state/child mismatch and every command; canonical refusal, no pointer/generation/source/domain changes, no child fabrication or foreign retirement; broken boot authority refuses before readiness rather than faking a public-ready negative |
| S11 | Failure/cancellation before commit with active child | TestStandingOperatorTransitionFailuresBothStores: barriers at pipeline exclusion, ingress wait, scheduler park, child drain and selected writer fault; original exact child/schedules/gate restored only after real drain and current-authority check, primary/cleanup errors retained |
| S12 | No-child rollback/cancellation for suspended, terminal and typed-invalid products | Same failures test: fail after exclusion/before commit in each D; unchanged persistent facts, no Wait/Retire/Restore on nonexistent child, gateway remains closed and operator context retained; real recovery scan stays excluded until cleanup releases it |
| S13 | Acknowledged commit then retirement/activation/publication/reopen/cleanup failure | Same failures test: committed N/outcome remains inspectable, no rollback fiction or old-child reopen; capture ownership until joined cleanup, ingress remains unavailable until exact admitted publication; distinguish failures before versus after actual commit |
| S14 | API replay vs fresh no-op and repeated reset | TestStandingOperatorRequestReplayPublicBothStores: same-key replay after later generations returns original response without process/SQL effects; different keys execute S02/S04 and reset advances once per successful new operation; request conflict remains refused |
| S15 | Actual exact scheduler/held-work drain and restoration | Extend existing context-manager/scheduler occurrence transition tests with real one-shot/recurring callbacks and held ingress/agent/node lease; no child retirement, pointer advance or restore while callbacks still active; no copying predecessor schedule structs into successor |
| S16 | Composed reset/shutdown and retained startup child creation | Existing served lifecycle/reset harness plus TestStandingOperatorTransitionDuringProcessResetBothStores: held operation versus process reset/shutdown, selected owner joined, no stale parent/child publication, reconstruction preserves same steady matrix; no detached cleanup/store release |
| S17 | Durable/control/source/generation races and two runtime contexts | TestStandingOperatorTransitionAuthorityRaceBothStores: change exact pointer/source/override/current run behind the process barrier via existing writer; transaction refuses stale expectation with no partial mutation; rollback cannot revive now-terminal N; loaded sibling child/ingress/scheduler facts remain unchanged |

Public assertions exhaust event/delivery pages and run/entity identity; no loose
ID-membership oracle. Supplementary typed standing statuses/relations/timers and
process child/scheduler snapshots prove facts unavailable on public readback;
inspection handles close before restart and production data is never repaired by
test SQL. The typed-invalid setup is an explicit test-only precondition, not a
public product feature or permission to equate malformed state with remediation.
No resume-before-reset workaround, pre-created child or manual recovery signal.

## Evidence And Honest Limits

G independently reran, in the disposable audit baseline with host PostgreSQL:

```sh
go test ./internal/store/internal/runtimepersistence -run '^(TestTerminalDeclaredStandingResetUsesLatestDeclarationSourceParity|TestTerminalOrphanStandingRestoreAndResetUsesLatestDeclarationSourceParity|TestInvalidStandingResetUsesLatestDeclarationSourceParity|TestSuspendedStandingResetInstallsSuccessorBeforePauseParity)$' -count=3 -timeout=3m -v
```

PASS, 6.056s, all 24 backend subtest executions, no skipped backend. This is
**selected-store contract control**, not public terminal/invalid reset or a
process fix. The prior archived suspended-reset selected-controller probe fails
3/3 per store and the actual compiled public reset fails both stores. Lead's
new-key repeat suspend/resume probes and exact refusal reasons are independently
recorded at the binding ruling; G does not misreport those as his own reruns.
Terminal/invalid no-child public failure remains code-path inference until S07/S08.

The prior numeric candidate's both-store 32/100 unsettled death/restart/continue
and 100 terminal-delivery convergence remains useful limited evidence. It does
not pay standing process composition, remaining P/M/RB/U rows or mutation proof.
No full suite, live-provider call, final PR audit or review-ready PR is claimed.
Runtime/process implementation WIP and the separate #2008 WIP are preserved.

## Proposed Authoritative Spec Amendment

On approval, update platform-spec.yaml in the implementation PR, not only this
draft. Reconcile all four places rather than append a contradictory exception:

1. Standing lifecycle/process_routing: durable command permission and exact child
   ownership are distinct. Encode the full D x C table, fresh no-ops, valid
   no-child suspended/terminal/validated-invalid reset, restore-before-reset for
   orphans, invalid composition refusals and latest declaration versus run source.
2. process_local_work_lifetime_authority.standing_lifecycle: child drain/park/join
   is conditional on exact captured child; recovery exclusion/ingress barrier
   persists on no-child operations. No nonexistent occurrence waits, synthetic
   leases or blanket empty transition. Rollback preserves prior suppression;
   acknowledged commit cannot resurrect the predecessor.
3. API/CLI suspend/resume/reset descriptions: fresh repeats are valid owner-backed
   no-ops, not keyed replay. Resume preserves exact existing active child; reset
   preserves suspension and obeys source/remediation. Keep current response shape,
   auth/idempotency and source-selected errors; no new endpoint or cached policy.
4. Existing proof/profile contract: require the named dual-store public/new-key/
   restart/failure matrix while retaining all original corpus/readback/pause
   obligations. No new scheduler, schema, outbox, ledger or compatibility mode.

## Tracker, Watchlist, Architecture And Gate Request

Tracker decision: update #2498 body/current phase and additive audit, and #2407's
R1.1 status now; no child issue or new architecture issue. Prior corpus/readback/
P01-P16 gates remain valid within their recorded scopes. Latest process gate is
insufficient; this amendment requests approval, not self-approval.

Watchlist-backed promotion: runtime-operations.delivery_and_replay_ownership
already tracks durable-versus-process standing authority, startup/continuation
eligibility and exact retained lifetime. Its reset-only refinement is insufficient:
repeat no-ops and terminal/invalid no-child remediation are live sibling pressure
to absorb the whole finite operator product now. Refine that existing owner and
manifestation mapping and maintenance-and-cleanup.invariant_suite_coverage with
separate public/inferred/store-only credit. Keep existing broader recovery,
selected-fork, source preservation and throughput families separate; their
evidence does not justify a general runtime rewrite.

Watchlist repair landed on docs master at **swarm-docs@e83cad7**, rebased onto
3047263 before editing only those two nodes' files. Both YAML files parse and
git diff --check passes. The prior reset-only entry remains historical evidence;
latest action pressure records insufficient/widen-class and this full matrix.
Runtime/spec WIP diff hash before/after this audit is unchanged:
`ddf449ca1cc974aad64ae712e217fd89929906071e07df6d018e3e7710b763c7`.
No runtime edits or new production probes are hidden in this documentation repair.

Architecture feedback: two correct owners are incorrectly composed by a
consumer that infers command permission from executable child presence and
publication from success alone. Long-run direction: systematic owner-backed
typed mutation/result consumption, not another transition framework. Concrete
repair is tracked/promoted now in #2498; broader phase debt stays #2250 and the
existing watchlist. Rough effort: one bounded existing-owner repair plus the
full 17-row proof matrix, a few implementation/proof days, medium confidence;
high ROI against repeated valid commands, unavailable suspended/terminal services
and stale authority restoration. One PR is feasible with no persisted model
change. A reset-only endpoint repair would leave two proven live interpreters.

Remaining chosen-class tail after successful complete proof: zero. Currently all
new S rows still require implementation/qualification, plus original corpus tail.
Broader parent tail is not zero or exhaustive here: #2250 orchestration,
#642 selected-fork capability, #2394 throughput/#2453 backpressure remain separate
tracked groups (at least four workstreams, low confidence in total slices).
#2407 also retains other R1 rows; no parent completion estimate is fabricated.

Required supported-surface proof is S01-S17 plus unchanged P/M/RB/U, targeted
small go test, then final default go run ./cmd/swarm-test (no --full unless asked).
Stop conditions: missing fresh independent approval; another command/child/durable
interpreter or genuinely broader contract; inability to keep current owners,
lock order, retained compensation or executable proof without new framework;
unreachable checkpoint or nondiscriminating mutation. Do not freeze again merely
for a named row becoming red after this complete matrix is approved.

**Focused independent re-gate requested:** approve/reject this whole standing
operator composition class, D x C matrix, existing-owner precondition/result
consumption, no-child exclusion/rollback rules, proposed authoritative spec delta
and S01-S17 proofs. Keep process-lifetime coding frozen until the outcome is
explicitly recorded on #2498. No merge or whole-suite closure is requested.
