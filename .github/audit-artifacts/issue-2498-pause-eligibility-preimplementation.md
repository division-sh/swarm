# Pre-Implementation Coverage Audit: Pause-Aware Work Eligibility Amendment

Agent-g, #2498, parent #2407 R1.1, 2026-09-30.
Audited production baseline: `origin/master@4fccc57ec73ce54827167dddc5bbafac41ae66d2`.
Candidate/probe base: `5a8167d0b`, including the separately approved readback repair `44a5f88fb`.
Binding independent ruling: [insufficient; widen class](https://github.com/division-sh/swarm/issues/2498#issuecomment-5902935619).
**Runtime implementation remains frozen; this is a repaired gate request, not approval or closure.**

This additive amendment supersedes the paused-stop artifact's proposed recovery-disabled workaround. The original corpus audit, 23 M rows/U1-U5 and four readback rows remain required, not restarted or credited by these probes. No production code, authoritative runtime semantics, original #2008 WIP, external handover, provider traffic or test framework changed in this audit.

## Classification And Binding Context

- Category: semantic drift / executable-work eligibility / SQLite-PostgreSQL parity, reached through supported e2e regression coverage.
- Symptom: after public pause at 32/100 issued, 68 owed, 32 events and 32 nonterminal deliveries, retained recovery-enabled startup refuses readiness. The six supported process executions already recorded are the entry point, not the audit boundary.
- Additional execution-proven manifestation: the continuation scanner selects paused handed pending/failed/stale-in-progress agent work; actual AgentManager `OnEvent` executes and delivers before continue. Direct continuation dispatch through the real Pipeline/engine also authors a node event while paused.
- Additional mechanical counterexample: acknowledged handed-only continue examines zero pipeline candidates and sends zero notifications to the installed real continuation owner on both stores. Successful public wake remains a required post-repair proof; the probe does not start a coordinator that cannot yet park correctly.
- Additional same-concept parity manifestation: PostgreSQL rejects continue without operator control; SQLite manufactures running control from lifecycle `paused` alone. Existing materialized-like paused API coverage is PostgreSQL-only.
- Exact concepts: active/reconstructable versus executable-now run state; operator pause provenance; fresh pipeline/delivery admission; retained continuation ownership and post-continue wake. Do not redefine causal execution mode, topology ownership, receipt/handoff semantics, terminal settlement or fork authority.
- Added chosen class: **pause-aware executable-work eligibility across startup, continuations and public continue**, including provenance parity. This PR aims to eliminate that complete class, not only remove a startup error. Existing corpus/readback classes also remain complete-closure obligations.
- Immediate runtime parent: delivery/recovery admission and continuation ownership. Broader runtime parent: generation-scoped executable lifecycle composition (#2250/watchlist). Coverage parent: #2407's reusable acceptance/merge-gate program.
- Framing: old startup-only framing is too narrow. This repaired class is broad enough and finite; do not absorb all delivery/replay, startup orchestration, performance or selected-fork capabilities.

Exact governing sections re-read:

1. `platform-spec.yaml#run_model.lifecycle.pause_resume`, `api_specification.methods.run.pause` and `run.continue`: halt new dispatch, permit already-dispatched handlers to complete; only committed operator pause authorizes ordinary continue.
2. `platform_tables.tables.run_control_state` and `platform_tables.tables.runs.terminal_evidence.continue_post_commit_recovery`: persisted control distinguishes fork-materialized pause; the transition commits before recovery; post-commit failure cannot invite transition replay.
3. `durable_pipeline_processing_obligation_authority.claim_contract.{recovery,scan,scan_blockage,startup_blockage_evidence,scan_result,startup_timer_handoff,startup_creation_handoff}`: bounded membership/cursors, fresh eligibility, remembered genuine blockers, explicit exhaustion, exact cleanup and ordered handoff.
4. `process_local_work_lifetime_authority.durable_handoff.delivery_continuation`: one exact generation/carrier/attempt authority, explicit return/consume/terminal resolution, joined retirement, no polling workaround.
5. `engine.dynamic_instance_lifecycle.creation.route_materialization` and `cli_specification.command_catalog.serve`: active paused topology reconstruction remains mandatory; runnable pipeline work settles before continuation release, autonomous producers and public readiness.
6. Startup recovery-disabled admission and typed delivery inventory, `durable_data_resources.deployment_origin`, fan-out serving grant/run eligibility and selected-fork control restrictions remain binding.
7. Standing restart policy is distinct: `standing_reconcile` may normalize an active intrinsic standing declaration; standing suspension persists separately. This amendment must not change that existing contract into durable generic run pause.

The existing spec says all remembered `Blocked`, including run dispatch blockage, refuses startup while also supporting pause/continue. The proposed clarification below moves lawful operator-paused debt outside **runnable** selection, not outside debt inventory. An arbitrary dispatch refusal is never evidence of lawful parking. This contradiction is submitted for approval rather than silently resolved. Guidelines and SEMANTIC_DRIFT were re-read.

## Full Supported Path And Gate Classification

| Ordered gate | Must succeed / exact owner | Disposition |
|---|---|---|
| YAML-only source admission, auth, listener, immutable source/grant | Existing compiled H composition, real verify/CLI/RPC; hostile cwd and relative operands | Different invocation/source concept, original corpus gates/proofs retained |
| `run start --data`, permanent creation receipt, feed pin | Deployment creation owner; exact request/version/feed binding | Different creation concept, original M01-M11 controls retained |
| First issuance, receiver construction, timers and public partial checkpoint | Ordinary publication/typed readiness; publicly observe a positive partial feed then acknowledge pause | Same class at pause linearization; exact numeric/timer semantics remain original corpus owners |
| Forced death, predecessor fencing, fresh serving generation | Existing process/startup ownership | Different generation concept, existing real child-death/fencing controls; no manual state repair |
| Recovery-policy and source-scoped topology preflight | Runtime typed inventory and readiness projection | Same class for paused debt accounting; missing/foreign/corrupt authority remains fail-closed |
| Mandatory Manager.Run/completed topology reconstruction | Existing topology/lifecycle owners for both running and paused active runs | Already consumes active lifecycle; do not remove paused reconstruction |
| Runnable pipeline exhaustion, both ordinary and decision-route phases | Selected pipeline owner -> EventBus -> RecoverToExhaustion | Same class; paused debt parked without claim/receipt/handoff, running/global blockers still fatal |
| Delivery generation adoption, continuation start/sync, agent and node claim | Delivery lifecycle -> one coordinator -> EventBus -> Manager/Pipeline | Same class; preserve exact authority, no premature new claim or recipient effect |
| Schedule/timer/maintenance release and public ready | Existing ordered startup producers; their ordinary publications consume bus/delivery admission | Already consumes corrected dispatch/claim entrances; no scheduler rewrite or callback suppression |
| Public `run.continue`, request replay, postcommit recovery | apiv1 -> source-selected Controller -> selected lifecycle/control transaction -> existing pipeline/continuation signals | Same class; require exact operator pause and wake handed work even with zero pipeline candidates |
| Feed suffix and all pending deliveries converge | Existing fan-out owner/pump and delivery settlement | Same class for fresh eligibility and once-only release; throughput #2394 remains separate |
| Exact receipt/cardinality/timer/numeric assertions, clean child stop | Original corpus owners and supported RPC/CLI plus named typed-store supplement | Original coverage class, no substituted status-only success |

## One Eligibility Contract, Without A New Owner

The existing run-lifecycle/control owner remains semantic authority. Extend its closed typed classification/private selected-store admission as needed; do not create a scheduler, execution registry, queue, ledger or parallel pause predicate. `runstate.Active` and shared `ActiveStateSQLValues` retain running **and paused** for topology, outstanding debt and allowed in-flight settlement. Execution selectors and claim entrances must consume the separate canonical dispatch classification, not change the universal active set to running.

| Persisted / authority posture | Topology / debt | New ordinary execution / startup |
|---|---|---|
| running, absent or running operator control, valid normal authority | Reconstruct and retain | Runnable; posture/source/generation/materialization still checked |
| paused with exact paused operator-control fact | Reconstruct and retain unchanged debt | Parked; no new pipeline/delivery attempt, node handler or agent OnEvent until continue |
| paused without operator control, fork/materialization state | Preserve exact fork evidence | Not ordinary continue authority or normal-runtime permission; only existing exact fork activation/execution rules may authorize it |
| completed/failed/cancelled/forked or stopped control | Existing terminal/lineage rules | No new execution; terminal fence/cleanup is not pause parking |
| contradictory, unknown, absent-parent or malformed source/control/authority | Diagnose through existing validation | Fail closed, never silently classified as paused or excluded as harmless |
| global runtime ingress paused | Separate persisted ingress owner | Existing global startup refusal remains; a run-local parking rule cannot override it |
| standing suspended / abandon / active intrinsic reconcile | Existing StandingRunRestartDisposition | Preserve suspension/abandon exclusions and existing active-declaration normalization; no new standing policy |
| runless allowed platform/control work | Existing origin/source rules | Unchanged explicit platform policy, not an inferred running run |

Pause versus claim must linearize through existing admission/selected-store ownership. A pipeline candidate snapshot or carrier acquired before pause is not proof of an executable delivery attempt. Recheck before new route dispatch and at the exact durable agent/node claim, under the existing parent-run transaction fence. A claim that actually won before pause is already in flight and may finish; its later queued recipients/follow-ups must respect the new pause. If pause wins, return/retain the exact continuation without incrementing attempts, retries or errors, fabricating receipts or losing wake. Preserve parent-before-delivery lock order and do not hold a run lock throughout handler execution.

Normal-source startup authority adoption is still necessary. Compare parked executable facts after that explicit generation adoption; only the required authority/generation rebind and predecessor-attempt expiry may differ from the pre-death snapshot. During the subsequent paused scan/dispatch interval, event bytes, feed cursor, routes, receipts, handoff markers, claim versions, retry/final selection/error facts must be identical. Do not call an expected generation rebind business execution or waive unexplained mutations.

## Exhaustive Systematic-Consumption Census

`moved` below means **proposed for this work after gate**, not already implemented. Canonical owners are actual persisted lifecycle, pipeline and delivery owners, not the nearest helper. Every known production consumer is classified; facade/generated forwarders delegate, not independent interpreters.

| Owner / consumer family | Disposition and concrete entrance |
|---|---|
| Run lifecycle/control transaction, both stores | Already owns persisted status/control and lock order. Move SQLite `continueRunControlTx` to exact operator-control provenance, matching PostgreSQL; no control upsert from a materialized-like pause |
| `runcontrol.Controller`, `RunDispatchBlocked` and EventBus `RunDispatchGate` | Already owns operator command/dispatch control. Move bool-only consumption where necessary to canonical typed parked-versus-blocked classification; errors remain errors |
| Pipeline ordinary and decision-route candidate pages, ascending/reverse boundary capture | Move `postgres/sqlitePipelineCandidatePage` through runnable classification without excluding invalid authority or changing phase/cursor/high-water rules |
| Pipeline exact-event hydration/recheck, scan and direct claims | Move `postgres/sqlitePipelineEligible`, recovery/decision purposes and fresh dispatch admission together; publication exclusion still permits committing accepted queued events |
| Pipeline inspection (`Inspect`, global presence and oldest eligible), run snapshots | Align processing-eligible projection with runnable selection; keep total outstanding/debt/completion facts inclusive of paused rows |
| `PreflightResume`, mock-only event/route admission | Different posture concept, already canonical; preserve full run-debt preflight before continue, including paused work rather than skipping it as non-runnable |
| EventBus startup/global/periodic/run/ingress sweeps, `publishClaimedPipeline`, decision-route execution | Move confirmed operator parking to non-execution/retention; unexpected blocked running work, retry release, busy claim and global pause retain existing `Blocked` semantics |
| `RecoveryManager.RecoverToExhaustion` | Already consumes explicit SweepResult. Preserve fatal genuine Blocked and explicit exhaustion; no string filtering or blanket accept-Blocked change |
| Delivery `Adapter.ScanContinuations`, both ordinary and selected authority arms | Move normal runnable projection; selected arm retains exact current selected-execution fence. Paused normal work must not be scheduled as executable; held entries must remain owned and re-observable |
| `Coordinator.Start`, `Synchronize`, scan/reconcileHeld, schedule and workers | Already single generation owner; move consumption of canonical parked facts, correct existing signal reconciliation and missing-row interpretation. No polling, second coordinator or parked-work registry |
| EventBus `DispatchDeliveryContinuation`, direct/publication election | Move fresh pause/global gate **before** interceptors/receiver queue; return exact carrier on a race, never Fatal for confirmed operator parking or ordinary success without execution |
| Delivery `ClaimDelivery`, both selected-store named transactions | Move fresh execution admission under the existing parent fence; active-source guard alone is insufficient. Keep source, generation, selected grant, materialization, busy/terminal and malformed-route checks |
| Manager dequeue/claim/OnEvent and Pipeline workflow-node claim/engine transaction | Already consume the delivery owner; handle its typed deferral without calling OnEvent/engine or spending a retry. Audit both exits/carrier resolution, not just bus queue admission |
| Retry settlement, renewal, success/failure, attempt cleanup and terminal release | Different already-in-flight settlement concept; retain exact accepted-attempt rights and owner joins. Pause must not revoke successful evidence or terminalize the debt |
| Delivery `InspectRecovery`, Runtime `inspectDeliveryRecoveryInventory`, `HasStartupBlockingRecoverableWork` | Different debt inventory versus runnable selection. Preserve pending/failed/in-progress counts even when paused; recovery-disabled startup still refuses retained debt on both stores |
| Runtime prepare/preflight/revalidate/start, managed activation and source-scoped completed topology | Already owns fixed ordering and active reconstruction. Preserve reconstruction -> runnable pipeline exhaustion -> continuation start/sync -> autonomous release -> ready; no unconditional optional recovery |
| `ReleaseRunQueue`, Controller postcommit recovery, bus `SignalDeliveryContinuations` | Move coupled wake: after acknowledged continue, existing pipeline owner drains event work and existing coordinator is signaled even when examined=0. Keep fan-out turn wake/candidate evidence under its existing owner; no pipeline receipt re-dispatch |
| apiv1 operator handlers, runtime-by-run selection, API idempotency, compiled CLI pause/continue | Already delegate to canonical control/source owner. Prove both-store public calls/replay and no duplicated effects; do not add endpoints or rebuild pause semantics in CLI |
| Fan-out `runAcceptsFanOut`/eligibility SQL, claim/commit grant and signal | Already excludes operator pause from **new turns**; permits previously accepted commit/settlement. Preserve B18 prefix/suffix, wake fresh suffix after continue, prove both handler/deployment origins |
| Standing writers/disposition and reconcile/suspend/resume/reset | Different standing lifecycle policy, but writes the same control facts. Already uses existing transaction and restart disposition; prove no change to active-intrinsic normalization or suspended exclusion |
| Workflow/generic timers, decision cards, external ingress and creation publishers | Different occurrence/verdict/creation concepts; they may persist canonical queued work. All new receiver execution must consume the corrected publication/continuation/claim entrances; no timer/outbox/decision rewrite |
| Historical/materialized/selected forks, terminal freeze, replay and source-scoped isolation | Different explicit authority; preserve fork-specific execution/activation and control refusal. Never authorize ordinary execution solely from paused/active lifecycle or rewrite selected source rows |
| Manager startup effect reconciliation/provider-drain/budget/lifecycle diagnostics | Different accepted-attempt observation/settlement; `HydrateForStartup` does not perform ordinary new recipient dispatch. Preserve current completion/diagnostic owner behavior, not a new recovery phase |

Census used repo-wide callsites for `PauseRunControlOutcome`, `ContinueRunControlOutcome`, `RunDispatchBlocked`, `ClaimDelivery`, `ScanDeliveryContinuations`, `DispatchDeliveryContinuation`, `ReleaseRunQueue`, `SignalDeliveryContinuations`, `StatePaused`, `run_control_state` and active SQL predicates. Important private files: `runlifecycle/run_control{,_sqlite}.go`, `runstate/active_guard.go`, `pipelinepersistence/owner_operations.go`, `delivery/{adapter,lifecycle}.go`; runtime bus publish/routing/sweeper, continuation coordinator, manager runtime, workflow-node delivery, run-control Controller and runtime startup diagnostics/managed composition. The previously omitted delivered-work seam and SQLite command interpreter are absorbed, not split or left behind.

Invalid old paths: using active lifecycle as new-execution proof; selecting paused debt as a required runnable startup blocker; scheduling handed deliveries without fresh pause admission; SQLite continue creating its own authority; relying on an empty pipeline sweep to wake continuations. Surviving authoritative paths: active topology/debt inventory, in-flight settlement, selected fork fence, standing policy, explicit failed/blocked recovery and existing generation cleanup. No compatibility, schema, migration, new abstraction or old-store preservation.

## Manifestation / Exact Proof Matrix

All P rows require SQLite and PostgreSQL. Test names marked planned are implementation proof obligations, not execution receipts. Baseline counterexamples are preserved as an audit-only patch, not enabled tests that normalize defective behavior.

| Row | Known manifestation / planned discriminating proof | Pre-gate evidence |
|---|---|---|
| P01 | Unhanded operator-paused ordinary event; planned `TestPausedUnhandedStartupAndContinueBothStores`: ready, zero claim/handoff/receipt while paused, same event executes once after public continue | `TestAuditPausedUnhandedRecoveryRefusesThenContinueReleasesBothStores` count3: startup refuses, existing Controller continue releases exactly one; actual compiled 32-row restart refuses 3/3 both stores |
| P02 | Handed pending agent; planned `TestPausedHandedAgentStartupAndContinueBothStores/pending`: OnEvent counter stays zero, exact durable snapshot unchanged until continue | `TestAuditPausedHandedAgentExecutesBeforeContinueBothStores/pending` count3: real coordinator/Manager executes and delivers while paused |
| P03 | Handed failed due/future agent; same proof `/failed_due` and `/failed_future`, preserve retry timing/count | Audit `/failed` count3 executes due retry while paused; future timer eligibility remains separately required, not credited |
| P04 | Dead predecessor in-progress lease; same proof `/stale_in_progress` and `/live_claim`: exact old attempt retirement only, no successor attempt before continue | Audit `/stale_in_progress` count3 executes while paused; deterministic fixture expiry keeps legal attempt facts. Live-claim control and genuine restart remain required |
| P05 | Handed node mutation; planned `TestPausedHandedNodeStartupAndContinueBothStores` with actual engine state/output/receipt assertions | `TestAuditPausedHandedNodeMutatesBeforeContinueBothStores` count3: real Pipeline/engine authors `selected` before continue; test carrier is the existing component fixture, not process-restart credit |
| P06 | Mixed paused/running in one source and two source contexts; planned `TestPausedMixedRunsStartupBothStores`: later running work settled before ready, paused event/delivery/feed facts unchanged, no paused registry claim | `TestAuditMixedPausedRunningRecoveryBothStores` count3: running sibling gets one receipt and paused delivery unchanged, but startup still refuses; two-context full startup remains required |
| P07 | Already-handed-only backlog has no pipeline candidate; planned `TestPublicContinueWakesHandedOnlyBothStores`: establish a genuinely parked coordinator, public continue once/replay, observed rescan and one real agent/node execution without manual Signal/Synchronize | `TestAuditHandedOnlyContinueDoesNotSignalBothStores` count3: real acknowledged Controller continue, examined=0, installed real coordinator receives zero notifications and pending handed snapshot is unchanged. Coordinator is not started in this mechanical signal probe; successful public wake **not proved yet**. Planned test must fail if the continue signal is removed |
| P08 | Mixed unhanded/handed plus feed suffix; planned compiled `TestGoldenNumericDataScatterParkRestartBothStores` and `TestPublicContinueReleasesBothBacklogsBothStores`: 32-row checkpoint, fresh generation ready while paused, exact old/new IDs and100 total after continue | Public checkpoint/refusal reproduced six times; B18 accepted prefix/suffix controls green. No successful restart or combined release credit |
| P09 | Running dispatchBlocked, claim busy, typed retry release, global ingress pause before/during scan | Existing `TestStartupRecoveryClassifiesBlockedBranchesOnBothStores` count3 and `TestRecoveryManager{StartupRequiresExplicitExhaustion,RejectsBlockedEvenWhenScanExhausted}` count3 green; preserve all five branches |
| P10 | Recovery disabled with pending/failed/live/reclaimable node/agent debt, including operator-paused variants | `TestRuntimeStartRecoveryDisabledRejectsExecutableDeliveryInventoryParity` count1 green across both stores/modes; add paused variants. Never hide paused inventory or switch the process proof's flag |
| P11 | Fork/materialized-like paused rows, absent/foreign/retired selected grant and terminal/stopped ordinary admission | `TestAuditUnownedPausedContinueBackendDivergence` count3: PG rejects, SQLite grants ordinary running authority. `TestSelectedDeliveryExecutionFenceBothStores` count3 green. Add actual both-store normal-fork no-execution/control proof and terminal snapshot/claim tests; no successful unsupported timer fork claim |
| P12 | Pause wins after selection/election but before durable claim versus claim wins before pause; handoff races and progress preservation | Planned `TestPauseClaimAndHandoffLinearizationBothStores`, deterministic barriers at existing owner boundaries: first ordering zero attempt/effects, second permits only exact in-flight completion. B18 real-pause/prepared/expiry/retirement controls green; not equivalent to full handed race proof |
| P13 | Rollback/cancellation/independent postcommit cleanup and generation retirement while parking/continuing | Existing `TestRunControlAcknowledgedCleanupFaultPreservesFollowUpBothStores` green. Planned `TestPausedContinuationCleanupAndContinueFailureBothStores`: no outstanding claim/carrier/join, no false transition replay; committed running wake retained despite caller cancellation/cleanup failure |
| P14 | Malformed control/source/origin, wrong authority and globally blocked context cannot become harmless parked debt | Planned `TestPausedEligibilityRejectsMalformedAuthorityBothStores`, plus existing selected fences and blocked controls; missing parent/control contradiction fails closed without dispatch or fabricated settlement |
| P15 | Standing intrinsic normalization versus durable standing suspension and accepted schedule/timer publication while a normal run is paused | `TestStandingReconciliationNormalizesRunPauseForActiveDeclarationParity` count3 green. Preserve existing suspension tests; planned queued timer/decision publication verifies dispatch debt parked rather than dropping canonical occurrences |

The P01-P15 names are supplemental to, not replacements for, M01-M23/U1-U5 and RB1-RB4. The required supported surface is the unchanged compiled CLI/RPC, real serving generations and exact public facts; component fixtures alone cannot qualify P07/P08/public continue. Agent evidence is an actual recording Agent.OnEvent, not paid-provider or live-server credit. Node evidence is actual engine execution, not a mocked callback. No passing baseline counterexample is reported as a repair.

## Proposed Authoritative Spec Delta For Approval

Implementation must update `platform-spec.yaml` in the same PR; this draft alone is not a merge artifact.

1. Clarify pause_resume/run_control_state: `active` permits reconstruction and existing owned settlement, not new execution. Ordinary continue requires lifecycle paused **and** persisted operator-control paused on both stores; fork-materialized state is insufficient.
2. Amend pipeline claim_contract recovery/scan/scan_blockage/startup evidence: lawful operator-paused ordinary debt is non-runnable and retained; only runnable candidates enter startup exhaustion. A pause race receives owner-backed parked classification, not arbitrary Blocked suppression. Unexpected refusal of a running candidate, busy claims, malformed authority, retry blockage and global ingress pause still refuse readiness.
3. Add pause consumption to durable delivery continuation/claim contract: normal handed pending/failed/stale work stays owned, no new attempt or recipient effects while paused; typed parked retention is not terminality, failure or successful delivery. Fresh store admission and existing parent-run lock order resolve races. Exact selected/fork authority and already-in-flight settlement remain unchanged.
4. Extend existing continue_post_commit_recovery with exact-generation continuation notification even when pipeline examined=0, plus existing fan-out wake. Signals follow committed transition and cannot be lost to caller cancellation; errors retain committed-transition evidence and ordinary retry/idempotency rules. No new queue or recovery result owner.
5. Clarify source-scoped startup order: mandatory reconstruction of running/paused topology -> runnable pipeline unblocked exhaustion -> runnable continuation start/sync -> existing autonomous producers -> readiness. Recovery-disabled retained debt still refuses, independent of pause; global ingress remains separate.
6. Preserve standing reconcile/suspension, accepted-prefix fan-out commit, terminal/fork control restrictions and existing fresh-store-only policy. Do not alter grammar, cause a startup reorder or invent execution permission for unsupported fork work.

## Probe Receipts And Reproduction

Disposable tree: `/home/youmew/dev/swarm/worktrees/agent-g-2498-pause-audit`, detached at `5a8167d0b`, test-only overlay. Preserved `.github/audit-artifacts/issue-2498-pause-eligibility-probes.patch`, SHA-256 `5cf6e55ed804f1fb2dc6f06faa09592dbc62883327b69a03b14de57421e69bdc`. Apply only to a disposable checkout of that base (or unchanged corresponding tests). Do **not** enable defect-expecting probes in production CI.

```sh
git apply .github/audit-artifacts/issue-2498-pause-eligibility-probes.patch
go test ./internal/runtime/bus -run '^TestAudit(Mixed|Paused)' -count=3 -v -timeout=3m
go test ./internal/runtime/bus -run '^TestAuditUnownedPaused' -count=3 -v -timeout=3m
go test ./internal/runtime/bus -run '^TestAuditHandedOnlyContinue' -count=3 -v -timeout=3m
go test ./internal/runtime/pipeline -run '^TestAuditPausedHandedNode' -count=3 -v -timeout=3m
go test ./internal/runtime/bus -run '^TestStartupRecoveryClassifiesBlockedBranchesOnBothStores$' -count=3 -timeout=3m
go test ./internal/runtime/pipeline -run '^TestRecoveryManager(StartupRequiresExplicitExhaustion|RejectsBlockedEvenWhenScanExhausted)$' -count=3
go test ./internal/runtime -run '^TestRuntimeStartRecoveryDisabledRejectsExecutableDeliveryInventoryParity$' -count=1 -timeout=4m
go test ./internal/store/internal/runtimepersistence -run '^(TestB18.*BothStores|TestRunControlAcknowledgedCleanupFaultPreservesFollowUpBothStores)$' -count=1 -timeout=4m
go test ./internal/store/internal/backend/delivery -run '^TestSelectedDeliveryExecutionFenceBothStores$' -count=3
go test ./internal/store/internal/runtimepersistence -run '^TestStandingReconciliationNormalizesRunPauseForActiveDeclarationParity$' -count=3
```

All commands above completed successfully as baseline counterexamples/controls, with host PostgreSQL; no backend skips. Local output files are `/home/youmew/.cache/agent-g-2498-pause-audit-{bus,unowned,wake,node,blocked,exhaustion,disabled,b18,selected,standing}.log`. The original supported failure log/hash remains in the stop artifact. No whole-suite run or post-repair result is claimed.

| Execution receipt | Duration / checksum |
|---|---|
| Actual agent/unhanded/mixed baseline probes, count3 | 8.532s; `0bf797e62bf03b350710a294727285a00006b9616a688c2f63c4c544a2673a36` |
| Continue provenance divergence, count3 | 2.169s; `69fda974e67c6a51b88ec985f91ff9da175e0731c79eebd5fa7912b1c3e7a031` |
| Handed-only missing notification, count3 | 2.080s; `837007f3bc8d8b2447eff79f973e62ceb0455dd4ae33bb750c0c39d29a29c55e` |
| Actual engine/node baseline probe, count3 | 3.210s; `12bc5ea105d140c7ec2a9bd6e583f11481f158cedc458531d56a5fc842418caa` |

The baseline probes assert the defect so a green probe means reproducible failure, not correct production behavior. Their exact source is preserved in the patch; qualifying implementation tests must assert the opposite behavioral outcome through the named P rows.

## Tracker, Parent, Watchlist And Feasibility Decisions

Tracker action: update #2498 body and this audit before code, explicitly absorb P01-P15 into the current PR boundary, retaining #2407 OPEN and all original acceptance history. No new child/prerequisite, superseded closed issue or POTENTIAL_ISSUES entry. Request **one focused independent gate**; latest recorded gate is insufficient, not approved. Old recovery-disabled proposal is rejected, not a remaining option.

Watchlist promotion check: existing `runtime-operations.delivery_and_replay_ownership` already names run-local blockage versus global recovery, handed continuation ownership, selected-authority separation, eligible-set completeness and startup exhaustion. These directly support absorbing the complete finite pause class now rather than fixing pipeline only. Refine that node and map #2498 there, in addition to existing `maintenance-and-cleanup.invariant_suite_coverage` and `semantic-correctness.durable_fan_out_issuance_and_progress_ownership`. Other live consumer families in that parent include fork/replay, general startup orchestration, throughput and backpressure; they are separate authority/performance concepts, not omitted pause interpreters. Prior watchlist notes calling paused-run policy separate from #2251 are historical scope, not a ban on this explicitly requested #2498 amendment.

Tracker/watchlist repair is completed before the re-gate request: existing-node refinement and #2498 delivery/recovery mapping are landed on docs master at `swarm-docs@0592142`. Runtime and invariant YAML and the exact new active-issue mapping validate. The prior readback mapping remains intact; no node or issue was invented.

Parent action: absorb all known same-concept pause manifestations now; leave the broader delivery/recovery and R1 program open. No other production pause-aware work selector/command owner was found outside the table. The paused symptom was too narrow; no first-slice waiver is requested for this chosen class. Fixing only startup would leave delivered-work and provenance interpreters live, so it cannot close the class.

Closure feasibility: one bounded implementation/proof pass appears feasible through existing run-control, private selected-store guards, pipeline/delivery owners, coordinator and existing wake plumbing. No table/schema/new lifetime framework is necessary. Estimated chosen-class tail after a successful PR: zero. Before coding it is all P rows plus original corpus/qualification, not zero runtime work. Broader known runtime tail groups remain #2250 startup decomposition, #642 deferred selected-fork work, #2394 throughput and #2453 backpressure: at least four separately tracked workstreams, provisionally one or more child slices each, with high confidence in classification but low confidence in total slice count/duration. This audit does not claim those estimates exhaust their parents. R1 still has separate repository-policy and monthly-delta obligations, and #2447's factoring slice stays separately open; this PR closes only R1.1 and claims no parent closure.

Intended closure level after all P/M/RB/U proofs: **failure class eliminated** for each complete chosen class. Current achieved level: pre-implementation class repair plus baseline counterexamples, bounded readback candidate only; no corpus/runtime merge closure.

Architecture feedback: conflating active lifecycle with current execution permission makes independent work readers/SQLite command semantics disagree. The long-run better direction here is systematic consumption of the existing run-control/claim owner, not a general scheduler or startup framework. Track concrete repair in #2498 and the existing delivery/recovery node; broader startup phase debt remains #2250. Rough effort: a few implementation/proof days for the bounded repair, high ROI because it prevents both unavailable paused services and premature side effects. No new architecture tracker is needed.

Blocking conditions: fresh independent approval still missing; any additional interpreter, inability to close this class without schema/queue/framework, unreachable genuinely unsettled checkpoint, nondiscriminating mutation, required reinterpretation of standing/fork authority or genuinely new contract contradiction requires another explicit ruling. Approval of this amendment must cover the SQLite provenance row too. Do not unfreeze runtime merely because the probes or existing controls pass.

Focused independent re-gate requested: approve or reject the complete P01-P15 owner/consumer boundary and proposed spec delta, including operator provenance parity, typed parking, claim linearization and empty-pipeline continuation notification. Prior corpus/readback approvals remain binding only within their exact scopes. Until that explicit outcome is recorded, the implementation boundary remains frozen.
