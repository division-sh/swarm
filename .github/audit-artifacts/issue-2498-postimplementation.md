# Post-Implementation Proof Audit: #2498

Agent-g, 2026-09-30. Historical local qualification candidate: `723af0b1b`, rebased on
`origin/master@8fac0f2e7`. **Final public-corpus qualification and all 14 required
default units pass. This is a candidate proof audit for independent merge review,
not merge approval or closure of either parent.** Later commits contain audit
metadata only at `277641150`; that head subsequently failed CI. **That local
receipt does not establish final-head merge readiness.** The bounded CI repair
below supersedes the old final-head claim; exact repaired-head CI is required.

## CI Repair And Proof Supplement

CI run 36723873326 on `277641150` exposed the following concrete gaps. No test
was skipped, made permissive, or assigned a longer deadline to hide a failure.

| Failure | Repair through existing authority | Proof / status |
|---|---|---|
| Static formatting | Format the changed pipeline test fixture | Whole-tree gofmt census empty |
| Catalog replay partitions/census/references | Count the new verify-only source in the closed 158-row census; runtime/replay allocation stays 99/94+5 | Census and proof-reference controls pass |
| Route and persistence authority guards | Classify the exact new pause-test consumer and every changed resolved finding; preserve exhaustive searches | Unchanged guard assertions; no exclusions |
| Release E2E imports server DTOs, validators and private store | Independent public wire projections and negative origin/page oracles; migrate only supplementary typed timer inspection into the already-compiled internal lifecycle harness | Numeric public/restart/refusal journey passes both stores, 107.264s; public-boundary assertion passes |
| Partial shutdown fixtures lack dispatch schema | Consume existing canonical fixture schema helper | All four failed shutdown controls pass |
| Fork history fixtures claim ordinary work while paused | Explicitly construct historical delivery rows through lifecycle-owned running admission and restore materialized pause before validator/discard; no supported selected-execution credit inferred from setup | Both-store discard and PostgreSQL activation controls pass, 3.322s |
| Retired normal delivery becomes fatal active-run error | Canonical pause guard distinguishes terminal/forked lifecycle from pause and retains existing dispatch fence; co-read run/control/source facts rather than four separate queries | Real both-store pending-source fork journey passes; stopped-run block/park negative controls pass |
| PostgreSQL 500-row ceiling exceeded (15.200s versus 15s) | Preserve workload/ceiling and investigate new admission cost; reduce redundant reads within the same canonical guard | Both-store three-repetition control passes, 103.249s; exact-head CI required; no causal timing closure claimed |
| Timing and required-summary aggregate failures | No runner/budget relaxation | Must converge from all required exact-head checks |

Fixture historical seeding does not grant selected-fork runtime execution and
does not make an ordinary paused claim legal. The real selected authority
controls and P01-P16 negative admission tests remain required. Timer inspection
remains supplementary H/persistence credit, never a public timer API or live
provider claim. The public tests no longer repeat the server's validator.

The same approved owners and finite classes apply; no new owner, schema,
framework, vendor dependency, compatibility behavior, parent closure or gate
widening. Final qualification and the exact new head are recorded in the PR
proof audit after required checks complete.

## Binding Boundary And Closure

Pre-audits are `issue-2498-preimplementation.md`,
`issue-2498-pause-eligibility-preimplementation.md`, and
`issue-2498-standing-operator-composition-preimplementation.md` in this directory.
Independent approvals are issue comments 5901252923 (paired corpus, owner/oracle
conditions), 5901655130 (two-origin readback), 5903816439 (P01-P16), and
5906100464 (S01-S17). No named in-scope proof failure was treated as approval
to introduce another owner or framework.

Concepts changed: reusable numeric-feed and actual agent-routing/configuration
proof credit; typed handler/deployment fan-out readback; ordinary paused-work
eligibility and continue provenance/wake; exact-current standing command validity
versus process child disposition, compensation and committed outcome evidence.

Chosen working classes: the complete bounded missing-coverage class
`unqualified_numeric_feed_and_cross_instance_delivery_e2e_regression_corpus`,
plus the three explicitly absorbed readback, pause and standing composition
classes above. The original handover verifier symptom was an entry point, not
the audit boundary. The framing became broader before coding through recorded
gates; this is not a symptom-only repair or a first-slice waiver.

Achieved candidate closure: **failure class eliminated** for these finite
classes, with no remaining same-concept bypass or
planned child tail. Parent coverage program #2407 and broader lifecycle/phase
architecture #2250 remain OPEN. Routing performance #2394, selected-fork #642,
backpressure #2453 and the observed safety timing residual #2353 are not closed.
The parent-tail grouping is unchanged: R1.3 lead-owned repository policy, R1.5
#2499 monthly regeneration/reporting, and the separately gated #2447 factoring
stream remain outside this PR. R1.2 and the R1.4 baseline are delivered, not
reimplemented here. This is at least three separate closure streams, not an
exhaustive PR count; confidence in a total slice estimate is low. #2250's broader
startup/phase/transaction decomposition is likewise not fully sized by this audit.
Implementation did not justify absorbing those different classes. No claim of
universal future regression prevention or successful timer/history fork execution.

Authoritative governing references in `platform-spec.yaml`:

- `flow_model.flow_instance_authoring.standing_activation_model.{lifecycle_authority,process_routing,operator_transition_matrix}`.
- `process_local_work_lifetime_authority.{standing_lifecycle,durable_handoff}`.
- `run_model.lifecycle.pause_resume`, `platform_tables.tables.run_control_state`,
  and `platform_tables.tables.runs.terminal_evidence.continue_post_commit_recovery`.
- `durable_pipeline_processing_obligation_authority.claim_contract` recovery,
  scan, blockage, explicit exhaustion and event-wide coordination contracts.
- `durable_data_resources.deployment_origin`, API/OpenRPC `FanOutIntentKey`,
  `FanOutIntentReadback`, `FanOutListPage`, and `run.fan_out.list`.
- `api_specification.method_catalog` run/standing commands and their
  `cli_specification.command_catalog` counterparts; `test_specification`
  source claims and compiled golden executor contracts.

These binding sections are updated alongside code where semantics changed.
Master already corrected input-pin `source: external` retirement; its stronger
wording is preserved, without a compatibility reader.

## Canonical Owners And Systematic Consumption

This is the complete known production consumer census, rechecked by exact
caller/SQL searches after rebasing. The pre-audit's exhaustive entrance lists
remain binding; the rows below name their post-change disposition and execution
proof, not merely a shared-owner assertion.

| Owner | Consumers / current disposition | Named execution evidence |
|---|---|---|
| `fanoutobligation.IntentRequest.OriginBundleHash`, closed `IntentKey` | Readback `projectIntent`, store fan-out serving load/enrichment and quiescence projection moved to the same typed origin; no store-local bundle interpreter | RB1-RB4 |
| Fan-out key ordering/wire projection | Store keyset ordering, public page comparator, API JSON/OpenRPC and CLI readback all consume the full origin identity; handler filters remain handler-only | Mixed-origin pagination, exact wire test, numeric public pages |
| `runstate.DispatchParked` plus canonical standing disposition | Pipeline ordinary/decision candidate hydration, exact-event/direct claim and decision processed admission moved to it; selected-store cursor still counts parked rows without claiming | P01/P06/P09/P14/P15 |
| Same normal dispatch guard | Delivery pending/failed/stale continuation scans, receiver materialization/recheck and transaction-local claim admission moved to it | P02-P05/P12/P14 |
| Run lifecycle/control selected-store writer and `runcontrol.Controller` | Both backends require paired paused control and canonical standing permission; public run.continue, CLI and bus queued publication/recovery use it; committed continue always signals continuations | P07/P10/P11/P13/P16 |
| EventBus pipeline dispatch/disposition | Startup/global/periodic/run/ingress sweeps park valid paused debt; genuine running/busy/retry/global blockage stays fatal. Recipient continuations never substitute for event-wide coordination with no node route | P06/P09/P15, closed receiver invocation counter |
| Normal generation delivery continuation owner | Startup inventory/rebind, scanner, carrier election, node/agent dispatch and retirement retain exact generation/lifetime; parked state schedules no attempt or future-retry acceleration | P02-P05/P08/P12/P13 |
| `standingdisposition.ReadByRun` -> `runlifecycle.ClassifyStandingRestart` | One production classifier, mechanically moved from pipeline without aliases. Standing writers/reconcile, run-control/dispatch, bus standing recovery, continuation withdrawal, manager readiness, startup inventory, timer restore and inbound source authority all consume the canonical complete-fact reader | Classification/deletion guard, store disposition matrix, public standing and startup controls |
| Selected-store standing writer | Suspend/resume/reset, reconciliation/orphan/restore, source revision and publication retain parent-before-run locking; all three operator transactions revalidate captured authority and return applied/exact-noop evidence | Corrupt no-op, every-authority race, public no-op/reset/rollback tests |
| `RuntimeContextManager` exact child/target/scheduler authority | All three commands use `BeginStandingServiceOperation`; exact active resume keeps its child. Existing low-level child-only transition still rejects absence. Publication, recovery/ingress lookup, suppression, shutdown and reset consume exact identities | Child composition/cancellation tests, served lifecycle and reset/shutdown proof |
| Existing serve controller + process supervisor | Single public API entrance for all three commands, serialized with reset/shutdown; retains pipeline exclusion, actual gateway fence, child/scheduler disposition and exact committed publication | S01-S17; real gateway body drain and selected-context writer barriers |
| Existing inbound gateway admission | Fence records prior open state; pre-commit compensation joins the actual admitted requests as well as child/callbacks before restoring still-authorized resources. No-child paths retain the same gateway/exclusion, not a fake lease | S11/S12, actual public signed ingress refusal/continuation |
| Existing timer lifecycle and mailbox decision owners | Already consume ordinary publication/recovery entrances. Accepted occurrences/verdicts persist while paused, then event-wide coordination executes after continue; no new timer or decision interpreter | P15 real timer and mailbox RPC, M08 typed timer supplementary inventory |
| Existing selected-contract authority | Different semantic concept: exact fork state/grant/lease permits execution while fork lifecycle is paused. Ordinary parked admission is explicitly not applied to selected claims; selected-state/current-grant fence remains unchanged | Selected delivery execution/grant retirement controls, existing selected-fork canary |
| Existing authored source/data/route/config owners | Numeric fixture uses typed data admission/feed origin; golden uses real omitted-ID/scoped agent and independent local/connect routes; descriptor persists budget/flow-data fields. No test-specific production resolver | M01-M19, U1-U5 |
| Existing testcatalog/testplanning/release executor | Structural verify-only source plus disjoint external runtime claim; sole executor releasee2e. Strict expected assertions actually execute; default/CI selectors require both-store children | M22 and catalog/proof-plan guard packages |

Old non-authoritative paths are removed/invalid: handler-only deployment bundle
selection and key wire schema/comparator; store-local fan-out bundle helper;
pipeline-owned standing classifier/type aliases; SQLite continue fabrication
from lifecycle-only paused rows; continuation scan/claim bypass of ordinary pause;
standing command permission inferred from child presence; unconditional child
drain/preparation/publication on durable no-ops; swallowed no-op disposition
errors; restoring resources before admitted ingress drains; old-epoch reset
suppression; recovery acknowledging no-node timer/decision events without their
coordinator. No old reader, fallback or producer is kept for legacy stores.

Surviving aggregate terminal/cleanup owners monotonically quiesce their own
resources and do not grant ordinary execution. Startup standing intrinsic
normalization, suspension, orphan restoration, source-bound run identity,
selected-contract paused execution and retired product APIs remain separate
existing contracts, not alternatives to the changed owners.

## Proof Vocabulary

Every table row has exactly one closure disposition. All persistent/public
families named below execute SQLite and host PostgreSQL. Public operator credit
uses authenticated RPC/compiled CLI; the existing retained mock lifecycle is
**H**, not a paid live-provider or fresh public mock-test proof. Component tests
with controlled handed debt, invalid state or faults are explicitly supplemental.
No manual wake after public continue, fabricated consumer, raw-SQL repair,
transport-cache replay or settled-only crash checkpoint earns closure credit.

Short proof names refer to these exact executable roots:

- **Numeric**: `releasee2e.TestGoldenNumericDataScatterParkRestartBothStores`.
- **Refusal**: `releasee2e.TestGoldenNumericDataScatterParkRefusalBothStores`.
- **Golden**: `releasee2e.TestGoldenAgentWorkloadSequentialRunsBothStores`,
  `TestGoldenAgentWorkloadRestartAndForcedKillOnBothBackends`, and unchanged
  `TestGoldenAgentWorkloadBurstConcurrencyOnBothBackendsIteration[12]`.
- **Public standing**: `releasee2e.TestStandingPauseAuthorityPublicBothStores`
  and `TestStandingResetNonExecutablePublicBothStores`, including their orphan,
  corruption, source-revision and SQL-rollback children.
- **Served standing**: `serveapp.TestServedParityHarnessStandingServiceLifecycle`.
- **Composition**: `serveapp.TestStandingServiceMutationsUseSelectedRuntimePipelineOnBothStores`.
- **Authority race**: `runtimepersistence.TestStandingOperatorRefusesEveryChangedAuthorityBothStores`.
- **Public wake**: `serveapp.TestServedParityHarnessRunControlLifecycle`;
  actual already-handed-only precondition, no pipeline debt, installed parked
  coordinator; removing the continue signal makes both stores fail.

P/S failure rows additionally exercise existing engine, run-control, scheduler,
gateway and selected-store owners directly to inject exact barriers or corruption.
They supplement public command proof; they do not claim that an external client
can synthesize invalid relations or access private lifecycle capabilities.

### Standing Transition Manifestations

| Row | Disposition | Exact proof / oracle |
|---|---|---|
| S01 | reproduced and fixed | Served standing: active suspend/reset wait for the real held route and scheduler; exact N unchanged until drain, then old projections retire |
| S02 | reproduced and fixed | Public standing + Composition: distinct fresh suspend keys retain N/journal/domain facts and no child; signed ingress stays unavailable |
| S03 | execution-proven through the same corrected path | Public/Served standing: resume preserves N, publishes its exact child, then actual signed ingress, turn and delivery settle |
| S04 | reproduced and fixed | Public standing + `TestStandingOperatorActiveNoopPreservesExactChildAndSchedule`: fresh active resume leaves child/scheduler/publication unchanged |
| S05 | reproduced and fixed | Public standing: successive suspended resets and retained restart/reset, no child, ordinary continue refusal, later standing.resume |
| S06 | execution-proven through the same corrected path | Served standing: active reset creates exact N+1/fresh occurrence, retires predecessor projections; keyed replay retains response |
| S07 | reproduced and fixed | Public standing terminal/current, terminal/revised and terminal-suspended children: cold terminal N remains historical; lawful reset uses latest source and retained override |
| S08 | reproduced and fixed | Public standing invalid-current/current and revised-source children: typed provable invalid remediation, exact N+1; broken relation separately refuses |
| S09 | execution-proven through the same corrected path | Public standing orphan/terminal-orphan/invalid-orphan: all commands refuse absent declaration, cold admitted restoration then lawful reset |
| S10 | reproduced and fixed | `TestStandingFreshNoopRejectsCorruptRelationBothStores` for each no-op; Public standing unknown/broken; Served standing all-command fenced/active-missing/suspended-live refusal; `TestStandingOperatorRejectsInvalidChildComposition` foreign/stale child matrix |
| S11 | reproduced and fixed | Composition: pipeline-exclusion cancellation, real admitted webhook body cancellation, held-child cancellation and pre-writer failure; exact gateway/child restored only after actual joins; `TestStandingOperatorCancelledSchedulerParkRetainsExactOwnerUntilJoin` |
| S12 | execution-proven through the same corrected path | Composition suspended/terminal no-child rollback plus Public standing SQL rollback in all five terminal/typed-invalid products; unchanged predecessor facts and suppressed admission |
| S13 | execution-proven through the same corrected path | Composition acknowledged cleanup/publication errors retain exact committed N/result, no child revival or open ingress; fresh resume cannot hide missing executable child |
| S14 | execution-proven through the same corrected path | Public standing new-key no-op versus old-key response replay after later generations, successive fresh resets; Served standing keyed replay |
| S15 | execution-proven through the same corrected path | Served standing real scheduler and held connector route drain; exact successor occurrence differs; scheduler park cancellation and held-child restore refuse before actual join |
| S16 | reproduced and fixed | Served standing concurrent public runtime.nuke and shutdown remain held behind exact child; `TestResetStandingSuppressionComesFromReconstructedEpoch` proves suppression belongs only to the reconstructed epoch |
| S17 | execution-proven through the same corrected path | Authority race: every command against source/generation/suspension/generic-pause/terminal changes; Composition terminal race cannot revive N, sibling selected context untouched |

### Pause Eligibility Manifestations

| Row | Disposition | Exact proof / oracle |
|---|---|---|
| P01 | reproduced and fixed | `bus.TestPausedUnhandedRecoveryParksThenContinueReleasesBothStores` and Numeric: ready while paused, debt unchanged, exact release after public continue |
| P02 | reproduced and fixed | `TestPausedHandedAgentParksUntilContinueBothStores/pending`: real Manager.OnEvent count zero before continue, one delivered attempt afterward |
| P03 | reproduced and fixed | Same root failed_due + `TestPausedFutureRetryAndLiveClaimRemainOwnedBothStores`: due/future timing and retry count remain owner-controlled |
| P04 | reproduced and fixed | Same root stale_in_progress + live-claim control; exact in-flight attempt may settle without a successor attempt while paused; Numeric real predecessor death |
| P05 | reproduced and fixed | `pipeline.TestPausedHandedNodeParksUntilContinueBothStores`: real engine remains unchanged while paused, authors exact output after continue |
| P06 | reproduced and fixed | `bus.TestMixedPausedRunningRecoveryBothStores` and `serveapp.TestPausedMixedRunsStartupBothStores`: same-source/two-context startup settles running siblings, never paused debt |
| P07 | reproduced and fixed | Public wake + signal removal/restoration: examined=0, no post-continue manual Signal/Synchronize, real exact node delivery/entity/run closure |
| P08 | reproduced and fixed | Numeric: public 32/100 issued and unsettled before death, both-store paused ready restart, same prefix plus all 100 exact rows after continue |
| P09 | execution-proven through the same corrected path | `TestStartupRecoveryClassifiesBlockedBranchesOnBothStores`: all five real blocked branches still refuse; coordinator explicit exhaustion controls |
| P10 | execution-proven through the same corrected path | `TestRuntimeStartRecoveryDisabledRejectsExecutableDeliveryInventoryParity`: both modes/stores, paused/running agent/node pending/failed/live/reclaimable debt refused unchanged; foreign source excluded |
| P11 | reproduced and fixed | `TestUnownedPausedContinueRefusesBothStores`: no fabricated ordinary authority; `TestSelectedDeliveryExecutionFenceBothStores` and receiver grant retirement controls preserve selected/terminal admission |
| P12 | execution-proven through the same corrected path | `TestPauseAfterCarrierElectionFencesClaimBothStores` exact barrier plus live-claim control; B18 real-pause/prepared-group controls retain acknowledged prefix and reject fresh suffix |
| P13 | execution-proven through the same corrected path | `TestRunControlAcknowledgedCleanupFaultPreservesFollowUpBothStores`, runcontrol cancellation/release controls, actual generation retirement/coordinator race controls: commit/wake retained and claims/carriers joined |
| P14 | reproduced and fixed | `TestPausedEligibilityRejectsBrokenControlBothStores` missing/contradictory controls; selected source/grant and five blockage controls reject, never park invalid authority |
| P15 | reproduced and fixed | `TestStandingReconciliationNormalizesRunPauseForActiveDeclarationParity` + `TestPausedAcceptedTimerAndDecisionPublicBothStores`: actual accepted timer/card event retained while paused and exact workflow transition after continue, one occurrence |
| P16 | reproduced and fixed | Public standing CLI/RPC suspend/reset/continue refusal/resume plus `TestStandingOwnedPauseRejectsGenericContinueBothStores` suspend/reset/orphan; unchanged authority, standing.resume alone clears override |

### Corpus And Readback Manifestations

| Row | Disposition | Exact proof / oracle |
|---|---|---|
| M01 | reproduced and fixed | Compiled numeric verify passes on current master; catalog strict admission/retired-source rejection, no exemption |
| M02 | reproduced and fixed | Numeric exact original 100-row checksum, integer/numeric public payload and per-key receiver state assertions |
| M03 | execution-proven through the same corrected path | Numeric all 48 optional null notes omitted, exact other values retained; shared codec whole-input rejection controls |
| M04 | reproduced and fixed | Numeric YAML-only relative source/config/data/token paths beneath hostile ancestor; real compiled invocation and exact source hash |
| M05 | reproduced and fixed | Numeric public feed-only run origin, pin/version and full fan-out pages; no synthetic import/batch producer |
| M06 | reproduced and fixed | Numeric exhaustively compares all 100 row/event/node target/entity chains and unknown/extra identities |
| M07 | reproduced and fixed | Numeric exact per-row committed fields and distinct sibling state, not merely item count |
| M08 | reproduced and fixed | Numeric joined selected-store typed timer inventory: 100 exact initial activations/run/entity/route/declaration/due identities before/after restart; no public timer-list credit |
| M09 | reproduced and fixed | Numeric feed cursor/ordinal closure AND every intended delivery delivered, no dead letters; parked future-timer run legitimately waits |
| M10 | reproduced and fixed | Numeric keyed creation replay after process/cache loss reaches permanent receipt; exact feed/events/targets unchanged |
| M11 | reproduced and fixed | Numeric exact settled facts and typed activations preserved after graceful retained stop/reopen; Golden field-bearing restart |
| M12 | reproduced and fixed | Numeric acknowledged partial pause before SIGKILL with genuinely unsettled prefix, ready paused restart and exact final convergence |
| M13 | reproduced and fixed | Refusal late malformed numeric/required null/duplicate row-100: no version/head/run/pin/feed acceptance or partial publication |
| M14 | reproduced and fixed | Golden actual candidate result -> keyed local finalizer state/delivery; U1 admitted route omission kills exact chain |
| M15 | reproduced and fixed | Golden omitted-ID candidate agent actually activates/turns/emits; U2 runtime subscriber loss kills chain after source admission |
| M16 | reproduced and fixed | Golden real root->static scout has exact distinct receiving entity/route; U3 mutates the canonical target classifier and kills that delivery |
| M17 | reproduced and fixed | Golden independent local scout completion and connected root collector targets; both U4 arm failures are exact, not loose ID membership |
| M18 | reproduced and fixed | Golden authored flow-data capability survives real activation/turn/restart and unchanged two N=10 bursts; U5 write/read omission controls |
| M19 | reproduced and fixed | `TestPersistedAgentReadinessFieldsBothStores`: exact budget 1.25, capability and revision; budget write/read U5 kills. Explicit store-only credit |
| M20 | execution-proven through the same corrected path | Unchanged `TestScatterGatherSafetyBothStores` six variants including reverse-100 and real join pass; earlier timing failure retained separately in #2353, not claimed fixed |
| M21 | split / escalated as separate class | #642 owns timer/history fork frontier; no optional/unsupported run.fork success credit or parent closure |
| M22 | reproduced and fixed | `TestNumericFeedAssertionsRejectMissingAndUnknownFields`, catalog claim/partition/sole-executor and testplanning required-child controls fail closed |
| M23 | execution-proven through the same corrected path | Existing release failure collector captured exact failing public/dead-letter/claim/readiness chain in U failures and interrupted numeric checkpoint; no log-based success oracle |
| RB1 | reproduced and fixed | `TestDeploymentFanOutReadbackPreservesOriginBundle` open/closed typed origin bundle, ordinary handler control |
| RB2 | reproduced and fixed | `TestFanOutReadMixedOriginPaginationBothStores` full origin tuple and cursor/page boundaries across feeds/handlers |
| RB3 | reproduced and fixed | `TestFanOutIntentKeyExactOriginWire`, origin closed-union rejection and API/OpenRPC schema tests; deployment omits empty element_ref |
| RB4 | reproduced and fixed | Numeric real runtime-enriched run.fan_out.list and compiled CLI exhaust exact public pages on both stores |
| U1 | reproduced and fixed | Archived bounded local-node omission: admitted real keyed agent result cannot settle finalizer; restored paired corpus passes |
| U2 | reproduced and fixed | Archived runtime omitted-ID subscriber loss: source admits, real fan-out route insertion fails exact subscriber; restored corpus passes |
| U3 | reproduced and fixed | Archived `ClassifyDeliveryTargetOwnership` root-entity contamination: exact scout target dead-letters; restored corpus passes |
| U4 | reproduced and fixed | Independent archived local suppression and connected-target collapse arms reach their real mixed publication and fail exact branch/target predicates |
| U5 | reproduced and fixed | Four separate descriptor write/read arms: flow-data reaches actual retained activation refusal; budget expected1.25 becomes0 on each store |

## Execution Receipts And Final Qualification

Historical before/after, race and public receipts with hashes are preserved in
`issue-2498-implementation-progress.md` and `issue-2498-mutations/README.md`.
All nine U arms have admitted semantic failure and clean restoration, never
compile/grammar failure credit. The final five mutation-owner files are byte-
identical to the qualified mutation candidate `cfb6b3b5f` and its reachable
rebased replacement `75b601728` (`git diff` empty),
and rebased positive paired execution is separately recorded below. Mutation
patches are data only; none is applied to production.

Final candidate focused controls PASS: rebased catalog/API/proof-plan guards
1.397s/1.910s/19.641s; standing/pause serve composition 24.271s; S11 real ingress
and writer compensation race x3 37.078s; selected-store standing/control/B18/
readiness and fan-out matrices 10.267s/0.005s/0.004s/1.162s. Earlier expanded
public P06/P07/P15 race x3 PASS137.277s; complete stale-authority matrix race x3
PASS115.651s. No vendor changes.

Final rebased public numeric/refusal/sequential-golden/standing matrix through
`go run ./cmd/swarm-test -- ./internal/releasee2e -run
'^(TestGoldenNumericDataScatterPark(Restart|Refusal)BothStores|TestGoldenAgentWorkloadSequentialRunsBothStores|TestStandingPauseAuthorityPublicBothStores|TestStandingResetNonExecutablePublicBothStores)$'
-count=1 -timeout=10m` PASS294.959s, both stores, on `723af0b1b`.
Raw receipt SHA256:
`50f5ee4275eb7f368dadbc39970e57117808ece6108d5c09baa3ba9717c91e27`.

Final default `go run ./cmd/swarm-test` PASS: all 14 planned units passed required
execution on `723af0b1b`, with zero failed test actions, completed
2026-09-30T13:39:20Z. Raw receipt SHA256:
`270536a0b96bd2cd9ec06b24180a85f8c4a7d3a40f0cc2e53912739d20730a38`.
The served canaries include public continue, standing reset/shutdown, mixed-source
pause startup and both timer/card outcomes; their exact backend children are
required by the default plan. The last full bus package passes40.358s.
No direct whole-tree go test or `--full`; focused tests use regular go test.
The final complexity measurement against current master passes without hotspot
growth: cognitive594/594, cyclomatic281/281, maxima302/184 unchanged.
Measured non-test Go delta against `8fac0f2e7`: +964/-481, net+483 including
generated facade and test-support code; excluding those, +925/-471, net+454.
The added production lines buy closed paused-work admission, exact durable/process
standing composition and two-origin public projection, while deleting the old
interpreters; they do not introduce another semantic owner. The 3000-line
production cap is not approached.

## Watchlist, Residual And Architecture Decisions

Existing nodes refined in swarm-docs: maintenance invariant coverage, runtime
delivery/replay ownership and semantic typed fan-out progress/readback.
Approval/refinement history is e83cad7 -> 02a955f -> **7f9e932**, published on docs
master after YAML and diff validation. Final qualification and candidate closure
are reflected without erasing historical failures. No new node, issue,
POTENTIAL_ISSUES entry, framework or dependency; independent merge review remains
required. #2498 remains open pending review/merge; #2407/#2250 remain open.

Preserved residual: the original unchanged SQLite reverse-100 safety run exceeded
20s with one gather delivery in_progress despite all201 expected events. Isolated
candidate/master reverse controls and the full six-variant pass do not classify
or fix it. Exact failure SHA256 `e1d2ee0d24c407e6ca387c862596dd403ac5b009a51619734c43cdac4c9e7fd7`
is tracked at #2353 comment5909982491; limits/workload/performance owner unchanged.
Any recurrence remains a failed execution requiring investigation.

Architecture feedback: coverage claims, authored source and execution selection
must remain mechanically coupled; durable operator permission cannot be inferred
from process child presence; continuation ownership cannot erase event-wide
coordination; compensation must join every actually captured resource. These
bounded gaps are repaired now through existing owners in #2498. Broader startup/
phase architecture remains tracked in #2250 and the existing watchlist, not an
unrecorded promise. Finite repair effort was several implementation/proof days;
ROI is high against ordinary repeated commands, restart and silent routing/config
regressions. The long-run direction is explicit composition phases and mechanically
qualified source/claim/owner consumption, tracked in #2250 and the same watchlist.
A broad phase-model repair would likely require several bounded PRs over weeks;
confidence is low without #2250's full census, and this is not an authorization or
closure promise. No legacy selected-store migration or compatibility.
