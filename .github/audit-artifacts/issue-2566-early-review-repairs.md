# #2566 Early Review Repairs

## Finite Caller Census And Fixture Repair

Lifecycle at `c6a90a08be52e613fe273a85875c9ff57bcd28b3` ran on server2
2026-10-07 08:27:01--08:31:40 UTC, plan
`bf3b6d7620ee46f8ba2bdb0c1538ccafd3018a8b4f2f8d0e1990ab678d4b93d5`.
Six units passed, four failed or were interrupted, and 45 did not start. The
actual release failures were `RUN_NEVER_COMPLETES` for no-final routing roots;
`runtime-full` and `serveapp-i-reporter` were canceled siblings, not separately
diagnosed defects. No supplement or soak ran. This receipt does not qualify
the repaired head.

The read-only census inspected 278 tracked disk schema files (190 independent
roots), 529 Go literal sites, and 441 finite-method/CLI references. Forty disk
roots had no final stage before this repair. Literal sites include nested flows,
fragments, negative oracles and component sources; they are not 529 independent
run-start sources. Actual source aliases and constructor callers were traced,
not classified from full-path grep alone.

The permanent tests cover 16 actual finite source variants and explicitly
classify all 38 remaining no-final disk roots. Service examples, standing
ingress, startup/provider fixtures, scenario imports, low-level construction,
fork components and the unreachable boot oracle are not made finite merely to
pass a test. `TestRewrite2566DeliberateServicesStillRefuseFiniteInitiation`
retains the refusal for default service constructors. The existing both-store
API refusal-before-effects and ordinary service-publication proofs remain due
in lifecycle qualification, with their original assertions intact.

| Actual finite consumer family | Existing fixture owner / repair | Named proof |
| --- | --- | --- |
| Managed Claude release, foreground observer overflow, optional live-Claude release | `claude_cli_managed_lifecycle`: routing-only root immediately ends at `done`; worker still enters `pending` and ends at its existing `done`. | `TestClaudeCLIManagedLifecycleFromReleaseBinaryDefaults`, `TestRunStartForegroundObserverOverflowFromReleaseBinary`, `TestRewrite2566FiniteReleaseRootsDoNotCompleteTheirWorkers`. Paid live-Claude proof is not claimed. |
| Numeric scatter pause/crash/restart, row-100 rejection, durable operation replay | `test-numeric-data-scatter-park`: routing-only root immediately ends at `done`; registry retains its original active stages, parked timers and `archived` final. | `TestGoldenNumericDataScatterParkRestartBothStores`, `TestGoldenNumericDataScatterParkRefusalBothStores`, `TestDurableDataOperationAggregatePublicRestartBothStores`, and the independent root/worker test. All five failing release roots pass locally, including their original PostgreSQL/SQLite children. |
| Deployment document corpus and two independent pinned feeds | `CopyNotifyAllChildren` gets an explicit finite-only option; `CopyTwoDeploymentFeeds` and the deployment conformance source select it. Default service variants remain unchanged. A producer-owned, connected close event ends the reusable portfolio; rows do not close it prematurely. | `TestDeploymentSourceTwoPinnedFeedsSettleIndependentlyBothStores`; corpus/empty-version and heavy fan-out proofs remain required supplements. |
| Text/file data, keyless multiplicity, selected root/singleton/dynamic feed forks | `CopySelectedDeploymentResource`: existing collectors stay active across rows, with declared primary entities and an explicit close transition. Dynamic close selects an existing receiver, not create-on-close. Data-declaration overrides retain the close declaration instead of deleting it. | Six route/key variants in `TestRewrite2566FiniteInitiationSourcesHaveCompleteClosure`; original text/restart/replay and selected-fork assertions are preserved. Keyless multiplicity additionally proves public close after settlement through existing API and run-read owners. |
| Connected novel scenario root-input run | `WriteNovelDerivedScenarioBundleWithRootInput`: routing root ends immediately; its existing complete-request handler advances fulfillment `pending` to `done`. The scenario-import-only variant remains a service. | Finite source/ordinary verification test; existing connected private/live-source tests retain their assertions. |
| Already finite reporter, root ingress, resource-read release, numeric ingress, deployment-run-start | Existing closed constructors and stages are unchanged. | Same 16-case loaded-source census, plus their existing supported-surface journeys in lifecycle/supplements. |

`finite-fixtures.json` uses the already committed hash/offset applier for eight
explicit disk/generated/caller files. It replays exactly from `c6a90a08b` and
is byte-idempotent; no end stage is inferred. The original 417-file/553-decision
retirement/equivalence ledger is unchanged and still replays exactly. The
permanent finite-source test also runs ordinary structural verification, so
unreachable decorative finals, absent primary entities and missing producer
authority cannot receive positive proof credit.

This is a test/source reconciliation within the approved class, not a runtime
relaxation. No production interpreter, compatibility path, alternate owner,
timeout/count/ceiling increase or blanket stateless-container exemption is
introduced. The added completion assertion consumes `operatorread.RunReader`,
not additional lifecycle SQL. The read-only scratch census is retained as text
evidence rather than an importable unclassified Go package.

The component deployment harness has no completion executor by default. Its
additional close proof therefore checks the actual persisted `done` stage
through `WorkflowInstancePersistenceReader`, not a fabricated whole-run
completion result. A trial enabling completion across the shared restart
harness exposed its intentionally separate executor-join requirement and was
discarded; neither the shared harness lifetime nor A/E's production ownership
was changed. The real release/served journeys retain whole-run completion
proof obligations. Failed trial receipts remain evidence, not qualification.

The four focused data journeys pass on both databases at `-race -count=3`
(159.402 seconds): independent pinned feeds, keyless equal-row multiplicity
with public close/persisted final, text-file restart/replay, and 37 dynamic
receivers. Source/ordinary-verification/service census controls pass
`-race -count=3`; the complete script tests also pass `-race -count=3`.
All 78 core structural guards pass; the actual persistence ratchet passes
without new raw authority or baseline changes. Build and vet pass. Full
repository census and complexity receipts are recorded at the next review head.
Lifecycle plus the 13 named supplements and hosted full CI are
still required before review-ready closure. The preceding 22/22 core receipt
remains bound to `2b9273490`; any carry-forward disposition must not relabel it
as an execution at this replacement head. Parent/watchlist and A/E ownership
boundaries are unchanged. This section is a repair checkpoint, not the final
Post-Implementation Proof Audit.

## Generated Readiness Order Repair

Core at `2b92734907b298c1e47bcfa5eeff2327a59431b6` passed all 22 units on
server2, 2026-10-07 07:34:51--07:45:01 UTC, plan
`841488275cc1443c9bf3660ee907db0c8e231ed2e21a5d7e522aa369caa1c273`.
Lifecycle at that same head stopped at 07:46:04: its initial/staged selected-fork
readiness matrices failed boot admission on both databases. The generated schema
put `complete` before `idle`, making `complete` the derived entry and `idle`
unreachable. Two units passed, four failed or were interrupted, 49 did not start.
Canceled sibling units receive no proof credit. No supplements or soaks ran.
Server2 was released after the joined failure; its receipts remain in the clean
exact-head qualification worktree.

The existing `localReadinessFixture` decoded and re-marshaled whole schema
documents through Go maps at three modification points. The ordered parser was
not defective. That modifier is now the named
`canonicalrouting.CopySelectedForkLocalReadiness` recipe: it preserves the
original lifecycle bytes and makes only the known preparation-route changes,
with exact unique-match refusals. All three map re-parsers are deleted from the
consumer. No stage inference, generic public mutator, compatibility adapter,
production interpreter, or replacement semantic owner is introduced.

`TestRewrite2566GeneratedSourcesMatchReviewedEntryGoldens` now covers all 27
legal declaration/frontier combinations, each through actual loaded, persisted
and retained-logical sources. Independent expectations require `idle` first,
the complete ordered stage list, and only `complete` final; all variants pass
race/count-three. The original 553 reviewed entry decisions and 417 exact,
byte-idempotent rewrite outputs are preserved. The existing initial/staged,
node/activity-loop execution witnesses also pass on both databases at
race/count-three with their original assertions intact.
The complete initial/staged matrices (all 108 backend/declaration/frontier
cells), creation-source and fork-recipient controls pass through the managed
wrapper at count-one. The full repository Census/Inventory/Registry sweep,
closed routing/partition checks, all 78 structural guards and the actual
persistence debt ratchet pass. Build and vet pass; no registry, ceiling,
repetition, assertion, budget or supported workload limit is changed.

The user explicitly directed the next server2 slot to lifecycle plus supplements
after these test/fixture-only repairs, retaining the preceding core receipt.
That receipt is still bound to `2b9273490`, not mislabeled as qualification of
the replacement head. Local sweeps and complete fork matrices are now green;
final lifecycle/supplement and hosted-full receipts remain due.

## Core 497985965 Integration Repair

Core at `497985965d25882531d5c2932b79258675fda404` completed on server2:
21 of 22 units passed; `broad-01` failed. Build and vet passed both on that
candidate and independently checked `origin/master@06018d2e1`. These were not
compile failures. The same routing-owner and proof-partition defects reproduced
in the local repository-wide census. The failed core receipt remains evidence,
not qualification of the repaired head.

| Failure family | Repair / exact preservation proof |
| --- | --- |
| Complete run-start routing sources authored outside the closed fixture owner | Moved the five API/runstart consumers to named `canonicalrouting.CopyFinite*` construction recipes. `TestCanonicalRoutingRepositoryUsesClosedConstructionAPI`, `TestFiniteStart*`, `TestFiniteRunStart*`, `TestDeploymentRunStartFeedOnlyAcrossSelectedStores`, and both reviewer feed witnesses preserve real loading, exact routes, both stores, replay, and refusal-before-effects assertions. No whitelist change. |
| Reporter proof absent from the original late partition/envelope oracle | The oracle now includes `TestIssue2566ReporterFiniteFixtureCanCloseBothStores` and its sqlite/postgres children, matching its existing actual policy owner. `TestServeappLateSplitRetainsProofEnvelopes` and `TestServeappLateSplitPreservesOriginalRootPartition` preserve exact once-only selection. No policy, budget, count, timeout, or environment change. |
| Engine fixture rejected missing flow before the production rejection boundary | An absent declared flow is left absent and passed to `Executor.Execute`; no topology is invented. Original ambiguous-handler, accumulator, and missing-carrier rejection tests retain their assertions. |
| Stage-fixture immutability oracle demanded an invalid raw-schema fallback | `TestSemanticFixtureCannotInferStageMembership` now checks the explicit declaration before execution and exact schema equality plus absent compiled topology after refusal; the original source remains untouched and no target is inferred. |
| Manager value-copied fixture tree disagreed with constructor schemas/parents | The existing fixture preparation synchronizes its declared schemas, exact parent pointers, and ByID references before compilation. Its nested container is explicitly declared. All originally failing readiness, ownership, recovery, retirement and compiled-initial roots retain their assertions. Hostile mutations after compilation remain hostile. |
| Exact-once test attempted its first timer activation after final entry | Accept the original event-anchored timer through the existing lifecycle owner while still in `new`, before executing the handler. `TestCreateEntityHandlerEffectsAreExactOnceAcrossStoreMutations` retains every event/outbox/gate/field-mutation assertion and additionally proves the identical activation reference survives `done` on both stores. This is component-fixture setup, not a new production arm or selected-store construction proof. |
| Revised timer generator attached nested YAML to `waiting: {}` | The revised source declares `waiting:` with its timer child; the timer-free source still declares `waiting: {}`. `TestWorkflowTimerLifecycleFirstRevisedInitialTimerUsesDynamicReadinessModeOnBothStores` preserves first-revision, runtime-mode, restart and both-store assertions. |
| Pin-rewrite untouched-section oracle disagreed with its input | Its lifecycle section uses the current valid `idle: {}` spelling on both sides. `TestRewriteRetainsCommentsOnUntouchedSections` still proves that the pins rewrite changes neither unrelated data nor comments. |

The local full census and the managed reproduction of all 74 previously failing
roots pass after these repairs. The committed rewrite output was refreshed while
retaining all 553 independently reviewed entry decisions; `-prove` still replays
417 exact files and is byte-idempotent. No ratchet/ceiling increase, production
compatibility seam, alternate owner, or final-stage authorization exemption is
introduced. Core, lifecycle and named supplements must qualify the fixed head
after an explicit server2 handoff; this repair record does not claim they passed.
The extra combined race/count-three command passed its API, runstart and
testplanning packages but timed out in the repository-wide routing AST/type
inventory at its ad-hoc three-minute package limit. The stack remains in Go's
package importer; no assertion failed. The full non-race census and the managed
74-root reproduction both pass that guard. The timed-out extra is retained as a
failed receipt, not reported green; no supported workload limit is changed.

Supersedes the incomplete consumer/proof inventory at review pin
`8781de3d251227f4b3db233a8c37802f61c98b67`. Binding Gate D6019841561 and
review6024587950 remain unchanged. This is checkpoint evidence, not the final
Post-Implementation Proof Audit, a core qualification receipt, or merge readiness.

## Owners And Cut

- `pinrouting.AdmitDeploymentFeedDeclaration` now returns the existing typed
  `SourceEvent` after exact importable-declaration/output-pin admission. API
  import and pin admission passes those facts to `runstart.ValidateFinite`;
  `CompiledConnectGraph.MatchingSourceEvent` selects the exact receivers.
  Producer publication does not construct the producer. Receiver closure still
  uses the same eager keyless selector as manager construction. Both atomic
  creation callers were changed; classifier and bus consumers were adapted to
  the same return contract, not a second reader or compatibility overload.
- The compiled stage-topology owner excludes final explicit sources and retains
  their named errors for `state_machine_coherence`. It covers loop operations,
  escape, and arrival-join completion/deadline overrides. Removing an executable
  edge cannot silently make the authored declaration valid. No new runtime
  refusal owner or accepted-work disposition was introduced.
- Permanent goldens cover all 553 independently reviewed positive source sites,
  not only 204 disk schemas. Go literal selectors come from the immutable
  pre-rewrite source, and expected entry/order/finals from the reviewed ledger.
  Tests admit the actual current literals. Actual generator outputs, the two
  moved gate/join families, retained logical reconstruction, selected-store
  reload, and selected-fork source loading have distinct proof rows.
- The reported compile consumers now use `Final`. Complexity repair factors
  preparation/file application and accepted timer retention without changing
  the grammar, accepted cancellation policy, or numeric ceilings.

## Manifestation Proofs

| Manifestation | Exact proof / currently achieved receipt |
| --- | --- |
| Independently selected nested feed bypasses root-only closure | `TestReviewer2566NestedFeedReceiverMustEnterFiniteClosure`; race/count-three pass |
| Feed-only and event+data imports/pins, multiple selected feeds, no-final worker, refusal leaves all 13 durable tables unchanged | `TestReviewer2566FeedOnlyRejectsNestedServiceBothStores`; both stores, race/count-three pass; includes event-only unselected-service and receipt replay preservation |
| Exact selected output versus unrelated output; keyed service producer is not constructed; recursive keyless receiver children | `TestFiniteStartSelectedFeedsUseExactRoutesAndRecursiveConstructors`; race/count-three pass |
| Final loop source passes verifier but runtime refuses | `TestReviewer2566LoopStartFromFinalIsRejectedBeforeRuntime`; race/count-three pass |
| Loop start/admit/repeat/close/escape and join completion/deadline source restrictions | `TestFinalStageExplicitLoopAndJoinSourcesCannotWidenEligibility`; final/non-final controls and exact carrier admission, race/count-three pass |
| Cycle-2 F2 remainder: non-advancing join outcomes evade source admission | `TestReviewer2566FinalJoinEmitOnlyStillRejects`, `TestFinalJoinStageAdmissionDoesNotDependOnOutcomeOrDeadline`, `TestFinalJoinDeclarationApplicabilityPreservesLoopAndFanOutKinds`; advance/emit/data outcomes, with/without deadline, loop-arrival and stage-free fan-out controls, race/count-three pass |
| Missing non-disk permanent entry oracles | `TestRewrite2566EntryGoldenMatchesTypedCorpus`, `TestRewrite2566EntryGoldenInventoryCoversEveryReviewedSite`; 553 independently reviewed selectors, including catalog fixture/spec embeddings |
| Generated versus lexer-only evidence | `TestRewrite2566GeneratedSourcesMatchReviewedEntryGoldens`, `TestWorkflowEntryGoldensProtectMovedGateAndJoinGeneratedSources`; actual generated/loaded output, race/count-three pass |
| Sorted, still-reachable source silently selects another entry | `TestRewrite2566EntryGoldenRejectsSortedDumpWithoutStranding`; retained hostile control |
| Retained source order/finals lost on new store reader or selected fork load | `TestStageCatalogRetainedStoreReloadAndSelectedForkSourceBothStores`; canonical store construction/reopen from the original path/DSN, closed predecessor, hostile changed live source and actual selected-fork source loader; both stores race/count-three pass |
| Materialized selected fork source changes its stage catalog | `assertRetainedStageCatalogsForFork` in `stageForkContentionFixtureAt`; `TestSelectedBranchPointActivationAtomicityBothStores`, event/deployment-revision and running/cancelled cases, both stores race/count-three pass |
| Exact retained catalog survives either writer/activation ordering | `TestSelectedRunForkActivationFrontierContentionBothStores/(sqlite\|postgres)/(writer\|activation)/commit`; all four cases, race/count-three pass (156.767s aggregate) |
| Two test interfaces fail to compile | Exact managed `core-structural-owner-guards` unit: all four packages pass |
| Public final projection still agrees with join settlement | `TestA2JoinPublicProjectionAgreementBothStores`; race/count-three pass (76.371s) |
| Reporter run.start fixture had no completion lifecycle | `TestServedReporterFiniteContractHasRealClosureCarriers` checks admitted topology; `TestIssue2566ReporterFiniteFixtureCanCloseBothStores` executes the declared close/ack path to completed, both stores race/count-three pass |
| CLI graph oracle still requires the retired terminal label | `TestDescribeCommandGraphRendersStageGraph`; exact final JSON/text projection and absence of the retired text marker, race/count-three pass |
| Old boot oracle still requires authored entry/end markers | `TestRun_StagesUseOrderedEntryAndOptionalFinal`; ordered waiting entry and zero final stages, valid service verifies; race/count-three pass |
| Persistence authority registry still names retired stage-catalog APIs | `TestPersistenceAuthorityFindingRegistry`, `TestPersistenceEffectiveMethodSetsDoNotExposeRawAuthority`, `TestPersistenceAuthorityDebtRatchetRejectsPaymentDuplicationReseedAndResurrection`; race/count-three pass; exact 18-for-18 signature update with all classifications preserved |
| Rewrite/runtime complexity introduces hotspots | Independent base/head measurement at `4c53d2712`: cognitive >=30 561/561, >=50 192/192; cyclomatic >=30 261/261, >=50 58/58; unchanged maxima/policy |

Whole-corpus replay remains 417 exact files / 553 decisions / byte-idempotent.
This one-time evidence does not substitute for the permanent semantic oracles.
Census and generated OpenRPC checks pass without raised ceilings.

## Failed Receipts And Qualification Boundary

The combined selected-store race/count-three command hit its 300-second aggregate
limit in the pre-existing contention matrix. It also exposed a test setup error:
a freshly rebuilt store facade had not passed canonical schema acceptance. The
fixture now uses existing canonical bootstrap owners; independent accepted
reload/source tests and branch-point materialization pass separately. The isolated
SQLite activation-first commit control and the four-cell both-store/writer-order
commit matrix pass without a timeout change.
The full combined contention command is not reported green or waived. Its complete
qualification remains due under the reviewer-bound tier.

All prior failed logs are retained in `test-results/agent-d-2566-cheap/`; none is
treated as a passing receipt. After the user's server2 handover, core at
`86b4b5950` stopped twice: first before tests because pg_config selected missing
PostgreSQL 18 server binaries (the installed PostgreSQL 16 binaries are selected
through the existing TEST_POSTGRES_BIN setting), then on the static-data
invocation identity oracle. The latter has unchanged content and two generated
IDs changed by rewritten bundle bytes. The existing updater regenerated only
those IDs; `TestStaticDataInvocationGoldenConsumesAdmittedIdentity` and catalog
oracle guards pass race/count-three with corruption assertions intact. Interrupted
units earn no qualification. The declaration-boundary repair changes production
code, so the replacement head requires a fresh core run. Core at `f60c098ae`
then passed the oracle guards but stopped on the served reporter family:
`run.start` correctly refused its stateless/no-final root. The reporter overlay
now has explicit active/final root and portfolio lifecycles, a fieldless root
entity, and a declared close-request/acknowledgement path with producer-owned
event schemas. Original held-delivery, exact transaction and supported-surface
assertions are preserved. `TestServedReporterFiniteContractHasRealClosureCarriers`
checks actual admitted topology; `TestIssue2566ReporterFiniteFixtureCanCloseBothStores`
executes the closure to completed on both stores rather than accepting decorative
final declarations. Production finite admission and the service example are
unchanged; the overlay inventory includes its one newly required entity file.
The combined four-test reporter race/count-three run hit its unchanged 180-second
package deadline without a reported assertion failure; it is not a green receipt.
The new both-store closure test passes separately at race/count-three. Original
reporter assertions are being qualified separately without changed limits.
`TestIssue2394HeldReporterConsumerKeepsRunUnreadyBothStores` (156.963s) and
`TestIssue2394ServedReporterTransactionCensusBothStores` (64.362s) pass separately
race/count-three; no assertion, repetition or timeout change.
`TestIssue2394ServedFanOutSupportedSurfacesBothStores` also passes separately
race/count-three, with the original supported-surface assertions unchanged.
The first replacement core at `324bf0454` stopped before execution: the newly
added closure proof had no primary full-plan owner. It is now explicitly selected
by the existing local reporter and full served-late units, with both store
children required. Existing selection, count modes, budgets and soaks are intact.
Core at `f5af987f9` then stopped on the CLI graph test's stale `[terminal]`
expectation while the supported output correctly reported `[final]`. The oracle
now requires the new label and rejects the old label. An adjacent obsolete boot
error oracle now explicitly checks declaration-order entry and optional final
admission; its valid service fixture declares all existing handler destinations.
Both focused tests pass race/count-three. The script ledger retains the original
entry/end decisions while replaying this bounded test update. Interrupted core
units are unqualified. Server2 was released; another run requires a new handoff.
After that explicit handoff, core at accepted `41f3a4739` progressed beyond the
earlier CLI/reporter failures, then stopped in `store-admission-full` on
`TestPersistenceAuthorityFindingRegistry`. Its 18 stale signatures are now
updated to the reviewed FinalCatalog/Final/FlowFinalStages APIs. The exact
one-to-one replacement preserves 4 typed-process-local, 12 private-backend and
2 typed-public-facade rows. No production code, guard logic, debt baseline or
ceiling changed. The existing updater passes, with no unclassified entries;
registry/raw-exposure/debt-control tests pass race/count-three (162.743s).
The first broader local race command also selected the heavyweight actual debt
census and timed out during historical-source type checking, before completing
that first test. It earns no passing credit; the census remains due through the
canonical managed unit. No test deadline was changed. The complete 41f core
receipt and interrupted units remain unqualified. Server2 was released for C,
and lifecycle/supplements did not start. The next core needs a fresh handoff.
The failed f60 core receipt remains unqualified. Lifecycle and
named supplements, hosted full, ten-family completion, and composed A/E supported
proof remain outstanding. No CI/PR was opened for these repairs.

## Complete cbaa Core Disposition And Local Repair

Core at `cbaa3127e` completed red; it is not qualified. The full receipt contains
three failure families, not only the first visible API assertion:

| Manifestation | Repair / separate execution proof |
| --- | --- |
| API/OpenRPC counts still expect 68 errors after `RUN_NEVER_COMPLETES` becomes error 69 | `TestPlatformAPISpecValidationCoverage`, `TestGeneratedOpenRPCArtifactMatchesPlatformSpec`, `TestGeneratedOpenRPCApplicationErrorCodesAreUnique`; race/count-three pass. Exact counts, named code/message, run.start binding and byte equality are retained. |
| Relation reorder control treats stage declaration order as presentation | `TestCompiledTransitionSourceAuthoredExactRelation`; race/count-three pass. The existing helper keeps stages and rules ordered, still reverses presentation-only declarations, and retains exact multiset/carrier/provenance assertions. The hostile sorted-entry control is unchanged. |
| New finite-refusal fixtures read raw pools | `TestFiniteRunStartRefusesServiceBeforeMutationBothStores`, `TestReviewer2566FeedOnlyRejectsNestedServiceBothStores`; both stores race/count-three pass. All 13 before/after durable counts move into the existing private selected-storage readback owner and its typed storetest facade; no caller SQL, table selection, callback or pool escapes. |
| New final-timer proof obtains a raw coordinator/database | `TestWorkflowFinalInitialTimersRemainUnarmedAcrossRestartBothStores`; both stores race/count-three pass (42.374s). Uses existing native timer construction/reopen fixtures, lifecycle commit/attachment owners, timer persistence and bounded storage observation. Final/non-final, two generations, run-wide and entity-local schedule counts, activation counts, scheduler active/draining and joined stop assertions remain. |
| New reporter closure proof carries a raw-pool served harness | `TestIssue2566ReporterFiniteFixtureCanCloseBothStores`; both stores race/count-three pass through the existing public serve process owner, original-DSN config and public run.diagnose. Opening delivery failure/quiescence/receipt settlement and declared close-to-completed are still checked. |
| New retained-source proof reconstructs a Postgres facade from its running pool | `TestStageCatalogRetainedStoreReloadAndSelectedForkSourceBothStores`; external component proof now constructs/reopens native selected stores from the original location via existing storetest owners. Exact order, typed initial/final facts and selected-fork source survive a changed live directory. |
| One partial native timer generator still emits `initial: %v` | The same committed rewrite script captures its exact deletion; ready remains first for transition starts and waiting first for initial/event starts. `TestWorkflowTimerCauseReplayEngineConsumersOnBothStores` passes race/count-three separately for sqlite (119.800s) and postgres (45.683s), covering every initial/event/transition cause and all five replay statuses, including later-cause assertions. |

No debt baseline or ceiling increases. The exact authority registry adds only
three classified operations inside the already-existing private runtime adapter;
no new raw call remains in the API, pipeline or served proof. The actual
`TestPersistenceAuthorityDebtRatchet` passes through the managed wrapper (56.029s).
The full managed `core-structural-owner-guards` unit passes all 78 required roots.
Every Census/Inventory/Registry test in all 40 touched executable packages passes
through the managed wrapper. The source specimen under pipeline/testdata is not
an executable package; store's TSV owner is explicitly included. The separate
canonical decoder census also checks monotonicity against `af250de63`.
Complexity passes with independently measured unchanged 561/192 cognitive and
261/58 cyclomatic hotspot counts and unchanged policy/maxima.

The first broad inventory command incorrectly exported the conformance-only
baseline environment variable into CLI/serve tests. Those tests correctly
rejected the unknown runtime setting. Its red receipt is retained; the corrected
complete run has no such export, and the monotonicity variable is scoped only
to its conformance package. No admission exception was added.
The combined native timer/generator race/count-three run hit its unchanged
180-second aggregate deadline during the third repetition, with the current
Postgres event/cancelled case running for one second and no assertion failure.
It earns no whole-command pass credit; generation/backend cases are being
qualified separately with the same assertions, repetitions and deadlines.
Those separate commands now pass for both databases, as does the final-timer
run-wide control. The complete rewrite/entry-golden package also passes
race/count-three (21.730s); the script still reproduces 417 files and retains all
553 independently reviewed entry/end decisions.

These are local working-tree receipts, not a replacement exact-head core tier
or merge approval. Server2 was released to E; D waits for the user's handoff
after E/C/A before running core on the committed repaired head. Lifecycle,
supplements, composed A/E proof and hosted full remain due. The scope and gate
are unchanged; no new issue, compatibility path or persistence authority owner
was introduced.

Existing watchlist mappings remain
`canonical_authored_grammar_effective_semantics_ownership` and
`workflow_stage_lifecycle_identity_carriage`; reviewer-d already refined their
selected-feed, explicit-source and non-disk-oracle manifestations. No new issue,
compatibility, migration, alternate router or semantic owner is introduced.

## Lifecycle Inventory Stop And Complete Guard Sweep

Lifecycle at `8bdd49d8d9d230d51289514f64d75dbcdaf050dd` stopped red on
`TestCompiledTransitionOwnershipGuard` in `contracts-first`. The joined receipt
is `lifecycle-20261007T111658.155166164`, plan
`987c0392ec98403d1d273c6f6d21a30e2b88fb5dea1c5b15ffde089468da22a2`:
15 units passed, one failed, three siblings were cancelled, and 36 did not start.
The cancelled serveapp process-temporal, native and runtime units are not
independent diagnosed failures or qualification receipts. No supplement, soak,
hosted CI or PR started. Server2 was released for the next lane.

The omitted guard was not selected by the earlier Census/Inventory/Registry
sweep. Its exact caller inventory now registers the approved canonical catalog
consumers rather than permitting a package, file or family wholesale:

| Exact caller / operation | Inventory reconciliation |
| --- | --- |
| `BuildWorkflowStageTopology` / graph metadata | 12 to 14: the compiler records declaration order in its private catalog. |
| `WorkflowContractBundle.flowStageCatalog` / `WorkflowStageTopology` | One canonical catalog lookup, not schema reconstruction. |
| `WorkflowStageTopology.FinalStageIDs` / graph metadata | One private final-set projection. |
| `WorkflowStageTopology.HandlerSourceErrors` / graph metadata | One copied diagnostic projection, not executable authority. |
| `WorkflowStageTopology.explicitHandlerSources` / graph metadata | Three exact accesses retain explicit final-source rejection in the compiler. |
| `WorkflowStageTopology.StageIDs` / graph metadata | Two to one: copied declaration order replaces the old map enumeration. |
| `workflowTimerMayArmAtStage` / `semanticview.WorkflowStageTopology` | One exact lookup for new timer arms; accepted work remains outside this predicate. |
| `runstart.ValidateFinite` / `semanticview.WorkflowStageTopology` | One exact lookup for each constructor/routing activation root. |
| `runstart.ValidateFinite` / graph metadata | One exact flow-identity check, not transition admission. |

The guard collector, exact-count comparison and hostile overlays are unchanged.
No production code, authority/debt baseline, complexity policy, deadline,
capability check or supported assertion changed in this repair.
`TestCompiledTransitionOwnershipGuard` and
`TestCompiledTransitionOwnershipGuardRejectsHostileUses` pass `-race -count=3`
(95.213s), including extra-use rejection inside an already registered function.

The complete managed local sweep now selects every
`Guard|Inventory|Census|Registry` test across `internal/...`, `cmd/...` and
`scripts/...`, plus the existing closed-construction and partition controls.
It passes: 200 package test commands, of which 65 execute selected
tests; 82 additional packages have no test files. All 40 touched executable
packages are covered. The only excluded touched Go directory is the retained
non-executable specimen under `pipeline/testdata/diagnostics`. Contracts passes
all selected roots in 151.866s; pipeline, engine, serveapp and private store
packages also pass. The actual persistence debt ratchet passes separately through
the managed wrapper (29.586s). The exact 78-root structural selection passes
through the same wrapper across all four required packages, without changing
the selection, assertions or inventories. These are local repair checks, not a
replacement lifecycle receipt.

This is inventory repair under the existing gate, not a new semantic class or
closure claim. Core's earlier 22/22 receipt is not relabelled as this head.
Lifecycle and all 13 named supplements remain due on the replacement head;
composed A/E proof and hosted full remain merge obligations. The existing
watchlist mappings above remain sufficient.

## API And Data Fixture Integration Repairs

Replacement lifecycle at `3bd1d380eb306740874f2a0e279f42dfe116a5b8` passes the
previous transition guard, then stops red. The joined receipt is
`lifecycle-20261007T114909.396823937`, plan
`646ca3ebefa9ef09004121e5491d433f1abb4cdbee67e6d78e5c80d1da7e5687`:
20 units pass, API and two conformance units contain actual failures, one
additional conformance unit is cancelled, and 31 units do not start. Supplements
remain unrun. Server2 was released immediately; the cancelled reporter/control
unit is not diagnosed as a separate or inherited semantic failure.

All actual failures are introduced by this branch. The same nine original
test roots pass on the clean `af250de63` baseline (API 20.062s, conformance
3.255s). This is execution evidence, not an inference from unchanged file names.

| Failure family | Bounded repair and preserved assertion |
| --- | --- |
| `TestOpenRPCMutatingHTTPRuntimeProbes` lacks the new declared `RUN_NEVER_COMPLETES` case | Add a service-source case through the existing HTTP probe harness and the actual finite-start owner. Its ordinary typed source is recompiled after removing final membership; no injected substitute validator. Existing declared-error, authentication, result and zero-effect assertions remain. |
| Six event-publish/runtime-context roots observe an extra root delivery | The shared API fixture's root close handler now listens only to its dedicated declared close input, not the business event. All original exact node/agent recipient counts, IDs, replay/idempotency, stored-completion and paused/existing-run assertions remain unchanged. |
| Fieldless import-shape proof assumes the fixture has only one declaration | Match the fieldless shape by exact declaration identity, require its non-nil empty fields, and independently check the one-field close declaration. Require exactly the two known declarations; selected-store and public `fields: []` assertions remain. |
| Dotted-collision hostile proof expects the old output block list | Restore that representation in the existing closed fixture owner while retaining the close pin and both connects. The original exact text-match, ambiguous-selector rejection and before/after no-mutation assertions are unchanged. No raw YAML reconstruction or new fixture owner. |

The seven API roots pass `-race -count=3` (231.363s). Both conformance roots,
including every SQLite/PostgreSQL hostile case and public fieldless readback,
pass `-race -count=3` (44.906s). The original 417-file/553-site ledger and
supplemental eight-file plan replay exactly and byte-idempotently; only the
supplemental reviewed output hashes/edits change. Production execution,
admission, assertion counts, budgets and timeouts are unchanged.

Full local API/conformance unit reproduction and the complete guard sweep are
separate obligations; focused green is not a lifecycle receipt. No replacement
PR proof audit is posted while qualification is red. The gate, class boundary
and existing watchlist mappings remain unchanged.

The first complete local `api-llm-bus-full` reproduction at `ececd0e6b` passes
the original repaired roots, bus and LLM, but finds two diagnostic child failures
inside `TestOperatorRunStartHandlersFailClosedBeforePersistence`. The fixture's
new dedicated close input correctly appears in the canonical declared/routable
vocabulary. Those exact expected lists now include both known inputs, in sorted
order; the unroutable case still requires an empty routable list. Error codes,
reasons and every no-persistence assertion remain unchanged. The entire root
passes `-race -count=3` (34.233s). The failed full-unit receipt is retained and
does not qualify this replacement; a new exact-head unit run remains required.
