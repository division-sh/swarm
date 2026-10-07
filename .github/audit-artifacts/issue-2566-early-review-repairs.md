# #2566 Early Review Repairs

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
