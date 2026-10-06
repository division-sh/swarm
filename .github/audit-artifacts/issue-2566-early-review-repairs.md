# #2566 Early Review Repairs

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
| Retained source order/finals lost on new store reader or selected fork load | `TestStageCatalogRetainedStoreReloadAndSelectedForkSourceBothStores`; independent accepted SQLite pool/PostgreSQL facade and actual selected-fork source loader, both stores race/count-three pass |
| Materialized selected fork source changes its stage catalog | `assertRetainedStageCatalogsForFork` in `stageForkContentionFixtureAt`; `TestSelectedBranchPointActivationAtomicityBothStores`, event/deployment-revision and running/cancelled cases, both stores race/count-three pass |
| Exact retained catalog survives either writer/activation ordering | `TestSelectedRunForkActivationFrontierContentionBothStores/(sqlite\|postgres)/(writer\|activation)/commit`; all four cases, race/count-three pass (156.767s aggregate) |
| Two test interfaces fail to compile | Exact managed `core-structural-owner-guards` unit: all four packages pass |
| Public final projection still agrees with join settlement | `TestA2JoinPublicProjectionAgreementBothStores`; race/count-three pass (76.371s) |
| Reporter run.start fixture had no completion lifecycle | `TestServedReporterFiniteContractHasRealClosureCarriers` checks admitted topology; `TestIssue2566ReporterFiniteFixtureCanCloseBothStores` executes the declared close/ack path to completed, both stores race/count-three pass |
| CLI graph oracle still requires the retired terminal label | `TestDescribeCommandGraphRendersStageGraph`; exact final JSON/text projection and absence of the retired text marker, race/count-three pass |
| Old boot oracle still requires authored entry/end markers | `TestRun_StagesUseOrderedEntryAndOptionalFinal`; ordered waiting entry and zero final stages, valid service verifies; race/count-three pass |
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
The failed f60 core receipt remains unqualified. Lifecycle and
named supplements, hosted full, ten-family completion, and composed A/E supported
proof remain outstanding. No CI/PR was opened for these repairs.

Existing watchlist mappings remain
`canonical_authored_grammar_effective_semantics_ownership` and
`workflow_stage_lifecycle_identity_carriage`; reviewer-d already refined their
selected-feed, explicit-source and non-disk-oracle manifestations. No new issue,
compatibility, migration, alternate router or semantic owner is introduced.
