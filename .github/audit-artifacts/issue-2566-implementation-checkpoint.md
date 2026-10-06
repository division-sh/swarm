# #2566 Integration Checkpoint

Baseline: `af250de638b413207372e70e709b95d1881b05cc`. The original head is
preserved on `agent-d/2566-before-af250de63`; the branch was rebased, not stacked
on an unmerged sibling. Master removed inert `instance_variables` in four
overlapping schemas; those removals were preserved.

This is progress evidence, **not a Post-Implementation Proof Audit or a
merge-readiness/parent-closure claim**. Gate D6019841561 remains binding.

## Implemented Owners

- Existing ordered schema admission and private stage catalog own first entry
  and explicit optional final membership. Old authored markers reject on
  presence. Public lists and describe remain projections, not stage authority.
- The finite predicate consumes compiled connect endpoints and the same eager
  keyless child selector as manager construction. It rejects missing or
  contradictory catalogs, includes connected templates and their constructor
  children, and excludes unrelated unconnected dormant templates. It does not
  infer graph sinks, prune CEL routes, or inspect filesystem membership.
- Both `run.start` creation paths use that predicate before their atomic
  creation owners. Existing selected-store idempotency lookup, permanent
  creation receipts and conflicts retain their owners. Ordinary service
  publication is not given a blanket finite-start veto.
- `RUN_NEVER_COMPLETES` is declared in the authoritative API spec and generated
  OpenRPC, with exact offending flow and guidance. CLI refusal exits6 and does
  not follow/subscribe after a failed start.
- New timer applicability consumes the catalog only at new-arm boundaries.
  Accepted topology/readback/fire/retry/cancel paths were not moved onto it.

## Corpus And Fixtures

The finite hash/anchor-checked script includes committed output,553 retained
positive entry/end decisions and explicit supplementary fragment/oracle
dispositions across417 files. `-prove` replays each file from the recorded Git baseline and
proves byte equality and second-application idempotence. That is a one-time
receipt, not a perpetual checksum freeze on unrelated future fixture changes.

204 permanent disk source/flow/entry/end goldens derive from the independently
reviewed pre-change decisions. A hostile sorted dump remains fully reachable
but fails the entry guard. The two original reviewed declaration moves remain
explicit; generated starting-stage variants move exact declarations rather
than restoring markers. Entity-field initial values remain independent.

The positive API fixture now has real end-stage carriers. The empty-feed
deployment fixture is loaded through actual source admission: its root is
explicitly ended and its workers are data-created templates, not stranded
eager children. Original empty-feed completion, pin cardinality, permanent
receipt replay and rejection-before-effects assertions remain intact.

## Focused Receipts

Executed on vemew; no core/lifecycle/full tier or hosted CI is claimed here.

| Surface | Exact Proof | Result |
| --- | --- | --- |
| Ordered/final grammar, presence and source parity | `TestSchemaAdmission*`, `TestFinalStage*`, `TestCompiledStage*` | race/count-three pass |
| Guard reachability and exact expression coordinates | `TestRunGuardTerminationReachabilityThroughSourceAdmission`, `TestScalar2556GateContextSourceDiagnostic` | race/count-three pass |
| Constructor/routing closure | `TestFiniteStart*` | race/count-three pass |
| Service refusal, three initiation forms,13 unchanged durable tables | `TestFiniteRunStartRefusesServiceBeforeMutationBothStores` | both stores, race/count-three pass |
| Ordinary publication remains separate | `TestFiniteRunStartDoesNotVetoOrdinaryServicePublicationBothStores` | both stores, race/count-three pass |
| Finite receipt replay/conflicts | `TestFiniteRunStartReceiptReplayAndConflictBothStores` | both stores, race/count-three pass |
| Existing initiation/data preservation | `TestOperatorRunStart*`, `TestRunStart*`, `TestFeedOnlyRunStart*`, `TestDeploymentRunStart*` | race/count-three pass |
| Persisted final no-arm and non-final positive control across restart | `TestWorkflowFinalInitialTimersRemainUnarmedAcrossRestartBothStores` | both stores, race/count-three pass |
| Accepted timer validation remains separate | `TestWorkflowFinalTimerNoArmDoesNotChangeAcceptedTopologyValidation` | race/count-three pass |
| Gate/join preservation | `TestWorkflowGateEntryCreatesMatchingActivationAndCardOnBothStores`, `TestWorkflowJoinExpectedZero*`, `TestWorkflowJoinArmArrivalRaceIsEarlyOrAdmittedOnBothStores` | race/count-three pass |
| Entry/capture/hash/anchor/disposition guards | `go test ./scripts/rewrite-stages-2566 -race -count=3` | pass |
| Public refusal and text projection | `TestRunCommandStartApplicationErrorsExitSixAndDoNotFollow`, `TestDescribeTextFactoringCharacterization` | race/count-three pass |
| Public compiled describe corpus | `TestReadProofFactoringCompiledDescribe` | generated output and no-update validation pass110.521s |
| Admission inventory | `TestCanonicalFormsRegistryRatchetsOffOwnerDecodeBypasses` | pass; no ceiling changes |

## Remaining Qualification

Request the user-controlled server2 core slot, then run Local-Tier lifecycle and
the exact Gate D supplements. Compose
E/A's relevant implementing pins before claiming late/no-handler versus
authority refusal or accepted-work disposition. Complete fresh source
restart/selected-fork and ten-family completion proof, exact-head hosted full
CI, final owner/manifestation inventory and the actual Post-Implementation Proof
Audit. Parent issues remain open. No compatibility, old-store migration,
alternate router or third stage owner is introduced.

First runnable size receipt against the baseline: runtime/CLI/store production
`+317/-232` (net `+85`); all non-test Go including offline rewrite tooling and
fixture generators `+1324/-470` (net `+854`). Corpus/golden output is separate.
No net-negative claim or artificially compressed/unrelated deletion is made.
