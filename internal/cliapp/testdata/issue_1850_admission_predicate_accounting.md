# #1850 Startup Predicate Census Accounting

Core qualification at `c9ad2d593` failed the independent startup predicate
census. The existing generator refreshes nine changed bodies and one added
helper from the approved clock integration. It removes no owner, changes no
existing classification, and grants no scanner exemption. Hash agreement is
drift evidence, not behavioral closure.

The census's inherited `audit_rows` and `proof_families` refer to #2286, not
#1850. The execution witnesses below refer to #1850's P01-P25 coverage.
Runtime entries are in `internal/runtime/runtime.go`; serve entries are in
`internal/serveapp/main.go`.

| Audited Body | Owner / Consumer Change | Execution Witness |
| --- | --- | --- |
| `Runtime.prepareStartLocked` | After grant and Manager admission, completes the exact prepared standing finalizers before source-scoped startup verification. A newly credential-enabled generation can be constructed after the initial snapshot; unrelated pending rows still need their own admission. The finalizer and unchanged readiness guard retain all source, attempt, and generation checks. This refresh changes only this body's hash. | `TestServedClockRetainsWithoutIngressCredentialsBothStores` (SQLite/PostgreSQL), `TestStartupTopologyRefusesUnfinalizedPostSnapshotConstruction`, `TestInterruptedStandingPreRunRequiresExplicitRecovery`, `TestComposedStartupWithholdsStandingTimerPublicationUntilRecoveryOnBothStores` |
| `Runtime.releaseAutonomousStartupProducers` | Startup restores only generic control families through `RestoreControlSchedules`. Declared instance clocks require their selected deployment owner; the recovery, continuation and scheduler-release ordering is unchanged. | `TestRuntimeControlRestorationDoesNotArmInstanceClocks`, `TestComposedStartupWithholdsStandingTimerPublicationUntilRecoveryOnBothStores` |
| `buildServeRuntimeBundleContext` | The existing composition selects clock binding only for deployment, never finite `LocalRun`. | `TestServedClockStartupAndFiniteHostFrontierBothStores`, `TestDeclaredClockBindingRequiresDeploymentSelection` |
| `buildRuntimeComposition` | The existing standing reconciler receives deployment selection explicitly. | `TestServedClockStartupAndFiniteHostFrontierBothStores`, `TestFiniteStandingReconciliationCannotOwnDeclarationAbsence` |
| `reconcileServeStandingServices` | Delegates the admitted candidate set to the existing selected-store reconciliation operations; it does not create another service owner. | `TestFiniteStandingReconciliationCannotOwnDeclarationAbsence`, `TestServedClockDeclarationRemovalRetiresLastBindingBothStores` |
| `reconcileStandingServiceCandidates` (added) | Deployment owns complete-set selection/removal. Finite hosting reconciles only its candidates and cannot cancel unselected deployment bindings. | `TestFiniteStandingReconciliationCannotOwnDeclarationAbsence`, `TestServedClockStartupAndFiniteHostFrontierBothStores` |
| `serveStandingServiceController.mutateStandingService` | Rejects finite-host clock mutation before the native operation, preserving independent ingress bindings and existing credential admission. | `TestFiniteHostCannotRearmIndependentlyStandingClockBothStores`, `TestFiniteClockSelectionPreservesIngressBindings` |
| `serveStandingServiceController.publishActiveService` | Publishes constructed targets and exact activations before arming through the selected Runtime clock owner. | `TestServedClockBindingDisarmResetAndRestartBothStores` |
| `prepareServeRuntimeContextSet` | Carries exact activations into the existing context manager; starts real construction/readiness before arming clocks. | `TestComposedStartupWithholdsStandingTimerPublicationUntilRecoveryOnBothStores`, `TestServedClockStartupAndFiniteHostFrontierBothStores` |
| `newServeStartupStandingRecoveryOwner` | Uses canonical `StandingExecutionIdentities`, including admitted keyless clock activations, to retain exact run/service/generation ownership. | `TestComposedStartupWithholdsStandingTimerPublicationUntilRecoveryOnBothStores`, `TestServedClockActiveDowntimeAndRetainedSourceBothStores` |
| `serveStartupStandingRecoveryOwner.BeginStandingRunRecovery` | Keeps existing admitted-work ownership and compares the exact generation against the canonical typed identity. | `TestServedClockActiveDowntimeAndRetainedSourceBothStores`, `TestServedClockPendingConsumerReturnsAndRestartsBothStores` |

The refresh uses only `SWARM_UPDATE_ADMISSION_PREDICATE_CENSUS=1` with the
existing `TestVerifyBootStartupPredicateCensus` generator. Normal qualification
must leave that variable absent and run the census plus all three
`TestAdmissionPredicateRatchetRejects*` mutation controls unchanged.

## #2438 Declaration/Receiver Separation Refresh

The checkpoint-3 census at `9f6b7720c` detected three changed startup consumers.
Their source-body hashes are regenerated, not hand-edited. Existing audit-row
classifications remain unchanged; the current A9 allocation and P01-P36 matrix
are recorded in #2438 comment6039539329. These component witnesses do not close
the unfinished nested, lifecycle or compiled-binary matrix.

| Audited Body | Owner / Consumer Change | Execution Witness |
| --- | --- | --- |
| `newRuntime` | Replaces two function-only constructor adapters with one capability-transparent Manager forwarding adapter, preserving preparation/finalization and the merged immutable construction receipt reader. It adds no constructor, router or fallback. | `TestA9KeyedRootIngressConstructionBothStores`: real served SQLite/PostgreSQL first construction, exact retry, original receipt and changed-key no-mutation refusal. |
| `serveStandingServiceController.mutateStandingService` | The canonical standing candidate carries declaration/source authority rather than mandatory receiver coordinates. Current-generation and desired-state operations remain owned by the existing Pipeline/native owners. | Native `TestA9KeyedStandingGenerationDoesNotConstructBothStores`; complete served keyed suspend/resume/reset proof remains pending under P29. |
| `reconcileServeRuntimeStandingTargets` | Uses `StandingConstructionIsKeyless` to distinguish a genuine eager construction from an unconstructed keyed declaration. Run/generation/publication facts remain mandatory; a typed absent construction does not manufacture a receiver. | `TestA9KeyedRootIngressConstructionBothStores`, `TestA9KeyedStandingGenerationDoesNotConstructBothStores`; nested ancestry remains P17, not credited by the root positive. |

The same census detected 38 changed production persistence-registry identities:
25 private-backend operations (binding/receipt SQL shape changes and the existing
canonical construction receipt read transaction), nine typed process-local
interfaces/context/forwarding carriers, and four typed public facade methods.
Each is explicitly classified in the regenerated exact registry. This does not
increase the separate authority-debt baseline or exempt new raw-SQL test sites.
The changed raw standing fixture writes are removed rather than grandfathered.
The workflow-timer restoration control uses explicit standing classification
input with real SQLite/PostgreSQL timer storage; it is not native standing
construction qualification. Bounded provider fixtures use the existing native
standing reconciliation/publication owners; their old entity-scoped readback
assertions are still being migrated and are not claimed passing.

The core failure remains a failed receipt, including its interrupted and
unstarted units. Focused execution does not substitute for a complete core,
lifecycle or hosted-CI run. #2411's exact E-owned dormant-sibling restart
exception was the historical disposition, not a CI waiver. The final #2526
correction now repairs that restart ordering through the existing release
owner; the original failed receipts remain retained and the same served
reproducer must pass on both stores before this correction is qualified.
