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

The core failure remains a failed receipt, including its interrupted and
unstarted units. Focused execution does not substitute for a complete core,
lifecycle or hosted-CI run. #2411's exact E-owned dormant-sibling restart
exception was the historical disposition, not a CI waiver. The final #2526
correction now repairs that restart ordering through the existing release
owner; the original failed receipts remain retained and the same served
reproducer must pass on both stores before this correction is qualified.
