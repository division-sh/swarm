# #2319 Startup Predicate Census Accounting

#2560 introduced the test-only `verify_admission_predicate_census.json` while
#2319 was in flight. Its first integration check rejects the already-approved
ingress admission changes, starting at `Runtime.ReplaceChannelActivationsContext`.
The generated refresh accounts for 11 added and 12 changed function bodies,
with no removed owner, changed classification, scanner exemption, or weakened
mutation test. The census is a drift detector, not execution/closure proof.

The inherited `audit_rows` and `proof_families` identify **#2286's** audit.
The explicit execution witnesses below belong to **#2319**. They are not
interchangeable numbering schemes. Canonical ownership remains the existing
credential snapshot, runtime frozen admission, selected-store responsibility,
complete channel publication/lease, and registration/effect owners.

All `serveChannelActivationRefresher`, `serveChannelConfirmationDispatcher`,
`serveConnectedChannelReadiness`, and `compileServe*` entries below are in
`internal/serveapp/channel_onboarding.go`. `Runtime` entries are in
`internal/runtime/runtime.go`; composition/readiness entries are in
`internal/serveapp/main.go`.

| Audited body | Change / consumer | Execution witness |
| --- | --- | --- |
| `Runtime.ReplaceChannelActivationsContext` | Existing owner now requires exact admitted replacement; unchanged admitted refresh preserves in-flight leases. | `TestAdmittedReplacementRechecksAfterDrainAndNeverRevivesStalePredecessor`, `TestAdmittedCurrentPublicationRefreshPreservesInFlightLease`, `TestChannelNativeInboxDelayedApplyDuringReconnectStartE2E` |
| `newRuntime` | Existing constructor freezes the declared prerequisite snapshot and keeps #2560's standing-restart reader; no request/readback admission. | `TestStandingIngressAdmissionDistinguishesAbsentFromInvalid`, `TestDormantIngressProvisionRestartSignedInputBothStores`, `TestServeIngressFrozenCredentialMutationAndRestartBothStores` |
| `serveChannelActivationRefresher.AdmitChannelTarget` | Explicit admission uses the selected runtime target, not another source or a receipt-blind declaration key. | `TestStaleLearnedRegistrationFreshReconnectBothStores`, `TestExplicitStandingAdmissionCannotRefreshSiblingAuthority`, `TestServeIngressFrozenCredentialSiblingIsolationBothStores` |
| `compileServeDeclaredChannelActivations` | Declaration publication consumes frozen runtime admission; absent credentials cannot create executable publication. | `TestDormantIngressDevPublishesNoAuthority`, `TestDormantIngressReadbackCannotHotActivate`, `TestServeIngressFrozenCredentialMutationAndRestartBothStores` |
| `compileServeLearnedChannelActivations` | Learned publication consumes exact retained responsibility and all written receipts/seals. | `TestStandingLearnedAuthorityCredentialMatrix`, `TestStaleLearnedIngressRecoveryRequiredBothStores`, `TestCompletedLearnedIngressRequiresFreshReconnectBothStores` |
| `compileServePrebindingActivations` | Preparing/pending registration consumes its exact operation and preserves an inherited reconnect obligation. | `TestChannelOnboardingPendingResetRestartToReadyE2E`, `TestChannelOnboardingConfirmedChildRotationResumeE2E`, `TestStaleLearnedRegistrationFreshReconnectBothStores` |
| `observeChannelActivationAdmissions` | Existing snapshot owner observes every compiled role and checks learned receipt/seal evidence; no fallback. | `TestStandingRegistrationRequiresEveryCompiledCredentialRole`, `TestAdmittedActivationCredentialRequiresExactValueAndReceipt`, `TestChannelProviderConsumersRequireExactAdmission` |
| `priorServeDeclaredActivations` | Carries only the exact previous declared publication; it cannot refresh a learned owner or hot-adopt a value. | `TestProviderRegistrationRotationCannotRefreshRuntimeIngressAdmission`, `TestServeIngressFrozenCredentialSiblingIsolationBothStores` |
| `publishServeChannelActivationSnapshot` | Publishes a complete admitted immutable snapshot through the existing owner, with predecessor lease fencing. | `TestChannelProviderConsumersRequireExactAdmission`, `TestAdmittedReplacementRechecksAfterDrainAndNeverRevivesStalePredecessor`, `TestChannelSourceLifecyclePublicJourney` |
| `reconcileServeProviderRegistrations` | Existing slot/effect owner consumes the exact admitted pair and preserves unresolved settlement without blind resend. | `TestProviderRegistrationReconcilerCollisionConvergenceAndNoResend`, `TestProviderRegistrationRetainsSettlementIdentityAndSharesCallbackCurrentness`, `TestProviderRegistrationHandoffKeepsAuthoritiesDistinctUntilPromotion` |
| `serveChannelCandidatesForPlan` | Onboarding discovery starts from declarations and exact existing responsibility, not execution-only targets. | `TestChannelSigningFallbackPublicRefusalBothStores`, `TestStaleLearnedRegistrationFreshReconnectBothStores` |
| `serveChannelDeclarationTarget` | Declaration-coordinate lookup carries source/flow/provider/alias scope without granting execution. | `TestChannelSigningFallbackPublicRefusalBothStores`, `TestResolveStandingTargetDeclarationsRequiresExactProviderPin`, `TestResolveStandingTargetDeclarationsConsumesCanonicalInputAssociation` |
| `serveDeclaredRegistrationEnabled` | Registration eligibility consumes selected frozen admission, not current key presence. | `TestProviderRegistrationRotationCannotRefreshRuntimeIngressAdmission`, `TestServeIngressFrozenCredentialMutationAndRestartBothStores` |
| `serveChannelActivationRefresher.RefreshChannelActivationCandidates` | Reconciliation keeps stale learned responsibility recoverable but non-executable; exact fresh admission alone restores execution. | `TestStaleLearnedIngressRecoveryRequiredBothStores`, `TestStaleLearnedRegistrationFreshReconnectBothStores`, `TestServeIngressFrozenCredentialSiblingIsolationBothStores` |
| `serveChannelActivationRefresher.publishChannelActivations` | Existing publication owner performs admitted replacement rather than rebuilding authority locally. | `TestAdmittedCurrentPublicationRefreshPreservesInFlightLease`, `TestChannelNativeInboxDelayedApplyDuringReconnectStartE2E`, `TestChannelSourceLifecyclePublicJourney` |
| `serveChannelConfirmationDispatcher.DispatchChannelConfirmation` | Confirmation uses exact frozen admission and rechecks responsibility at provider boundaries. | `TestChannelConfirmationConsumesOneSealValidatedCredentialObservation`, `TestChannelProviderPreflightGuardsAuthorizationLaunchAndDispatch`, `TestChannelOnboardingAdmittedEffectRestart` |
| `serveConnectedChannelReadiness.ProjectConnectedChannelReadiness` | Readiness checks selected admitted registration and credentials without resealing replacements. | `TestRegistrationSelectionCurrentnessAtProviderBoundaries`, `TestProviderRegistrationRetainsSettlementIdentityAndSharesCallbackCurrentness`, `TestProviderRegistrationRotationCannotRefreshRuntimeIngressAdmission` |
| `compileServeChannelActivationSnapshot` | Owner-local assembly joins declared/learned/prebinding plans with exact evidence before publication. | `TestStandingLearnedAuthorityCredentialMatrix`, `TestStandingRegistrationRequiresEveryCompiledCredentialRole`, `TestChannelProviderConsumersRequireExactAdmission` |
| `serveChannelOnboardingCatalog` | Declaration-first discovery survives dormancy; it cannot fabricate an enabled target. | `TestChannelSigningFallbackPublicRefusalBothStores`, `TestDormantIngressProvisionRestartSignedInputBothStores`, `TestChannelConnectTelegramFirstUserJourney` |
| `serveStandingServiceController.mutateStandingService` | Selected-runtime mutation captures exact credential admission and retains existing drain/rollback/cancellation fences. | `TestStandingServiceMutationsUseSelectedRuntimePipelineOnBothStores`, `TestRuntimeContextManagerRejectsCredentialProjectionStaleAfterSuppression`, `TestStandingMutationsRemainFencedUntilResetConsumersConverge` |
| `buildRuntimeComposition` | Complete credential-aware standing desired state is reconciled before runtime publication; structural/backend errors remain independent. | `TestDormantIngressProvisionRestartSignedInputBothStores`, `TestDormantIngressDoesNotWaiveIndependentOutboundCredential`, `TestDormantIngressCannotMaskStructuralRefusal` |
| `buildServeRuntimeBundleContext` | Runtime context consumes the admitted module and frozen credential owner; replacement/readback cannot hot-admit. | `TestDormantIngressReadbackCannotHotActivate`, `TestServeIngressFrozenCredentialMutationAndRestartBothStores`, `TestChannelSourceLifecyclePublicJourney` |
| `reportServeStandingReadiness` | Public diagnostics distinguish dormant/recovery-required identity, current presence, and frozen executable eligibility. | `TestDormantIngressProvisionRestartSignedInputBothStores`, `TestRuntimeContextManagerReadbackSeparatesPresenceFromFrozenAdmission`, `TestServeIngressFrozenCredentialMutationAndRestartBothStores` |

The refresh uses only the existing
`SWARM_UPDATE_ADMISSION_PREDICATE_CENSUS=1` generator. Normal qualification must
run with that variable absent, including `TestVerifyBootStartupPredicateCensus`
and the three `TestAdmissionPredicateRatchetRejects*` mutation controls.

#2560's verify consumer remains an inspection consumer. Its exact retained
inventory helper explicitly grants neither successor runtime binding nor
executable credential authority. The executable-reader census retains both
`internal/cliapp/verify_deployment.go` and
`internal/runtime/channel_activation_admission.go`; neither consumer is omitted.

The three conflict resolutions preserve both branches' behavior: runtime field
union, removal of the retired ingress-mode interpreter in favor of
`cliapp.ResolveServePublicIngressMode`, and reader-census union. This accounting
refresh changes test evidence only. It does not justify treating hashes as
closure proof, changing #2319's gate, or claiming #2286's broader class closed.
