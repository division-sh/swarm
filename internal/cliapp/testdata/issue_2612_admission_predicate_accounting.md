# #2612 Startup Predicate Census Accounting

Hosted run 38140877329 at 5ce411aae correctly rejected an uncaptured runtime
body. The existing generator adds exactly two helpers and changes exactly one
body; it removes no owner and changes no audit/proof classification or scanner.
The A/P identifiers below refer to the inherited #2286 audit, not #2611's
manifestation numbering. This is owner/caller accounting, not a hash waiver.

## Changed Owners And Consumers

| Measured body | Actual owner, consumers and invariant | Execution witnesses |
| --- | --- | --- |
| `Runtime.drainAdmittedIngress` (added) | The existing shutdownAdmission gate owns admitted ingress. `stopWithOptions`, `drainIngressForContextWithdrawal`, and `RuntimeContextManager.deactivateBundleHashWithOptions`'s rejected-drain branch consume its join. It closes/joins the existing fan-out producer, then joins all admitted producers before continuation retirement or dependency destruction, including grace-expiry cleanup. It grants no startup, provider, actor, source or delivery authority. | `TestRuntimeShutdown_ClosesAdmissionBeforeManagerDrainAndInboundIngress`, `TestRuntimeShutdownWithOptions_PropagatesConfiguredGraceToManagerDrain`, `TestRuntimeShutdownFanOutCommittedTurnKeepsDependenciesLive`, `TestRuntimeContextUseOwnsOneIngressFrameUntilDone`, `TestRuntimeContextUseOwnerRefusalReleasesIngressFrame`, `TestRuntimeContextManagerRejectedDrainJoinsRetiredStandingChildren`. |
| `Runtime.drainIngressForContextWithdrawal` (added) | `RuntimeContextManager.deactivateBundleHashWithOptions` consumes the exact runtime's join after visibility/admission fencing and before coordinator/standing/runtime retirement. Single withdrawal and `DeactivateAllWithOptions` both reach this owner. It preserves Manager shutdown admission and the original finite grace; rejection still joins already-retired children and retains incomplete cleanup ownership. | `TestRuntimeContextManagerDeactivationBoundsStuckAdmissionByConfiguredGrace`, `TestRuntimeContextManagerDeactivateAllFencesCompleteSetBeforeFirstDrain`, `TestRuntimeContextManagerRejectedDrainJoinsRetiredStandingChildren`, `TestRuntimeContextManagerTerminalDrainKeepsRefreshIncompleteAndFencesWholeSet`. |
| `Runtime.stopWithOptions` (changed) | Public Shutdown/ShutdownWithOptions and PrepareStart's owned failure cleanup retain the same shutdown owner. Only the extracted join's placement changes: admitted ingress drains before delivery-continuation retirement, followed by lifecycle/Manager and runtime cleanup. No cancellation is reported as a successful refresh; no provider adapter or boot predicate changes. | The native shutdown controls above, `TestRuntimeStartOwnsEarlyFailureAndPanicCleanup`, and `TestRuntimeContextManagerRetriesShutdownAfterPriorFailure`. |

The real served `TestPublicCompletionManagedCommitCutBothStores` matrix remains
the managed native-COMMIT/default-restart witness in #2612's audit. Its
cancellation-winning cells prove durable terminal disposition only: actual
nonempty `AcceptCommitted` transfer is UNOBSERVED there. Positive handoff and
held-tail controls retain their separate provenance.

## Preserved #2286 Admission Obligations

These runtime.go helpers are included because the independent census measures
every function in the file. They are cleanup/execution consumers, not new
read-only verifier operations. `newRuntime`, RuntimeDeps validation, PrepareStart,
pure admission owners, and retained inspection readers are unchanged. Verification
does not acquire an ingress lease or execute either drain. The listed witnesses
are re-executed, not inferred from generator success or an unchanged registry.

| Audit rows / proof family | Preserved owner and concrete execution witnesses |
| --- | --- |
| A05 / P04 | Source/mock effect purpose and RuntimeDeps admission: `TestCommandMockStartupRejectsRestoredLiveDescriptor`, `TestCommandLiveStartupRequiresCredentialDespiteSourceDoubles`, `TestRuntimeDepsValidateOwnsRequiredBootInputs`, `TestWorkflowAdmissionAccountsForPostRegistryClauses`. |
| A08 / P08 | Activity/provider source admission remains pure and fail-closed: `TestValidateWorkflowContractSurface_DurableActivityHTTPToolRequiresEffectClass`, `TestValidateWorkflowContractSurface_DurableActivityMinimalHTTPAccepted`, `TestValidateWorkflowContractSurface_TelegramProviderConnectorToolAdmitted`, `TestStandingIngressAdmissionOmissionIsPackRequired`. Real selected-store read scope/expiry remains `TestAdmissionInspectionRealReadScopeBothStores`. |
| A11 / P09 | Exact source/profile/model selection and inbound integrity: `TestDeclaredAgentModelAdmissionUsesConfiguredProfileWithoutRuntimeOrCredentials`, `TestDeclaredAgentModelAdmissionRequiresSourceAndAdmittedConfiguration`, `TestNewRuntimeValidatesInboundPublicationIntegrityBeforeWiringGateway`, `TestRuntimeDepsValidatedDerivesCanonicalBootGraph`. |
| A13 / P11 | Native-tool capability/permission/workspace admission stays non-executing: `TestNativeToolBootAdmissionDoesNotConstructProviderOrWorkspaceRuntime`, `TestNativeToolAdmissionRejectsInvalidContractBeforeCapabilityOrFallback`, `TestValidateNativeToolBootConfig_CLINativeWebSearchDoesNotRequireFallbackProviderPolicy`, `TestValidateNativeToolBootConfig_NonCLIRuntimeRequiresWebSearchFallbackCredential`. |
| A18/A25 / P15/P21 | Static managed-provider/workspace/binding admission is not a fabricated execution handshake: `TestValidateClaudeStartupConfig_RequiresWorkspaceAndGateway`, `TestValidateClaudeManagedAgentWorkspacesUsesNonExecutingCapabilityAdmission`, `TestValidateManagedProviderPreflightConsumesRunlessBlueprintBeforeLiveAdmission`, `TestManagedProviderPreflightRejectsExecutableForkBeforeProviderResolution`, `TestStartupCallRejectsManualOrMutatedTransportBinding`. No paid provider or adapter modification. |
| A23/A24 / P20/P21 | Recovery inspection and actor/readiness admission remain distinct from execution/draining: `TestSharedStartupRecoveryAdmissionPartitionsAndDecides`, `TestSharedStartupRecoveryAdmissionMissingReadsNeverMeanEmpty`, `TestSharedStartupRecoveryAdmissionErrorsAndCancellation`, `TestRuntimeStart_RecoveryInspectionAndManagerHydrationFailureIsBootFatal`, `TestManagedStartupStaticCensusRejectsBeforeExecutionAuthorityOrPersistence`. |

The complete `catalog-required-verify` unit re-executes the public verification
consumer family, normal census, and all three actual
`TestAdmissionPredicateRatchetRejects*` omission/refusal mutations. Normal
qualification leaves `SWARM_UPDATE_ADMISSION_PREDICATE_CENSUS` unset. The existing
update mechanism only regenerates characterization; it supplies no execution
credit and cannot waive hosted fail-fast cancellations or timing INCOMPLETE.
