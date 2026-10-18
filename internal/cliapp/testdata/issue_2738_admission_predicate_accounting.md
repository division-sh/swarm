# #2738 Prepared Surface Custody Accounting

This is owner/caller and execution evidence for the existing A11/A12/A18/A25
classification (P09/P10/P15/P21), not a new startup check or a new proof
namespace. Semantic/reservation gates: #2738 comments 6250538502, 6250534868,
6251218361 and 6251218086. Exact-head acceptance remains separate.

## Owner and Callers

`runtime_claude_startup.go::ManagedProviderPreflightAuthority.saveSurface`
remains the save boundary used by the three existing calls in
`validateManagedProviderPreflightConfigs`: the planned surface, the
provider-observed surface, and the gateway-list-settled surface. They keep
their exact authority, catalog, actor, phase, error propagation and surrounding
provider/gateway checks. No caller or receipt phase was removed.

Ordinary startup (`prepared == nil`) still calls the same capability store
directly. Non-executable selected preparation uses
`startupownership.PreparedManagedCapabilitySurfaceOperation` on its retained
native process, followed by that process's existing `RunPreparedOperation`.
The command validates the preparation/actor/catalog and the supplied original
custody before saving. It neither issues the child grant early nor substitutes
a current grant for a foreign, stale or refused original. It saves one cloned
surface synchronously; provider and gateway work remain outside the operation.

## Preserved Admission and Execution Proofs

| Existing Rows | Preserved Boundary | Existing Execution Witnesses |
| --- | --- | --- |
| A11 / P09 | Exact source, actor, model and prospective catalog are still required before probing or saving. | `TestDeclaredAgentModelAdmissionUsesConfiguredProfileWithoutRuntimeOrCredentials`, `TestPreparedProviderCatalogExactIdentity`, `TestPreparedProviderCatalogRejectsCensusAndCancellation`, `TestPreparedProviderPreflightRejectsChangedCatalogBeforeDispatch`, `TestPreparedProviderPreflightRejectsChangedGatewayCatalog`, `TestPreparedProviderPreflightRejectsCensusBeforeProviderResolution`. |
| A12 / P10 | Credential/profile resolution is unchanged; a prepared-surface save grants no credential and performs no provider resolution. | `TestProviderCredentialResolver_StoreWinsOverEnvForActiveProfiles`, `TestProviderCredentialResolverMissingPortIsUnavailableNotMissingSecret`, `TestProviderCredentialResolverReadFailurePreservesCauseWithoutSecretDisclosure`, `TestProviderCredentialResolver_EnvOnlyFailsClosed`, `TestProviderCredentialResolver_ActiveCredentiallessProfileNeedsNoSyntheticKey`. |
| A18 / P15 | Gateway/workspace/probe admission and exact provider/list evidence are unchanged. Ordinary startup is not routed through the preparation command. | `TestValidateClaudeStartupConfig_RequiresWorkspaceAndGateway`, `TestValidateClaudeStartupConfigForActiveAgents_RequiresFullCLIBindingForRecoveredManagerAgent`, `TestValidateManagedProviderPreflightFailsClosedWhenLiveClaudeRuntimeLacksStartupProbe`, `TestStartupCallRejectsManualOrMutatedTransportBinding`, `TestPreparedProviderPreflightFailureDoesNotReturnReceipts`. |
| A25 / P15,P21 | Runless preparation is not executable selected-fork authority; verify's catalog observation remains nonexecuting and does not persist a runtime store. | `TestValidateManagedProviderPreflightConsumesRunlessBlueprintBeforeLiveAdmission`, `TestManagedProviderPreflightRejectsExecutableForkBeforeProviderResolution`, `TestPreparedProviderPreflightUsesRunlessPlansWithoutManager`, `TestManagedProviderPreflightAuthorityCarriesLiveExecutionIdentity`, `TestVerifyDeploymentManagedCatalogConsumesLoadedSourceWithoutExecution`. |

The existing both-store
`TestSelectedConstructedAgentInputExecutesBothStores/prepared_surface_custody`
reach exercises genuine native prepared-surface saving, foreign/stale/refused
custody and the actual selected-child transition. Its reached-control counter
and the served business/diagnostic assertions remain required; passing this
static census cannot replace those proofs.

The existing predicate-census owner registers the new closed save method and
refreshes the body pin of its three-caller function. The owner, caller positions,
row/proof classification and all refusal controls above stay in force. No
startup check, proof family, timeout, baseline, tier or root is added or relaxed
by this accounting amendment.
