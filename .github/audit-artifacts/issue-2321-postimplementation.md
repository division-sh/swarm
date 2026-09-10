# Post-Implementation Proof Audit: #2321

Current status and final-head proof: [cycle 4](issue-2441-cycle4-postimplementation.md).
The checkpoint status below is historical; its original live/mock/retained
manifestation receipts remain separate evidence.

Cycle-3 checkpoint supersedes current-status claims below:
`issue-2321-pr2441-review3-proof.md` and
`issue-2442-query-cancellation-boundary.md`. Shutdown dependency ordering and
PostgreSQL exit repairs are implemented at e1b6ecd52, but full qualification has
exposed native query-cancellation provenance failures. Not review-ready; no
complete failure-class closure. Historical live/mock/retained receipts remain
separate evidence, not a waiver of the new failing proof.

Current review-cycle-2 status supersedes the historical gate/queue descriptions
below: `issue-2321-pr2441-review2-proof.md`. Coordinator repairs and focused proofs
are implemented; the real PostgreSQL cancellation/cleanup result is escalated,
and J5 remains unclassified. No final-head full-suite or merge closure claimed.

Rebased integration and current qualification receipts:
`issue-2321-pr2441-rebase-accounting.md`. Historical heads below are not rebased
full-suite proof. LSF-030 remains unresolved.

## PR #2441 Review Pass 1 Repair

**Current result: not review-ready; no failure-class elimination claimed.**
The full-profile attempt started at edb169908 has failed the API SQLite pin-page
test with `delivery continuation coordinator is retired`. LSF-030 and the
deterministic 3/3 runtime probe are recorded in #2353 and
`issue-2321-pr2441-review1-escalation.md`; the requested absorb/split ruling is
https://github.com/division-sh/swarm/issues/2321#issuecomment-5609541948.
Ten isolated both-store passes are classification controls, not a repair.

The actual required CI SQLite smoke passed at edb169908. CI also exposed the
existing pack-config guard's assumption that the now-empty fixture directory
exists in a fresh clone. The guard now starts at `.github` and inspects fixture
and workflow files; clean-directory proof passes (0.045s). This is a bounded
consumer-deletion repair, not a skipped guard or compatibility fixture. CI run
34410900554 remains failed, with its summary/timing failures derivative of the
pack guard. No production runtime patch was made for LSF-030. Final-head CI is
separate from the failed full-profile attempt; no green isolated result waives it.

The changes-needed review of cea51bf81 identified one missed CI consumer and two
stale diagnostic spec summaries. The prior elimination claim is superseded pending
repaired-head qualification and required CI. No runtime owner or product boundary
changes in this correction. M2 now includes hidden `.github/fixtures` producers and
the actual `sqlite-local-dev` public command in `.github/workflows/ci.yml`.
`TestCIExecutionSelectionConsumers` reproduced both the obsolete fixture and its
workflow consumer before deletion. The fixture supplied no legitimate deployment
facts, so it and the `--config` argument are removed, not replaced by mock proof.
The guard walks hidden CI fixture YAML and pins the real no-selector live command.

Focused config/structural controls pass (3.268s), API-spec tests pass (1.499s), and
the actual compiled `swarm run start examples/routing/root-ingress --event
item.received --payload <valid JSON file> --api-port 18091`, with the test DSN unset
and a disposable unchanged source copy, exits 0: completed, event_count=3,
entity_count=1. Receipt: `/tmp/agent-g-pr2441-sqlite-smoke.log`. No provider messages
or settled delivery replays were performed. The failed full-profile attempt and CI
accounting above supersede the original pending status; old successful receipts
remain historical.

The diagnostic contract correction and exact dual-store proofs are recorded in
the separate #2432 table. Watchlist decision: consume the reviewer's existing
`maintenance-and-cleanup.yaml#invariant_suite_coverage` refinement b1eff4d without
modifying concurrent docs work. No new issue, framework or pre-audit is needed.
#2353 LSF-028/029 remain observed/unclassified residuals, not fixed by later passes.

## Previous Qualified Receipt

Agent-g. **Historical qualification preceding review pass 1.** Code/test head:
`be9e98fe3fb2504b68dcefa487eaec66a222a519`, based on merged B #2440 at
`47c0e70d8ef89a2c99f0be4af632f24692121b83`. Final integrated full-profile
swarm-test passed at qualifying HEAD `1f5680f25bdb7fc988ac5e74a9c8bb27c1698336`
(only audit documents differ from the code/test head). The lead accepted explicit residual tracking for
LSF-028/029 under #2353 in issuecomment-5607051620; missing historical evidence
does not block opening the PR after current-head defects and qualification are
resolved, as recorded below. Provisioned
live proof has now executed successfully on both stores, with failed attempts
retained below. This is the consolidated current claim;
dated progress artifacts retain historical failures and receipts, not competing
current contracts. This audit is posted as a PR comment alongside separate #2432
and #2439 proof tables. Final head accounting: after qualification, audit documents
changed and CI identified two gofmt alignment omissions in internal/serveapp/main.go.
Applying gofmt changes whitespace only; comparing that file to gofmt of the
qualified source is byte-identical. No executable tokens, test or spec changed.
The initial CI static failure remains recorded, not called a passing CI run.
After formatting, repo-wide gofmt reports no files; go build ./..., go vet ./...,
OpenRPC --check and the remote canonical-routing tracker check all pass locally.

## Boundary And Authority

Category: semantic migration and CLI/runtime/store parity. The original generated
source/store refusal was symptom-shaped and treated as an entry point, not scope.
Chosen class: `command_owned_execution_and_zero_project_edit_startup`.
Immediate parent: `execution_selection_admission_dispatch_authority_drift`.
Broader parent: `deployment_and_test_authority_leaks_into_portable_source`.
Concepts changed: command-owned live/mock execution; inert authored doubles in
live mode; exact executable descriptors; structural versus deployment admission;
private fresh test lifetime; retained recovery defaults; admitted-source readers.
Approved live-discovered additions cover CLI capability-channel/presence integrity,
provider backing lifetime and failure handoff, and inherited executable-channel
authority with publication before startup execution. No universal scheduler,
exactly-once provider invocation or generic lifecycle framework is claimed.

Binding gate cycle 2:
https://github.com/division-sh/swarm/issues/2321#issuecomment-5584592517.
Product ruling:
https://github.com/division-sh/swarm/issues/2321#issuecomment-5577792999.
The approved #2319 acceptance split is
https://github.com/division-sh/swarm/issues/2321#issuecomment-5590488648.
The checked-in pre-audit and capability, provider-restart and activation-lifetime
amendments contain the additional independent approvals and complete censuses.
B's serialized settlement ruling is consumed, not bypassed:
https://github.com/division-sh/swarm/issues/2432#issuecomment-5604559542.

Authoritative `platform-spec.yaml` is updated in this branch. Governing sections:
`engine.process_execution_posture`,
`engine.agent_session_management.llm_provider_selection_config_authority.mock_agent_runtime`,
`engine.boot_verification`, `workspace_model.workspace_backend_selection`,
`workspace_model.claude_provider_state`,
`configuration_source_authority.unified_swarm_config`,
`engine.runtime_store_backend_selection`,
`cli_specification.foundations.local_runtime_state_authority`,
`test_specification.deterministic_scenario_runner`,
`cli_specification.command_catalog.test`, and
`tool_model.hitl_channel_pack_interface.connected_channel_onboarding.activation_rule`.
Selected-store causal execution mode, immutable source/run authority, effect and
fork policy are retained. Issue prose does not substitute for the changed spec.

## Owners And Systematic Consumption

| Canonical owner | Exhaustive known consumer families and after-change disposition |
| --- | --- |
| Shared `buildRuntimeComposition`, command purpose and existing lifetime owners | Public serve/dev/LocalRun select Live; public RunTestSession selects MockOnly. Private test owns fresh SQLite, root, auth and loopback transport, then joins runtime/store/projection cleanup. H is a compiled-test-only retained entrance using the same constructor; no production mock-serve switch exists. |
| `selection.ResolveAgentExecutionSelection` and `llm.ResolveAgentExecution` | Fresh static/template construction, manager spawn/adoption/reconfigure, selected-fork blueprint and native boot use command selection. Source doubles remain authored data, not a live selector. Missing performances are enumerated across scoped declarations before mock acquisition. |
| `ValidateAgentExecutionDescriptor` | AgentRuntimeSet, executable persisted adoption, hydration prevalidation, reconfigure readback, standalone selected container and fork-chat validate persisted decisions without reselection or repair. Teardown-only adoption stays non-executable. |
| Structural source admission and purpose-aware checks | verify, describe, graph, routes, test pre-admission and pack validation consume the admitted effective source. Structural validity, mock completeness and live readiness are distinct. Doctor/live boot retain credentials, capabilities, native/exec and workspace checks. |
| Unified config/source authority | Every layer rejects retired execution selectors before merge; shipped artifacts contain no hidden deployment selector. Explicit recovery false, backend/store possession and dev epochs retain their existing owners. No old-store compatibility or migration is added. |
| Existing public scenario compiler/runner/API | Public fresh test executes through the same private public-API client and mutation/readback owners. Explicit remote/ambient target options refuse before acquisition. Existing derived/authored/setup/profile semantics remain. |
| CLI stream accumulator and exact capability reader | Startup, fresh/resumed/tool-result managed calls, observation validation and fork display consume presence-aware native/MCP inventories. Missing/invalid is not empty; MCP display names never become native grants. Fork local-policy restrictions remain distinct. |
| Typed Claude state request and Docker workspace manager | Exact session/actor/source or delivery authority selects private backing. Reusable sessions and deliveries persist their own volumes; probes, fork-chat and other stateless invocations use isolated tmpfs. Resume refuses missing/corrupt backing; no fresh-session fallback. Invocation release delegates to existing identity-checked projection disposal and immutable Docker IDs. |
| Existing CLI attempt/effect settlement and bounded diagnostics | Started failures return nonretryable outcome_uncertain, preserving the structured explanation; only proven prelaunch failure may retry. No replay of sent/settled deliveries or new attempt ledger. |
| `channelactivation.Owner`/Lease and existing TurnContextRegistry | Model presentation, nested executor discovery, managed/startup/fork registration, HTTP tools/list/tools/call/direct routes and private activity retain exact inherited authority. Expiry/unregister/reset/parent completion revoke and join children. Ordinary new turns use current admission; no process lease is persisted. |
| Existing PreparedStartup, ContextManager and channel publication | Single/multi-context preparation, ordinary startup, forced recovery and reset candidate startup publish the complete executable set before releasing Manager/topology/finalization and autonomous recovery producers. Failure cannot publish readiness; rollback/unwind retains the existing owners. |
| Existing EventBus, pipeline, completion, timers, standing, decision and fork owners | Causal mode and accepted-work fences remain authoritative across replay/resume/decision/fork. Mock execution cannot authorize real effects. These are downstream consumers, not new command selectors. |
| Lifecycle diagnostic and SQLite termination owners | Separate #2432/#2439 commits, gates and proof tables in this same PR. B eventpersistence settlement is the only diagnostic writer; SQLite exact-connection cleanup is not a diagnostic retry. |

Invalid/deleted paths: config/posture inference, mock-presence live waivers,
connected public test, descriptor repair on read, source deployment rewrites,
native fallback from VisibleTools, missing-inventory-as-empty, disposable-only
provider transcripts, retryable started CLI failures, nested root lease
reacquisition and early recovery execution before channel publication. Legitimate
survivors are persisted causal mode, ordinary live deployment checks, existing
scenario/API owners, explicit fork sandbox policy and test-only H. Shared ownership
is not by itself closure proof; the named executions below are required.

## Original Manifestation Denominator

T = public fresh mock test; L = public live serve/dev; H = internal retained
compiled lifecycle. H is never credited as public mock restart or genuine live
provider execution. Classifications below use the qualified executable head,
with the separately dated live/network-disabled Docker receipts stated explicitly.

| Row | Classification | Exact proof |
| --- | --- | --- |
| M1 untouched archetypes | reproduced and fixed | TestScaffoldAdmittedArchetypesRunUneditedZeroCredentialSQLite (T); TestCommandLiveUneditedScaffoldReadiness (L). |
| M2 shipped configuration, hidden CI fixtures and workflow invocation | reproduced and fixed | TestScaffoldEmbedsNoHiddenDeploymentConfig; TestCatalogRequiredVerifyGateRejectsAddedDeploymentArtifact and catalog inventory; TestCIExecutionSelectionConsumers fails before both CI producer/consumer removals and passes after; actual compiled selector-free live SQLite smoke exits 0 with event_count=3/entity_count=1. Required repaired-head CI remains pending. |
| M3 retired selectors | reproduced and fixed | TestLoadRejectsRetiredExecutionPosture; TestCommandConfigRejectsMaskedExecutionSelectors. |
| M4 invocation/source roots | execution-proven through the same corrected path | TestGoldenInvocationRootDevReadiness; TestCommandLiveUneditedScaffoldReadiness; unchanged authored-tree comparisons. |
| M5 live source doubles | reproduced and fixed | TestCommandOwnedAgentSelection; TestCommandSelectionSeparatesSourceDoubleFromLiveDescriptor; TestCanonicalTelegramAgentLiveSelectionPreservesAuthoredDoubles; provisioned L journey. |
| M6 complete scoped census | reproduced and fixed | TestCredentialChecksCensusScopedAgentsHiddenByAmbiguousAlias; TestCredentialChecksCensusScopedActivitiesHiddenByAmbiguousAlias; TestAuthoredMockStaticAndInstantiatedAgentsSpawnPersistRecoverMock. |
| M7 credentials/non-agent requirements | execution-proven through the same corrected path | TestCredentialChecksUseTypedSelectionAndExactResponseAuthority; TestCredentialChecksRetainAgentFreeToolRequirement; TestCredentialChecksRetainNonToolRequirementsSharingAKey. |
| M8 workspace/native/CLI selection | reproduced and fixed | TestCommandWorkspaceSelectionIgnoresLiveSourceDoubles; TestCommandPurposeControlsClaudeStartupRequirements; TestValidateNativeToolBootConfig_CommandPurposeOwnsNativeAdmission. |
| M9 durable descriptor parity | execution-proven through the same corrected path | TestCommandDescriptorBothStoresPreserveSelectionAndRefuseCorruption, repeated reads and unchanged bytes. |
| M10 restart/reconfigure | execution-proven through the same corrected path | TestAuthoredMockSelectionSurvivesReconfigureAndRestart; TestAgentRuntimeSetRecoveryRejectsBackendDriftWithoutRewritingDescriptor; TestCommandRecoveryNeverSelectsAnIncompleteStoredDescriptor; H J1-J4 and genuine L restart. |
| M11 selected/conversation fork | execution-proven through the same corrected path | TestSelectedContractContainerValidatesStoredSelectionsWithoutRepair; TestSelectedContractContainerRejectsIncompleteStoredDescriptor; TestLLMForkChatExecutorRefusesConflictingStoredBackendWithoutReselection; supported fork controls. |
| M12 private test ownership | reproduced and fixed | TestPrivateTestDoesNotSelectConcurrentLiveRuntime; TestPrivateTestAdmissionPrecedesResourceAcquisition. |
| M13 scenario paths | execution-proven through the same corrected path | TestServedParityHarnessDerivedScenarioLifecycle; TestServedParityHarnessGeneratedInputFixtureLifecycle; TestInternalLifecycleScenarioConsumesAdmittedSourceAcrossSupportedBackendsAndModes; T credit remains separate. |
| M14 joined cleanup | execution-proven through the same corrected path | TestPrivateTestSessionJoinedCleanupAndPrivateTransport; TestPrivateTestStartupFailureUnwindsAcquiredResources; TestPrivateTestRootCleanupRemovesImmutableProjectionsWithoutFollowingLinks; terminal store/listener/late-readiness failure controls. |
| M15 no external effects in mock | execution-proven through the same corrected path | TestMockEffectFenceRejectsEveryExternalAdapterBeforeAuthorization; TestMockOnlyPostureRejectsLiveActivityBeforeJournalCredentialsAndHTTP; private auth/ingress and ambient trap counters. |
| M16 recovery policy | execution-proven through the same corrected path | TestCommandConfigRecoveryDefaultsAndExplicitFalse; H J1-J4; L omitted-recovery pending delivery convergence. |
| M17 dev epoch | execution-proven through the same corrected path | TestGoldenAgentWorkloadSQLiteDevScratchRestartStartsFreshEpoch (H); L two fresh dev epochs and unchanged retained-source snapshots. |
| M18 config precedence | execution-proven through the same corrected path | TestCommandConfigRejectsMaskedExecutionSelectors and existing config path/provenance tests. |
| M19 restart/webhook process | execution-proven through the same corrected path | TestCompiledProcessFullLifecycleJourneysSQLitePostgres (H J1-J5); TestCommandLiveServeAndRestartParity (L signed ingress, exact ordered receipt/route, reply/card and settlement). |
| M20 public provenance | execution-proven through the same corrected path | Private callback authenticated health.check ready/mock_only, predecessor token refusal and exact golden health posture; T/L/H distinct. |
| M21 structural checks | reproduced and fixed | TestStructuralValidationNeverObservesDeployment; TestStructuralCheckPurposeCensus; TestStructuralValidationRejectsUnknownPurpose. |
| M22 credential-absent webhook activation | split / escalated as separate class | F-owned #2319, approved nonblocking acceptance split; not credited from provisioned L. |
| M23 other config precedence | split / escalated as separate class | #1801; no broader precedence closure claimed. |
| M24 native prerequisites | split / escalated as separate class | #2284; genuine native/Claude/Docker refusal retained. |
| M25 durable mode readers | execution-proven through the same corrected path | TestGenericScheduleExecutionModeSurvivesReplayAndStoreReconstructionOnBothStores; TestWorkflowTimerLifecyclePreservesMockExecutionModeOnBothStores; standing/pipeline/effect/decision/directive/fork mode matrices. |
| M26 describe and sibling readers | reproduced and fixed | TestStructuralReadersShareAdmittedSource; TestStructuralReadersRetainPostBootInvalidity; TestStructuralReadersDoNotConstructChannelActivation; compiled untouched-source proof. |
| M27 truthful readiness output | reproduced and fixed | Structural check census/output tests: structural/not_evaluated, retained production_valid meaning, no operational capability claim. |
| M28 selection failures | execution-proven through the same corrected path | TestCommandOwnedSelectionRefusesMissingPurposeAndRetiredSelectors; provider/pin/model table and descriptor corruption controls. |
| M29 deployment noninterference | execution-proven through the same corrected path | TestPrivateTestIgnoresDeploymentResources; TestPrivateTestRejectsEachExplicitDeploymentTargetBeforeAcquisition; concurrent live sentinels and counted ambient trap. |

## Absorbed Manifestations

These rows retain the additive gates' denominator, rather than treating a shared
helper as proof. Historical execution details are preserved in the capability,
provider and activation artifacts. #2432 F1-F11 and #2439 T1-T14 remain separate
complete tables, not hidden inside these rows.

| Row / manifestation | Classification | Exact proof |
| --- | --- | --- |
| C1 MCP display name misclassified as native | reproduced and fixed | TestCLIInventoryChannelIntegrity/mcp_only. |
| C2 native-only/mixed/canonical collision/explicit empty | execution-proven through the same corrected path | TestCLIInventoryChannelIntegrity; TestCLIInventoryForkDisplayPreservesEmptyAndLocalPolicy. |
| C3 missing/null/wrong type/malformed/nameless inventory | reproduced and fixed | TestCLIInventoryPresence; managed/fork reader refusal and serialized presence. |
| C4 later messages fabricate or erase inventory | execution-proven through the same corrected path | TestCLIInventoryCannotBeManufacturedOrRepairedByMessages. |
| C5 missing/unexpected native, unplanned/disconnected MCP | execution-proven through the same corrected path | TestCLIInventoryChannelIntegrity; TestManagedCapabilityPlanRejectsMissingAndUnexpectedProviderBuiltins. |
| C6 startup zero-native observation | execution-proven through the same corrected path | TestClaudeCLIStartupInventoryPresence, exact process count per success/refusal. |
| C7 fresh/resumed/tool-result inventory failure | execution-proven through the same corrected path | TestClaudeCLIManagedInventoryFailureSettlesWithoutRetry; one uncertain settlement, no extra invocation/local effect. |
| C8 valid resumed frame/drained completion | execution-proven through the same corrected path | TestClaudeCLIManagedRequestEncodesCanonicalExecutionFrame; TestClaudeMemoryDrainedCompletionStopsBeforeProviderHeadProjection. |
| C9 fork/API/mock policy controls | execution-proven through the same corrected path | TestCLIInventoryForkDisplayPreservesEmptyAndLocalPolicy; TestClaudeInvocationToolProjectionCoversEveryNativeFamily; TestClaudeInvocationToolProjectionRequiresOneExactOwner; TestClaudeInvocationToolProjectionDoesNotReconstructForkBuiltinsFromSourceActor; TestClaudeInvocationToolProjectionRejectsUnknownForkBuiltinEvidence; TestValidateCLIResponseToolCallsForTurn_FailsClosedForNonEmitToolOutsideObservedSurface; TestValidateCLIResponseToolCallsForTurn_ManagedTurnRejectsMissingCapabilitySurface; TestValidateCLIResponseToolCallsForTurn_AllowsObservedMCPToolAndEmitFallback; TestValidateCLIResponseToolCallsForTurn_ForkSandboxAllowsPlannedToolWhenObservedMetadataIsAbsent; TestManagedCapabilityPlanPreservesConcreteAPIFallbackDefinition; TestObserveMockRuntimeCapabilitySurfaceBindsExactInterpreterInput. |
| C10 genuine accepted turn and retained restart | execution-proven through the same corrected path | TestCommandLiveServeAndRestartParity, real Claude/MCP/Telegram and complete delivery settlement on both stores. |
| R1 full source/actor/run/session/kind isolation | execution-proven through the same corrected path | TestClaudeStateNamespace; full-hash suffix, run, session and state-kind changes yield distinct keys. |
| R2 stateless reclaimed delivery identity | execution-proven through the same corrected path | TestClaudeStateNamespace and TestClaudeStateDockerRetentionAndRefusal; same delivery/new claim reattaches, another delivery differs, foreign run refuses. |
| R3 retained transcript after projection release/stop/forced removal | reproduced and fixed | TestClaudeStateDockerRetentionAndRefusal with real Docker, network none and synthetic JSONL; retained head recognized before/after teardown without reconstruction. Genuine restart separately uses L. |
| R4 missing/corrupt backing | execution-proven through the same corrected path | Same real Docker test; missing confirmed volume is not recreated, malformed transcript refuses. |
| R5 probe/fork/non-delivery temporary state | execution-proven through the same corrected path | Same real Docker test; within-invocation reuse, tmpfs destruction, probe/ordinary cross-read refusal, no provider volume for temporary invocations. |
| R6 shared logical workspace/private provider memory | execution-proven through the same corrected path | TestClaudeStateDockerSharedWorkspaceIsolation, equal session UUID across actors, shared authored work and private files isolated. |
| R7 candidate backing failure/head promotion | execution-proven through the same corrected path | TestClaudeMissingCandidateBackingNeverPromotesHead on both stores: one dispatch, terminal uncertainty, no retries or promoted head. |
| R8 cleanup result and immutable target | execution-proven through the same corrected path | TestClaudeInvocationReleasePreservesSessionReadbackAndError; TestClaudeCLIManagedLifecycleFromReleaseBinaryDefaults; TestReleaseEvidenceRejectsDuplicateClosureAttempts; TestReleaseDockerImmutableTargetCannotSelectSameNameSuccessor. |
| R9 streaming/nonstreaming started errors, timeout and joined settlement failure | reproduced and fixed | TestClaudeCLIUncertainFailureDisposition; TestClaudeRetryableSettlementFailureRemainsTerminal on both stores; original provider cause retained. |
| R10 prelaunch versus postlaunch/commit/settlement | execution-proven through the same corrected path | TestClaudeAttemptIdentitySelectedStoreMemoryAndProcessParity; existing prelaunch retry controls and exact attempt counts; no postlaunch retry. |
| R11 structured reason after large prefix | reproduced and fixed | TestClaudeStructuredErrorSummary JSON/JSONL; terminal delivery error readback retains explanation. |
| R12 secret/malformed/unknown/UTF-8 boundary | execution-proven through the same corrected path | TestClaudeCommandDiagnosticRedaction; TestClaudeStructuredErrorSummary; redaction before truncation. |
| R13 graceful/forced retained restart, dev and history | execution-proven through the same corrected path | TestCommandLiveServeAndRestartParity: both stores, six fresh ingresses each, two SQLite dev epochs, unchanged retained history and old settled delivery snapshots. |
| A1 nested predecessor catalog versus successor drain | reproduced and fixed | TestPresentationDescendantsJoinPredecessorUnderFence; TestChannelPresentationExecutorAndRegisteredForkCatalogUnderReplacement. |
| A2 managed HTTP list/call/direct/private activity | reproduced and fixed | TestConfiguredChannelRuntimeDispatchesImportedAgentDurablyAcrossSelectedStores/registered_managed_*; actual claim, registration, HTTP, executor and connector under replacement to an unreachable successor, both stores and race repeats. |
| A3 source/actor/run/input/token/owner mismatch | execution-proven through the same corrected path | Same managed cases mutate source, actor, run, inbound presence and expiry with zero extra connector calls; owner scope matrix covers flow/entity/foreign owner/revoked parent. |
| A4 conversation-fork catalog and sandbox | execution-proven through the same corrected path | TestChannelPresentationExecutorAndRegisteredForkCatalogUnderReplacement; inherited catalog survives the fence without exposing forbidden channel tools. |
| A5 unrelated admission/canceled replacement | execution-proven through the same corrected path | TestPresentationUnrelatedAdmissionAndCancelledReplacement. |
| A6 expiry/unregister/reset/parent cancellation/completion joins | execution-proven through the same corrected path | TestPresentationExpiryRefusesAlreadyResolvedBindingWithoutTimer; TestTurnPresentationRevocationJoinsRequests; race repeats. |
| A7 due schedules/timers and aborted/failing preparation | reproduced and fixed | TestRuntimeStartWithholdsDueSchedulesAndTimersUntilDynamicTopologyCompletesOnBothStores; preparation abort, standing-finalization error, double-release and post-shutdown refusal controls. |
| A8 standing ingress/source topology | execution-proven through the same corrected path | TestStandingIngressSupportedSurfaceSQLiteRestartPreservesAuthorityAndReplies and PostgreSQL sibling; local provider protocol is distinct from paid L. |
| A9 selected-fork managed startup/turn | execution-proven through the same corrected path | TestExecuteSelectedContractRunForkClaudeOAuthPersistsStartupAndTurnCapabilityAuthority; TestSelectedContractForkManagedPreflightUsesExactProviderPromptAndExecutesEligibleMCPToolCall. |
| A10 no-channel/configured/multi-context startup failure | execution-proven through the same corrected path | TestConnectedChannelRecoveryRunsWithoutPublicIngressOwner; TestConnectedChannelLocalRecoveryFailureBlocksChannelPublication; TestChannelOnboardingE2E12MultipleExactContexts; TestDynamicTopologyStartupPreflightPostgresScopesTwoContextsAndRefusesAtomically; TestValidateServeMultiContextToolGatewayAdmission; TestServeRuntimeConfiguredChannelBindingProjectsOnce; TestPrivateTestStartupFailureUnwindsAcquiredResources; TestInboundAdmissionSupportedSurfaceStartupFailuresSQLiteAndPostgres; TestRuntimeStart_DisablePersistentStartupRecoverySkipsUnscopedStoreReads. |
| A11 merged reset complete-set publication and source loading | execution-proven through the same corrected path | TestResetCandidateSetFencesConsumersWhileSecondPreparesBothStores; TestPreparedRuntimeStartupKeepsFirstCandidateUnadmittedWhileSecondBlocksOrFails; TestPreparedRuntimeStartupRejectsCancellationBeforeExecutionRelease; TestServeActivatesSelectedStoreBeforeProcessOwnedConstruction checks ordinary and reset loaders. |
| A12 live forced/dev and unchanged scaffolds | execution-proven through the same corrected path | TestCommandLiveServeAndRestartParity; TestCommandLiveUneditedScaffoldReadiness, no authored source changes and no business-message credit for readiness alone. |

## Closure And Qualification

Commitment: eliminate the entire approved chosen class and its absorbed additions.
Achieved implementation closure claim: **failure class eliminated** within the
approved boundary, supported by the complete manifestation tables and qualified
full-profile run. This is not independent merge approval or closure of the parents.
Parents remain open. No same-concept selector/reader bypass is intentionally left
as a follow-up; #2319/#1801/#2284 and #2250 are distinct boundaries. Parent tail is
several owner-specific workstreams with low numeric-estimate confidence, not a
new staged plan for this class. Integration added exact cleanup delegation and
corrected tests, not another runtime ownership layer.

Watchlist decision: consume the existing approved #2321, #2432 and #2439
refinements, including 038244f, e8bdb9f, 11ec18c, 823590f and B's merged reset
refinement. No new node, issue, POTENTIAL_ISSUES entry or docs-tree edit is needed
for this integration. Broader orchestration/phase-order and PostgreSQL unwind
remain #2250. The long-run direction is explicit existing owner boundaries and
fewer interpreters; a generic framework has low ROI. Remaining broader work is
multiple bounded passes, not credibly a single numeric estimate.

Current evidence: dual-store diagnostic race count=3 PASS (656.324s store,
116.361s actual catalog/fork); offline real Docker retention/isolation PASS
51.423s; repaired release fixture/immutable-ID negative controls and dual-store
static-data invocation shards PASS 83.847s; API PASS 1.589s; exact persistence
inventory PASS 2.530s. These are supplemental checkpoints, not a full-suite claim.
The full-profile run at 5426deb7b FAILED in exactly one package: custom native
web-search rate-limit testing measured the gap between server arrivals rather
than the rate owner's earlier admissions (228.741us observed versus 25ms required).
Production `doNormalizedSearch` admits before effect preparation and transport,
so a delayed first transport can consume the rate window before arrival. The
test-only ae8bbb782 correction checks elapsed admission-window time, both recorded
admissions and both outbound requests, with an explicit delayed-first-transport
case. Twenty repetitions pass; native/provider and gateway refusal race controls
pass count=3. No runtime rate, wait policy or production semantics changed.
The failed whole-run receipt remains /tmp/agent-g-rebase-integrated-full-v2.log,
SHA256 2687831d127e20ac981ec67a25f9358915f9b3c6b5eadd188ebad7e548ec3497.
That run passed releasee2e (841.922s), catalog/fork (494.842s), runtimepersistence
(558.061s), serveapp (501.481s) and cliapp (216.734s); these do not turn the whole
command green. The stale ae8bbb782 queue request was cancelled before execution
(exit 143) after the lead reproduced T7 and requested failure-capture repairs.
Only G's exact queue process was signalled. Final qualification at the same
be9e98fe3 executable/test tree, qualifying HEAD 1f5680f25:

`SWARM_TEST_PROOF_PROFILE=full go run ./cmd/swarm-test -- -count=1 -timeout=30m ./...`

**PASS, exit 0**, no skips/retries added to clear a failure. It waited 13m39s for
the shared slot, then completed the full workload. Release-process suite 840.195s,
actual catalog/fork 503.274s, runtimepersistence 552.917s, serveapp 502.424s,
CLI 234.210s, tools 47.785s, SQLite backend 15.800s, store/inventory 38.120s,
API spec 1.619s. Log /tmp/agent-g-final-bounded-repairs-full.log, SHA256
1e449b463692002e4457cfb68cdee552a3a29edbad738d679fe6ec8234307db0.
LSF-028/029 did not recur in this qualification; that is not a causal fix claim.

Bounded final corrections authorized in
https://github.com/division-sh/swarm/issues/2321#issuecomment-5607051620:

| Row | Classification | Exact proof |
| --- | --- | --- |
| E1 health/RPC failure evidence | reproduced and fixed | TestReleaseRPCFailureEvidence: absent/null result, wrong/missing response ID, error envelope, secret redaction, non-JSON/malformed body, bounded output. HTTP status, endpoint without credentials/query, expected process PID, request/response IDs and body length/hash survive. Arbitrary result/error-data payloads are omitted; supplied secrets are redacted before truncation. One request only, no retry. |
| E2 golden collector failure | reproduced and fixed | TestGoldenFailureEvidenceSurvivesCollectorFailure: entity error/timeout, partial event/entity page failure, total-budget cancellation. Exact earlier public facts flush before the failing entity collector; later collectors run or record their own cancellation/error. Normal success assertions remain strict; one-second part budgets and five-second total capture budget do not extend the workload completion deadline. |
| #2439 T7 automatic rollback | split / escalated as separate class | Separate SQLite commit 74461cda7 and #2439 proof table; read cancellation/deadline joins existing errors rather than returning only ErrTxDone. |

Offline E1/E2 plus effect-approval mode controls pass race count=3 (4.128s),
log /tmp/agent-g-failure-evidence-final.log, SHA256
676da0bffa343b7cdd0fba598d5c97616ab9324f291a41fdcf33d00b64207574.
No additional live message or speculative PostgreSQL runtime patch was made.

L proof at runtime head 5426deb7b is complete by named
surface: SQLite and both untouched scaffold readiness cases passed within the
first host-network command (that command exited 1 solely for PostgreSQL's
pre-ingress health failure). The PostgreSQL-only command then exited 0, 124.420s.
Logs: /tmp/agent-g-rebase-live-host-final.log and
/tmp/agent-g-rebase-live-host-postgres.log. Both journeys executed first turn,
graceful restart, forced restart with pending work, later ingress with old-delivery
snapshot unchanged, two fresh SQLite dev epochs and unchanged retained history.
Exactly twelve successful fresh Telegram replies total; no additional live
business tests are scheduled. T, L and H credit remain separate. These are genuine
historical live receipts at 5426deb7b, not another live run at be9e98fe3. Later
changes are the rate-limit test, #2439 cancellation/error preservation, and the
failure-only harness capture/response-identity checks. Lead explicitly requested
offline proofs and integrated qualification, not another live journey/replay.

Live receipt SHA256s: host combined
449be10ff06dab66b84e8ad2f0ffc31b6ead44654c26677201c2f399bbdb2b17;
PostgreSQL-only e38660a78b9480747de7f1fa62b57f58ca46800e716eec4cf608580007dd5ad9.
Diagnostic race receipt SHA256:
b1d6bcf4e7651e86c946a6043c3b28d922e4425f5852ce2600778872dd3a764f.

Live environment check on 2026-09-09: the first rebased L run timed out before
the first reply card on SQLite and PostgreSQL. A fresh SQLite diagnostic repeat
captured the runtime waiting on Claude stdout/stderr, without the suspected MCP
lock cycle. Both mas_default and Docker bridge provider HTTPS connections timed
out; the host and a host-network container returned HTTP 404 from the provider.
The same token/image in a standalone, tools-disabled host-network Claude control
then completed one turn in 6.365s (reported cost USD 0.021999). This isolates an
environment egress failure, not a new runtime design. The subsequent successful
L runs explicitly provisioned the already-supported workspace network=host. No default, runtime
timeout, retry, target validation or production networking behavior was changed.
The temporary failure-only SIGQUIT probe was removed after capturing evidence;
its three failed attempts' exact projection containers were removed by immutable
IDs. Provider volumes and unrelated containers were not deleted. No failed or
settled delivery was replayed. These failed runs are not live closure credit.

The host-network run completed the SQLite L journey and both untouched scaffold
readiness cases. Its PostgreSQL journey failed before ingress with
`health.check returned no result`, while the captured child output was empty.
The response body was not retained, so a port-allocation collision is only a
hypothesis, not an established runtime defect or fixed cause. A fresh PostgreSQL-
only run passed (124.420s); the failed pre-ingress attempt stays in the record.
Neither a provider reply nor a PostgreSQL restart pass is credited from that
failed attempt. No SQLite message was repeated to obtain PostgreSQL evidence.
This is LSF-029, observed/unclassified in #2353, under the lead's explicit
nonblocking historical-evidence disposition. Any recurrence still fails proof
and requires investigation, not retry-to-green.

The earlier PostgreSQL N=10 burst timeout remains unclassified: all ten analyzed,
nine completed, all 43 deliveries delivered, zero unsettled/fanout owed, one due
timer. Exact payload evidence was lost with the disposable store. Failure-only
event/log retention is now present; neither subsequent green repetitions nor the
SQLite cleanup prove its cause. This is LSF-028, observed/unclassified in #2353;
lead disposition issuecomment-5607051620 permits opening after bounded repairs
and required current-head proof without endless historical reruns. This is not
a fix or merge approval. No deadline/workload relaxation or production golden-path
workaround is included. Watchlist refinement 07a5d05 records these distinctions;
no further node, issue or architecture framework is required.
