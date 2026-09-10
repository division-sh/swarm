# Issue #2321 Verification Accounting

Historical and cumulative accounting. The latest activation/startup implementation
and outstanding checks are recorded in `issue-2321-activation-progress.md`.
Earlier blocked live rows below remain observations of their named heads, not
claims about the repaired final head.

Agent: agent-g. Development checklist, not the final Post-Implementation Proof
Audit. The cycle-2 gate and its full 29-manifestation denominator remain binding.
No row below grants final-head closure. T = actual public fresh mock test;
L = actual public live serve/dev; H = internal retained mock lifecycle execution.
An H result never substitutes for L, or for a public test restart feature.

Latest provisioned L update, 2026-09-09: runtime head `97a9ea04a` passes the
complete `TestCommandLiveServeAndRestartParity` (209.675s), with real Claude,
Docker and Telegram on both retained stores. Six fresh ingresses per backend
cover first/graceful/forced/subsequent turns and two dev epochs; every reply/card
and delivery settles. Dev uses SQLite in both journeys. Retained history is
reopened and unchanged after dev. No previously settled failed delivery was
replayed. This supersedes earlier failed live observations below, not their
historical evidence. It is lifecycle-fixture proof, not untouched-scaffold proof.
#2432 approval/repair remains an acceptance obligation. The full-profile
`SWARM_TEST_PROOF_PROFILE=full go run ./cmd/swarm-test -- -timeout=30m ./...`
passes at code/test head `eace6c528` (exit 0). Opt-in L journey/scaffold executions
are separately recorded, not credited from their full-suite skips. Unchanged
packages may be cached, as recorded in the run log. Re-run integrated proof and
bind the final audit after #2432 lands; green execution does not close that known,
independently reproduced diagnostic defect.

## Owner And Consumption Recheck

Additional approved owner/consumer census: cliStreamAccumulator/Response owns
exact native/MCP observations plus inventory presence. exactCLIProviderVisibleTools
is the checked reader used by observeCLIResponse, ValidateCLIProviderCapabilitySurface
and validateClaudeInvocationProviderBuiltins. Startup consumes the same managed
reader; fresh/resumed/tool-result calls each construct a new accumulator. Fork
display consumes presence but keeps policy-owned local tools, not native grants.
API/mock request owners and the existing completion/effect owner are unchanged.
The VisibleTools native fallback and permissive duplicate MCP entry parser are
removed, and native synthetic test producers now consume the real parser.

## Absorbed Capability Proof Rows

Approved class: exact CLI capability observation channel and presence integrity
(issuecomment-5592428086). Focused tests below passed three repetitions, 0.438s;
this is not live or final integrated-head closure credit.

| Manifestation | Exact proof / status |
| --- | --- |
| MCP-only false native mismatch | TestCLIInventoryChannelIntegrity/mcp_only; reproduced and fixed. |
| Native-only/mixed/canonical collision/empty/controls | TestCLIInventoryChannelIntegrity and TestCLIInventoryForkDisplayPreservesEmptyAndLocalPolicy; execution-proven through the same corrected path. |
| Missing/null/wrong-type/malformed/name-less inventory | TestCLIInventoryPresence; reproduced and fixed; managed/fork reader refusals and response serialization. |
| Later data fabricates or erases inventory | TestCLIInventoryCannotBeManufacturedOrRepairedByMessages; execution-proven through the same corrected path. |
| Unknown/wrong/missing native, unplanned/disconnected MCP | TestCLIInventoryChannelIntegrity plus existing TestManagedCapabilityPlanRejectsMissingAndUnexpectedProviderBuiltins; execution-proven through the same corrected path. |
| Startup zero-native observation | TestClaudeCLIStartupInventoryPresence; execution-proven through the same corrected path, one process per success/refusal. |
| Fresh/resumed/tool-result failure settlement | TestClaudeCLIManagedInventoryFailureSettlesWithoutRetry, both real process paths with missing/null/malformed/unexpected-native; execution-proven through the same corrected path, exactly one uncertain settlement, no extra invocation/local effect. |
| Valid resumed native/MCP and drained settlement | TestClaudeCLIManagedRequestEncodesCanonicalExecutionFrame and TestClaudeMemoryDrainedCompletionStopsBeforeProviderHeadProjection; execution-proven through the same corrected path. |
| Fork local/canonical policy, API/mock controls | TestCLIInventoryForkDisplayPreservesEmptyAndLocalPolicy, TestClaudeInvocationToolProjection*, TestValidateCLIResponseToolCallsForTurn_*, TestManagedCapabilityPlanPreservesConcreteAPIFallbackDefinition, TestObserveMockRuntimeCapabilitySurfaceBindsExactInterpreterInput; separate policy preserved and execution-proven. |
| Live accepted turn/delivery SQLite/PostgreSQL | TestCommandLiveServeAndRestartParity reaches first-turn causal reply and original delivery convergence on both stores; execution-proven through the corrected path. |
| Live retained restart SQLite/PostgreSQL | Same L test fails second ingress after graceful restart (246.257s total). Separate provider-process failure is escalated for classification in issue-2321-live-restart-observation.md; not closure credit. |

Watchlist: lead's 038244f refinement covers the absorbed class. No extra node,
new issue or framework. #2432 remains split/tracked and frozen; #2319 nonblocking.
The intended bounded class closure has no planned child tail, but live evidence
and #2321's integrated full suite remain required before final review readiness.

Broader executed evidence: full LLM package via swarm-test PASS (2.105s), focused
inventory/startup/managed process race PASS (13.431s), API-spec PASS (1.751s), full
retained H J1-J5 SQLite/PostgreSQL PASS (55.288s), L receipt/reference/setup helper
controls PASS three repetitions (0.009s), diff check PASS. H remains H, not L.
The runtime repair stays limited to the approved observation class; no provider
state persistence/recovery or #2432 implementation is included.

The bounded repair is pushed as 2c958d04f. Fresh PG diagnostic repeat failed
(120.763s): provider reports error_during_execution with zero turns/API time/cost
after retained restart, before capability validation. Its original explanation
is not available in the truncated persisted stdout. All proof jobs are complete;
no live restart or full-suite success is credited. Classification is requested on
#2321 before any production scope expansion.

## Original Owner And Consumption Recheck

- Command composition: `serveapp.Run` passes Live and `RunTestSession` passes
  MockOnly to `buildRuntimeComposition`; `cmd/swarm` injects the CLI session port.
  `LocalRun` retains the Live serve path. H's parent-owned retained entry exists
  only in compiled test code. No production mock-serving selector was added.
- Fresh descriptor selection: `llm.ResolveAgentExecution` delegates to
  `selection.ResolveAgentExecutionSelection`. Consumers include manager fresh
  spawn, static/template blueprint creation, reconfiguration, selected-fork
  blueprint creation and native boot admission. Source-level selection consumers
  are boot checks/reachability, workspace selection and Claude preflight.
- Persisted descriptor consumption: `ValidateAgentExecutionDescriptor` is used by
  AgentRuntimeSet, executable adoption, static hydration prevalidation,
  reconfigure readback, standalone selected-contract container and served
  fork-chat. Lifecycle-only teardown does not construct an executable adapter.
  `MaterializeAdmittedAgentForExecution` has no production caller; it is a fresh
  admitted execution test entrance, not the production persisted adoption reader.
- Source/report: CLI structural admission projects pack metadata and effective
  source before verify, describe, graph, routes and test pre-admission. Existing
  boot-check producers split mixed checks by purpose. Operational doctor/serve
  retain real credential, native, MCP and workspace observation.
- Config: unified loader rejects retired selectors in every source layer before
  merge can mask them; config validation also rejects them. Presence-aware merge
  preserves explicit recovery false. Artifact inventory forbids shipped deployment
  selectors rather than rewriting generated sources after admission.
- Lifetime: shared composition delegates to existing selected-store lifecycle,
  process capability/generation, supervisor, workspace/source projection, API,
  gateway, Runtime and ContextManager owners. Public test owns its SQLite/root,
  memory auth, endpoint and empty private credentials; it does not acquire ambient
  registration, exposure or provider ingress. Scenario execution consumes the
  same public API client at the private endpoint.
- Durable causal authority remains with EventBus, pipeline/activity/completion,
  timer/schedule, standing/readiness, decision cards, replay/resume and fork
  owners. Their mode fences are retained; retirement of config posture does not
  retire persisted causal mode or permit promotion from mock to live.

Old config-posture reads, source-presence startup waivers, connected public test,
read-side backend migration and shipped live/mock configs are invalid. Test-only
scenario protocol injection and H are explicit proof surfaces, not compatibility
paths. `#1801`, `#2284` and F's `#2319` stay distinct.

## Manifestation Evidence Checklist

Named tests are implemented unless explicitly marked pending. Their execution
must be rebound to the final integrated head before the final audit is posted.

| ID | Named evidence / remaining obligation |
|---|---|
| M1 | `TestScaffoldAdmittedArchetypesRunUneditedZeroCredentialSQLite` proves T/structural zero-edit startup. `TestCommandLiveUneditedScaffoldReadiness` PASS (23.930s): both fresh archetypes reach public retained/dev L readiness with no generated-source edits; real operator-global Claude/image/network provisioning, no command backend/config/source/store selector. No message/turn credit claimed by this readiness test. |
| M2 | `TestScaffoldEmbedsNoHiddenDeploymentConfig`, `TestCatalogRequiredVerifyGateRejectsAddedDeploymentArtifact`, canonical catalog full inventory. |
| M3 | `TestLoadRejectsRetiredExecutionPosture`, `TestCommandConfigRejectsMaskedExecutionSelectors`, missing-purpose selector negatives. |
| M4 | Compiled scaffold T/verify proof and `TestGoldenInvocationRootDevReadiness` L; `TestCommandLiveUneditedScaffoldReadiness` PASS (23.930s) runs bare verify/describe/test and public retained/dev serve for both roots, asserting authored bytes unchanged. Backend credentials are provisioned by public secrets set, with Claude chosen in operator-global config. Built-in Anthropic default is unchanged; no project config or source operand. |
| M5 | `TestCommandOwnedAgentSelection`, `TestCommandSelectionSeparatesSourceDoubleFromLiveDescriptor`, `TestCanonicalTelegramAgentLiveSelectionPreservesAuthoredDoubles`; `TestCommandLiveServeAndRestartParity` PASS with authored doubles retained and genuine live turns on both stores at 97a9ea04a. |
| M6 | `TestCredentialChecksCensusScopedAgentsHiddenByAmbiguousAlias`, `TestCredentialChecksCensusScopedActivitiesHiddenByAmbiguousAlias`, `TestAuthoredMockStaticAndInstantiatedAgentsSpawnPersistRecoverMock`; retain exact scoped identities in H. |
| M7 | `TestCredentialChecksUseTypedSelectionAndExactResponseAuthority`, `TestCredentialChecksRetainAgentFreeToolRequirement`, `TestCredentialChecksRetainNonToolRequirementsSharingAKey`, live/mock startup credential negatives. |
| M8 | `TestCommandWorkspaceSelectionIgnoresLiveSourceDoubles`, `TestCommandPurposeControlsClaudeStartupRequirements`, `TestValidateNativeToolBootConfig_CommandPurposeOwnsNativeAdmission`, managed Claude capability/probe matrix. |
| M9 | `TestCommandDescriptorBothStoresPreserveSelectionAndRefuseCorruption` executes Live/MockOnly on SQLite/PostgreSQL, preserves custom write-time model and exact double bytes, and rejects repeated reads with missing model/provider/transport without changing persisted bytes. H separately proves retained reopen. |
| M10 | `TestAuthoredMockSelectionSurvivesReconfigureAndRestart`, `TestAgentRuntimeSetRecoveryRejectsBackendDriftWithoutRewritingDescriptor`, `TestCommandRecoveryNeverSelectsAnIncompleteStoredDescriptor`, H J1-J4; `TestCommandLiveServeAndRestartParity` PASS, genuine graceful/forced L retained restart on both stores at 97a9ea04a. |
| M11 | `TestSelectedContractContainerValidatesStoredSelectionsWithoutRepair`, `TestSelectedContractContainerRejectsIncompleteStoredDescriptor`, `TestLLMForkChatExecutorRefusesConflictingStoredBackendWithoutReselection`, served selected-fork and fork-chat positives. |
| M12 | `TestPrivateTestDoesNotSelectConcurrentLiveRuntime`, `TestPrivateTestAdmissionPrecedesResourceAcquisition`; private API shared-client boundary guard. |
| M13 | `TestServedParityHarnessDerivedScenarioLifecycle`, `TestServedParityHarnessGeneratedInputFixtureLifecycle`, `TestInternalLifecycleScenarioConsumesAdmittedSourceAcrossSupportedBackendsAndModes`; public fresh authored/derived proof must remain separately credited. |
| M14 | `TestPrivateTestSessionJoinedCleanupAndPrivateTransport` covers success, failure, cancellation and scenario deadline; `TestPrivateTestStartupFailureUnwindsAcquiredResources`, `TestPrivateTestRootCleanupRemovesImmutableProjectionsWithoutFollowingLinks`; resource-by-resource mapping and executed listener, late-readiness, projection and ownership failures are tabulated below. |
| M15 | Private transport proves auth/absent ingress; the registered-adapter fence proves zero authorized external attempts; activity proof checks zero journal attempts, credential reads and HTTP requests. Deployment isolation checks zero ambient API calls. Exact counter map below; H outcomes remain separate from provisioned L. |
| M16 | `TestCommandConfigRecoveryDefaultsAndExplicitFalse`, H J1-J4 and exact startup refusal tests; `TestCommandLiveServeAndRestartParity` PASS with omitted recovery and exact pending-delivery convergence on both stores at 97a9ea04a. |
| M17 | `TestGoldenInvocationRootDevReadiness` L and `TestGoldenAgentWorkloadSQLiteDevScratchRestartStartsFreshEpoch` H, existing scratch-path/borrowed-store refusals. `TestCommandLiveServeAndRestartParity` PASS for two fresh SQLite dev epochs, later started_at/empty predecessor history, and exact retained-source history preservation in both parent-store journeys. |
| M18 | `TestCommandConfigRejectsMaskedExecutionSelectors` plus existing config/path provenance and precedence tests. |
| M19 | `TestCompiledProcessFullLifecycleJourneysSQLitePostgres` H J1-J5, golden H smoke/restart/burst, separate compiled L possession/invocation proof. `TestCommandLiveServeAndRestartParity` PASS at 97a9ea04a: real signed ingress, ordered receipt/event/delivery route, live reply/card, graceful/forced retained restart and fresh dev. H remains distinct; no public retained-test feature claimed. |
| M20 | Private callback checks authenticated `health.check` ready/mock_only before execution and rejects predecessor tokens; golden exact health posture checks and controlled live adapter observation. Bind notices/subscriptions/root modes to final T/L/H result set. |
| M21 | `TestStructuralValidationNeverObservesDeployment`, `TestStructuralCheckPurposeCensus`, `TestStructuralValidationRejectsUnknownPurpose`, structural CLI output tests. |
| M22 | Split / tracked separately: F-owned `#2319` retains credential-absent webhook startup, subsequent activation and Telegram example collapse. These are not passed or credited to provisioned L/internal H proof. Under the user-approved split, they do not block #2321 review/merge/closure; the broader onboarding claim remains unproven. |
| M23 | Split / tracked separately: `#1801`; existing non-posture config-layer precedence stays authoritative. |
| M24 | Split / tracked separately: `#2284`; genuine native/Claude/Docker prerequisites preserved. |
| M25 | `TestGenericScheduleExecutionModeSurvivesReplayAndStoreReconstructionOnBothStores`, `TestWorkflowTimerLifecyclePreservesMockExecutionModeOnBothStores`, standing census, pipeline preflight/claim negatives, external-effect recovery and directive/conversation-fork exact-mode matrices. Replay/resume/decision consumer map and focused execution results below. |
| M26 | `TestStructuralReadersShareAdmittedSource`, `TestStructuralReadersRetainPostBootInvalidity`, `TestStructuralReadersDoNotConstructChannelActivation`; compiled reader proof on untouched source. |
| M27 | Structural check census and no-deployment-observation tests; output explicitly says structural/not_evaluated, preserves production_valid harness meaning, removes operational capability fields. |
| M28 | `TestCommandOwnedSelectionRefusesMissingPurposeAndRetiredSelectors`, provider/pin/model table, descriptor corruption tests, masked-config rejection. |
| M29 | `TestPrivateTestIgnoresDeploymentResources` passes with both ambient store backends, an unreadable credential/token/password document, occupied API/MCP ports, a counted ambient API trap, unavailable Docker and retained-store/workspace sentinels. `TestPrivateTestRejectsEachExplicitDeploymentTargetBeforeAcquisition` covers all three target flags. Concurrent-live noninterference remains separate. |

## Resource And Reintroduction Evidence

Focused resource run on code head `34480efd9` passed (2.778s). This is additional
evidence, not a claim that the final integrated suite or all acceptance is done.

| Acquired owner / failure | Named execution proof |
|---|---|
| Private root, selected store and schema | `TestPrivateTestStartupFailureUnwindsAcquiredResources`: before store, after store/schema, workspace prepare/release failure, runtime-construction rejection; callback not called, store closed, root gone, project untouched. |
| Immutable data/source projection | `TestPrivateTestRootCleanupRemovesImmutableProjectionsWithoutFollowingLinks`; private scenario success/abort root removal; `TestCloseServeRuntimeReleasesProjectionAfterShutdown` proves release follows shutdown. |
| Runtime occurrence after construction | `TestBuildServeRuntimeContextFailureAfterRuntimeConstructionJoinsOccurrence`: real context construction fails after occurrence acquisition, no returned runtime and exact active count zero. |
| API/MCP listener binding | `TestRunServeRuntimeListenerBindFailuresExitBeforeReadiness`: independently occupied API and MCP refuse in the shared constructor with no readiness. Private success/failure/cancellation/deadline test proves API unreachable after return. |
| Process work versus selected-store lifetime | `TestTerminalStoreReleaseJoinsDelayedAccessBothStores`: delayed accepted SQLite/PostgreSQL access survives until its lease ends; close happens once after join. |
| Process capability and store release failure | `TestActivatedServeLifecycleRetriesRetainedCapabilityAndExactClose` injects first release/close failures and retains diagnostics; probe labels are not independent backend execution credit. `TestProcessLifecycleShutdownContinuesAfterTerminalCapabilitySettlementFailure` preserves remaining cleanup. |
| Timeout while accepted work is still active | `TestActivatedServeLifecycleTimeoutReportsButWaitsForExactWork`: neither capability nor store closes before exact work settles; timeout remains failure. |
| Final readiness barrier | `TestStartLocalRunServeLateReadinessGateFailureDoesNotCommit`: real loopback /readyz remains 503 at injected final failure, public command fails rather than publishing readiness. |
| Scenario callback and transport/auth lifetime | `TestPrivateTestSessionJoinedCleanupAndPrivateTransport`: success, failure, cancellation, deadline, current-token auth, predecessor-token refusal, ready/mock_only readback, absent provider ingress, closed DB/listener/root after return. |

Focused external-effect/delay run also passed on `34480efd9`: llm 2.838s,
pipeline 2.039s, generic schedule 0.008s. Named proofs are
`TestMockEffectFenceRejectsEveryExternalAdapterBeforeAuthorization`,
`TestExecuteMockCompletionUsesPythonAndCanonicalCompletionAuthority`,
`TestMockOnlyPostureRejectsLiveActivityBeforeJournalCredentialsAndHTTP`,
`TestMockOnlyPostureRejectsLiveDecisionAndDeferralBeforeMutationPreparation`,
`TestWorkflowTimerLifecyclePreservesMockExecutionModeOnBothStores`,
`TestWorkflowJoinSchedulePreservesMockExecutionModeOnBothStores`,
`TestMockOnlyPostureRejectsLiveScheduleBeforePersistence`, and
`TestOccurrenceEventPreservesImmutableExecutionMode`.

Replay/resume consumer check: event.replay and agent.replay share the typed
`operator_event_replay.go` original-mode admission and exact-mode event constructor.
`TestOperatorReplayMockOnlyRejectsLiveOriginalBeforeMutation` is the negative;
`TestServedParityHarnessLiveAgentEventReplayLifecycle` is the dual-store supported
positive. Runtime ingress resume invokes `PreflightRuntimeIngressQueue` before its
state mutation (`TestResumePreflightsQueueBeforeRuntimeStateMutation`). Run/runtime
public positives remain `TestServedParityHarnessRunControlLifecycle` and
`TestServedParityHarnessRuntimeIngressControlLifecycle`. The integrated execution
results must still be recorded. `agent.replay_backlog` is already retired on the
integrated baseline; only its forbidden-consumer guard remains. It is not a live
interpreter to preserve or reintroduce for this migration.

Additional focused reintroduction proof passed after `a4229f1c8` (only the new test
digest gained its canonical sha256 prefix): apiv1 3.143s, runcontrol 0.024s,
ingress 0.010s, runtimepersistence 23.091s. This is not a provisioned live-provider
result. The exact coverage, rather than shared-owner credit, is:

| Consumer / forbidden effect | Executed proof and observation |
|---|---|
| Every registered external adapter | `TestMockEffectFenceRejectsEveryExternalAdapterBeforeAuthorization` enumerates the registration owner, rejects every non-mock_python adapter, and checks zero authorized attempts. |
| Live activity in a mock command | `TestMockOnlyPostureRejectsLiveActivityBeforeJournalCredentialsAndHTTP` checks zero persisted attempts, zero credential reads and zero counted HTTP calls. |
| Ambient target/deployment resources | `TestPrivateTestIgnoresDeploymentResources` checks zero calls to the counted ambient API and unchanged retained store/workspace sentinels for both deployment store selections. |
| Public event.replay / agent.replay | `TestOperatorReplayMockOnlyRejectsLiveOriginalBeforeMutation` exercises both API methods on PostgreSQL, checks unchanged event/delivery counts and zero idempotency reservations. It is not labelled a dual-store negative. |
| Run continue and ingress resume ordering | `TestControllerContinuePreflightsQueueBeforeRunMutation` and `TestResumePreflightsQueueBeforeRuntimeStateMutation` prove their distinct mutation entrances stop at the failed preflight. |
| Run/runtime queue admission on both stores | `TestMockOnlyPipelinePreflightRejectsLiveQueuesWithoutClaimOnSQLiteAndPostgres` checks raw, immediate decision and future decision work, both scopes, unchanged decision snapshots and no receipt creation; `TestMockOnlyPipelinePreflightAdmitsExactMockQueueWithoutClaimOnSQLiteAndPostgres` preserves the positive. |
| Recovery claim on both stores | `TestMockOnlyPipelineScanRejectsPersistedLiveWorkBeforeClaimOnSQLiteAndPostgres` rejects the mock claim, then proves the live owner can claim the same untouched exact event. |
| Decision and deferral preparation | `TestMockOnlyPostureRejectsLiveDecisionAndDeferralBeforeMutationPreparation` exercises both mutation kinds, allowing only the canonical card read before refusal. |
| Directive transition on both stores | `TestDirectiveExecutionAdmitsExactPersistedModeBeforeTransitionOnSQLiteAndPostgres` proves a live operation remains prepared/unclaimed and an exact mock operation proceeds. |
| Conversation fork-chat admission | `TestConversationForkChatAdmitsExactSourceActorModeBeforeMutation` checks the persisted source actor's mode before fork mutation. |
| Standing reconstruction on both stores | `TestStandingServicePostureCensusAdmitsExactMockWork` executes the exact persisted-mode census on SQLite and PostgreSQL. |

These close the previously unnamed M14/M15/M25 proof-accounting gaps, not the
outstanding final-head/full-profile and provisioned L acceptance obligations.

## Honest Completion Boundary

The first unfiltered swarm-test run failed in nine packages. No runtime blocker was
established by those failures: they included old descriptor fixtures, retired
selector assertions, structural-reader/import guards, an unused import, a removed
empty fixture directory, an invalid model alias and a nil-vs-empty persisted
configuration revision. The second unfiltered run completed with failures in two
packages: conformance and serveapp. Its remaining packages passed, including
runtimepersistence, releasee2e, runtime, apiv1, cataloge2e and cliapp.

The remaining fixtures now have targeted passing proof: conformance startup and
decoder inventory (4.827s), serve abandonment (1.493s), and the complete real mock
emission matrix (37.306s: both stores, both naming forms, cardinality 1/2/3). The
mock fixture explicitly selects MockOnly with a configured live profile, uses the
canonical runtime set, and carries mock mode through root events and maintenance
context. No production mode fence was relaxed. An earlier obsolete matrix run was
cancelled after the live-context fixture mismatch was reproduced and classified;
it is not passing evidence. The new dual-store descriptor proof passed (1.807s).
The third integrated run with the full proof profile passed releasee2e (533.640s),
including complete retained journeys and golden restart/burst proof, but failed
conformance cleanup with a lifecycle diagnostic projection conflict. A remaining
serve mixed-agent fixture and audit-wording guard also failed; their bounded repair
passes both store cases and the exact guard (15.019s). Runtimepersistence exhausted
its aggregate ten-minute timeout with the current subtest only two seconds old.
No whole-suite pass is claimed.

A deterministic production-projector probe reproduced duplicate emission plus
one mark conflict in three of three executions. Ten real SQLite focused retries
passed, which does not erase the full-profile failure. The reproduction and exact
evidence/limitations are in the adjacent progress artifact and probe source. This
diagnostic finding is now classified by the lead as separate prerequisite #2432:
convergence plus provenance, including sequential failed acknowledgement retry and
cross-manager attribution. Its independent pre-audit is posted at
https://github.com/division-sh/swarm/issues/2432#issuecomment-5588649360;
The first repair gate was insufficient on selected-fork provenance. The additive
amendment at cfc60b3b4 is posted at
https://github.com/division-sh/swarm/issues/2432#issuecomment-5589274582;
independent re-gate approval is pending. #2321's existing gate remains approved. The relevant
production projector and store CAS are unchanged here; this is not silently
absorbed into command selection or repaired by weakening failure handling.

Remaining independent work includes the next integrated result and final-head
verification. External acceptance
still requires completing the planned L serve/restart journey, its provisioned
environment. #2319 is now a nonblocking follow-up, not an acceptance prerequisite,
under https://github.com/division-sh/swarm/issues/2321#issuecomment-5590488648
and watchlist a5ed497. Its deferred rows are not passed. #2432's blocking gate is
unchanged. The default-network workspace build failed on container DNS. A
build-time host-network retry of the unchanged repository Dockerfile succeeded;
the supported public workspace build then passed its runnable-CLI validation
using cached layers. Image swarm-workspace:agent-g-2321 contains executable
Claude Code 2.1.87. This proves image provisioning only, not provider authentication,
runtime network readiness, a live turn or retained restart. The expected OAuth
credential is absent from this process environment; no credential values were read
or logged and no authenticated live-provider proof is credited.
The existing paid Claude test does not cover the entire planned L matrix.
The specific `TestCommandLiveServeAndRestartParity` harness is now implemented
using the real public process, both retained stores and separate SQLite dev epochs.
Only its prerequisite refusal and shared H-helper regression have executed so far;
its authenticated L rows remain unproven, as do the separate live archetype rows.
After #2432 integration the full-profile rerun uses an explicit 30-minute package
timeout without relaxing scenario deadlines. No final audit or
review-ready PR exists. This checklist must not be submitted as proof of closure.

Latest provisioned checkpoint: protected credentials and the dedicated private
Telegram destination are now supplied. Standalone authenticated Claude succeeds;
the retained H dual-store J1-J5 matrix passes (50.505s). The first L run exposed an
invalid test message ID; the second stalled before reply approval. A paired socket
probe found the harness MCP listener unreachable from Docker at the advertised host
address. The bounded test/deployment correction uses a schema-valid counter and an
explicit reachable MCP listener IP; the actual corrected L rerun is queued, not
passed. These findings do not authorize a diagnostic/runtime scope expansion.

Subsequent L attempt reached a real live approval card, then failed an H-only
mock expectation before approval or outbound delivery. The helper now checks the
explicit expected mode, with passing local RPC regression cases for mock and
live. The corrected real SQLite run is queued. Card creation is partial live
evidence only; no Telegram-send, restart or L matrix pass is credited yet.

The next SQLite attempt passed the repaired approval but failed inbound delivery
completion with unclassified_runtime_error (22.624s after the queue). The user
confirmed receipt of the test message, establishing an external send despite the
failed delivery. Root cause and durable completion remain unresolved; no restart
or L matrix pass is credited. Public failure-evidence capture has been expanded
before cleanup, and a fresh isolated diagnostic journey is queued, not a replay
of the failed delivery. Runtime code and #2432's frozen gate remain unchanged.

Expanded capture now pins the repeated live failure to capability validation:
the reader substitutes canonical MCP names when exact provider-native names are
empty. A deterministic probe fails 3/3 on the branch and 3/3 on fresh master;
mixed/native-negative controls pass. The separate bounded escalation artifact
records this pre-existing capability defect for explicit lead disposition before
production repair. L remains blocked; external-send success is not turn acceptance,
retained restart, PostgreSQL L, or final-suite closure.

## Provider Restart Gate Follow-Through

Lead independently confirmed the transcript-loss defect and approved three bounded
additions at issuecomment-5593141304. The inventory repair is already pushed;
the new first repair covers uncertain-failure disposition and bounded structured
error explanations. See issue-2321-provider-restart-amendment.md for the complete
R1-R19 accounting and executed manifestation rows. Full LLM package, focused race,
API-spec, both-store terminal delivery/restart, persistence-fault and prelaunch
retry controls pass. This is not provider-private backing or live restart closure.
R1-R10/R16 and separate #2432 remain outstanding. No new live sends or full
integrated suite were run for this commit.
