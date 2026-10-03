# #2544 Fixed-Tier Proof Census

Source: `0fa24140aaa05e397509f451ba780fef3a260867`. Membership approved as amended by gate5965227266; no profile changed or candidate execution credit yet.

Tier selection uses plain CI-Tier/Local-Tier PR-body lines, not an authorization record. For #2544 both are full. Lifecycle is provisional: retain it only if actual comparable whole-run cost saves at least20% versus full with exact declared obligations; otherwise collapse it into full in this PR.

Historical component equivalents: core60.10, lifecycle236.28, full339.10 runner-minutes. The102.82-minute/30.3% lifecycle difference is measured old-job accounting, not candidate-tier execution or savings. See the audit's measured component table and gate.

Each existing unit is retained. Core projections select named existing roots; higher tiers consume their canonical containing partitions once rather than running overlapping helper projections twice.
A full-only family remains full even when its source changes: this is the accepted fixed-frequency tradeoff, not affected-owner selection.

Costs are whole assigned jobs from historical run 37083235335 attempt 1, not candidate estimates. A dash means that exact unit was not assigned; never infer a free proof.

Historical PR burst jobs skipped their N=10 workloads. Their short job durations are not evidence of full-tier race-burst execution cost.

| Existing unit | Proposed whole-family minimum | Active roots | Historical runner min | Current profiles |
| --- | --- | ---: | ---: | --- |
| `api-llm-bus-full` | lifecycle | 1181 | 3.85 | pr-common, pr-escalated, full, nightly |
| `catalog-full` | full | 89 | - |  |
| `catalog-replay-1` | full | 30 | 2.75 | pr-escalated, full, nightly |
| `catalog-replay-2-3` | full | 2 | 3.15 | pr-escalated, full, nightly |
| `catalog-required-inventory` | core | 13 | 0.75 | local, pr-common, pr-escalated, full, nightly |
| `catalog-required-smoke` | core | 1 | - | pr-common |
| `catalog-required-verify` | core | 753 | 6.60 | local, pr-common, pr-escalated, full, nightly |
| `catalog-runtime` | full | 54 | 3.18 | pr-escalated, full, nightly |
| `catalog-runtime-fork-readiness` | lifecycle | 2 | 3.28 | pr-escalated, full, nightly |
| `catalog-runtime-scatter-safety` | core | 1 | 2.10 | pr-escalated, full, nightly |
| `conformance-1` | lifecycle | 26 | 3.42 | pr-common, pr-escalated, full, nightly |
| `conformance-2` | lifecycle | 162 | 4.70 | pr-common, pr-escalated, full, nightly |
| `conformance-2394-core` | lifecycle | 14 | 4.52 | pr-common, pr-escalated, full, nightly |
| `conformance-2394-pressure` | core | 5 | 3.85 | pr-common, pr-escalated, full, nightly |
| `conformance-2394-reporter` | full | 1 | 7.38 | pr-common, pr-escalated, full, nightly |
| `conformance-heavy-fanout` | full | 2 | 4.97 | pr-common, pr-escalated, full, nightly |
| `conformance-soak-postgres` | full | 1 | 17.28 | nightly |
| `conformance-soak-sqlite` | full | 1 | 16.87 | nightly |
| `contracts-first` | lifecycle | 304 | 3.45 | pr-common, pr-escalated, full, nightly |
| `contracts-rest` | lifecycle | 341 | 2.53 | pr-common, pr-escalated, full, nightly |
| `delivery-continuation-full` | lifecycle | 38 | 0.90 | pr-common, pr-escalated, full, nightly |
| `hitl-releasee2e-burst-1` | full | 1 | 0.73 | pr-common, pr-escalated, full, nightly |
| `hitl-releasee2e-burst-2` | full | 1 | 0.92 | pr-common, pr-escalated, full, nightly |
| `hitl-releasee2e-golden` | lifecycle | 12 | 5.67 | pr-common, pr-escalated, full, nightly |
| `hitl-releasee2e-invocation-1` | full | 1 | 1.93 | pr-common, pr-escalated, full, nightly |
| `hitl-releasee2e-invocation-2` | full | 1 | 2.33 | pr-common, pr-escalated, full, nightly |
| `hitl-releasee2e-invocation-3` | full | 1 | 2.48 | pr-common, pr-escalated, full, nightly |
| `hitl-releasee2e-invocation-4` | full | 1 | 2.37 | pr-common, pr-escalated, full, nightly |
| `hitl-releasee2e-invocation-5` | full | 1 | 2.17 | pr-common, pr-escalated, full, nightly |
| `hitl-releasee2e-invocation-6` | full | 1 | 1.80 | pr-common, pr-escalated, full, nightly |
| `hitl-releasee2e-journeys` | lifecycle | 2 | 1.58 | pr-common, pr-escalated, full, nightly |
| `hitl-releasee2e-rest` | lifecycle | 47 | 9.43 | pr-common, pr-escalated, full, nightly |
| `local-api-matrix-registry` | core | 1 | - | local |
| `local-api-routing-canaries` | core | 2 | - | local |
| `local-catalog-smoke` | core | 1 | - | local |
| `local-delivery-continuation` | core | 8 | - | local |
| `local-fanout-handoff-ack-loss` | core | 1 | - | local |
| `local-generated-fanout-fixture` | core | 1 | - | local |
| `local-release-golden-restart` | lifecycle | 2 | - | local |
| `local-release-lifecycle-smoke` | core | 1 | - | local |
| `local-routing-reporter` | core | 3 | - | local |
| `local-runtime-bus-full` | core | 591 | - | local |
| `local-serveapp-canaries` | core | 10 | - | local |
| `parity-connected-channel-onboarding-surface` | core | 1 | 1.13 |  |
| `parity-destructive-reset-crash` | lifecycle | 1 | 1.27 |  |
| `parity-golden-forced-restart` | lifecycle | 1 | 1.77 |  |
| `parity-served-source-artifact` | core | 1 | 1.20 |  |
| `runtime-full` | lifecycle | 413 | 2.02 | pr-common, pr-escalated, full, nightly |
| `selected-store-fast` | lifecycle | 4 | - |  |
| `serveapp-channel` | lifecycle | 57 | 4.47 | pr-common, pr-escalated, full, nightly |
| `serveapp-channel-delivery` | lifecycle | 36 | 7.13 | pr-common, pr-escalated, full, nightly |
| `serveapp-channel-learned` | lifecycle | 6 | 9.50 | pr-common, pr-escalated, full, nightly |
| `serveapp-channel-lifecycle` | lifecycle | 3 | 5.23 | pr-common, pr-escalated, full, nightly |
| `serveapp-channel-native` | lifecycle | 8 | 6.77 | pr-common, pr-escalated, full, nightly |
| `serveapp-channel-process-temporal` | lifecycle | 7 | 9.35 | pr-common, pr-escalated, full, nightly |
| `serveapp-i-reporter` | full | 3 | 11.33 | pr-common, pr-escalated, full, nightly |
| `serveapp-journeys-a-c` | lifecycle | 16 | 2.60 | pr-common, pr-escalated, full, nightly |
| `serveapp-journeys-d-l` | lifecycle | 16 | 3.00 | pr-common, pr-escalated, full, nightly |
| `serveapp-journeys-first` | lifecycle | 58 | 3.47 | pr-common, pr-escalated, full, nightly |
| `serveapp-journeys-m-z` | lifecycle | 34 | 3.73 | pr-common, pr-escalated, full, nightly |
| `serveapp-mailbox` | lifecycle | 10 | 3.82 | pr-common, pr-escalated, full, nightly |
| `serveapp-mailbox-p-q` | lifecycle | 3 | 3.38 | pr-common, pr-escalated, full, nightly |
| `serveapp-other` | lifecycle | 40 | 3.42 | pr-common, pr-escalated, full, nightly |
| `serveapp-other-late` | lifecycle | 57 | 9.22 | pr-common, pr-escalated, full, nightly |
| `serveapp-publication-text` | lifecycle | 1 | 2.85 | pr-common, pr-escalated, full, nightly |
| `serveapp-runtime` | lifecycle | 92 | 3.92 | pr-common, pr-escalated, full, nightly |
| `serveapp-selected` | lifecycle | 8 | 3.55 | pr-common, pr-escalated, full, nightly |
| `serveapp-selected-rest` | lifecycle | 22 | 2.90 | pr-common, pr-escalated, full, nightly |
| `serveapp-standing` | lifecycle | 28 | 2.62 | pr-common, pr-escalated, full, nightly |
| `serveapp-surfaces` | lifecycle | 1 | 2.87 | pr-common, pr-escalated, full, nightly |
| `store-admission-full` | core | 141 | 1.48 | pr-common, pr-escalated, full, nightly |
| `store-runtime-fanout` | lifecycle | 107 | 2.67 | pr-common, pr-escalated, full, nightly |
| `store-runtime-fanout-process` | lifecycle | 1 | 2.23 | pr-common, pr-escalated, full, nightly |
| `store-runtime-fork-generation` | lifecycle | 3 | 5.12 | pr-common, pr-escalated, full, nightly |
| `store-runtime-full-01` | lifecycle | 128 | 2.97 | pr-common, pr-escalated, full, nightly |
| `store-runtime-full-02` | lifecycle | 126 | 2.43 | pr-common, pr-escalated, full, nightly |
| `store-runtime-full-03` | lifecycle | 101 | 3.20 | pr-common, pr-escalated, full, nightly |
| `store-runtime-full-03-i-l` | lifecycle | 155 | 3.90 | pr-common, pr-escalated, full, nightly |
| `store-runtime-full-04` | lifecycle | 223 | 3.30 | pr-common, pr-escalated, full, nightly |
| `store-runtime-full-05` | lifecycle | 61 | 2.48 | pr-common, pr-escalated, full, nightly |
| `store-runtime-full-06` | lifecycle | 254 | 3.02 | pr-common, pr-escalated, full, nightly |
| `store-runtime-full-07-fork` | lifecycle | 119 | 2.70 | pr-common, pr-escalated, full, nightly |

## Generated Ordinary Packages

Every automatically discovered non-special package remains core; no import graph or change-based selection is introduced.
The existing generated broad shards are physical partitions, not additional proof owners.

| Current full partition | Packages | Active roots | Historical runner min |
| --- | ---: | ---: | ---: |
| `broad-01` | 25 | 368 | 3.13 |
| `broad-02` | 26 | 411 | 2.68 |
| `broad-03` | 26 | 758 | 6.42 |
| `broad-04` | 31 | 384 | 2.15 |
| `broad-05` | 31 | 810 | 2.17 |
| `broad-06` | 31 | 689 | 2.72 |
| `broad-07` | 31 | 576 | 2.30 |
| `broad-08` | 32 | 453 | 1.83 |
| `broad-09` | 30 | 553 | 1.85 |

## Public Backend Proof Catalogue

Every current typed reference receives an explicit minimum tier. Backend children, capabilities and purpose identities remain unchanged. Non-Go references retain their existing separate owner.

| Proof ID | Exact executable root | Proposed minimum | Required backend/child evidence |
| --- | --- | --- | --- |
| `runtime-open-sqlite` | `TestOpenRuntimeSQLiteResolvesRequiredAndOptionalProductsOnce` | core | default_sqlite |
| `runtime-open-postgres` | `TestOpenRuntimePostgresResolvesRequiredAndOptionalBundleWritersOnce` | core | explicit_postgres |
| `runtime-port-projection` | `TestServeCompositionProvidesExactRuntimePorts` | lifecycle |  |
| `runtime-close-postgres-ping` | `TestPostgresFailedPingClosesTheConstructedPool` | core |  |
| `runtime-close-unactivated` | `TestOwnerUnactivatedCloseFailureRetainsCloseAuthority` | core |  |
| `authority-inspection-sqlite` | `TestStoreStatusReadsCurrentSQLiteStoreWithoutMutationBootstrap` | core | default_sqlite |
| `authority-inspection-postgres` | `TestStoreStatusDoesNotBootstrapEmptyPostgres` | core | explicit_postgres |
| `authority-inspection-close` | `TestAuthorityInspectionPropagatesOperationAndCloseErrors` | core |  |
| `authority-repair-parity` | `TestAuthorityRepairParity` | lifecycle | postgres, sqlite |
| `authority-maintenance-sqlite` | `TestSQLiteAuthorityMaintenanceConstructionRetainsBackendPossession` | core | default_sqlite |
| `authority-maintenance-close` | `TestAuthorityMaintenancePropagatesCloseError` | core |  |
| `store-infrastructure-sqlite` | `TestBuildStoresAcceptsSQLiteSelectedCoreRuntimeStore` | lifecycle | default_sqlite |
| `schema-ddl` | `TestGeneratePlatformTableDDLs` | core | default_sqlite, explicit_postgres |
| `resource-data-surface` | `TestDurableDataHTTPPublicSurfaceAcrossSelectedStores` | lifecycle | sqlite, postgres |
| `served-source-artifact` | `TestInternalLifecycleScenarioConsumesAdmittedSourceAcrossSupportedBackendsAndModes` | core | sqlite/nondev, sqlite/dev, postgres/nondev |
| `served-event-publish` | `TestServedParityHarnessEventPublishDynamicAutoEmitLifecycle` | lifecycle | default_sqlite, explicit_postgres |
| `served-run-start-deployment-feed` | `TestServedParityHarnessRunStartDeploymentFeedLifecycle` | lifecycle | default_sqlite, explicit_postgres |
| `served-event-replay` | `TestServedParityHarnessLiveAgentEventReplayLifecycle` | lifecycle | default_sqlite, explicit_postgres |
| `event-bus-roles` | `TestEventBusDurableDependenciesAreExplicit` | lifecycle | default_sqlite, explicit_postgres |
| `author-activity-parity` | `TestAuthorActivityRollbackReusesSequenceOnBothStores` | lifecycle | sqlite, postgres |
| `run-fork-revision-parity` | `TestRunForkRevisionThirteenFamilySelectedStoreParity` | lifecycle | sqlite, postgres |
| `operator-channel-surface` | `TestServedParityHarnessOperatorChannelLifecycle` | lifecycle | default_sqlite, explicit_postgres |
| `connected-channel-onboarding-surface` | `TestChannelConnectTelegramFirstUserJourney` | core | default_sqlite, explicit_postgres |
| `channel-delivery-surface` | `TestChannelDeliveryNativeInboxE2E` | lifecycle | sqlite, postgres |
| `served-agent-restart` | `TestServedParityHarnessAgentRestartLifecycle` | lifecycle | default_sqlite, explicit_postgres |
| `served-agent-directive` | `TestServedParityHarnessAgentDirectiveOutcomeLifecycle` | lifecycle | default_sqlite, explicit_postgres |
| `agent-manager-roles` | `TestAgentManagerPersistenceRolesAreExplicit` | lifecycle | default_sqlite, explicit_postgres |
| `external-effect-recovery` | `TestExternalEffectRecoveryPostureAdmissionGenericSQLiteAndPostgres` | lifecycle | sqlite, postgres |
| `served-selected-contract-fork` | `TestServedParityHarnessRunForkLifecycle` | lifecycle | default_sqlite, explicit_postgres |
| `served-selected-reset-construction` | `TestSelectedForkPublicChangedTargetAfterResetBothStores` | lifecycle | default_sqlite, explicit_postgres |
| `workflow-settlement-parity` | `TestWorkflowEngineMutationSettlesExactNodeDeliveryAtomicallyOnBothStores` | lifecycle | sqlite, postgres |
| `served-test-setup` | `TestServedParityHarnessTestSetupEntitiesLifecycle` | lifecycle | default_sqlite, explicit_postgres |
| `workflow-opaque-owner` | `TestWorkflowPersistenceIsOpaqueAndConcreteStoreDoesNotEscape` | lifecycle | default_sqlite, explicit_postgres |
| `conversation-sqlite` | `TestSQLiteAgentConversationOwnerBacksSupportedAPISurface` | lifecycle | default_sqlite |
| `conversation-postgres` | `TestPostgresAgentConversationOwnerBacksSupportedAPISurface` | lifecycle | explicit_postgres |
| `served-conversation-fork` | `TestServedParityHarnessConversationForkLifecycle` | lifecycle | default_sqlite, explicit_postgres |
| `served-mailbox-decision` | `TestServedParityHarnessMailboxDecisionLifecycle` | lifecycle | default_sqlite, explicit_postgres |
| `api-idempotency-sqlite` | `TestSQLiteRuntimeStoreAPIIdempotencySerializesAcrossSamePathHandles` | lifecycle | default_sqlite |
| `api-idempotency-postgres` | `TestPostgresStore_APIIdempotencyReplaysAndConflicts` | lifecycle | explicit_postgres |
| `served-run-control` | `TestServedParityHarnessRunControlLifecycle` | core | default_sqlite, explicit_postgres |
| `served-ingress-control` | `TestServedParityHarnessRuntimeIngressControlLifecycle` | lifecycle | default_sqlite, explicit_postgres |
| `budget-recovery-parity` | `TestCompletionBudgetRecoveryProjectionParity` | lifecycle | sqlite, postgres |
| `timer-schedule-parity` | `TestGenericScheduleLifecyclePublishesOneShotAndRecurringThroughWorkflowRuntimeOnBothStores` | lifecycle | sqlite, postgres |
| `scenario-execution-sqlite` | `TestSQLiteScenarioSetupPersistsExactExecutionProfileAtomically` | lifecycle | default_sqlite |
| `scenario-execution-postgres` | `TestPostgresScenarioSetupPersistsExactExecutionProfileAtomically` | lifecycle | explicit_postgres |
| `observability-sqlite` | `TestSQLiteRuntimeLogPersistenceWritesLoggerRowsForObservability` | lifecycle | default_sqlite |
| `observability-postgres` | `TestPostgresRuntimeLogPersistencePreservesRunSourceAndLineage` | lifecycle | explicit_postgres |
| `source-startup-integrity` | `TestSourceArtifactStartupIntegrityParityPreservesRunHistory` | lifecycle | sqlite, postgres |
| `served-startup-integrity` | `TestRunServeSourceArtifactIntegrityRejectsBeforeReadinessBothStores` | lifecycle | sqlite/missing, sqlite/corrupt, sqlite/hash_mismatch, postgres/missing, postgres/corrupt, postgres/hash_mismatch |
| `destructive-reset-postgres` | `TestRunServeRuntimeNukeQuiescesSessionWriterBeforeCleanupPostgres` | lifecycle | explicit_postgres |
| `destructive-reset-served` | `TestServedResetRetainClearAndHistoricalReplayBothStores` | core | default_sqlite/clear=false, default_sqlite/clear=true, explicit_postgres/clear=false, explicit_postgres/clear=true |
| `destructive-reset-cli` | `TestServedResetCLIDryRunApplyAndReplayBothStores` | lifecycle | default_sqlite, explicit_postgres |
| `destructive-reset-final-receipt` | `TestRunServeResetFinalReceiptFailureRetryPreservesLiveSuccessorBothStores` | lifecycle | default_sqlite, explicit_postgres |
| `destructive-reset-crash` | `TestResetProcessDeathRecoversBeforeSourceAdmissionBothStores` | lifecycle | default_sqlite/admitted/clear=false, default_sqlite/admitted/clear=true, default_sqlite/containers_settled/clear=false, default_sqlite/containers_settled/clear=true, explicit_postgres/admitted/clear=false, explicit_postgres/admitted/clear=true, explicit_postgres/containers_settled/clear=false, explicit_postgres/containers_settled/clear=true |
| `destructive-reset-candidates` | `TestResetCandidateSetFencesConsumersWhileSecondPreparesBothStores` | lifecycle | sqlite/failure=none, sqlite/failure=second_preparation, sqlite/failure=registered_release, sqlite/failure=normal_registered_release, sqlite/failure=normal_active_release, sqlite/failure=reset_active_release, sqlite/failure=normal_second_preparation, postgres/failure=none, postgres/failure=second_preparation, postgres/failure=registered_release, postgres/failure=normal_registered_release, postgres/failure=normal_active_release, postgres/failure=reset_active_release, postgres/failure=normal_second_preparation |
| `destructive-reset-provider-guard` | `TestResetRetainedCleanupProviderAuthorityBothStores` | lifecycle | sqlite/pending_current, sqlite/pending_drain, sqlite/terminal_current, sqlite/terminal_drain, postgres/pending_current, postgres/pending_drain, postgres/terminal_current, postgres/terminal_drain |
| `destructive-reset-directive-guard` | `TestResetRetainedCleanupDirectiveAuthorityBothStores` | lifecycle | prepared/sqlite, prepared/postgres, executing/sqlite, executing/postgres, indeterminate/sqlite, indeterminate/postgres, succeeded/sqlite, succeeded/postgres, failed/sqlite, failed/postgres, expired_succeeded/sqlite, expired_succeeded/postgres, expired_failed/sqlite, expired_failed/postgres |
| `destructive-reset-scope-guard` | `TestResetRetainedCleanupScopeAndRollbackBothStores` | lifecycle | foreign_key/sqlite, foreign_key/postgres, late_source_run/sqlite, late_source_run/postgres, late_retained_run/sqlite, late_retained_run/postgres, forged_quiescence/sqlite, forged_quiescence/postgres |
| `websocket-different-concept` | `TestOpenRPCWebSocketRuntimeProbes` | lifecycle | default_sqlite, explicit_postgres |
| `golden-forced-restart` | `TestGoldenAgentWorkloadRestartAndForcedKillOnBothBackends` | lifecycle | sqlite, postgres |
| `fanout-contention-2274` | `` | separate non-Go proof |  |
| `dev-restart-abandonment-2364` | `` | separate non-Go proof |  |

## Current Bound Plan Totals

| Existing profile | Units | Packages | Selected occurrences | Required | Deferred | Digest |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| local | 14 | 272 | 6389 | 6382 | 7 | `a36fa8613c017a8c37783dbcec1e3d836fcf933c5401c13080e4ac570aeb4812` |
| pr-common | 67 | 278 | 10250 | 10217 | 33 | `864a25cebac2300c0ee11d740836340ab75ac3455fce75f96beb23ca42f9b9b2` |
| pr-escalated | 71 | 278 | 10338 | 10304 | 34 | `2c2edce77d464dc26d24f3b89a779d89719b42cddd932096fe90f00b9665b75c` |
| full | 71 | 278 | 10338 | 10306 | 32 | `148af2cce56f3aaf8d5516c9b4d0049a871aa0b15ef2d0b70f790c0dcbb85b98` |
| nightly | 73 | 278 | 10340 | 10308 | 32 | `cfb86b417568868a469c30350bc4b0f3f9b1914f9cf25bbb4617a227cb426753` |

Unique active roots: 10339. Proposed lowest-tier root classification: {"core"=>6537, "full"=>99, "lifecycle"=>3703}.
These are static classifications, not proof that the proposed selector, cumulative scope, resource isolation, cost envelope or hosted admission has been implemented.
The retained JSON includes each unit command/children, all root coordinates/deferrals and every public backend catalogue reference.
