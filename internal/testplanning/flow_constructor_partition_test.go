package testplanning

import (
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

func TestFlowConstructorProofPartitionRequiresBothStores(t *testing.T) {
	f, err := os.Open(filepath.Join("..", "..", ".github", "test-proof-plan.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	policy, err := LoadPolicy(f)
	if err != nil {
		t.Fatal(err)
	}
	for id, roots := range map[string][]string{
		"local-serveapp-canaries": {"TestReceiverCompositionRestartBothStores"},
		"serveapp-channel":        {"TestReceiverCompositionRestartBothStores"},
		"runtime-full": {
			"TestRuntimeStartRestoresWorkflowTimersWithoutGenericScheduleStoreOnBothStores",
			"TestRuntimeConstructedActorCensusRestartBothStores",
			"TestRuntimeConstructedActorCensusSourceSetRebindBothStores",
			"TestRuntimeConstructedActorRetainedRefreshShutdownBothStores",
			"TestRuntimeConstructedActorAtomicRebindRetryBothStores",
			"TestRuntimeConstructedActorPartialRefreshShutdownBothStores",
		},
		"store-runtime-full-01": {
			"TestRunControlControllerStopReconcilesBothTimerFamiliesOnBothStores",
			"TestAgentLifecycleReadinessSourceSetRebindBothStores",
			"TestAgentLifecyclePreparedReadinessRebindBothStores",
		},
		"store-runtime-full-02": {
			"TestDynamicFlowRuntimeCreationOccurrenceLinearizesWithTerminalizationOnBothStores",
			"TestDynamicFlowRuntimeCreationOccurrenceRollsBackAppendedEventOnBothStores",
			"TestDynamicFlowRuntimeCreationOccurrenceRejectsRetiredAttemptOnBothStores",
			"TestDynamicFlowCreationSourceSetRebindBothStores",
			"TestFieldlessFlowConstructionKeepsLifecycleWithoutStateRowBothStores",
		},
		"store-runtime-flow-lifecycle": {
			"TestFlowActivationSourceSetRebindBothStores",
			"TestFlowAttachmentNativeLostAckAfterRebindBothStores",
			"TestFlowConstructorAcknowledgedFailureRetainsExactIdentityBothStores",
			"TestFlowConstructorReplayAndRefusalBothStores",
			"TestFlowConstructorImmutableReplayConflictBothStores",
			"TestFlowConstructorActivationConsumesExactInputBothStores",
			"TestFlowConstructorPersistsCreatingInputWithoutAutoEmitBothStores",
			"TestFlowConstructorCommitsKeylessDescendantsBothStores",
			"TestFlowConstructorRootEagerTreeBothStores",
			"TestFlowConstructorHistoricalFieldlessSnapshotBothStores",
			"TestFlowConstructorHistoricalFieldedAndTerminalSnapshotBothStores",
			"TestFlowConstructorScenarioImportCannotAcquireExecutionBothStores",
			"TestFlowConstructorDescendantFaultRollsBackTreeBothStores",
			"TestFlowConstructorDescendantCancellationAndCorruptionBothStores",
			"TestFlowAttachmentPhaseFailureRetainsConstructionBothStores",
			"TestFlowAttachmentCleanupRetainsExactPredecessorBothStores",
			"TestFlowAttachmentTimerAcquisitionFailureCannotBecomeReadyBothStores",
			"TestFlowAttachmentTimerAcquisitionCutsBothStores",
			"TestFlowAttachmentAgentAcquisitionCutsBothStores",
			"TestFlowAttachmentActivationVerificationCutsBothStores",
			"TestFlowAttachmentNativeCommitAcknowledgmentBothStores",
		},
		"store-runtime-full-03-i-l": {
			"TestOrdinaryHandlerRequiresCanonicalConstructionBothStores",
			"TestOrdinaryWorkflowMutationCannotConstructOrRepairBothStores",
		},
		"store-runtime-full-03": {"TestGenericConstructedGateForkUsesFixedSnapshotBothStores"},
		"store-runtime-full-05": {
			"TestReceiverConfigActivationRaceAndRollbackBothStores",
			"TestReceiverEntitylessPendingAttachmentExecutesBothStores",
			"TestReceiverPublicationCommitUncertaintyBothStores",
			"TestReceiverPublicationExplicitTargetsBothStores",
			"TestReceiverHistoricalMaterializationRefusesCrossedEvidenceBothStores",
			"TestReceiverHistoricalMaterializationRefusesLaterNativeConstructionBothStores",
			"TestR7PreparedSelectionRechecksDurableAdmissionBothStores",
			"TestR7HistoricalDirectoryIgnoresPostCutConstructionAndLifecycleBothStores",
		},
		"store-runtime-selected": {"TestSelectedRunTargetOwnersUseConstructedHeadersBothStores"},
		"store-runtime-s-z-rest": {
			"TestWorkflowTimerSchedulerConsumesCommittedErrorOnBothStores",
			"TestWorkflowGateConsumesCommittedErrorWithoutRouteReplayOnBothStores",
		},
	} {
		unit := policy.Units[id]
		pattern := regexp.MustCompile(unit.Run)
		for _, root := range roots {
			stores := map[string]bool{"sqlite": false, "postgres": false}
			for _, child := range unit.RequiredChildren[root] {
				store, _, _ := strings.Cut(child, "/")
				if _, known := stores[store]; !known {
					t.Errorf("%s has unexpected backend proof %s/%s", id, root, child)
				}
				stores[store] = true
			}
			if !pattern.MatchString(root) || !stores["sqlite"] || !stores["postgres"] {
				t.Errorf("%s must select %s and require both stores", id, root)
			}
		}
	}
	for id, proofs := range map[string]map[string][]string{
		"store-runtime-flow-lifecycle": {
			"TestFlowAttachmentCleanupRetainsExactPredecessorBothStores": {
				"postgres/abandonment/fail_once", "postgres/abandonment/persistent", "postgres/abandonment_response_loss/fail_once", "postgres/abandonment_response_loss/persistent",
				"postgres/agent_fence/fail_once", "postgres/agent_join/fail_once", "postgres/agent_join/persistent", "postgres/timer/fail_once", "postgres/timer/persistent",
				"sqlite/abandonment/fail_once", "sqlite/abandonment/persistent", "sqlite/abandonment_response_loss/fail_once", "sqlite/abandonment_response_loss/persistent",
				"sqlite/agent_fence/fail_once", "sqlite/agent_join/fail_once", "sqlite/agent_join/persistent", "sqlite/timer/fail_once", "sqlite/timer/persistent",
			},
		},
		"store-runtime-full-05": {
			"TestReceiverConstructionReceiptBindsCreatingPublicationBothStores": {
				"postgres/fieldless_descendant/contradictory_control", "postgres/fieldless_descendant/contradictory_origin", "postgres/fieldless_descendant/crossed_agent_scope",
				"postgres/fieldless_descendant/exact", "postgres/fieldless_descendant/missing_receipt", "postgres/fieldless_descendant/partial_receipt",
				"postgres/fieldless_descendant/unrelated_event", "postgres/keyed/contradictory_control", "postgres/keyed/contradictory_origin",
				"postgres/keyed/crossed_agent_scope", "postgres/keyed/exact", "postgres/keyed/missing_receipt",
				"postgres/keyed/partial_receipt", "postgres/keyed/unrelated_event", "postgres/root/contradictory_control",
				"postgres/root/contradictory_origin", "postgres/root/crossed_agent_scope", "postgres/root/exact",
				"postgres/root/missing_receipt", "postgres/root/partial_receipt", "postgres/root/unrelated_event",
				"sqlite/fieldless_descendant/contradictory_control", "sqlite/fieldless_descendant/contradictory_origin", "sqlite/fieldless_descendant/crossed_agent_scope",
				"sqlite/fieldless_descendant/exact", "sqlite/fieldless_descendant/missing_receipt", "sqlite/fieldless_descendant/partial_receipt",
				"sqlite/fieldless_descendant/unrelated_event", "sqlite/keyed/contradictory_control", "sqlite/keyed/contradictory_origin",
				"sqlite/keyed/crossed_agent_scope", "sqlite/keyed/exact", "sqlite/keyed/missing_receipt",
				"sqlite/keyed/partial_receipt", "sqlite/keyed/unrelated_event", "sqlite/root/contradictory_control",
				"sqlite/root/contradictory_origin", "sqlite/root/crossed_agent_scope", "sqlite/root/exact",
				"sqlite/root/missing_receipt", "sqlite/root/partial_receipt", "sqlite/root/unrelated_event",
				"sqlite/keyed/conflicting_historical", "postgres/keyed/conflicting_historical", "sqlite/later_native_fork/exact",
				"sqlite/later_native_fork/stored_exact", "sqlite/later_native_fork/missing_receipt", "postgres/later_native_fork/exact",
				"postgres/later_native_fork/stored_exact", "postgres/later_native_fork/missing_receipt",
			},
			"TestReceiverHistoricalMaterializationRefusesCrossedEvidenceBothStores": {
				"sqlite/exact", "sqlite/source_run", "sqlite/revision", "sqlite/scope", "sqlite/logical_instance", "sqlite/native_missing_receipt",
				"postgres/exact", "postgres/source_run", "postgres/revision", "postgres/scope", "postgres/logical_instance", "postgres/native_missing_receipt",
			},
			"TestReceiverPublicationExplicitTargetsBothStores": {
				"sqlite/selected_review", "sqlite/selected_other", "sqlite/selected_root", "sqlite/selected_foreign_root_control", "sqlite/missing_header_entity", "sqlite/foreign_run", "sqlite/foreign_flow", "sqlite/foreign_instance", "sqlite/foreign_header_entity",
				"postgres/selected_review", "postgres/selected_other", "postgres/selected_root", "postgres/selected_foreign_root_control", "postgres/missing_header_entity", "postgres/foreign_run", "postgres/foreign_flow", "postgres/foreign_instance", "postgres/foreign_header_entity",
			},
			"TestR7PreparedSelectionRechecksDurableAdmissionBothStores": {
				"sqlite/unchanged", "sqlite/run_stopped", "sqlite/source_replaced", "sqlite/stored_winner", "sqlite/terminal_winner",
				"postgres/unchanged", "postgres/run_stopped", "postgres/source_replaced", "postgres/stored_winner", "postgres/terminal_winner",
			},
		},
		"store-runtime-full-02": {
			"TestDeploymentConstructionNativeCommitBothStores": {"sqlite/rollback_before_ack", "sqlite/commit_before_lost_ack", "postgres/rollback_before_ack", "postgres/commit_before_lost_ack"},
		},
		"store-runtime-s-z-rest": {
			"TestScenarioConstructionNativeCommitBothStores":   {"sqlite/rollback_before_ack", "sqlite/commit_before_lost_ack", "postgres/rollback_before_ack", "postgres/commit_before_lost_ack"},
			"TestScenarioConstructionFieldlessStateBothStores": {"sqlite", "postgres"},
		},
		"store-runtime-selected": {
			"TestSelectedContractOrdinarySourceStatePresenceBothStores": {
				"sqlite/absent", "sqlite/zero", "sqlite/fieldless", "sqlite/missing", "sqlite/corrupt", "sqlite/wrong-header-flow", "sqlite/wrong-header-type", "sqlite/loop",
				"postgres/absent", "postgres/zero", "postgres/fieldless", "postgres/missing", "postgres/corrupt", "postgres/wrong-header-flow", "postgres/wrong-header-type", "postgres/loop",
			},
		},
		"local-serveapp-canaries": {
			"TestReleaseReceiverInitializationBothStores":    {"default_sqlite/connected_typed_creation", "default_sqlite/direct_provider_schema", "default_sqlite/connected_restart/before_restart", "default_sqlite/connected_restart/after_restart", "explicit_postgres/connected_typed_creation", "explicit_postgres/direct_provider_schema", "explicit_postgres/connected_restart/before_restart", "explicit_postgres/connected_restart/after_restart"},
			"TestProviderSelectedRootStandingBootBothStores": {"default_sqlite", "explicit_postgres"},
		},
		"serveapp-other-late": {"TestProviderSelectedRootStandingBootBothStores": {"default_sqlite", "explicit_postgres"}},
		"local-catalog-smoke": {"TestReceiverConstructionBeforeNodeAndAgentExecutionBothStores": {"sqlite/collector", "sqlite/renamed-observer", "postgres/collector", "postgres/renamed-observer"}},
		"catalog-runtime":     {"TestReceiverConstructionBeforeNodeAndAgentExecutionBothStores": {"sqlite/collector", "sqlite/renamed-observer", "postgres/collector", "postgres/renamed-observer"}},
		"api-llm-bus-full":    {"TestOperatorEventPublishAgentHeaderSelectorsBothStores": {"sqlite/review", "sqlite/other", "postgres/review", "postgres/other"}},
		"serveapp-runtime":    {"TestReleaseReceiverInitializationBothStores": {"default_sqlite/connected_typed_creation", "default_sqlite/direct_provider_schema", "default_sqlite/connected_restart/before_restart", "default_sqlite/connected_restart/after_restart", "explicit_postgres/connected_typed_creation", "explicit_postgres/direct_provider_schema", "explicit_postgres/connected_restart/before_restart", "explicit_postgres/connected_restart/after_restart"}},
	} {
		unit := policy.Units[id]
		pattern := regexp.MustCompile(unit.Run)
		for root, children := range proofs {
			if !pattern.MatchString(root) || !reflect.DeepEqual(unit.RequiredChildren[root], children) {
				t.Errorf("%s must select %s and require all named public construction surfaces", id, root)
			}
		}
	}
}
