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
			"TestFlowAttachmentRouteAcquisitionCutsBothStores",
			"TestFlowAttachmentNativeCommitAcknowledgmentBothStores",
		},
		"store-runtime-full-03-i-l": {
			"TestOrdinaryHandlerRequiresCanonicalConstructionBothStores",
			"TestOrdinaryWorkflowMutationCannotConstructOrRepairBothStores",
		},
		"store-runtime-full-03": {"TestGenericConstructedGateForkUsesFixedSnapshotBothStores"},
		"store-runtime-full-05": {"TestReceiverConfigActivationRaceAndRollbackBothStores"},
		"store-runtime-full-06": {
			"TestSelectedRunTargetOwnersUseConstructedHeadersBothStores",
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
		"store-runtime-full-02": {
			"TestDeploymentConstructionNativeCommitBothStores": {"sqlite/rollback_before_ack", "sqlite/commit_before_lost_ack", "postgres/rollback_before_ack", "postgres/commit_before_lost_ack"},
		},
		"store-runtime-full-06": {
			"TestScenarioConstructionNativeCommitBothStores":   {"sqlite/rollback_before_ack", "sqlite/commit_before_lost_ack", "postgres/rollback_before_ack", "postgres/commit_before_lost_ack"},
			"TestScenarioConstructionFieldlessStateBothStores": {"sqlite", "postgres"},
			"TestSelectedContractOrdinarySourceStatePresenceBothStores": {
				"sqlite/absent", "sqlite/zero", "sqlite/fieldless", "sqlite/missing", "sqlite/corrupt", "sqlite/wrong-header-flow", "sqlite/wrong-header-type", "sqlite/loop",
				"postgres/absent", "postgres/zero", "postgres/fieldless", "postgres/missing", "postgres/corrupt", "postgres/wrong-header-flow", "postgres/wrong-header-type", "postgres/loop",
			},
		},
		"local-serveapp-canaries": {
			"TestReleaseReceiverInitializationBothStores":    {"default_sqlite/connected_typed_creation", "default_sqlite/direct_provider_schema", "explicit_postgres/connected_typed_creation", "explicit_postgres/direct_provider_schema"},
			"TestProviderSelectedRootStandingBootBothStores": {"default_sqlite", "explicit_postgres"},
		},
		"serveapp-other-late": {"TestProviderSelectedRootStandingBootBothStores": {"default_sqlite", "explicit_postgres"}},
		"local-catalog-smoke": {"TestReceiverConstructionBeforeNodeAndAgentExecutionBothStores": {"sqlite/collector", "sqlite/renamed-observer", "postgres/collector", "postgres/renamed-observer"}},
		"catalog-runtime":     {"TestReceiverConstructionBeforeNodeAndAgentExecutionBothStores": {"sqlite/collector", "sqlite/renamed-observer", "postgres/collector", "postgres/renamed-observer"}},
		"serveapp-runtime":    {"TestReleaseReceiverInitializationBothStores": {"default_sqlite/connected_typed_creation", "default_sqlite/direct_provider_schema", "explicit_postgres/connected_typed_creation", "explicit_postgres/direct_provider_schema"}},
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
