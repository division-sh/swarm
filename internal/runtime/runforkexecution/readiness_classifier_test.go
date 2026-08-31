package runforkexecution

import (
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func TestBuildSelectedContractReadinessClassifierEmitsCompleteOwnerMatrix(t *testing.T) {
	frontier := runfork.RunForkContractFrontierAdmission{
		Owner:                        runfork.RunForkContractFrontierAdmissionOwner,
		NonMutating:                  true,
		HistoricalExecutionSupported: false,
		ContractSelection: runfork.RunForkContractSelection{
			Mode:            "selected_contracts",
			ContractsRoot:   "/tmp/contracts",
			WorkflowName:    "workflow",
			WorkflowVersion: "v1",
		},
		FrontierEventCount: 1,
		FrontierEvents: []runfork.RunForkContractFrontierEvent{{
			SourceEventID: "source-event",
			EventName:     "work.begin",
			DerivedRecipients: []runfork.RunForkContractFrontierRecipient{
				testAgentFrontierRecipient("worker", "flow-a/worker", "selected_contracts", agentidentity.Plan{}),
			},
		}},
	}
	model := testSelectedContractExecutionModel(t, frontier)
	plan := runfork.RunForkPlan{
		SourceRunID: "source-run",
		ForkPoint: runfork.RunForkPoint{
			Input:     "source-event",
			EventID:   "source-event",
			EventName: "work.begin",
			Timestamp: time.Unix(1700000707, 0).UTC(),
		},
		ReplayResumeAdmission: runfork.RunForkReplayResumeAdmission{
			Owner:                    runfork.RunForkReplayResumeAdmissionOwner,
			DeliveryEventReplayReady: true,
			Dispositions: []runfork.RunForkReplayResumeDisposition{{
				Fact:           runfork.RunForkReplayResumeFactDeliveryPendingHistory,
				Disposition:    runfork.RunForkReplayResumeDispositionForkReplay,
				Classification: runfork.RunForkPendingClassificationPending,
				Message:        "pending delivery can be replayed",
			}, {
				Fact:        runfork.RunForkReplayResumeFactSourceAdvanced,
				Disposition: runfork.RunForkReplayResumeDispositionLineageOnly,
				Owner:       runfork.RunForkSelectedContractSourceAdvancedConversationHistoryPolicyOwner,
				Message:     "post-T source conversation history is branch divergence evidence",
			}},
		},
	}

	readiness, err := BuildSelectedContractReadinessClassifier(SelectedContractReadinessClassifierRequest{
		Plan:                      plan,
		ContractFrontierAdmission: frontier,
		SelectedContractExecution: model,
	})
	if err != nil {
		t.Fatalf("BuildSelectedContractReadinessClassifier: %v", err)
	}
	if readiness.Owner != runfork.RunForkSelectedContractReadinessClassifierOwner ||
		!readiness.NonMutating ||
		readiness.ReplayResumeAdmissionOwner != runfork.RunForkReplayResumeAdmissionOwner ||
		readiness.ContractFrontierAdmissionOwner != runfork.RunForkContractFrontierAdmissionOwner ||
		readiness.RouteTopologyOwner != runfork.RunForkSelectedContractRouteTopologyOwner ||
		readiness.RecipientPlanningOwner != runfork.RunForkSelectedContractRecipientPlanningOwner ||
		readiness.SelectedExecutionModelOwner != runfork.RunForkSelectedContractExecutionModelOwner {
		t.Fatalf("readiness ownership = %#v", readiness)
	}
	assertReadinessFactSet(t, readiness.FactMatrix)
	assertReadinessFact(t, readiness.FactMatrix, runfork.RunForkSelectedContractReadinessFactSourceDeliveries, runfork.RunForkSelectedContractReadinessDispositionExecutableForkWork, runfork.RunForkHistoricalReplayExecutionOwner)
	assertReadinessFact(t, readiness.FactMatrix, runfork.RunForkSelectedContractReadinessFactForkEvents, runfork.RunForkSelectedContractReadinessDispositionExecutableForkWork, runfork.RunForkSelectedContractExecutionOwner)
	assertReadinessFact(t, readiness.FactMatrix, runfork.RunForkSelectedContractReadinessFactSelectedRecipientsRoutes, runfork.RunForkSelectedContractReadinessDispositionReconstructedForkState, runfork.RunForkSelectedContractRecipientPlanningOwner)
	assertReadinessFact(t, readiness.FactMatrix, runfork.RunForkSelectedContractReadinessFactSourcePostTFacts, runfork.RunForkSelectedContractReadinessDispositionBranchDivergenceEvidence, runfork.RunForkSelectedContractSourceAdvancedConversationHistoryPolicyOwner)
	assertReadinessFact(t, readiness.FactMatrix, runfork.RunForkSelectedContractReadinessFactOperatorConsumers, runfork.RunForkSelectedContractReadinessDispositionUnsupportedSplitSibling, runfork.RunForkHistoricalReplayExecutionAdmissionOwner)
	if !executionBoundaryHas(readiness.InvalidPaths, "explain_output_authorizes_mutation", runfork.RunForkSelectedContractDispositionInvalid) {
		t.Fatalf("invalid paths = %#v, want explain output non-authoritative", readiness.InvalidPaths)
	}
	if !executionBoundaryHas(readiness.RequiredConsumers, "fork_local_runtime_container", runfork.RunForkSelectedContractDispositionPrerequisite) {
		t.Fatalf("required consumers = %#v, want fork-local runtime container prerequisite", readiness.RequiredConsumers)
	}
	if executionBoundaryHas(readiness.BlockedSiblings, "fork_local_runtime_container", runfork.RunForkSelectedContractDispositionBlockedSibling) {
		t.Fatalf("blocked siblings = %#v, fork-local runtime container should be an implemented required consumer", readiness.BlockedSiblings)
	}
}

func TestSelectedContractRunForkRouteConsumersAreClassifiedOutsideEventBusRouteAuthority(t *testing.T) {
	frontier := runfork.RunForkContractFrontierAdmission{
		Owner:                        runfork.RunForkContractFrontierAdmissionOwner,
		NonMutating:                  true,
		HistoricalExecutionSupported: false,
		ContractSelection: runfork.RunForkContractSelection{
			Mode:            "selected_contracts",
			ContractsRoot:   "/tmp/contracts",
			WorkflowName:    "workflow",
			WorkflowVersion: "v1",
		},
		FrontierEventCount: 1,
		FrontierEvents: []runfork.RunForkContractFrontierEvent{{
			SourceEventID: "source-event",
			EventName:     "work.begin",
			DerivedRecipients: []runfork.RunForkContractFrontierRecipient{
				testNodeFrontierRecipient("worker", "flow-a/worker", "selected_contracts"),
			},
		}},
	}
	routeAdmission := testSelectedContractRouteAdmission(frontier)
	routeTopology := testSelectedContractRouteTopologyFromAdmission(t, frontier, routeAdmission)
	recipientPlanning, err := BuildSelectedContractRecipientPlanning(SelectedContractRecipientPlanningRequest{
		Admission:      frontier,
		RouteAdmission: routeAdmission,
		RouteTopology:  routeTopology,
	})
	if err != nil {
		t.Fatalf("BuildSelectedContractRecipientPlanning: %v", err)
	}
	model, err := BuildSelectedContractExecutionModel(SelectedContractExecutionModelRequest{
		Admission:      frontier,
		RouteAdmission: routeAdmission,
		RouteTopology:  routeTopology,
	})
	if err != nil {
		t.Fatalf("BuildSelectedContractExecutionModel: %v", err)
	}
	readiness, err := BuildSelectedContractReadinessClassifier(SelectedContractReadinessClassifierRequest{
		Plan: runfork.RunForkPlan{
			ReplayResumeAdmission: runfork.RunForkReplayResumeAdmission{Owner: runfork.RunForkReplayResumeAdmissionOwner},
		},
		ContractFrontierAdmission: frontier,
		SelectedContractExecution: model,
	})
	if err != nil {
		t.Fatalf("BuildSelectedContractReadinessClassifier: %v", err)
	}
	selectedAdmission := testContractSwapSelectedExecutionAdmission(frontier.ContractSelection)
	selectedAdmission.RecipientPlanning.RecipientPlanEvents = []runfork.RunForkSelectedContractRecipientPlanEvent{{
		SourceEventID: "source-event",
		EventName:     "work.begin",
		Recipients: []runfork.RunForkContractFrontierRecipient{
			testNodeFrontierRecipient("worker", "flow-a/worker", "selected_contracts"),
		},
		Disposition: runfork.RunForkSelectedContractDispositionForkLocalTruth,
	}}
	routeRecovery := testContractSwapRouteRecovery(selectedAdmission)
	replayAdmission := runfork.RunForkReplayResumeAdmission{
		Owner:                    runfork.RunForkReplayResumeAdmissionOwner,
		DeliveryEventReplayReady: true,
		Dispositions: []runfork.RunForkReplayResumeDisposition{{
			Fact:        runfork.RunForkReplayResumeFactDeliveryPendingHistory,
			Disposition: runfork.RunForkReplayResumeDispositionForkReplay,
			Message:     "pending selected-contract source delivery can be replayed",
		}},
	}
	contractSwapAdmission, err := BuildContractSwapBootResumeAdmission(ContractSwapBootResumeAdmissionRequest{
		SelectedExecutionAdmission: selectedAdmission,
		ReplayResumeAdmission:      replayAdmission,
		RouteRecovery:              &routeRecovery,
	})
	if err != nil {
		t.Fatalf("BuildContractSwapBootResumeAdmission: %v", err)
	}
	historicalAdmission, err := BuildHistoricalReplayExecutionAdmission(HistoricalReplayExecutionAdmissionRequest{
		ReplayResumeAdmission:      replayAdmission,
		SelectedExecutionAdmission: selectedAdmission,
		ContractSwapAdmission:      contractSwapAdmission,
		RouteRecovery:              &routeRecovery,
	})
	if err != nil {
		t.Fatalf("BuildHistoricalReplayExecutionAdmission: %v", err)
	}
	historicalExecution, err := BuildHistoricalReplayExecution(HistoricalReplayExecutionRequest{
		Admission:             historicalAdmission,
		ReplayResumeAdmission: replayAdmission,
		PendingWork: []runfork.RunForkPendingWork{{
			EventID:        "source-event",
			EventName:      "work.begin",
			DeliveryID:     "source-delivery-1",
			SubscriberType: "agent",
			SubscriberID:   "source-agent",
			Status:         "pending",
			Classification: runfork.RunForkPendingClassificationPending,
		}},
	})
	if err != nil {
		t.Fatalf("BuildHistoricalReplayExecution: %v", err)
	}
	contractSwapExecution, err := BuildHistoricalReplayContractSwapBootResumeExecution(HistoricalReplayContractSwapBootResumeRequest{
		SelectedExecutionAdmission: selectedAdmission,
		ContractSwapAdmission:      contractSwapAdmission,
		HistoricalReplayAdmission:  historicalAdmission,
		HistoricalReplayExecution:  historicalExecution,
		RouteRecovery:              &routeRecovery,
	})
	if err != nil {
		t.Fatalf("BuildHistoricalReplayContractSwapBootResumeExecution: %v", err)
	}

	const (
		routeAuthorityClass           = "route_authority"
		separateOwnerClass            = "separate_owner"
		carrierReadinessConsumerClass = "carrier_readiness_consumer"
		diagnosticObserverClass       = "diagnostic_observer"
	)
	rows := []struct {
		consumer       string
		owner          string
		classification string
		consumedOwners []string
	}{
		{
			consumer:       "internal/runtime/runforkadmission.AdmitSelectedContractRouteHistory",
			owner:          routeAdmission.Owner,
			classification: separateOwnerClass,
		},
		{
			consumer:       "internal/runtime/runforkexecution.BuildSelectedContractRouteTopology",
			owner:          routeTopology.Owner,
			classification: separateOwnerClass,
			consumedOwners: []string{routeTopology.RouteAdmissionOwner},
		},
		{
			consumer:       "internal/runtime/runforkexecution.BuildSelectedContractRecipientPlanning",
			owner:          recipientPlanning.Owner,
			classification: separateOwnerClass,
			consumedOwners: []string{recipientPlanning.RouteAdmissionOwner, recipientPlanning.RouteTopologyOwner},
		},
		{
			consumer:       "selected_contract_recipient_planning.eventbus_publish_recipient_guard",
			owner:          runForkBoundaryOwner(t, recipientPlanning.RequiredConsumers, "eventbus_publish_recipient_guard"),
			classification: carrierReadinessConsumerClass,
		},
		{
			consumer:       "internal/runtime/runforkexecution.BuildSelectedContractExecutionModel",
			owner:          model.Owner,
			classification: carrierReadinessConsumerClass,
			consumedOwners: []string{model.RouteTopology.Owner, model.RouteTopology.RouteAdmissionOwner},
		},
		{
			consumer:       "internal/runtime/runforkexecution.BuildSelectedContractExecutionAdmission",
			owner:          selectedAdmission.Owner,
			classification: carrierReadinessConsumerClass,
			consumedOwners: []string{
				selectedAdmission.ContractBindingOwner,
				selectedAdmission.ExecutionModelOwner,
				selectedAdmission.RouteTopology.Owner,
				selectedAdmission.RecipientPlanning.Owner,
			},
		},
		{
			consumer:       "store.RecordRunForkSelectedContractRouteRecovery",
			owner:          routeRecovery.Owner,
			classification: separateOwnerClass,
			consumedOwners: []string{routeRecovery.RouteTopologyOwner, routeRecovery.RecipientPlanningOwner},
		},
		{
			consumer:       "internal/runtime/runforkexecution.RecoverSelectedContractRouteTruth",
			owner:          routeRecovery.RuntimeRecoveryOwner,
			classification: carrierReadinessConsumerClass,
			consumedOwners: []string{routeRecovery.Owner, routeRecovery.RouteTopologyOwner, routeRecovery.RecipientPlanningOwner},
		},
		{
			consumer:       "internal/runtime/runforkexecution.BuildSelectedContractReadinessClassifier",
			owner:          readiness.Owner,
			classification: diagnosticObserverClass,
			consumedOwners: []string{readiness.RouteTopologyOwner, readiness.RecipientPlanningOwner, readiness.SelectedExecutionModelOwner},
		},
		{
			consumer:       "internal/runtime/runforkexecution.ActivateSelectedContractRunFork",
			owner:          runfork.RunForkSelectedContractExecutionActivationGateOwner,
			classification: carrierReadinessConsumerClass,
		},
		{
			consumer:       "internal/runtime/runforkexecution.selectedContractForkLocalRuntimeContainer",
			owner:          runfork.RunForkSelectedContractForkLocalRuntimeContainerOwner,
			classification: carrierReadinessConsumerClass,
		},
		{
			consumer:       "internal/runtime/runforkexecution.RequireSelectedContractAgentDeliveryMaterialization",
			owner:          runfork.RunForkSelectedContractAuthoritativeAgentDeliveryMaterializationOwner,
			classification: carrierReadinessConsumerClass,
		},
		{
			consumer:       "runtime.run_fork.selected_contract_execution",
			owner:          model.FutureExecutionOwner,
			classification: carrierReadinessConsumerClass,
			consumedOwners: []string{model.Owner, model.RouteTopology.Owner, model.RouteTopology.RouteAdmissionOwner},
		},
		{
			consumer:       "internal/runtime/runforkexecution.BuildContractSwapBootResumeAdmission",
			owner:          contractSwapAdmission.Owner,
			classification: carrierReadinessConsumerClass,
			consumedOwners: []string{
				contractSwapAdmission.SelectedExecutionAdmissionOwner,
				contractSwapAdmission.ReplayResumeAdmissionOwner,
				contractSwapAdmission.RouteTopologyOwner,
				contractSwapAdmission.RouteRecoveryOwner,
				contractSwapAdmission.RuntimeRouteRecoveryOwner,
				contractSwapAdmission.RecipientPlanningOwner,
			},
		},
		{
			consumer:       "internal/runtime/runforkexecution.BuildHistoricalReplayExecutionAdmission",
			owner:          historicalAdmission.Owner,
			classification: carrierReadinessConsumerClass,
			consumedOwners: []string{
				historicalAdmission.SelectedExecutionAdmissionOwner,
				historicalAdmission.ContractSwapAdmissionOwner,
				historicalAdmission.RouteTopologyOwner,
				historicalAdmission.RouteRecoveryOwner,
				historicalAdmission.RuntimeRouteRecoveryOwner,
				historicalAdmission.RecipientPlanningOwner,
			},
		},
		{
			consumer:       "internal/runtime/runforkexecution.BuildHistoricalReplayContractSwapBootResumeExecution",
			owner:          contractSwapExecution.Owner,
			classification: carrierReadinessConsumerClass,
			consumedOwners: []string{
				contractSwapExecution.ParentHistoricalReplayExecutionOwner,
				contractSwapExecution.HistoricalReplayExecutionAdmissionOwner,
				contractSwapExecution.ContractSwapAdmissionOwner,
				contractSwapExecution.SelectedExecutionAdmissionOwner,
				contractSwapExecution.RouteTopologyOwner,
				contractSwapExecution.RouteRecoveryOwner,
				contractSwapExecution.RuntimeRouteRecoveryOwner,
				contractSwapExecution.RecipientPlanningOwner,
			},
		},
	}
	const expectedRunForkExecutionRouteConsumerRows = 16
	if len(rows) != expectedRunForkExecutionRouteConsumerRows {
		t.Fatalf("classification rows = %d, want %d current runforkexecution route/readiness consumers", len(rows), expectedRunForkExecutionRouteConsumerRows)
	}
	allowed := map[string]struct{}{
		routeAuthorityClass:           {},
		separateOwnerClass:            {},
		carrierReadinessConsumerClass: {},
		diagnosticObserverClass:       {},
	}
	for _, row := range rows {
		if strings.TrimSpace(row.owner) == "" {
			t.Fatalf("%s has empty owner in classification row %#v", row.consumer, row)
		}
		if _, ok := allowed[row.classification]; !ok {
			t.Fatalf("%s classification = %q, want supported classification", row.consumer, row.classification)
		}
		if row.classification == routeAuthorityClass {
			t.Fatalf("%s incorrectly classified as live EventBus route authority", row.consumer)
		}
		for _, owner := range row.consumedOwners {
			if strings.TrimSpace(owner) == "" {
				t.Fatalf("%s has empty consumed owner in classification row %#v", row.consumer, row)
			}
		}
	}

	if !model.NonMutating || model.ExecutionSupported {
		t.Fatalf("selected execution model mutation flags = non_mutating:%v execution:%v", model.NonMutating, model.ExecutionSupported)
	}
	if !recipientPlanning.NonMutating || recipientPlanning.DeliveryWritesSupported {
		t.Fatalf("recipient planning mutation flags = non_mutating:%v delivery_writes:%v", recipientPlanning.NonMutating, recipientPlanning.DeliveryWritesSupported)
	}
	if readiness.RouteTopologyOwner != runfork.RunForkSelectedContractRouteTopologyOwner ||
		readiness.RecipientPlanningOwner != runfork.RunForkSelectedContractRecipientPlanningOwner ||
		readiness.SelectedExecutionModelOwner != runfork.RunForkSelectedContractExecutionModelOwner {
		t.Fatalf("readiness owner consumption = %#v", readiness)
	}
	if contractSwapAdmission.RouteTopologyOwner != runfork.RunForkSelectedContractRouteTopologyOwner ||
		contractSwapAdmission.RouteRecoveryOwner != runfork.RunForkSelectedContractRoutePersistenceOwner ||
		contractSwapAdmission.RuntimeRouteRecoveryOwner != runfork.RunForkSelectedContractRouteRecoveryOwner ||
		contractSwapAdmission.RecipientPlanningOwner != runfork.RunForkSelectedContractRecipientPlanningOwner {
		t.Fatalf("contract-swap admission route owner consumption = %#v", contractSwapAdmission)
	}
	if historicalAdmission.RouteTopologyOwner != runfork.RunForkSelectedContractRouteTopologyOwner ||
		historicalAdmission.RouteRecoveryOwner != runfork.RunForkSelectedContractRoutePersistenceOwner ||
		historicalAdmission.RuntimeRouteRecoveryOwner != runfork.RunForkSelectedContractRouteRecoveryOwner ||
		historicalAdmission.RecipientPlanningOwner != runfork.RunForkSelectedContractRecipientPlanningOwner {
		t.Fatalf("historical replay admission route owner consumption = %#v", historicalAdmission)
	}
	if contractSwapExecution.RouteTopologyOwner != runfork.RunForkSelectedContractRouteTopologyOwner ||
		contractSwapExecution.RouteRecoveryOwner != runfork.RunForkSelectedContractRoutePersistenceOwner ||
		contractSwapExecution.RuntimeRouteRecoveryOwner != runfork.RunForkSelectedContractRouteRecoveryOwner ||
		contractSwapExecution.RecipientPlanningOwner != runfork.RunForkSelectedContractRecipientPlanningOwner {
		t.Fatalf("contract-swap execution route owner consumption = %#v", contractSwapExecution)
	}
	if !executionBoundaryHas(recipientPlanning.InvalidPaths, "delivery_planner_as_canonical_owner", runfork.RunForkSelectedContractDispositionInvalid) {
		t.Fatalf("recipient planning invalid paths = %#v, want generic delivery planner invalid as canonical owner", recipientPlanning.InvalidPaths)
	}
}

func TestBuildSelectedContractReadinessClassifierRejectsLocalExecutionModel(t *testing.T) {
	_, err := BuildSelectedContractReadinessClassifier(SelectedContractReadinessClassifierRequest{
		Plan: runfork.RunForkPlan{
			ReplayResumeAdmission: runfork.RunForkReplayResumeAdmission{Owner: runfork.RunForkReplayResumeAdmissionOwner},
		},
		ContractFrontierAdmission: runfork.RunForkContractFrontierAdmission{
			Owner:       runfork.RunForkContractFrontierAdmissionOwner,
			NonMutating: true,
		},
		SelectedContractExecution: runfork.RunForkSelectedContractExecution{
			Owner:       "cmd.swarm.local_readiness",
			NonMutating: true,
		},
	})
	if err == nil || !strings.Contains(err.Error(), runfork.RunForkSelectedContractExecutionModelOwner) {
		t.Fatalf("error = %v, want canonical execution model owner failure", err)
	}
}

func assertReadinessFactSet(t *testing.T, facts []runfork.RunForkSelectedContractReadinessFact) {
	t.Helper()
	required := []string{
		runfork.RunForkSelectedContractReadinessFactSourceEvents,
		runfork.RunForkSelectedContractReadinessFactForkEvents,
		runfork.RunForkSelectedContractReadinessFactSourceDeliveries,
		runfork.RunForkSelectedContractReadinessFactForkDeliveries,
		runfork.RunForkSelectedContractReadinessFactSelectedRecipientsRoutes,
		runfork.RunForkSelectedContractReadinessFactTimers,
		runfork.RunForkSelectedContractReadinessFactSessions,
		runfork.RunForkSelectedContractReadinessFactTurns,
		runfork.RunForkSelectedContractReadinessFactAudits,
		runfork.RunForkSelectedContractReadinessFactCommittedReplayScopeMarkers,
		runfork.RunForkSelectedContractReadinessFactPlatformRuntimeDiagnostics,
		runfork.RunForkSelectedContractReadinessFactReceipts,
		runfork.RunForkSelectedContractReadinessFactDeadLetters,
		runfork.RunForkSelectedContractReadinessFactRetryIdempotency,
		runfork.RunForkSelectedContractReadinessFactEmittedFollowUps,
		runfork.RunForkSelectedContractReadinessFactSourcePostTFacts,
		runfork.RunForkSelectedContractReadinessFactCurrentStateSnapshots,
		runfork.RunForkSelectedContractReadinessFactNonAgentNodeSystemWork,
		runfork.RunForkSelectedContractReadinessFactRestartRecovery,
		runfork.RunForkSelectedContractReadinessFactOperatorConsumers,
	}
	seen := map[string]struct{}{}
	for _, fact := range facts {
		if _, ok := seen[fact.Fact]; ok {
			t.Fatalf("readiness fact %s repeated in %#v", fact.Fact, facts)
		}
		seen[fact.Fact] = struct{}{}
	}
	for _, fact := range required {
		if _, ok := seen[fact]; !ok {
			t.Fatalf("readiness fact %s missing from %#v", fact, facts)
		}
	}
}

func assertReadinessFact(t *testing.T, facts []runfork.RunForkSelectedContractReadinessFact, fact, disposition, owner string) {
	t.Helper()
	for _, item := range facts {
		if item.Fact == fact {
			if item.Disposition != disposition || item.Owner != owner {
				t.Fatalf("readiness fact %s = %#v, want disposition=%s owner=%s", fact, item, disposition, owner)
			}
			return
		}
	}
	t.Fatalf("readiness fact %s missing from %#v", fact, facts)
}

func runForkBoundaryOwner(t *testing.T, items []runfork.RunForkSelectedContractExecutionBoundary, concept string) string {
	t.Helper()
	for _, item := range items {
		if item.Concept == concept {
			return item.Owner
		}
	}
	t.Fatalf("boundary %s missing from %#v", concept, items)
	return ""
}
