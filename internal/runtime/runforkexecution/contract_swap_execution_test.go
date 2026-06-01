package runforkexecution

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/store"
)

func TestBuildHistoricalReplayContractSwapBootResumeConsumesOwnersAndSelectedRecipients(t *testing.T) {
	selectedAdmission, contractSwapAdmission, historicalAdmission, historicalExecution, routeRecovery := testContractSwapExecutionInputs(t)

	execution, err := BuildHistoricalReplayContractSwapBootResumeExecution(HistoricalReplayContractSwapBootResumeRequest{
		SelectedExecutionAdmission: selectedAdmission,
		ContractSwapAdmission:      contractSwapAdmission,
		HistoricalReplayAdmission:  historicalAdmission,
		HistoricalReplayExecution:  historicalExecution,
		RouteRecovery:              &routeRecovery,
	})
	if err != nil {
		t.Fatalf("BuildHistoricalReplayContractSwapBootResumeExecution: %v", err)
	}
	if execution.Owner != store.RunForkHistoricalReplayContractSwapBootResumeOwner ||
		execution.ParentHistoricalReplayExecutionOwner != store.RunForkHistoricalReplayExecutionOwner ||
		execution.HistoricalReplayExecutionAdmissionOwner != store.RunForkHistoricalReplayExecutionAdmissionOwner ||
		execution.ContractSwapAdmissionOwner != store.RunForkContractSwapBootResumeAdmissionOwner ||
		execution.SelectedExecutionAdmissionOwner != store.RunForkSelectedContractExecutionAdmissionOwner ||
		execution.RecipientPlanningOwner != store.RunForkSelectedContractRecipientPlanningOwner ||
		execution.RouteRecoveryOwner != store.RunForkSelectedContractRoutePersistenceOwner ||
		!execution.DeliveryEventReplayReady {
		t.Fatalf("owner consumption = %#v", execution)
	}
	if len(execution.ExecutableWork) != 1 ||
		execution.ExecutableWork[0].SourceEventID != "source-event" ||
		len(execution.ExecutableWork[0].SourceDeliveryIDs) != 2 ||
		len(execution.ExecutableWork[0].SelectedRecipients) != 1 ||
		execution.ExecutableWork[0].SelectedRecipients[0].SubscriberID != "selected-node" {
		t.Fatalf("executable work = %#v, want selected recipient-plan work grouped by source event", execution.ExecutableWork)
	}
	if execution.ExecutableWork[0].SelectedRecipients[0].SubscriberID == "source-agent" {
		t.Fatalf("source subscriber leaked into selected recipient truth: %#v", execution.ExecutableWork[0])
	}
	if !executionBoundaryHas(execution.InvalidPaths, "source_subscriber_as_selected_recipient", store.RunForkSelectedContractDispositionInvalid) ||
		!executionBoundaryHas(execution.BlockedSiblings, "full_historical_replay_resume", store.RunForkSelectedContractDispositionBlockedSibling) {
		t.Fatalf("boundaries invalid=%#v blocked=%#v", execution.InvalidPaths, execution.BlockedSiblings)
	}
}

func TestBuildHistoricalReplayContractSwapBootResumeRejectsSourceSubscriberFallback(t *testing.T) {
	selectedAdmission, contractSwapAdmission, historicalAdmission, historicalExecution, routeRecovery := testContractSwapExecutionInputs(t)
	selectedAdmission.RecipientPlanning.RecipientPlanEvents = nil

	_, err := BuildHistoricalReplayContractSwapBootResumeExecution(HistoricalReplayContractSwapBootResumeRequest{
		SelectedExecutionAdmission: selectedAdmission,
		ContractSwapAdmission:      contractSwapAdmission,
		HistoricalReplayAdmission:  historicalAdmission,
		HistoricalReplayExecution:  historicalExecution,
		RouteRecovery:              &routeRecovery,
	})
	if err == nil || !strings.Contains(err.Error(), "selected recipient plan") {
		t.Fatalf("error = %v, want selected recipient planning failure instead of source subscriber fallback", err)
	}
}

func TestBuildHistoricalReplayContractSwapBootResumeRejectsFailClosedSiblingFact(t *testing.T) {
	selectedAdmission, contractSwapAdmission, historicalAdmission, historicalExecution, routeRecovery := testContractSwapExecutionInputs(t)
	historicalExecution.FactAdmissions = append([]store.RunForkHistoricalReplayFactAdmission(nil), historicalExecution.FactAdmissions...)
	for i := range historicalExecution.FactAdmissions {
		if historicalExecution.FactAdmissions[i].Fact == store.RunForkHistoricalReplayFactTimers {
			historicalExecution.FactAdmissions[i].Admission = store.RunForkHistoricalReplayAdmissionFailClosedBlocker
			historicalExecution.FactAdmissions[i].BlockerCode = store.RunForkBlockerTimerHistoryUnproven
			break
		}
	}

	_, err := BuildHistoricalReplayContractSwapBootResumeExecution(HistoricalReplayContractSwapBootResumeRequest{
		SelectedExecutionAdmission: selectedAdmission,
		ContractSwapAdmission:      contractSwapAdmission,
		HistoricalReplayAdmission:  historicalAdmission,
		HistoricalReplayExecution:  historicalExecution,
		RouteRecovery:              &routeRecovery,
	})
	if err == nil || !strings.Contains(err.Error(), store.RunForkHistoricalReplayFactTimers) {
		t.Fatalf("error = %v, want fail-closed timer sibling blocker", err)
	}
}

func testContractSwapExecutionInputs(t *testing.T) (
	store.RunForkSelectedContractExecutionAdmission,
	store.RunForkContractSwapBootResumeAdmission,
	store.RunForkHistoricalReplayExecutionAdmission,
	store.RunForkHistoricalReplayExecution,
	store.RunForkSelectedContractRouteRecovery,
) {
	t.Helper()
	selection := testContractSwapSelection()
	selectedAdmission := testContractSwapSelectedExecutionAdmission(selection)
	selectedAdmission.RecipientPlanning.RecipientPlanEvents = []store.RunForkSelectedContractRecipientPlanEvent{{
		SourceEventID: "source-event",
		EventName:     "work.begin",
		Recipients: []store.RunForkContractFrontierRecipient{{
			SubscriberType: "node",
			SubscriberID:   "selected-node",
			Path:           "flow-a/selected-node",
			RouteSource:    "selected_contracts",
		}},
		Disposition: store.RunForkSelectedContractDispositionForkLocalTruth,
	}}
	routeRecovery := testContractSwapRouteRecovery(selectedAdmission)
	replayAdmission := store.RunForkReplayResumeAdmission{
		Owner:                    store.RunForkReplayResumeAdmissionOwner,
		DeliveryEventReplayReady: true,
		Dispositions: []store.RunForkReplayResumeDisposition{{
			Fact:        store.RunForkReplayResumeFactDeliveryPendingHistory,
			Disposition: store.RunForkReplayResumeDispositionForkReplay,
			Message:     "pending unstarted source delivery can be replayed",
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
		PendingWork: []store.RunForkPendingWork{
			{
				EventID:        "source-event",
				EventName:      "work.begin",
				DeliveryID:     "source-delivery-1",
				SubscriberType: "agent",
				SubscriberID:   "source-agent",
				Status:         "pending",
				Classification: store.RunForkPendingClassificationPending,
			},
			{
				EventID:        "source-event",
				EventName:      "work.begin",
				DeliveryID:     "source-delivery-2",
				SubscriberType: "agent",
				SubscriberID:   "other-source-agent",
				Status:         "pending",
				Classification: store.RunForkPendingClassificationPending,
			},
		},
	})
	if err != nil {
		t.Fatalf("BuildHistoricalReplayExecution: %v", err)
	}
	return selectedAdmission, contractSwapAdmission, historicalAdmission, historicalExecution, routeRecovery
}
