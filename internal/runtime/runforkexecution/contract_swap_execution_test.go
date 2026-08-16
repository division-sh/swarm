package runforkexecution

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/runfork"
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
	if execution.Owner != runfork.RunForkHistoricalReplayContractSwapBootResumeOwner ||
		execution.ParentHistoricalReplayExecutionOwner != runfork.RunForkHistoricalReplayExecutionOwner ||
		execution.HistoricalReplayExecutionAdmissionOwner != runfork.RunForkHistoricalReplayExecutionAdmissionOwner ||
		execution.ContractSwapAdmissionOwner != runfork.RunForkContractSwapBootResumeAdmissionOwner ||
		execution.SelectedExecutionAdmissionOwner != runfork.RunForkSelectedContractExecutionAdmissionOwner ||
		execution.RecipientPlanningOwner != runfork.RunForkSelectedContractRecipientPlanningOwner ||
		execution.RouteRecoveryOwner != runfork.RunForkSelectedContractRoutePersistenceOwner ||
		!execution.DeliveryEventReplayReady {
		t.Fatalf("owner consumption = %#v", execution)
	}
	if len(execution.ExecutableWork) != 1 ||
		execution.ExecutableWork[0].SourceEventID != "source-event" ||
		len(execution.ExecutableWork[0].SourceDeliveryIDs) != 2 ||
		len(execution.ExecutableWork[0].SelectedRecipients) != 1 ||
		execution.ExecutableWork[0].SelectedRecipients[0].Recipient.ID() != mustRunForkNode("flow-a", "selected-node").Key() {
		t.Fatalf("executable work = %#v, want selected recipient-plan work grouped by source event", execution.ExecutableWork)
	}
	if execution.ExecutableWork[0].SelectedRecipients[0].Recipient.ID() == "source-agent" {
		t.Fatalf("source subscriber leaked into selected recipient truth: %#v", execution.ExecutableWork[0])
	}
	if !executionBoundaryHas(execution.InvalidPaths, "source_subscriber_as_selected_recipient", runfork.RunForkSelectedContractDispositionInvalid) ||
		!executionBoundaryHas(execution.BlockedSiblings, "full_historical_replay_resume", runfork.RunForkSelectedContractDispositionBlockedSibling) {
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
	historicalExecution.FactAdmissions = append([]runfork.RunForkHistoricalReplayFactAdmission(nil), historicalExecution.FactAdmissions...)
	for i := range historicalExecution.FactAdmissions {
		if historicalExecution.FactAdmissions[i].Fact == runfork.RunForkHistoricalReplayFactTimers {
			historicalExecution.FactAdmissions[i].Admission = runfork.RunForkHistoricalReplayAdmissionFailClosedBlocker
			historicalExecution.FactAdmissions[i].BlockerCode = runfork.RunForkBlockerTimerHistoryUnproven
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
	if err == nil || !strings.Contains(err.Error(), runfork.RunForkHistoricalReplayFactTimers) {
		t.Fatalf("error = %v, want fail-closed timer sibling blocker", err)
	}
}

func testContractSwapExecutionInputs(t *testing.T) (
	runfork.RunForkSelectedContractExecutionAdmission,
	runfork.RunForkContractSwapBootResumeAdmission,
	runfork.RunForkHistoricalReplayExecutionAdmission,
	runfork.RunForkHistoricalReplayExecution,
	runfork.RunForkSelectedContractRouteRecovery,
) {
	t.Helper()
	selection := testContractSwapSelection()
	selectedAdmission := testContractSwapSelectedExecutionAdmission(selection)
	selectedAdmission.RecipientPlanning.RecipientPlanEvents = []runfork.RunForkSelectedContractRecipientPlanEvent{{
		SourceEventID: "source-event",
		EventName:     "work.begin",
		Recipients: []runfork.RunForkContractFrontierRecipient{
			testNodeFrontierRecipient("selected-node", "flow-a/selected-node", "selected_contracts"),
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
		PendingWork: []runfork.RunForkPendingWork{
			{
				EventID:        "source-event",
				EventName:      "work.begin",
				DeliveryID:     "source-delivery-1",
				SubscriberType: "agent",
				SubscriberID:   "source-agent",
				Status:         "pending",
				Classification: runfork.RunForkPendingClassificationPending,
			},
			{
				EventID:        "source-event",
				EventName:      "work.begin",
				DeliveryID:     "source-delivery-2",
				SubscriberType: "agent",
				SubscriberID:   "other-source-agent",
				Status:         "pending",
				Classification: runfork.RunForkPendingClassificationPending,
			},
		},
	})
	if err != nil {
		t.Fatalf("BuildHistoricalReplayExecution: %v", err)
	}
	return selectedAdmission, contractSwapAdmission, historicalAdmission, historicalExecution, routeRecovery
}
