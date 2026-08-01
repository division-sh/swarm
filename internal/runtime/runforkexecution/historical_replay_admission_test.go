package runforkexecution

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func TestBuildHistoricalReplayExecutionAdmissionClassifiesFactsAndConsumesOwners(t *testing.T) {
	selection := testContractSwapSelection()
	selectedAdmission := testContractSwapSelectedExecutionAdmission(selection)
	routeRecovery := testContractSwapRouteRecovery(selectedAdmission)
	replayAdmission := runfork.RunForkReplayResumeAdmission{
		Owner:                    runfork.RunForkReplayResumeAdmissionOwner,
		ReplayResumeFactsPresent: true,
		Dispositions: []runfork.RunForkReplayResumeDisposition{
			{
				Fact:        runfork.RunForkReplayResumeFactDeliveryDeadLetterHistory,
				Disposition: runfork.RunForkReplayResumeDispositionFailClosedBlocker,
				BlockerCode: runfork.RunForkBlockerDeliveryHistoryUnproven,
				Message:     "dead-letter delivery history is not replayable",
			},
			{
				Fact:        runfork.RunForkReplayResumeFactDeliveryPendingHistory,
				Disposition: runfork.RunForkReplayResumeDispositionFailClosedBlocker,
				BlockerCode: runfork.RunForkBlockerNonAgentDeliveryReplayUnsupported,
				Message:     "non-agent work is not replayable",
			},
			{
				Fact:        runfork.RunForkReplayResumeFactTimerHistory,
				Disposition: runfork.RunForkReplayResumeDispositionFailClosedBlocker,
				BlockerCode: runfork.RunForkBlockerTimerHistoryUnproven,
				Message:     "timer history is unproven",
			},
			{
				Fact:        runfork.RunForkReplayResumeFactRouteHistory,
				Disposition: runfork.RunForkReplayResumeDispositionFailClosedBlocker,
				BlockerCode: runfork.RunForkBlockerFlowRouteHistoryUnproven,
				Message:     "route history is unproven",
			},
			{
				Fact:        runfork.RunForkReplayResumeFactSessionHistory,
				Disposition: runfork.RunForkReplayResumeDispositionFailClosedBlocker,
				BlockerCode: runfork.RunForkBlockerSessionHistoryUnproven,
				Message:     "session history is unproven",
			},
			{
				Fact:        runfork.RunForkReplayResumeFactActiveTurnHistory,
				Disposition: runfork.RunForkReplayResumeDispositionFailClosedBlocker,
				BlockerCode: runfork.RunForkBlockerActiveTurnHistoryUnproven,
				Message:     "turn history is unproven",
			},
			{
				Fact:        runfork.RunForkReplayResumeFactConversationAuditHistory,
				Disposition: runfork.RunForkReplayResumeDispositionFailClosedBlocker,
				BlockerCode: runfork.RunForkBlockerConversationAuditUnproven,
				Message:     "audit history is unproven",
			},
			{
				Fact:        runfork.RunForkReplayResumeFactSourceAdvanced,
				Disposition: runfork.RunForkReplayResumeDispositionFailClosedBlocker,
				BlockerCode: "source_events_advanced_after_fork_point",
				Message:     "source advanced after fork point",
			},
		},
		UnsupportedBlockers: []runfork.RunForkUnsupportedBlocker{
			{Code: runfork.RunForkBlockerDeliveryHistoryUnproven, Message: "delivery history is not replayable"},
			{Code: runfork.RunForkBlockerNonAgentDeliveryReplayUnsupported, Message: "non-agent work is not replayable"},
			{Code: runfork.RunForkBlockerTimerHistoryUnproven, Message: "timer history is unproven"},
			{Code: runfork.RunForkBlockerFlowRouteHistoryUnproven, Message: "route history is unproven"},
			{Code: runfork.RunForkBlockerSessionHistoryUnproven, Message: "session history is unproven"},
			{Code: runfork.RunForkBlockerActiveTurnHistoryUnproven, Message: "turn history is unproven"},
			{Code: runfork.RunForkBlockerConversationAuditUnproven, Message: "audit history is unproven"},
			{Code: "source_events_advanced_after_fork_point", Message: "source advanced after fork point"},
		},
	}
	contractSwapAdmission, err := BuildContractSwapBootResumeAdmission(ContractSwapBootResumeAdmissionRequest{
		SelectedExecutionAdmission: selectedAdmission,
		ReplayResumeAdmission:      replayAdmission,
		RouteRecovery:              &routeRecovery,
	})
	if err != nil {
		t.Fatalf("BuildContractSwapBootResumeAdmission: %v", err)
	}

	admission, err := BuildHistoricalReplayExecutionAdmission(HistoricalReplayExecutionAdmissionRequest{
		ReplayResumeAdmission:      replayAdmission,
		SelectedExecutionAdmission: selectedAdmission,
		ContractSwapAdmission:      contractSwapAdmission,
		RouteRecovery:              &routeRecovery,
	})
	if err != nil {
		t.Fatalf("BuildHistoricalReplayExecutionAdmission: %v", err)
	}
	if admission.Owner != runfork.RunForkHistoricalReplayExecutionAdmissionOwner ||
		!admission.NonMutating ||
		admission.ExecutionSupported ||
		admission.FutureExecutionOwner != runfork.RunForkHistoricalReplayExecutionOwner {
		t.Fatalf("admission owner/support = %#v", admission)
	}
	if admission.ReplayResumeAdmissionOwner != runfork.RunForkReplayResumeAdmissionOwner ||
		admission.SelectedExecutionAdmissionOwner != runfork.RunForkSelectedContractExecutionAdmissionOwner ||
		admission.SelectedBindingOwner != runfork.RunForkSelectedContractBindingOwner ||
		admission.RouteTopologyOwner != runfork.RunForkSelectedContractRouteTopologyOwner ||
		admission.RecipientPlanningOwner != runfork.RunForkSelectedContractRecipientPlanningOwner ||
		admission.RouteRecoveryOwner != runfork.RunForkSelectedContractRoutePersistenceOwner ||
		admission.RuntimeRouteRecoveryOwner != runfork.RunForkSelectedContractRouteRecoveryOwner ||
		admission.ContractSwapAdmissionOwner != runfork.RunForkContractSwapBootResumeAdmissionOwner {
		t.Fatalf("owner consumption = %#v", admission)
	}
	assertHistoricalReplayFactSet(t, admission.FactAdmissions)
	assertHistoricalReplayAdmission(t, admission.FactAdmissions, runfork.RunForkHistoricalReplayFactSourceEvents, runfork.RunForkHistoricalReplayAdmissionLineageOnlyEvidence, "")
	assertHistoricalReplayAdmission(t, admission.FactAdmissions, runfork.RunForkHistoricalReplayFactReceipts, runfork.RunForkHistoricalReplayAdmissionLineageOnlyEvidence, "")
	assertHistoricalReplayAdmission(t, admission.FactAdmissions, runfork.RunForkHistoricalReplayFactEventDeliveries, runfork.RunForkHistoricalReplayAdmissionFailClosedBlocker, runfork.RunForkBlockerDeliveryHistoryUnproven)
	assertHistoricalReplayAdmission(t, admission.FactAdmissions, runfork.RunForkHistoricalReplayFactDeadLetters, runfork.RunForkHistoricalReplayAdmissionFailClosedBlocker, runfork.RunForkBlockerDeliveryHistoryUnproven)
	assertHistoricalReplayAdmission(t, admission.FactAdmissions, runfork.RunForkHistoricalReplayFactTimers, runfork.RunForkHistoricalReplayAdmissionFailClosedBlocker, runfork.RunForkBlockerTimerHistoryUnproven)
	assertHistoricalReplayAdmission(t, admission.FactAdmissions, runfork.RunForkHistoricalReplayFactRoutes, runfork.RunForkHistoricalReplayAdmissionFailClosedBlocker, runfork.RunForkBlockerFlowRouteHistoryUnproven)
	assertHistoricalReplayAdmission(t, admission.FactAdmissions, runfork.RunForkHistoricalReplayFactSessions, runfork.RunForkHistoricalReplayAdmissionFailClosedBlocker, runfork.RunForkBlockerSessionHistoryUnproven)
	assertHistoricalReplayAdmission(t, admission.FactAdmissions, runfork.RunForkHistoricalReplayFactTurns, runfork.RunForkHistoricalReplayAdmissionFailClosedBlocker, runfork.RunForkBlockerActiveTurnHistoryUnproven)
	assertHistoricalReplayAdmission(t, admission.FactAdmissions, runfork.RunForkHistoricalReplayFactAudits, runfork.RunForkHistoricalReplayAdmissionFailClosedBlocker, runfork.RunForkBlockerConversationAuditUnproven)
	assertHistoricalReplayAdmission(t, admission.FactAdmissions, runfork.RunForkHistoricalReplayFactNonAgentNodeSystemWork, runfork.RunForkHistoricalReplayAdmissionFailClosedBlocker, runfork.RunForkBlockerNonAgentDeliveryReplayUnsupported)
	assertHistoricalReplayAdmission(t, admission.FactAdmissions, runfork.RunForkHistoricalReplayFactSourceAdvancedPostTFacts, runfork.RunForkHistoricalReplayAdmissionFailClosedBlocker, "source_events_advanced_after_fork_point")
	assertHistoricalReplayAdmission(t, admission.FactAdmissions, runfork.RunForkHistoricalReplayFactRetryIdempotency, runfork.RunForkHistoricalReplayAdmissionSplitSibling, "")
	assertHistoricalReplayAdmission(t, admission.FactAdmissions, runfork.RunForkHistoricalReplayFactEmittedFollowUps, runfork.RunForkHistoricalReplayAdmissionSplitSibling, "")
	assertHistoricalReplayAdmission(t, admission.FactAdmissions, runfork.RunForkHistoricalReplayFactRuntimeRestartRecovery, runfork.RunForkHistoricalReplayAdmissionSplitSibling, "")
	assertHistoricalReplayAdmission(t, admission.FactAdmissions, runfork.RunForkHistoricalReplayFactCLIApiDashboardOperator, runfork.RunForkHistoricalReplayAdmissionSplitSibling, "")
	if !unsupportedBlockerHas(admission.UnsupportedBlockers, runfork.RunForkBlockerHistoricalReplayExecutionAdmissionNonMutating) ||
		!unsupportedBlockerHas(admission.UnsupportedBlockers, runfork.RunForkBlockerContractSwapBootResumeAdmissionNonMutating) ||
		!unsupportedBlockerHas(admission.UnsupportedBlockers, runfork.RunForkBlockerTimerHistoryUnproven) {
		t.Fatalf("unsupported blockers = %#v", admission.UnsupportedBlockers)
	}
	if !executionBoundaryHas(admission.InvalidPaths, "source_event_copy", runfork.RunForkSelectedContractDispositionInvalid) ||
		!executionBoundaryHas(admission.InvalidPaths, "selected_frontier_execution_as_full_replay", runfork.RunForkSelectedContractDispositionInvalid) ||
		!executionBoundaryHas(admission.BlockedSiblings, "mutating_historical_replay_execution", runfork.RunForkSelectedContractDispositionBlockedSibling) {
		t.Fatalf("boundaries invalid=%#v blocked=%#v", admission.InvalidPaths, admission.BlockedSiblings)
	}
}

func TestBuildHistoricalReplayExecutionAdmissionReportsReplayablePrimitiveWithoutMutation(t *testing.T) {
	selectedAdmission := testContractSwapSelectedExecutionAdmission(testContractSwapSelection())
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

	admission, err := BuildHistoricalReplayExecutionAdmission(HistoricalReplayExecutionAdmissionRequest{
		ReplayResumeAdmission:      replayAdmission,
		SelectedExecutionAdmission: selectedAdmission,
		ContractSwapAdmission:      contractSwapAdmission,
		RouteRecovery:              &routeRecovery,
	})
	if err != nil {
		t.Fatalf("BuildHistoricalReplayExecutionAdmission: %v", err)
	}
	assertHistoricalReplayFactSet(t, admission.FactAdmissions)
	assertHistoricalReplayAdmission(t, admission.FactAdmissions, runfork.RunForkHistoricalReplayFactEventDeliveries, runfork.RunForkHistoricalReplayAdmissionExecutableForkWork, "")
	if !admission.NonMutating || admission.ExecutionSupported {
		t.Fatalf("admission mutation flags = %#v", admission)
	}
	if !unsupportedBlockerHas(admission.UnsupportedBlockers, runfork.RunForkBlockerHistoricalReplayExecutionAdmissionNonMutating) {
		t.Fatalf("unsupported blockers = %#v, want non-mutating blocker", admission.UnsupportedBlockers)
	}
}

func TestBuildHistoricalReplayExecutionAdmissionRejectsStaleTimerReconstructionDisposition(t *testing.T) {
	selectedAdmission := testContractSwapSelectedExecutionAdmission(testContractSwapSelection())
	routeRecovery := testContractSwapRouteRecovery(selectedAdmission)
	replayAdmission := runfork.RunForkReplayResumeAdmission{
		Owner:                    runfork.RunForkReplayResumeAdmissionOwner,
		ReplayResumeFactsPresent: true,
		Dispositions: []runfork.RunForkReplayResumeDisposition{{
			Fact:           runfork.RunForkReplayResumeFactTimerHistory,
			Disposition:    runfork.RunForkReplayResumeDispositionReconstruct,
			Classification: runfork.RunForkHistoricalReplayAdmissionReconstructedForkState,
			Message:        "stale timer reconstruction disposition",
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

	admission, err := BuildHistoricalReplayExecutionAdmission(HistoricalReplayExecutionAdmissionRequest{
		ReplayResumeAdmission:      replayAdmission,
		SelectedExecutionAdmission: selectedAdmission,
		ContractSwapAdmission:      contractSwapAdmission,
		RouteRecovery:              &routeRecovery,
	})
	if err != nil {
		t.Fatalf("BuildHistoricalReplayExecutionAdmission: %v", err)
	}
	timerAdmission, ok := historicalReplayFactAdmission(admission.FactAdmissions, runfork.RunForkHistoricalReplayFactTimers)
	if !ok {
		t.Fatalf("timer fact admission missing: %#v", admission.FactAdmissions)
	}
	if timerAdmission.Admission != runfork.RunForkHistoricalReplayAdmissionSplitSibling ||
		timerAdmission.Tracker != "#642" {
		t.Fatalf("timer admission = %#v", timerAdmission)
	}
}

func TestBuildHistoricalReplayExecutionAdmissionReportsSelectedContractConversationLineageOwner(t *testing.T) {
	selectedAdmission := testContractSwapSelectedExecutionAdmission(testContractSwapSelection())
	routeRecovery := testContractSwapRouteRecovery(selectedAdmission)
	replayAdmission := runfork.RunForkReplayResumeAdmission{
		Owner:                   runfork.RunForkReplayResumeAdmissionOwner,
		StateOnlyExecutionReady: true,
		Dispositions: []runfork.RunForkReplayResumeDisposition{
			{
				Fact:        runfork.RunForkReplayResumeFactSessionHistory,
				Disposition: runfork.RunForkReplayResumeDispositionLineageOnly,
				Owner:       runfork.RunForkSelectedContractSessionTurnAuditLineagePolicyOwner,
				Message:     "selected-contract session lineage/no-action evidence",
			},
			{
				Fact:        runfork.RunForkReplayResumeFactActiveTurnHistory,
				Disposition: runfork.RunForkReplayResumeDispositionLineageOnly,
				Owner:       runfork.RunForkSelectedContractSessionTurnAuditLineagePolicyOwner,
				Message:     "selected-contract turn lineage/no-action evidence",
			},
			{
				Fact:        runfork.RunForkReplayResumeFactConversationAuditHistory,
				Disposition: runfork.RunForkReplayResumeDispositionLineageOnly,
				Owner:       runfork.RunForkSelectedContractSessionTurnAuditLineagePolicyOwner,
				Message:     "selected-contract audit lineage/no-action evidence",
			},
		},
	}
	contractSwapAdmission, err := BuildContractSwapBootResumeAdmission(ContractSwapBootResumeAdmissionRequest{
		SelectedExecutionAdmission: selectedAdmission,
		ReplayResumeAdmission:      replayAdmission,
		RouteRecovery:              &routeRecovery,
	})
	if err != nil {
		t.Fatalf("BuildContractSwapBootResumeAdmission: %v", err)
	}

	admission, err := BuildHistoricalReplayExecutionAdmission(HistoricalReplayExecutionAdmissionRequest{
		ReplayResumeAdmission:      replayAdmission,
		SelectedExecutionAdmission: selectedAdmission,
		ContractSwapAdmission:      contractSwapAdmission,
		RouteRecovery:              &routeRecovery,
	})
	if err != nil {
		t.Fatalf("BuildHistoricalReplayExecutionAdmission: %v", err)
	}
	for _, fact := range []string{
		runfork.RunForkHistoricalReplayFactSessions,
		runfork.RunForkHistoricalReplayFactTurns,
		runfork.RunForkHistoricalReplayFactAudits,
	} {
		item, ok := historicalReplayFactAdmission(admission.FactAdmissions, fact)
		if !ok {
			t.Fatalf("missing fact admission for %s", fact)
		}
		if item.Admission != runfork.RunForkHistoricalReplayAdmissionLineageOnlyEvidence ||
			item.SourceOwner != runfork.RunForkSelectedContractSessionTurnAuditLineagePolicyOwner ||
			item.Tracker != "#661" {
			t.Fatalf("%s admission = %#v, want #661 lineage owner", fact, item)
		}
	}
}

func TestBuildHistoricalReplayExecutionAdmissionReportsSelectedContractSourceAdvancedConversationHistoryOwner(t *testing.T) {
	selectedAdmission := testContractSwapSelectedExecutionAdmission(testContractSwapSelection())
	routeRecovery := testContractSwapRouteRecovery(selectedAdmission)
	replayAdmission := runfork.RunForkReplayResumeAdmission{
		Owner:                   runfork.RunForkReplayResumeAdmissionOwner,
		StateOnlyExecutionReady: true,
		Dispositions: []runfork.RunForkReplayResumeDisposition{{
			Fact:           runfork.RunForkReplayResumeFactSourceAdvanced,
			Disposition:    runfork.RunForkReplayResumeDispositionLineageOnly,
			Owner:          runfork.RunForkSelectedContractSourceAdvancedConversationHistoryPolicyOwner,
			Classification: "source_turns_advanced_after_fork_point",
			Message:        "selected-contract source-advanced conversation-history lineage/no-action evidence",
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

	admission, err := BuildHistoricalReplayExecutionAdmission(HistoricalReplayExecutionAdmissionRequest{
		ReplayResumeAdmission:      replayAdmission,
		SelectedExecutionAdmission: selectedAdmission,
		ContractSwapAdmission:      contractSwapAdmission,
		RouteRecovery:              &routeRecovery,
	})
	if err != nil {
		t.Fatalf("BuildHistoricalReplayExecutionAdmission: %v", err)
	}
	item, ok := historicalReplayFactAdmission(admission.FactAdmissions, runfork.RunForkHistoricalReplayFactSourceAdvancedPostTFacts)
	if !ok {
		t.Fatalf("missing source-advanced admission: %#v", admission.FactAdmissions)
	}
	if item.Admission != runfork.RunForkHistoricalReplayAdmissionLineageOnlyEvidence ||
		item.SourceOwner != runfork.RunForkSelectedContractSourceAdvancedConversationHistoryPolicyOwner ||
		item.Tracker != "#671" {
		t.Fatalf("source-advanced admission = %#v, want #671 conversation-history lineage owner", item)
	}
}

func TestBuildHistoricalReplayExecutionAdmissionReportsSelectedContractReplayScopeMarkerOwner(t *testing.T) {
	selectedAdmission := testContractSwapSelectedExecutionAdmission(testContractSwapSelection())
	routeRecovery := testContractSwapRouteRecovery(selectedAdmission)
	replayAdmission := runfork.RunForkReplayResumeAdmission{
		Owner:                   runfork.RunForkReplayResumeAdmissionOwner,
		StateOnlyExecutionReady: true,
		Dispositions: []runfork.RunForkReplayResumeDisposition{{
			Fact:        runfork.RunForkReplayResumeFactCommittedReplayScope,
			Disposition: runfork.RunForkReplayResumeDispositionLineageOnly,
			Owner:       runfork.RunForkSelectedContractCommittedReplayScopeMarkerPolicyOwner,
			Message:     "selected-contract replay-scope marker lineage/no-action evidence",
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

	admission, err := BuildHistoricalReplayExecutionAdmission(HistoricalReplayExecutionAdmissionRequest{
		ReplayResumeAdmission:      replayAdmission,
		SelectedExecutionAdmission: selectedAdmission,
		ContractSwapAdmission:      contractSwapAdmission,
		RouteRecovery:              &routeRecovery,
	})
	if err != nil {
		t.Fatalf("BuildHistoricalReplayExecutionAdmission: %v", err)
	}
	item, ok := historicalReplayFactAdmission(admission.FactAdmissions, runfork.RunForkHistoricalReplayFactEventDeliveries)
	if !ok {
		t.Fatalf("missing event_deliveries admission: %#v", admission.FactAdmissions)
	}
	if item.Admission != runfork.RunForkHistoricalReplayAdmissionLineageOnlyEvidence ||
		item.SourceOwner != runfork.RunForkSelectedContractCommittedReplayScopeMarkerPolicyOwner ||
		item.Tracker != "#663" {
		t.Fatalf("event_deliveries admission = %#v, want #663 marker lineage owner", item)
	}
}

func TestBuildHistoricalReplayExecutionAdmissionReportsActiveSourceDeliveryCouplingOwner(t *testing.T) {
	selectedAdmission := testContractSwapSelectedExecutionAdmission(testContractSwapSelection())
	routeRecovery := testContractSwapRouteRecovery(selectedAdmission)
	replayAdmission := runfork.RunForkReplayResumeAdmission{
		Owner:                   runfork.RunForkReplayResumeAdmissionOwner,
		StateOnlyExecutionReady: true,
		Dispositions: []runfork.RunForkReplayResumeDisposition{{
			Fact:           runfork.RunForkReplayResumeFactDeliveryInProgressHistory,
			Disposition:    runfork.RunForkReplayResumeDispositionLineageOnly,
			Owner:          runfork.RunForkSelectedContractActiveSourceDeliveryConversationCouplingPolicyOwner,
			Classification: runfork.RunForkSelectedContractActiveSourceDeliveryConversationCouplingClassification,
			Message:        "selected-contract same-source active delivery lineage/no-action evidence",
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

	admission, err := BuildHistoricalReplayExecutionAdmission(HistoricalReplayExecutionAdmissionRequest{
		ReplayResumeAdmission:      replayAdmission,
		SelectedExecutionAdmission: selectedAdmission,
		ContractSwapAdmission:      contractSwapAdmission,
		RouteRecovery:              &routeRecovery,
	})
	if err != nil {
		t.Fatalf("BuildHistoricalReplayExecutionAdmission: %v", err)
	}
	item, ok := historicalReplayFactAdmission(admission.FactAdmissions, runfork.RunForkHistoricalReplayFactEventDeliveries)
	if !ok {
		t.Fatalf("missing event_deliveries admission: %#v", admission.FactAdmissions)
	}
	if item.Admission != runfork.RunForkHistoricalReplayAdmissionLineageOnlyEvidence ||
		item.SourceOwner != runfork.RunForkSelectedContractActiveSourceDeliveryConversationCouplingPolicyOwner ||
		item.Tracker != "#678" {
		t.Fatalf("event_deliveries admission = %#v, want #678 active source delivery owner", item)
	}
}

func TestBuildHistoricalReplayExecutionConsumesAdmissionForDeliveryEventReplayMutation(t *testing.T) {
	replayAdmission := runfork.RunForkReplayResumeAdmission{
		Owner:                    runfork.RunForkReplayResumeAdmissionOwner,
		DeliveryEventReplayReady: true,
		Dispositions: []runfork.RunForkReplayResumeDisposition{{
			Fact:        runfork.RunForkReplayResumeFactDeliveryPendingHistory,
			Disposition: runfork.RunForkReplayResumeDispositionForkReplay,
			Message:     "pending unstarted source delivery can be replayed",
		}},
	}
	admission, err := BuildHistoricalReplayDeliveryEventReplayAdmission(HistoricalReplayDeliveryEventReplayAdmissionRequest{
		ForkRunID:             "fork-run",
		SourceRunID:           "source-run",
		ForkEventID:           "fork-event",
		ReplayResumeAdmission: replayAdmission,
	})
	if err != nil {
		t.Fatalf("BuildHistoricalReplayDeliveryEventReplayAdmission: %v", err)
	}

	execution, err := BuildHistoricalReplayExecution(HistoricalReplayExecutionRequest{
		Admission:             admission,
		ReplayResumeAdmission: replayAdmission,
		PendingWork: []runfork.RunForkPendingWork{{
			EventID:        "source-event",
			DeliveryID:     "source-delivery",
			SubscriberType: "agent",
			SubscriberID:   "agent-a",
			Classification: runfork.RunForkPendingClassificationPending,
			Status:         "pending",
		}},
	})
	if err != nil {
		t.Fatalf("BuildHistoricalReplayExecution: %v", err)
	}
	if execution.Owner != runfork.RunForkHistoricalReplayExecutionOwner ||
		execution.AdmissionOwner != runfork.RunForkHistoricalReplayExecutionAdmissionOwner ||
		execution.ReplayResumeAdmissionOwner != runfork.RunForkReplayResumeAdmissionOwner ||
		!execution.DeliveryEventReplayReady {
		t.Fatalf("execution = %#v", execution)
	}
	if execution.EventDeliveriesAdmission.Fact != runfork.RunForkHistoricalReplayFactEventDeliveries ||
		execution.EventDeliveriesAdmission.Admission != runfork.RunForkHistoricalReplayAdmissionExecutableForkWork {
		t.Fatalf("event deliveries admission = %#v", execution.EventDeliveriesAdmission)
	}
	if execution.ClosureLevel != "canonical_owner_promotion_with_delivery_event_replay_ready_only" ||
		!execution.FullReplayUnsupported ||
		len(execution.FactAdmissions) != 15 ||
		len(execution.RequiredConsumers) == 0 {
		t.Fatalf("execution broad owner accounting = %#v", execution)
	}
	if len(execution.DeliveryEventReplayWork) != 1 ||
		execution.DeliveryEventReplayWork[0].SourceEventID != "source-event" ||
		execution.DeliveryEventReplayWork[0].SourceDeliveryID != "source-delivery" {
		t.Fatalf("owner-authorized delivery replay work = %#v", execution.DeliveryEventReplayWork)
	}
	if !executionBoundaryHas(execution.InvalidPaths, "source_delivery_copy", runfork.RunForkSelectedContractDispositionInvalid) ||
		!executionBoundaryHas(execution.InvalidPaths, "source_outcome_suppression", runfork.RunForkSelectedContractDispositionInvalid) ||
		!executionBoundaryHas(execution.BlockedSiblings, "timer_reconstruction", runfork.RunForkSelectedContractDispositionBlockedSibling) {
		t.Fatalf("execution boundaries invalid=%#v blocked=%#v", execution.InvalidPaths, execution.BlockedSiblings)
	}
}

func TestBuildHistoricalReplayExecutionRejectsTaxonomyReadyWithoutOwnerWork(t *testing.T) {
	replayAdmission := runfork.RunForkReplayResumeAdmission{
		Owner:                    runfork.RunForkReplayResumeAdmissionOwner,
		DeliveryEventReplayReady: true,
		Dispositions: []runfork.RunForkReplayResumeDisposition{{
			Fact:        runfork.RunForkReplayResumeFactDeliveryPendingHistory,
			Disposition: runfork.RunForkReplayResumeDispositionForkReplay,
			Message:     "pending unstarted source delivery can be replayed",
		}},
	}
	admission, err := BuildHistoricalReplayDeliveryEventReplayAdmission(HistoricalReplayDeliveryEventReplayAdmissionRequest{
		ForkRunID:             "fork-run",
		SourceRunID:           "source-run",
		ForkEventID:           "fork-event",
		ReplayResumeAdmission: replayAdmission,
	})
	if err != nil {
		t.Fatalf("BuildHistoricalReplayDeliveryEventReplayAdmission: %v", err)
	}

	_, err = BuildHistoricalReplayExecution(HistoricalReplayExecutionRequest{
		Admission:             admission,
		ReplayResumeAdmission: replayAdmission,
		PendingWork: []runfork.RunForkPendingWork{{
			EventID:        "source-event",
			DeliveryID:     "source-delivery",
			SubscriberType: "agent",
			SubscriberID:   "agent-a",
			Classification: runfork.RunForkPendingClassificationPending,
			Status:         "pending",
			RetryCount:     1,
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "owner-authorized delivery_event_replay_ready work") {
		t.Fatalf("error = %v, want owner-authorized work failure", err)
	}
}

func TestBuildHistoricalReplayExecutionRejectsNonExecutableAdmission(t *testing.T) {
	replayAdmission := runfork.RunForkReplayResumeAdmission{
		Owner:                    runfork.RunForkReplayResumeAdmissionOwner,
		DeliveryEventReplayReady: true,
	}
	_, err := BuildHistoricalReplayExecution(HistoricalReplayExecutionRequest{
		Admission: runfork.RunForkHistoricalReplayExecutionAdmission{
			Owner:                      runfork.RunForkHistoricalReplayExecutionAdmissionOwner,
			NonMutating:                true,
			FutureExecutionOwner:       runfork.RunForkHistoricalReplayExecutionOwner,
			ReplayResumeAdmissionOwner: runfork.RunForkReplayResumeAdmissionOwner,
			FactAdmissions: []runfork.RunForkHistoricalReplayFactAdmission{{
				Fact:      runfork.RunForkHistoricalReplayFactEventDeliveries,
				Admission: runfork.RunForkHistoricalReplayAdmissionLineageOnlyEvidence,
				Message:   "not executable",
			}},
		},
		ReplayResumeAdmission: replayAdmission,
	})
	if err == nil || !strings.Contains(err.Error(), "event_deliveries executable fork work") {
		t.Fatalf("error = %v, want executable event_deliveries admission failure", err)
	}
}

func TestBuildHistoricalReplayExecutionAdmissionRejectsNonCanonicalReplayTaxonomy(t *testing.T) {
	selectedAdmission := testContractSwapSelectedExecutionAdmission(testContractSwapSelection())
	contractSwapAdmission := runfork.RunForkContractSwapBootResumeAdmission{
		Owner:                           runfork.RunForkContractSwapBootResumeAdmissionOwner,
		NonMutating:                     true,
		SelectedExecutionAdmissionOwner: selectedAdmission.Owner,
		ReplayResumeAdmissionOwner:      "cmd.swarm.local_replay_helper",
		ForkRunID:                       selectedAdmission.ForkRunID,
		SourceRunID:                     selectedAdmission.SourceRunID,
		ForkEventID:                     selectedAdmission.ForkEventID,
	}

	_, err := BuildHistoricalReplayExecutionAdmission(HistoricalReplayExecutionAdmissionRequest{
		ReplayResumeAdmission:      runfork.RunForkReplayResumeAdmission{Owner: "cmd.swarm.local_replay_helper"},
		SelectedExecutionAdmission: selectedAdmission,
		ContractSwapAdmission:      contractSwapAdmission,
	})
	if err == nil || !strings.Contains(err.Error(), runfork.RunForkReplayResumeAdmissionOwner) {
		t.Fatalf("error = %v, want canonical replay admission owner failure", err)
	}
}

func assertHistoricalReplayFactSet(t *testing.T, admissions []runfork.RunForkHistoricalReplayFactAdmission) {
	t.Helper()
	required := []string{
		runfork.RunForkHistoricalReplayFactSourceEvents,
		runfork.RunForkHistoricalReplayFactEventDeliveries,
		runfork.RunForkHistoricalReplayFactReceipts,
		runfork.RunForkHistoricalReplayFactDeadLetters,
		runfork.RunForkHistoricalReplayFactRetryIdempotency,
		runfork.RunForkHistoricalReplayFactEmittedFollowUps,
		runfork.RunForkHistoricalReplayFactTimers,
		runfork.RunForkHistoricalReplayFactRoutes,
		runfork.RunForkHistoricalReplayFactSessions,
		runfork.RunForkHistoricalReplayFactTurns,
		runfork.RunForkHistoricalReplayFactAudits,
		runfork.RunForkHistoricalReplayFactNonAgentNodeSystemWork,
		runfork.RunForkHistoricalReplayFactSourceAdvancedPostTFacts,
		runfork.RunForkHistoricalReplayFactRuntimeRestartRecovery,
		runfork.RunForkHistoricalReplayFactCLIApiDashboardOperator,
	}
	if len(admissions) != len(required) {
		t.Fatalf("fact admission count = %d, want %d: %#v", len(admissions), len(required), admissions)
	}
	seen := map[string]int{}
	for _, admission := range admissions {
		seen[admission.Fact]++
	}
	for _, fact := range required {
		if seen[fact] != 1 {
			t.Fatalf("fact %s count = %d, want exactly once in %#v", fact, seen[fact], admissions)
		}
	}
}

func assertHistoricalReplayAdmission(t *testing.T, admissions []runfork.RunForkHistoricalReplayFactAdmission, fact, admission, blockerCode string) {
	t.Helper()
	for _, item := range admissions {
		if item.Fact != fact {
			continue
		}
		if item.Admission != admission || strings.TrimSpace(item.BlockerCode) != strings.TrimSpace(blockerCode) {
			t.Fatalf("fact %s admission = %#v, want admission=%s blocker=%s", fact, item, admission, blockerCode)
		}
		return
	}
	t.Fatalf("missing fact admission %s in %#v", fact, admissions)
}
