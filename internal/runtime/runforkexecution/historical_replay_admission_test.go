package runforkexecution

import (
	"strings"
	"testing"

	"swarm/internal/store"
)

func TestBuildHistoricalReplayExecutionAdmissionClassifiesFactsAndConsumesOwners(t *testing.T) {
	selection := testContractSwapSelection()
	selectedAdmission := testContractSwapSelectedExecutionAdmission(selection)
	routeRecovery := testContractSwapRouteRecovery(selectedAdmission)
	replayAdmission := store.RunForkReplayResumeAdmission{
		Owner:                    store.RunForkReplayResumeAdmissionOwner,
		HistoricalReplayRequired: true,
		Dispositions: []store.RunForkReplayResumeDisposition{
			{
				Fact:        store.RunForkReplayResumeFactDeliveryDeadLetterHistory,
				Disposition: store.RunForkReplayResumeDispositionFailClosedBlocker,
				BlockerCode: store.RunForkBlockerDeliveryHistoryUnproven,
				Message:     "dead-letter delivery history is not replayable",
			},
			{
				Fact:        store.RunForkReplayResumeFactDeliveryPendingHistory,
				Disposition: store.RunForkReplayResumeDispositionFailClosedBlocker,
				BlockerCode: store.RunForkBlockerNonAgentDeliveryReplayUnsupported,
				Message:     "non-agent work is not replayable",
			},
			{
				Fact:        store.RunForkReplayResumeFactTimerHistory,
				Disposition: store.RunForkReplayResumeDispositionFailClosedBlocker,
				BlockerCode: store.RunForkBlockerTimerHistoryUnproven,
				Message:     "timer history is unproven",
			},
			{
				Fact:        store.RunForkReplayResumeFactRouteHistory,
				Disposition: store.RunForkReplayResumeDispositionFailClosedBlocker,
				BlockerCode: store.RunForkBlockerFlowRouteHistoryUnproven,
				Message:     "route history is unproven",
			},
			{
				Fact:        store.RunForkReplayResumeFactSessionHistory,
				Disposition: store.RunForkReplayResumeDispositionFailClosedBlocker,
				BlockerCode: store.RunForkBlockerSessionHistoryUnproven,
				Message:     "session history is unproven",
			},
			{
				Fact:        store.RunForkReplayResumeFactActiveTurnHistory,
				Disposition: store.RunForkReplayResumeDispositionFailClosedBlocker,
				BlockerCode: store.RunForkBlockerActiveTurnHistoryUnproven,
				Message:     "turn history is unproven",
			},
			{
				Fact:        store.RunForkReplayResumeFactConversationAuditHistory,
				Disposition: store.RunForkReplayResumeDispositionFailClosedBlocker,
				BlockerCode: store.RunForkBlockerConversationAuditUnproven,
				Message:     "audit history is unproven",
			},
			{
				Fact:        store.RunForkReplayResumeFactSourceAdvanced,
				Disposition: store.RunForkReplayResumeDispositionFailClosedBlocker,
				BlockerCode: "source_events_advanced_after_fork_point",
				Message:     "source advanced after fork point",
			},
		},
		UnsupportedBlockers: []store.RunForkUnsupportedBlocker{
			{Code: store.RunForkBlockerDeliveryHistoryUnproven, Message: "delivery history is not replayable"},
			{Code: store.RunForkBlockerNonAgentDeliveryReplayUnsupported, Message: "non-agent work is not replayable"},
			{Code: store.RunForkBlockerTimerHistoryUnproven, Message: "timer history is unproven"},
			{Code: store.RunForkBlockerFlowRouteHistoryUnproven, Message: "route history is unproven"},
			{Code: store.RunForkBlockerSessionHistoryUnproven, Message: "session history is unproven"},
			{Code: store.RunForkBlockerActiveTurnHistoryUnproven, Message: "turn history is unproven"},
			{Code: store.RunForkBlockerConversationAuditUnproven, Message: "audit history is unproven"},
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
	if admission.Owner != store.RunForkHistoricalReplayExecutionAdmissionOwner ||
		!admission.NonMutating ||
		admission.ExecutionSupported ||
		admission.FutureExecutionOwner != store.RunForkHistoricalReplayExecutionOwner {
		t.Fatalf("admission owner/support = %#v", admission)
	}
	if admission.ReplayResumeAdmissionOwner != store.RunForkReplayResumeAdmissionOwner ||
		admission.SelectedExecutionAdmissionOwner != store.RunForkSelectedContractExecutionAdmissionOwner ||
		admission.SelectedBindingOwner != store.RunForkSelectedContractBindingOwner ||
		admission.RouteTopologyOwner != store.RunForkSelectedContractRouteTopologyOwner ||
		admission.RecipientPlanningOwner != store.RunForkSelectedContractRecipientPlanningOwner ||
		admission.RouteRecoveryOwner != store.RunForkSelectedContractRoutePersistenceOwner ||
		admission.RuntimeRouteRecoveryOwner != store.RunForkSelectedContractRouteRecoveryOwner ||
		admission.ContractSwapAdmissionOwner != store.RunForkContractSwapBootResumeAdmissionOwner {
		t.Fatalf("owner consumption = %#v", admission)
	}
	assertHistoricalReplayFactSet(t, admission.FactAdmissions)
	assertHistoricalReplayAdmission(t, admission.FactAdmissions, store.RunForkHistoricalReplayFactSourceEvents, store.RunForkHistoricalReplayAdmissionLineageOnlyEvidence, "")
	assertHistoricalReplayAdmission(t, admission.FactAdmissions, store.RunForkHistoricalReplayFactReceipts, store.RunForkHistoricalReplayAdmissionLineageOnlyEvidence, "")
	assertHistoricalReplayAdmission(t, admission.FactAdmissions, store.RunForkHistoricalReplayFactEventDeliveries, store.RunForkHistoricalReplayAdmissionFailClosedBlocker, store.RunForkBlockerDeliveryHistoryUnproven)
	assertHistoricalReplayAdmission(t, admission.FactAdmissions, store.RunForkHistoricalReplayFactDeadLetters, store.RunForkHistoricalReplayAdmissionFailClosedBlocker, store.RunForkBlockerDeliveryHistoryUnproven)
	assertHistoricalReplayAdmission(t, admission.FactAdmissions, store.RunForkHistoricalReplayFactTimers, store.RunForkHistoricalReplayAdmissionFailClosedBlocker, store.RunForkBlockerTimerHistoryUnproven)
	assertHistoricalReplayAdmission(t, admission.FactAdmissions, store.RunForkHistoricalReplayFactRoutes, store.RunForkHistoricalReplayAdmissionFailClosedBlocker, store.RunForkBlockerFlowRouteHistoryUnproven)
	assertHistoricalReplayAdmission(t, admission.FactAdmissions, store.RunForkHistoricalReplayFactSessions, store.RunForkHistoricalReplayAdmissionFailClosedBlocker, store.RunForkBlockerSessionHistoryUnproven)
	assertHistoricalReplayAdmission(t, admission.FactAdmissions, store.RunForkHistoricalReplayFactTurns, store.RunForkHistoricalReplayAdmissionFailClosedBlocker, store.RunForkBlockerActiveTurnHistoryUnproven)
	assertHistoricalReplayAdmission(t, admission.FactAdmissions, store.RunForkHistoricalReplayFactAudits, store.RunForkHistoricalReplayAdmissionFailClosedBlocker, store.RunForkBlockerConversationAuditUnproven)
	assertHistoricalReplayAdmission(t, admission.FactAdmissions, store.RunForkHistoricalReplayFactNonAgentNodeSystemWork, store.RunForkHistoricalReplayAdmissionFailClosedBlocker, store.RunForkBlockerNonAgentDeliveryReplayUnsupported)
	assertHistoricalReplayAdmission(t, admission.FactAdmissions, store.RunForkHistoricalReplayFactSourceAdvancedPostTFacts, store.RunForkHistoricalReplayAdmissionFailClosedBlocker, "source_events_advanced_after_fork_point")
	assertHistoricalReplayAdmission(t, admission.FactAdmissions, store.RunForkHistoricalReplayFactRetryIdempotency, store.RunForkHistoricalReplayAdmissionSplitSibling, "")
	assertHistoricalReplayAdmission(t, admission.FactAdmissions, store.RunForkHistoricalReplayFactEmittedFollowUps, store.RunForkHistoricalReplayAdmissionSplitSibling, "")
	assertHistoricalReplayAdmission(t, admission.FactAdmissions, store.RunForkHistoricalReplayFactRuntimeRestartRecovery, store.RunForkHistoricalReplayAdmissionSplitSibling, "")
	assertHistoricalReplayAdmission(t, admission.FactAdmissions, store.RunForkHistoricalReplayFactCLIApiDashboardOperator, store.RunForkHistoricalReplayAdmissionSplitSibling, "")
	if !unsupportedBlockerHas(admission.UnsupportedBlockers, store.RunForkBlockerHistoricalReplayExecutionAdmissionNonMutating) ||
		!unsupportedBlockerHas(admission.UnsupportedBlockers, store.RunForkBlockerContractSwapBootResumeAdmissionNonMutating) ||
		!unsupportedBlockerHas(admission.UnsupportedBlockers, store.RunForkBlockerTimerHistoryUnproven) {
		t.Fatalf("unsupported blockers = %#v", admission.UnsupportedBlockers)
	}
	if !executionBoundaryHas(admission.InvalidPaths, "source_event_copy", store.RunForkSelectedContractDispositionInvalid) ||
		!executionBoundaryHas(admission.InvalidPaths, "selected_frontier_execution_as_full_replay", store.RunForkSelectedContractDispositionInvalid) ||
		!executionBoundaryHas(admission.BlockedSiblings, "mutating_historical_replay_execution", store.RunForkSelectedContractDispositionBlockedSibling) {
		t.Fatalf("boundaries invalid=%#v blocked=%#v", admission.InvalidPaths, admission.BlockedSiblings)
	}
}

func TestBuildHistoricalReplayExecutionAdmissionReportsReplayablePrimitiveWithoutMutation(t *testing.T) {
	selectedAdmission := testContractSwapSelectedExecutionAdmission(testContractSwapSelection())
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
	assertHistoricalReplayAdmission(t, admission.FactAdmissions, store.RunForkHistoricalReplayFactEventDeliveries, store.RunForkHistoricalReplayAdmissionExecutableForkWork, "")
	if !admission.NonMutating || admission.ExecutionSupported {
		t.Fatalf("admission mutation flags = %#v", admission)
	}
	if !unsupportedBlockerHas(admission.UnsupportedBlockers, store.RunForkBlockerHistoricalReplayExecutionAdmissionNonMutating) {
		t.Fatalf("unsupported blockers = %#v, want non-mutating blocker", admission.UnsupportedBlockers)
	}
}

func TestBuildHistoricalReplayExecutionAdmissionReportsTimerReconstructionOwner(t *testing.T) {
	selectedAdmission := testContractSwapSelectedExecutionAdmission(testContractSwapSelection())
	routeRecovery := testContractSwapRouteRecovery(selectedAdmission)
	replayAdmission := store.RunForkReplayResumeAdmission{
		Owner:                    store.RunForkReplayResumeAdmissionOwner,
		HistoricalReplayRequired: true,
		Dispositions: []store.RunForkReplayResumeDisposition{{
			Fact:           store.RunForkReplayResumeFactTimerHistory,
			Disposition:    store.RunForkReplayResumeDispositionReconstruct,
			Classification: store.RunForkHistoricalReplayAdmissionReconstructedForkState,
			Message:        "timer reconstruction owner admits active source timers",
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
	timerAdmission, ok := historicalReplayFactAdmission(admission.FactAdmissions, store.RunForkHistoricalReplayFactTimers)
	if !ok {
		t.Fatalf("timer fact admission missing: %#v", admission.FactAdmissions)
	}
	if timerAdmission.Admission != store.RunForkHistoricalReplayAdmissionReconstructedForkState ||
		timerAdmission.SourceOwner != store.RunForkHistoricalReplayTimerReconstructionOwner ||
		timerAdmission.Tracker != "#642" {
		t.Fatalf("timer admission = %#v", timerAdmission)
	}
}

func TestBuildHistoricalReplayExecutionConsumesAdmissionForDeliveryEventReplayMutation(t *testing.T) {
	replayAdmission := store.RunForkReplayResumeAdmission{
		Owner:                    store.RunForkReplayResumeAdmissionOwner,
		DeliveryEventReplayReady: true,
		Dispositions: []store.RunForkReplayResumeDisposition{{
			Fact:        store.RunForkReplayResumeFactDeliveryPendingHistory,
			Disposition: store.RunForkReplayResumeDispositionForkReplay,
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
		PendingWork: []store.RunForkPendingWork{{
			EventID:        "source-event",
			DeliveryID:     "source-delivery",
			SubscriberType: "agent",
			SubscriberID:   "agent-a",
			Classification: store.RunForkPendingClassificationPending,
			Status:         "pending",
		}},
	})
	if err != nil {
		t.Fatalf("BuildHistoricalReplayExecution: %v", err)
	}
	if execution.Owner != store.RunForkHistoricalReplayExecutionOwner ||
		execution.AdmissionOwner != store.RunForkHistoricalReplayExecutionAdmissionOwner ||
		execution.ReplayResumeAdmissionOwner != store.RunForkReplayResumeAdmissionOwner ||
		!execution.DeliveryEventReplayReady {
		t.Fatalf("execution = %#v", execution)
	}
	if execution.EventDeliveriesAdmission.Fact != store.RunForkHistoricalReplayFactEventDeliveries ||
		execution.EventDeliveriesAdmission.Admission != store.RunForkHistoricalReplayAdmissionExecutableForkWork {
		t.Fatalf("event deliveries admission = %#v", execution.EventDeliveriesAdmission)
	}
	if execution.ClosureLevel != "canonical_owner_promotion_with_delivery_event_replay_ready_only" ||
		execution.FullReplayResumeSupported ||
		len(execution.FactAdmissions) != 15 ||
		len(execution.RequiredConsumers) == 0 {
		t.Fatalf("execution broad owner accounting = %#v", execution)
	}
	if len(execution.DeliveryEventReplayWork) != 1 ||
		execution.DeliveryEventReplayWork[0].SourceEventID != "source-event" ||
		execution.DeliveryEventReplayWork[0].SourceDeliveryID != "source-delivery" {
		t.Fatalf("owner-authorized delivery replay work = %#v", execution.DeliveryEventReplayWork)
	}
	if !executionBoundaryHas(execution.InvalidPaths, "source_delivery_copy", store.RunForkSelectedContractDispositionInvalid) ||
		!executionBoundaryHas(execution.InvalidPaths, "source_outcome_suppression", store.RunForkSelectedContractDispositionInvalid) ||
		!executionBoundaryHas(execution.BlockedSiblings, "timer_reconstruction", store.RunForkSelectedContractDispositionBlockedSibling) {
		t.Fatalf("execution boundaries invalid=%#v blocked=%#v", execution.InvalidPaths, execution.BlockedSiblings)
	}
}

func TestBuildHistoricalReplayExecutionRejectsTaxonomyReadyWithoutOwnerWork(t *testing.T) {
	replayAdmission := store.RunForkReplayResumeAdmission{
		Owner:                    store.RunForkReplayResumeAdmissionOwner,
		DeliveryEventReplayReady: true,
		Dispositions: []store.RunForkReplayResumeDisposition{{
			Fact:        store.RunForkReplayResumeFactDeliveryPendingHistory,
			Disposition: store.RunForkReplayResumeDispositionForkReplay,
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
		PendingWork: []store.RunForkPendingWork{{
			EventID:        "source-event",
			DeliveryID:     "source-delivery",
			SubscriberType: "agent",
			SubscriberID:   "agent-a",
			Classification: store.RunForkPendingClassificationPending,
			Status:         "pending",
			RetryCount:     1,
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "owner-authorized delivery_event_replay_ready work") {
		t.Fatalf("error = %v, want owner-authorized work failure", err)
	}
}

func TestBuildHistoricalReplayExecutionRejectsNonExecutableAdmission(t *testing.T) {
	replayAdmission := store.RunForkReplayResumeAdmission{
		Owner:                    store.RunForkReplayResumeAdmissionOwner,
		DeliveryEventReplayReady: true,
	}
	_, err := BuildHistoricalReplayExecution(HistoricalReplayExecutionRequest{
		Admission: store.RunForkHistoricalReplayExecutionAdmission{
			Owner:                      store.RunForkHistoricalReplayExecutionAdmissionOwner,
			NonMutating:                true,
			FutureExecutionOwner:       store.RunForkHistoricalReplayExecutionOwner,
			ReplayResumeAdmissionOwner: store.RunForkReplayResumeAdmissionOwner,
			FactAdmissions: []store.RunForkHistoricalReplayFactAdmission{{
				Fact:      store.RunForkHistoricalReplayFactEventDeliveries,
				Admission: store.RunForkHistoricalReplayAdmissionLineageOnlyEvidence,
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
	contractSwapAdmission := store.RunForkContractSwapBootResumeAdmission{
		Owner:                           store.RunForkContractSwapBootResumeAdmissionOwner,
		NonMutating:                     true,
		SelectedExecutionAdmissionOwner: selectedAdmission.Owner,
		ReplayResumeAdmissionOwner:      "cmd.swarm.local_replay_helper",
		ForkRunID:                       selectedAdmission.ForkRunID,
		SourceRunID:                     selectedAdmission.SourceRunID,
		ForkEventID:                     selectedAdmission.ForkEventID,
	}

	_, err := BuildHistoricalReplayExecutionAdmission(HistoricalReplayExecutionAdmissionRequest{
		ReplayResumeAdmission:      store.RunForkReplayResumeAdmission{Owner: "cmd.swarm.local_replay_helper"},
		SelectedExecutionAdmission: selectedAdmission,
		ContractSwapAdmission:      contractSwapAdmission,
	})
	if err == nil || !strings.Contains(err.Error(), store.RunForkReplayResumeAdmissionOwner) {
		t.Fatalf("error = %v, want canonical replay admission owner failure", err)
	}
}

func assertHistoricalReplayFactSet(t *testing.T, admissions []store.RunForkHistoricalReplayFactAdmission) {
	t.Helper()
	required := []string{
		store.RunForkHistoricalReplayFactSourceEvents,
		store.RunForkHistoricalReplayFactEventDeliveries,
		store.RunForkHistoricalReplayFactReceipts,
		store.RunForkHistoricalReplayFactDeadLetters,
		store.RunForkHistoricalReplayFactRetryIdempotency,
		store.RunForkHistoricalReplayFactEmittedFollowUps,
		store.RunForkHistoricalReplayFactTimers,
		store.RunForkHistoricalReplayFactRoutes,
		store.RunForkHistoricalReplayFactSessions,
		store.RunForkHistoricalReplayFactTurns,
		store.RunForkHistoricalReplayFactAudits,
		store.RunForkHistoricalReplayFactNonAgentNodeSystemWork,
		store.RunForkHistoricalReplayFactSourceAdvancedPostTFacts,
		store.RunForkHistoricalReplayFactRuntimeRestartRecovery,
		store.RunForkHistoricalReplayFactCLIApiDashboardOperator,
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

func assertHistoricalReplayAdmission(t *testing.T, admissions []store.RunForkHistoricalReplayFactAdmission, fact, admission, blockerCode string) {
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
