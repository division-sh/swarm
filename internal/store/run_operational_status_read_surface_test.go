package store

import (
	"testing"
	"time"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

func TestProjectRunOperationalStatus_UsesDeliveryLifecycleWhenRunIsOperationallyStalled(t *testing.T) {
	report := RunDebugReport{
		RunTableStatus: "running",
		LastEventAt:    time.Unix(1700000000, 0).UTC(),
		Deliveries: []RunDebugDeliveryCount{
			{SubscriberID: "agent-1", Status: "delivered", Count: 2},
			{SubscriberID: "agent-2", Status: "failed", Count: 1},
		},
	}

	got := ProjectRunOperationalStatus(report)
	if got.State != "stalled" {
		t.Fatalf("state = %q, want stalled", got.State)
	}
	if got.BlockingLayer != "delivery_lifecycle" {
		t.Fatalf("blocking_layer = %q, want delivery_lifecycle", got.BlockingLayer)
	}
	if got.BlockingReason != "no_active_deliveries" {
		t.Fatalf("blocking_reason = %q, want no_active_deliveries", got.BlockingReason)
	}
}

func TestProjectRunOperationalStatus_UsesScoringOutcomeBlockingLayer(t *testing.T) {
	report := RunDebugReport{
		RunTableStatus: "running",
		LastEventAt:    time.Unix(1700000000, 0).UTC(),
		EventCounts: []RunDebugEventCount{
			{EventName: "scoring/scoring.requested", Count: 1},
		},
		Deliveries: []RunDebugDeliveryCount{
			{SubscriberID: "agent-1", Status: "delivered", Count: 1},
		},
	}

	got := ProjectRunOperationalStatus(report)
	if got.State != "stalled" {
		t.Fatalf("state = %q, want stalled", got.State)
	}
	if got.BlockingLayer != "scoring_terminal_outcome" {
		t.Fatalf("blocking_layer = %q, want scoring_terminal_outcome", got.BlockingLayer)
	}
	if got.BlockingReason != "terminal_scoring_outcome_missing" {
		t.Fatalf("blocking_reason = %q, want terminal_scoring_outcome_missing", got.BlockingReason)
	}
}

func TestProjectRunOperationalStatus_TreatsScopedShortlistAsTerminalScoringOutcome(t *testing.T) {
	report := RunDebugReport{
		RunTableStatus: "running",
		LastEventAt:    time.Unix(1700000000, 0).UTC(),
		EventCounts: []RunDebugEventCount{
			{EventName: "scoring/scoring.requested", Count: 1},
			{EventName: "scoring/vertical.shortlisted", Count: 1},
		},
		Deliveries: []RunDebugDeliveryCount{
			{SubscriberID: "agent-1", Status: "delivered", Count: 1},
		},
	}

	got := ProjectRunOperationalStatus(report)
	if got.State != "stalled" {
		t.Fatalf("state = %q, want stalled", got.State)
	}
	if got.BlockingLayer != "delivery_lifecycle" {
		t.Fatalf("blocking_layer = %q, want delivery_lifecycle", got.BlockingLayer)
	}
	if got.BlockingReason != "no_active_deliveries" {
		t.Fatalf("blocking_reason = %q, want no_active_deliveries", got.BlockingReason)
	}
}

func TestProjectRunOperationalStatus_PreservesHealthyRunningWhenActiveDeliveriesRemain(t *testing.T) {
	report := RunDebugReport{
		RunTableStatus: "running",
		LastEventAt:    time.Unix(1700000000, 0).UTC(),
		Deliveries: []RunDebugDeliveryCount{
			{SubscriberID: "agent-1", Status: "in_progress", Count: 1},
			{SubscriberID: "agent-2", Status: "delivered", Count: 1},
		},
	}

	got := ProjectRunOperationalStatus(report)
	if got.State != "running" {
		t.Fatalf("state = %q, want running", got.State)
	}
	if got.BlockingLayer != "" || got.BlockingReason != "" {
		t.Fatalf("unexpected blocking projection: %#v", got)
	}
	if got.Heuristics == nil {
		t.Fatal("heuristics = nil, want empty slice")
	}
	if len(got.Heuristics) != 0 {
		t.Fatalf("heuristics = %#v, want empty", got.Heuristics)
	}
}

func TestProjectRunOperationalStatus_EmitsDeadLetterHeuristic(t *testing.T) {
	report := RunDebugReport{
		RunTableStatus: "running",
		LastEventAt:    time.Unix(1700000000, 0).UTC(),
		Deliveries: []RunDebugDeliveryCount{
			{SubscriberID: "agent-1", Status: "in_progress", Count: 1},
		},
		DeadLetters: []RunDebugDeadLetter{
			{OriginalEvent: "evt-1", Failure: testFailureEnvelope(runtimefailures.ClassInternalFailure, "test_handler_failure", nil)},
		},
	}

	got := ProjectRunOperationalStatus(report)
	if len(got.Heuristics) != 1 {
		t.Fatalf("heuristics = %#v, want one item", got.Heuristics)
	}
	if got.Heuristics[0] != "dead letters exist for this run" {
		t.Fatalf("heuristic = %q, want dead letters note", got.Heuristics[0])
	}
}
