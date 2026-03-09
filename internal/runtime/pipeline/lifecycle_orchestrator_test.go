package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"empireai/internal/events"
	runtimetestkit "empireai/internal/runtime/testkit"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func lifecycleTestInsertVertical(t *testing.T, ctx context.Context, db *sql.DB, stage string) string {
	t.Helper()
	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, raw_signals)
		VALUES ($1::uuid, 'Lifecycle Vertical', $2, 'us', $3, 'factory', '{}'::jsonb)
	`, verticalID, "lifecycle-"+verticalID[:8], stage); err != nil {
		t.Fatalf("insert vertical: %v", err)
	}
	return verticalID
}

func TestHandler_lifecycle_orchestrator_budget_threshold_crossed(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, nil)
	n := &LifecycleOrchestrator{coordinator: pc}
	if !n.Handle(context.Background(), events.Event{Type: events.EventType("budget.threshold_crossed")}) {
		t.Fatal("expected budget.threshold_crossed to be handled")
	}
}

func TestHandler_lifecycle_orchestrator_mailbox_item_decided(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, nil)
	n := &LifecycleOrchestrator{coordinator: pc}
	if !n.Handle(context.Background(), events.Event{Type: events.EventType("mailbox.item_decided")}) {
		t.Fatal("expected mailbox.item_decided to be handled")
	}
}

func TestHandler_lifecycle_orchestrator_qa_validation_passed(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	n := &LifecycleOrchestrator{coordinator: pc}
	ctx := context.Background()

	verticalID := lifecycleTestInsertVertical(t, ctx, db, "building")
	pc.updateVerticalStage(ctx, verticalID, "building", "")
	if !n.Handle(ctx, events.Event{
		Type:       events.EventType("qa.validation_passed"),
		VerticalID: verticalID,
	}) {
		t.Fatal("expected qa.validation_passed to be handled")
	}

	instance, ok, err := pc.workflowStore.Load(ctx, verticalID)
	if err != nil || !ok {
		t.Fatalf("load workflow instance: ok=%v err=%v", ok, err)
	}
	if !truthyMetadataFlag(instance.Metadata["qa_passed"]) {
		t.Fatalf("expected qa_passed metadata, metadata=%v", instance.Metadata)
	}
}

func TestHandler_lifecycle_orchestrator_review_deploy_feedback(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	n := &LifecycleOrchestrator{coordinator: pc}
	ctx := context.Background()

	verticalID := lifecycleTestInsertVertical(t, ctx, db, "pre_launch")
	pc.updateVerticalStage(ctx, verticalID, "pre_launch", "")
	if !n.Handle(ctx, events.Event{
		Type:       events.EventType("review.deploy_feedback"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"approved": true}),
	}) {
		t.Fatal("expected review.deploy_feedback to be handled")
	}

	instance, ok, err := pc.workflowStore.Load(ctx, verticalID)
	if err != nil || !ok {
		t.Fatalf("load workflow instance: ok=%v err=%v", ok, err)
	}
	if !truthyMetadataFlag(instance.Metadata["deploy_approved"]) {
		t.Fatalf("expected deploy_approved metadata, metadata=%v", instance.Metadata)
	}
}

func TestHandler_lifecycle_orchestrator_launch_ready(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	n := &LifecycleOrchestrator{coordinator: pc}
	ctx := context.Background()

	verticalID := lifecycleTestInsertVertical(t, ctx, db, "pre_launch")
	pc.PersistWorkflowMetadata(ctx, verticalID, func(metadata map[string]any) {
		metadata["deploy_approved"] = true
	})
	if !n.Handle(ctx, events.Event{
		Type:       events.EventType("launch_ready"),
		VerticalID: verticalID,
	}) {
		t.Fatal("expected launch_ready to be handled")
	}
	if got := pc.currentWorkflowState(ctx, verticalID).Stage; got != NormalizePipelineStage("launched") {
		t.Fatalf("expected launched stage, got %s", got)
	}
}

func TestHandler_lifecycle_orchestrator_opco_steady_state_reached(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	n := &LifecycleOrchestrator{coordinator: pc}
	ctx := context.Background()

	verticalID := lifecycleTestInsertVertical(t, ctx, db, "launched")
	if !n.Handle(ctx, events.Event{
		Type:       events.EventType("opco.steady_state_reached"),
		VerticalID: verticalID,
	}) {
		t.Fatal("expected opco.steady_state_reached to be handled")
	}
	if got := pc.currentWorkflowState(ctx, verticalID).Stage; got != NormalizePipelineStage("operating") {
		t.Fatalf("expected operating stage, got %s", got)
	}
}

func TestHandler_lifecycle_orchestrator_opco_growth_triggered(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	n := &LifecycleOrchestrator{coordinator: pc}
	ctx := context.Background()

	verticalID := lifecycleTestInsertVertical(t, ctx, db, "operating")
	if !n.Handle(ctx, events.Event{
		Type:       events.EventType("opco.growth_triggered"),
		VerticalID: verticalID,
	}) {
		t.Fatal("expected opco.growth_triggered to be handled")
	}
	if got := pc.currentWorkflowState(ctx, verticalID).Stage; got != NormalizePipelineStage("expanding") {
		t.Fatalf("expected expanding stage, got %s", got)
	}
}

func TestHandler_lifecycle_orchestrator_opco_growth_stabilized(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	n := &LifecycleOrchestrator{coordinator: pc}
	ctx := context.Background()

	verticalID := lifecycleTestInsertVertical(t, ctx, db, "expanding")
	if !n.Handle(ctx, events.Event{
		Type:       events.EventType("opco.growth_stabilized"),
		VerticalID: verticalID,
	}) {
		t.Fatal("expected opco.growth_stabilized to be handled")
	}
	if got := pc.currentWorkflowState(ctx, verticalID).Stage; got != NormalizePipelineStage("operating") {
		t.Fatalf("expected operating stage, got %s", got)
	}
}

func TestHandler_lifecycle_orchestrator_opco_teardown_requested(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	n := &LifecycleOrchestrator{coordinator: pc}
	ctx := context.Background()

	verticalID := lifecycleTestInsertVertical(t, ctx, db, "operating")
	if !n.Handle(ctx, events.Event{
		Type:       events.EventType("opco.teardown_requested"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"vertical_id": verticalID, "mailbox_decision_id": uuid.NewString()}),
	}) {
		t.Fatal("expected opco.teardown_requested to be handled")
	}
	if got := pc.currentWorkflowState(ctx, verticalID).Stage; got != NormalizePipelineStage("winding_down") {
		t.Fatalf("expected winding_down stage, got %s", got)
	}
}

func TestLifecycleOrchestrator_HandleMarginalKillTimerKillsMarginalVertical(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	n := &LifecycleOrchestrator{coordinator: pc}
	ctx := context.Background()

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, raw_signals)
		VALUES ($1::uuid, 'Timed Out Vertical', 'timed-out-vertical', 'us', 'marginal_review', 'factory', '{}'::jsonb)
	`, verticalID); err != nil {
		t.Fatalf("insert vertical: %v", err)
	}

	ch := bus.Subscribe("watch-marginal-kill", events.EventType("vertical.killed"))
	handled := n.Handle(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("timer.marginal_kill"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"parked_days": 60}),
	})
	if !handled {
		t.Fatal("expected timer.marginal_kill to be handled")
	}

	got := runtimetestkit.WaitForEventTypes(t, ch, []string{"vertical.killed"}, 0)["vertical.killed"]
	var payload map[string]any
	if err := json.Unmarshal(got.Payload, &payload); err != nil {
		t.Fatalf("decode vertical.killed payload: %v", err)
	}
	if asString(payload["reason"]) != "marginal_kill_timer" {
		t.Fatalf("expected marginal_kill_timer reason, got %#v", payload["reason"])
	}
	if asString(payload["timer_id"]) != "marginal_kill_timer" {
		t.Fatalf("expected marginal_kill_timer timer_id, got %#v", payload["timer_id"])
	}
}

func TestLifecycleOrchestrator_HandleMarginalReviewTimerPublishesInjectedReviewEvent(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	n := &LifecycleOrchestrator{coordinator: pc}
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, raw_signals)
		VALUES ($1::uuid, 'Parked One', 'parked-one', 'us', 'marginal_review', 'factory', '{}'::jsonb)
	`, uuid.NewString()); err != nil {
		t.Fatalf("insert marginal vertical: %v", err)
	}

	ch := bus.Subscribe("empire-coordinator", events.EventType("timer.marginal_review"))
	handled := n.Handle(ctx, events.Event{
		ID:        uuid.NewString(),
		Type:      events.EventType("timer.marginal_review"),
		CreatedAt: time.Now().UTC(),
	})
	if !handled {
		t.Fatal("expected timer.marginal_review to be handled")
	}

	received := collectEvents(ch, 350*time.Millisecond)
	if len(received) == 0 {
		t.Fatal("expected injected marginal review event")
	}
	payload := parsePayloadMap(received[len(received)-1].Payload)
	if !boolFromAny(payload["review_request_injected"]) {
		t.Fatalf("expected review_request_injected=true, payload=%v", payload)
	}
	if asString(payload["parked_marginals_summary"]) == "" {
		t.Fatalf("expected parked_marginals_summary, payload=%v", payload)
	}
}

func TestLifecycleOrchestrator_HandlePortfolioDigestTimerPublishesInjectedDigestEvent(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, nil)
	n := &LifecycleOrchestrator{coordinator: pc}
	ctx := context.Background()

	ch := bus.Subscribe("empire-coordinator", events.EventType("timer.portfolio_digest"))
	handled := n.Handle(ctx, events.Event{
		ID:        uuid.NewString(),
		Type:      events.EventType("timer.portfolio_digest"),
		Payload:   mustJSON(map[string]any{"trigger_reason": "scheduled"}),
		CreatedAt: time.Now().UTC(),
	})
	if !handled {
		t.Fatal("expected timer.portfolio_digest to be handled")
	}

	received := collectEvents(ch, 350*time.Millisecond)
	if len(received) == 0 {
		t.Fatal("expected injected portfolio digest event")
	}
	payload := parsePayloadMap(received[len(received)-1].Payload)
	if !boolFromAny(payload["scoring_rejections_injected"]) {
		t.Fatalf("expected scoring_rejections_injected=true, payload=%v", payload)
	}
	if asString(payload["trigger_reason"]) != "scheduled" {
		t.Fatalf("expected trigger_reason to be preserved, payload=%v", payload)
	}
}
