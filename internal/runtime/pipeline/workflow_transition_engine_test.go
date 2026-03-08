package pipeline

import (
	"context"
	"database/sql"
	"testing"

	"empireai/internal/events"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestFactoryPipelineCoordinator_ApplyWorkflowEventTransition_ValidationStarted(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)
	ctx := context.Background()
	verticalID := uuid.NewString()
	emitted := []events.Event{}
	ctx = context.WithValue(ctx, pipelineEmitCollectorKey{}, &emitted)

	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'Claims Automation', 'claims-automation', 'us', 'shortlisted', 'factory', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("insert vertical: %v", err)
	}

	outcome, ok := pc.applyWorkflowEventTransition(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("vertical.shortlisted"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"vertical_id": verticalID, "composite_score": 82.5}),
	})
	if !ok {
		t.Fatal("expected shortlisted workflow transition")
	}
	if outcome.Transition.Name != "shortlisted_to_researching" {
		t.Fatalf("unexpected transition: %+v", outcome.Transition)
	}
	assertVerticalStage(t, ctx, db, verticalID, "researching")
	if len(emitted) != 1 || string(emitted[0].Type) != "validation.started" {
		t.Fatalf("expected validation.started emit, got %+v", emitted)
	}
}

func TestFactoryPipelineCoordinator_ApplyWorkflowEventTransition_IncrementsRevisionCount(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)
	ctx := context.Background()
	verticalID := uuid.NewString()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'EU VAT Reconciliation', 'eu-vat-reconciliation', 'eu', 'cto_spec_review', 'factory', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("insert vertical: %v", err)
	}
	pc.validationGate.states[verticalID] = &validationPipelineState{
		VerticalID:     verticalID,
		Status:         "active",
		G1Research:     true,
		G2Spec:         true,
		G3CTO:          true,
		RevisionCount:  0,
		SpecVersion:    1,
		ScoringPayload: mustJSON(map[string]any{"vertical_id": verticalID}),
	}

	outcome, ok := pc.applyWorkflowEventTransition(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("spec.validation_failed"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"status": "blocker"}),
	})
	if !ok {
		t.Fatal("expected spec.validation_failed workflow transition")
	}
	if outcome.Transition.Name != "validation_failed_to_speccing" {
		t.Fatalf("unexpected transition: %+v", outcome.Transition)
	}
	assertVerticalStage(t, ctx, db, verticalID, "mvp_speccing")
	if got := pc.validationGate.states[verticalID].RevisionCount; got != 1 {
		t.Fatalf("expected revision_count=1, got %d", got)
	}
}

func TestFactoryPipelineCoordinator_ApplyWorkflowEventTransition_RequiresHumanApproval(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)
	ctx := context.Background()
	verticalID := uuid.NewString()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'Revenue Recovery', 'revenue-recovery', 'us', 'ready_for_review', 'factory', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("insert vertical: %v", err)
	}

	if _, ok := pc.applyWorkflowEventTransition(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("vertical.approved"),
		SourceAgent: "pipeline-coordinator",
		VerticalID:  verticalID,
		Payload:     mustJSON(map[string]any{"vertical_id": verticalID}),
	}); ok {
		t.Fatal("expected non-human vertical.approved to fail guard evaluation")
	}
	assertVerticalStage(t, ctx, db, verticalID, "ready_for_review")

	outcome, ok := pc.applyWorkflowEventTransition(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("vertical.approved"),
		SourceAgent: "human",
		VerticalID:  verticalID,
		Payload:     mustJSON(map[string]any{"vertical_id": verticalID, "mailbox_decision_id": uuid.NewString()}),
	})
	if !ok {
		t.Fatal("expected human vertical.approved to transition")
	}
	if outcome.Transition.Name != "ready_to_approved" {
		t.Fatalf("unexpected transition: %+v", outcome.Transition)
	}
	assertVerticalStage(t, ctx, db, verticalID, "approved")
}

func TestFactoryPipelineCoordinator_HandleScoringRequested_ProjectsScoringStage(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)
	ctx := context.Background()
	verticalID := uuid.NewString()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'Clinical Billing', 'clinical-billing', 'us', 'discovered', 'factory', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("insert vertical: %v", err)
	}

	pc.handleScoringRequested(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("vertical.discovered"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"vertical_id": verticalID, "vertical_name": "Clinical Billing", "geography": "us", "mode": "saas_gap", "signal_strength": 81}),
	})

	assertVerticalStage(t, ctx, db, verticalID, "scoring")
}

func TestFactoryPipelineCoordinator_FinalizeScoringAccumulator_UsesWorkflowTransition(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	store := &captureStore{}
	bus := NewEventBus(store)
	pc := NewFactoryPipelineCoordinator(bus, db)
	bus.SetInterceptors(pc)
	ctx := context.Background()
	verticalID := uuid.NewString()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'Claims Workflow', 'claims-workflow', 'us', 'scoring', 'factory', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("insert vertical: %v", err)
	}

	acc := newUniversalAccumulator(verticalID, "Claims Workflow", "us", "saas_gap")
	setScores(acc, map[string]int{
		"build_complexity":        90,
		"automation_completeness": 90,
		"icp_crispness":           90,
		"distribution_leverage":   90,
		"time_to_value":           90,
		"operational_drag":        90,
		"pain_severity":           90,
		"competition_gap":         90,
		"monetization_clarity":    90,
		"retention_architecture":  90,
		"expansion_potential":     90,
	})
	pc.mu.Lock()
	pc.scoringState.accumulators[verticalID] = acc
	pc.mu.Unlock()

	pc.finalizeScoringAccumulator(ctx, verticalID, false)

	assertVerticalStage(t, ctx, db, verticalID, "shortlisted")
	if !hasPersistedEventType(store.events, "vertical.shortlisted") {
		t.Fatalf("expected vertical.shortlisted emit, got %+v", store.events)
	}
	instance, ok, err := pc.workflowStore.Load(ctx, verticalID)
	if err != nil {
		t.Fatalf("load workflow instance: %v", err)
	}
	if !ok {
		t.Fatal("expected workflow instance")
	}
	found := false
	for _, item := range instance.TransitionHistory {
		if item.To == "shortlisted" && (item.TransitionID == "scoring_to_shortlisted" || item.TransitionID == "vertical.scored") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected shortlisted transition record in history, got %+v", instance.TransitionHistory)
	}
}

func assertVerticalStage(t *testing.T, ctx context.Context, db *sql.DB, verticalID, want string) {
	t.Helper()
	var got string
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(stage,'') FROM verticals WHERE id = $1::uuid`, verticalID).Scan(&got); err != nil {
		t.Fatalf("load vertical stage: %v", err)
	}
	if got != want {
		t.Fatalf("expected stage=%q got=%q", want, got)
	}
}
