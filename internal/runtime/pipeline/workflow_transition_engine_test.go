package pipeline

import (
	"context"
	"database/sql"
	"testing"

	"empireai/internal/events"
	runtimecontracts "empireai/internal/runtime/contracts"
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
	instance, ok, err := pc.workflowStore.Load(ctx, verticalID)
	if err != nil || !ok {
		t.Fatalf("expected workflow instance after revision increment, ok=%v err=%v", ok, err)
	}
	if got := asInt(instance.Metadata["revision_count"]); got != 1 {
		t.Fatalf("expected workflow metadata revision_count=1, got %+v", instance.Metadata)
	}
	bucket, ok := asObject(instance.AccumulatorState["validation-orchestrator"])
	if !ok {
		t.Fatalf("expected validation-orchestrator bucket, got %+v", instance.AccumulatorState)
	}
	if got := asInt(bucket["revision_count"]); got != 1 {
		t.Fatalf("expected validation-orchestrator revision_count=1, got %+v", bucket)
	}
}

func TestFactoryPipelineCoordinator_ApplyWorkflowEventTransition_IncrementsRevisionCountFromWorkflowState(t *testing.T) {
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
	if err := pc.workflowStore.Upsert(ctx, WorkflowInstance{
		InstanceID:      verticalID,
		WorkflowName:    pc.ContractBundle().Workflow.Workflow.Name,
		WorkflowVersion: pc.ContractBundle().Workflow.Workflow.Version,
		CurrentStage:    "cto_spec_review",
		Metadata: map[string]any{
			"status":         "active",
			"revision_count": 2,
			"g1_research":    true,
			"g2_spec":        true,
			"g3_cto":         true,
		},
		AccumulatorState: map[string]any{
			"validation-orchestrator": map[string]any{
				"vertical_id": verticalID,
				"gate_state": map[string]any{
					"g1_research": true,
					"g2_spec":     true,
					"g3_cto":      true,
					"g4_brand":    false,
				},
				"revision_count": 2,
			},
		},
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
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
	st := pc.validationGate.states[verticalID]
	if st == nil {
		t.Fatal("expected workflow-backed validation state to hydrate into memory")
	}
	if got := st.RevisionCount; got != 3 {
		t.Fatalf("expected hydrated revision_count=3, got %+v", st)
	}
	instance, ok, err := pc.workflowStore.Load(ctx, verticalID)
	if err != nil || !ok {
		t.Fatalf("expected workflow instance after revision increment, ok=%v err=%v", ok, err)
	}
	if got := asInt(instance.Metadata["revision_count"]); got != 3 {
		t.Fatalf("expected workflow metadata revision_count=3, got %+v", instance.Metadata)
	}
	bucket, ok := asObject(instance.AccumulatorState["validation-orchestrator"])
	if !ok {
		t.Fatalf("expected validation-orchestrator bucket, got %+v", instance.AccumulatorState)
	}
	if got := asInt(bucket["revision_count"]); got != 3 {
		t.Fatalf("expected validation-orchestrator revision_count=3, got %+v", bucket)
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

func TestFactoryPipelineCoordinator_ApplyWorkflowDataAccumulation_PersistsDeclaredWrites(t *testing.T) {
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
	if err := pc.workflowStore.Upsert(ctx, WorkflowInstance{
		InstanceID:       verticalID,
		WorkflowName:     "empire_vertical_pipeline",
		WorkflowVersion:  "2.2.0",
		CurrentStage:     "discovered",
		AccumulatorState: map[string]any{},
		Metadata:         map[string]any{},
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}

	transition := WorkflowTransition{
		Name: "discovered_to_scoring",
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			Writes:      []string{"name", "geography", "signal_strength"},
			SourceEvent: "vertical.discovered",
		},
	}
	evt := events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("vertical.discovered"),
		VerticalID: verticalID,
		Payload: mustJSON(map[string]any{
			"name":            "Clinical Billing",
			"geography":       "us",
			"signal_strength": 81,
			"ignored_field":   "x",
		}),
	}

	pc.applyWorkflowDataAccumulation(ctx, verticalID, transition, evt)

	instance, ok, err := pc.workflowStore.Load(ctx, verticalID)
	if err != nil {
		t.Fatalf("load workflow instance: %v", err)
	}
	if !ok {
		t.Fatal("expected workflow instance")
	}
	entityProjection, ok := instance.AccumulatorState["entity_projection"].(map[string]any)
	if !ok {
		t.Fatalf("expected entity_projection map, got %#v", instance.AccumulatorState["entity_projection"])
	}
	if asString(entityProjection["name"]) != "Clinical Billing" {
		t.Fatalf("expected name to be accumulated, got %#v", entityProjection["name"])
	}
	if asString(entityProjection["geography"]) != "us" {
		t.Fatalf("expected geography to be accumulated, got %#v", entityProjection["geography"])
	}
	if asInt(entityProjection["signal_strength"]) != 81 {
		t.Fatalf("expected signal_strength to be accumulated, got %#v", entityProjection["signal_strength"])
	}
	if _, exists := entityProjection["ignored_field"]; exists {
		t.Fatalf("did not expect ignored_field in entity_projection: %#v", entityProjection)
	}
	if asString(instance.Metadata["last_data_accumulation_event"]) != "vertical.discovered" {
		t.Fatalf("expected last_data_accumulation_event to be recorded, got %#v", instance.Metadata["last_data_accumulation_event"])
	}
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
