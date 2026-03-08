package pipeline

import (
	"context"
	"testing"
	"time"

	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestFactoryPipelineCoordinator_UpdateVerticalStageProjectsWorkflowInstance(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(pipelineTestBus{}, db)
	ctx := context.Background()
	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage)
		VALUES ($1::uuid, 'Test Vertical', 'test-vertical', 'Asuncion, Paraguay', 'shortlisted')
	`, verticalID); err != nil {
		t.Fatalf("insert vertical: %v", err)
	}

	pc.validationGate.states[verticalID] = &validationPipelineState{
		VerticalID:         verticalID,
		Status:             "active",
		G1Research:         true,
		RevisionCount:      2,
		InnerRevisionCount: 1,
		SpecVersion:        3,
	}
	pc.updateVerticalStage(ctx, verticalID, "researching", "vertical.shortlisted")

	inst, ok, err := pc.workflowStore.Load(ctx, verticalID)
	if err != nil {
		t.Fatalf("load workflow instance: %v", err)
	}
	if !ok {
		t.Fatal("expected workflow instance to be stored")
	}
	if inst.CurrentStage != "researching" {
		t.Fatalf("unexpected current stage: %+v", inst)
	}
	if inst.WorkflowName != "empire_vertical_pipeline" || inst.WorkflowVersion != "2.1.0" {
		t.Fatalf("unexpected workflow identity: %+v", inst)
	}
	if len(inst.TransitionHistory) != 1 {
		t.Fatalf("expected one transition record, got %+v", inst.TransitionHistory)
	}
	record := inst.TransitionHistory[0]
	if record.TransitionID != "shortlisted_to_researching" {
		t.Fatalf("unexpected transition record: %+v", record)
	}
	if got := asInt(inst.Metadata["revision_count"]); got != 2 {
		t.Fatalf("unexpected metadata: %+v", inst.Metadata)
	}
	acc, ok := inst.AccumulatorState["pipeline-coordinator"].(map[string]any)
	if !ok {
		t.Fatalf("expected accumulator state bucket, got %+v", inst.AccumulatorState)
	}
	if got := asInt(acc["spec_version"]); got != 3 {
		t.Fatalf("unexpected accumulator state: %+v", acc)
	}
}

func TestFactoryPipelineCoordinator_UpdateVerticalStagePreservesEnteredStageAtForIdempotentStage(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(pipelineTestBus{}, db)
	ctx := context.Background()
	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage)
		VALUES ($1::uuid, 'Test Vertical 2', 'test-vertical-2', 'Asuncion, Paraguay', 'researching')
	`, verticalID); err != nil {
		t.Fatalf("insert vertical: %v", err)
	}
	initial := time.Now().UTC().Add(-5 * time.Minute).Round(time.Second)
	if err := pc.workflowStore.Upsert(ctx, WorkflowInstance{
		InstanceID:      verticalID,
		WorkflowName:    "empire_vertical_pipeline",
		WorkflowVersion: "2.1.0",
		CurrentStage:    "researching",
		EnteredStageAt:  initial,
		Metadata:        map[string]any{"revision_count": 1},
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}

	pc.updateVerticalStage(ctx, verticalID, "researching", "research.completed")

	inst, ok, err := pc.workflowStore.Load(ctx, verticalID)
	if err != nil {
		t.Fatalf("load workflow instance: %v", err)
	}
	if !ok {
		t.Fatal("expected workflow instance to be stored")
	}
	if !inst.EnteredStageAt.Equal(initial) {
		t.Fatalf("expected entered_stage_at to be preserved: got=%s want=%s", inst.EnteredStageAt, initial)
	}
	if len(inst.TransitionHistory) != 0 {
		t.Fatalf("expected idempotent stage update to avoid new transition record: %+v", inst.TransitionHistory)
	}
}

func TestFactoryPipelineCoordinator_CurrentWorkflowStatePrefersWorkflowInstanceMetadata(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(pipelineTestBus{}, db)
	ctx := context.Background()
	verticalID := uuid.NewString()

	if err := pc.workflowStore.Upsert(ctx, WorkflowInstance{
		InstanceID:      verticalID,
		WorkflowName:    "empire_vertical_pipeline",
		WorkflowVersion: "2.1.0",
		CurrentStage:    "cto_spec_review",
		Metadata: map[string]any{
			"revision_count": 3,
			"g1_research":    true,
			"status":         "active",
		},
		AccumulatorState: map[string]any{
			"pipeline-coordinator": map[string]any{
				"g2_spec": true,
			},
		},
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}

	state := pc.currentWorkflowState(ctx, verticalID)
	if state.Stage != StageCTOSpecReview {
		t.Fatalf("unexpected stage: %+v", state)
	}
	if got := asInt(state.Metadata["revision_count"]); got != 3 {
		t.Fatalf("expected revision_count from workflow instance, got %+v", state.Metadata)
	}
	if !truthyMetadataFlag(state.Metadata["g1_research"]) || !truthyMetadataFlag(state.Metadata["g2_spec"]) {
		t.Fatalf("expected merged workflow metadata, got %+v", state.Metadata)
	}
	if state.Status != "active" {
		t.Fatalf("expected status from workflow instance, got %+v", state)
	}
}

func TestFactoryPipelineCoordinator_EnsureStateLoadedRestoresWorkflowValidationAndScoringState(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(pipelineTestBus{}, db)
	ctx := context.Background()
	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage)
		VALUES ($1::uuid, 'Restored Vertical', 'restored-vertical', 'Asuncion, Paraguay', 'scoring')
	`, verticalID); err != nil {
		t.Fatalf("insert vertical: %v", err)
	}

	if err := pc.workflowStore.Upsert(ctx, WorkflowInstance{
		InstanceID:      verticalID,
		WorkflowName:    "empire_vertical_pipeline",
		WorkflowVersion: "2.1.0",
		CurrentStage:    "scoring",
		Metadata: map[string]any{
			"status":         "active",
			"revision_count": 4,
			"g1_research":    true,
		},
		AccumulatorState: map[string]any{
			"pipeline-coordinator": map[string]any{
				"g2_spec":          true,
				"spec_version":     2,
				"research_payload": map[string]any{"brief": "ok"},
			},
			"scoring-state": encodeScoringAccumulator(&scoringAccumulator{
				VerticalID:       verticalID,
				VerticalName:     "Restored Vertical",
				Geography:        "Asuncion, Paraguay",
				Mode:             "saas_gap",
				Rubric:           "universal",
				Expected:         []string{"build_complexity"},
				Received:         map[string]scoreDimensionResult{"build_complexity": {Score: 82, Evidence: "seed"}},
				Contested:        map[string]contestedDimension{},
				ContestNotified:  map[string]bool{},
				RequestedAt:      time.Now().UTC().Add(-2 * time.Minute),
				LastUpdatedAt:    time.Now().UTC().Add(-time.Minute),
				DiscoveryContext: map[string]any{"source": "workflow"},
			}),
		},
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}

	pc.ensureStateLoaded(ctx)

	restoredValidation := pc.validationGate.states[verticalID]
	if restoredValidation == nil {
		t.Fatal("expected validation state restored from workflow instance")
	}
	if restoredValidation.RevisionCount != 4 || !restoredValidation.G1Research || !restoredValidation.G2Spec {
		t.Fatalf("unexpected restored validation state: %+v", restoredValidation)
	}
	restoredScoring := pc.scoringState.accumulators[verticalID]
	if restoredScoring == nil {
		t.Fatal("expected scoring accumulator restored from workflow instance")
	}
	if restoredScoring.Mode != "saas_gap" || restoredScoring.Rubric != "universal" {
		t.Fatalf("unexpected restored scoring accumulator: %+v", restoredScoring)
	}
	if got := restoredScoring.Received["build_complexity"].Score; got != 82 {
		t.Fatalf("unexpected restored scoring dimension score: %+v", restoredScoring.Received)
	}
}
