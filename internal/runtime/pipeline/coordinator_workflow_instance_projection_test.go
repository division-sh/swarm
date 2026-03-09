package pipeline

import (
	"context"
	"testing"
	"time"

	"empireai/internal/events"
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
	if inst.WorkflowName != "empire_vertical_pipeline" || inst.WorkflowVersion != pc.ContractBundle().Workflow.Workflow.Version {
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
	validationBucket, ok := inst.AccumulatorState["validation-orchestrator"].(map[string]any)
	if !ok {
		t.Fatalf("expected validation-orchestrator bucket, got %+v", inst.AccumulatorState)
	}
	gateState, ok := validationBucket["gate_state"].(map[string]any)
	if !ok || !truthyMetadataFlag(gateState["g1_research"]) {
		t.Fatalf("expected contract-shaped gate_state, got %+v", validationBucket)
	}
	if got := asInt(validationBucket["revision_count"]); got != 2 {
		t.Fatalf("expected validation-orchestrator revision_count=2, got %+v", validationBucket)
	}
	for key := range validationBucket {
		switch key {
		case "vertical_id", "gate_state", "revision_count", "started_at", "completed_at":
		default:
			t.Fatalf("unexpected non-schema validation field %q in %+v", key, validationBucket)
		}
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
		WorkflowVersion: "2.2.0",
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
		WorkflowVersion: "2.2.0",
		CurrentStage:    "cto_spec_review",
		Metadata: map[string]any{
			"revision_count": 3,
			"g1_research":    true,
			"status":         "active",
		},
		AccumulatorState: map[string]any{
			"validation-orchestrator": map[string]any{
				"gate_state": map[string]any{
					"g2_spec": true,
				},
				"revision_count": 3,
			},
			"pipeline-coordinator": map[string]any{
				"spec_version": 2,
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

func TestFactoryPipelineCoordinator_CurrentWorkflowStateIgnoresPipelineCoordinatorCompatibilityBucket(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(pipelineTestBus{}, db)
	ctx := context.Background()
	verticalID := uuid.NewString()

	if err := pc.workflowStore.Upsert(ctx, WorkflowInstance{
		InstanceID:      verticalID,
		WorkflowName:    pc.ContractBundle().Workflow.Workflow.Name,
		WorkflowVersion: pc.ContractBundle().Workflow.Workflow.Version,
		CurrentStage:    "cto_spec_review",
		Metadata: map[string]any{
			"status":         "active",
			"revision_count": 3,
			"g1_research":    true,
		},
		AccumulatorState: map[string]any{
			"validation-orchestrator": map[string]any{
				"gate_state": map[string]any{
					"g2_spec": true,
				},
				"revision_count": 3,
			},
			"pipeline-coordinator": map[string]any{
				"status":         "stale",
				"revision_count": 99,
				"g3_cto":         true,
			},
		},
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}

	state := pc.currentWorkflowState(ctx, verticalID)
	if state.Status != "active" {
		t.Fatalf("expected metadata/validation status to win over compatibility bucket, got %+v", state)
	}
	if got := asInt(state.Metadata["revision_count"]); got != 3 {
		t.Fatalf("expected revision_count=3 from canonical workflow state, got %+v", state.Metadata)
	}
	if truthyMetadataFlag(state.Metadata["g3_cto"]) {
		t.Fatalf("expected compatibility-only g3_cto not to leak into live workflow state, got %+v", state.Metadata)
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
		WorkflowVersion: "2.2.0",
		CurrentStage:    "scoring",
		Metadata: map[string]any{
			"status":         "active",
			"revision_count": 4,
			"g1_research":    true,
		},
		AccumulatorState: map[string]any{
			"validation-orchestrator": map[string]any{
				"gate_state": map[string]any{
					"g2_spec": true,
				},
				"revision_count": 4,
			},
			"scoring-node": map[string]any{
				"vertical_id":          verticalID,
				"dimensions_requested": []any{"build_complexity"},
				"dimensions_received":  map[string]any{"build_complexity": map[string]any{"score": 82, "evidence": "seed"}},
				"started_at":           time.Now().UTC().Add(-2 * time.Minute).Format(time.RFC3339Nano),
				"completed_at":         time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano),
			},
			"pipeline-coordinator": map[string]any{
				"spec_version": 2,
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
	inst, ok, err := pc.workflowStore.Load(ctx, verticalID)
	if err != nil || !ok {
		t.Fatalf("load workflow instance: ok=%v err=%v", ok, err)
	}
	scoringBucket, ok := inst.AccumulatorState["scoring-node"].(map[string]any)
	if !ok {
		t.Fatalf("expected scoring-node bucket, got %+v", inst.AccumulatorState)
	}
	for key := range scoringBucket {
		switch key {
		case "vertical_id", "dimensions_requested", "dimensions_received", "analyst_id", "started_at", "completed_at":
		default:
			t.Fatalf("unexpected non-schema scoring field %q in %+v", key, scoringBucket)
		}
	}
}

func TestFactoryPipelineCoordinator_EnsureStateLoadedPrefersScoringNodeBucketOverCompatibilityBucket(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(pipelineTestBus{}, db)
	ctx := context.Background()
	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode)
		VALUES ($1::uuid, 'Scoring Merge Vertical', 'scoring-merge-vertical', 'US', 'scoring', 'saas_gap')
	`, verticalID); err != nil {
		t.Fatalf("insert vertical: %v", err)
	}

	if err := pc.workflowStore.Upsert(ctx, WorkflowInstance{
		InstanceID:      verticalID,
		WorkflowName:    pc.ContractBundle().Workflow.Workflow.Name,
		WorkflowVersion: pc.ContractBundle().Workflow.Workflow.Version,
		CurrentStage:    "scoring",
		AccumulatorState: map[string]any{
			"scoring-node": map[string]any{
				"vertical_id":          verticalID,
				"dimensions_requested": []any{"build_complexity"},
				"dimensions_received":  map[string]any{"build_complexity": map[string]any{"score": 91, "evidence": "node"}},
			},
			"scoring-state": encodeScoringAccumulator(&scoringAccumulator{
				VerticalID:       verticalID,
				VerticalName:     "Compat Vertical",
				Geography:        "US",
				Mode:             "saas_gap",
				Rubric:           "universal",
				Expected:         []string{"build_complexity"},
				Received:         map[string]scoreDimensionResult{"build_complexity": {Score: 40, Evidence: "compat"}},
				Contested:        map[string]contestedDimension{},
				ContestNotified:  map[string]bool{},
				DiscoveryContext: map[string]any{"source": "compat"},
			}),
		},
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}

	pc.ensureStateLoaded(ctx)

	restoredScoring := pc.scoringState.accumulators[verticalID]
	if restoredScoring == nil {
		t.Fatal("expected scoring accumulator restored from workflow instance")
	}
	if got := restoredScoring.Received["build_complexity"].Score; got != 91 {
		t.Fatalf("expected scoring-node dimensions_received to win, got %+v", restoredScoring.Received)
	}
	if restoredScoring.Rubric != "universal" || restoredScoring.Mode != "saas_gap" {
		t.Fatalf("expected compatibility bucket to backfill rubric/mode, got %+v", restoredScoring)
	}
}

func TestFactoryPipelineCoordinator_PersistRuntimeStateProjectsScanStateToWorkflowInstances(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(pipelineTestBus{}, db)
	ctx := context.Background()
	scanID := uuid.NewString()
	dedupID := uuid.NewString()

	pc.scanCoordinator.scans[scanID] = &scanAccumulator{
		ScanID:      scanID,
		CampaignID:  uuid.NewString(),
		Mode:        "saas_gap",
		Geography:   "US",
		Expected:    2,
		Reports:     1,
		Discovered:  1,
		Skipped:     0,
		CompletedBy: map[string]struct{}{"market_research.scan_complete": {}},
		CreatedAt:   time.Now().UTC().Add(-time.Minute),
		ReportData:  []map[string]any{{"scan_id": scanID, "mode": "saas_gap"}},
	}
	pc.scanCoordinator.pendingDedup[dedupID] = pendingCandidate{
		DedupEventID: dedupID,
		ScanID:       scanID,
		CampaignID:   pc.scanCoordinator.scans[scanID].CampaignID,
		Mode:         "saas_gap",
		Geography:    "US",
		Name:         "AI Intake Automation",
		Signal:       0.88,
		Payload:      map[string]any{"opportunity_name": "AI Intake Automation"},
	}

	pc.persistRuntimeState(ctx)

	inst, ok, err := pc.workflowStore.Load(ctx, scanID)
	if err != nil {
		t.Fatalf("load scan workflow instance: %v", err)
	}
	if !ok {
		t.Fatal("expected scan workflow instance")
	}
	if got := asString(inst.Metadata["instance_kind"]); got != "scan" {
		t.Fatalf("expected instance_kind=scan, got metadata=%v", inst.Metadata)
	}
	scanBucket, ok := inst.AccumulatorState["scan-state"].(map[string]any)
	if !ok {
		t.Fatalf("expected scan-state bucket, got %v", inst.AccumulatorState)
	}
	if got := asString(scanBucket["scan_id"]); got != scanID {
		t.Fatalf("expected scan-state scan_id=%s, got %q", scanID, got)
	}
	expectedScanners, ok := scanBucket["expected_scanners"].([]any)
	if !ok || len(expectedScanners) != 2 || asString(expectedScanners[0]) != "market_research" || asString(expectedScanners[1]) != "slot:2" {
		t.Fatalf("expected contract-shaped expected_scanners, got %v", scanBucket["expected_scanners"])
	}
	completedScanners, ok := scanBucket["completed_scanners"].([]any)
	if !ok || len(completedScanners) != 1 || asString(completedScanners[0]) != "market_research.scan_complete" {
		t.Fatalf("expected contract-shaped completed_scanners, got %v", scanBucket["completed_scanners"])
	}
	if got := asString(scanBucket["status"]); got != "active" {
		t.Fatalf("expected scan-state status=active, got %q", got)
	}
	for key := range scanBucket {
		switch key {
		case "scan_id", "campaign_id", "geography", "mode", "expected_scanners", "completed_scanners", "started_at", "completed_at", "status":
		default:
			t.Fatalf("unexpected non-schema scan field %q in %+v", key, scanBucket)
		}
	}
	items, ok := inst.AccumulatorState["pending-dedup"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("expected one pending-dedup entry, got %v", inst.AccumulatorState["pending-dedup"])
	}
	pendingItem, ok := items[0].(map[string]any)
	if !ok {
		t.Fatalf("expected pending-dedup map entry, got %T", items[0])
	}
	if got := asString(pendingItem["id"]); got != dedupID {
		t.Fatalf("expected discovery-shaped id=%s, got %q", dedupID, got)
	}
	if got := asString(pendingItem["opportunity_name"]); got != "AI Intake Automation" {
		t.Fatalf("expected discovery-shaped opportunity_name, got %+v", pendingItem)
	}
	if got := asString(pendingItem["status"]); got != "pending" {
		t.Fatalf("expected discovery-shaped status=pending, got %+v", pendingItem)
	}
	discoveryItems, ok := inst.AccumulatorState["discovery-aggregator"].([]any)
	if !ok || len(discoveryItems) != 1 {
		t.Fatalf("expected one discovery-aggregator entry, got %v", inst.AccumulatorState["discovery-aggregator"])
	}
	discoveryItem, ok := discoveryItems[0].(map[string]any)
	if !ok {
		t.Fatalf("expected discovery-aggregator map entry, got %T", discoveryItems[0])
	}
	for key := range discoveryItem {
		switch key {
		case "id", "opportunity_name", "signal_strength", "evidence", "status", "matched_vertical_id", "created_at":
		default:
			t.Fatalf("unexpected non-schema discovery field %q in %+v", key, discoveryItem)
		}
	}
}

func TestFactoryPipelineCoordinator_EnsureStateLoadedRestoresScanStateFromWorkflowInstances(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(pipelineTestBus{}, db)
	ctx := context.Background()
	scanID := uuid.NewString()
	dedupID := uuid.NewString()

	if err := pc.workflowStore.Upsert(ctx, WorkflowInstance{
		InstanceID:      scanID,
		WorkflowName:    "empire_vertical_pipeline",
		WorkflowVersion: "2.2.0",
		CurrentStage:    "scanning",
		Metadata:        map[string]any{"instance_kind": "scan"},
		AccumulatorState: map[string]any{
			"scan-state": map[string]any{
				"scan_id":            scanID,
				"campaign_id":        uuid.NewString(),
				"mode":               "saas_gap",
				"geography":          "US",
				"expected_scanners":  []any{"market_research", "slot:2"},
				"completed_scanners": []any{"market_research.scan_complete"},
				"started_at":         time.Now().UTC().Format(time.RFC3339Nano),
				"status":             "active",
			},
			"pending-dedup": []any{
				map[string]any{
					"id":               dedupID,
					"scan_id":          scanID,
					"campaign_id":      uuid.NewString(),
					"mode":             "saas_gap",
					"geography":        "US",
					"opportunity_name": "AI Intake Automation",
					"signal_strength":  0.88,
					"status":           "pending",
					"evidence":         map[string]any{"opportunity_name": "AI Intake Automation"},
				},
			},
		},
	}); err != nil {
		t.Fatalf("seed scan workflow instance: %v", err)
	}

	pc.ensureStateLoaded(ctx)

	acc := pc.scanCoordinator.scans[scanID]
	if acc == nil {
		t.Fatal("expected scan accumulator restored from workflow instance")
	}
	if acc.Mode != "saas_gap" || acc.Expected != 2 {
		t.Fatalf("unexpected restored scan accumulator: %+v", acc)
	}
	cand, ok := pc.scanCoordinator.pendingDedup[dedupID]
	if !ok {
		t.Fatal("expected pending dedup restored from workflow instance")
	}
	if cand.Name != "AI Intake Automation" {
		t.Fatalf("unexpected restored pending dedup: %+v", cand)
	}
}

func TestScanCoordinator_HandleDedupResolved_RestoresPendingCandidateFromWorkflowInstance(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	ctx := context.Background()
	scanID := uuid.NewString()
	dedupID := uuid.NewString()
	campaignID := uuid.NewString()

	if err := pc.workflowStore.Upsert(ctx, WorkflowInstance{
		InstanceID:      scanID,
		WorkflowName:    "empire_vertical_pipeline",
		WorkflowVersion: "2.2.0",
		CurrentStage:    "scanning",
		Metadata:        map[string]any{"instance_kind": "scan"},
		AccumulatorState: map[string]any{
			"scan-state": map[string]any{
				"scan_id":            scanID,
				"campaign_id":        campaignID,
				"mode":               "saas_gap",
				"geography":          "US",
				"expected_scanners":  []any{"market_research"},
				"completed_scanners": []any{},
				"started_at":         time.Now().UTC().Format(time.RFC3339Nano),
				"status":             "active",
			},
			"pending-dedup": []any{
				map[string]any{
					"id":               dedupID,
					"scan_id":          scanID,
					"campaign_id":      campaignID,
					"mode":             "saas_gap",
					"geography":        "US",
					"opportunity_name": "AI Intake Automation",
					"signal_strength":  0.88,
					"status":           "pending",
					"evidence":         map[string]any{"opportunity_name": "AI Intake Automation"},
				},
			},
		},
	}); err != nil {
		t.Fatalf("seed scan workflow instance: %v", err)
	}

	ch := bus.Subscribe("workflow-scan-restore", events.EventType("vertical.discovered"))
	pc.scanCoordinator.handleDedupResolved(ctx, events.Event{
		ID:   uuid.NewString(),
		Type: events.EventType("dedup.resolved"),
		Payload: mustJSON(map[string]any{
			"dedup_event_id": dedupID,
			"scan_id":        scanID,
			"action":         "keep_both",
		}),
	})

	select {
	case evt := <-ch:
		if evt.Type != events.EventType("vertical.discovered") {
			t.Fatalf("expected vertical.discovered, got %s", evt.Type)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected vertical.discovered from restored pending dedup")
	}
	if _, ok := pc.scanCoordinator.pendingDedup[dedupID]; ok {
		t.Fatal("expected restored pending dedup to be consumed")
	}
}

func TestFactoryPipelineCoordinator_EnsureStateLoadedMergesWorkflowAndLegacyState(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(pipelineTestBus{}, db)
	ctx := context.Background()

	workflowScanID := uuid.NewString()
	legacyScanID := uuid.NewString()
	workflowVerticalID := uuid.NewString()
	legacyVerticalID := uuid.NewString()
	legacyDedupID := uuid.NewString()

	for _, verticalID := range []string{workflowVerticalID, legacyVerticalID} {
		if _, err := db.ExecContext(ctx, `
			INSERT INTO verticals (id, name, slug, geography, stage)
			VALUES ($1::uuid, 'Merge Vertical', $2, 'US', 'shortlisted')
		`, verticalID, "merge-"+verticalID[:8]); err != nil {
			t.Fatalf("seed vertical %s: %v", verticalID, err)
		}
	}

	if err := pc.workflowStore.Upsert(ctx, WorkflowInstance{
		InstanceID:      workflowScanID,
		WorkflowName:    "empire_vertical_pipeline",
		WorkflowVersion: "2.2.0",
		CurrentStage:    "scanning",
		Metadata:        map[string]any{"instance_kind": "scan"},
		AccumulatorState: map[string]any{
			"scan-state": map[string]any{
				"scan_id":            workflowScanID,
				"campaign_id":        uuid.NewString(),
				"mode":               "saas_gap",
				"geography":          "US",
				"expected_scanners":  []any{"market_research"},
				"completed_scanners": []any{},
				"started_at":         time.Now().UTC().Format(time.RFC3339Nano),
				"status":             "active",
			},
		},
	}); err != nil {
		t.Fatalf("seed workflow scan instance: %v", err)
	}
	if err := pc.workflowStore.Upsert(ctx, WorkflowInstance{
		InstanceID:      workflowVerticalID,
		WorkflowName:    "empire_vertical_pipeline",
		WorkflowVersion: "2.2.0",
		CurrentStage:    "researching",
		Metadata: map[string]any{
			"status":         "active",
			"revision_count": 3,
		},
		AccumulatorState: map[string]any{
			"validation-orchestrator": map[string]any{
				"vertical_id": workflowVerticalID,
				"gate_state": map[string]any{
					"g1_research": true,
				},
				"revision_count": 3,
			},
		},
	}); err != nil {
		t.Fatalf("seed workflow validation instance: %v", err)
	}

	pc.stateStore.Persist(ctx,
		map[string]*scanAccumulator{
			legacyScanID: {
				ScanID:      legacyScanID,
				CampaignID:  uuid.NewString(),
				Mode:        "local_services",
				Geography:   "MX",
				Expected:    5,
				CompletedBy: map[string]struct{}{},
				ReportData:  []map[string]any{},
				CreatedAt:   time.Now().UTC().Add(-2 * time.Minute),
			},
		},
		map[string]pendingCandidate{
			legacyDedupID: {
				DedupEventID: legacyDedupID,
				ScanID:       legacyScanID,
				CampaignID:   uuid.NewString(),
				Mode:         "local_services",
				Geography:    "MX",
				Name:         "Legacy Candidate",
				Signal:       0.71,
				Payload:      map[string]any{"opportunity_name": "Legacy Candidate"},
			},
		},
		map[string]*validationPipelineState{
			legacyVerticalID: {
				VerticalID:      legacyVerticalID,
				Status:          "active",
				G1Research:      true,
				RevisionCount:   1,
				SpecVersion:     1,
				ResearchPayload: mustJSON(map[string]any{"legacy": true}),
			},
		},
	)

	pc.ensureStateLoaded(ctx)

	if pc.scanCoordinator.scans[workflowScanID] == nil {
		t.Fatal("expected workflow scan state to be restored")
	}
	if pc.scanCoordinator.scans[legacyScanID] == nil {
		t.Fatal("expected legacy scan state gap to be merged")
	}
	if _, ok := pc.scanCoordinator.pendingDedup[legacyDedupID]; !ok {
		t.Fatal("expected legacy pending dedup gap to be merged")
	}
	if pc.validationGate.states[workflowVerticalID] == nil {
		t.Fatal("expected workflow validation state to be restored")
	}
	if pc.validationGate.states[legacyVerticalID] == nil {
		t.Fatal("expected legacy validation state gap to be merged")
	}
}

func TestFactoryPipelineCoordinator_TransitionStateSnapshotUsesWorkflowValidationWhenMemoryEmpty(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(pipelineTestBus{}, db)
	ctx := context.Background()
	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage)
		VALUES ($1::uuid, 'Workflow Snapshot Vertical', 'workflow-snapshot-vertical', 'US', 'cto_spec_review')
	`, verticalID); err != nil {
		t.Fatalf("insert vertical: %v", err)
	}
	if err := pc.workflowStore.Upsert(ctx, WorkflowInstance{
		InstanceID:      verticalID,
		WorkflowName:    "empire_vertical_pipeline",
		WorkflowVersion: "2.2.0",
		CurrentStage:    "cto_spec_review",
		Metadata:        map[string]any{"status": "active"},
		AccumulatorState: map[string]any{
			"validation-orchestrator": map[string]any{
				"vertical_id": verticalID,
				"gate_state": map[string]any{
					"g1_research": true,
					"g2_spec":     true,
				},
				"revision_count": 4,
			},
		},
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}

	snapshot := pc.transitionStateSnapshot("spec.approved", events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("spec.approved"),
		VerticalID: verticalID,
	}, map[string]any{"vertical_id": verticalID})

	state, ok := snapshot["validation_state"].(map[string]any)
	if !ok {
		t.Fatalf("expected validation_state in snapshot, got %+v", snapshot)
	}
	if !truthyMetadataFlag(state["g1_research"]) || !truthyMetadataFlag(state["g2_spec"]) {
		t.Fatalf("expected workflow-backed gate state, got %+v", state)
	}
	if got := asInt(state["revision_count"]); got != 4 {
		t.Fatalf("expected workflow-backed revision_count=4, got %+v", state)
	}
	if got := asInt(snapshot["validations"]); got != 1 {
		t.Fatalf("expected workflow-backed validations=1, got %+v", snapshot)
	}
	if got := asInt(snapshot["scoring_active"]); got != 0 {
		t.Fatalf("expected scoring_active=0 without scoring bucket, got %+v", snapshot)
	}
}

func TestScoringState_HandleScoreDimensionComplete_RestoresAccumulatorFromWorkflowInstance(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	ctx := context.Background()
	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode)
		VALUES ($1::uuid, 'Workflow Scoring Vertical', 'workflow-scoring-vertical', 'US', 'scoring', 'saas_gap')
	`, verticalID); err != nil {
		t.Fatalf("insert vertical: %v", err)
	}
	requestedAt := time.Now().UTC().Add(-2 * time.Minute)
	if err := pc.workflowStore.Upsert(ctx, WorkflowInstance{
		InstanceID:      verticalID,
		WorkflowName:    "empire_vertical_pipeline",
		WorkflowVersion: "2.2.0",
		CurrentStage:    "scoring",
		AccumulatorState: map[string]any{
			"scoring-node": map[string]any{
				"vertical_id":          verticalID,
				"dimensions_requested": []any{"build_complexity", "automation_completeness", "distribution_leverage"},
				"dimensions_received": map[string]any{
					"build_complexity": map[string]any{"score": 81, "evidence": "workflow restore"},
				},
				"started_at": requestedAt.Format(time.RFC3339Nano),
			},
		},
	}); err != nil {
		t.Fatalf("seed scoring workflow instance: %v", err)
	}

	pc.scoringState.handleScoreDimensionComplete(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("score.dimension_complete"),
		VerticalID: verticalID,
		Payload: mustJSON(map[string]any{
			"vertical_id": verticalID,
			"dimension":   "automation_completeness",
			"score":       79,
			"evidence":    "second dimension",
		}),
	})

	acc := pc.scoringState.accumulators[verticalID]
	if acc == nil {
		t.Fatal("expected scoring accumulator to restore from workflow instance")
	}
	if got := acc.Received["build_complexity"].Score; got != 81 {
		t.Fatalf("expected restored build_complexity score=81, got %+v", acc.Received)
	}
	if got := acc.Received["automation_completeness"].Score; got != 79 {
		t.Fatalf("expected new automation_completeness score=79, got %+v", acc.Received)
	}
}

func TestFactoryPipelineCoordinator_TransitionStateSnapshotUsesWorkflowScoringCountWhenMemoryEmpty(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(pipelineTestBus{}, db)
	ctx := context.Background()
	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode)
		VALUES ($1::uuid, 'Workflow Snapshot Scoring', 'workflow-snapshot-scoring', 'US', 'scoring', 'saas_gap')
	`, verticalID); err != nil {
		t.Fatalf("insert vertical: %v", err)
	}
	if err := pc.workflowStore.Upsert(ctx, WorkflowInstance{
		InstanceID:      verticalID,
		WorkflowName:    pc.ContractBundle().Workflow.Workflow.Name,
		WorkflowVersion: pc.ContractBundle().Workflow.Workflow.Version,
		CurrentStage:    "scoring",
		AccumulatorState: map[string]any{
			"scoring-node": map[string]any{
				"vertical_id":          verticalID,
				"dimensions_requested": []any{"build_complexity"},
				"dimensions_received":  map[string]any{},
				"started_at":           time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano),
			},
		},
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}

	snapshot := pc.transitionStateSnapshot("score.dimension_complete", events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("score.dimension_complete"),
		VerticalID: verticalID,
	}, map[string]any{"vertical_id": verticalID})

	if got := asInt(snapshot["scoring_active"]); got != 1 {
		t.Fatalf("expected workflow-backed scoring_active=1, got %+v", snapshot)
	}
}

func TestFactoryPipelineCoordinator_HandleScoringRequested_RestoresAccumulatorFromWorkflowInstance(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	ctx := context.Background()
	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode)
		VALUES ($1::uuid, 'Workflow Requested Vertical', 'workflow-requested-vertical', 'US', 'scoring', 'saas_gap')
	`, verticalID); err != nil {
		t.Fatalf("insert vertical: %v", err)
	}
	if err := pc.workflowStore.Upsert(ctx, WorkflowInstance{
		InstanceID:      verticalID,
		WorkflowName:    "empire_vertical_pipeline",
		WorkflowVersion: "2.2.0",
		CurrentStage:    "scoring",
		AccumulatorState: map[string]any{
			"scoring-node": map[string]any{
				"vertical_id":          verticalID,
				"dimensions_requested": []any{"build_complexity", "automation_completeness"},
				"dimensions_received": map[string]any{
					"build_complexity": map[string]any{"score": 88, "evidence": "persisted"},
				},
				"started_at": time.Now().UTC().Add(-2 * time.Minute).Format(time.RFC3339Nano),
			},
		},
	}); err != nil {
		t.Fatalf("seed scoring workflow instance: %v", err)
	}

	pc.handleScoringRequested(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("scoring.requested"),
		VerticalID: verticalID,
		Payload: mustJSON(map[string]any{
			"vertical_id": verticalID,
			"mode":        "saas_gap",
			"name":        "Workflow Requested Vertical",
			"geography":   "US",
		}),
	})

	acc := pc.scoringState.accumulators[verticalID]
	if acc == nil {
		t.Fatal("expected scoring accumulator to restore on scoring.requested")
	}
	if got := acc.Received["build_complexity"].Score; got != 88 {
		t.Fatalf("expected restored build_complexity score=88, got %+v", acc.Received)
	}
}
