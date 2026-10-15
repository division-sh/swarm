package runtimepersistence

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestOperatorEntityReadOwnerListGetAggregateAndCursor(t *testing.T) {
	ctx := testAuthorActivityContext()
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)

	runA := uuid.NewString()
	runB := uuid.NewString()
	entityA := uuid.NewString()
	entityB := uuid.NewString()
	sharedEntity := uuid.NewString()
	base := time.Unix(1700000000, 0).UTC()
	requireRunFixtureForTest(t, ctx, pg, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runA, StartedAt: base})
	requireRunFixtureForTest(t, ctx, pg, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runB, StartedAt: base})
	seedEntity := func(runID, entityID, flow, entityType, fields string, createdAt time.Time) {
		t.Helper()
		if _, err := db.ExecContext(ctx, `
			INSERT INTO entity_state (
				run_id, entity_id, flow_instance, entity_type, slug, name,
				fields, created_at, updated_at
			) VALUES (
				$1::uuid, $2::uuid, $3, $4, NULL, NULL,
				$5::jsonb, $6, $6
			)
		`, runID, entityID, flow, entityType, fields, createdAt); err != nil {
			t.Fatalf("seed entity %s/%s: %v", runID, entityID, err)
		}
	}
	seedEntity(runA, entityA, "review/primary", "mvp_spec", `{"priority":"high","score":3}`, base.Add(time.Minute))
	seedEntity(runA, entityB, "review/secondary", "mvp_spec", `{"priority":"low"}`, base)
	seedEntity(runA, sharedEntity, "triage", "ticket", `{"priority":"high"}`, base.Add(-time.Minute))
	seedEntity(runB, sharedEntity, "triage", "ticket", `{"priority":"low"}`, base.Add(-2*time.Minute))
	if _, err := db.ExecContext(ctx, `
		INSERT INTO flow_instances (
			run_id, instance_path, entity_id, entity_type, flow_template, mode, config,
			stage_defined, current_state, gates, bookkeeping, accumulator, revision,
			entered_state_at, created_at, updated_at
		)
		VALUES
			($1::uuid, 'review/primary', $4::uuid, 'mvp_spec', 'review', 'template', $7::jsonb,
			 TRUE, 'collecting', '{"approved":true}', '{}', $10::jsonb, 1, $3, $3, $3),
			($1::uuid, 'review/secondary', $5::uuid, 'mvp_spec', 'review', 'template', $8::jsonb,
			 TRUE, 'done', '{}', '{}', '{}', 1, $3, $3, $3),
			($1::uuid, 'triage', $6::uuid, 'ticket', 'triage', 'static', $9::jsonb,
			 TRUE, 'collecting', '{}', '{}', '{}', 1, $3, $3, $3),
			($2::uuid, 'triage', $6::uuid, 'ticket', 'triage', 'static', $9::jsonb,
			 TRUE, 'done', '{}', '{}', '{}', 1, $3, $3, $3)
	`, runA, runB, base, entityA, entityB, sharedEntity,
		operatorEntityHeaderConfig(t, "review/primary", "v1"), operatorEntityHeaderConfig(t, "review/secondary", "v2"),
		operatorEntityHeaderConfig(t, "triage", "v1"), operatorLoopAccumulatedJSON(t, runA, entityA, base)); err != nil {
		t.Fatalf("seed flow instances: %v", err)
	}

	page1, err := pg.ListOperatorEntities(ctx, operatorread.OperatorEntityListOptions{
		RunID: runA,
		Flow:  "review",
		Type:  "mvp_spec",
		Limit: 1,
	})
	if err != nil {
		t.Fatalf("ListOperatorEntities page1: %v", err)
	}
	if len(page1.Entities) != 1 || page1.Entities[0].EntityID != entityA || page1.NextCursor == "" {
		t.Fatalf("page1 = %#v", page1)
	}
	if got := page1.Entities[0].EntityType; got != "mvp_spec" {
		t.Fatalf("page1 entity_type = %q, want mvp_spec", got)
	}
	page2, err := pg.ListOperatorEntities(ctx, operatorread.OperatorEntityListOptions{
		RunID:  runA,
		Flow:   "review",
		Type:   "mvp_spec",
		Limit:  1,
		Cursor: page1.NextCursor,
	})
	if err != nil {
		t.Fatalf("ListOperatorEntities page2: %v", err)
	}
	if len(page2.Entities) != 1 || page2.Entities[0].EntityID != entityB {
		t.Fatalf("page2 = %#v", page2)
	}

	full, err := pg.LoadOperatorEntity(ctx, entityA, runA)
	if err != nil {
		t.Fatalf("LoadOperatorEntity: %v", err)
	}
	if full.Entity.FlowInstance != "review/primary" || full.Fields["priority"] != "high" || !full.Gates["approved"] {
		t.Fatalf("full entity = %#v", full)
	}
	if full.Entity.EntityType != "mvp_spec" {
		t.Fatalf("full entity_type = %q, want mvp_spec", full.Entity.EntityType)
	}
	assertOperatorEntityOwnerClocks(t, full.Entity, base)
	if full.Accumulated["score"] != int64(3) {
		t.Fatalf("full accumulated = %#v, want score", full.Accumulated)
	}
	if bucket, ok := full.Accumulated["accumulator"].(map[string]any); !ok || bucket["count"] != int64(2) {
		t.Fatalf("full accumulated bucket = %#v, want count", full.Accumulated["accumulator"])
	}
	if notes, ok := full.Accumulated["notes"].([]any); !ok || len(notes) != 2 {
		t.Fatalf("full accumulated notes = %#v, want two entries", full.Accumulated["notes"])
	}
	assertOperatorLoopProjection(t, full)
	if _, err := pg.LoadOperatorEntity(ctx, sharedEntity, ""); !errors.Is(err, operatorread.ErrAmbiguousEntityRunID) {
		t.Fatalf("LoadOperatorEntity ambiguous = %v, want operatorread.ErrAmbiguousEntityRunID", err)
	}
	if _, err := pg.LoadOperatorEntity(ctx, uuid.NewString(), ""); !errors.Is(err, operatorread.ErrEntityNotFound) {
		t.Fatalf("LoadOperatorEntity missing = %v, want operatorread.ErrEntityNotFound", err)
	}

	stateAgg, err := pg.AggregateOperatorEntities(ctx, operatorread.OperatorEntityAggregateOptions{RunID: runA, GroupBy: "current_state"})
	if err != nil {
		t.Fatalf("AggregateOperatorEntities current_state: %v", err)
	}
	if stateAgg.Counts["collecting"] != 2 || stateAgg.Counts["done"] != 1 || stateAgg.AbsentCount != 0 {
		t.Fatalf("state aggregate = %#v", stateAgg.Counts)
	}
	typedStateAgg, err := pg.AggregateOperatorEntities(ctx, operatorread.OperatorEntityAggregateOptions{RunID: runA, GroupBy: "current_state", Type: "mvp_spec"})
	if err != nil {
		t.Fatalf("AggregateOperatorEntities typed current_state: %v", err)
	}
	if typedStateAgg.Counts["collecting"] != 1 || typedStateAgg.Counts["done"] != 1 || typedStateAgg.Counts["ticket"] != 0 {
		t.Fatalf("typed state aggregate = %#v", typedStateAgg.Counts)
	}
	typeAgg, err := pg.AggregateOperatorEntities(ctx, operatorread.OperatorEntityAggregateOptions{RunID: runA, GroupBy: "entity_type"})
	if err != nil {
		t.Fatalf("AggregateOperatorEntities entity_type: %v", err)
	}
	if typeAgg.Counts["mvp_spec"] != 2 || typeAgg.Counts["ticket"] != 1 || typeAgg.Counts["default"] != 0 {
		t.Fatalf("entity_type aggregate = %#v", typeAgg.Counts)
	}
	fieldAgg, err := pg.AggregateOperatorEntities(ctx, operatorread.OperatorEntityAggregateOptions{RunID: runA, GroupBy: "fields.priority"})
	if err != nil {
		t.Fatalf("AggregateOperatorEntities fields.priority: %v", err)
	}
	if fieldAgg.Counts["high"] != 2 || fieldAgg.Counts["low"] != 1 {
		t.Fatalf("field aggregate = %#v", fieldAgg.Counts)
	}
	versionAgg, err := pg.AggregateOperatorEntities(ctx, operatorread.OperatorEntityAggregateOptions{RunID: runA, GroupBy: "workflow_version"})
	if err != nil {
		t.Fatalf("AggregateOperatorEntities workflow_version: %v", err)
	}
	if versionAgg.Counts["v1"] != 2 || versionAgg.Counts["v2"] != 1 {
		t.Fatalf("workflow version aggregate = %#v", versionAgg.Counts)
	}
	nameAgg, err := pg.AggregateOperatorEntities(ctx, operatorread.OperatorEntityAggregateOptions{RunID: runA, GroupBy: "workflow_name"})
	if err != nil {
		t.Fatalf("AggregateOperatorEntities workflow_name: %v", err)
	}
	if nameAgg.Counts["review"] != 2 || nameAgg.Counts["triage"] != 1 {
		t.Fatalf("workflow name aggregate = %#v", nameAgg.Counts)
	}
	if _, err := pg.AggregateOperatorEntities(ctx, operatorread.OperatorEntityAggregateOptions{RunID: runA, GroupBy: "fields.priority->unsafe"}); !errors.Is(err, operatorread.ErrInvalidEntityReadParam) {
		t.Fatalf("AggregateOperatorEntities unsafe group = %v, want operatorread.ErrInvalidEntityReadParam", err)
	}
}

func TestSQLiteOperatorEntityReadOwnerListGetAggregateAndCursor(t *testing.T) {
	ctx := testAuthorActivityContext()
	sqliteStore := newBootstrappedSQLiteRuntimeStoreForTest(t)
	db := sqliteStore.backend.ConstructionHandle()

	runA := uuid.NewString()
	runB := uuid.NewString()
	entityA := uuid.NewString()
	entityB := uuid.NewString()
	sharedEntity := uuid.NewString()
	base := time.Unix(1700000000, 0).UTC()
	requireRunFixtureForTest(t, ctx, sqliteStore, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runA, StartedAt: base})
	requireRunFixtureForTest(t, ctx, sqliteStore, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runB, StartedAt: base})
	seedEntity := func(runID, entityID, flow, entityType, fields string, createdAt time.Time) {
		t.Helper()
		if _, err := db.ExecContext(ctx, `
			INSERT INTO entity_state (
				run_id, entity_id, flow_instance, entity_type, slug, name,
				fields, created_at, updated_at
			) VALUES (
				?, ?, ?, ?, NULL, NULL,
				?, ?, ?
			)
		`, runID, entityID, flow, entityType, fields, createdAt, createdAt); err != nil {
			t.Fatalf("seed sqlite entity %s/%s: %v", runID, entityID, err)
		}
	}
	seedEntity(runA, entityA, "review/primary", "mvp_spec", `{"priority":"high","score":3}`, base.Add(time.Minute))
	seedEntity(runA, entityB, "review/secondary", "mvp_spec", `{"priority":"low"}`, base)
	seedEntity(runA, sharedEntity, "triage", "ticket", `{"priority":"high"}`, base.Add(-time.Minute))
	seedEntity(runB, sharedEntity, "triage", "ticket", `{"priority":"low"}`, base.Add(-2*time.Minute))
	if _, err := db.ExecContext(ctx, `
		INSERT INTO flow_instances (
			run_id, instance_path, entity_id, entity_type, flow_template, mode, config,
			stage_defined, current_state, gates, bookkeeping, accumulator, revision,
			entered_state_at, created_at, updated_at
		)
		VALUES
			(?1, 'review/primary', ?4, 'mvp_spec', 'review', 'template', ?7,
			 TRUE, 'collecting', '{"approved":true}', '{}', ?10, 1, ?3, ?3, ?3),
			(?1, 'review/secondary', ?5, 'mvp_spec', 'review', 'template', ?8,
			 TRUE, 'done', '{}', '{}', '{}', 1, ?3, ?3, ?3),
			(?1, 'triage', ?6, 'ticket', 'triage', 'static', ?9,
			 TRUE, 'collecting', '{}', '{}', '{}', 1, ?3, ?3, ?3),
			(?2, 'triage', ?6, 'ticket', 'triage', 'static', ?9,
			 TRUE, 'done', '{}', '{}', '{}', 1, ?3, ?3, ?3)
	`, runA, runB, base, entityA, entityB, sharedEntity,
		operatorEntityHeaderConfig(t, "review/primary", "v1"), operatorEntityHeaderConfig(t, "review/secondary", "v2"),
		operatorEntityHeaderConfig(t, "triage", "v1"), operatorLoopAccumulatedJSON(t, runA, entityA, base)); err != nil {
		t.Fatalf("seed sqlite flow instances: %v", err)
	}

	page1, err := sqliteStore.ListOperatorEntities(ctx, operatorread.OperatorEntityListOptions{
		RunID: runA,
		Flow:  "review",
		Type:  "mvp_spec",
		Limit: 1,
	})
	if err != nil {
		t.Fatalf("ListOperatorEntities page1: %v", err)
	}
	if len(page1.Entities) != 1 || page1.Entities[0].EntityID != entityA || page1.NextCursor == "" {
		t.Fatalf("page1 = %#v", page1)
	}
	if got := page1.Entities[0].EntityType; got != "mvp_spec" {
		t.Fatalf("page1 entity_type = %q, want mvp_spec", got)
	}
	page2, err := sqliteStore.ListOperatorEntities(ctx, operatorread.OperatorEntityListOptions{
		RunID:  runA,
		Flow:   "review",
		Type:   "mvp_spec",
		Limit:  1,
		Cursor: page1.NextCursor,
	})
	if err != nil {
		t.Fatalf("ListOperatorEntities page2: %v", err)
	}
	if len(page2.Entities) != 1 || page2.Entities[0].EntityID != entityB {
		t.Fatalf("page2 = %#v", page2)
	}
	stateFiltered, err := sqliteStore.ListOperatorEntities(ctx, operatorread.OperatorEntityListOptions{RunID: runA, CurrentState: "collecting", Limit: 10})
	if err != nil {
		t.Fatalf("ListOperatorEntities current_state: %v", err)
	}
	if len(stateFiltered.Entities) != 2 {
		t.Fatalf("state filtered = %#v, want two collecting entities in runA", stateFiltered.Entities)
	}

	full, err := sqliteStore.LoadOperatorEntity(ctx, entityA, runA)
	if err != nil {
		t.Fatalf("LoadOperatorEntity: %v", err)
	}
	if full.Entity.FlowInstance != "review/primary" || full.Fields["priority"] != "high" || !full.Gates["approved"] {
		t.Fatalf("full entity = %#v", full)
	}
	if full.Entity.EntityType != "mvp_spec" {
		t.Fatalf("full entity_type = %q, want mvp_spec", full.Entity.EntityType)
	}
	assertOperatorEntityOwnerClocks(t, full.Entity, base)
	if full.Accumulated["score"] != int64(3) {
		t.Fatalf("full accumulated = %#v, want score", full.Accumulated)
	}
	if bucket, ok := full.Accumulated["accumulator"].(map[string]any); !ok || bucket["count"] != int64(2) {
		t.Fatalf("full accumulated bucket = %#v, want count", full.Accumulated["accumulator"])
	}
	assertOperatorLoopProjection(t, full)
	if notes, ok := full.Accumulated["notes"].([]any); !ok || len(notes) != 2 {
		t.Fatalf("full accumulated notes = %#v, want two entries", full.Accumulated["notes"])
	}
	if _, err := sqliteStore.LoadOperatorEntity(ctx, sharedEntity, ""); !errors.Is(err, operatorread.ErrAmbiguousEntityRunID) {
		t.Fatalf("LoadOperatorEntity ambiguous = %v, want operatorread.ErrAmbiguousEntityRunID", err)
	}
	if _, err := sqliteStore.LoadOperatorEntity(ctx, uuid.NewString(), ""); !errors.Is(err, operatorread.ErrEntityNotFound) {
		t.Fatalf("LoadOperatorEntity missing = %v, want operatorread.ErrEntityNotFound", err)
	}
	if _, err := sqliteStore.LoadOperatorEntity(ctx, "not-a-uuid", ""); !errors.Is(err, operatorread.ErrInvalidEntityReadParam) {
		t.Fatalf("LoadOperatorEntity invalid entity id = %v, want operatorread.ErrInvalidEntityReadParam", err)
	}
	if _, err := sqliteStore.LoadOperatorEntity(ctx, entityA, "not-a-uuid"); !errors.Is(err, operatorread.ErrInvalidEntityReadParam) {
		t.Fatalf("LoadOperatorEntity invalid run id = %v, want operatorread.ErrInvalidEntityReadParam", err)
	}
	if _, err := sqliteStore.ListOperatorEntities(ctx, operatorread.OperatorEntityListOptions{RunID: "not-a-uuid"}); !errors.Is(err, operatorread.ErrInvalidEntityReadParam) {
		t.Fatalf("ListOperatorEntities invalid run id = %v, want operatorread.ErrInvalidEntityReadParam", err)
	}
	if _, err := sqliteStore.ListOperatorEntities(ctx, operatorread.OperatorEntityListOptions{Cursor: "bad"}); !errors.Is(err, operatorread.ErrInvalidEntityCursor) {
		t.Fatalf("ListOperatorEntities invalid cursor = %v, want operatorread.ErrInvalidEntityCursor", err)
	}

	stateAgg, err := sqliteStore.AggregateOperatorEntities(ctx, operatorread.OperatorEntityAggregateOptions{RunID: runA, GroupBy: "current_state"})
	if err != nil {
		t.Fatalf("AggregateOperatorEntities current_state: %v", err)
	}
	if stateAgg.Counts["collecting"] != 2 || stateAgg.Counts["done"] != 1 || stateAgg.AbsentCount != 0 {
		t.Fatalf("state aggregate = %#v", stateAgg.Counts)
	}
	typedStateAgg, err := sqliteStore.AggregateOperatorEntities(ctx, operatorread.OperatorEntityAggregateOptions{RunID: runA, GroupBy: "current_state", Type: "mvp_spec"})
	if err != nil {
		t.Fatalf("AggregateOperatorEntities typed current_state: %v", err)
	}
	if typedStateAgg.Counts["collecting"] != 1 || typedStateAgg.Counts["done"] != 1 || typedStateAgg.Counts["ticket"] != 0 {
		t.Fatalf("typed state aggregate = %#v", typedStateAgg.Counts)
	}
	typeAgg, err := sqliteStore.AggregateOperatorEntities(ctx, operatorread.OperatorEntityAggregateOptions{RunID: runA, GroupBy: "entity_type"})
	if err != nil {
		t.Fatalf("AggregateOperatorEntities entity_type: %v", err)
	}
	if typeAgg.Counts["mvp_spec"] != 2 || typeAgg.Counts["ticket"] != 1 || typeAgg.Counts["default"] != 0 {
		t.Fatalf("entity_type aggregate = %#v", typeAgg.Counts)
	}
	fieldAgg, err := sqliteStore.AggregateOperatorEntities(ctx, operatorread.OperatorEntityAggregateOptions{RunID: runA, GroupBy: "fields.priority"})
	if err != nil {
		t.Fatalf("AggregateOperatorEntities fields.priority: %v", err)
	}
	if fieldAgg.Counts["high"] != 2 || fieldAgg.Counts["low"] != 1 {
		t.Fatalf("field aggregate = %#v", fieldAgg.Counts)
	}
	versionAgg, err := sqliteStore.AggregateOperatorEntities(ctx, operatorread.OperatorEntityAggregateOptions{RunID: runA, GroupBy: "workflow_version"})
	if err != nil {
		t.Fatalf("AggregateOperatorEntities workflow_version: %v", err)
	}
	if versionAgg.Counts["v1"] != 2 || versionAgg.Counts["v2"] != 1 {
		t.Fatalf("workflow version aggregate = %#v", versionAgg.Counts)
	}
	nameAgg, err := sqliteStore.AggregateOperatorEntities(ctx, operatorread.OperatorEntityAggregateOptions{RunID: runA, GroupBy: "workflow_name"})
	if err != nil {
		t.Fatalf("AggregateOperatorEntities workflow_name: %v", err)
	}
	if nameAgg.Counts["review"] != 2 || nameAgg.Counts["triage"] != 1 {
		t.Fatalf("workflow name aggregate = %#v", nameAgg.Counts)
	}
	if _, err := sqliteStore.AggregateOperatorEntities(ctx, operatorread.OperatorEntityAggregateOptions{RunID: runA, GroupBy: "fields.priority->unsafe"}); !errors.Is(err, operatorread.ErrInvalidEntityReadParam) {
		t.Fatalf("AggregateOperatorEntities unsafe group = %v, want operatorread.ErrInvalidEntityReadParam", err)
	}
	if _, err := sqliteStore.AggregateOperatorEntities(ctx, operatorread.OperatorEntityAggregateOptions{RunID: "not-a-uuid"}); !errors.Is(err, operatorread.ErrInvalidEntityReadParam) {
		t.Fatalf("AggregateOperatorEntities invalid run id = %v, want operatorread.ErrInvalidEntityReadParam", err)
	}
}

func assertOperatorEntityOwnerClocks(t *testing.T, entity operatorread.OperatorEntitySummary, headerAt time.Time) {
	t.Helper()
	if entity.StageDefined == nil || !*entity.StageDefined || entity.CurrentState == nil || *entity.CurrentState != "collecting" || entity.Revision == nil || *entity.Revision != 1 {
		t.Fatalf("operator lifecycle did not consume the seeded header: %+v", entity)
	}
	if !entity.CreatedAt.Equal(headerAt.Add(time.Minute)) || !entity.UpdatedAt.Equal(headerAt.Add(time.Minute)) {
		t.Fatalf("operator data clocks borrowed header timestamps: %+v", entity)
	}
	for _, clock := range []*time.Time{entity.EnteredStateAt, entity.LifecycleCreatedAt, entity.LifecycleUpdatedAt} {
		if clock == nil || !clock.Equal(headerAt) {
			t.Fatalf("operator lifecycle clock borrowed the data timestamp: %+v", entity)
		}
	}
}

func operatorEntityHeaderConfig(t *testing.T, path, version string) string {
	t.Helper()
	payload, err := pipeline.WorkflowInstanceHeaderPayloadForRoute(flowidentity.RouteForInstancePath(path), version)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func operatorLoopAccumulatedJSON(t *testing.T, runID, entityID string, now time.Time) string {
	t.Helper()
	activation, err := loopruntime.New(runID, entityID, "review", "revision", "revision_id", uuid.NewString(), "collecting", 3, now)
	if err != nil {
		t.Fatal(err)
	}
	buckets := map[string]map[string]any{}
	if err := loopruntime.Store(buckets, activation); err != nil {
		t.Fatal(err)
	}
	persisted := map[string]any{
		"score": float64(3), "accumulator": map[string]any{"count": float64(2)},
		"notes":               []any{"a", map[string]any{"text": "probe"}},
		loopruntime.BucketKey: buckets[loopruntime.BucketKey],
	}
	raw, err := json.Marshal(persisted)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func assertOperatorLoopProjection(t *testing.T, full operatorread.OperatorEntityFull) {
	t.Helper()
	if _, leaked := full.Accumulated[loopruntime.BucketKey]; leaked {
		t.Fatalf("reserved loop state leaked through accumulated readback: %#v", full.Accumulated)
	}
	if len(full.Loops) != 1 || full.Loops[0].ID != "revision" || full.Loops[0].Attempt != 1 || full.Loops[0].Status != loopruntime.StatusOpen {
		t.Fatalf("typed loop projection = %#v", full.Loops)
	}
}
