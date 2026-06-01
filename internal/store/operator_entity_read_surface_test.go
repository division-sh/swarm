package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestOperatorEntityReadOwnerListGetAggregateAndCursor(t *testing.T) {
	ctx := context.Background()
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}

	runA := uuid.NewString()
	runB := uuid.NewString()
	entityA := uuid.NewString()
	entityB := uuid.NewString()
	sharedEntity := uuid.NewString()
	base := time.Unix(1700000000, 0).UTC()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at) VALUES
			($1::uuid, 'running', $3),
			($2::uuid, 'running', $3)
	`, runA, runB, base); err != nil {
		t.Fatalf("seed runs: %v", err)
	}
	seedEntity := func(runID, entityID, flow, entityType, state, fields, gates, accumulated string, createdAt time.Time) {
		t.Helper()
		if _, err := db.ExecContext(ctx, `
			INSERT INTO entity_state (
				run_id, entity_id, flow_instance, entity_type, slug, name,
				current_state, gates, fields, accumulator, revision,
				entered_state_at, created_at, updated_at
			) VALUES (
				$1::uuid, $2::uuid, $3, $4, NULL, NULL,
				$5, $6::jsonb, $7::jsonb, $8::jsonb, 1,
				$9, $9, $9
			)
		`, runID, entityID, flow, entityType, state, gates, fields, accumulated, createdAt); err != nil {
			t.Fatalf("seed entity %s/%s: %v", runID, entityID, err)
		}
	}
	seedEntity(runA, entityA, "review/primary", "mvp_spec", "collecting", `{"priority":"high","score":3}`, `{"approved":true}`, `{"score":3,"accumulator":{"count":2},"notes":["a",{"text":"probe"}]}`, base.Add(time.Minute))
	seedEntity(runA, entityB, "review/secondary", "mvp_spec", "done", `{"priority":"low"}`, `{}`, `{}`, base)
	seedEntity(runA, sharedEntity, "triage", "ticket", "collecting", `{"priority":"high"}`, `{}`, `{}`, base.Add(-time.Minute))
	seedEntity(runB, sharedEntity, "triage", "ticket", "done", `{"priority":"low"}`, `{}`, `{}`, base.Add(-2*time.Minute))
	if _, err := db.ExecContext(ctx, `
		INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
		VALUES
			('review/primary', 'review', 'template', '{"workflow_version":"v1"}'::jsonb, 'active', $1),
			('review/secondary', 'review', 'template', '{"workflow_version":"v2"}'::jsonb, 'active', $1),
			('triage', 'triage', 'static', '{"workflow_version":"v1"}'::jsonb, 'active', $1)
	`, base); err != nil {
		t.Fatalf("seed flow instances: %v", err)
	}

	page1, err := pg.ListOperatorEntities(ctx, OperatorEntityListOptions{
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
	page2, err := pg.ListOperatorEntities(ctx, OperatorEntityListOptions{
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
	if full.Accumulated["score"] != float64(3) {
		t.Fatalf("full accumulated = %#v, want score", full.Accumulated)
	}
	if bucket, ok := full.Accumulated["accumulator"].(map[string]any); !ok || bucket["count"] != float64(2) {
		t.Fatalf("full accumulated bucket = %#v, want count", full.Accumulated["accumulator"])
	}
	if notes, ok := full.Accumulated["notes"].([]any); !ok || len(notes) != 2 {
		t.Fatalf("full accumulated notes = %#v, want two entries", full.Accumulated["notes"])
	}
	if _, err := pg.LoadOperatorEntity(ctx, sharedEntity, ""); !errors.Is(err, ErrAmbiguousEntityRunID) {
		t.Fatalf("LoadOperatorEntity ambiguous = %v, want ErrAmbiguousEntityRunID", err)
	}
	if _, err := pg.LoadOperatorEntity(ctx, uuid.NewString(), ""); !errors.Is(err, ErrEntityNotFound) {
		t.Fatalf("LoadOperatorEntity missing = %v, want ErrEntityNotFound", err)
	}

	stateAgg, err := pg.AggregateOperatorEntities(ctx, OperatorEntityAggregateOptions{RunID: runA, GroupBy: "current_state"})
	if err != nil {
		t.Fatalf("AggregateOperatorEntities current_state: %v", err)
	}
	if stateAgg.Counts["collecting"] != 2 || stateAgg.Counts["done"] != 1 {
		t.Fatalf("state aggregate = %#v", stateAgg.Counts)
	}
	typedStateAgg, err := pg.AggregateOperatorEntities(ctx, OperatorEntityAggregateOptions{RunID: runA, GroupBy: "current_state", Type: "mvp_spec"})
	if err != nil {
		t.Fatalf("AggregateOperatorEntities typed current_state: %v", err)
	}
	if typedStateAgg.Counts["collecting"] != 1 || typedStateAgg.Counts["done"] != 1 || typedStateAgg.Counts["ticket"] != 0 {
		t.Fatalf("typed state aggregate = %#v", typedStateAgg.Counts)
	}
	typeAgg, err := pg.AggregateOperatorEntities(ctx, OperatorEntityAggregateOptions{RunID: runA, GroupBy: "entity_type"})
	if err != nil {
		t.Fatalf("AggregateOperatorEntities entity_type: %v", err)
	}
	if typeAgg.Counts["mvp_spec"] != 2 || typeAgg.Counts["ticket"] != 1 || typeAgg.Counts["default"] != 0 {
		t.Fatalf("entity_type aggregate = %#v", typeAgg.Counts)
	}
	fieldAgg, err := pg.AggregateOperatorEntities(ctx, OperatorEntityAggregateOptions{RunID: runA, GroupBy: "fields.priority"})
	if err != nil {
		t.Fatalf("AggregateOperatorEntities fields.priority: %v", err)
	}
	if fieldAgg.Counts["high"] != 2 || fieldAgg.Counts["low"] != 1 {
		t.Fatalf("field aggregate = %#v", fieldAgg.Counts)
	}
	versionAgg, err := pg.AggregateOperatorEntities(ctx, OperatorEntityAggregateOptions{RunID: runA, GroupBy: "workflow_version"})
	if err != nil {
		t.Fatalf("AggregateOperatorEntities workflow_version: %v", err)
	}
	if versionAgg.Counts["v1"] != 2 || versionAgg.Counts["v2"] != 1 {
		t.Fatalf("workflow version aggregate = %#v", versionAgg.Counts)
	}
	nameAgg, err := pg.AggregateOperatorEntities(ctx, OperatorEntityAggregateOptions{RunID: runA, GroupBy: "workflow_name"})
	if err != nil {
		t.Fatalf("AggregateOperatorEntities workflow_name: %v", err)
	}
	if nameAgg.Counts["review"] != 2 || nameAgg.Counts["triage"] != 1 {
		t.Fatalf("workflow name aggregate = %#v", nameAgg.Counts)
	}
	if _, err := pg.AggregateOperatorEntities(ctx, OperatorEntityAggregateOptions{RunID: runA, GroupBy: "fields.priority->unsafe"}); !errors.Is(err, ErrInvalidEntityReadParam) {
		t.Fatalf("AggregateOperatorEntities unsafe group = %v, want ErrInvalidEntityReadParam", err)
	}
}

func TestSQLiteOperatorEntityReadOwnerListGetAggregateAndCursor(t *testing.T) {
	ctx := context.Background()
	sqliteStore := newBootstrappedSQLiteRuntimeStoreForTest(t)
	db := sqliteStore.DB

	runA := uuid.NewString()
	runB := uuid.NewString()
	entityA := uuid.NewString()
	entityB := uuid.NewString()
	sharedEntity := uuid.NewString()
	base := time.Unix(1700000000, 0).UTC()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at) VALUES
			(?, 'running', ?),
			(?, 'running', ?)
	`, runA, base, runB, base); err != nil {
		t.Fatalf("seed sqlite runs: %v", err)
	}
	seedEntity := func(runID, entityID, flow, entityType, state, fields, gates, accumulated string, createdAt time.Time) {
		t.Helper()
		if _, err := db.ExecContext(ctx, `
			INSERT INTO entity_state (
				run_id, entity_id, flow_instance, entity_type, slug, name,
				current_state, gates, fields, accumulator, revision,
				entered_state_at, created_at, updated_at
			) VALUES (
				?, ?, ?, ?, NULL, NULL,
				?, ?, ?, ?, 1,
				?, ?, ?
			)
		`, runID, entityID, flow, entityType, state, gates, fields, accumulated, createdAt, createdAt, createdAt); err != nil {
			t.Fatalf("seed sqlite entity %s/%s: %v", runID, entityID, err)
		}
	}
	seedEntity(runA, entityA, "review/primary", "mvp_spec", "collecting", `{"priority":"high","score":3}`, `{"approved":true}`, `{"score":3,"accumulator":{"count":2},"notes":["a",{"text":"probe"}]}`, base.Add(time.Minute))
	seedEntity(runA, entityB, "review/secondary", "mvp_spec", "done", `{"priority":"low"}`, `{}`, `{}`, base)
	seedEntity(runA, sharedEntity, "triage", "ticket", "collecting", `{"priority":"high"}`, `{}`, `{}`, base.Add(-time.Minute))
	seedEntity(runB, sharedEntity, "triage", "ticket", "done", `{"priority":"low"}`, `{}`, `{}`, base.Add(-2*time.Minute))
	if _, err := db.ExecContext(ctx, `
		INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
		VALUES
			('review/primary', 'review', 'template', '{"workflow_version":"v1"}', 'active', ?),
			('review/secondary', 'review', 'template', '{"workflow_version":"v2"}', 'active', ?),
			('triage', 'triage', 'static', '{"workflow_version":"v1"}', 'active', ?)
	`, base, base, base); err != nil {
		t.Fatalf("seed sqlite flow instances: %v", err)
	}

	page1, err := sqliteStore.ListOperatorEntities(ctx, OperatorEntityListOptions{
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
	page2, err := sqliteStore.ListOperatorEntities(ctx, OperatorEntityListOptions{
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
	stateFiltered, err := sqliteStore.ListOperatorEntities(ctx, OperatorEntityListOptions{RunID: runA, CurrentState: "collecting", Limit: 10})
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
	if full.Accumulated["score"] != float64(3) {
		t.Fatalf("full accumulated = %#v, want score", full.Accumulated)
	}
	if bucket, ok := full.Accumulated["accumulator"].(map[string]any); !ok || bucket["count"] != float64(2) {
		t.Fatalf("full accumulated bucket = %#v, want count", full.Accumulated["accumulator"])
	}
	if notes, ok := full.Accumulated["notes"].([]any); !ok || len(notes) != 2 {
		t.Fatalf("full accumulated notes = %#v, want two entries", full.Accumulated["notes"])
	}
	if _, err := sqliteStore.LoadOperatorEntity(ctx, sharedEntity, ""); !errors.Is(err, ErrAmbiguousEntityRunID) {
		t.Fatalf("LoadOperatorEntity ambiguous = %v, want ErrAmbiguousEntityRunID", err)
	}
	if _, err := sqliteStore.LoadOperatorEntity(ctx, uuid.NewString(), ""); !errors.Is(err, ErrEntityNotFound) {
		t.Fatalf("LoadOperatorEntity missing = %v, want ErrEntityNotFound", err)
	}
	if _, err := sqliteStore.LoadOperatorEntity(ctx, "not-a-uuid", ""); !errors.Is(err, ErrInvalidEntityReadParam) {
		t.Fatalf("LoadOperatorEntity invalid entity id = %v, want ErrInvalidEntityReadParam", err)
	}
	if _, err := sqliteStore.LoadOperatorEntity(ctx, entityA, "not-a-uuid"); !errors.Is(err, ErrInvalidEntityReadParam) {
		t.Fatalf("LoadOperatorEntity invalid run id = %v, want ErrInvalidEntityReadParam", err)
	}
	if _, err := sqliteStore.ListOperatorEntities(ctx, OperatorEntityListOptions{RunID: "not-a-uuid"}); !errors.Is(err, ErrInvalidEntityReadParam) {
		t.Fatalf("ListOperatorEntities invalid run id = %v, want ErrInvalidEntityReadParam", err)
	}
	if _, err := sqliteStore.ListOperatorEntities(ctx, OperatorEntityListOptions{Cursor: "bad"}); !errors.Is(err, ErrInvalidEntityCursor) {
		t.Fatalf("ListOperatorEntities invalid cursor = %v, want ErrInvalidEntityCursor", err)
	}

	stateAgg, err := sqliteStore.AggregateOperatorEntities(ctx, OperatorEntityAggregateOptions{RunID: runA, GroupBy: "current_state"})
	if err != nil {
		t.Fatalf("AggregateOperatorEntities current_state: %v", err)
	}
	if stateAgg.Counts["collecting"] != 2 || stateAgg.Counts["done"] != 1 {
		t.Fatalf("state aggregate = %#v", stateAgg.Counts)
	}
	typedStateAgg, err := sqliteStore.AggregateOperatorEntities(ctx, OperatorEntityAggregateOptions{RunID: runA, GroupBy: "current_state", Type: "mvp_spec"})
	if err != nil {
		t.Fatalf("AggregateOperatorEntities typed current_state: %v", err)
	}
	if typedStateAgg.Counts["collecting"] != 1 || typedStateAgg.Counts["done"] != 1 || typedStateAgg.Counts["ticket"] != 0 {
		t.Fatalf("typed state aggregate = %#v", typedStateAgg.Counts)
	}
	typeAgg, err := sqliteStore.AggregateOperatorEntities(ctx, OperatorEntityAggregateOptions{RunID: runA, GroupBy: "entity_type"})
	if err != nil {
		t.Fatalf("AggregateOperatorEntities entity_type: %v", err)
	}
	if typeAgg.Counts["mvp_spec"] != 2 || typeAgg.Counts["ticket"] != 1 || typeAgg.Counts["default"] != 0 {
		t.Fatalf("entity_type aggregate = %#v", typeAgg.Counts)
	}
	fieldAgg, err := sqliteStore.AggregateOperatorEntities(ctx, OperatorEntityAggregateOptions{RunID: runA, GroupBy: "fields.priority"})
	if err != nil {
		t.Fatalf("AggregateOperatorEntities fields.priority: %v", err)
	}
	if fieldAgg.Counts["high"] != 2 || fieldAgg.Counts["low"] != 1 {
		t.Fatalf("field aggregate = %#v", fieldAgg.Counts)
	}
	versionAgg, err := sqliteStore.AggregateOperatorEntities(ctx, OperatorEntityAggregateOptions{RunID: runA, GroupBy: "workflow_version"})
	if err != nil {
		t.Fatalf("AggregateOperatorEntities workflow_version: %v", err)
	}
	if versionAgg.Counts["v1"] != 2 || versionAgg.Counts["v2"] != 1 {
		t.Fatalf("workflow version aggregate = %#v", versionAgg.Counts)
	}
	nameAgg, err := sqliteStore.AggregateOperatorEntities(ctx, OperatorEntityAggregateOptions{RunID: runA, GroupBy: "workflow_name"})
	if err != nil {
		t.Fatalf("AggregateOperatorEntities workflow_name: %v", err)
	}
	if nameAgg.Counts["review"] != 2 || nameAgg.Counts["triage"] != 1 {
		t.Fatalf("workflow name aggregate = %#v", nameAgg.Counts)
	}
	if _, err := sqliteStore.AggregateOperatorEntities(ctx, OperatorEntityAggregateOptions{RunID: runA, GroupBy: "fields.priority->unsafe"}); !errors.Is(err, ErrInvalidEntityReadParam) {
		t.Fatalf("AggregateOperatorEntities unsafe group = %v, want ErrInvalidEntityReadParam", err)
	}
	if _, err := sqliteStore.AggregateOperatorEntities(ctx, OperatorEntityAggregateOptions{RunID: "not-a-uuid"}); !errors.Is(err, ErrInvalidEntityReadParam) {
		t.Fatalf("AggregateOperatorEntities invalid run id = %v, want ErrInvalidEntityReadParam", err)
	}
}
