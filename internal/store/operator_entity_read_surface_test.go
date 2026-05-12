package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"swarm/internal/testutil"
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
	seedEntity(runA, entityA, "review/primary", "mvp_spec", "collecting", `{"priority":"high","score":3}`, `{"approved":true}`, `{"notes":["a"]}`, base.Add(time.Minute))
	seedEntity(runA, entityB, "review/secondary", "mvp_spec", "done", `{"priority":"low"}`, `{}`, `{}`, base)
	seedEntity(runA, sharedEntity, "triage", "ticket", "collecting", `{"priority":"high"}`, `{}`, `{}`, base.Add(-time.Minute))
	seedEntity(runB, sharedEntity, "triage", "ticket", "done", `{"priority":"low"}`, `{}`, `{}`, base.Add(-2*time.Minute))

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
	fieldAgg, err := pg.AggregateOperatorEntities(ctx, OperatorEntityAggregateOptions{RunID: runA, GroupBy: "fields.priority"})
	if err != nil {
		t.Fatalf("AggregateOperatorEntities fields.priority: %v", err)
	}
	if fieldAgg.Counts["high"] != 2 || fieldAgg.Counts["low"] != 1 {
		t.Fatalf("field aggregate = %#v", fieldAgg.Counts)
	}
	if _, err := pg.AggregateOperatorEntities(ctx, OperatorEntityAggregateOptions{RunID: runA, GroupBy: "fields.priority->unsafe"}); !errors.Is(err, ErrInvalidEntityReadParam) {
		t.Fatalf("AggregateOperatorEntities unsafe group = %v, want ErrInvalidEntityReadParam", err)
	}
}
