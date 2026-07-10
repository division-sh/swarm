package store

import (
	"context"
	"errors"
	"testing"
	"time"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestRunAPIReadSurface_LoadAndListRunHeaders(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	now := time.Unix(1700000000, 0).UTC()
	newer := uuid.NewString()
	middle := uuid.NewString()
	older := uuid.NewString()
	newerEvent := uuid.NewString()
	middleEvent := uuid.NewString()
	olderEvent := uuid.NewString()
	newerEntityA := uuid.NewString()
	newerEntityB := uuid.NewString()
	middleEntity := uuid.NewString()
	olderEventOnlyA := uuid.NewString()
	olderEventOnlyB := uuid.NewString()
	bundleA := "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	bundleB := "bundle-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	failedRunFailure := mustMarshalTestFailure(t, testFailureEnvelope(runtimefailures.ClassInternalFailure, "run_failed", nil))
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (
			run_id, status, bundle_hash, bundle_source, trigger_event_id, trigger_event_type, forked_from_run_id, entity_count, event_count, failure, started_at, ended_at
		)
		VALUES
			($1::uuid, 'running', $2, 'persisted', $3::uuid, 'scan.requested', NULL, 3, 2, NULL, $4, NULL),
			($5::uuid, 'completed', $6, 'persisted', $7::uuid, 'scan.requested', $1::uuid, 5, 1, NULL, $8, $9),
			($10::uuid, 'failed', $2, 'persisted', $11::uuid, 'scan.failed', NULL, 1, 1, $14::jsonb, $12, $13)
	`, newer, bundleA, newerEvent, now, middle, bundleB, middleEvent, now.Add(-time.Hour), now.Add(-30*time.Minute), older, olderEvent, now.Add(-2*time.Hour), now.Add(-90*time.Minute), failedRunFailure); err != nil {
		t.Fatalf("seed runs: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (run_id, event_id, event_name, entity_id, scope, payload, produced_by, produced_by_type, created_at)
		VALUES
			($1::uuid, $2::uuid, 'scan.requested', NULL, 'global', '{}'::jsonb, 'test', 'agent', $3),
			($1::uuid, gen_random_uuid(), 'scan.completed', NULL, 'global', '{}'::jsonb, 'test', 'agent', $4),
			($5::uuid, $6::uuid, 'scan.requested', NULL, 'global', '{}'::jsonb, 'test', 'agent', $7),
			($8::uuid, $9::uuid, 'scan.failed', $10::uuid, 'global', '{}'::jsonb, 'test', 'agent', $12),
			($8::uuid, gen_random_uuid(), 'scan.replayed', $11::uuid, 'global', '{}'::jsonb, 'test', 'agent', $13)
	`, newer, newerEvent, now.Add(time.Second), now.Add(2*time.Second), middle, middleEvent, now.Add(-time.Hour+time.Second), older, olderEvent, olderEventOnlyA, olderEventOnlyB, now.Add(-2*time.Hour+time.Second), now.Add(-2*time.Hour+2*time.Second)); err != nil {
		t.Fatalf("seed events: %v", err)
	}
	seedPostgresEntityStateRows(t, db, ctx, newer, newerEntityA, newerEntityB)
	seedPostgresEntityStateRows(t, db, ctx, middle, middleEntity)

	header, err := pg.LoadRunHeader(ctx, middle)
	if err != nil {
		t.Fatalf("LoadRunHeader: %v", err)
	}
	if header.RunID != middle || header.Status != "completed" || header.TriggerEventID != middleEvent || header.ForkedFromRunID != newer {
		t.Fatalf("header = %#v", header)
	}
	if header.EndedAt == nil {
		t.Fatalf("header.EndedAt = nil, want terminal timestamp")
	}
	if header.EntityCount != 1 {
		t.Fatalf("header.EntityCount = %d, want entity_state count 1 despite stale run counter", header.EntityCount)
	}

	firstPage, cursor, err := pg.ListRunHeaders(ctx, RunHeaderListOptions{Status: "running", Limit: 1})
	if err != nil {
		t.Fatalf("ListRunHeaders first page: %v", err)
	}
	if len(firstPage) != 1 || firstPage[0].RunID != newer {
		t.Fatalf("first page = %#v, want newer running run", firstPage)
	}
	if firstPage[0].EntityCount != 2 {
		t.Fatalf("first page entity_count = %d, want entity_state count 2 despite event undercount", firstPage[0].EntityCount)
	}
	if cursor != "" {
		t.Fatalf("running-only cursor = %q, want empty", cursor)
	}

	allFirstPage, cursor, err := pg.ListRunHeaders(ctx, RunHeaderListOptions{Limit: 2})
	if err != nil {
		t.Fatalf("ListRunHeaders all first page: %v", err)
	}
	if len(allFirstPage) != 2 || allFirstPage[0].RunID != newer || allFirstPage[1].RunID != middle {
		t.Fatalf("all first page = %#v", allFirstPage)
	}
	if cursor == "" {
		t.Fatal("cursor empty for truncated run list")
	}
	allSecondPage, next, err := pg.ListRunHeaders(ctx, RunHeaderListOptions{Limit: 2, Cursor: cursor})
	if err != nil {
		t.Fatalf("ListRunHeaders all second page: %v", err)
	}
	if len(allSecondPage) != 1 || allSecondPage[0].RunID != older || next != "" {
		t.Fatalf("all second page = %#v cursor=%q, want older only and no next cursor", allSecondPage, next)
	}
	if allSecondPage[0].EntityCount != 0 {
		t.Fatalf("older entity_count = %d, want entity_state count 0 despite event overcount", allSecondPage[0].EntityCount)
	}

	since := now.Add(-90 * time.Minute)
	recent, _, err := pg.ListRunHeaders(ctx, RunHeaderListOptions{Since: &since})
	if err != nil {
		t.Fatalf("ListRunHeaders since: %v", err)
	}
	if len(recent) != 2 {
		t.Fatalf("recent runs len = %d, want 2: %#v", len(recent), recent)
	}
	bundleRuns, _, err := pg.ListRunHeaders(ctx, RunHeaderListOptions{BundleHash: bundleA, Limit: 10})
	if err != nil {
		t.Fatalf("ListRunHeaders bundle_hash: %v", err)
	}
	if len(bundleRuns) != 2 || bundleRuns[0].RunID != newer || bundleRuns[1].RunID != older {
		t.Fatalf("bundle-filtered runs = %#v, want newer and older only", bundleRuns)
	}
	runningBundleRuns, _, err := pg.ListRunHeaders(ctx, RunHeaderListOptions{Status: "running", BundleHash: bundleA, Limit: 10})
	if err != nil {
		t.Fatalf("ListRunHeaders status+bundle_hash: %v", err)
	}
	if len(runningBundleRuns) != 1 || runningBundleRuns[0].RunID != newer {
		t.Fatalf("status+bundle-filtered runs = %#v, want newer only", runningBundleRuns)
	}
}

func TestRunAPIReadSurface_LoadRunHeaderNotFound(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	_, err := pg.LoadRunHeader(context.Background(), uuid.NewString())
	if !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("LoadRunHeader error = %v, want ErrRunNotFound", err)
	}
}
