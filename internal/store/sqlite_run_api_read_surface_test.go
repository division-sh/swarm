package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestSQLiteRunAPIReadSurface_LoadListAndDiagnoseEvidence(t *testing.T) {
	ctx := context.Background()
	sqliteStore := newBootstrappedSQLiteRuntimeStoreForTest(t)
	now := time.Unix(1700000000, 0).UTC()
	newer := uuid.NewString()
	older := uuid.NewString()
	newerEvent := uuid.NewString()
	olderEvent := uuid.NewString()
	bundleA := "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	bundleB := "bundle-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

	if _, err := sqliteStore.DB.ExecContext(ctx, `
		INSERT INTO runs (
			run_id, status, bundle_hash, bundle_source, trigger_event_id, trigger_event_type,
			forked_from_run_id, entity_count, event_count, error_summary, started_at, ended_at
		)
		VALUES
			(?, 'running', ?, 'ephemeral', ?, 'scan.requested', NULL, 3, 0, NULL, ?, NULL),
			(?, 'completed', ?, 'ephemeral', ?, 'scan.completed', ?, 5, 0, NULL, ?, ?)
	`, newer, bundleA, newerEvent, now, older, bundleB, olderEvent, newer, now.Add(-time.Hour), now.Add(-30*time.Minute)); err != nil {
		t.Fatalf("seed sqlite runs: %v", err)
	}
	if _, err := sqliteStore.DB.ExecContext(ctx, `
		INSERT INTO events (run_id, event_id, event_name, scope, payload, produced_by, produced_by_type, created_at)
		VALUES
			(?, ?, 'scan.requested', 'global', '{}', 'test', 'agent', ?),
			(?, ?, 'scan.completed', 'global', '{}', 'test', 'agent', ?),
			(?, ?, 'platform.runtime_log', 'global', '{"log_level":"error","message":"boom","details":{"component":"runtime","action":"proof","error_code":"E_PROOF"}}', 'runtime', 'platform', ?)
	`, newer, newerEvent, now.Add(time.Second), older, olderEvent, now.Add(-time.Hour+time.Second), newer, uuid.NewString(), now.Add(2*time.Second)); err != nil {
		t.Fatalf("seed sqlite events: %v", err)
	}
	if _, err := sqliteStore.DB.ExecContext(ctx, `
		INSERT INTO event_deliveries (delivery_id, run_id, event_id, subscriber_type, subscriber_id, status, created_at)
		VALUES (?, ?, ?, 'agent', 'agent-1', 'pending', ?)
	`, uuid.NewString(), newer, newerEvent, now.Add(3*time.Second)); err != nil {
		t.Fatalf("seed sqlite delivery: %v", err)
	}

	header, err := sqliteStore.LoadRunHeader(ctx, older)
	if err != nil {
		t.Fatalf("LoadRunHeader: %v", err)
	}
	if header.RunID != older || header.Status != "completed" || header.TriggerEventID != olderEvent || header.ForkedFromRunID != newer {
		t.Fatalf("header = %#v", header)
	}
	if header.EndedAt == nil {
		t.Fatal("header.EndedAt = nil, want terminal timestamp")
	}

	firstPage, cursor, err := sqliteStore.ListRunHeaders(ctx, RunHeaderListOptions{Limit: 1})
	if err != nil {
		t.Fatalf("ListRunHeaders first page: %v", err)
	}
	if len(firstPage) != 1 || firstPage[0].RunID != newer {
		t.Fatalf("first page = %#v, want newer run", firstPage)
	}
	if cursor == "" {
		t.Fatal("cursor empty for truncated sqlite run list")
	}
	secondPage, next, err := sqliteStore.ListRunHeaders(ctx, RunHeaderListOptions{Limit: 1, Cursor: cursor})
	if err != nil {
		t.Fatalf("ListRunHeaders second page: %v", err)
	}
	if len(secondPage) != 1 || secondPage[0].RunID != older || next != "" {
		t.Fatalf("second page = %#v cursor=%q, want older only and no next cursor", secondPage, next)
	}
	filtered, _, err := sqliteStore.ListRunHeaders(ctx, RunHeaderListOptions{Status: "running", BundleHash: bundleA, Limit: 10})
	if err != nil {
		t.Fatalf("ListRunHeaders filtered: %v", err)
	}
	if len(filtered) != 1 || filtered[0].RunID != newer {
		t.Fatalf("filtered = %#v, want newer only", filtered)
	}

	report, err := sqliteStore.LoadRunDebugReport(ctx, newer, RunDebugQueryOptions{})
	if err != nil {
		t.Fatalf("LoadRunDebugReport: %v", err)
	}
	if report.RunID != newer || report.RootEventID != newerEvent || report.WarnErrorLogCount != 1 {
		t.Fatalf("report = %#v", report)
	}
	if len(report.Deliveries) != 1 || report.Deliveries[0].SubscriberID != "agent-1" {
		t.Fatalf("report deliveries = %#v, want agent-1 delivery count", report.Deliveries)
	}
	if len(report.RuntimeLogs) != 1 || report.RuntimeLogs[0].Component != "runtime" || report.RuntimeLogs[0].Action != "proof" {
		t.Fatalf("runtime logs = %#v, want runtime proof log", report.RuntimeLogs)
	}
}

func TestSQLiteRunAPIReadSurface_LoadRunHeaderNotFound(t *testing.T) {
	sqliteStore := newBootstrappedSQLiteRuntimeStoreForTest(t)
	_, err := sqliteStore.LoadRunHeader(context.Background(), uuid.NewString())
	if !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("LoadRunHeader error = %v, want ErrRunNotFound", err)
	}
}
