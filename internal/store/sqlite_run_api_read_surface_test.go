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
	newerMiddleEvent := uuid.NewString()
	newerLatestEvent := uuid.NewString()
	olderEvent := uuid.NewString()
	newerEntityA := uuid.NewString()
	newerEntityB := uuid.NewString()
	olderEntity := uuid.NewString()
	olderEventOnly := uuid.NewString()
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
		INSERT INTO events (run_id, event_id, event_name, entity_id, scope, payload, produced_by, produced_by_type, created_at)
		VALUES
			(?, ?, 'scan.requested', NULL, 'global', '{}', 'test', 'agent', ?),
			(?, ?, 'scan.progressed', NULL, 'global', '{}', 'test', 'agent', ?),
			(?, ?, 'scan.finished', NULL, 'global', '{}', 'test', 'agent', ?),
			(?, ?, 'scan.completed', ?, 'global', '{}', 'test', 'agent', ?),
			(?, ?, 'scan.replayed', ?, 'global', '{}', 'test', 'agent', ?),
			(?, ?, 'platform.runtime_log', NULL, 'global', '{"log_level":"error","message":"boom","details":{"component":"runtime","action":"proof","error_code":"E_PROOF"}}', 'runtime', 'platform', ?)
	`, newer, newerEvent, now.Add(time.Second), newer, newerMiddleEvent, now.Add(2*time.Second), newer, newerLatestEvent, now.Add(3*time.Second), older, olderEvent, olderEntity, now.Add(-time.Hour+time.Second), older, uuid.NewString(), olderEventOnly, now.Add(-time.Hour+2*time.Second), newer, uuid.NewString(), now.Add(4*time.Second)); err != nil {
		t.Fatalf("seed sqlite events: %v", err)
	}
	seedSQLiteEntityStateRows(t, sqliteStore.DB, ctx, newer, newerEntityA, newerEntityB)
	seedSQLiteEntityStateRows(t, sqliteStore.DB, ctx, older, olderEntity)
	if _, err := sqliteStore.DB.ExecContext(ctx, `
			INSERT INTO event_deliveries (delivery_id, run_id, event_id, subscriber_type, subscriber_id, status, created_at)
			VALUES (?, ?, ?, 'agent', 'agent-1', 'pending', ?)
		`, uuid.NewString(), newer, newerEvent, now.Add(3*time.Second)); err != nil {
		t.Fatalf("seed sqlite delivery: %v", err)
	}
	agentFailedDeliveryID := uuid.NewString()
	nodeDeadDeliveryID := uuid.NewString()
	if _, err := sqliteStore.DB.ExecContext(ctx, `
			INSERT INTO event_deliveries (
				delivery_id, run_id, event_id, subscriber_type, subscriber_id, status,
				retry_count, reason_code, last_error, created_at, started_at, delivered_at
			)
			VALUES
				(?, ?, ?, 'agent', 'agent-failed', 'failed', 1, 'handler_error', 'agent boom', ?, ?, NULL),
				(?, ?, ?, 'node', 'node-dead', 'dead_letter', 2, 'retry_exhausted', 'node boom', ?, ?, ?)
	`, agentFailedDeliveryID, newer, newerMiddleEvent, now.Add(4*time.Second), now.Add(5*time.Second), nodeDeadDeliveryID, newer, newerLatestEvent, now.Add(6*time.Second), now.Add(7*time.Second), now.Add(8*time.Second)); err != nil {
		t.Fatalf("seed sqlite failed deliveries: %v", err)
	}
	successDeliveryID := uuid.NewString()
	if _, err := sqliteStore.DB.ExecContext(ctx, `
			INSERT INTO event_deliveries (
				delivery_id, run_id, event_id, subscriber_type, subscriber_id, status,
				retry_count, reason_code, last_error, created_at, started_at, delivered_at
			)
			VALUES (?, ?, ?, 'node', 'node-success', 'delivered', 0, 'node_processed', '', ?, ?, ?)
		`, successDeliveryID, newer, newerMiddleEvent, now.Add(5*time.Second), now.Add(6*time.Second), now.Add(7*time.Second)); err != nil {
		t.Fatalf("seed sqlite successful delivery: %v", err)
	}
	deadLetterID := uuid.NewString()
	if _, err := sqliteStore.DB.ExecContext(ctx, `
			INSERT INTO dead_letters (
				dead_letter_id, original_event_id, original_event, original_payload, flow_instance,
				failure_type, error_message, retry_count, chain_depth, handler_node, created_at
			)
			VALUES (?, ?, 'scan.finished', '{}', 'flow-1', 'retry_exhausted', 'node boom', 2, 0, 'node-dead', ?)
		`, deadLetterID, newerLatestEvent, now.Add(9*time.Second)); err != nil {
		t.Fatalf("seed sqlite dead letter: %v", err)
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
	if header.EntityCount != 1 {
		t.Fatalf("header.EntityCount = %d, want entity_state count 1 despite stale run counter and event overcount", header.EntityCount)
	}

	firstPage, cursor, err := sqliteStore.ListRunHeaders(ctx, RunHeaderListOptions{Limit: 1})
	if err != nil {
		t.Fatalf("ListRunHeaders first page: %v", err)
	}
	if len(firstPage) != 1 || firstPage[0].RunID != newer {
		t.Fatalf("first page = %#v, want newer run", firstPage)
	}
	if firstPage[0].EntityCount != 2 {
		t.Fatalf("first page entity_count = %d, want entity_state count 2 despite event undercount", firstPage[0].EntityCount)
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

	report, err := sqliteStore.LoadRunDebugReport(ctx, newer, RunDebugQueryOptions{EventLimit: 2})
	if err != nil {
		t.Fatalf("LoadRunDebugReport: %v", err)
	}
	if report.RunID != newer || report.RootEventID != newerEvent || report.WarnErrorLogCount != 1 {
		t.Fatalf("report = %#v", report)
	}
	if report.EntityCount != 2 {
		t.Fatalf("report.EntityCount = %d, want entity_state count 2", report.EntityCount)
	}
	if len(report.Deliveries) != 4 {
		t.Fatalf("report deliveries = %#v, want pending/delivered/failed/dead_letter delivery count groups", report.Deliveries)
	}
	if len(report.FailedDeliveries) != 2 {
		t.Fatalf("report failed deliveries = %#v, want 2", report.FailedDeliveries)
	}
	for _, got := range report.FailedDeliveries {
		if got.DeliveryID == successDeliveryID {
			t.Fatalf("successful delivered/node_processed delivery appeared in FailedDeliveries: %#v", report.FailedDeliveries)
		}
	}
	if got := report.FailedDeliveries[0]; got.DeliveryID != nodeDeadDeliveryID || got.SubscriberType != "node" || got.RetryCount != 2 || got.RetryEligible || !got.Terminal || len(got.DeadLetters) != 1 {
		t.Fatalf("node failed delivery evidence = %#v", got)
	}
	if got := report.FailedDeliveries[1]; got.DeliveryID != agentFailedDeliveryID || got.SubscriberType != "agent" || got.RetryCount != 1 || !got.RetryEligible || got.Terminal || got.LastError != "agent boom" {
		t.Fatalf("agent failed delivery evidence = %#v", got)
	}
	traceRows, _, err := sqliteStore.LoadRunDebugTracePage(ctx, newer, RunDebugTraceQueryOptions{
		Limit: 10,
		Filter: RunDebugTraceFilter{
			DeliveryStatuses: []string{"failed"},
		},
	})
	if err != nil {
		t.Fatalf("LoadRunDebugTracePage sqlite failed filter: %v", err)
	}
	if len(traceRows) != 1 {
		t.Fatalf("sqlite failed trace rows = %#v, want one failed delivery row", traceRows)
	}
	if got := traceRows[0]; got.DeliveryID != agentFailedDeliveryID || got.DeliveryLastError != "agent boom" || got.DeliveryRetryCount != 1 || !got.DeliveryRetryEligible || got.DeliveryTerminal {
		t.Fatalf("sqlite trace delivery failure evidence = %#v", got)
	}
	if len(report.Events) != 2 || report.Events[0].EventID != newerLatestEvent || report.Events[1].EventID != newerMiddleEvent {
		t.Fatalf("report events = %#v, want latest non-log events first", report.Events)
	}
	full, err := sqliteStore.LoadOperatorEvent(ctx, newerLatestEvent)
	if err != nil {
		t.Fatalf("LoadOperatorEvent sqlite latest: %v", err)
	}
	if len(full.DeadLetters) != 1 || full.DeadLetters[0].DeadLetterID != deadLetterID {
		t.Fatalf("sqlite event dead letters = %#v", full.DeadLetters)
	}
	if len(full.Deliveries) != 1 || len(full.Deliveries[0].DeadLetters) != 1 || !full.Deliveries[0].Terminal {
		t.Fatalf("sqlite event delivery evidence = %#v", full.Deliveries)
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
