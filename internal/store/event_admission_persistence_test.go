package store

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestPostgresEventAdmissionRejectsMalformedChildDirectAppend(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	err := pg.AppendEvent(ctx, events.NewChildEventWithLineage(
		"",
		events.EventType("task.completed"),
		"agent-1",
		"",
		json.RawMessage(`{"ok":true}`),
		0,
		events.EventLineage{},
		events.EventEnvelope{},
		time.Time{},
	))
	if err == nil {
		t.Fatal("expected malformed child event to fail admission")
	}
	if !strings.Contains(err.Error(), "requires admitted run_id") {
		t.Fatalf("AppendEvent error = %v, want missing run_id admission error", err)
	}

	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_name = 'task.completed'`).Scan(&count); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if count != 0 {
		t.Fatalf("persisted malformed child rows = %d, want 0", count)
	}
}

func TestPostgresEventAdmissionRejectsProjectionDirectAppendWithoutAuthoritativeFacts(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	err := pg.AppendEvent(ctx, eventtest.Projection(
		"",
		events.EventType("task.completed"),
		"agent-1",
		"",
		json.RawMessage(`{"ok":true}`),
		0,
		"",
		"",
		events.EventEnvelope{},
		time.Time{}))
	if err == nil {
		t.Fatal("expected malformed projection event to fail admission")
	}
	if !strings.Contains(err.Error(), "authoritative event_id") {
		t.Fatalf("AppendEvent error = %v, want missing authoritative event_id admission error", err)
	}

	err = pg.AppendEvent(runtimecorrelation.WithRuntimeLineage(ctx, runtimecorrelation.RuntimeLineage{
		RunID: uuid.NewString(),
	}), eventtest.Projection(
		uuid.NewString(),
		events.EventType("task.completed"),
		"agent-1",
		"",
		json.RawMessage(`{"ok":true}`),
		0,
		"",
		"",
		events.EventEnvelope{},
		time.Date(2026, 6, 6, 10, 11, 12, 0, time.UTC)),
	)
	if err == nil {
		t.Fatal("expected projection event missing own run_id to fail admission")
	}
	if !strings.Contains(err.Error(), "authoritative run_id") {
		t.Fatalf("AppendEvent error = %v, want missing authoritative run_id admission error", err)
	}

	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_name = 'task.completed'`).Scan(&count); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if count != 0 {
		t.Fatalf("persisted malformed projection rows = %d, want 0", count)
	}
}

func TestSQLiteEventAdmissionRejectsMalformedChildDirectAppend(t *testing.T) {
	sqliteStore := newBootstrappedSQLiteRuntimeStoreForTest(t)
	ctx := context.Background()

	err := sqliteStore.AppendEvent(ctx, events.NewChildEventWithLineage(
		"",
		events.EventType("task.completed"),
		"agent-1",
		"",
		json.RawMessage(`{"ok":true}`),
		0,
		events.EventLineage{},
		events.EventEnvelope{},
		time.Time{},
	))
	if err == nil {
		t.Fatal("expected malformed child event to fail admission")
	}
	if !strings.Contains(err.Error(), "requires admitted run_id") {
		t.Fatalf("AppendEvent error = %v, want missing run_id admission error", err)
	}

	var count int
	if err := sqliteStore.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_name = 'task.completed'`).Scan(&count); err != nil {
		t.Fatalf("count sqlite events: %v", err)
	}
	if count != 0 {
		t.Fatalf("persisted malformed sqlite child rows = %d, want 0", count)
	}
}

func TestSQLiteEventAdmissionRejectsProjectionDirectAppendWithoutAuthoritativeFacts(t *testing.T) {
	sqliteStore := newBootstrappedSQLiteRuntimeStoreForTest(t)
	ctx := context.Background()

	err := sqliteStore.AppendEvent(ctx, eventtest.Projection(
		"",
		events.EventType("task.completed"),
		"agent-1",
		"",
		json.RawMessage(`{"ok":true}`),
		0,
		"",
		"",
		events.EventEnvelope{},
		time.Time{}))
	if err == nil {
		t.Fatal("expected malformed projection event to fail admission")
	}
	if !strings.Contains(err.Error(), "authoritative event_id") {
		t.Fatalf("AppendEvent error = %v, want missing authoritative event_id admission error", err)
	}

	err = sqliteStore.AppendEvent(runtimecorrelation.WithRuntimeLineage(ctx, runtimecorrelation.RuntimeLineage{
		RunID: uuid.NewString(),
	}), eventtest.Projection(
		uuid.NewString(),
		events.EventType("task.completed"),
		"agent-1",
		"",
		json.RawMessage(`{"ok":true}`),
		0,
		"",
		"",
		events.EventEnvelope{},
		time.Date(2026, 6, 6, 10, 11, 12, 0, time.UTC)),
	)
	if err == nil {
		t.Fatal("expected projection event missing own run_id to fail admission")
	}
	if !strings.Contains(err.Error(), "authoritative run_id") {
		t.Fatalf("AppendEvent error = %v, want missing authoritative run_id admission error", err)
	}

	var count int
	if err := sqliteStore.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_name = 'task.completed'`).Scan(&count); err != nil {
		t.Fatalf("count sqlite events: %v", err)
	}
	if count != 0 {
		t.Fatalf("persisted malformed sqlite projection rows = %d, want 0", count)
	}
}

func TestPostgresDiagnosticDirectWritersUseAdmissionFactsAndRemainNonRouted(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	logger := runtimepkg.NewRuntimeLogger(pg)
	if err := logger.Log(ctx, runtimepkg.RuntimeLogEntry{
		Level:     diaglog.LevelWarn,
		Message:   "admitted global runtime log",
		Component: "admission",
		Action:    "runtime_log_admission",
	}); err != nil {
		t.Fatalf("RuntimeLogger.Log: %v", err)
	}

	entityID := uuid.NewString()
	providerEventID := "provider-event-admission"
	provider := "webhook"
	inserted, err := pg.RecordInboundEvent(ctx, providerEventID, entityID, provider)
	if err != nil {
		t.Fatalf("RecordInboundEvent: %v", err)
	}
	if !inserted {
		t.Fatal("RecordInboundEvent inserted=false, want first insert")
	}
	duplicate, err := pg.RecordInboundEvent(ctx, providerEventID, entityID, provider)
	if err != nil {
		t.Fatalf("RecordInboundEvent duplicate: %v", err)
	}
	if duplicate {
		t.Fatal("RecordInboundEvent duplicate inserted=true, want false")
	}

	logEventID, logRunID, logCreatedAt := loadPostgresAdmissionEventFacts(t, ctx, db, `
		SELECT event_id::text, COALESCE(run_id::text, ''), created_at
		FROM events
		WHERE event_name = 'platform.runtime_log'
		  AND payload->>'message' = $1
	`, "admitted global runtime log")
	if logEventID == "" || logRunID != "" || logCreatedAt.IsZero() {
		t.Fatalf("runtime_log facts id=%q run=%q created_at=%s, want id/no-run/created", logEventID, logRunID, logCreatedAt)
	}

	inboundEventID, inboundRunID, inboundCreatedAt := loadPostgresAdmissionEventFacts(t, ctx, db, `
		SELECT event_id::text, COALESCE(run_id::text, ''), created_at
		FROM events
		WHERE event_name = 'platform.inbound_recorded'
		  AND idempotency_key = $1
	`, inboundEventIdempotencyKey(providerEventID, entityID, provider))
	if inboundEventID == "" || inboundRunID != "" || inboundCreatedAt.IsZero() {
		t.Fatalf("inbound_recorded facts id=%q run=%q created_at=%s, want id/no-run/created", inboundEventID, inboundRunID, inboundCreatedAt)
	}

	assertPostgresNoDeliveries(t, ctx, db, logEventID)
	assertPostgresNoDeliveries(t, ctx, db, inboundEventID)
}

func TestSQLiteRuntimeLogDiagnosticDirectUsesAdmissionFacts(t *testing.T) {
	sqliteStore := newBootstrappedSQLiteRuntimeStoreForTest(t)
	ctx := context.Background()

	logger := runtimepkg.NewRuntimeLogger(sqliteStore)
	if err := logger.Log(ctx, runtimepkg.RuntimeLogEntry{
		Level:     diaglog.LevelWarn,
		Message:   "admitted sqlite global runtime log",
		Component: "admission",
		Action:    "sqlite_runtime_log_admission",
	}); err != nil {
		t.Fatalf("RuntimeLogger.Log sqlite: %v", err)
	}

	var eventID, runID, createdAt string
	if err := sqliteStore.DB.QueryRowContext(ctx, `
		SELECT event_id, COALESCE(run_id, ''), created_at
		FROM events
		WHERE event_name = 'platform.runtime_log'
		  AND json_extract(payload, '$.message') = ?
	`, "admitted sqlite global runtime log").Scan(&eventID, &runID, &createdAt); err != nil {
		t.Fatalf("load sqlite runtime_log facts: %v", err)
	}
	if eventID == "" || runID != "" || strings.TrimSpace(createdAt) == "" {
		t.Fatalf("sqlite runtime_log facts id=%q run=%q created_at=%s, want id/no-run/created", eventID, runID, createdAt)
	}

	var deliveries int
	if err := sqliteStore.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE event_id = ?`, eventID).Scan(&deliveries); err != nil {
		t.Fatalf("count sqlite runtime_log deliveries: %v", err)
	}
	if deliveries != 0 {
		t.Fatalf("sqlite runtime_log deliveries = %d, want 0", deliveries)
	}
}

func loadPostgresAdmissionEventFacts(t *testing.T, ctx context.Context, db rowQueryer, query string, arg any) (string, string, time.Time) {
	t.Helper()
	var (
		eventID   string
		runID     string
		createdAt time.Time
	)
	if err := db.QueryRowContext(ctx, query, arg).Scan(&eventID, &runID, &createdAt); err != nil {
		t.Fatalf("load postgres event facts: %v", err)
	}
	return eventID, runID, createdAt
}

func assertPostgresNoDeliveries(t *testing.T, ctx context.Context, db rowQueryer, eventID string) {
	t.Helper()
	var deliveries int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE event_id = $1::uuid`, eventID).Scan(&deliveries); err != nil {
		t.Fatalf("count event deliveries for %s: %v", eventID, err)
	}
	if deliveries != 0 {
		t.Fatalf("event %s deliveries = %d, want 0", eventID, deliveries)
	}
}
