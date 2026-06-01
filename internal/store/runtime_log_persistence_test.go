package store

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestSQLiteRuntimeLogPersistenceWritesLoggerRowsForObservability(t *testing.T) {
	ctx := context.Background()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	runID := uuid.NewString()
	subjectEventID := uuid.NewString()
	ctx = runtimecorrelation.WithRunID(ctx, runID)
	if err := store.AppendEvent(ctx, events.Event{
		ID:          subjectEventID,
		RunID:       runID,
		Type:        events.EventType("validation/validation.package_ready"),
		SourceAgent: "agent-1",
		Payload:     json.RawMessage(`{"ready":true}`),
		CreatedAt:   time.Now().UTC(),
	}); err != nil {
		t.Fatalf("seed sqlite subject event: %v", err)
	}

	logger := runtimepkg.NewRuntimeLogger(store)
	if err := logger.Log(ctx, runtimepkg.RuntimeLogEntry{
		Level:     "warn",
		Message:   "sqlite diagnostic persisted",
		Component: "eventbus",
		Action:    "lineage_lookup",
		EventID:   subjectEventID,
		EventType: "validation/validation.package_ready",
		SessionID: "session-1",
	}); err != nil {
		t.Fatalf("RuntimeLogger.Log sqlite: %v", err)
	}

	logs, err := store.ListOperatorRuntimeLogs(ctx, OperatorRuntimeLogListOptions{
		RunID:     runID,
		Component: "eventbus",
		Level:     "warn",
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("ListOperatorRuntimeLogs sqlite: %v", err)
	}
	if len(logs.Logs) != 1 {
		t.Fatalf("sqlite runtime logs = %#v, want one logger-written row", logs.Logs)
	}
	log := logs.Logs[0]
	if log.RunID != runID || log.SessionID != "session-1" || log.Message != "sqlite diagnostic persisted" {
		t.Fatalf("sqlite runtime log = %#v, want run/session/message", log)
	}
	gotParentEventID, _ := log.Details["parent_event_id"].(string)
	if got := strings.TrimSpace(gotParentEventID); got != subjectEventID {
		t.Fatalf("sqlite runtime log parent_event_id = %q, want %q", got, subjectEventID)
	}
	var sourceEventID string
	if err := store.DB.QueryRowContext(ctx, `
		SELECT COALESCE(source_event_id, '')
		FROM events
		WHERE event_id = ?
	`, log.LogID).Scan(&sourceEventID); err != nil {
		t.Fatalf("load sqlite runtime log source_event_id: %v", err)
	}
	if sourceEventID != subjectEventID {
		t.Fatalf("sqlite source_event_id = %q, want %q", sourceEventID, subjectEventID)
	}
}

func TestPostgresRuntimeLogPersistencePreservesRunSourceAndLineage(t *testing.T) {
	ctx := context.Background()
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	pg := &PostgresStore{DB: db}
	if _, err := pg.BindSchemaCapabilities(ctx); err != nil {
		t.Fatalf("BindSchemaCapabilities: %v", err)
	}

	runID := uuid.NewString()
	subjectEventID := uuid.NewString()
	sourceFact := runtimecorrelation.BundleSourceFact{
		BundleHash:        "bundle-v1:sha256:1111111111111111111111111111111111111111111111111111111111111111",
		BundleSource:      storerunlifecycle.BundleSourcePersisted,
		BundleFingerprint: "sha256:1111111111111111111111111111111111111111111111111111111111111111",
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO bundles (bundle_hash, content_yaml, parsed_json)
		VALUES ($1, 'name: test', '{}'::jsonb)
	`, sourceFact.BundleHash); err != nil {
		t.Fatalf("seed bundle row: %v", err)
	}
	ctx = runtimecorrelation.WithRunID(ctx, runID)
	ctx = runtimecorrelation.WithBundleSourceFact(ctx, sourceFact)
	if err := pg.AppendEvent(ctx, events.Event{
		ID:          subjectEventID,
		RunID:       runID,
		Type:        events.EventType("validation/validation.package_ready"),
		SourceAgent: "agent-1",
		Payload:     json.RawMessage(`{"ready":true}`),
		CreatedAt:   time.Now().UTC(),
	}); err != nil {
		t.Fatalf("seed postgres subject event: %v", err)
	}

	logger := runtimepkg.NewRuntimeLogger(pg)
	if err := logger.Log(ctx, runtimepkg.RuntimeLogEntry{
		Level:     "warn",
		Message:   "postgres diagnostic persisted",
		Component: "eventbus",
		Action:    "lineage_lookup",
		EventID:   subjectEventID,
		EventType: "validation/validation.package_ready",
	}); err != nil {
		t.Fatalf("RuntimeLogger.Log postgres: %v", err)
	}

	var gotHash, gotSource, gotFingerprint, sourceEventID string
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(bundle_hash, ''), bundle_source, COALESCE(bundle_fingerprint, '')
		FROM runs
		WHERE run_id = $1::uuid
	`, runID).Scan(&gotHash, &gotSource, &gotFingerprint); err != nil {
		t.Fatalf("load postgres run bundle source: %v", err)
	}
	if gotHash != sourceFact.BundleHash || gotSource != sourceFact.BundleSource || gotFingerprint != sourceFact.BundleFingerprint {
		t.Fatalf("postgres run bundle source = hash:%q source:%q fingerprint:%q, want %#v", gotHash, gotSource, gotFingerprint, sourceFact)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(source_event_id::text, '')
		FROM events
		WHERE event_name = 'platform.runtime_log'
		ORDER BY created_at DESC
		LIMIT 1
	`).Scan(&sourceEventID); err != nil {
		t.Fatalf("load postgres runtime log source_event_id: %v", err)
	}
	if sourceEventID != subjectEventID {
		t.Fatalf("postgres source_event_id = %q, want %q", sourceEventID, subjectEventID)
	}
}
