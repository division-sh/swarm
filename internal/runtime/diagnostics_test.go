package runtime

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	runtimebus "swarm/internal/runtime/bus"
	runtimecorrelation "swarm/internal/runtime/correlation"
	storerunlifecycle "swarm/internal/store/runlifecycle"
	"swarm/internal/testutil"
)

type runtimeLogCapabilityStub struct {
	enabled  bool
	hasRunID bool
	err      error
}

func (s runtimeLogCapabilityStub) CanonicalRuntimeLogCapability(context.Context) (bool, bool, error) {
	return s.enabled, s.hasRunID, s.err
}

func TestRuntimeLogger_Log_AppendsSpecShapedFlightRecorderEntry(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectExec(`INSERT INTO events`).
		WithArgs(runtimeLogPayloadArg{
			level:      "warn",
			message:    "Tool execution was denied for save_entity_field",
			component:  "tool-executor",
			action:     "tool_execution_denied",
			eventID:    "evt-1",
			eventType:  "validation/requested",
			agentID:    "agent-1",
			entityID:   "entity-1",
			sessionID:  "session-1",
			errorText:  "runtime_error code=cross_flow_write_forbidden",
			durationUS: 1200,
			detail: map[string]any{
				"tool_name":     "save_entity_field",
				"denial_layer":  "executor",
				"denial_reason": "cross_flow_write_forbidden",
			},
		}, "").
		WillReturnResult(sqlmock.NewResult(0, 1))

	logger := NewRuntimeLogger(db, runtimeLogCapabilityStub{enabled: true})
	recorder := runtimebus.NewEmittedEventsRecorder()
	ctx := runtimebus.WithEmittedEventsRecorder(context.Background(), recorder)

	if err := logger.Log(ctx, RuntimeLogEntry{
		Level:      "warn",
		Message:    "Tool execution was denied for save_entity_field",
		Component:  "tool-executor",
		Action:     "tool_execution_denied",
		EventID:    "evt-1",
		EventType:  "validation/requested",
		AgentID:    "agent-1",
		EntityID:   "entity-1",
		SessionID:  "session-1",
		Error:      "runtime_error code=cross_flow_write_forbidden",
		DurationUS: 1200,
		Detail: map[string]any{
			"tool_name":     "save_entity_field",
			"denial_layer":  "executor",
			"denial_reason": "cross_flow_write_forbidden",
		},
	}); err != nil {
		t.Fatalf("logger.Log() error = %v", err)
	}

	entries := recorder.SnapshotFlightRecorder()
	if len(entries) != 1 {
		t.Fatalf("flight recorder count = %d, want 1", len(entries))
	}
	entry := entries[0]
	if entry.Kind != "runtime_log" {
		t.Fatalf("kind = %q, want runtime_log", entry.Kind)
	}
	if entry.LogLevel != "warn" {
		t.Fatalf("log_level = %q, want warn", entry.LogLevel)
	}
	if entry.Message != "Tool execution was denied for save_entity_field" {
		t.Fatalf("message = %q", entry.Message)
	}
	details, ok := entry.Details.(map[string]any)
	if !ok {
		t.Fatalf("details type = %T, want map[string]any", entry.Details)
	}
	if details["component"] != "tool-executor" {
		t.Fatalf("details.component = %#v", details["component"])
	}
	if details["action"] != "tool_execution_denied" {
		t.Fatalf("details.action = %#v", details["action"])
	}
	if details["tool_name"] != "save_entity_field" {
		t.Fatalf("details.tool_name = %#v", details["tool_name"])
	}
	if details["denial_layer"] != "executor" {
		t.Fatalf("details.denial_layer = %#v", details["denial_layer"])
	}
	if details["error"] != "runtime_error code=cross_flow_write_forbidden" {
		t.Fatalf("details.error = %#v", details["error"])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("ExpectationsWereMet() error = %v", err)
	}
}

func TestRuntimeLogger_Log_AppendsCanonicalFlightRecorderDefaults(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectExec(`INSERT INTO events`).
		WithArgs(runtimeLogPayloadArg{
			level:     "warn",
			message:   "runtime warning",
			component: "runtime",
			action:    "unknown",
			eventType: "diagnostic/actual",
		}, "").
		WillReturnResult(sqlmock.NewResult(0, 1))

	logger := NewRuntimeLogger(db, runtimeLogCapabilityStub{enabled: true})
	recorder := runtimebus.NewEmittedEventsRecorder()
	ctx := runtimebus.WithEmittedEventsRecorder(context.Background(), recorder)

	if err := logger.Log(ctx, RuntimeLogEntry{
		Level:     "warning",
		Message:   "  runtime warning  ",
		Component: "  ",
		Action:    "  ",
		EventType: "diagnostic/actual",
		Detail: map[string]any{
			"component":  123,
			"action":     false,
			"event_name": "diagnostic/drifted",
			"event_type": "diagnostic/drifted",
		},
	}); err != nil {
		t.Fatalf("logger.Log() error = %v", err)
	}

	entries := recorder.SnapshotFlightRecorder()
	if len(entries) != 1 {
		t.Fatalf("flight recorder count = %d, want 1", len(entries))
	}
	entry := entries[0]
	if entry.LogLevel != "warn" {
		t.Fatalf("log_level = %q, want warn", entry.LogLevel)
	}
	if entry.Message != "runtime warning" {
		t.Fatalf("message = %q", entry.Message)
	}
	details, ok := entry.Details.(map[string]any)
	if !ok {
		t.Fatalf("details type = %T, want map[string]any", entry.Details)
	}
	if details["component"] != "runtime" {
		t.Fatalf("details.component = %#v, want runtime", details["component"])
	}
	if details["action"] != "unknown" {
		t.Fatalf("details.action = %#v, want unknown", details["action"])
	}
	if details["event_name"] != "diagnostic/actual" {
		t.Fatalf("details.event_name = %#v, want diagnostic/actual", details["event_name"])
	}
	if details["event_type"] != "diagnostic/actual" {
		t.Fatalf("details.event_type = %#v, want diagnostic/actual", details["event_type"])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("ExpectationsWereMet() error = %v", err)
	}
}

func TestRuntimeLogger_Log_PersistsRuntimeLogPayloadViaCapabilityOwner(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectExec(`INSERT INTO events`).
		WithArgs(runtimeLogPayloadArg{
			level:      "warn",
			message:    "Tool execution was denied for save_entity_field",
			component:  "tool-executor",
			action:     "tool_execution_denied",
			eventID:    "evt-1",
			eventType:  "validation/requested",
			agentID:    "agent-1",
			entityID:   "entity-1",
			sessionID:  "session-1",
			errorText:  "runtime_error code=cross_flow_write_forbidden",
			durationUS: 1200,
			detail: map[string]any{
				"tool_name":     "save_entity_field",
				"denial_layer":  "executor",
				"denial_reason": "cross_flow_write_forbidden",
			},
		}, "").
		WillReturnResult(sqlmock.NewResult(0, 1))

	logger := NewRuntimeLogger(db, runtimeLogCapabilityStub{enabled: true})
	if err := logger.Log(context.Background(), RuntimeLogEntry{
		Level:      "warn",
		Message:    "Tool execution was denied for save_entity_field",
		Component:  "tool-executor",
		Action:     "tool_execution_denied",
		EventID:    "evt-1",
		EventType:  "validation/requested",
		AgentID:    "agent-1",
		EntityID:   "entity-1",
		SessionID:  "session-1",
		Error:      "runtime_error code=cross_flow_write_forbidden",
		DurationUS: 1200,
		Detail: map[string]any{
			"tool_name":     "save_entity_field",
			"denial_layer":  "executor",
			"denial_reason": "cross_flow_write_forbidden",
		},
	}); err != nil {
		t.Fatalf("logger.Log() error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("ExpectationsWereMet() error = %v", err)
	}
}

func TestRuntimeLogger_Log_EnsuresRunRowBeforePersistingRunScopedEntry(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	const runID = "8d4891f8-0f8e-4c85-b34b-9e0e7f4327dd"
	recorder := runtimebus.NewEmittedEventsRecorder()
	ctx := runtimebus.WithEmittedEventsRecorder(context.Background(), recorder)
	ctx = runtimecorrelation.WithRunID(ctx, runID)

	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO runs`).
		WithArgs(runID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO events`).
		WithArgs(runID, sqlmock.AnyArg(), "").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE runs`).
		WithArgs(runID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	logger := NewRuntimeLogger(db, runtimeLogCapabilityStub{enabled: true, hasRunID: true})
	if err := logger.Log(ctx, RuntimeLogEntry{
		Level:     "error",
		Message:   "runtime log",
		Component: "workflow-runtime",
		Action:    "handler_error",
	}); err != nil {
		t.Fatalf("logger.Log() error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("ExpectationsWereMet() error = %v", err)
	}
	entries := recorder.SnapshotFlightRecorder()
	if len(entries) != 1 {
		t.Fatalf("flight recorder count = %d, want 1", len(entries))
	}
	details, ok := entries[0].Details.(map[string]any)
	if !ok {
		t.Fatalf("details type = %T, want map[string]any", entries[0].Details)
	}
	if got := strings.TrimSpace(asString(details["run_id"])); got != runID {
		t.Fatalf("details.run_id = %q, want %q", got, runID)
	}
}

func TestRuntimeLogger_Log_StampsBundleSourceFactOnRunRow(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	logger := NewRuntimeLogger(db, runtimeLogCapabilityStub{enabled: true, hasRunID: true})
	runID := uuid.NewString()
	sourceFact := runtimecorrelation.BundleSourceFact{
		BundleHash:        "bundle-v1:sha256:1111111111111111111111111111111111111111111111111111111111111111",
		BundleSource:      storerunlifecycle.BundleSourcePersisted,
		BundleFingerprint: "sha256:1111111111111111111111111111111111111111111111111111111111111111",
	}
	seedRuntimeLogBundleRow(t, db, sourceFact.BundleHash)
	ctx := runtimecorrelation.WithRunID(context.Background(), runID)
	ctx = runtimecorrelation.WithBundleSourceFact(ctx, sourceFact)

	if err := logger.Log(ctx, RuntimeLogEntry{
		Level:     "info",
		Message:   "runtime log",
		Component: "workflow-runtime",
		Action:    "bundle_source_fact",
	}); err != nil {
		t.Fatalf("logger.Log: %v", err)
	}
	var gotHash, gotSource, gotFingerprint string
	if err := db.QueryRow(`
		SELECT COALESCE(bundle_hash, ''), bundle_source, COALESCE(bundle_fingerprint, '')
		FROM runs
		WHERE run_id = $1::uuid
	`, runID).Scan(&gotHash, &gotSource, &gotFingerprint); err != nil {
		t.Fatalf("load run bundle source: %v", err)
	}
	if gotHash != sourceFact.BundleHash || gotSource != sourceFact.BundleSource || gotFingerprint != sourceFact.BundleFingerprint {
		t.Fatalf("run bundle source = hash:%q source:%q fingerprint:%q, want %#v", gotHash, gotSource, gotFingerprint, sourceFact)
	}
}

func TestRuntimeLogger_LogRejectsDeletedPersistedBundleSourceFact(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	logger := NewRuntimeLogger(db, runtimeLogCapabilityStub{enabled: true, hasRunID: true})
	runID := uuid.NewString()
	sourceFact := runtimecorrelation.BundleSourceFact{
		BundleHash:        "bundle-v1:sha256:1111111111111111111111111111111111111111111111111111111111111111",
		BundleSource:      storerunlifecycle.BundleSourcePersisted,
		BundleFingerprint: "sha256:1111111111111111111111111111111111111111111111111111111111111111",
	}
	seedRuntimeLogBundleRow(t, db, sourceFact.BundleHash)
	if _, err := db.ExecContext(context.Background(), `DELETE FROM bundles WHERE bundle_hash = $1`, sourceFact.BundleHash); err != nil {
		t.Fatalf("delete bundle row: %v", err)
	}
	ctx := runtimecorrelation.WithRunID(context.Background(), runID)
	ctx = runtimecorrelation.WithBundleSourceFact(ctx, sourceFact)

	err := logger.Log(ctx, RuntimeLogEntry{
		Level:     "info",
		Message:   "runtime log",
		Component: "workflow-runtime",
		Action:    "bundle_source_deleted",
	})
	if !errors.Is(err, storerunlifecycle.ErrPersistedBundleUnavailable) {
		t.Fatalf("logger.Log error = %v, want ErrPersistedBundleUnavailable", err)
	}
	assertRunRowExists(t, db, runID, false)
	if count := countRuntimeLogRowsForRun(t, db, runID); count != 0 {
		t.Fatalf("runtime log rows for %s = %d, want 0", runID, count)
	}
}

func TestRuntimeLogger_Log_ReturnsPersistenceFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	writeErr := errors.New("insert failed")
	mock.ExpectExec(`INSERT INTO events`).
		WithArgs(sqlmock.AnyArg(), "").
		WillReturnError(writeErr)

	recorder := runtimebus.NewEmittedEventsRecorder()
	ctx := runtimebus.WithEmittedEventsRecorder(context.Background(), recorder)
	logger := NewRuntimeLogger(db, runtimeLogCapabilityStub{enabled: true})
	err = logger.Log(ctx, RuntimeLogEntry{
		Level:     "error",
		Message:   "Persisting the pipeline receipt failed",
		Component: "eventbus",
		Action:    "pipeline_receipt_persist_failed",
	})
	if !errors.Is(err, writeErr) {
		t.Fatalf("logger.Log() error = %v, want %v", err, writeErr)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("ExpectationsWereMet() error = %v", err)
	}
	if entries := recorder.SnapshotFlightRecorder(); len(entries) != 0 {
		t.Fatalf("flight recorder count = %d, want 0", len(entries))
	}
}

func TestRuntimeLogger_Log_AllowsEmptyCanonicalMessageWhenDetailsExist(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectExec(`INSERT INTO events`).
		WithArgs(runtimeLogPayloadArg{
			level:     "info",
			message:   "",
			component: "agent-manager",
			action:    "delivery_lifecycle_transition",
			eventID:   "evt-1",
			agentID:   "agent-a",
			detail: map[string]any{
				"delivery_state":          "launching",
				"delivery_transition":     "launching",
				"delivery_previous_state": "queued",
				"delivery_reason":         "agent_processing",
				"subscriber_type":         "agent",
				"subscriber_id":           "agent-a",
			},
		}, "").
		WillReturnResult(sqlmock.NewResult(0, 1))

	logger := NewRuntimeLogger(db, runtimeLogCapabilityStub{enabled: true})
	if err := logger.Log(context.Background(), RuntimeLogEntry{
		Level:     "debug",
		Message:   "",
		Component: "agent-manager",
		Action:    "delivery_lifecycle_transition",
		EventID:   "evt-1",
		AgentID:   "agent-a",
		Detail: map[string]any{
			"delivery_state":          "launching",
			"delivery_transition":     "launching",
			"delivery_previous_state": "queued",
			"delivery_reason":         "agent_processing",
			"subscriber_type":         "agent",
			"subscriber_id":           "agent-a",
		},
	}); err != nil {
		t.Fatalf("logger.Log() error = %v, want nil", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("ExpectationsWereMet() error = %v", err)
	}
}

func TestDecodeCanonicalRuntimeLogPayload_FailsClosedOnMissingMessageField(t *testing.T) {
	_, err := DecodeCanonicalRuntimeLogPayload([]byte(`{
		"log_level":"debug",
		"details":{"component":"agent-manager","action":"delivery_lifecycle_transition"}
	}`))
	if err == nil || !strings.Contains(err.Error(), "runtime log message is required") {
		t.Fatalf("DecodeCanonicalRuntimeLogPayload() error = %v, want missing message failure", err)
	}
}

func TestRuntimeLogger_Log_FailsClosedWithoutCanonicalCapability(t *testing.T) {
	capabilityErr := errors.New("capability unavailable")
	tests := []struct {
		name         string
		db           bool
		capabilities runtimeLogCapabilityResolver
	}{
		{name: "missing database", capabilities: runtimeLogCapabilityStub{enabled: true}},
		{name: "missing resolver", db: true},
		{name: "disabled", db: true, capabilities: runtimeLogCapabilityStub{}},
		{name: "resolver error", db: true, capabilities: runtimeLogCapabilityStub{err: capabilityErr}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var (
				db   *sql.DB
				mock sqlmock.Sqlmock
				err  error
			)
			if tt.db {
				db, mock, err = sqlmock.New()
				if err != nil {
					t.Fatalf("sqlmock: %v", err)
				}
				defer db.Close()
			}

			recorder := runtimebus.NewEmittedEventsRecorder()
			ctx := runtimebus.WithEmittedEventsRecorder(context.Background(), recorder)
			logger := NewRuntimeLogger(db, tt.capabilities)
			if err := logger.Log(ctx, RuntimeLogEntry{
				Level:   "info",
				Message: "runtime log",
			}); err != nil {
				t.Fatalf("logger.Log() error = %v", err)
			}

			if entries := recorder.SnapshotFlightRecorder(); len(entries) != 0 {
				t.Fatalf("flight recorder count = %d, want 0", len(entries))
			}
			if mock != nil {
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Fatalf("expectations: %v", err)
				}
			}
		})
	}
}

func TestRuntimeLogger_Log_DoesNotAppendFlightRecorderOnPayloadValidationFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	recorder := runtimebus.NewEmittedEventsRecorder()
	ctx := runtimebus.WithEmittedEventsRecorder(context.Background(), recorder)
	logger := NewRuntimeLogger(db, runtimeLogCapabilityStub{enabled: true})
	err = logger.Log(ctx, RuntimeLogEntry{
		Level:     "warn",
		Message:   "runtime log",
		Component: "diagnostics",
		Action:    "payload_validation",
		Detail: map[string]any{
			"correlation": []any{"not-a-string-map"},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "details.correlation") {
		t.Fatalf("logger.Log() error = %v, want correlation validation failure", err)
	}
	if entries := recorder.SnapshotFlightRecorder(); len(entries) != 0 {
		t.Fatalf("flight recorder count = %d, want 0", len(entries))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestRuntimeLogger_Log_DoesNotAppendFlightRecorderOnLineageLookupFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	runID := uuid.NewString()
	subjectEventID := uuid.NewString()
	lineageErr := errors.New("lineage lookup failed")
	mock.ExpectQuery(`SELECT EXISTS`).
		WithArgs(runID, subjectEventID).
		WillReturnError(lineageErr)

	recorder := runtimebus.NewEmittedEventsRecorder()
	ctx := runtimebus.WithEmittedEventsRecorder(context.Background(), recorder)
	ctx = runtimecorrelation.WithRunID(ctx, runID)
	logger := NewRuntimeLogger(db, runtimeLogCapabilityStub{enabled: true, hasRunID: true})
	err = logger.Log(ctx, RuntimeLogEntry{
		Level:     "warn",
		Message:   "runtime log",
		Component: "eventbus",
		Action:    "lineage_lookup",
		EventID:   subjectEventID,
	})
	if !errors.Is(err, lineageErr) {
		t.Fatalf("logger.Log() error = %v, want %v", err, lineageErr)
	}
	if entries := recorder.SnapshotFlightRecorder(); len(entries) != 0 {
		t.Fatalf("flight recorder count = %d, want 0", len(entries))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestRuntimeLogger_Log_DoesNotAppendFlightRecorderOnRunRowFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	runID := uuid.NewString()
	runRowErr := errors.New("run row failed")
	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO runs`).
		WithArgs(runID).
		WillReturnError(runRowErr)
	mock.ExpectRollback()

	recorder := runtimebus.NewEmittedEventsRecorder()
	ctx := runtimebus.WithEmittedEventsRecorder(context.Background(), recorder)
	ctx = runtimecorrelation.WithRunID(ctx, runID)
	logger := NewRuntimeLogger(db, runtimeLogCapabilityStub{enabled: true, hasRunID: true})
	err = logger.Log(ctx, RuntimeLogEntry{
		Level:     "error",
		Message:   "runtime log",
		Component: "workflow-runtime",
		Action:    "handler_error",
	})
	if !errors.Is(err, runRowErr) {
		t.Fatalf("logger.Log() error = %v, want %v", err, runRowErr)
	}
	if entries := recorder.SnapshotFlightRecorder(); len(entries) != 0 {
		t.Fatalf("flight recorder count = %d, want 0", len(entries))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestRuntimeLogger_Log_DoesNotAppendFlightRecorderOnSyncCountsFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	runID := uuid.NewString()
	syncErr := errors.New("sync failed")
	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO runs`).
		WithArgs(runID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO events`).
		WithArgs(runID, sqlmock.AnyArg(), "").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE runs`).
		WithArgs(runID).
		WillReturnError(syncErr)
	mock.ExpectRollback()

	recorder := runtimebus.NewEmittedEventsRecorder()
	ctx := runtimebus.WithEmittedEventsRecorder(context.Background(), recorder)
	ctx = runtimecorrelation.WithRunID(ctx, runID)
	logger := NewRuntimeLogger(db, runtimeLogCapabilityStub{enabled: true, hasRunID: true})
	err = logger.Log(ctx, RuntimeLogEntry{
		Level:     "error",
		Message:   "runtime log",
		Component: "workflow-runtime",
		Action:    "handler_error",
	})
	if !errors.Is(err, syncErr) {
		t.Fatalf("logger.Log() error = %v, want %v", err, syncErr)
	}
	if entries := recorder.SnapshotFlightRecorder(); len(entries) != 0 {
		t.Fatalf("flight recorder count = %d, want 0", len(entries))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestRuntimeLogger_Log_PersistsCanonicalRunOwnershipFromContext(t *testing.T) {
	ctx := context.Background()
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	logger := NewRuntimeLogger(db, runtimeLogCapabilityStub{enabled: true, hasRunID: true})
	runID := uuid.NewString()
	spoofedRunID := uuid.NewString()
	ctx = runtimecorrelation.WithRunID(ctx, runID)

	if err := logger.Log(ctx, RuntimeLogEntry{
		Level:     "warn",
		Message:   "canonical runtime log",
		Component: "diagnostics",
		Action:    "canonical_run_context",
		Detail: map[string]any{
			"run_id": spoofedRunID,
			"note":   "context must win",
		},
	}); err != nil {
		t.Fatalf("logger.Log() error = %v", err)
	}

	row := loadLatestRuntimeLogRow(t, db)
	if row.RunID != runID {
		t.Fatalf("persisted run_id = %q, want %q", row.RunID, runID)
	}
	if got := strings.TrimSpace(asString(row.Detail["run_id"])); got != runID {
		t.Fatalf("payload details.run_id = %q, want %q", got, runID)
	}
	if got := strings.TrimSpace(asString(row.Detail["note"])); got != "context must win" {
		t.Fatalf("payload details.note = %q, want context must win", got)
	}
	assertRunRowExists(t, db, runID, true)
	assertRunRowExists(t, db, spoofedRunID, false)
}

func TestRuntimeLogger_Log_DoesNotInferRunOwnershipFromDetailPayload(t *testing.T) {
	ctx := context.Background()
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	logger := NewRuntimeLogger(db, runtimeLogCapabilityStub{enabled: true, hasRunID: true})
	payloadRunID := uuid.NewString()

	if err := logger.Log(ctx, RuntimeLogEntry{
		Level:     "warn",
		Message:   "uncorrelated runtime log",
		Component: "diagnostics",
		Action:    "payload_run_id_ignored",
		Detail: map[string]any{
			"run_id": payloadRunID,
			"note":   "must remain unscoped",
		},
	}); err != nil {
		t.Fatalf("logger.Log() error = %v", err)
	}

	row := loadLatestRuntimeLogRow(t, db)
	if row.RunID != "" {
		t.Fatalf("persisted run_id = %q, want empty", row.RunID)
	}
	if got := strings.TrimSpace(asString(row.Detail["run_id"])); got != "" {
		t.Fatalf("payload details.run_id = %q, want empty", got)
	}
	if got := strings.TrimSpace(asString(row.Detail["note"])); got != "must remain unscoped" {
		t.Fatalf("payload details.note = %q, want must remain unscoped", got)
	}
	assertRunRowExists(t, db, payloadRunID, false)
}

func TestRuntimeLogger_Log_DerivesLineageFromPersistedSubjectEvent(t *testing.T) {
	ctx := context.Background()
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	logger := NewRuntimeLogger(db, runtimeLogCapabilityStub{enabled: true, hasRunID: true})
	runID := uuid.NewString()
	subjectEventID := uuid.NewString()
	ctx = runtimecorrelation.WithRunID(ctx, runID)
	if err := ensureRuntimeLogRunRow(ctx, db, runID); err != nil {
		t.Fatalf("ensure run row: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			run_id, event_id, event_name, scope, payload, produced_by, produced_by_type
		)
		VALUES (
			$1::uuid, $2::uuid, 'validation/validation.package_ready', 'global', '{}'::jsonb,
			'runtime.run_fork.selected_contract_execution', 'agent'
		)
	`, runID, subjectEventID); err != nil {
		t.Fatalf("seed subject event: %v", err)
	}

	if err := logger.Log(ctx, RuntimeLogEntry{
		Level:     "warn",
		Message:   "Persisted event replay skipped because committed replay scope is unavailable",
		Component: "eventbus",
		Action:    "outbox_replay_scope_unavailable",
		EventID:   subjectEventID,
		EventType: "validation/validation.package_ready",
		Detail: map[string]any{
			"reason": "missing_committed_replay_scope",
		},
	}); err != nil {
		t.Fatalf("logger.Log() error = %v", err)
	}

	row := loadLatestRuntimeLogRow(t, db)
	if row.RunID != runID {
		t.Fatalf("persisted run_id = %q, want %q", row.RunID, runID)
	}
	if row.SourceEventID != subjectEventID {
		t.Fatalf("persisted source_event_id = %q, want subject event %q", row.SourceEventID, subjectEventID)
	}
	if got := strings.TrimSpace(asString(row.Detail["parent_event_id"])); got != subjectEventID {
		t.Fatalf("payload details.parent_event_id = %q, want subject event %q", got, subjectEventID)
	}
}

func TestRuntimeLogger_Log_DoesNotDeriveLineageFromUnpersistedSubjectEvent(t *testing.T) {
	ctx := context.Background()
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	logger := NewRuntimeLogger(db, runtimeLogCapabilityStub{enabled: true, hasRunID: true})
	runID := uuid.NewString()
	ctx = runtimecorrelation.WithRunID(ctx, runID)

	missingSubjectEventID := uuid.NewString()
	if err := logger.Log(ctx, RuntimeLogEntry{
		Level:     "warn",
		Message:   "uncorrelated runtime diagnostic",
		Component: "eventbus",
		Action:    "outbox_replay_scope_unavailable",
		EventID:   missingSubjectEventID,
		EventType: "validation/validation.package_ready",
	}); err != nil {
		t.Fatalf("logger.Log() error = %v", err)
	}

	row := loadLatestRuntimeLogRow(t, db)
	if row.RunID != runID {
		t.Fatalf("persisted run_id = %q, want %q", row.RunID, runID)
	}
	if row.SourceEventID != "" {
		t.Fatalf("persisted source_event_id = %q, want empty for unpersisted subject event", row.SourceEventID)
	}
	if got := strings.TrimSpace(asString(row.Detail["parent_event_id"])); got != "" {
		t.Fatalf("payload details.parent_event_id = %q, want empty", got)
	}
}

func TestRuntimeLogger_Log_PersistsTypedRuntimeLineage(t *testing.T) {
	ctx := context.Background()
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	logger := NewRuntimeLogger(db, runtimeLogCapabilityStub{enabled: true, hasRunID: true})
	runID := uuid.NewString()
	subjectEventID := uuid.NewString()
	ctx = runtimecorrelation.WithRunID(ctx, runID)
	ctx = runtimecorrelation.WithRuntimeLineage(ctx, runtimecorrelation.RuntimeLineage{
		Owner:               "runtime.run_fork.selected_contract_execution.fork_local_runtime_typed_lineage",
		RunID:               runID,
		SubjectEventID:      subjectEventID,
		SubjectEventType:    "validation/validation.package_ready",
		ParentEventID:       subjectEventID,
		RowCategory:         runtimecorrelation.RuntimeLineageRowCategoryDiagnostic,
		SelectedForkOwner:   "runtime.run_fork.selected_contract_execution.fork_local_runtime_container",
		Classification:      runtimecorrelation.RuntimeLineageClassificationForkLocal,
		SelectedForkContext: true,
	})
	if err := ensureRuntimeLogRunRow(ctx, db, runID); err != nil {
		t.Fatalf("ensure run row: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			run_id, event_id, event_name, scope, payload, produced_by, produced_by_type
		)
		VALUES (
			$1::uuid, $2::uuid, 'validation/validation.package_ready', 'global', '{}'::jsonb,
			'runtime.run_fork.selected_contract_execution', 'agent'
		)
	`, runID, subjectEventID); err != nil {
		t.Fatalf("seed subject event: %v", err)
	}

	if err := logger.Log(ctx, RuntimeLogEntry{
		Level:     "warn",
		Message:   "typed runtime diagnostic",
		Component: "eventbus",
		Action:    "outbox_replay_scope_unavailable",
	}); err != nil {
		t.Fatalf("logger.Log() error = %v", err)
	}

	row := loadLatestRuntimeLogRow(t, db)
	if row.RunID != runID {
		t.Fatalf("persisted run_id = %q, want %q", row.RunID, runID)
	}
	if row.SourceEventID != subjectEventID {
		t.Fatalf("persisted source_event_id = %q, want typed parent %q", row.SourceEventID, subjectEventID)
	}
	if got := strings.TrimSpace(asString(row.Detail["parent_event_id"])); got != subjectEventID {
		t.Fatalf("payload details.parent_event_id = %q, want typed parent %q", got, subjectEventID)
	}
	if got := strings.TrimSpace(asString(row.Detail["runtime_lineage_owner"])); got != "runtime.run_fork.selected_contract_execution.fork_local_runtime_typed_lineage" {
		t.Fatalf("runtime_lineage_owner = %q", got)
	}
	if got := strings.TrimSpace(asString(row.Detail["runtime_lineage_row_category"])); got != "diagnostic" {
		t.Fatalf("runtime_lineage_row_category = %q, want diagnostic", got)
	}
	if got := strings.TrimSpace(asString(row.Detail["runtime_lineage_classification"])); got != "fork_local" {
		t.Fatalf("runtime_lineage_classification = %q, want fork_local", got)
	}
}

type persistedRuntimeLogRow struct {
	RunID         string
	SourceEventID string
	Detail        map[string]any
}

func loadLatestRuntimeLogRow(t *testing.T, db *sql.DB) persistedRuntimeLogRow {
	t.Helper()
	var (
		runID         string
		sourceEventID string
		payloadRaw    []byte
	)
	if err := db.QueryRowContext(context.Background(), `
		SELECT COALESCE(run_id::text, ''), COALESCE(source_event_id::text, ''), payload
		FROM events
		WHERE event_name = 'platform.runtime_log'
		ORDER BY created_at DESC
		LIMIT 1
	`).Scan(&runID, &sourceEventID, &payloadRaw); err != nil {
		t.Fatalf("load runtime log row: %v", err)
	}
	payload := map[string]any{}
	if err := json.Unmarshal(payloadRaw, &payload); err != nil {
		t.Fatalf("decode runtime log payload: %v", err)
	}
	detail, _ := payload["details"].(map[string]any)
	if detail == nil {
		detail = map[string]any{}
	}
	return persistedRuntimeLogRow{
		RunID:         strings.TrimSpace(runID),
		SourceEventID: strings.TrimSpace(sourceEventID),
		Detail:        detail,
	}
}

func assertRunRowExists(t *testing.T, db *sql.DB, runID string, want bool) {
	t.Helper()
	var exists bool
	if err := db.QueryRowContext(context.Background(), `SELECT EXISTS (SELECT 1 FROM runs WHERE run_id = $1::uuid)`, runID).Scan(&exists); err != nil {
		t.Fatalf("check run row %s: %v", runID, err)
	}
	if exists != want {
		t.Fatalf("run row exists = %v for %q, want %v", exists, runID, want)
	}
}

func seedRuntimeLogBundleRow(t *testing.T, db *sql.DB, bundleHash string) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO bundles (bundle_hash, content_yaml, parsed_json)
		VALUES ($1, 'name: test', '{}'::jsonb)
	`, bundleHash); err != nil {
		t.Fatalf("seed bundle row: %v", err)
	}
}

func countRuntimeLogRowsForRun(t *testing.T, db *sql.DB, runID string) int {
	t.Helper()
	var count int
	if err := db.QueryRowContext(context.Background(), `
		SELECT COUNT(*)
		FROM events
		WHERE run_id = $1::uuid
		  AND event_name = 'platform.runtime_log'
	`, runID).Scan(&count); err != nil {
		t.Fatalf("count runtime log rows for %s: %v", runID, err)
	}
	return count
}

type runtimeLogPayloadArg struct {
	level      string
	message    string
	component  string
	action     string
	eventID    string
	eventType  string
	agentID    string
	entityID   string
	sessionID  string
	errorText  string
	durationUS int
	detail     map[string]any
}

func (m runtimeLogPayloadArg) Match(v driver.Value) bool {
	text, ok := v.(string)
	if !ok {
		return false
	}
	decoded := map[string]any{}
	if err := json.Unmarshal([]byte(text), &decoded); err != nil {
		return false
	}
	if strings.TrimSpace(asString(decoded["log_level"])) != m.level {
		return false
	}
	if strings.TrimSpace(asString(decoded["message"])) != m.message {
		return false
	}
	details, ok := decoded["details"].(map[string]any)
	if !ok {
		return false
	}
	if strings.TrimSpace(asString(details["component"])) != m.component {
		return false
	}
	if strings.TrimSpace(asString(details["action"])) != m.action {
		return false
	}
	if strings.TrimSpace(asString(details["event_id"])) != m.eventID {
		return false
	}
	if strings.TrimSpace(asString(details["event_name"])) != m.eventType {
		return false
	}
	if strings.TrimSpace(asString(details["event_type"])) != m.eventType {
		return false
	}
	if strings.TrimSpace(asString(details["agent_id"])) != m.agentID {
		return false
	}
	if strings.TrimSpace(asString(details["entity_id"])) != m.entityID {
		return false
	}
	if strings.TrimSpace(asString(details["session_id"])) != m.sessionID {
		return false
	}
	if strings.TrimSpace(asString(details["error"])) != m.errorText {
		return false
	}
	if int(asFloat(details["duration_us"])) != m.durationUS {
		return false
	}
	for key, want := range m.detail {
		if got := details[key]; got != want {
			return false
		}
	}
	return true
}

func asFloat(v any) float64 {
	switch typed := v.(type) {
	case float64:
		return typed
	case int:
		return float64(typed)
	case int64:
		return float64(typed)
	default:
		return 0
	}
}
