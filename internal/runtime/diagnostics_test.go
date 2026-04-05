package runtime

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	runtimebus "swarm/internal/runtime/bus"
	runtimecorrelation "swarm/internal/runtime/correlation"
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
	logger := NewRuntimeLogger(nil, nil)
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
}

func TestRuntimeLogger_Log_PersistsRuntimeLogPayloadViaCapabilityOwner(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectExec(`INSERT INTO events`).
		WithArgs("", runtimeLogPayloadArg{
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

	logger := NewRuntimeLogger(db, runtimeLogCapabilityStub{enabled: true, hasRunID: true})
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
	ctx := runtimecorrelation.WithRunID(context.Background(), runID)

	mock.ExpectExec(`INSERT INTO runs`).
		WithArgs(runID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO events`).
		WithArgs(runID, sqlmock.AnyArg(), "").
		WillReturnResult(sqlmock.NewResult(0, 1))

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
}

func TestRuntimeLogger_Log_ReturnsPersistenceFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	writeErr := errors.New("insert failed")
	mock.ExpectExec(`INSERT INTO events`).
		WithArgs("", sqlmock.AnyArg(), "").
		WillReturnError(writeErr)

	logger := NewRuntimeLogger(db, runtimeLogCapabilityStub{enabled: true, hasRunID: true})
	err = logger.Log(context.Background(), RuntimeLogEntry{
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
}

func TestRuntimeLogger_Log_FailsClosedWithoutCanonicalCapability(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	logger := NewRuntimeLogger(db, runtimeLogCapabilityStub{})
	if err := logger.Log(context.Background(), RuntimeLogEntry{
		Level:   "info",
		Message: "runtime log",
	}); err != nil {
		t.Fatalf("logger.Log() error = %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
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
