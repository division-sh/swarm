package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	storerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/google/uuid"
)

type runtimeLogPersistenceCapture struct {
	records         []RuntimeLogPersistenceRecord
	err             error
	lineageErr      error
	lineageRequests [][3]string
}

func (c *runtimeLogPersistenceCapture) PersistRuntimeLog(_ context.Context, record RuntimeLogPersistenceRecord) error {
	c.records = append(c.records, record)
	return c.err
}

func (c *runtimeLogPersistenceCapture) RuntimeLogLineageParentEventID(_ context.Context, runID, explicitParentEventID, subjectEventID string) (string, error) {
	c.lineageRequests = append(c.lineageRequests, [3]string{runID, explicitParentEventID, subjectEventID})
	if c.lineageErr != nil {
		return "", c.lineageErr
	}
	return strings.TrimSpace(explicitParentEventID), nil
}

func (*runtimeLogPersistenceCapture) PersistLifecycleDiagnostic(context.Context, diaglog.LifecycleDiagnostic, RuntimeLogPersistenceRecord) (bool, error) {
	return false, fmt.Errorf("lifecycle diagnostic persistence is not configured")
}

func TestRuntimeLogCaptureRetainsExactFactsWithoutInventingDurability(t *testing.T) {
	capture := &runtimeLogPersistenceCapture{}
	record := RuntimeLogPersistenceRecord{
		EventID: uuid.NewString(), CreatedAt: time.Unix(123, 456).UTC(), RunID: uuid.NewString(),
		ParentEventID: uuid.NewString(), Payload: []byte(`{"message":"exact unit record"}`), ExecutionMode: executionmode.Live,
	}
	if err := capture.PersistRuntimeLog(context.Background(), record); err != nil || len(capture.records) != 1 || !reflect.DeepEqual(capture.records[0], record) {
		t.Fatalf("capture replaced typed record facts: records=%+v err=%v", capture.records, err)
	}
	subject := uuid.NewString()
	if parent, err := capture.RuntimeLogLineageParentEventID(context.Background(), record.RunID, "", subject); err != nil || parent != "" {
		t.Fatalf("capture invented persisted subject lineage: %q/%v", parent, err)
	}
	if parent, err := capture.RuntimeLogLineageParentEventID(context.Background(), record.RunID, record.ParentEventID, subject); err != nil || parent != record.ParentEventID {
		t.Fatalf("capture lost supplied explicit lineage fact: %q/%v", parent, err)
	}
}

func newTestRuntimeLogger(persistence RuntimeLogPersistence) *RuntimeLogger {
	return NewRuntimeLogger(persistence, executionposture.Live, func(_ context.Context, event events.Event, flowID string) (events.PayloadAdmission, error) {
		return eventtest.PayloadAdmission(event, flowID, string(event.Type()))
	})
}

func assertCapturedRuntimeLog(t testing.TB, capture *runtimeLogPersistenceCapture, want runtimeLogPayloadArg, wantRunID, wantParentEventID string) {
	t.Helper()
	if capture == nil || len(capture.records) != 1 {
		t.Fatalf("captured runtime logs = %#v, want one", capture)
	}
	record := capture.records[0]
	if record.RunID != wantRunID || record.ParentEventID != wantParentEventID {
		t.Fatalf("captured runtime log scope = run:%q parent:%q, want run:%q parent:%q", record.RunID, record.ParentEventID, wantRunID, wantParentEventID)
	}
	if !want.MatchPayload(record.Payload) {
		t.Fatalf("captured runtime log payload = %s, want %#v", record.Payload, want)
	}
}

func TestRuntimeLoggerPersistsNormalizedAdmissionBytes(t *testing.T) {
	capture := &runtimeLogPersistenceCapture{}
	logger := NewRuntimeLogger(capture, executionposture.Live, func(_ context.Context, event events.Event, flowID string) (events.PayloadAdmission, error) {
		var payload map[string]any
		if err := json.Unmarshal(event.Payload(), &payload); err != nil {
			return events.PayloadAdmission{}, err
		}
		details := payload["details"].(map[string]any)
		delete(details, "admission_only_field")
		normalized, err := json.Marshal(payload)
		if err != nil {
			return events.PayloadAdmission{}, err
		}
		fixture, err := eventtest.PayloadAdmission(event, flowID, string(event.Type()))
		if err != nil {
			return events.PayloadAdmission{}, err
		}
		return events.NewPayloadAdmission(normalized, fixture.Binding())
	})
	if err := logger.Log(context.Background(), RuntimeLogEntry{
		Level: "info", Message: "normalized", Component: "runtime", Action: "proof",
		Detail: map[string]any{"admission_only_field": "remove-me"},
	}); err != nil {
		t.Fatalf("Log: %v", err)
	}
	if len(capture.records) != 1 {
		t.Fatalf("records = %d, want 1", len(capture.records))
	}
	record := capture.records[0]
	if string(record.Payload) != string(record.PayloadAdmission.Payload()) {
		t.Fatalf("persisted payload = %s, admission = %s", record.Payload, record.PayloadAdmission.Payload())
	}
	var persisted map[string]any
	if err := json.Unmarshal(record.Payload, &persisted); err != nil {
		t.Fatalf("decode persisted payload: %v", err)
	}
	if _, present := persisted["details"].(map[string]any)["admission_only_field"]; present {
		t.Fatalf("pre-admission detail survived persistence: %#v", persisted)
	}
}

func TestRuntimeLoggerOmitsAbsentNestedDetailFieldsBeforeAdmission(t *testing.T) {
	capture := &runtimeLogPersistenceCapture{}
	logger := NewRuntimeLogger(capture, executionposture.Live, func(_ context.Context, event events.Event, flowID string) (events.PayloadAdmission, error) {
		return eventtest.PayloadAdmission(event, flowID, string(event.Type()))
	})
	if err := logger.Log(context.Background(), RuntimeLogEntry{
		Level: "debug", Message: "delivered", Component: "eventbus", Action: "delivered",
		Detail: map[string]any{
			"absent": nil,
			"nested": map[string]any{"present": "value", "absent": nil},
		},
	}); err != nil {
		t.Fatalf("Log: %v", err)
	}
	if len(capture.records) != 1 {
		t.Fatalf("records = %d, want 1", len(capture.records))
	}
	var payload map[string]any
	if err := json.Unmarshal(capture.records[0].Payload, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	details := payload["details"].(map[string]any)
	if _, present := details["absent"]; present {
		t.Fatalf("top-level absent detail survived admission: %#v", details)
	}
	nested := details["nested"].(map[string]any)
	if _, present := nested["absent"]; present || nested["present"] != "value" {
		t.Fatalf("nested details = %#v, want only present value", nested)
	}
}

func TestRuntimeLoggerRejectsNullDetailListElements(t *testing.T) {
	capture := &runtimeLogPersistenceCapture{}
	logger := NewRuntimeLogger(capture, executionposture.Live, func(_ context.Context, event events.Event, flowID string) (events.PayloadAdmission, error) {
		return eventtest.PayloadAdmission(event, flowID, string(event.Type()))
	})
	err := logger.Log(context.Background(), RuntimeLogEntry{
		Level: "debug", Message: "delivered", Component: "eventbus", Action: "delivered",
		Detail: map[string]any{"items": []any{"present", nil}},
	})
	if err == nil || !strings.Contains(err.Error(), "cannot contain null") {
		t.Fatalf("Log error = %v, want null rejection", err)
	}
	if len(capture.records) != 0 {
		t.Fatalf("records = %d, want no persistence after rejected admission", len(capture.records))
	}
}

func TestCanonicalRuntimeLogDecoderRejectsNestedNullDetailListElements(t *testing.T) {
	_, err := DecodeCanonicalRuntimeLogPayload([]byte(`{"log_level":"debug","message":"delivered","details":{"component":"eventbus","action":"delivered","nested":{"items":["present",null]}}}`))
	if err == nil || !strings.Contains(err.Error(), "cannot contain null") {
		t.Fatalf("DecodeCanonicalRuntimeLogPayload error = %v, want null-list refusal", err)
	}
}

func TestRuntimeLogger_Log_AppendsSpecShapedFlightRecorderEntry(t *testing.T) {
	capture := &runtimeLogPersistenceCapture{}
	wantPersisted := runtimeLogPayloadArg{
		level:        "warn",
		message:      "Tool execution was denied for save_entity_field",
		component:    "tool-executor",
		action:       "tool_execution_denied",
		eventID:      "evt-1",
		eventType:    "validation/requested",
		agentID:      "agent-1",
		entityID:     "entity-1",
		sessionID:    "session-1",
		failureCode:  "cross_flow_write_forbidden",
		failureClass: runtimefailures.ClassAuthorizationDenied,
		durationUS:   1200,
		detail: map[string]any{
			"tool_name":     "save_entity_field",
			"denial_layer":  "executor",
			"denial_reason": "cross_flow_write_forbidden",
		},
	}

	logger := newTestRuntimeLogger(capture)
	recorder := runtimebus.NewEmittedEventsRecorder()
	ctx := runtimebus.WithEmittedEventsRecorder(testAuthorActivityContext(context.Background()), recorder)
	failure := runtimefailures.Normalize(runtimefailures.New(
		runtimefailures.ClassAuthorizationDenied,
		"cross_flow_write_forbidden",
		"tool-executor",
		"tool_execution_denied",
		map[string]any{"action": "write_entity_field"},
	), "tool-executor", "tool_execution_denied")

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
		Failure:    &failure,
		DurationUS: 1200,
		Detail: map[string]any{
			"tool_name":     "save_entity_field",
			"denial_layer":  "executor",
			"denial_reason": "cross_flow_write_forbidden",
		},
	}); err != nil {
		t.Fatalf("logger.Log() error = %v", err)
	}
	assertCapturedRuntimeLog(t, capture, wantPersisted, "", "")

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
	if _, ok := details["error"]; ok {
		t.Fatalf("details.error survives canonicalization: %#v", details["error"])
	}
	failureMap, ok := details["failure"].(map[string]any)
	if !ok || failureMap["class"] != string(runtimefailures.ClassAuthorizationDenied) {
		t.Fatalf("details.failure = %#v", details["failure"])
	}
	detail, ok := failureMap["detail"].(map[string]any)
	if !ok || detail["code"] != "cross_flow_write_forbidden" {
		t.Fatalf("details.failure.detail = %#v", failureMap["detail"])
	}
}

func TestRuntimeLogger_Log_AppendsCanonicalFlightRecorderDefaults(t *testing.T) {
	capture := &runtimeLogPersistenceCapture{}
	wantPersisted := runtimeLogPayloadArg{
		level:     "warn",
		message:   "runtime warning",
		component: "runtime",
		action:    "unknown",
		eventType: "diagnostic/actual",
	}

	logger := newTestRuntimeLogger(capture)
	recorder := runtimebus.NewEmittedEventsRecorder()
	ctx := runtimebus.WithEmittedEventsRecorder(testAuthorActivityContext(context.Background()), recorder)

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
	assertCapturedRuntimeLog(t, capture, wantPersisted, "", "")

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
}

func TestRuntimeLogger_Log_PersistsRuntimeLogPayloadViaCapabilityOwner(t *testing.T) {
	capture := &runtimeLogPersistenceCapture{}
	wantPersisted := runtimeLogPayloadArg{
		level:        "warn",
		message:      "Tool execution was denied for save_entity_field",
		component:    "tool-executor",
		action:       "tool_execution_denied",
		eventID:      "evt-1",
		eventType:    "validation/requested",
		agentID:      "agent-1",
		entityID:     "entity-1",
		sessionID:    "session-1",
		failureCode:  "cross_flow_write_forbidden",
		failureClass: runtimefailures.ClassAuthorizationDenied,
		durationUS:   1200,
		detail: map[string]any{
			"tool_name":     "save_entity_field",
			"denial_layer":  "executor",
			"denial_reason": "cross_flow_write_forbidden",
		},
	}

	logger := newTestRuntimeLogger(capture)
	failure := runtimefailures.Normalize(runtimefailures.New(
		runtimefailures.ClassAuthorizationDenied,
		"cross_flow_write_forbidden",
		"tool-executor",
		"tool_execution_denied",
		map[string]any{"action": "write_entity_field"},
	), "tool-executor", "tool_execution_denied")
	if err := logger.Log(testAuthorActivityContext(context.Background()), RuntimeLogEntry{
		Level:      "warn",
		Message:    "Tool execution was denied for save_entity_field",
		Component:  "tool-executor",
		Action:     "tool_execution_denied",
		EventID:    "evt-1",
		EventType:  "validation/requested",
		AgentID:    "agent-1",
		EntityID:   "entity-1",
		SessionID:  "session-1",
		Failure:    &failure,
		DurationUS: 1200,
		Detail: map[string]any{
			"tool_name":     "save_entity_field",
			"denial_layer":  "executor",
			"denial_reason": "cross_flow_write_forbidden",
		},
	}); err != nil {
		t.Fatalf("logger.Log() error = %v", err)
	}
	assertCapturedRuntimeLog(t, capture, wantPersisted, "", "")
}

func TestRuntimeLogger_Log_PassesRunScopeToPersistenceOwner(t *testing.T) {
	const runID = "8d4891f8-0f8e-4c85-b34b-9e0e7f4327dd"
	recorder := runtimebus.NewEmittedEventsRecorder()
	ctx := runtimebus.WithEmittedEventsRecorder(testAuthorActivityContext(context.Background()), recorder)
	ctx = runtimecorrelation.WithRunID(ctx, runID)

	capture := &runtimeLogPersistenceCapture{}
	logger := newTestRuntimeLogger(capture)
	if err := logger.Log(ctx, RuntimeLogEntry{
		Level:     "error",
		Message:   "runtime log",
		Component: "workflow-runtime",
		Action:    "handler_error",
	}); err != nil {
		t.Fatalf("logger.Log() error = %v", err)
	}
	assertCapturedRuntimeLog(t, capture, runtimeLogPayloadArg{
		level: "error", message: "runtime log", component: "workflow-runtime", action: "handler_error",
		detail: map[string]any{"run_id": runID},
	}, runID, "")
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

func VerifyRuntimeLogger_Log_StampsSourceArtifactFactOnRunRowForTest(t *testing.T, open RuntimeLogNativeOpenerForTest) {
	artifact := admittedRuntimeLogSourceArtifactForTest(t)
	fixture := open(t, artifact)
	logger := newTestRuntimeLogger(fixture.Persistence)
	runID := uuid.NewString()
	sourceFact := testPersistedSourceArtifactFact(t, artifact.BundleHash())
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContextForBundle(testAuthorActivityContext(context.Background()), sourceFact.BundleHash()), runID)
	ctx = runtimecorrelation.WithSourceArtifactFact(ctx, sourceFact)
	before := requireNativeRuntimeLogRunForTest(t, fixture, ctx, runID)

	if err := logger.Log(ctx, RuntimeLogEntry{
		Level:     "info",
		Message:   "runtime log",
		Component: "workflow-runtime",
		Action:    "source_artifact_fact",
	}); err != nil {
		t.Fatalf("logger.Log: %v", err)
	}
	stored, err := fixture.RunSnapshot(ctx, runID)
	if err != nil {
		t.Fatalf("load run source artifact: %v", err)
	}
	gotHash := stored.BundleHash
	if gotHash != sourceFact.BundleHash() {
		t.Fatalf("run source artifact hash = %q, want %q", gotHash, sourceFact.BundleHash())
	}
	requireNativeRuntimeLogRunPreservedForTest(t, fixture, ctx, before)
}

func VerifyRuntimeLogSetupRejectsDeletedPersistedSourceArtifactFactForTest(t *testing.T, open RuntimeLogNativeOpenerForTest) {
	artifact := admittedRuntimeLogSourceArtifactForTest(t)
	fixture := open(t, artifact)
	runID := uuid.NewString()
	sourceFact := testPersistedSourceArtifactFact(t, artifact.BundleHash())
	if removed, err := fixture.RemoveArtifact(fixture.Context, sourceFact.BundleHash()); err != nil || removed != 1 {
		t.Fatalf("delete source artifact row: %v", err)
	}
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContextForBundle(testAuthorActivityContext(context.Background()), sourceFact.BundleHash()), runID)
	ctx = runtimecorrelation.WithSourceArtifactFact(ctx, sourceFact)

	before := fixture.Physical(ctx)
	_, err := fixture.Runs.CreateRun(ctx, storerunlifecycle.CreateRequest{RunID: runID, Source: sourceFact, Origin: storerunlifecycle.ScenarioSetupRunOrigin(), StartedAt: time.Now().UTC()})
	if !errors.Is(err, storerunlifecycle.ErrSourceArtifactUnavailable) {
		t.Fatalf("run setup error = %v, want ErrSourceArtifactUnavailable", err)
	}
	assertRunRowExists(t, fixture, ctx, runID, false)
	if counts := fixture.Physical(ctx); counts != before || counts.Events != 0 {
		t.Fatalf("source-refused setup changed physical storage: before=%+v after=%+v", before, counts)
	}
}

func TestRuntimeLogger_Log_ReturnsPersistenceFailure(t *testing.T) {
	writeErr := errors.New("insert failed")
	capture := &runtimeLogPersistenceCapture{err: writeErr}

	recorder := runtimebus.NewEmittedEventsRecorder()
	ctx := runtimebus.WithEmittedEventsRecorder(testAuthorActivityContext(context.Background()), recorder)
	logger := newTestRuntimeLogger(capture)
	err := logger.Log(ctx, RuntimeLogEntry{
		Level:     "error",
		Message:   "Persisting the pipeline receipt failed",
		Component: "eventbus",
		Action:    "pipeline_receipt_persist_failed",
	})
	if !errors.Is(err, writeErr) {
		t.Fatalf("logger.Log() error = %v, want %v", err, writeErr)
	}
	if entries := recorder.SnapshotFlightRecorder(); len(entries) != 0 {
		t.Fatalf("flight recorder count = %d, want 0", len(entries))
	}
}

func TestRuntimeLogger_Log_AllowsEmptyCanonicalMessageWhenDetailsExist(t *testing.T) {
	capture := &runtimeLogPersistenceCapture{}
	wantPersisted := runtimeLogPayloadArg{
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
	}

	logger := newTestRuntimeLogger(capture)
	if err := logger.Log(testAuthorActivityContext(context.Background()), RuntimeLogEntry{
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
	assertCapturedRuntimeLog(t, capture, wantPersisted, "", "")
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

func TestDecodeCanonicalRuntimeLogPayloadRejectsRetiredErrorCarrier(t *testing.T) {
	_, err := DecodeCanonicalRuntimeLogPayload([]byte(`{
		"log_level":"error",
		"message":"legacy",
		"details":{"component":"runtime","action":"legacy_failure","error":"raw prose"}
	}`))
	if err == nil || !strings.Contains(err.Error(), "details.error is not a supported details field") {
		t.Fatalf("DecodeCanonicalRuntimeLogPayload() error = %v, want retired error carrier failure", err)
	}
}

func TestRuntimeLogger_Log_DoesNotAppendFlightRecorderOnPayloadValidationFailure(t *testing.T) {
	recorder := runtimebus.NewEmittedEventsRecorder()
	ctx := runtimebus.WithEmittedEventsRecorder(testAuthorActivityContext(context.Background()), recorder)
	capture := &runtimeLogPersistenceCapture{}
	logger := newTestRuntimeLogger(capture)
	err := logger.Log(ctx, RuntimeLogEntry{
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
	if len(capture.records) != 0 {
		t.Fatal("payload refusal reached persistence")
	}
	if entries := recorder.SnapshotFlightRecorder(); len(entries) != 0 {
		t.Fatalf("flight recorder count = %d, want 0", len(entries))
	}
}

func TestRuntimeLogger_Log_DoesNotAppendFlightRecorderOnLineageLookupFailure(t *testing.T) {
	runID := uuid.NewString()
	subjectEventID := uuid.NewString()
	lineageErr := errors.New("lineage lookup failed")
	capture := &runtimeLogPersistenceCapture{lineageErr: lineageErr}

	recorder := runtimebus.NewEmittedEventsRecorder()
	ctx := runtimebus.WithEmittedEventsRecorder(testAuthorActivityContext(context.Background()), recorder)
	ctx = runtimecorrelation.WithRunID(ctx, runID)
	logger := newTestRuntimeLogger(capture)
	err := logger.Log(ctx, RuntimeLogEntry{
		Level:     "warn",
		Message:   "runtime log",
		Component: "eventbus",
		Action:    "lineage_lookup",
		EventID:   subjectEventID,
	})
	if !errors.Is(err, lineageErr) {
		t.Fatalf("logger.Log() error = %v, want %v", err, lineageErr)
	}
	if len(capture.lineageRequests) != 1 || capture.lineageRequests[0] != ([3]string{runID, "", subjectEventID}) || len(capture.records) != 0 {
		t.Fatalf("lineage refusal lost exact arguments or reached persistence: %+v", capture)
	}
	if entries := recorder.SnapshotFlightRecorder(); len(entries) != 0 {
		t.Fatalf("flight recorder count = %d, want 0", len(entries))
	}
}

func TestRuntimeLogger_Log_DoesNotAppendFlightRecorderOnRunOwnerFailure(t *testing.T) {
	runID := uuid.NewString()
	runRowErr := errors.New("run row failed")
	capture := &runtimeLogPersistenceCapture{err: runRowErr}

	recorder := runtimebus.NewEmittedEventsRecorder()
	ctx := runtimebus.WithEmittedEventsRecorder(testAuthorActivityContext(context.Background()), recorder)
	ctx = runtimecorrelation.WithRunID(ctx, runID)
	logger := newTestRuntimeLogger(capture)
	err := logger.Log(ctx, RuntimeLogEntry{
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
}

func TestRuntimeLogger_Log_DoesNotAppendFlightRecorderOnPostAppendOwnerFailure(t *testing.T) {
	runID := uuid.NewString()
	syncErr := errors.New("sync failed")
	capture := &runtimeLogPersistenceCapture{err: syncErr}

	recorder := runtimebus.NewEmittedEventsRecorder()
	ctx := runtimebus.WithEmittedEventsRecorder(testAuthorActivityContext(context.Background()), recorder)
	ctx = runtimecorrelation.WithRunID(ctx, runID)
	logger := newTestRuntimeLogger(capture)
	err := logger.Log(ctx, RuntimeLogEntry{
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
}

func VerifyRuntimeLogger_Log_PersistsCanonicalRunOwnershipFromContextForTest(t *testing.T, open RuntimeLogNativeOpenerForTest) {
	artifact := admittedRuntimeLogSourceArtifactForTest(t)
	fixture := open(t, artifact)
	ctx := testAuthorActivityContextForBundle(context.Background(), artifact.BundleHash())
	logger := newTestRuntimeLogger(fixture.Persistence)
	runID := uuid.NewString()
	spoofedRunID := uuid.NewString()
	ctx = runtimecorrelation.WithRunID(ctx, runID)
	before := requireNativeRuntimeLogRunForTest(t, fixture, ctx, runID)

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

	row := loadLatestRuntimeLogRow(t, fixture, ctx)
	if row.RunID != runID {
		t.Fatalf("persisted run_id = %q, want %q", row.RunID, runID)
	}
	if got := strings.TrimSpace(asString(row.Detail["run_id"])); got != runID {
		t.Fatalf("payload details.run_id = %q, want %q", got, runID)
	}
	if got := strings.TrimSpace(asString(row.Detail["note"])); got != "context must win" {
		t.Fatalf("payload details.note = %q, want context must win", got)
	}
	assertRunRowExists(t, fixture, ctx, runID, true)
	assertRunRowExists(t, fixture, ctx, spoofedRunID, false)
	requireNativeRuntimeLogRunPreservedForTest(t, fixture, ctx, before)
}

func VerifyRuntimeLogger_Log_DoesNotInferRunOwnershipFromDetailPayloadForTest(t *testing.T, open RuntimeLogNativeOpenerForTest) {
	artifact := admittedRuntimeLogSourceArtifactForTest(t)
	fixture := open(t, artifact)
	ctx := fixture.Context
	logger := newTestRuntimeLogger(fixture.Persistence)
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

	row := loadLatestRuntimeLogRow(t, fixture, ctx)
	if row.RunID != "" {
		t.Fatalf("persisted run_id = %q, want empty", row.RunID)
	}
	if got := strings.TrimSpace(asString(row.Detail["run_id"])); got != "" {
		t.Fatalf("payload details.run_id = %q, want empty", got)
	}
	if got := strings.TrimSpace(asString(row.Detail["note"])); got != "must remain unscoped" {
		t.Fatalf("payload details.note = %q, want must remain unscoped", got)
	}
	assertRunRowExists(t, fixture, ctx, payloadRunID, false)
}

func VerifyRuntimeLogger_Log_DerivesLineageFromPersistedSubjectEventForTest(t *testing.T, open RuntimeLogNativeOpenerForTest) {
	artifact := admittedRuntimeLogSourceArtifactForTest(t)
	fixture := open(t, artifact)
	ctx := testAuthorActivityContextForBundle(context.Background(), artifact.BundleHash())
	logger := newTestRuntimeLogger(fixture.Persistence)
	runID := uuid.NewString()
	subjectEventID := uuid.NewString()
	ctx = runtimecorrelation.WithRunID(ctx, runID)
	before := requireNativeRuntimeLogRunForTest(t, fixture, ctx, runID)
	fixture.PublishSubject(ctx, eventtest.PersistedChildForProducer(
		subjectEventID, events.EventType("validation/validation.package_ready"),
		eventtest.Producer(events.EventProducerAgent, "runtime.run_fork.selected_contract_execution"),
		"", []byte(`{}`), 0, runID, eventtest.UUID("diagnostic-subject-parent:"+subjectEventID), events.EventEnvelope{Scope: events.EventScopeGlobal}, time.Now().UTC(),
	))

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

	row := loadLatestRuntimeLogRow(t, fixture, ctx)
	if row.RunID != runID {
		t.Fatalf("persisted run_id = %q, want %q", row.RunID, runID)
	}
	if row.SourceEventID != subjectEventID {
		t.Fatalf("persisted source_event_id = %q, want subject event %q", row.SourceEventID, subjectEventID)
	}
	if got := strings.TrimSpace(asString(row.Detail["parent_event_id"])); got != subjectEventID {
		t.Fatalf("payload details.parent_event_id = %q, want subject event %q", got, subjectEventID)
	}
	requireNativeRuntimeLogRunPreservedForTest(t, fixture, ctx, before)
}

func VerifyRuntimeLogger_Log_DoesNotDeriveLineageFromUnpersistedSubjectEventForTest(t *testing.T, open RuntimeLogNativeOpenerForTest) {
	artifact := admittedRuntimeLogSourceArtifactForTest(t)
	fixture := open(t, artifact)
	ctx := testAuthorActivityContextForBundle(context.Background(), artifact.BundleHash())
	logger := newTestRuntimeLogger(fixture.Persistence)
	runID := uuid.NewString()
	ctx = runtimecorrelation.WithRunID(ctx, runID)
	before := requireNativeRuntimeLogRunForTest(t, fixture, ctx, runID)

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

	row := loadLatestRuntimeLogRow(t, fixture, ctx)
	if row.RunID != runID {
		t.Fatalf("persisted run_id = %q, want %q", row.RunID, runID)
	}
	if row.SourceEventID != "" {
		t.Fatalf("persisted source_event_id = %q, want empty for unpersisted subject event", row.SourceEventID)
	}
	if got := strings.TrimSpace(asString(row.Detail["parent_event_id"])); got != "" {
		t.Fatalf("payload details.parent_event_id = %q, want empty", got)
	}
	requireNativeRuntimeLogRunPreservedForTest(t, fixture, ctx, before)
}

func VerifyRuntimeLogger_Log_PersistsTypedRuntimeLineageForTest(t *testing.T, open RuntimeLogNativeOpenerForTest) {
	artifact := admittedRuntimeLogSourceArtifactForTest(t)
	fixture := open(t, artifact)
	ctx := testAuthorActivityContextForBundle(context.Background(), artifact.BundleHash())
	logger := newTestRuntimeLogger(fixture.Persistence)
	runID := uuid.NewString()
	subjectEventID := uuid.NewString()
	ctx = runtimecorrelation.WithRunID(ctx, runID)
	before := requireNativeRuntimeLogRunForTest(t, fixture, ctx, runID)
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
	fixture.PublishSubject(ctx, eventtest.PersistedChildForProducer(
		subjectEventID, events.EventType("validation/validation.package_ready"),
		eventtest.Producer(events.EventProducerAgent, "runtime.run_fork.selected_contract_execution"),
		"", []byte(`{}`), 0, runID, eventtest.UUID("diagnostic-subject-parent:"+subjectEventID), events.EventEnvelope{Scope: events.EventScopeGlobal}, time.Now().UTC(),
	))

	if err := logger.Log(ctx, RuntimeLogEntry{
		Level:     "warn",
		Message:   "typed runtime diagnostic",
		Component: "eventbus",
		Action:    "outbox_replay_scope_unavailable",
	}); err != nil {
		t.Fatalf("logger.Log() error = %v", err)
	}

	row := loadLatestRuntimeLogRow(t, fixture, ctx)
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
	requireNativeRuntimeLogRunPreservedForTest(t, fixture, ctx, before)
}

type persistedRuntimeLogRow struct {
	RunID         string
	SourceEventID string
	Detail        map[string]any
}

func loadLatestRuntimeLogRow(t *testing.T, fixture RuntimeLogNativeFixtureForTest, ctx context.Context) persistedRuntimeLogRow {
	t.Helper()
	event, err := fixture.LatestLog(ctx)
	if err != nil {
		t.Fatalf("load runtime log row: %v", err)
	}
	payload := map[string]any{}
	if err := json.Unmarshal(event.Payload(), &payload); err != nil {
		t.Fatalf("decode runtime log payload: %v", err)
	}
	detail, _ := payload["details"].(map[string]any)
	if detail == nil {
		detail = map[string]any{}
	}
	return persistedRuntimeLogRow{
		RunID:         event.RunID(),
		SourceEventID: event.ParentEventID(),
		Detail:        detail,
	}
}

func assertRunRowExists(t *testing.T, fixture RuntimeLogNativeFixtureForTest, ctx context.Context, runID string, want bool) {
	t.Helper()
	exists, err := fixture.RunPresence(ctx, runID)
	if err != nil {
		t.Fatalf("check run row %s: %v", runID, err)
	}
	if exists != want {
		t.Fatalf("run row exists = %v for %q, want %v", exists, runID, want)
	}
}

func admittedRuntimeLogSourceArtifactForTest(t *testing.T) *sourceartifact.AdmittedSourceArtifact {
	t.Helper()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "schema.yaml"), []byte("name: runtime-log-test\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	artifact, err := sourceartifact.AdmitDirectory(root)
	if err != nil {
		t.Fatalf("admit runtime log source artifact: %v", err)
	}
	return artifact
}

type runtimeLogPayloadArg struct {
	level        string
	message      string
	component    string
	action       string
	eventID      string
	eventType    string
	agentID      string
	entityID     string
	sessionID    string
	failureCode  string
	failureClass runtimefailures.Class
	durationUS   int
	detail       map[string]any
}

func (m runtimeLogPayloadArg) MatchPayload(payload []byte) bool {
	decoded := map[string]any{}
	if err := json.Unmarshal(payload, &decoded); err != nil {
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
	if m.failureCode != "" {
		failure, ok := details["failure"].(map[string]any)
		class := m.failureClass
		if class == "" {
			class = runtimefailures.ClassInternalFailure
		}
		if !ok || strings.TrimSpace(asString(failure["class"])) != string(class) {
			return false
		}
		detail, ok := failure["detail"].(map[string]any)
		if !ok || strings.TrimSpace(asString(detail["code"])) != m.failureCode {
			return false
		}
	} else if _, ok := details["failure"]; ok {
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
