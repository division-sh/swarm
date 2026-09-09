package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

// RuntimeLogEntry is a structured runtime operation record (spec v2.0.14).
type RuntimeLogEntry struct {
	Level     diaglog.Level
	Message   string
	Component string
	Action    string

	EventID     string
	EventType   string
	AgentID     string
	EntityID    string
	SessionID   string
	Correlation map[string]string

	Detail     any
	Failure    *runtimefailures.Envelope
	StackTrace string
	DurationUS int
}

func (e RuntimeLogEntry) EffectiveEntityID() string {
	return strings.TrimSpace(e.EntityID)
}

func (e *RuntimeLogEntry) NormalizeEntityID() {
	if e == nil {
		return
	}
	entityID := e.EffectiveEntityID()
	e.EntityID = entityID
}

type RuntimeLogger struct {
	persistence     RuntimeLogPersistence
	posture         executionposture.Posture
	payloadAdmitter runtimebus.PayloadAdmitter
}

// RuntimeLogPersistence owns backend-specific platform.runtime_log persistence
// and lineage lookup while RuntimeLogger owns canonical payload construction.
type RuntimeLogPersistence interface {
	PersistLifecycleDiagnostic(context.Context, diaglog.LifecycleDiagnostic, RuntimeLogPersistenceRecord) (bool, error)
	RuntimeLogLineageParentEventID(ctx context.Context, runID, explicitParentEventID, subjectEventID string) (string, error)
	PersistRuntimeLog(ctx context.Context, record RuntimeLogPersistenceRecord) error
}

type RuntimeLogPersistenceRecord struct {
	EventID          string
	CreatedAt        time.Time
	RunID            string
	Payload          []byte
	PayloadAdmission events.PayloadAdmission
	ParentEventID    string
	ExecutionMode    executionmode.Mode
}

// RuntimeLogFacts are explicit persistence facts. Durable domain projections
// supply their original occurrence; ordinary logs resolve these facts at call time.
type RuntimeLogFacts struct {
	EventID, RunID, ParentEventID, HandlerID string
	CreatedAt                                time.Time
	ExecutionMode                            executionmode.Mode
}

func EncodeRuntimeLogRecord(facts RuntimeLogFacts, e RuntimeLogEntry) (RuntimeLogPersistenceRecord, CanonicalRuntimeLogPayload, error) {
	if e.Failure != nil {
		if err := runtimefailures.ValidateEnvelope(*e.Failure); err != nil {
			return RuntimeLogPersistenceRecord{}, CanonicalRuntimeLogPayload{}, err
		}
	}
	detail, err := json.Marshal(e.Detail)
	if err != nil {
		return RuntimeLogPersistenceRecord{}, CanonicalRuntimeLogPayload{}, err
	}
	detailMap := map[string]any{}
	if e.Detail != nil {
		if err := json.Unmarshal(detail, &detailMap); err != nil {
			return RuntimeLogPersistenceRecord{}, CanonicalRuntimeLogPayload{}, err
		}
	}
	component, action := strings.TrimSpace(e.Component), strings.TrimSpace(e.Action)
	if component == "" {
		component = "runtime"
	}
	if action == "" {
		action = "unknown"
	}
	encoded, err := json.Marshal(runtimeLogPayload(diaglog.NormalizeLevel(e.Level.String()).String(), component, action, e, detailMap, facts.RunID, facts.ParentEventID, facts.HandlerID))
	if err != nil {
		return RuntimeLogPersistenceRecord{}, CanonicalRuntimeLogPayload{}, err
	}
	payload, err := DecodeCanonicalRuntimeLogPayload(encoded)
	if err != nil {
		return RuntimeLogPersistenceRecord{}, CanonicalRuntimeLogPayload{}, err
	}
	return RuntimeLogPersistenceRecord{EventID: facts.EventID, CreatedAt: facts.CreatedAt, RunID: facts.RunID, ParentEventID: facts.ParentEventID, ExecutionMode: facts.ExecutionMode, Payload: encoded}, payload, nil
}

func NewRuntimeLogger(persistence RuntimeLogPersistence, posture executionposture.Posture, payloadAdmitter runtimebus.PayloadAdmitter) *RuntimeLogger {
	return &RuntimeLogger{persistence: persistence, posture: posture, payloadAdmitter: payloadAdmitter}
}

// EncodeLifecycleDiagnosticLog is the canonical lifecycle diagnostic payload
// owner shared by presentation and exact selected-store admission.
func EncodeLifecycleDiagnosticLog(item diaglog.LifecycleDiagnostic) ([]byte, error) {
	if err := item.Validate(); err != nil {
		return nil, err
	}
	detail := make(map[string]any, len(item.Payload)+4)
	for key, value := range item.Payload {
		detail[key] = value
	}
	detail["outbox_id"] = item.OutboxID
	detail["operation_id"] = item.OperationID
	detail["event_name"] = item.EventName
	fields, err := item.Identity.StorageFields()
	if err != nil {
		return nil, err
	}
	detail["agent_identity"] = fields
	entry := RuntimeLogEntry{Level: diaglog.LevelInfo, Component: "agent-lifecycle", Action: item.EventName, AgentID: item.AgentID}
	lineage, err := item.ProducerLineage()
	if err != nil {
		return nil, err
	}
	if lineage.RunID != "" {
		runtimeLogAddLineageDetails(detail, runtimeLogLineageForEntry(lineage, entry))
	}
	return json.Marshal(runtimeLogPayload("info", entry.Component, entry.Action, entry, detail, item.Identity.RunID, "", ""))
}

func (l *RuntimeLogger) ProjectLifecycleDiagnostic(ctx context.Context, item diaglog.LifecycleDiagnostic) error {
	if l == nil || l.persistence == nil {
		return fmt.Errorf("lifecycle diagnostic persistence is required")
	}
	encoded, err := EncodeLifecycleDiagnosticLog(item)
	if err != nil {
		return err
	}
	payload, err := DecodeCanonicalRuntimeLogPayload(encoded)
	if err != nil {
		return err
	}
	inserted, err := l.persistence.PersistLifecycleDiagnostic(ctx, item, RuntimeLogPersistenceRecord{
		Payload: encoded, ExecutionMode: executionmode.Mode(l.posture.RootMode()),
	})
	if err != nil {
		return err
	}
	if inserted {
		if recorder, ok := runtimebus.EmittedEventsRecorderFromContext(ctx); ok && recorder != nil {
			recorder.AppendRuntimeLog(runtimeLogRecorderEntry(payload))
		}
	}
	return nil
}

func (l *RuntimeLogger) Log(ctx context.Context, e RuntimeLogEntry) error {
	if l == nil {
		return nil
	}
	level := diaglog.NormalizeLevel(e.Level.String())
	component := strings.TrimSpace(e.Component)
	if component == "" {
		component = "runtime"
	}
	action := strings.TrimSpace(e.Action)
	if action == "" {
		action = "unknown"
	}
	e.NormalizeEntityID()
	if e.Failure != nil {
		if err := runtimefailures.ValidateEnvelope(*e.Failure); err != nil {
			return err
		}
	}
	if l.persistence == nil {
		return nil
	}
	detail := marshalJSONOrEmpty(e.Detail)
	payload, err := logRuntimeEventSpec(ctx, l.persistence, l.payloadAdmitter, l.posture, true, level.String(), component, action, e, detail)
	if err != nil {
		return err
	}
	if recorder, ok := runtimebus.EmittedEventsRecorderFromContext(ctx); ok && recorder != nil {
		recorder.AppendRuntimeLog(runtimeLogRecorderEntry(payload))
	}
	return nil
}

func (l *RuntimeLogger) Warn(ctx context.Context, component, action string, detail any, err error) error {
	if l == nil {
		return nil
	}
	var failure *runtimefailures.Envelope
	if err != nil {
		normalized := runtimefailures.Normalize(err, strings.TrimSpace(component), strings.TrimSpace(action))
		failure = &normalized
	}
	return l.Log(ctx, RuntimeLogEntry{
		Level:     diaglog.LevelWarn,
		Message:   runtimeLogHelperMessage(diaglog.LevelWarn, component, action),
		Component: strings.TrimSpace(component),
		Action:    strings.TrimSpace(action),
		Detail:    detail,
		Failure:   failure,
	})
}

func (l *RuntimeLogger) Error(ctx context.Context, component, action string, detail any, err error) error {
	if l == nil {
		return nil
	}
	var failure *runtimefailures.Envelope
	if err != nil {
		normalized := runtimefailures.Normalize(err, strings.TrimSpace(component), strings.TrimSpace(action))
		failure = &normalized
	}
	return l.Log(ctx, RuntimeLogEntry{
		Level:     diaglog.LevelError,
		Message:   runtimeLogHelperMessage(diaglog.LevelError, component, action),
		Component: strings.TrimSpace(component),
		Action:    strings.TrimSpace(action),
		Detail:    detail,
		Failure:   failure,
	})
}

func runtimeLogHelperMessage(level diaglog.Level, component, action string) string {
	component = strings.TrimSpace(component)
	action = strings.TrimSpace(action)
	switch diaglog.NormalizeLevel(level.String()) {
	case diaglog.LevelError:
		if component != "" {
			return "Runtime error recorded by " + component
		}
		return "Runtime error recorded"
	default:
		if component != "" {
			return "Runtime warning recorded by " + component
		}
		return "Runtime warning recorded"
	}
}

func handleRuntimeLogPersistenceError(component, action string, err error) {
	if err == nil {
		return
	}
	diaglog.ProcessLog("error", "diagnostics", "runtime log persistence failed",
		"component", strings.TrimSpace(component),
		"action", strings.TrimSpace(action),
		"error", err.Error(),
	)
}

func marshalJSONOrEmpty(v any) []byte {
	if v == nil {
		return []byte("{}")
	}
	b, err := json.Marshal(v)
	if err != nil || len(b) == 0 {
		return []byte("{}")
	}
	return b
}

func sanitizeStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(in))
	for rawKey, rawValue := range in {
		key := strings.TrimSpace(rawKey)
		if key == "" {
			continue
		}
		value := strings.TrimSpace(rawValue)
		if value == "" {
			continue
		}
		out[key] = value
	}
	return out
}

func logRuntimeEventSpec(ctx context.Context, persistence RuntimeLogPersistence, payloadAdmitter runtimebus.PayloadAdmitter, posture executionposture.Posture, hasRunID bool, level, component, action string, e RuntimeLogEntry, detail []byte) (CanonicalRuntimeLogPayload, error) {
	if persistence == nil {
		return CanonicalRuntimeLogPayload{}, nil
	}
	detailMap := map[string]any{}
	_ = json.Unmarshal(detail, &detailMap)
	runID := strings.TrimSpace(runtimecorrelation.RunIDFromContext(ctx))
	lineage, hasLineage := runtimecorrelation.RuntimeLineageFromContext(ctx)
	if runID == "" && hasLineage {
		runID = strings.TrimSpace(lineage.RunID)
	}
	lineageRunID := runID
	if !hasRunID {
		lineageRunID = ""
	}
	explicitParentEventID := asString(detailMap["parent_event_id"])
	subjectEventID := strings.TrimSpace(e.EventID)
	if hasLineage {
		lineage = runtimeLogLineageForEntry(lineage, e)
		if explicitParentEventID == "" {
			explicitParentEventID = strings.TrimSpace(lineage.ParentEventID)
		}
		if subjectEventID == "" {
			subjectEventID = strings.TrimSpace(lineage.SubjectEventID)
		}
		runtimeLogAddLineageDetails(detailMap, lineage)
	}
	parentEventID, err := runtimeLogLineageParentEventID(ctx, persistence, lineageRunID, explicitParentEventID, subjectEventID)
	if err != nil {
		return CanonicalRuntimeLogPayload{}, err
	}
	handlerID := strings.TrimSpace(runtimecorrelation.HandlerIDFromContext(ctx))
	if handlerID == "" {
		handlerID = strings.TrimSpace(asString(detailMap["handler_id"]))
	}
	mode := runtimeeffects.ExecutionMode(posture.RootMode())
	if contextualMode, ok := runtimeeffects.ExecutionModeFromContext(ctx); ok {
		mode = contextualMode
	}
	e.Level, e.Component, e.Action, e.Detail = diaglog.NormalizeLevel(level), component, action, detailMap
	record, canonicalPayload, err := EncodeRuntimeLogRecord(RuntimeLogFacts{RunID: runID, ParentEventID: parentEventID, HandlerID: handlerID, ExecutionMode: executionmode.Mode(mode)}, e)
	if err != nil {
		return CanonicalRuntimeLogPayload{}, err
	}
	if payloadAdmitter == nil {
		return CanonicalRuntimeLogPayload{}, fmt.Errorf("runtime log payload admission owner is required")
	}
	admissionEvent, err := events.NewStandaloneDiagnosticDirectEvent(events.StandaloneRuntimeEventInput{Facts: events.EventFacts{
		Type: events.EventTypePlatformRuntimeLog, Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: "runtime"},
		Payload: record.Payload, ExecutionMode: record.ExecutionMode,
	}})
	if err != nil {
		return CanonicalRuntimeLogPayload{}, fmt.Errorf("construct runtime log payload admission event: %w", err)
	}
	record.PayloadAdmission, err = payloadAdmitter(ctx, admissionEvent, "")
	if err != nil {
		return CanonicalRuntimeLogPayload{}, fmt.Errorf("admit runtime log payload: %w", err)
	}
	record.Payload = record.PayloadAdmission.Payload()
	canonicalPayload, err = DecodeCanonicalRuntimeLogPayload(record.Payload)
	if err != nil {
		return CanonicalRuntimeLogPayload{}, err
	}
	if !hasRunID {
		record.RunID = ""
	}
	if err := persistence.PersistRuntimeLog(ctx, record); err != nil {
		return CanonicalRuntimeLogPayload{}, err
	}
	return canonicalPayload, nil
}

func runtimeLogRecorderEntry(payload CanonicalRuntimeLogPayload) diaglog.RunEntry {
	return diaglog.RunEntry{
		Level:       diaglog.NormalizeLevel(payload.LogLevel),
		Message:     payload.Message,
		Component:   payload.Component,
		Action:      payload.Action,
		EventID:     payload.EventID,
		EventType:   payload.EventType,
		AgentID:     payload.AgentID,
		EntityID:    payload.EntityID,
		SessionID:   payload.SessionID,
		Correlation: payload.Correlation,
		Detail:      payload.Detail,
		Failure:     runtimefailures.CloneEnvelope(payload.Failure),
		StackTrace:  payload.StackTrace,
		DurationUS:  payload.DurationUS,
	}
}

func runtimeLogLineageForEntry(lineage runtimecorrelation.RuntimeLineage, e RuntimeLogEntry) runtimecorrelation.RuntimeLineage {
	lineage = lineage.Normalized()
	if v := strings.TrimSpace(e.EventID); v != "" {
		lineage.SubjectEventID = v
		if strings.TrimSpace(lineage.ParentEventID) == "" {
			lineage.ParentEventID = v
		}
	}
	if v := strings.TrimSpace(e.EventType); v != "" {
		lineage.SubjectEventType = v
	}
	if lineage.RowCategory == "" {
		lineage.RowCategory = runtimecorrelation.RuntimeLineageRowCategoryDiagnostic
	}
	return lineage.Normalized()
}

func runtimeLogAddLineageDetails(detailMap map[string]any, lineage runtimecorrelation.RuntimeLineage) {
	if detailMap == nil {
		return
	}
	lineage = lineage.Normalized()
	if v := strings.TrimSpace(lineage.Owner); v != "" {
		detailMap["runtime_lineage_owner"] = v
	}
	if v := strings.TrimSpace(lineage.RunID); v != "" {
		detailMap["runtime_lineage_run_id"] = v
	}
	if v := strings.TrimSpace(lineage.SubjectEventID); v != "" {
		detailMap["runtime_lineage_subject_event_id"] = v
	}
	if v := strings.TrimSpace(lineage.SubjectEventType); v != "" {
		detailMap["runtime_lineage_subject_event_type"] = v
	}
	if v := strings.TrimSpace(lineage.ParentEventID); v != "" {
		detailMap["runtime_lineage_parent_event_id"] = v
	}
	if v := strings.TrimSpace(string(lineage.RowCategory)); v != "" {
		detailMap["runtime_lineage_row_category"] = v
	}
	if v := strings.TrimSpace(lineage.SelectedForkOwner); v != "" {
		detailMap["runtime_lineage_selected_fork_owner"] = v
	}
	if v := strings.TrimSpace(string(lineage.Classification)); v != "" {
		detailMap["runtime_lineage_classification"] = v
	}
	if lineage.SelectedForkContext {
		detailMap["runtime_lineage_selected_fork_context"] = true
	}
}

func runtimeLogLineageParentEventID(ctx context.Context, persistence RuntimeLogPersistence, runID, explicitParentEventID, subjectEventID string) (string, error) {
	if persistence == nil {
		return "", nil
	}
	return persistence.RuntimeLogLineageParentEventID(ctx, runID, explicitParentEventID, subjectEventID)
}

func runtimeLogPayload(level, component, action string, e RuntimeLogEntry, detailMap map[string]any, runID, parentEventID, handlerID string) map[string]any {
	details := map[string]any{}
	for k, v := range detailMap {
		key := strings.TrimSpace(k)
		if key == "" {
			continue
		}
		if key == "run_id" {
			continue
		}
		if v == nil {
			continue
		}
		details[key] = omitRuntimeLogObjectNulls(v)
	}
	if component = strings.TrimSpace(component); component != "" {
		details["component"] = component
	}
	if action = strings.TrimSpace(action); action != "" {
		details["action"] = action
	}
	if v := strings.TrimSpace(e.EventID); v != "" {
		details["event_id"] = v
	}
	if v := strings.TrimSpace(e.EventType); v != "" {
		details["event_name"] = v
		details["event_type"] = v
	}
	if v := strings.TrimSpace(e.AgentID); v != "" {
		details["agent_id"] = v
	}
	if v := strings.TrimSpace(e.EffectiveEntityID()); v != "" {
		details["entity_id"] = v
	}
	if v := strings.TrimSpace(e.SessionID); v != "" {
		details["session_id"] = v
	}
	if v := strings.TrimSpace(runID); v != "" {
		details["run_id"] = v
	}
	if v := strings.TrimSpace(parentEventID); v != "" {
		details["parent_event_id"] = v
	}
	if v := strings.TrimSpace(handlerID); v != "" {
		details["handler_id"] = v
	}
	if corr := sanitizeStringMap(e.Correlation); len(corr) > 0 {
		details["correlation"] = corr
	}
	if e.Failure != nil {
		details["failure"] = *e.Failure
	}
	if e.DurationUS > 0 {
		details["duration_us"] = e.DurationUS
	}
	payload := map[string]any{
		"log_level": strings.TrimSpace(level),
		"message":   strings.TrimSpace(e.Message),
		"details":   details,
	}
	if v := strings.TrimSpace(e.StackTrace); v != "" {
		payload["stack_trace"] = v
	}
	return payload
}

func omitRuntimeLogObjectNulls(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			if item == nil {
				continue
			}
			out[key] = omitRuntimeLogObjectNulls(item)
		}
		return out
	case []any:
		out := make([]any, len(typed))
		for index, item := range typed {
			out[index] = omitRuntimeLogObjectNulls(item)
		}
		return out
	default:
		return value
	}
}
