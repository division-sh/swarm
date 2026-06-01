package runtime

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
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
	Error      string
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
	persistence RuntimeLogPersistence
}

// RuntimeLogPersistence owns backend-specific platform.runtime_log persistence
// and lineage lookup while RuntimeLogger owns canonical payload construction.
type RuntimeLogPersistence interface {
	CanonicalRuntimeLogCapability(context.Context) (bool, bool, error)
	RuntimeLogLineageParentEventID(ctx context.Context, runID, explicitParentEventID, subjectEventID string) (string, error)
	PersistRuntimeLog(ctx context.Context, record RuntimeLogPersistenceRecord) error
}

type RuntimeLogPersistenceRecord struct {
	RunID         string
	Payload       []byte
	ParentEventID string
}

type InstanceDigestRow struct {
	EntityID  string
	Name      string
	Stage     string
	UpdatedAt time.Time
}

type DigestPersistence interface {
	CountActiveInstances(ctx context.Context) (int, error)
	ListInstanceDigestRows(ctx context.Context, limit int) ([]InstanceDigestRow, error)
}

func NewRuntimeLogger(persistence RuntimeLogPersistence) *RuntimeLogger {
	return &RuntimeLogger{persistence: persistence}
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
	if l.persistence == nil {
		return nil
	}
	enabled, hasRunID, err := l.persistence.CanonicalRuntimeLogCapability(withoutSQLTxContext(ctx))
	if err != nil || !enabled {
		return nil
	}

	detail := marshalJSONOrEmpty(e.Detail)
	payload, err := logRuntimeEventSpec(withoutSQLTxContext(ctx), l.persistence, hasRunID, level.String(), component, action, e, detail)
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
	errText := ""
	if err != nil {
		errText = strings.TrimSpace(err.Error())
	}
	return l.Log(ctx, RuntimeLogEntry{
		Level:     diaglog.LevelWarn,
		Message:   runtimeLogHelperMessage(diaglog.LevelWarn, component, action),
		Component: strings.TrimSpace(component),
		Action:    strings.TrimSpace(action),
		Detail:    detail,
		Error:     errText,
	})
}

func (l *RuntimeLogger) Error(ctx context.Context, component, action string, detail any, err error) error {
	if l == nil {
		return nil
	}
	errText := ""
	if err != nil {
		errText = strings.TrimSpace(err.Error())
	}
	return l.Log(ctx, RuntimeLogEntry{
		Level:     diaglog.LevelError,
		Message:   runtimeLogHelperMessage(diaglog.LevelError, component, action),
		Component: strings.TrimSpace(component),
		Action:    strings.TrimSpace(action),
		Detail:    detail,
		Error:     errText,
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

func logRuntimeEventSpec(ctx context.Context, persistence RuntimeLogPersistence, hasRunID bool, level, component, action string, e RuntimeLogEntry, detail []byte) (CanonicalRuntimeLogPayload, error) {
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
	payload := runtimeLogPayload(level, component, action, e, detailMap, runID, parentEventID, handlerID)
	encoded, err := json.Marshal(payload)
	if err != nil {
		return CanonicalRuntimeLogPayload{}, err
	}
	canonicalPayload, err := DecodeCanonicalRuntimeLogPayload(encoded)
	if err != nil {
		return CanonicalRuntimeLogPayload{}, err
	}
	record := RuntimeLogPersistenceRecord{
		Payload:       encoded,
		ParentEventID: parentEventID,
	}
	if hasRunID {
		record.RunID = runID
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
		Error:       payload.Error,
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
		details[key] = v
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
	if v := strings.TrimSpace(e.Error); v != "" {
		details["error"] = v
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
