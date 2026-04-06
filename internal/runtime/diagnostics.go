package runtime

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"time"

	"github.com/google/uuid"
	runtimebus "swarm/internal/runtime/bus"
	runtimecorrelation "swarm/internal/runtime/correlation"
	"swarm/internal/runtime/diaglog"
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
	db           *sql.DB
	capabilities runtimeLogCapabilityResolver
}

type runtimeLogCapabilityResolver interface {
	CanonicalRuntimeLogCapability(context.Context) (bool, bool, error)
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

func NewRuntimeLogger(db *sql.DB, capabilities runtimeLogCapabilityResolver) *RuntimeLogger {
	return &RuntimeLogger{db: db, capabilities: capabilities}
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
	if recorder, ok := runtimebus.EmittedEventsRecorderFromContext(ctx); ok && recorder != nil {
		recorder.AppendRuntimeLog(diaglog.RunEntry{
			Level:       e.Level,
			Message:     e.Message,
			Component:   e.Component,
			Action:      e.Action,
			EventID:     e.EventID,
			EventType:   e.EventType,
			AgentID:     e.AgentID,
			EntityID:    e.EntityID,
			SessionID:   e.SessionID,
			Correlation: e.Correlation,
			Detail:      e.Detail,
			Error:       e.Error,
			StackTrace:  e.StackTrace,
			DurationUS:  e.DurationUS,
		})
	}
	if l.db == nil {
		return nil
	}
	if l.capabilities == nil {
		return nil
	}
	enabled, hasRunID, err := l.capabilities.CanonicalRuntimeLogCapability(withoutSQLTxContext(ctx))
	if err != nil || !enabled {
		return nil
	}

	detail := marshalJSONOrEmpty(e.Detail)
	if err := logRuntimeEventSpec(withoutSQLTxContext(ctx), l.db, hasRunID, level.String(), component, action, e, detail); err != nil {
		return err
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

func logRuntimeEventSpec(ctx context.Context, db *sql.DB, hasRunID bool, level, component, action string, e RuntimeLogEntry, detail []byte) error {
	if db == nil {
		return nil
	}
	detailMap := map[string]any{}
	_ = json.Unmarshal(detail, &detailMap)
	runID := strings.TrimSpace(runtimecorrelation.RunIDFromContext(ctx))
	parentEventID := strings.TrimSpace(asString(detailMap["parent_event_id"]))
	handlerID := strings.TrimSpace(runtimecorrelation.HandlerIDFromContext(ctx))
	if handlerID == "" {
		handlerID = strings.TrimSpace(asString(detailMap["handler_id"]))
	}
	payload := runtimeLogPayload(level, component, action, e, detailMap, runID, parentEventID, handlerID)
	encoded, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if hasRunID {
		if err := ensureRuntimeLogRunRow(ctx, db, runID); err != nil {
			return err
		}
		_, err = db.ExecContext(ctx, `
			INSERT INTO events (
				run_id, event_id, event_name, entity_id, flow_instance, scope, payload,
				chain_depth, produced_by, produced_by_type, source_event_id, created_at
			)
			VALUES (
				NULLIF($1,'')::uuid, gen_random_uuid(), 'platform.runtime_log', NULL, NULL, 'global', $2::jsonb,
				0, 'runtime', 'platform', NULLIF($3,'')::uuid, now()
			)
		`, runID, string(encoded), parentEventID)
		return err
	}
	_, err = db.ExecContext(ctx, `
		INSERT INTO events (
			event_id, event_name, entity_id, flow_instance, scope, payload,
			chain_depth, produced_by, produced_by_type, source_event_id, created_at
		)
		VALUES (
			gen_random_uuid(), 'platform.runtime_log', NULL, NULL, 'global', $1::jsonb,
			0, 'runtime', 'platform', NULLIF($2,'')::uuid, now()
		)
	`, string(encoded), parentEventID)
	if err != nil {
		return err
	}
	return nil
}

func ensureRuntimeLogRunRow(ctx context.Context, db *sql.DB, runID string) error {
	if db == nil {
		return nil
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return nil
	}
	if _, err := uuid.Parse(runID); err != nil {
		return err
	}
	_, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES ($1::uuid, 'running', now())
		ON CONFLICT (run_id) DO NOTHING
	`, runID)
	return err
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
