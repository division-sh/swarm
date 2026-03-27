package runtime

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"

	runtimecorrelation "swarm/internal/runtime/correlation"
	"github.com/google/uuid"
)

// RuntimeLogEntry is a structured runtime operation record (spec v2.0.14).
type RuntimeLogEntry struct {
	Level     string
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
	db *sql.DB
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

type deferredPipelineTransition struct {
	db    *sql.DB
	input PipelineTransitionInput
}

type pipelineTransitionCollectorKey struct{}

func NewRuntimeLogger(db *sql.DB) *RuntimeLogger {
	return &RuntimeLogger{db: db}
}

func (l *RuntimeLogger) Log(ctx context.Context, e RuntimeLogEntry) {
	if l == nil || l.db == nil {
		return
	}
	level := strings.ToLower(strings.TrimSpace(e.Level))
	if level == "" {
		level = "info"
	}
	component := strings.TrimSpace(e.Component)
	if component == "" {
		component = "runtime"
	}
	action := strings.TrimSpace(e.Action)
	if action == "" {
		action = "unknown"
	}
	e.NormalizeEntityID()

	detail := marshalJSONOrEmpty(e.Detail)
	_ = logRuntimeEventSpec(withoutSQLTxContext(ctx), l.db, level, component, action, e, detail)
}

type PipelineTransitionInput struct {
	EventID      string
	EventType    string
	Handler      string
	PipelineType string
	PipelineID   string
	Action       string

	StateBefore   any
	StateAfter    any
	EventsEmitted []string
	DropReason    string
	Error         string
	Duration      time.Duration
}

func RecordPipelineTransition(ctx context.Context, db *sql.DB, in PipelineTransitionInput) error {
	if db == nil {
		return nil
	}
	ctx = withoutSQLTxContext(ctx)
	eventID := strings.TrimSpace(in.EventID)
	pipelineID := strings.TrimSpace(in.PipelineID)
	if _, err := uuid.Parse(eventID); err != nil {
		return errors.New("pipeline transition requires valid event_id")
	}
	if _, err := uuid.Parse(pipelineID); err != nil {
		return errors.New("pipeline transition requires valid pipeline_id")
	}
	handler := strings.TrimSpace(in.Handler)
	if handler == "" {
		handler = "unknown"
	}
	pipelineType := strings.TrimSpace(in.PipelineType)
	if pipelineType == "" {
		pipelineType = "workflow"
	}
	action := strings.TrimSpace(in.Action)
	if action == "" {
		action = "consumed"
	}
	before := marshalJSONOrNil(in.StateBefore)
	after := marshalJSONOrNil(in.StateAfter)
	eventsEmitted := sanitizeStringSlice(in.EventsEmitted)

	durationUS := int(in.Duration / time.Microsecond)
	if durationUS <= 0 {
		durationUS = 0
	}

	var eventExists bool
	if err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM events WHERE event_id = $1::uuid)`, eventID).Scan(&eventExists); err != nil {
		return err
	}
	if !eventExists {
		// Best effort only; skip when events persistence is disabled in tests.
		return nil
	}

	return recordPipelineTransitionSpec(ctx, db, eventID, handler, pipelineType, pipelineID, action, before, after, eventsEmitted, durationUS, in.DropReason, in.Error)
}

func withPipelineTransitionCollector(ctx context.Context, collector *[]deferredPipelineTransition) context.Context {
	if collector == nil {
		return ctx
	}
	return context.WithValue(ctx, pipelineTransitionCollectorKey{}, collector)
}

func appendDeferredPipelineTransition(ctx context.Context, item deferredPipelineTransition) bool {
	if item.db == nil {
		return false
	}
	collector, ok := ctx.Value(pipelineTransitionCollectorKey{}).(*[]deferredPipelineTransition)
	if !ok || collector == nil {
		return false
	}
	*collector = append(*collector, item)
	return true
}

func flushDeferredPipelineTransitions(ctx context.Context, deferred []deferredPipelineTransition) {
	for _, item := range deferred {
		if err := RecordPipelineTransition(ctx, item.db, item.input); err != nil {
			runtimeWarn(
				"diagnostics",
				"failed to persist deferred pipeline transition event_id=%s event_type=%s pipeline_id=%s: %v",
				strings.TrimSpace(item.input.EventID),
				strings.TrimSpace(item.input.EventType),
				strings.TrimSpace(item.input.PipelineID),
				err,
			)
		}
	}
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

func marshalJSONOrNil(v any) []byte {
	if v == nil {
		return nil
	}
	b, err := json.Marshal(v)
	if err != nil || len(b) == 0 {
		return nil
	}
	return b
}

func maybeJSONString(v []byte) any {
	if len(v) == 0 {
		return nil
	}
	return string(v)
}

func nullableUUID(raw string) any {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	if _, err := uuid.Parse(raw); err != nil {
		return nil
	}
	return raw
}

func sanitizeStringSlice(in []string) []string {
	out := make([]string, 0, len(in))
	seen := make(map[string]struct{}, len(in))
	for _, raw := range in {
		v := strings.TrimSpace(raw)
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
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

func isMissingDiagnosticsTable(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "event_name") ||
		strings.Contains(msg, "subscriber_id") ||
		strings.Contains(msg, "side_effects") ||
		strings.Contains(msg, "duration_ms")
}

func logRuntimeEventSpec(ctx context.Context, db *sql.DB, level, component, action string, e RuntimeLogEntry, detail []byte) error {
	if db == nil {
		return nil
	}
	detailMap := map[string]any{}
	_ = json.Unmarshal(detail, &detailMap)
	traceID := strings.TrimSpace(runtimecorrelation.TraceIDFromContext(ctx))
	if traceID == "" {
		traceID = strings.TrimSpace(asString(detailMap["trace_id"]))
	}
	parentEventID := strings.TrimSpace(asString(detailMap["parent_event_id"]))
	handlerID := strings.TrimSpace(runtimecorrelation.HandlerIDFromContext(ctx))
	if handlerID == "" {
		handlerID = strings.TrimSpace(asString(detailMap["handler_id"]))
	}
	payload := map[string]any{
		"level":           level,
		"component":       component,
		"action":          action,
		"event_id":        strings.TrimSpace(e.EventID),
		"event_type":      strings.TrimSpace(e.EventType),
		"agent_id":        strings.TrimSpace(e.AgentID),
		"entity_id":       strings.TrimSpace(e.EffectiveEntityID()),
		"session_id":      strings.TrimSpace(e.SessionID),
		"trace_id":        traceID,
		"parent_event_id": parentEventID,
		"handler_id":      handlerID,
		"correlation":     sanitizeStringMap(e.Correlation),
		"detail":          json.RawMessage(detail),
		"error":           strings.TrimSpace(e.Error),
		"duration_us":     e.DurationUS,
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, `
		INSERT INTO events (
			event_id, event_name, entity_id, flow_instance, scope, payload,
			chain_depth, produced_by, produced_by_type, created_at
		)
		VALUES (
			gen_random_uuid(), 'platform.runtime_log', NULL, NULL, 'global', $1::jsonb,
			0, 'runtime', 'platform', now()
		)
	`, string(encoded))
	if err != nil {
		return err
	}
	return nil
}

func recordPipelineTransitionSpec(ctx context.Context, db *sql.DB, eventID, handler, pipelineType, pipelineID, action string, before, after []byte, eventsEmitted []string, durationMS int, dropReason, errText string) error {
	traceID := strings.TrimSpace(runtimecorrelation.TraceIDFromContext(ctx))
	handlerID := strings.TrimSpace(runtimecorrelation.HandlerIDFromContext(ctx))
	if handlerID == "" {
		handlerID = strings.TrimSpace(handler)
	}
	sideEffects, err := json.Marshal(map[string]any{
		"pipeline_type":  pipelineType,
		"pipeline_id":    pipelineID,
		"action":         action,
		"handler_id":     handlerID,
		"trace_id":       traceID,
		"events_emitted": eventsEmitted,
		"drop_reason":    strings.TrimSpace(dropReason),
		"error":          strings.TrimSpace(errText),
	})
	if err != nil {
		return err
	}
	outcome := "success"
	if strings.TrimSpace(errText) != "" {
		outcome = "dead_letter"
	} else if strings.TrimSpace(dropReason) != "" {
		outcome = "discard"
	}
	_, err = db.ExecContext(ctx, `
		INSERT INTO event_receipts (
			event_id, subscriber_type, subscriber_id, entity_id, flow_instance,
			outcome, state_before, state_after, side_effects, duration_ms, processed_at
		)
		SELECT
			e.event_id, 'platform', $2, e.entity_id, e.flow_instance,
			$3, NULLIF($4,''), NULLIF($5,''), $6::jsonb, NULLIF($7,0), now()
		FROM events e
		WHERE e.event_id = $1::uuid
		ON CONFLICT (event_id, subscriber_id) DO UPDATE SET
			outcome = EXCLUDED.outcome,
			state_before = EXCLUDED.state_before,
			state_after = EXCLUDED.state_after,
			side_effects = EXCLUDED.side_effects,
			duration_ms = EXCLUDED.duration_ms,
			processed_at = now()
	`, eventID, "pipeline:"+pipelineID, outcome, string(before), string(after), string(sideEffects), durationMS)
	return err
}
