package runtime

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
)

// RuntimeLogEntry is a structured runtime operation record (spec v2.0.14).
type RuntimeLogEntry struct {
	Level     string
	Component string
	Action    string

	EventID    string
	EventType  string
	AgentID    string
	EntityID   string
	CampaignID string
	ScanID     string
	SessionID  string

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
	EntityID       string
	Name           string
	Stage          string
	UsersTotal     int
	MRRCents       int
	SpendCents30d  int
	LastMetricDate time.Time
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
	entityID := e.EffectiveEntityID()

	detail := marshalJSONOrEmpty(e.Detail)
	_, err := l.db.ExecContext(withoutSQLTxContext(ctx), `
		INSERT INTO runtime_log (
			level, component, action,
			event_id, event_type, agent_id, vertical_id, campaign_id, scan_id, session_id,
			detail, error, duration_us
		)
		VALUES (
			$1, $2, $3,
			$4::uuid, NULLIF($5,''), NULLIF($6,''), $7::uuid, $8::uuid, $9::uuid, $10::uuid,
			$11::jsonb, NULLIF($12,''), NULLIF($13,0)
		)
	`,
		level,
		component,
		action,
		nullableUUID(e.EventID),
		strings.TrimSpace(e.EventType),
		strings.TrimSpace(e.AgentID),
		nullableUUID(entityID),
		nullableUUID(e.CampaignID),
		nullableUUID(e.ScanID),
		nullableUUID(e.SessionID),
		string(detail),
		strings.TrimSpace(e.Error),
		e.DurationUS,
	)
	if err != nil && !isMissingDiagnosticsTable(err) {
		// Best-effort logging only.
		return
	}
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
		pipelineType = "validation"
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
	if err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM events WHERE id = $1::uuid)`, eventID).Scan(&eventExists); err != nil {
		if isMissingDiagnosticsTable(err) {
			return nil
		}
		return err
	}
	if !eventExists {
		// Best effort only; skip when events persistence is disabled in tests.
		return nil
	}

	_, err := db.ExecContext(ctx, `
		INSERT INTO pipeline_transitions (
			event_id, event_type, handler, pipeline_type, pipeline_id, action,
			state_before, state_after, events_emitted, drop_reason, error, duration_us
		)
		VALUES (
			$1::uuid, $2, $3, $4, $5::uuid, $6,
			$7::jsonb, $8::jsonb, $9, NULLIF($10,''), NULLIF($11,''), NULLIF($12,0)
		)
	`,
		eventID,
		strings.TrimSpace(in.EventType),
		handler,
		pipelineType,
		pipelineID,
		action,
		maybeJSONString(before),
		maybeJSONString(after),
		pq.Array(eventsEmitted),
		strings.TrimSpace(in.DropReason),
		strings.TrimSpace(in.Error),
		durationUS,
	)
	if err != nil && isMissingDiagnosticsTable(err) {
		return nil
	}
	return err
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

func isMissingDiagnosticsTable(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "does not exist") &&
		(strings.Contains(msg, "runtime_log") || strings.Contains(msg, "pipeline_transitions"))
}
