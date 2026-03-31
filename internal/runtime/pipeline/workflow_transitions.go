package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	runtimecorrelation "swarm/internal/runtime/correlation"
)

type DeferredPipelineTransition struct {
	db    *sql.DB
	input PipelineTransitionInput
}

type pipelineTransitionCollectorKey struct{}

type PipelineTransitionInput struct {
	EventID       string
	EventType     string
	Handler       string
	PipelineType  string
	PipelineID    string
	Action        string
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
		return nil
	}
	return recordPipelineTransitionReceipt(ctx, db, eventID, handler, pipelineType, pipelineID, action, before, after, eventsEmitted, durationUS, in.DropReason, in.Error)
}

func WithPipelineTransitionCollector(ctx context.Context, collector *[]DeferredPipelineTransition) context.Context {
	if collector == nil {
		return ctx
	}
	return context.WithValue(ctx, pipelineTransitionCollectorKey{}, collector)
}

func AppendDeferredPipelineTransition(ctx context.Context, item DeferredPipelineTransition) bool {
	if item.db == nil {
		return false
	}
	collector, ok := ctx.Value(pipelineTransitionCollectorKey{}).(*[]DeferredPipelineTransition)
	if !ok || collector == nil {
		return false
	}
	*collector = append(*collector, item)
	return true
}

func FlushDeferredPipelineTransitions(ctx context.Context, deferred []DeferredPipelineTransition) {
	for _, item := range deferred {
		if err := RecordPipelineTransition(ctx, item.db, item.input); err != nil {
			runtimeWarn("diagnostics", "failed to persist deferred pipeline transition event_id=%s event_type=%s pipeline_id=%s: %v", strings.TrimSpace(item.input.EventID), strings.TrimSpace(item.input.EventType), strings.TrimSpace(item.input.PipelineID), err)
		}
	}
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
	return strings.Contains(msg, "subscriber_id") ||
		strings.Contains(msg, "reason_code") ||
		strings.Contains(msg, "side_effects") ||
		strings.Contains(msg, "duration_ms")
}

func recordPipelineTransitionReceipt(ctx context.Context, db *sql.DB, eventID, handler, pipelineType, pipelineID, action string, before, after []byte, eventsEmitted []string, durationMS int, dropReason, errText string) error {
	traceID := strings.TrimSpace(runtimecorrelation.TraceIDFromContext(ctx))
	handlerID := strings.TrimSpace(runtimecorrelation.HandlerIDFromContext(ctx))
	if handlerID == "" {
		handlerID = strings.TrimSpace(handler)
	}
	reasonCode := "pipeline_transition_applied"
	if strings.TrimSpace(errText) != "" {
		reasonCode = "pipeline_transition_error"
	} else if strings.TrimSpace(dropReason) != "" {
		reasonCode = "pipeline_transition_discarded"
	}
	sideEffects, err := json.Marshal(map[string]any{
		"pipeline_type":  pipelineType,
		"pipeline_id":    pipelineID,
		"action":         action,
		"handler_id":     handlerID,
		"reason_code":    reasonCode,
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
			outcome, reason_code, state_before, state_after, side_effects, duration_ms, processed_at
		)
		SELECT
			e.event_id, 'platform', $2, e.entity_id, e.flow_instance,
			$3, NULLIF($4,''), NULLIF($5,''), NULLIF($6,''), $7::jsonb, NULLIF($8,0), now()
		FROM events e
		WHERE e.event_id = $1::uuid
		ON CONFLICT (event_id, subscriber_id) DO UPDATE SET
			outcome = EXCLUDED.outcome,
			reason_code = EXCLUDED.reason_code,
			state_before = EXCLUDED.state_before,
			state_after = EXCLUDED.state_after,
			side_effects = EXCLUDED.side_effects,
			duration_ms = EXCLUDED.duration_ms,
			processed_at = now()
	`, eventID, "pipeline:"+pipelineID, outcome, reasonCode, string(before), string(after), string(sideEffects), durationMS)
	if err == nil || !isMissingDiagnosticsTable(err) {
		return err
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
