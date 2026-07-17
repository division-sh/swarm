package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runforkrevision "github.com/division-sh/swarm/internal/runtime/runforkrevision"
	"github.com/google/uuid"
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
	Failure       *runtimefailures.Envelope
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
	return recordPipelineTransitionReceipt(ctx, db, eventID, handler, pipelineType, pipelineID, action, before, after, eventsEmitted, durationUS, in.DropReason, in.Failure)
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
			processWarn("diagnostics", "failed to persist deferred pipeline transition event_id=%s event_type=%s pipeline_id=%s: %v", strings.TrimSpace(item.input.EventID), strings.TrimSpace(item.input.EventType), strings.TrimSpace(item.input.PipelineID), err)
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

func recordPipelineTransitionReceipt(ctx context.Context, db *sql.DB, eventID, handler, pipelineType, pipelineID, action string, before, after []byte, eventsEmitted []string, durationMS int, dropReason string, failure *runtimefailures.Envelope) error {
	handlerID := strings.TrimSpace(runtimecorrelation.HandlerIDFromContext(ctx))
	if handlerID == "" {
		handlerID = strings.TrimSpace(handler)
	}
	reasonCode := "pipeline_transition_applied"
	if failure != nil {
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
		"events_emitted": eventsEmitted,
		"drop_reason":    strings.TrimSpace(dropReason),
	})
	if err != nil {
		return err
	}
	outcome := "success"
	if failure != nil {
		outcome = "dead_letter"
	} else if strings.TrimSpace(dropReason) != "" {
		outcome = "discard"
	}
	var failureJSON any
	if failure != nil {
		raw, marshalErr := runtimefailures.MarshalEnvelope(*failure)
		if marshalErr != nil {
			return fmt.Errorf("marshal pipeline transition failure: %w", marshalErr)
		}
		failureJSON = string(raw)
	}
	tx, borrowed := PipelineSQLTxFromContext(ctx)
	if !borrowed || tx == nil {
		tx, err = db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin pipeline transition receipt: %w", err)
		}
		defer func() { _ = tx.Rollback() }()
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO event_receipts (
			event_id, subscriber_type, subscriber_id, entity_id, flow_instance,
			outcome, reason_code, state_before, state_after, side_effects, failure, duration_ms, processed_at
		)
		SELECT
			e.event_id, 'platform', $2, e.entity_id, e.flow_instance,
			$3, NULLIF($4,''), NULLIF($5,''), NULLIF($6,''), $7::jsonb, $8::jsonb, NULLIF($9,0), now()
		FROM events e
		WHERE e.event_id = $1::uuid
		ON CONFLICT (event_id, subscriber_type, subscriber_id) DO UPDATE SET
			outcome = EXCLUDED.outcome,
			reason_code = EXCLUDED.reason_code,
			state_before = EXCLUDED.state_before,
			state_after = EXCLUDED.state_after,
			side_effects = EXCLUDED.side_effects,
			failure = EXCLUDED.failure,
			duration_ms = EXCLUDED.duration_ms,
			processed_at = now()
	`, eventID, "pipeline:"+pipelineID, outcome, reasonCode, string(before), string(after), string(sideEffects), failureJSON, durationMS)
	if err != nil {
		return err
	}
	if borrowed {
		return nil
	}
	if _, err := runforkrevision.CaptureCurrentTransaction(ctx, tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit pipeline transition receipt: %w", err)
	}
	return nil
}
