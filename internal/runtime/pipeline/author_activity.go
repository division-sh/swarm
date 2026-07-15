package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimedeadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
)

func runPostgresAuthorActivityMutation(ctx context.Context, db *sql.DB, label string, fn func(context.Context, *sql.Tx) error) error {
	if fn == nil {
		return nil
	}
	if tx, ok := PipelineSQLTxFromContext(ctx); ok && tx != nil {
		if runtimeauthoractivity.InMutation(ctx, tx) {
			return fn(ctx, tx)
		}
		if !runtimeauthoractivity.FinalizedMutation(ctx, tx) {
			return fmt.Errorf("%s entered from a raw transaction without author activity ownership", label)
		}
		ctx = WithoutPipelineSQLTxContext(ctx)
	}
	if db == nil {
		return fmt.Errorf("%s database is required", label)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("%s begin transaction: %w", label, err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	txctx := withSQLTxContext(ctx, tx)
	storyctx, err := runtimeauthoractivity.Begin(txctx, tx, runtimeauthoractivity.DialectPostgres)
	if err != nil {
		return err
	}
	if err := fn(storyctx, tx); err != nil {
		return err
	}
	if err := CapturePipelineRunForkRevisionChanges(storyctx, tx); err != nil {
		return err
	}
	if err := runtimeauthoractivity.Finalize(storyctx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("%s commit transaction: %w", label, err)
	}
	committed = true
	return nil
}

type systemNodeDeliveryStorySource struct {
	DeliveryID string
	RunID      string
	EventType  string
	EntityID   string
	FlowID     string
	RetryCount int
}

func loadSystemNodeDeliveryStorySource(ctx context.Context, tx *sql.Tx, nodeID, eventID, targetJSON string, postgres bool) (systemNodeDeliveryStorySource, error) {
	if err := runtimeauthoractivity.Require(ctx); err != nil {
		return systemNodeDeliveryStorySource{}, err
	}
	query := `
		SELECT CAST(d.delivery_id AS TEXT), COALESCE(CAST(d.run_id AS TEXT), ''), COALESCE(e.event_name, ''),
		       COALESCE(CAST(e.entity_id AS TEXT), ''), COALESCE(e.flow_instance, ''), COALESCE(d.retry_count, 0)
		FROM event_deliveries d
		JOIN events e ON e.event_id = d.event_id
		WHERE d.event_id = ? AND d.subscriber_type = 'node' AND d.subscriber_id = ?
		  AND COALESCE(d.delivery_target_route, '{}') = ?
	`
	args := []any{eventID, nodeID, targetJSON}
	if postgres {
		query = `
			SELECT d.delivery_id::text, COALESCE(d.run_id::text, ''), COALESCE(e.event_name, ''),
			       COALESCE(e.entity_id::text, ''), COALESCE(e.flow_instance, ''), COALESCE(d.retry_count, 0)
			FROM event_deliveries d
			JOIN events e ON e.event_id = d.event_id
			WHERE d.event_id = $1::uuid AND d.subscriber_type = 'node' AND d.subscriber_id = $2
			  AND COALESCE(d.delivery_target_route, '{}'::jsonb) = $3::jsonb
			FOR UPDATE OF d
		`
	}
	var source systemNodeDeliveryStorySource
	if err := tx.QueryRowContext(ctx, query, args...).Scan(&source.DeliveryID, &source.RunID, &source.EventType, &source.EntityID, &source.FlowID, &source.RetryCount); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return systemNodeDeliveryStorySource{}, fmt.Errorf("%w: node %s event %s", ErrSystemNodeDeliveryAuthorityMissing, nodeID, eventID)
		}
		return systemNodeDeliveryStorySource{}, err
	}
	if source.RunID != "" {
		dialect := storerunlifecycle.DialectSQLite
		if postgres {
			dialect = storerunlifecycle.DialectPostgres
		}
		if err := storerunlifecycle.RequireActive(ctx, tx, source.RunID, dialect); err != nil {
			return systemNodeDeliveryStorySource{}, fmt.Errorf("admit system node delivery mutation: %w", err)
		}
	}
	return source, nil
}

func recordSystemNodeDeliveryStory(ctx context.Context, source systemNodeDeliveryStorySource, nodeID, transition, reasonCode string, retryCount int, failure *runtimefailures.Envelope, occurredAt time.Time) error {
	dedupKey := fmt.Sprintf("delivery:%s:%s:%d", source.DeliveryID, transition, retryCount)
	persistedAt, found, err := runtimeauthoractivity.PersistedOccurredAt(ctx, dedupKey)
	if err != nil {
		return err
	}
	if found {
		occurredAt = persistedAt
	}
	return runtimeauthoractivity.Record(ctx, runtimeauthoractivity.Draft{
		Kind: runtimeauthoractivity.KindDeliveryLifecycle, Transition: transition,
		SourceOwner: "event_deliveries", SourceIdentity: source.DeliveryID,
		DedupKey:   dedupKey,
		OccurredAt: occurredAt, RunID: source.RunID, EntityID: source.EntityID, FlowID: source.FlowID,
		Projection: runtimeauthoractivity.Projection{
			SubjectType: "node", SubjectID: strings.TrimSpace(nodeID), EventType: source.EventType,
			SubscriberType: "node", SubscriberID: strings.TrimSpace(nodeID), RetryCount: intPointer(retryCount),
			ReasonCode: strings.TrimSpace(reasonCode),
		},
		Failure: failure,
	})
}

func recordActivityAttemptStory(ctx context.Context, rec ActivityAttemptRecord, transition string) error {
	retry := rec.Attempt
	eventType := rec.ResultEventType
	failure := rec.Failure
	if transition == ActivityAttemptStatusStarted {
		eventType = ""
		failure = nil
	}
	return runtimeauthoractivity.Record(ctx, runtimeauthoractivity.Draft{
		Kind: runtimeauthoractivity.KindActivityLifecycle, Transition: transition,
		SourceOwner: "activity_attempts", SourceIdentity: rec.RequestEventID,
		DedupKey:   "activity:" + rec.RequestEventID + ":" + transition,
		OccurredAt: activityOccurrenceTime(rec, transition), RunID: rec.RunID, EntityID: rec.EntityID, FlowID: rec.FlowInstance,
		Projection: runtimeauthoractivity.Projection{
			SubjectType: "activity", SubjectID: rec.ActivityID, NodeID: rec.NodeID, Activity: rec.ActivityID,
			Tool: rec.Tool, EffectClass: rec.EffectClass, Attempt: intPointer(retry), EventType: eventType,
		},
		Failure: failure,
	})
}

func activityOccurrenceTime(rec ActivityAttemptRecord, transition string) time.Time {
	if transition == ActivityAttemptStatusStarted && !rec.StartedAt.IsZero() {
		return rec.StartedAt.UTC()
	}
	if transition != "started" && rec.CompletedAt != nil && !rec.CompletedAt.IsZero() {
		return rec.CompletedAt.UTC()
	}
	if !rec.UpdatedAt.IsZero() {
		return rec.UpdatedAt.UTC()
	}
	if !rec.StartedAt.IsZero() {
		return rec.StartedAt.UTC()
	}
	return time.Now().UTC()
}

func intPointer(value int) *int { return &value }

func recordPipelineDeadLetter(ctx context.Context, db *sql.DB, rec runtimedeadletters.Record) error {
	return runPostgresAuthorActivityMutation(ctx, db, "pipeline dead letter", func(txctx context.Context, tx *sql.Tx) error {
		if err := runtimeauthoractivity.Require(txctx); err != nil {
			return err
		}
		var runID, entityID, flowID, eventType string
		if err := tx.QueryRowContext(txctx, `SELECT COALESCE(run_id::text, ''), COALESCE(entity_id::text, ''), COALESCE(flow_instance, ''), event_name FROM events WHERE event_id = $1::uuid`, strings.TrimSpace(rec.OriginalEventID)).Scan(&runID, &entityID, &flowID, &eventType); err != nil {
			return fmt.Errorf("load pipeline dead letter source event: %w", err)
		}
		result, err := runtimedeadletters.InsertTxWithResult(txctx, tx, rec)
		if err != nil {
			return err
		}
		if !result.Inserted {
			return nil
		}
		identity := result.DeadLetterID
		retry := rec.RetryCount
		return runtimeauthoractivity.Record(txctx, runtimeauthoractivity.Draft{
			Kind: runtimeauthoractivity.KindDeadLetterRecorded, Transition: "recorded",
			SourceOwner: "dead_letters", SourceIdentity: identity, DedupKey: "dead-letter:" + identity,
			OccurredAt: pipelineDeadLetterOccurredAt(rec.Timestamp), RunID: runID, EntityID: entityID, FlowID: flowID,
			Projection: runtimeauthoractivity.Projection{
				SubjectType: "event", SubjectID: strings.TrimSpace(rec.OriginalEventID), EventType: eventType,
				RetryCount: &retry, ReasonCode: rec.Failure.Detail.Code, NodeID: strings.TrimSpace(rec.HandlerNode),
			},
			Failure: &rec.Failure,
		})
	})
}

func pipelineDeadLetterOccurredAt(raw string) time.Time {
	if parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(raw)); err == nil {
		return parsed.UTC()
	}
	return time.Now().UTC()
}
