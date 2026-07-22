package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimedeadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
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
			Tool: rec.Tool, EffectClass: rec.EffectClass, Attempt: intPointer(retry), EventType: eventType, ExecutionMode: string(rec.ExecutionMode),
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
		if err != nil || !result.Inserted {
			return err
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

func pipelineFailureJSON(failure *runtimefailures.Envelope) (string, error) {
	if failure == nil {
		return "", nil
	}
	encoded, err := json.Marshal(failure)
	if err != nil {
		return "", fmt.Errorf("marshal pipeline failure: %w", err)
	}
	return string(encoded), nil
}
