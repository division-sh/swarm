package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	authoractivityfixture "github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
)

type pipelineTestActivityJournal struct {
	store *workflowInstanceStore
}

func installPipelineTestActivityJournal(store *workflowInstanceStore) *workflowInstanceStore {
	if store != nil {
		store.activityJournal = pipelineTestActivityJournal{store: store}
	}
	return store
}

func (j pipelineTestActivityJournal) StartActivityAttempt(ctx context.Context, record ActivityAttemptRecord) (out ActivityAttemptRecord, inserted bool, err error) {
	record = NormalizeActivityAttemptRecord(record)
	record.Status = ActivityAttemptStatusStarted
	if err := ValidateActivityAttemptStart(record); err != nil {
		return ActivityAttemptRecord{}, false, err
	}
	err = j.store.runInPipelineTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		if err := j.requireActiveRun(txctx, record.RunID); err != nil {
			return err
		}
		out, inserted, err = j.startTx(txctx, tx, record)
		return err
	})
	return
}

func (j pipelineTestActivityJournal) ClaimActivityAttemptForLoopGeneration(ctx context.Context, record ActivityAttemptRecord) (out ActivityAttemptRecord, inserted bool, err error) {
	record = NormalizeActivityAttemptRecord(record)
	if !record.Generation.Valid() {
		return j.StartActivityAttempt(ctx, record)
	}
	err = j.store.runInPipelineTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		if err := j.requireActiveRun(txctx, record.RunID); err != nil {
			return err
		}
		instance, found, err := j.store.Load(txctx, testRunScopedWorkflowInstanceForRun(record.RunID, record.FlowInstance))
		if err != nil {
			return err
		}
		if !found {
			return runtimefailures.New(runtimefailures.ClassUnexpectedArrival, "activity_loop_instance_missing", "activity-runtime", "claim_activity_attempt", map[string]any{
				"activity_id": record.ActivityID, "entity_id": record.EntityID, "loop_id": record.Generation.LoopID,
			})
		}
		current, err := workflowLoopGenerationCurrent(&instance, record.Generation, record.LoopStage)
		if err != nil {
			return err
		}
		if !current {
			return runtimefailures.New(runtimefailures.ClassStaleArrival, "activity_loop_generation_stale", "activity-runtime", "claim_activity_attempt", map[string]any{
				"activity_id": record.ActivityID, "entity_id": record.EntityID, "loop_id": record.Generation.LoopID,
				"revision_id": record.Generation.RevisionID, "expected_stage": record.LoopStage,
			})
		}
		out, inserted, err = j.startTx(txctx, tx, record)
		return err
	})
	return
}

func (j pipelineTestActivityJournal) startTx(ctx context.Context, tx *sql.Tx, record ActivityAttemptRecord) (ActivityAttemptRecord, bool, error) {
	generation, err := json.Marshal(record.Generation)
	if err != nil {
		return ActivityAttemptRecord{}, false, err
	}
	query := `INSERT INTO activity_attempts (request_event_id, run_id, source_event_id, parent_event_id, entity_id, flow_instance, node_id, handler_event_key, activity_id, tool, effect_class, attempt, status, success_event, failure_event, input_hash, loop_generation, loop_stage, reply_context_id, execution_mode) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULLIF(?, ''), ?) ON CONFLICT (request_event_id) DO NOTHING`
	if !j.store.isSQLite() {
		query = `INSERT INTO activity_attempts (request_event_id, run_id, source_event_id, parent_event_id, entity_id, flow_instance, node_id, handler_event_key, activity_id, tool, effect_class, attempt, status, success_event, failure_event, input_hash, loop_generation, loop_stage, reply_context_id, execution_mode) VALUES ($1::uuid, $2::uuid, NULLIF($3, '')::uuid, NULLIF($4, '')::uuid, NULLIF($5, '')::uuid, NULLIF($6, ''), $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17::jsonb, NULLIF($18, ''), NULLIF($19, ''), $20) ON CONFLICT (request_event_id) DO NOTHING`
	}
	result, err := tx.ExecContext(ctx, query,
		record.RequestEventID, record.RunID, testNullable(record.SourceEventID), testNullable(record.ParentEventID), testNullable(record.EntityID), testNullable(record.FlowInstance),
		record.NodeID, record.HandlerEventKey, record.ActivityID, record.Tool, record.EffectClass, record.Attempt, record.Status,
		record.SuccessEvent, record.FailureEvent, record.InputHash, string(generation), testNullable(record.LoopStage), record.ReplyContextID, record.ExecutionMode,
	)
	if err != nil {
		return ActivityAttemptRecord{}, false, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return ActivityAttemptRecord{}, false, err
	}
	actual, found, err := j.load(ctx, tx, record.RequestEventID)
	if err != nil || !found {
		if err == nil {
			err = fmt.Errorf("activity attempt %s was not readable after start", record.RequestEventID)
		}
		return ActivityAttemptRecord{}, false, err
	}
	if err := ValidateActivityAttemptClaimIdentity(actual, record); err != nil {
		return ActivityAttemptRecord{}, false, err
	}
	if err := authoractivityfixture.Record(ctx, ActivityAttemptStoryDraft(actual, ActivityAttemptStatusStarted)); err != nil {
		return ActivityAttemptRecord{}, false, err
	}
	return actual, rows > 0, nil
}

func (j pipelineTestActivityJournal) CompleteActivityAttempt(ctx context.Context, record ActivityAttemptRecord) (out ActivityAttemptRecord, err error) {
	record = NormalizeActivityAttemptRecord(record)
	if err := ValidateActivityAttemptTerminal(record); err != nil {
		return ActivityAttemptRecord{}, err
	}
	err = j.store.runInPipelineTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		if err := j.requireActiveRun(txctx, record.RunID); err != nil {
			return err
		}
		payload, err := json.Marshal(record.ResultPayload)
		if err != nil {
			return err
		}
		failure, err := pipelineFailureJSON(record.Failure)
		if record.Status == ActivityAttemptStatusSucceeded {
			failure, err = "", nil
		}
		if err != nil {
			return err
		}
		query := `UPDATE activity_attempts SET status = ?, result_event_id = ?, result_event_type = ?, result_payload = ?, failure = NULLIF(?, ''), completed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE request_event_id = ? AND execution_mode = ? AND status = 'started'`
		if !j.store.isSQLite() {
			query = `UPDATE activity_attempts SET status = $1, result_event_id = $2::uuid, result_event_type = $3, result_payload = $4::jsonb, failure = NULLIF($5, '')::jsonb, completed_at = NOW(), updated_at = NOW() WHERE request_event_id = $6::uuid AND execution_mode = $7 AND status = 'started'`
		}
		result, err := tx.ExecContext(txctx, query, record.Status, record.ResultEventID, record.ResultEventType, string(payload), testNullable(failure), record.RequestEventID, record.ExecutionMode)
		if err != nil {
			return err
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return err
		}
		var found bool
		out, found, err = j.load(txctx, tx, record.RequestEventID)
		if err != nil || !found {
			if err == nil {
				err = fmt.Errorf("activity attempt %s was not found for completion", record.RequestEventID)
			}
			return err
		}
		if err := ValidateActivityAttemptTerminalMode(out, record); err != nil {
			return err
		}
		if rows == 0 && out.Status == ActivityAttemptStatusStarted {
			return fmt.Errorf("activity attempt %s remained started after terminal update", record.RequestEventID)
		}
		return authoractivityfixture.Record(txctx, ActivityAttemptStoryDraft(out, out.Status))
	})
	return
}

func (j pipelineTestActivityJournal) MarkActivityAttemptUncertain(ctx context.Context, record ActivityAttemptRecord) (out ActivityAttemptRecord, err error) {
	record = NormalizeActivityAttemptRecord(record)
	record.Status = ActivityAttemptStatusUncertain
	if err := ValidateActivityAttemptTerminal(record); err != nil {
		return ActivityAttemptRecord{}, err
	}
	err = j.store.runInPipelineTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		if err := j.requireActiveRun(txctx, record.RunID); err != nil {
			return err
		}
		payload, err := json.Marshal(record.ResultPayload)
		if err != nil {
			return err
		}
		failure, err := pipelineFailureJSON(record.Failure)
		if err != nil {
			return err
		}
		query := `UPDATE activity_attempts SET status = 'uncertain', result_event_id = ?, result_event_type = ?, result_payload = ?, failure = ?, completed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE request_event_id = ? AND execution_mode = ? AND status = 'started'`
		if !j.store.isSQLite() {
			query = `UPDATE activity_attempts SET status = 'uncertain', result_event_id = $1::uuid, result_event_type = $2, result_payload = $3::jsonb, failure = $4::jsonb, completed_at = NOW(), updated_at = NOW() WHERE request_event_id = $5::uuid AND execution_mode = $6 AND status = 'started'`
		}
		if _, err := tx.ExecContext(txctx, query, record.ResultEventID, record.ResultEventType, string(payload), failure, record.RequestEventID, record.ExecutionMode); err != nil {
			return err
		}
		var found bool
		out, found, err = j.load(txctx, tx, record.RequestEventID)
		if err != nil || !found {
			if err == nil {
				err = fmt.Errorf("activity attempt %s was not found for uncertain transition", record.RequestEventID)
			}
			return err
		}
		if err := ValidateActivityAttemptTerminalMode(out, record); err != nil {
			return err
		}
		return authoractivityfixture.Record(txctx, ActivityAttemptStoryDraft(out, ActivityAttemptStatusUncertain))
	})
	return
}

func (j pipelineTestActivityJournal) LoadActivityAttempt(ctx context.Context, requestEventID string) (ActivityAttemptRecord, bool, error) {
	return j.load(ctx, j.store.testDB(), requestEventID)
}

func (j pipelineTestActivityJournal) load(ctx context.Context, db interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, requestEventID string) (ActivityAttemptRecord, bool, error) {
	query := testActivityAttemptSelect + ` WHERE request_event_id = ?`
	if !j.store.isSQLite() {
		query = testActivityAttemptSelect + ` WHERE request_event_id = $1::uuid`
	}
	record, err := scanPipelineTestActivityAttempt(db.QueryRowContext(ctx, query, requestEventID))
	if err == sql.ErrNoRows {
		return ActivityAttemptRecord{}, false, nil
	}
	return record, err == nil, err
}

func (j pipelineTestActivityJournal) requireActiveRun(ctx context.Context, runID string) error {
	if j.store == nil || j.store.runLifecycle == nil {
		return fmt.Errorf("activity run lifecycle owner is required")
	}
	return j.store.runLifecycle.RequireActiveRun(ctx, runID)
}

const testActivityAttemptSelect = `SELECT request_event_id, run_id, source_event_id, parent_event_id, entity_id, flow_instance, node_id, handler_event_key, activity_id, tool, effect_class, attempt, status, success_event, failure_event, result_event_id, result_event_type, result_payload, failure, input_hash, loop_generation, loop_stage, reply_context_id, execution_mode, started_at, completed_at, updated_at FROM activity_attempts`

func scanPipelineTestActivityAttempt(row interface{ Scan(...any) error }) (ActivityAttemptRecord, error) {
	var record ActivityAttemptRecord
	var sourceEventID, parentEventID, entityID, flowInstance sql.NullString
	var resultEventID, resultEventType, loopStage, replyContextID sql.NullString
	var payloadRaw, failureRaw, generationRaw any
	var startedAtRaw, completedAtRaw, updatedAtRaw any
	if err := row.Scan(
		&record.RequestEventID, &record.RunID, &sourceEventID, &parentEventID, &entityID, &flowInstance,
		&record.NodeID, &record.HandlerEventKey, &record.ActivityID, &record.Tool, &record.EffectClass, &record.Attempt, &record.Status,
		&record.SuccessEvent, &record.FailureEvent, &resultEventID, &resultEventType, &payloadRaw, &failureRaw, &record.InputHash,
		&generationRaw, &loopStage, &replyContextID, &record.ExecutionMode, &startedAtRaw, &completedAtRaw, &updatedAtRaw,
	); err != nil {
		return ActivityAttemptRecord{}, err
	}
	record.SourceEventID, record.ParentEventID, record.EntityID, record.FlowInstance = sourceEventID.String, parentEventID.String, entityID.String, flowInstance.String
	record.ResultEventID, record.ResultEventType, record.LoopStage, record.ReplyContextID = resultEventID.String, resultEventType.String, loopStage.String, replyContextID.String
	if raw := testJSONRaw(generationRaw); len(raw) > 0 && string(raw) != "null" && string(raw) != "{}" {
		if err := json.Unmarshal(raw, &record.Generation); err != nil {
			return ActivityAttemptRecord{}, err
		}
	}
	if raw := testJSONRaw(failureRaw); len(raw) > 0 && string(raw) != "null" {
		failure, err := runtimefailures.UnmarshalEnvelope(raw)
		if err != nil {
			return ActivityAttemptRecord{}, err
		}
		record.Failure = &failure
	}
	if raw := testJSONRaw(payloadRaw); len(raw) > 0 && string(raw) != "null" {
		if err := json.Unmarshal(raw, &record.ResultPayload); err != nil {
			return ActivityAttemptRecord{}, err
		}
	}
	var err error
	if record.StartedAt, _, err = testActivityTime(startedAtRaw); err != nil {
		return ActivityAttemptRecord{}, err
	}
	if record.UpdatedAt, _, err = testActivityTime(updatedAtRaw); err != nil {
		return ActivityAttemptRecord{}, err
	}
	if completed, ok, err := testActivityTime(completedAtRaw); err != nil {
		return ActivityAttemptRecord{}, err
	} else if ok {
		record.CompletedAt = &completed
	}
	return NormalizeActivityAttemptRecord(record), nil
}

func testNullable(value string) any {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return value
}

func testJSONRaw(raw any) []byte {
	switch typed := raw.(type) {
	case nil:
		return nil
	case []byte:
		return typed
	case string:
		return []byte(typed)
	default:
		encoded, _ := json.Marshal(typed)
		return encoded
	}
}

func testActivityTime(raw any) (time.Time, bool, error) {
	if raw == nil {
		return time.Time{}, false, nil
	}
	if value, ok := raw.(time.Time); ok {
		return value.UTC(), !value.IsZero(), nil
	}
	value := strings.TrimSpace(string(testJSONRaw(raw)))
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999-07:00", "2006-01-02 15:04:05.999999999Z07:00", "2006-01-02 15:04:05.999999999", "2006-01-02 15:04:05"} {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed.UTC(), true, nil
		}
	}
	return time.Time{}, false, fmt.Errorf("unsupported activity timestamp %q", value)
}
