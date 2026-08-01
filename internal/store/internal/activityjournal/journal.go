package activityjournal

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

type Dialect uint8

const (
	DialectUnknown Dialect = iota
	DialectPostgres
	DialectSQLite
)

type ActiveRunOwner interface {
	RequireActiveRun(context.Context, string) error
}

type QueryRower interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func Start(ctx context.Context, tx *sql.Tx, dialect Dialect, runs ActiveRunOwner, record runtimepipeline.ActivityAttemptRecord) (runtimepipeline.ActivityAttemptRecord, bool, error) {
	return start(ctx, tx, dialect, runs, record, true)
}

func Claim(ctx context.Context, tx *sql.Tx, dialect Dialect, runs ActiveRunOwner, record runtimepipeline.ActivityAttemptRecord) (runtimepipeline.ActivityAttemptRecord, bool, error) {
	record = runtimepipeline.NormalizeActivityAttemptRecord(record)
	if !record.Generation.Valid() {
		return start(ctx, tx, dialect, runs, record, true)
	}
	if err := requireMutation(tx, dialect, runs); err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, false, err
	}
	if err := runs.RequireActiveRun(ctx, record.RunID); err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, false, err
	}
	metadata, stateBuckets, found, err := loadLoopState(ctx, tx, dialect, record.RunID, record.EntityID)
	if err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, false, err
	}
	if !found {
		return runtimepipeline.ActivityAttemptRecord{}, false, runtimefailures.New(runtimefailures.ClassUnexpectedArrival, "activity_loop_instance_missing", "activity-runtime", "claim_activity_attempt", map[string]any{
			"activity_id": record.ActivityID, "entity_id": record.EntityID, "loop_id": record.Generation.LoopID,
		})
	}
	current, err := runtimepipeline.WorkflowLoopGenerationCurrent(metadata, stateBuckets, record.Generation, record.LoopStage)
	if err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, false, err
	}
	if !current {
		return runtimepipeline.ActivityAttemptRecord{}, false, runtimefailures.New(runtimefailures.ClassStaleArrival, "activity_loop_generation_stale", "activity-runtime", "claim_activity_attempt", map[string]any{
			"activity_id": record.ActivityID, "entity_id": record.EntityID, "loop_id": record.Generation.LoopID,
			"revision_id": record.Generation.RevisionID, "expected_stage": record.LoopStage,
		})
	}
	actual, inserted, err := start(ctx, tx, dialect, runs, record, false)
	if err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, false, err
	}
	if err := runtimepipeline.ValidateActivityAttemptClaimIdentity(actual, record); err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, false, err
	}
	return actual, inserted, nil
}

func start(ctx context.Context, tx *sql.Tx, dialect Dialect, runs ActiveRunOwner, record runtimepipeline.ActivityAttemptRecord, verifyRun bool) (runtimepipeline.ActivityAttemptRecord, bool, error) {
	record = runtimepipeline.NormalizeActivityAttemptRecord(record)
	record.Status = runtimepipeline.ActivityAttemptStatusStarted
	if err := runtimepipeline.ValidateActivityAttemptStart(record); err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, false, err
	}
	if err := requireMutation(tx, dialect, runs); err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, false, err
	}
	if verifyRun {
		if err := runs.RequireActiveRun(ctx, record.RunID); err != nil {
			return runtimepipeline.ActivityAttemptRecord{}, false, err
		}
	}
	generationJSON, err := json.Marshal(record.Generation)
	if err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, false, fmt.Errorf("marshal activity loop generation: %w", err)
	}
	query := `
		INSERT INTO activity_attempts (
			request_event_id, run_id, source_event_id, parent_event_id, entity_id, flow_instance,
			node_id, handler_event_key, activity_id, tool, effect_class, attempt, status,
			success_event, failure_event, input_hash, loop_generation, loop_stage, reply_context_id, execution_mode
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULLIF(?, ''), ?)
		ON CONFLICT (request_event_id) DO NOTHING
	`
	if dialect == DialectPostgres {
		query = `
			INSERT INTO activity_attempts (
				request_event_id, run_id, source_event_id, parent_event_id, entity_id, flow_instance,
				node_id, handler_event_key, activity_id, tool, effect_class, attempt, status,
				success_event, failure_event, input_hash, loop_generation, loop_stage, reply_context_id, execution_mode
			) VALUES (
				$1::uuid, $2::uuid, NULLIF($3, '')::uuid, NULLIF($4, '')::uuid, NULLIF($5, '')::uuid, NULLIF($6, ''),
				$7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17::jsonb, NULLIF($18, ''), NULLIF($19, ''), $20
			)
			ON CONFLICT (request_event_id) DO NOTHING
		`
	}
	result, err := tx.ExecContext(ctx, query,
		record.RequestEventID, record.RunID, nullableUUID(record.SourceEventID), nullableUUID(record.ParentEventID), nullableUUID(record.EntityID), nullableString(record.FlowInstance),
		record.NodeID, record.HandlerEventKey, record.ActivityID, record.Tool, record.EffectClass, record.Attempt, record.Status,
		record.SuccessEvent, record.FailureEvent, record.InputHash, string(generationJSON), nullableString(record.LoopStage), record.ReplyContextID, record.ExecutionMode,
	)
	if err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, false, fmt.Errorf("start activity attempt %s: %w", record.RequestEventID, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, false, fmt.Errorf("read activity attempt insert disposition %s: %w", record.RequestEventID, err)
	}
	actual, found, err := Load(ctx, tx, dialect, record.RequestEventID)
	if err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, false, err
	}
	if !found {
		return runtimepipeline.ActivityAttemptRecord{}, false, fmt.Errorf("activity attempt %s was not readable after start", record.RequestEventID)
	}
	if err := runtimepipeline.ValidateActivityAttemptClaimIdentity(actual, record); err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, false, err
	}
	if err := runtimeauthoractivity.Record(ctx, runtimepipeline.ActivityAttemptStoryDraft(actual, runtimepipeline.ActivityAttemptStatusStarted)); err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, false, err
	}
	return actual, rows > 0, nil
}

func Complete(ctx context.Context, tx *sql.Tx, dialect Dialect, runs ActiveRunOwner, record runtimepipeline.ActivityAttemptRecord) (runtimepipeline.ActivityAttemptRecord, error) {
	record = runtimepipeline.NormalizeActivityAttemptRecord(record)
	if err := runtimepipeline.ValidateActivityAttemptTerminal(record); err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, err
	}
	if err := requireMutation(tx, dialect, runs); err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, err
	}
	if err := runs.RequireActiveRun(ctx, record.RunID); err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, err
	}
	payload, err := json.Marshal(record.ResultPayload)
	if err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, fmt.Errorf("marshal activity attempt result payload: %w", err)
	}
	failure, err := failureJSON(record.Failure)
	if record.Status == runtimepipeline.ActivityAttemptStatusSucceeded {
		failure, err = "", nil
	}
	if err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, err
	}
	query := `
		UPDATE activity_attempts
		SET status = ?, result_event_id = ?, result_event_type = ?, result_payload = ?, failure = NULLIF(?, ''),
		    completed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
		WHERE request_event_id = ? AND execution_mode = ? AND status = 'started'
	`
	if dialect == DialectPostgres {
		query = `
			UPDATE activity_attempts
			SET status = $1, result_event_id = $2::uuid, result_event_type = $3, result_payload = $4::jsonb,
			    failure = NULLIF($5, '')::jsonb, completed_at = NOW(), updated_at = NOW()
			WHERE request_event_id = $6::uuid AND execution_mode = $7 AND status = 'started'
		`
	}
	result, err := tx.ExecContext(ctx, query, record.Status, record.ResultEventID, record.ResultEventType, string(payload), nullableString(failure), record.RequestEventID, record.ExecutionMode)
	if err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, fmt.Errorf("complete activity attempt %s: %w", record.RequestEventID, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, fmt.Errorf("read activity attempt completion disposition %s: %w", record.RequestEventID, err)
	}
	actual, found, err := Load(ctx, tx, dialect, record.RequestEventID)
	if err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, err
	}
	if !found {
		return runtimepipeline.ActivityAttemptRecord{}, fmt.Errorf("activity attempt %s was not found for completion", record.RequestEventID)
	}
	if err := runtimepipeline.ValidateActivityAttemptTerminalMode(actual, record); err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, err
	}
	if rows == 0 && actual.Status == runtimepipeline.ActivityAttemptStatusStarted {
		return runtimepipeline.ActivityAttemptRecord{}, fmt.Errorf("activity attempt %s remained started after terminal update", record.RequestEventID)
	}
	if err := runtimeauthoractivity.Record(ctx, runtimepipeline.ActivityAttemptStoryDraft(actual, actual.Status)); err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, err
	}
	return actual, nil
}

func MarkUncertain(ctx context.Context, tx *sql.Tx, dialect Dialect, runs ActiveRunOwner, record runtimepipeline.ActivityAttemptRecord) (runtimepipeline.ActivityAttemptRecord, error) {
	record = runtimepipeline.NormalizeActivityAttemptRecord(record)
	record.Status = runtimepipeline.ActivityAttemptStatusUncertain
	if err := runtimepipeline.ValidateActivityAttemptTerminal(record); err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, err
	}
	if err := requireMutation(tx, dialect, runs); err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, err
	}
	if err := runs.RequireActiveRun(ctx, record.RunID); err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, err
	}
	payload, err := json.Marshal(record.ResultPayload)
	if err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, fmt.Errorf("marshal activity attempt uncertain payload: %w", err)
	}
	failure, err := failureJSON(record.Failure)
	if err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, err
	}
	query := `
		UPDATE activity_attempts
		SET status = 'uncertain', result_event_id = ?, result_event_type = ?, result_payload = ?, failure = ?,
		    completed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
		WHERE request_event_id = ? AND execution_mode = ? AND status = 'started'
	`
	if dialect == DialectPostgres {
		query = `
			UPDATE activity_attempts
			SET status = 'uncertain', result_event_id = $1::uuid, result_event_type = $2, result_payload = $3::jsonb,
			    failure = $4::jsonb, completed_at = NOW(), updated_at = NOW()
			WHERE request_event_id = $5::uuid AND execution_mode = $6 AND status = 'started'
		`
	}
	if _, err := tx.ExecContext(ctx, query, record.ResultEventID, record.ResultEventType, string(payload), failure, record.RequestEventID, record.ExecutionMode); err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, fmt.Errorf("mark activity attempt %s uncertain: %w", record.RequestEventID, err)
	}
	actual, found, err := Load(ctx, tx, dialect, record.RequestEventID)
	if err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, err
	}
	if !found {
		return runtimepipeline.ActivityAttemptRecord{}, fmt.Errorf("activity attempt %s was not found for uncertain transition", record.RequestEventID)
	}
	if err := runtimepipeline.ValidateActivityAttemptTerminalMode(actual, record); err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, err
	}
	if err := runtimeauthoractivity.Record(ctx, runtimepipeline.ActivityAttemptStoryDraft(actual, runtimepipeline.ActivityAttemptStatusUncertain)); err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, err
	}
	return actual, nil
}

func Load(ctx context.Context, db QueryRower, dialect Dialect, requestEventID string) (runtimepipeline.ActivityAttemptRecord, bool, error) {
	requestEventID = strings.TrimSpace(requestEventID)
	if requestEventID == "" {
		return runtimepipeline.ActivityAttemptRecord{}, false, fmt.Errorf("activity attempt request_event_id is required")
	}
	if _, err := uuid.Parse(requestEventID); err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, false, fmt.Errorf("activity attempt request_event_id must be a UUID: %w", err)
	}
	if db == nil {
		return runtimepipeline.ActivityAttemptRecord{}, false, fmt.Errorf("activity attempt reader is required")
	}
	query := activityAttemptSelect + ` WHERE request_event_id = ?`
	if dialect == DialectPostgres {
		query = activityAttemptSelect + ` WHERE request_event_id = $1::uuid`
	}
	record, err := scan(db.QueryRowContext(ctx, query, requestEventID))
	if err == sql.ErrNoRows {
		return runtimepipeline.ActivityAttemptRecord{}, false, nil
	}
	if err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, false, fmt.Errorf("load activity attempt %s: %w", requestEventID, err)
	}
	return record, true, nil
}

const activityAttemptSelect = `
	SELECT request_event_id, run_id, source_event_id, parent_event_id, entity_id, flow_instance,
	       node_id, handler_event_key, activity_id, tool, effect_class, attempt, status,
	       success_event, failure_event, result_event_id, result_event_type, result_payload,
	       failure, input_hash, loop_generation, loop_stage, reply_context_id, execution_mode,
	       started_at, completed_at, updated_at
	FROM activity_attempts
`

func scan(row interface{ Scan(...any) error }) (runtimepipeline.ActivityAttemptRecord, error) {
	var record runtimepipeline.ActivityAttemptRecord
	var sourceEventID, parentEventID, entityID, flowInstance sql.NullString
	var resultEventID, resultEventType, loopStage, replyContextID sql.NullString
	var rawPayload, rawFailure, rawGeneration any
	var startedAtRaw, completedAtRaw, updatedAtRaw any
	if err := row.Scan(
		&record.RequestEventID, &record.RunID, &sourceEventID, &parentEventID, &entityID, &flowInstance,
		&record.NodeID, &record.HandlerEventKey, &record.ActivityID, &record.Tool, &record.EffectClass, &record.Attempt, &record.Status,
		&record.SuccessEvent, &record.FailureEvent, &resultEventID, &resultEventType, &rawPayload,
		&rawFailure, &record.InputHash, &rawGeneration, &loopStage, &replyContextID, &record.ExecutionMode,
		&startedAtRaw, &completedAtRaw, &updatedAtRaw,
	); err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, err
	}
	startedAt, ok, err := decodeTime(startedAtRaw)
	if err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, fmt.Errorf("decode activity attempt started_at: %w", err)
	}
	if ok {
		record.StartedAt = startedAt
	}
	updatedAt, ok, err := decodeTime(updatedAtRaw)
	if err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, fmt.Errorf("decode activity attempt updated_at: %w", err)
	}
	if ok {
		record.UpdatedAt = updatedAt
	}
	completedAt, ok, err := decodeTime(completedAtRaw)
	if err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, fmt.Errorf("decode activity attempt completed_at: %w", err)
	}
	if ok {
		record.CompletedAt = &completedAt
	}
	record.SourceEventID = sourceEventID.String
	record.ParentEventID = parentEventID.String
	record.EntityID = entityID.String
	record.FlowInstance = flowInstance.String
	record.ResultEventID = resultEventID.String
	record.ResultEventType = resultEventType.String
	record.ReplyContextID = replyContextID.String
	record.LoopStage = loopStage.String
	if raw := jsonRaw(rawGeneration); len(raw) > 0 && string(raw) != "null" && string(raw) != "{}" {
		if err := json.Unmarshal(raw, &record.Generation); err != nil {
			return runtimepipeline.ActivityAttemptRecord{}, fmt.Errorf("decode activity attempt loop generation: %w", err)
		}
	}
	if raw := jsonRaw(rawFailure); len(raw) > 0 && string(raw) != "null" {
		failure, err := runtimefailures.UnmarshalEnvelope(raw)
		if err != nil {
			return runtimepipeline.ActivityAttemptRecord{}, fmt.Errorf("decode activity attempt failure: %w", err)
		}
		record.Failure = &failure
	}
	if rawPayload != nil {
		payload, err := decodePayload(rawPayload)
		if err != nil {
			return runtimepipeline.ActivityAttemptRecord{}, err
		}
		record.ResultPayload = payload
	}
	return runtimepipeline.NormalizeActivityAttemptRecord(record), nil
}

func loadLoopState(ctx context.Context, tx *sql.Tx, dialect Dialect, runID, entityID string) (map[string]any, map[string]any, bool, error) {
	query := `SELECT fields, gates, accumulator FROM entity_state WHERE run_id = ? AND entity_id = ?`
	if dialect == DialectPostgres {
		query = `SELECT fields, gates, accumulator FROM entity_state WHERE run_id = $1::uuid AND entity_id = $2::uuid FOR UPDATE`
	}
	var fieldsRaw, gatesRaw, accumulatorRaw any
	err := tx.QueryRowContext(ctx, query, runID, entityID).Scan(&fieldsRaw, &gatesRaw, &accumulatorRaw)
	if err == sql.ErrNoRows {
		return nil, nil, false, nil
	}
	if err != nil {
		return nil, nil, false, err
	}
	metadata, err := decodeMap(fieldsRaw, "entity_state.fields")
	if err != nil {
		return nil, nil, false, err
	}
	gates, err := decodeMap(gatesRaw, "entity_state.gates")
	if err != nil {
		return nil, nil, false, err
	}
	if len(gates) > 0 {
		metadata["gates"] = gates
	}
	stateBuckets, err := decodeMap(accumulatorRaw, "entity_state.accumulator")
	if err != nil {
		return nil, nil, false, err
	}
	return metadata, stateBuckets, true, nil
}

func requireMutation(tx *sql.Tx, dialect Dialect, runs ActiveRunOwner) error {
	if tx == nil {
		return fmt.Errorf("activity attempt operation requires selected-store transaction")
	}
	if dialect != DialectPostgres && dialect != DialectSQLite {
		return fmt.Errorf("activity attempt operation requires selected-store dialect")
	}
	if runs == nil {
		return fmt.Errorf("activity attempt operation requires run lifecycle owner")
	}
	return nil
}

func failureJSON(failure *runtimefailures.Envelope) (string, error) {
	if failure == nil {
		return "", nil
	}
	raw, err := json.Marshal(failure)
	if err != nil {
		return "", fmt.Errorf("marshal activity attempt failure: %w", err)
	}
	return string(raw), nil
}

func nullableString(value string) any {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return value
}

func nullableUUID(value string) any { return nullableString(value) }

func jsonRaw(raw any) []byte {
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

func decodeMap(raw any, label string) (map[string]any, error) {
	bytes := jsonRaw(raw)
	if len(bytes) == 0 {
		return nil, fmt.Errorf("%s is empty", label)
	}
	var out map[string]any
	if err := json.Unmarshal(bytes, &out); err != nil {
		return nil, fmt.Errorf("decode %s: %w", label, err)
	}
	if out == nil {
		return nil, fmt.Errorf("%s must be a JSON object", label)
	}
	return out, nil
}

func decodePayload(raw any) (map[string]any, error) {
	bytes := jsonRaw(raw)
	if len(strings.TrimSpace(string(bytes))) == 0 {
		return nil, nil
	}
	var out map[string]any
	if err := json.Unmarshal(bytes, &out); err != nil {
		return nil, fmt.Errorf("decode activity attempt result_payload: %w", err)
	}
	return out, nil
}

func decodeTime(raw any) (time.Time, bool, error) {
	switch typed := raw.(type) {
	case nil:
		return time.Time{}, false, nil
	case time.Time:
		if typed.IsZero() {
			return time.Time{}, false, nil
		}
		return typed.UTC(), true, nil
	case []byte:
		return parseTime(string(typed))
	case string:
		return parseTime(typed)
	default:
		return time.Time{}, false, fmt.Errorf("unsupported timestamp type %T", raw)
	}
}

func parseTime(raw string) (time.Time, bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, false, nil
	}
	for _, layout := range []string{
		time.RFC3339Nano,
		"2006-01-02T15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
	} {
		if value, err := time.Parse(layout, raw); err == nil {
			return value.UTC(), true, nil
		}
	}
	return time.Time{}, false, fmt.Errorf("unsupported timestamp %q", raw)
}
