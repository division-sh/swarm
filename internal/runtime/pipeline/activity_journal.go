package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

const (
	ActivityAttemptStatusStarted   = "started"
	ActivityAttemptStatusSucceeded = "succeeded"
	ActivityAttemptStatusFailed    = "failed"
	ActivityAttemptStatusUncertain = "uncertain"
)

type ActivityAttemptRecord struct {
	RequestEventID  string
	RunID           string
	SourceEventID   string
	ParentEventID   string
	EntityID        string
	FlowInstance    string
	NodeID          string
	HandlerEventKey string
	ActivityID      string
	Tool            string
	EffectClass     string
	Attempt         int
	Status          string
	SuccessEvent    string
	FailureEvent    string
	ResultEventID   string
	ResultEventType string
	ResultPayload   map[string]any
	Failure         *runtimefailures.Envelope
	InputHash       string
	ReplyContextID  string
	StartedAt       time.Time
	CompletedAt     *time.Time
	UpdatedAt       time.Time
}

func (s *WorkflowInstanceStore) StartActivityAttempt(ctx context.Context, rec ActivityAttemptRecord) (ActivityAttemptRecord, bool, error) {
	rec = rec.normalized()
	rec.Status = ActivityAttemptStatusStarted
	if err := rec.validateStart(); err != nil {
		return ActivityAttemptRecord{}, false, err
	}
	var out ActivityAttemptRecord
	var inserted bool
	err := s.runInPipelineTransaction(ctx, func(txctx context.Context, _ *sql.Tx) error {
		query := `
			INSERT INTO activity_attempts (
				request_event_id, run_id, source_event_id, parent_event_id, entity_id, flow_instance,
				node_id, handler_event_key, activity_id, tool, effect_class, attempt, status,
				success_event, failure_event, input_hash, reply_context_id
			) VALUES (
				?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULLIF(?, '')
			)
			ON CONFLICT (request_event_id) DO NOTHING
		`
		if !s.isSQLite() {
			query = `
				INSERT INTO activity_attempts (
					request_event_id, run_id, source_event_id, parent_event_id, entity_id, flow_instance,
					node_id, handler_event_key, activity_id, tool, effect_class, attempt, status,
					success_event, failure_event, input_hash, reply_context_id
				) VALUES (
					$1::uuid, $2::uuid, NULLIF($3, '')::uuid, NULLIF($4, '')::uuid, NULLIF($5, '')::uuid, NULLIF($6, ''),
					$7, $8, $9, $10, $11, $12, $13, $14, $15, $16, NULLIF($17, '')
				)
				ON CONFLICT (request_event_id) DO NOTHING
			`
		}
		res, err := dbExecContext(txctx, s.db, query, rec.RequestEventID, rec.RunID, nullableUUID(rec.SourceEventID), nullableUUID(rec.ParentEventID), nullableUUID(rec.EntityID), nullableString(rec.FlowInstance),
			rec.NodeID, rec.HandlerEventKey, rec.ActivityID, rec.Tool, rec.EffectClass, rec.Attempt, rec.Status,
			rec.SuccessEvent, rec.FailureEvent, rec.InputHash, rec.ReplyContextID)
		if err != nil {
			return fmt.Errorf("start activity attempt %s: %w", rec.RequestEventID, err)
		}
		rows, _ := res.RowsAffected()
		inserted = rows > 0
		loaded, ok, err := s.LoadActivityAttempt(txctx, rec.RequestEventID)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("activity attempt %s was not readable after start", rec.RequestEventID)
		}
		out = loaded
		return nil
	})
	if err != nil {
		return ActivityAttemptRecord{}, false, err
	}
	return out, inserted, nil
}

func (s *WorkflowInstanceStore) CompleteActivityAttempt(ctx context.Context, rec ActivityAttemptRecord) (ActivityAttemptRecord, error) {
	rec = rec.normalized()
	if err := rec.validateTerminal(); err != nil {
		return ActivityAttemptRecord{}, err
	}
	var out ActivityAttemptRecord
	err := s.runInPipelineTransaction(ctx, func(txctx context.Context, _ *sql.Tx) error {
		rawPayload, err := json.Marshal(rec.ResultPayload)
		if err != nil {
			return fmt.Errorf("marshal activity attempt result payload: %w", err)
		}
		query := `
			UPDATE activity_attempts
			SET status = ?,
			    result_event_id = ?,
			    result_event_type = ?,
			    result_payload = ?,
			    failure = NULLIF(?, ''),
			    completed_at = CURRENT_TIMESTAMP,
			    updated_at = CURRENT_TIMESTAMP
			WHERE request_event_id = ?
			  AND status = 'started'
		`
		if !s.isSQLite() {
			query = `
				UPDATE activity_attempts
				SET status = $1,
				    result_event_id = $2::uuid,
				    result_event_type = $3,
				    result_payload = $4::jsonb,
				    failure = NULLIF($5, '')::jsonb,
				    completed_at = NOW(),
				    updated_at = NOW()
				WHERE request_event_id = $6::uuid
				  AND status = 'started'
			`
		}
		failureJSON, err := pipelineFailureJSON(rec.Failure)
		if rec.Status == ActivityAttemptStatusSucceeded {
			failureJSON, err = "", nil
		}
		if err != nil {
			return err
		}
		res, err := dbExecContext(txctx, s.db, query, rec.Status, rec.ResultEventID, rec.ResultEventType, string(rawPayload), nullableString(failureJSON), rec.RequestEventID)
		if err != nil {
			return fmt.Errorf("complete activity attempt %s: %w", rec.RequestEventID, err)
		}
		rows, _ := res.RowsAffected()
		loaded, ok, err := s.LoadActivityAttempt(txctx, rec.RequestEventID)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("activity attempt %s was not found for completion", rec.RequestEventID)
		}
		if rows == 0 && loaded.Status == ActivityAttemptStatusStarted {
			return fmt.Errorf("activity attempt %s remained started after terminal update", rec.RequestEventID)
		}
		out = loaded
		return nil
	})
	if err != nil {
		return ActivityAttemptRecord{}, err
	}
	return out, nil
}

func (s *WorkflowInstanceStore) MarkActivityAttemptUncertain(ctx context.Context, rec ActivityAttemptRecord) (ActivityAttemptRecord, error) {
	rec = rec.normalized()
	rec.Status = ActivityAttemptStatusUncertain
	if err := rec.validateTerminal(); err != nil {
		return ActivityAttemptRecord{}, err
	}
	var out ActivityAttemptRecord
	err := s.runInPipelineTransaction(ctx, func(txctx context.Context, _ *sql.Tx) error {
		rawPayload, err := json.Marshal(rec.ResultPayload)
		if err != nil {
			return fmt.Errorf("marshal activity attempt uncertain payload: %w", err)
		}
		query := `
			UPDATE activity_attempts
			SET status = 'uncertain',
			    result_event_id = ?,
			    result_event_type = ?,
			    result_payload = ?,
			    failure = ?,
			    completed_at = CURRENT_TIMESTAMP,
			    updated_at = CURRENT_TIMESTAMP
			WHERE request_event_id = ?
			  AND status = 'started'
		`
		if !s.isSQLite() {
			query = `
				UPDATE activity_attempts
				SET status = 'uncertain',
				    result_event_id = $1::uuid,
				    result_event_type = $2,
				    result_payload = $3::jsonb,
				    failure = $4::jsonb,
				    completed_at = NOW(),
				    updated_at = NOW()
				WHERE request_event_id = $5::uuid
				  AND status = 'started'
			`
		}
		failureJSON, err := pipelineFailureJSON(rec.Failure)
		if err != nil {
			return err
		}
		if _, err := dbExecContext(txctx, s.db, query, rec.ResultEventID, rec.ResultEventType, string(rawPayload), failureJSON, rec.RequestEventID); err != nil {
			return fmt.Errorf("mark activity attempt %s uncertain: %w", rec.RequestEventID, err)
		}
		loaded, ok, err := s.LoadActivityAttempt(txctx, rec.RequestEventID)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("activity attempt %s was not found for uncertain transition", rec.RequestEventID)
		}
		out = loaded
		return nil
	})
	if err != nil {
		return ActivityAttemptRecord{}, err
	}
	return out, nil
}

func (s *WorkflowInstanceStore) LoadActivityAttempt(ctx context.Context, requestEventID string) (ActivityAttemptRecord, bool, error) {
	requestEventID = strings.TrimSpace(requestEventID)
	if requestEventID == "" || s == nil || s.db == nil {
		return ActivityAttemptRecord{}, false, nil
	}
	if _, err := uuid.Parse(requestEventID); err != nil {
		return ActivityAttemptRecord{}, false, fmt.Errorf("activity attempt request_event_id must be a UUID: %w", err)
	}
	query := `
		SELECT request_event_id, run_id, source_event_id, parent_event_id, entity_id, flow_instance,
		       node_id, handler_event_key, activity_id, tool, effect_class, attempt, status,
		       success_event, failure_event, result_event_id, result_event_type, result_payload,
		       failure, input_hash, reply_context_id, started_at, completed_at, updated_at
		FROM activity_attempts
		WHERE request_event_id = ?
	`
	if !s.isSQLite() {
		query = `
			SELECT request_event_id, run_id, source_event_id, parent_event_id, entity_id, flow_instance,
			       node_id, handler_event_key, activity_id, tool, effect_class, attempt, status,
			success_event, failure_event, result_event_id, result_event_type, result_payload,
			       failure, input_hash, reply_context_id, started_at, completed_at, updated_at
			FROM activity_attempts
			WHERE request_event_id = $1::uuid
		`
	}
	row := dbQueryRowContext(ctx, s.db, query, requestEventID)
	rec, err := scanActivityAttempt(row)
	if err == nil {
		return rec, true, nil
	}
	if err == sql.ErrNoRows {
		return ActivityAttemptRecord{}, false, nil
	}
	return ActivityAttemptRecord{}, false, fmt.Errorf("load activity attempt %s: %w", requestEventID, err)
}

func scanActivityAttempt(row *sql.Row) (ActivityAttemptRecord, error) {
	var rec ActivityAttemptRecord
	var sourceEventID, parentEventID, entityID, flowInstance sql.NullString
	var resultEventID, resultEventType, replyContextID sql.NullString
	var rawPayload, rawFailure any
	var startedAtRaw, completedAtRaw, updatedAtRaw any
	if err := row.Scan(
		&rec.RequestEventID, &rec.RunID, &sourceEventID, &parentEventID, &entityID, &flowInstance,
		&rec.NodeID, &rec.HandlerEventKey, &rec.ActivityID, &rec.Tool, &rec.EffectClass, &rec.Attempt, &rec.Status,
		&rec.SuccessEvent, &rec.FailureEvent, &resultEventID, &resultEventType, &rawPayload,
		&rawFailure, &rec.InputHash, &replyContextID, &startedAtRaw, &completedAtRaw, &updatedAtRaw,
	); err != nil {
		return ActivityAttemptRecord{}, err
	}
	startedAt, ok, err := decodeActivityAttemptTime(startedAtRaw)
	if err != nil {
		return ActivityAttemptRecord{}, fmt.Errorf("decode activity attempt started_at: %w", err)
	}
	if ok {
		rec.StartedAt = startedAt
	}
	updatedAt, ok, err := decodeActivityAttemptTime(updatedAtRaw)
	if err != nil {
		return ActivityAttemptRecord{}, fmt.Errorf("decode activity attempt updated_at: %w", err)
	}
	if ok {
		rec.UpdatedAt = updatedAt
	}
	completedAt, ok, err := decodeActivityAttemptTime(completedAtRaw)
	if err != nil {
		return ActivityAttemptRecord{}, fmt.Errorf("decode activity attempt completed_at: %w", err)
	}
	if ok {
		rec.CompletedAt = &completedAt
	}
	rec.SourceEventID = sourceEventID.String
	rec.ParentEventID = parentEventID.String
	rec.EntityID = entityID.String
	rec.FlowInstance = flowInstance.String
	rec.ResultEventID = resultEventID.String
	rec.ResultEventType = resultEventType.String
	rec.ReplyContextID = replyContextID.String
	if raw := activityAttemptJSONRaw(rawFailure); len(raw) > 0 && string(raw) != "null" {
		failure, err := runtimefailures.UnmarshalEnvelope(raw)
		if err != nil {
			return ActivityAttemptRecord{}, fmt.Errorf("decode activity attempt failure: %w", err)
		}
		rec.Failure = &failure
	}
	if rawPayload != nil {
		decoded, err := decodeActivityAttemptPayload(rawPayload)
		if err != nil {
			return ActivityAttemptRecord{}, err
		}
		rec.ResultPayload = decoded
	}
	return rec.normalized(), nil
}

func decodeActivityAttemptTime(raw any) (time.Time, bool, error) {
	switch typed := raw.(type) {
	case nil:
		return time.Time{}, false, nil
	case time.Time:
		if typed.IsZero() {
			return time.Time{}, false, nil
		}
		return typed, true, nil
	case []byte:
		return parseActivityAttemptTimeString(string(typed))
	case string:
		return parseActivityAttemptTimeString(typed)
	default:
		return time.Time{}, false, fmt.Errorf("unsupported timestamp type %T", raw)
	}
}

func parseActivityAttemptTimeString(raw string) (time.Time, bool, error) {
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
		if ts, err := time.Parse(layout, raw); err == nil {
			return ts, true, nil
		}
	}
	return time.Time{}, false, fmt.Errorf("unsupported timestamp %q", raw)
}

func activityAttemptJSONRaw(raw any) []byte {
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

func decodeActivityAttemptPayload(raw any) (map[string]any, error) {
	var bytes []byte
	switch typed := raw.(type) {
	case nil:
		return nil, nil
	case []byte:
		bytes = typed
	case string:
		bytes = []byte(typed)
	default:
		bytes, _ = json.Marshal(typed)
	}
	if len(strings.TrimSpace(string(bytes))) == 0 {
		return nil, nil
	}
	var out map[string]any
	if err := json.Unmarshal(bytes, &out); err != nil {
		return nil, fmt.Errorf("decode activity attempt result_payload: %w", err)
	}
	return out, nil
}

func (rec ActivityAttemptRecord) normalized() ActivityAttemptRecord {
	rec.RequestEventID = strings.TrimSpace(rec.RequestEventID)
	rec.RunID = strings.TrimSpace(rec.RunID)
	rec.SourceEventID = strings.TrimSpace(rec.SourceEventID)
	rec.ParentEventID = strings.TrimSpace(rec.ParentEventID)
	rec.EntityID = strings.TrimSpace(rec.EntityID)
	rec.FlowInstance = strings.TrimSpace(rec.FlowInstance)
	rec.NodeID = strings.TrimSpace(rec.NodeID)
	rec.HandlerEventKey = strings.TrimSpace(rec.HandlerEventKey)
	rec.ActivityID = strings.TrimSpace(rec.ActivityID)
	rec.Tool = strings.TrimSpace(rec.Tool)
	rec.EffectClass = strings.TrimSpace(rec.EffectClass)
	rec.Status = strings.TrimSpace(rec.Status)
	rec.SuccessEvent = strings.TrimSpace(rec.SuccessEvent)
	rec.FailureEvent = strings.TrimSpace(rec.FailureEvent)
	rec.ResultEventID = strings.TrimSpace(rec.ResultEventID)
	rec.ResultEventType = strings.TrimSpace(rec.ResultEventType)
	rec.Failure = runtimefailures.CloneEnvelope(rec.Failure)
	rec.InputHash = strings.TrimSpace(rec.InputHash)
	rec.ReplyContextID = strings.TrimSpace(rec.ReplyContextID)
	if rec.Attempt <= 0 {
		rec.Attempt = 1
	}
	if rec.ResultPayload == nil && rec.Status != ActivityAttemptStatusStarted {
		rec.ResultPayload = map[string]any{}
	}
	return rec
}

func (rec ActivityAttemptRecord) validateStart() error {
	if err := validateActivityAttemptUUID(rec.RequestEventID, "request_event_id"); err != nil {
		return err
	}
	if err := validateActivityAttemptUUID(rec.RunID, "run_id"); err != nil {
		return err
	}
	for field, value := range map[string]string{
		"source_event_id": rec.SourceEventID,
		"parent_event_id": rec.ParentEventID,
		"entity_id":       rec.EntityID,
	} {
		if err := validateOptionalActivityAttemptUUID(value, field); err != nil {
			return err
		}
	}
	if rec.ActivityID == "" || rec.Tool == "" || rec.EffectClass == "" || rec.SuccessEvent == "" || rec.FailureEvent == "" || rec.InputHash == "" {
		return fmt.Errorf("activity attempt %s is missing required identity fields", rec.RequestEventID)
	}
	if rec.EffectClass != "non_idempotent_write" {
		return fmt.Errorf("activity attempt effect_class %q is not supported by the non-idempotent journal", rec.EffectClass)
	}
	if rec.Attempt != 1 {
		return fmt.Errorf("activity attempt attempt = %d, want 1 for non-idempotent journal", rec.Attempt)
	}
	return nil
}

func (rec ActivityAttemptRecord) validateTerminal() error {
	if err := validateActivityAttemptUUID(rec.RequestEventID, "request_event_id"); err != nil {
		return err
	}
	if rec.Status != ActivityAttemptStatusSucceeded && rec.Status != ActivityAttemptStatusFailed && rec.Status != ActivityAttemptStatusUncertain {
		return fmt.Errorf("activity attempt status %q is not terminal", rec.Status)
	}
	if err := validateActivityAttemptUUID(rec.ResultEventID, "result_event_id"); err != nil {
		return err
	}
	if rec.ResultEventType == "" {
		return fmt.Errorf("activity attempt terminal result_event_type is required")
	}
	if rec.ResultPayload == nil {
		return fmt.Errorf("activity attempt terminal result_payload is required")
	}
	if rec.Status == ActivityAttemptStatusSucceeded && rec.Failure != nil {
		return fmt.Errorf("successful activity attempt must not carry failure")
	}
	if (rec.Status == ActivityAttemptStatusFailed || rec.Status == ActivityAttemptStatusUncertain) && rec.Failure == nil {
		return fmt.Errorf("failed or uncertain activity attempt requires canonical failure")
	}
	return nil
}

func validateActivityAttemptUUID(value, field string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return fmt.Errorf("activity attempt %s is required", field)
	}
	if _, err := uuid.Parse(value); err != nil {
		return fmt.Errorf("activity attempt %s must be a UUID: %w", field, err)
	}
	return nil
}

func validateOptionalActivityAttemptUUID(value, field string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	if _, err := uuid.Parse(value); err != nil {
		return fmt.Errorf("activity attempt %s must be a UUID when present: %w", field, err)
	}
	return nil
}

func nullableString(value string) any {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return value
}

func nullableUUID(value string) any {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return value
}
