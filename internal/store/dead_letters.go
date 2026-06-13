package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	runtimedeadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	"github.com/google/uuid"
)

var _ eventMutationDeadLetterTxRecorder = (*PostgresStore)(nil)
var _ eventMutationDeadLetterTxRecorder = (*SQLiteRuntimeStore)(nil)

func (s *PostgresStore) RecordDeadLetter(ctx context.Context, rec runtimedeadletters.Record) error {
	return runtimedeadletters.Insert(ctx, s.DB, rec)
}

func (s *PostgresStore) RecordDeadLetterTx(ctx context.Context, tx *sql.Tx, rec runtimedeadletters.Record) error {
	return runtimedeadletters.InsertTx(ctx, tx, rec)
}

func (s *SQLiteRuntimeStore) RecordDeadLetter(ctx context.Context, rec runtimedeadletters.Record) error {
	return s.RecordDeadLetterTx(ctx, nil, rec)
}

func (s *SQLiteRuntimeStore) RecordDeadLetterTx(ctx context.Context, tx *sql.Tx, rec runtimedeadletters.Record) error {
	if tx == nil {
		return s.runRuntimeMutation(ctx, "sqlite record dead letter", func(txctx context.Context, tx *sql.Tx) error {
			return s.RecordDeadLetterTx(txctx, tx, rec)
		})
	}
	rec, createdAt, err := normalizeSQLiteDeadLetterRecord(s, rec)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO dead_letters (
			dead_letter_id, original_event_id, original_event, original_payload, entity_id, flow_instance,
			failure_type, target_failure_reason, target_context, error_message, retry_count, chain_depth, handler_node, created_at
		)
		SELECT
			?,
			?,
			COALESCE(NULLIF(?, ''), COALESCE((SELECT e.event_name FROM events e WHERE e.event_id = ?), '')),
			COALESCE(NULLIF(?, 'null'), COALESCE((SELECT e.payload FROM events e WHERE e.event_id = ?), '{}')),
			?,
			COALESCE(NULLIF(?, ''), COALESCE((SELECT NULLIF(e.flow_instance, '') FROM events e WHERE e.event_id = ?), 'runtime')),
			?,
			NULLIF(?, ''),
			COALESCE(NULLIF(?, 'null'), '{}'),
			NULLIF(?, ''),
			?,
			?,
			NULLIF(?, ''),
			?
		WHERE NOT EXISTS (
			SELECT 1
			FROM dead_letters dl
			WHERE dl.original_event_id = ?
			  AND dl.failure_type = ?
			  AND COALESCE(dl.handler_node, '') = COALESCE(NULLIF(?, ''), '')
		)
	`,
		uuid.NewString(),
		rec.OriginalEventID,
		rec.OriginalEvent,
		rec.OriginalEventID,
		string(rec.OriginalPayload),
		rec.OriginalEventID,
		sqliteNullUUID(rec.EntityID),
		rec.FlowInstance,
		rec.OriginalEventID,
		rec.FailureType,
		rec.TargetFailureReason,
		string(rec.TargetContext),
		rec.ErrorMessage,
		rec.RetryCount,
		rec.ChainDepth,
		rec.HandlerNode,
		createdAt,
		rec.OriginalEventID,
		rec.FailureType,
		rec.HandlerNode,
	); err != nil {
		return fmt.Errorf("insert sqlite dead letter: %w", err)
	}
	return nil
}

func normalizeSQLiteDeadLetterRecord(s *SQLiteRuntimeStore, rec runtimedeadletters.Record) (runtimedeadletters.Record, time.Time, error) {
	rec.OriginalEventID = strings.TrimSpace(rec.OriginalEventID)
	rec.OriginalEvent = strings.TrimSpace(rec.OriginalEvent)
	rec.EntityID = strings.TrimSpace(rec.EntityID)
	rec.FlowInstance = strings.TrimSpace(rec.FlowInstance)
	rec.FailureType = strings.TrimSpace(rec.FailureType)
	rec.TargetFailureReason = strings.TrimSpace(rec.TargetFailureReason)
	rec.ErrorMessage = strings.TrimSpace(rec.ErrorMessage)
	rec.HandlerNode = strings.TrimSpace(rec.HandlerNode)
	rec.Timestamp = strings.TrimSpace(rec.Timestamp)
	if rec.OriginalEventID == "" {
		return rec, time.Time{}, fmt.Errorf("dead letter original event id is required")
	}
	if _, err := uuid.Parse(rec.OriginalEventID); err != nil {
		return rec, time.Time{}, fmt.Errorf("dead letter original event id must be a uuid: %w", err)
	}
	if rec.EntityID != "" {
		if _, err := uuid.Parse(rec.EntityID); err != nil {
			rec.EntityID = ""
		}
	}
	if rec.FailureType == "" {
		return rec, time.Time{}, fmt.Errorf("dead letter failure type is required")
	}
	if len(rec.OriginalPayload) == 0 {
		rec.OriginalPayload = json.RawMessage(`{}`)
	}
	if !json.Valid(rec.OriginalPayload) {
		return rec, time.Time{}, fmt.Errorf("dead letter original payload must be valid json")
	}
	if len(rec.TargetContext) == 0 {
		rec.TargetContext = json.RawMessage(`{}`)
	}
	if !json.Valid(rec.TargetContext) {
		return rec, time.Time{}, fmt.Errorf("dead letter target context must be valid json")
	}
	if rec.RetryCount < 0 {
		rec.RetryCount = 0
	}
	if rec.ChainDepth < 0 {
		rec.ChainDepth = 0
	}
	createdAt := time.Now().UTC()
	if s != nil {
		createdAt = s.now()
	}
	if rec.Timestamp != "" {
		parsed, err := time.Parse(time.RFC3339Nano, rec.Timestamp)
		if err != nil {
			return rec, time.Time{}, fmt.Errorf("dead letter timestamp must be RFC3339Nano: %w", err)
		}
		createdAt = parsed.UTC()
	}
	return rec, createdAt, nil
}
