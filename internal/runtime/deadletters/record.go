package deadletters

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runforkrevision "github.com/division-sh/swarm/internal/runtime/runforkrevision"
	"github.com/google/uuid"
)

type Record struct {
	OriginalEventID string
	OriginalEvent   string
	OriginalPayload json.RawMessage
	EntityID        string
	FlowInstance    string
	Failure         runtimefailures.Envelope
	RetryCount      int
	ChainDepth      int
	HandlerNode     string
	Timestamp       string
}

type InsertResult struct {
	DeadLetterID string
	Inserted     bool
}

type queryExecer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func Insert(ctx context.Context, db *sql.DB, rec Record) error {
	if db == nil {
		return fmt.Errorf("dead letter db is required")
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin dead letter transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	result, err := insert(ctx, tx, rec)
	if err != nil {
		return err
	}
	if result.Inserted {
		if _, err := runforkrevision.CaptureForEvent(ctx, tx, rec.OriginalEventID, runforkrevision.FamilyDeadLetters); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit dead letter transaction: %w", err)
	}
	return nil
}

func InsertTx(ctx context.Context, tx *sql.Tx, rec Record) error {
	_, err := InsertTxWithResult(ctx, tx, rec)
	return err
}

func InsertTxWithResult(ctx context.Context, tx *sql.Tx, rec Record) (InsertResult, error) {
	if tx == nil {
		return InsertResult{}, fmt.Errorf("dead letter tx is required")
	}
	return insert(ctx, tx, rec)
}

func insert(ctx context.Context, db queryExecer, rec Record) (InsertResult, error) {
	rec.OriginalEventID = strings.TrimSpace(rec.OriginalEventID)
	rec.OriginalEvent = strings.TrimSpace(rec.OriginalEvent)
	rec.EntityID = strings.TrimSpace(rec.EntityID)
	rec.FlowInstance = strings.TrimSpace(rec.FlowInstance)
	rec.HandlerNode = strings.TrimSpace(rec.HandlerNode)
	rec.Timestamp = strings.TrimSpace(rec.Timestamp)
	if rec.OriginalEventID == "" {
		return InsertResult{}, fmt.Errorf("dead letter original event id is required")
	}
	if rec.EntityID != "" {
		if _, err := uuid.Parse(rec.EntityID); err != nil {
			rec.EntityID = ""
		}
	}
	failureJSON, err := runtimefailures.MarshalEnvelope(rec.Failure)
	if err != nil {
		return InsertResult{}, fmt.Errorf("dead letter failure is invalid: %w", err)
	}
	if len(rec.OriginalPayload) == 0 {
		rec.OriginalPayload = json.RawMessage(`{}`)
	}
	if rec.RetryCount < 0 {
		rec.RetryCount = 0
	}
	if rec.ChainDepth < 0 {
		rec.ChainDepth = 0
	}
	if rec.Timestamp == "" {
		rec.Timestamp = time.Now().UTC().Format(time.RFC3339Nano)
	}
	deadLetterID := uuid.NewString()

	const q = `
		INSERT INTO dead_letters (
			dead_letter_id, original_event_id, original_event, original_payload, entity_id, flow_instance,
			failure, retry_count, chain_depth, handler_node, created_at
		)
		SELECT
			$1::uuid,
			$2::uuid,
			COALESCE(NULLIF($3, ''), COALESCE((SELECT e.event_name FROM events e WHERE e.event_id = $2::uuid), '')),
			COALESCE(NULLIF($4::jsonb, 'null'::jsonb), COALESCE((SELECT e.payload FROM events e WHERE e.event_id = $2::uuid), '{}'::jsonb)),
			NULLIF($5, '')::uuid,
			COALESCE(NULLIF($6, ''), COALESCE((SELECT NULLIF(e.flow_instance, '') FROM events e WHERE e.event_id = $2::uuid), 'runtime')),
			$7::jsonb,
			$8,
			$9,
			NULLIF($10, ''),
			COALESCE(NULLIF($11, '')::timestamptz, now())
		WHERE NOT EXISTS (
			SELECT 1
			FROM dead_letters dl
			WHERE dl.original_event_id = $2::uuid
			  AND dl.failure = $7::jsonb
			  AND COALESCE(dl.handler_node, '') = COALESCE(NULLIF($10, ''), '')
		)
	`
	result, err := db.ExecContext(
		ctx,
		q,
		deadLetterID,
		rec.OriginalEventID,
		rec.OriginalEvent,
		[]byte(rec.OriginalPayload),
		rec.EntityID,
		rec.FlowInstance,
		failureJSON,
		rec.RetryCount,
		rec.ChainDepth,
		rec.HandlerNode,
		rec.Timestamp,
	)
	if err != nil {
		return InsertResult{}, fmt.Errorf("insert dead letter: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return InsertResult{}, fmt.Errorf("read inserted dead letter rows: %w", err)
	}
	if rows > 0 {
		return InsertResult{DeadLetterID: deadLetterID, Inserted: true}, nil
	}
	if err := db.QueryRowContext(ctx, `
		SELECT dead_letter_id::text
		FROM dead_letters
		WHERE original_event_id = $1::uuid
		  AND failure = $2::jsonb
		  AND COALESCE(handler_node, '') = COALESCE(NULLIF($3, ''), '')
	`, rec.OriginalEventID, failureJSON, rec.HandlerNode).Scan(&deadLetterID); err != nil {
		return InsertResult{}, fmt.Errorf("load existing dead letter: %w", err)
	}
	return InsertResult{DeadLetterID: deadLetterID}, nil
}
