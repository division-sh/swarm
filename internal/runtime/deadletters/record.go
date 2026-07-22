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
	DeliveryID      string
	ClaimVersion    int64
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
	rec.DeliveryID = strings.TrimSpace(rec.DeliveryID)
	rec.OriginalEvent = strings.TrimSpace(rec.OriginalEvent)
	rec.EntityID = strings.TrimSpace(rec.EntityID)
	rec.FlowInstance = strings.TrimSpace(rec.FlowInstance)
	rec.HandlerNode = strings.TrimSpace(rec.HandlerNode)
	rec.Timestamp = strings.TrimSpace(rec.Timestamp)
	if rec.OriginalEventID == "" {
		return InsertResult{}, fmt.Errorf("dead letter original event id is required")
	}
	if (rec.DeliveryID == "") != (rec.ClaimVersion == 0) {
		return InsertResult{}, fmt.Errorf("dead letter delivery id and claim version must be supplied together")
	}
	if rec.DeliveryID != "" {
		if _, err := uuid.Parse(rec.DeliveryID); err != nil {
			return InsertResult{}, fmt.Errorf("dead letter delivery id: %w", err)
		}
		if rec.ClaimVersion <= 0 {
			return InsertResult{}, fmt.Errorf("dead letter claim version must be positive")
		}
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
			dead_letter_id, original_event_id, delivery_id, claim_version, original_event, original_payload, entity_id, flow_instance,
			failure, retry_count, chain_depth, handler_node, created_at
		)
		SELECT
			$1::uuid,
			$2::uuid,
			NULLIF($3, '')::uuid,
			NULLIF($4, 0),
			COALESCE(NULLIF($5, ''), COALESCE((SELECT e.event_name FROM events e WHERE e.event_id = $2::uuid), '')),
			COALESCE(NULLIF($6::jsonb, 'null'::jsonb), COALESCE((SELECT e.payload FROM events e WHERE e.event_id = $2::uuid), '{}'::jsonb)),
			NULLIF($7, '')::uuid,
			COALESCE(NULLIF($8, ''), COALESCE((SELECT NULLIF(e.flow_instance, '') FROM events e WHERE e.event_id = $2::uuid), 'runtime')),
			$9::jsonb,
			$10,
			$11,
			NULLIF($12, ''),
			COALESCE(NULLIF($13, '')::timestamptz, now())
		WHERE NOT EXISTS (
			SELECT 1
			FROM dead_letters dl
			WHERE (NULLIF($3, '') IS NOT NULL AND dl.delivery_id = NULLIF($3, '')::uuid AND dl.claim_version = NULLIF($4, 0))
			   OR (NULLIF($3, '') IS NULL AND dl.delivery_id IS NULL AND dl.original_event_id = $2::uuid
			       AND dl.failure = $9::jsonb
			       AND COALESCE(dl.handler_node, '') = COALESCE(NULLIF($12, ''), ''))
		)
	`
	result, err := db.ExecContext(
		ctx,
		q,
		deadLetterID,
		rec.OriginalEventID,
		rec.DeliveryID,
		rec.ClaimVersion,
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
		WHERE (NULLIF($1, '') IS NOT NULL AND delivery_id = NULLIF($1, '')::uuid AND claim_version = NULLIF($2, 0))
		   OR (NULLIF($1, '') IS NULL AND delivery_id IS NULL AND original_event_id = $3::uuid
		       AND failure = $4::jsonb AND COALESCE(handler_node, '') = COALESCE(NULLIF($5, ''), ''))
	`, rec.DeliveryID, rec.ClaimVersion, rec.OriginalEventID, failureJSON, rec.HandlerNode).Scan(&deadLetterID); err != nil {
		return InsertResult{}, fmt.Errorf("load existing dead letter: %w", err)
	}
	return InsertResult{DeadLetterID: deadLetterID}, nil
}
