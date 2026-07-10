package deadletters

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

type execer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func Insert(ctx context.Context, db *sql.DB, rec Record) error {
	if db == nil {
		return fmt.Errorf("dead letter db is required")
	}
	return insert(ctx, db, rec)
}

func InsertTx(ctx context.Context, tx *sql.Tx, rec Record) error {
	if tx == nil {
		return fmt.Errorf("dead letter tx is required")
	}
	return insert(ctx, tx, rec)
}

func insert(ctx context.Context, db execer, rec Record) error {
	rec.OriginalEventID = strings.TrimSpace(rec.OriginalEventID)
	rec.OriginalEvent = strings.TrimSpace(rec.OriginalEvent)
	rec.EntityID = strings.TrimSpace(rec.EntityID)
	rec.FlowInstance = strings.TrimSpace(rec.FlowInstance)
	rec.HandlerNode = strings.TrimSpace(rec.HandlerNode)
	rec.Timestamp = strings.TrimSpace(rec.Timestamp)
	if rec.OriginalEventID == "" {
		return fmt.Errorf("dead letter original event id is required")
	}
	if rec.EntityID != "" {
		if _, err := uuid.Parse(rec.EntityID); err != nil {
			rec.EntityID = ""
		}
	}
	failureJSON, err := runtimefailures.MarshalEnvelope(rec.Failure)
	if err != nil {
		return fmt.Errorf("dead letter failure is invalid: %w", err)
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

	const q = `
		INSERT INTO dead_letters (
			original_event_id, original_event, original_payload, entity_id, flow_instance,
			failure, retry_count, chain_depth, handler_node, created_at
		)
		SELECT
			$1::uuid,
			COALESCE(NULLIF($2, ''), COALESCE((SELECT e.event_name FROM events e WHERE e.event_id = $1::uuid), '')),
			COALESCE(NULLIF($3::jsonb, 'null'::jsonb), COALESCE((SELECT e.payload FROM events e WHERE e.event_id = $1::uuid), '{}'::jsonb)),
			NULLIF($4, '')::uuid,
			COALESCE(NULLIF($5, ''), COALESCE((SELECT NULLIF(e.flow_instance, '') FROM events e WHERE e.event_id = $1::uuid), 'runtime')),
			$6::jsonb,
			$7,
			$8,
			NULLIF($9, ''),
			COALESCE(NULLIF($10, '')::timestamptz, now())
		WHERE NOT EXISTS (
			SELECT 1
			FROM dead_letters dl
			WHERE dl.original_event_id = $1::uuid
			  AND dl.failure = $6::jsonb
			  AND COALESCE(dl.handler_node, '') = COALESCE(NULLIF($9, ''), '')
		)
	`
	result, err := db.ExecContext(
		ctx,
		q,
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
		return fmt.Errorf("insert dead letter: %w", err)
	}
	rows, err := result.RowsAffected()
	if err == nil && rows >= 0 {
		return nil
	}
	return nil
}
