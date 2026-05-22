package deadletters

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

type Record struct {
	OriginalEventID     string
	OriginalEvent       string
	OriginalPayload     json.RawMessage
	EntityID            string
	FlowInstance        string
	FailureType         string
	TargetFailureReason string
	TargetContext       json.RawMessage
	ErrorMessage        string
	RetryCount          int
	ChainDepth          int
	HandlerNode         string
	Timestamp           string
}

func Insert(ctx context.Context, db *sql.DB, rec Record) error {
	if db == nil {
		return fmt.Errorf("dead letter db is required")
	}
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
		return fmt.Errorf("dead letter original event id is required")
	}
	if rec.EntityID != "" {
		if _, err := uuid.Parse(rec.EntityID); err != nil {
			rec.EntityID = ""
		}
	}
	if rec.FailureType == "" {
		return fmt.Errorf("dead letter failure type is required")
	}
	if len(rec.OriginalPayload) == 0 {
		rec.OriginalPayload = json.RawMessage(`{}`)
	}
	if len(rec.TargetContext) == 0 {
		rec.TargetContext = json.RawMessage(`{}`)
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
			failure_type, target_failure_reason, target_context, error_message, retry_count, chain_depth, handler_node, created_at
		)
		SELECT
			$1::uuid,
			COALESCE(NULLIF($2, ''), COALESCE((SELECT e.event_name FROM events e WHERE e.event_id = $1::uuid), '')),
			COALESCE(NULLIF($3::jsonb, 'null'::jsonb), COALESCE((SELECT e.payload FROM events e WHERE e.event_id = $1::uuid), '{}'::jsonb)),
			NULLIF($4, '')::uuid,
			COALESCE(NULLIF($5, ''), COALESCE((SELECT NULLIF(e.flow_instance, '') FROM events e WHERE e.event_id = $1::uuid), 'runtime')),
			$6,
			NULLIF($7, ''),
			COALESCE(NULLIF($8::jsonb, 'null'::jsonb), '{}'::jsonb),
			NULLIF($9, ''),
			$10,
			$11,
			NULLIF($12, ''),
			COALESCE(NULLIF($13, '')::timestamptz, now())
		WHERE NOT EXISTS (
			SELECT 1
			FROM dead_letters dl
			WHERE dl.original_event_id = $1::uuid
			  AND dl.failure_type = $6
			  AND COALESCE(dl.handler_node, '') = COALESCE(NULLIF($12, ''), '')
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
		rec.FailureType,
		rec.TargetFailureReason,
		[]byte(rec.TargetContext),
		rec.ErrorMessage,
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
