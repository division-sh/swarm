package deadletters

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

type Record struct {
	OriginalEventID string
	OriginalEvent   string
	OriginalPayload json.RawMessage
	EntityID        string
	FlowInstance    string
	FailureType     string
	ErrorMessage    string
	RetryCount      int
	ChainDepth      int
	HandlerNode     string
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
	rec.ErrorMessage = strings.TrimSpace(rec.ErrorMessage)
	rec.HandlerNode = strings.TrimSpace(rec.HandlerNode)
	if rec.OriginalEventID == "" {
		return fmt.Errorf("dead letter original event id is required")
	}
	if rec.FailureType == "" {
		return fmt.Errorf("dead letter failure type is required")
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

	const q = `
		INSERT INTO dead_letters (
			original_event_id, original_event, original_payload, entity_id, flow_instance,
			failure_type, error_message, retry_count, chain_depth, handler_node
		)
		SELECT
			e.event_id,
			COALESCE(NULLIF($2, ''), e.event_name),
			COALESCE(NULLIF($3::jsonb, 'null'::jsonb), e.payload, '{}'::jsonb),
			NULLIF(COALESCE(NULLIF($4, ''), COALESCE(e.entity_id::text, '')), '')::uuid,
			COALESCE(NULLIF($5, ''), NULLIF(e.flow_instance, ''), 'runtime'),
			$6,
			NULLIF($7, ''),
			$8,
			$9,
			NULLIF($10, '')
		FROM events e
		WHERE e.event_id = $1::uuid
		  AND NOT EXISTS (
				SELECT 1
				FROM dead_letters dl
				WHERE dl.original_event_id = e.event_id
				  AND dl.failure_type = $6
				  AND COALESCE(dl.handler_node, '') = COALESCE(NULLIF($10, ''), '')
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
		rec.ErrorMessage,
		rec.RetryCount,
		rec.ChainDepth,
		rec.HandlerNode,
	)
	if err != nil {
		return fmt.Errorf("insert dead letter: %w", err)
	}
	rows, err := result.RowsAffected()
	if err == nil && rows > 0 {
		return nil
	}
	var exists bool
	if err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM events WHERE event_id = $1::uuid)`, rec.OriginalEventID).Scan(&exists); err != nil {
		return fmt.Errorf("verify dead letter source event: %w", err)
	}
	if !exists {
		return fmt.Errorf("source event %s not found for dead letter", rec.OriginalEventID)
	}
	return nil
}
