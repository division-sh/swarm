package replycontextstore

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/replycontext"
)

func insertReplyContextRow(ctx context.Context, tx *sql.Tx, record replycontext.Record, origin []byte, postgres bool) (sql.Result, error) {
	query := `INSERT INTO reply_contexts (
		reply_context_id, run_id, request_event_id, requester_flow_id,
		request_output_pin, reply_input_pin, provider_flow_id,
		provider_input_pin, provider_output_pin, origin_route,
		request_correlation_id, correlation_key, state,
		accepted_reply_event_id, created_at, updated_at, terminal_at,
		source_run_id, source_request_event_id, enclosing_reply_context_id, source_run_owner_key
	) VALUES (
		$1, NULLIF($2, '')::uuid, $3::uuid, $4, $5, $6, $7, $8, $9, $10::jsonb,
		$11, NULLIF($12, ''), $13, NULLIF($14, '')::uuid, $15, $16, $17,
		NULLIF($18, '')::uuid, $19::uuid, NULLIF($20, ''), COALESCE('run:' || CAST(NULLIF($18, '')::uuid AS TEXT), '')
	) ON CONFLICT DO NOTHING`
	args := []any{record.ID, record.RunID, record.RequestEventID, record.RequesterFlowID,
		record.RequestOutputPin, record.ReplyInputPin, record.ProviderFlowID,
		record.ProviderInputPin, record.ProviderOutputPin, string(origin),
		record.RequestCorrelationID, record.CorrelationKey, string(record.State),
		record.AcceptedReplyEventID, record.CreatedAt, record.UpdatedAt, record.TerminalAt,
		record.SourceRunID, record.SourceRequestEventID, record.EnclosingReplyContextID}
	if !postgres {
		query = `INSERT INTO reply_contexts (
			reply_context_id, run_id, request_event_id, requester_flow_id,
			request_output_pin, reply_input_pin, provider_flow_id,
			provider_input_pin, provider_output_pin, origin_route,
			request_correlation_id, correlation_key, state,
			accepted_reply_event_id, created_at, updated_at, terminal_at,
			source_run_id, source_request_event_id, enclosing_reply_context_id, source_run_owner_key
		) VALUES (?, NULLIF(?, ''), ?, ?, ?, ?, ?, ?, ?, ?, ?, NULLIF(?, ''), ?, NULLIF(?, ''), ?, ?, ?,
		NULLIF(?, ''), ?, NULLIF(?, ''), COALESCE('run:' || NULLIF(?, ''), '')) ON CONFLICT DO NOTHING`
		args = append(args, record.SourceRunID)
	}
	return tx.ExecContext(ctx, query, args...)
}

func requireEnclosingReplyContext(ctx context.Context, tx *sql.Tx, record replycontext.Record, postgres bool) error {
	if record.EnclosingReplyContextID == "" {
		return nil
	}
	var enclosing replycontext.Record
	var err error
	if postgres {
		enclosing, err = loadPostgresReplyContext(ctx, tx, record.EnclosingReplyContextID, true)
	} else {
		enclosing, err = loadSQLiteReplyContext(ctx, tx, record.EnclosingReplyContextID)
	}
	if err != nil {
		return fmt.Errorf("admit enclosing reply context: %w", err)
	}
	if err := record.ValidateEnclosing(enclosing); err != nil {
		return err
	}
	// A projected row retains its cut's state. A newly published request must
	// still be serving an open inbound obligation at the atomic commit boundary.
	if record.RunID == record.SourceRunID && record.RequestEventID == record.SourceRequestEventID {
		return record.BindEnclosing(enclosing)
	}
	return nil
}
