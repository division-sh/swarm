package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimereplycontext "github.com/division-sh/swarm/internal/runtime/replycontext"
	runforkrevision "github.com/division-sh/swarm/internal/runtime/runforkrevision"
)

type replyContextSQL interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

var _ runtimereplycontext.Store = (*PostgresStore)(nil)
var _ runtimereplycontext.Store = (*SQLiteRuntimeStore)(nil)

func (s *PostgresStore) CreateReplyContext(ctx context.Context, record runtimereplycontext.Record) error {
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		return createPostgresReplyContext(ctx, tx, record)
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin reply context create: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := createPostgresReplyContext(ctx, tx, record); err != nil {
		return err
	}
	if _, err := runforkrevision.CaptureCurrentTransaction(ctx, tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit reply context create: %w", err)
	}
	return nil
}

func createPostgresReplyContext(ctx context.Context, db replyContextSQL, record runtimereplycontext.Record) error {
	record = record.Normalized()
	if err := record.Validate(); err != nil {
		return err
	}
	origin, err := json.Marshal(record.Origin)
	if err != nil {
		return fmt.Errorf("encode reply context origin: %w", err)
	}
	result, err := db.ExecContext(ctx, `
		INSERT INTO reply_contexts (
			reply_context_id, run_id, request_event_id, requester_flow_id,
			request_output_pin, reply_input_pin, provider_flow_id,
			provider_input_pin, provider_output_pin, origin_route,
			request_correlation_id, correlation_key, state,
			accepted_reply_event_id, created_at, updated_at, terminal_at
		)
		VALUES (
			$1, NULLIF($2, '')::uuid, $3::uuid, $4,
			$5, $6, $7, $8, $9, $10::jsonb,
			$11, NULLIF($12, ''), $13,
			NULLIF($14, '')::uuid, $15, $16, $17
		)
		ON CONFLICT DO NOTHING
	`, record.ID, record.RunID, record.RequestEventID, record.RequesterFlowID,
		record.RequestOutputPin, record.ReplyInputPin, record.ProviderFlowID,
		record.ProviderInputPin, record.ProviderOutputPin, string(origin),
		record.RequestCorrelationID, record.CorrelationKey, string(record.State),
		record.AcceptedReplyEventID, record.CreatedAt, record.UpdatedAt, record.TerminalAt)
	if err != nil {
		return fmt.Errorf("create reply context: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("create reply context rows: %w", err)
	}
	if rows == 1 {
		return nil
	}
	existing, loadErr := loadPostgresReplyContext(ctx, db, record.ID, false)
	return resolveReplyContextCreateConflict(record, existing, loadErr)
}

func (s *SQLiteRuntimeStore) CreateReplyContext(ctx context.Context, record runtimereplycontext.Record) error {
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		return createSQLiteReplyContextTx(ctx, tx, record)
	}
	return s.runRuntimeMutation(ctx, "sqlite reply context create", func(txctx context.Context, tx *sql.Tx) error {
		return createSQLiteReplyContextTx(txctx, tx, record)
	})
}

func createSQLiteReplyContextTx(ctx context.Context, db replyContextSQL, record runtimereplycontext.Record) error {
	record = record.Normalized()
	if err := record.Validate(); err != nil {
		return err
	}
	origin, err := json.Marshal(record.Origin)
	if err != nil {
		return fmt.Errorf("encode reply context origin: %w", err)
	}
	result, err := db.ExecContext(ctx, `
		INSERT OR IGNORE INTO reply_contexts (
			reply_context_id, run_id, request_event_id, requester_flow_id,
			request_output_pin, reply_input_pin, provider_flow_id,
			provider_input_pin, provider_output_pin, origin_route,
			request_correlation_id, correlation_key, state,
			accepted_reply_event_id, created_at, updated_at, terminal_at
		)
		VALUES (?, NULLIF(?, ''), ?, ?, ?, ?, ?, ?, ?, ?, ?, NULLIF(?, ''), ?, NULLIF(?, ''), ?, ?, ?)
	`, record.ID, record.RunID, record.RequestEventID, record.RequesterFlowID,
		record.RequestOutputPin, record.ReplyInputPin, record.ProviderFlowID,
		record.ProviderInputPin, record.ProviderOutputPin, string(origin),
		record.RequestCorrelationID, record.CorrelationKey, string(record.State),
		record.AcceptedReplyEventID, record.CreatedAt, record.UpdatedAt, record.TerminalAt)
	if err != nil {
		return fmt.Errorf("create sqlite reply context: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("create sqlite reply context rows: %w", err)
	}
	if rows == 1 {
		return nil
	}
	existing, loadErr := loadSQLiteReplyContext(ctx, db, record.ID)
	return resolveReplyContextCreateConflict(record, existing, loadErr)
}

func resolveReplyContextCreateConflict(record, existing runtimereplycontext.Record, loadErr error) error {
	if loadErr == nil && existing.SameIdentity(record) {
		return nil
	}
	if loadErr != nil && !errors.Is(loadErr, runtimereplycontext.ErrNotFound) {
		return fmt.Errorf("load conflicting reply context: %w", loadErr)
	}
	record = record.Normalized()
	return fmt.Errorf(
		"reply context correlation collision for flow %q origin %q and correlation %q; use a unique carried correlation value for each in-flight request",
		record.RequesterFlowID,
		record.Origin.FlowInstance,
		record.RequestCorrelationID,
	)
}

func (s *PostgresStore) LoadReplyContext(ctx context.Context, id string) (runtimereplycontext.Record, error) {
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		return loadPostgresReplyContext(ctx, tx, id, false)
	}
	return loadPostgresReplyContext(ctx, s.DB, id, false)
}

func loadPostgresReplyContext(ctx context.Context, db replyContextSQL, id string, forUpdate bool) (runtimereplycontext.Record, error) {
	query := postgresReplyContextSelect + ` WHERE reply_context_id = $1`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	return scanReplyContext(db.QueryRowContext(ctx, query, strings.TrimSpace(id)))
}

func (s *SQLiteRuntimeStore) LoadReplyContext(ctx context.Context, id string) (runtimereplycontext.Record, error) {
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		return loadSQLiteReplyContext(ctx, tx, id)
	}
	return loadSQLiteReplyContext(ctx, s.DB, id)
}

func loadSQLiteReplyContext(ctx context.Context, db replyContextSQL, id string) (runtimereplycontext.Record, error) {
	return scanReplyContext(db.QueryRowContext(ctx, sqliteReplyContextSelect+` WHERE reply_context_id = ?`, strings.TrimSpace(id)))
}

func (s *PostgresStore) ClaimReplyContext(ctx context.Context, id, replyEventID string) (runtimereplycontext.Record, runtimereplycontext.ClaimOutcome, error) {
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		return claimPostgresReplyContext(ctx, tx, id, replyEventID)
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return runtimereplycontext.Record{}, "", err
	}
	defer func() { _ = tx.Rollback() }()
	record, outcome, err := claimPostgresReplyContext(ctx, tx, id, replyEventID)
	if err != nil {
		return runtimereplycontext.Record{}, "", err
	}
	if _, err := runforkrevision.CaptureCurrentTransaction(ctx, tx); err != nil {
		return runtimereplycontext.Record{}, "", err
	}
	if err := tx.Commit(); err != nil {
		return runtimereplycontext.Record{}, "", err
	}
	return record, outcome, nil
}

func claimPostgresReplyContext(ctx context.Context, tx *sql.Tx, id, replyEventID string) (runtimereplycontext.Record, runtimereplycontext.ClaimOutcome, error) {
	record, err := loadPostgresReplyContext(ctx, tx, id, true)
	if err != nil {
		return runtimereplycontext.Record{}, "", err
	}
	return claimLoadedReplyContextTx(ctx, tx, record, replyEventID, true)
}

func (s *SQLiteRuntimeStore) ClaimReplyContext(ctx context.Context, id, replyEventID string) (runtimereplycontext.Record, runtimereplycontext.ClaimOutcome, error) {
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		record, err := loadSQLiteReplyContext(ctx, tx, id)
		if err != nil {
			return runtimereplycontext.Record{}, "", err
		}
		return claimLoadedReplyContextTx(ctx, tx, record, replyEventID, false)
	}
	var record runtimereplycontext.Record
	var outcome runtimereplycontext.ClaimOutcome
	err := s.runRuntimeMutation(ctx, "sqlite reply context claim", func(txctx context.Context, tx *sql.Tx) error {
		loaded, err := loadSQLiteReplyContext(txctx, tx, id)
		if err != nil {
			return err
		}
		record, outcome, err = claimLoadedReplyContextTx(txctx, tx, loaded, replyEventID, false)
		return err
	})
	return record, outcome, err
}

func claimLoadedReplyContextTx(ctx context.Context, db replyContextSQL, record runtimereplycontext.Record, replyEventID string, postgres bool) (runtimereplycontext.Record, runtimereplycontext.ClaimOutcome, error) {
	replyEventID = strings.TrimSpace(replyEventID)
	if replyEventID == "" {
		return runtimereplycontext.Record{}, "", fmt.Errorf("reply event id is required")
	}
	if record.State == runtimereplycontext.StateTerminal {
		if record.AcceptedReplyEventID == replyEventID {
			return record, runtimereplycontext.ClaimIdempotent, nil
		}
		return record, runtimereplycontext.ClaimTerminal, nil
	}
	existsQuery := `SELECT EXISTS (SELECT 1 FROM events WHERE event_id = ?)`
	if postgres {
		existsQuery = `SELECT EXISTS (SELECT 1 FROM events WHERE event_id = $1::uuid)`
	}
	var replyEventPersisted bool
	if err := db.QueryRowContext(ctx, existsQuery, replyEventID).Scan(&replyEventPersisted); err != nil {
		return runtimereplycontext.Record{}, "", fmt.Errorf("verify reply event %s persistence before claim: %w", replyEventID, err)
	}
	if !replyEventPersisted {
		return runtimereplycontext.Record{}, "", fmt.Errorf("reply event %s must be persisted in the reply-context mutation before terminal claim", replyEventID)
	}
	now := time.Now().UTC()
	query := `UPDATE reply_contexts SET state = ?, accepted_reply_event_id = ?, terminal_at = ?, updated_at = ? WHERE reply_context_id = ? AND state = 'open'`
	args := []any{string(runtimereplycontext.StateTerminal), replyEventID, now, now, record.ID}
	if postgres {
		query = `UPDATE reply_contexts SET state = $1, accepted_reply_event_id = $2::uuid, terminal_at = $3, updated_at = $4 WHERE reply_context_id = $5 AND state = 'open'`
	}
	result, err := db.ExecContext(ctx, query, args...)
	if err != nil {
		return runtimereplycontext.Record{}, "", fmt.Errorf("claim reply context %s for reply event %s: %w", record.ID, replyEventID, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return runtimereplycontext.Record{}, "", fmt.Errorf("claim reply context rows: %w", err)
	}
	if rows != 1 {
		return runtimereplycontext.Record{}, "", fmt.Errorf("reply context claim lost atomic update")
	}
	record.State = runtimereplycontext.StateTerminal
	record.AcceptedReplyEventID = replyEventID
	record.TerminalAt = &now
	record.UpdatedAt = now
	return record.Normalized(), runtimereplycontext.ClaimAccepted, nil
}

const postgresReplyContextSelect = `
	SELECT reply_context_id, COALESCE(run_id::text, ''), request_event_id::text,
	       requester_flow_id, request_output_pin, reply_input_pin,
	       provider_flow_id, provider_input_pin, provider_output_pin,
	       origin_route, request_correlation_id, COALESCE(correlation_key, ''),
	       state, COALESCE(accepted_reply_event_id::text, ''),
	       created_at, updated_at, terminal_at
	FROM reply_contexts`

const sqliteReplyContextSelect = `
	SELECT reply_context_id, COALESCE(run_id, ''), request_event_id,
	       requester_flow_id, request_output_pin, reply_input_pin,
	       provider_flow_id, provider_input_pin, provider_output_pin,
	       origin_route, request_correlation_id, COALESCE(correlation_key, ''),
	       state, COALESCE(accepted_reply_event_id, ''),
	       created_at, updated_at, terminal_at
	FROM reply_contexts`

type replyContextRowScanner interface {
	Scan(...any) error
}

func scanReplyContext(row replyContextRowScanner) (runtimereplycontext.Record, error) {
	var record runtimereplycontext.Record
	var originJSON []byte
	var createdAtRaw, updatedAtRaw, terminalAtRaw any
	if err := row.Scan(
		&record.ID, &record.RunID, &record.RequestEventID,
		&record.RequesterFlowID, &record.RequestOutputPin, &record.ReplyInputPin,
		&record.ProviderFlowID, &record.ProviderInputPin, &record.ProviderOutputPin,
		&originJSON, &record.RequestCorrelationID, &record.CorrelationKey,
		&record.State, &record.AcceptedReplyEventID,
		&createdAtRaw, &updatedAtRaw, &terminalAtRaw,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return runtimereplycontext.Record{}, runtimereplycontext.ErrNotFound
		}
		return runtimereplycontext.Record{}, fmt.Errorf("load reply context: %w", err)
	}
	if err := json.Unmarshal(originJSON, &record.Origin); err != nil {
		return runtimereplycontext.Record{}, fmt.Errorf("decode reply context origin: %w", err)
	}
	createdAt, ok, err := sqliteTimeValue(createdAtRaw)
	if err != nil {
		return runtimereplycontext.Record{}, fmt.Errorf("decode reply context created_at: %w", err)
	}
	if !ok {
		return runtimereplycontext.Record{}, fmt.Errorf("decode reply context created_at: value is required")
	}
	updatedAt, ok, err := sqliteTimeValue(updatedAtRaw)
	if err != nil {
		return runtimereplycontext.Record{}, fmt.Errorf("decode reply context updated_at: %w", err)
	}
	if !ok {
		return runtimereplycontext.Record{}, fmt.Errorf("decode reply context updated_at: value is required")
	}
	record.CreatedAt = createdAt
	record.UpdatedAt = updatedAt
	if terminalAt, ok, err := sqliteTimeValue(terminalAtRaw); err != nil {
		return runtimereplycontext.Record{}, fmt.Errorf("decode reply context terminal_at: %w", err)
	} else if ok {
		record.TerminalAt = &terminalAt
	}
	record = record.Normalized()
	if err := record.Validate(); err != nil {
		return runtimereplycontext.Record{}, fmt.Errorf("invalid persisted reply context: %w", err)
	}
	return record, nil
}
