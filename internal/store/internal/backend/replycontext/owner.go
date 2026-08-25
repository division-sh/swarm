package replycontextstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	runtimereplycontext "github.com/division-sh/swarm/internal/runtime/replycontext"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	runforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	storerunstate "github.com/division-sh/swarm/internal/store/internal/backend/runstate"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
)

type replyContextSQL interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type ReplyPostgresOwner struct {
	backend *postgresbackend.Backend
}

type ReplySQLiteOwner struct {
	backend *sqlitebackend.Backend
}

func NewPostgres(backend *postgresbackend.Backend) (*ReplyPostgresOwner, error) {
	if backend == nil || !backend.Valid() {
		return nil, fmt.Errorf("postgres reply-context backend is required")
	}
	return &ReplyPostgresOwner{backend: backend}, nil
}

func NewSQLite(backend *sqlitebackend.Backend) (*ReplySQLiteOwner, error) {
	if backend == nil || !backend.Valid() {
		return nil, fmt.Errorf("sqlite reply-context backend is required")
	}
	return &ReplySQLiteOwner{backend: backend}, nil
}

var _ runtimereplycontext.Store = (*ReplyPostgresOwner)(nil)
var _ runtimereplycontext.Store = (*ReplySQLiteOwner)(nil)

func (s *ReplyPostgresOwner) CreateReplyContext(ctx context.Context, record runtimereplycontext.Record) error {
	return s.backend.RunTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		if err := createPostgresReplyContext(txctx, tx, record); err != nil {
			return err
		}
		effects := runforkrevision.NewEffects()
		if err := effects.Add(record.RunID, runforkrevision.FamilyReplyContexts); err != nil {
			return err
		}
		_, err := runforkrevision.FinalizePostgres(txctx, tx, effects)
		return err
	})
}

func createPostgresReplyContext(ctx context.Context, db *sql.Tx, record runtimereplycontext.Record) error {
	record = record.Normalized()
	if err := record.Validate(); err != nil {
		return err
	}
	if err := storerunstate.RequirePostgresActiveTx(ctx, db, record.RunID); err != nil {
		return fmt.Errorf("create reply context: %w", err)
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

func (s *ReplyPostgresOwner) CreateWithinTransaction(ctx context.Context, tx *sql.Tx, record runtimereplycontext.Record) error {
	return createPostgresReplyContext(ctx, tx, record)
}

func (s *ReplySQLiteOwner) CreateReplyContext(ctx context.Context, record runtimereplycontext.Record) error {
	return s.backend.RunTransaction(ctx, "sqlite reply context create", func(txctx context.Context, tx *sql.Tx) error {
		if err := createSQLiteReplyContextTx(txctx, tx, record); err != nil {
			return err
		}
		effects := runforkrevision.NewEffects()
		if err := effects.Add(record.RunID, runforkrevision.FamilyReplyContexts); err != nil {
			return err
		}
		_, err := runforkrevision.FinalizeSQLite(txctx, tx, effects)
		return err
	})
}

func createSQLiteReplyContextTx(ctx context.Context, db *sql.Tx, record runtimereplycontext.Record) error {
	record = record.Normalized()
	if err := record.Validate(); err != nil {
		return err
	}
	if err := storerunstate.RequireSQLiteActiveTx(ctx, db, record.RunID); err != nil {
		return fmt.Errorf("create sqlite reply context: %w", err)
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

func (s *ReplySQLiteOwner) CreateWithinTransaction(ctx context.Context, tx *sql.Tx, record runtimereplycontext.Record) error {
	return createSQLiteReplyContextTx(ctx, tx, record)
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

func (s *ReplyPostgresOwner) LoadReplyContext(ctx context.Context, id string) (runtimereplycontext.Record, error) {
	return loadPostgresReplyContext(ctx, s.backend, id, false)
}

func loadPostgresReplyContext(ctx context.Context, db replyContextSQL, id string, forUpdate bool) (runtimereplycontext.Record, error) {
	query := postgresReplyContextSelect + ` WHERE reply_context_id = $1`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	return scanReplyContext(db.QueryRowContext(ctx, query, strings.TrimSpace(id)))
}

func (s *ReplySQLiteOwner) LoadReplyContext(ctx context.Context, id string) (runtimereplycontext.Record, error) {
	return loadSQLiteReplyContext(ctx, s.backend, id)
}

func loadSQLiteReplyContext(ctx context.Context, db replyContextSQL, id string) (runtimereplycontext.Record, error) {
	return scanReplyContext(db.QueryRowContext(ctx, sqliteReplyContextSelect+` WHERE reply_context_id = ?`, strings.TrimSpace(id)))
}

func (s *ReplyPostgresOwner) ClaimReplyContext(ctx context.Context, id, replyEventID string) (runtimereplycontext.Record, runtimereplycontext.ClaimOutcome, error) {
	var record runtimereplycontext.Record
	var outcome runtimereplycontext.ClaimOutcome
	err := s.backend.RunTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		record, outcome, err = claimPostgresReplyContext(txctx, tx, id, replyEventID)
		if err != nil {
			return err
		}
		effects := runforkrevision.NewEffects()
		if err := effects.Add(record.RunID, runforkrevision.FamilyReplyContexts); err != nil {
			return err
		}
		_, err = runforkrevision.FinalizePostgres(txctx, tx, effects)
		return err
	})
	return record, outcome, err
}

func claimPostgresReplyContext(ctx context.Context, tx *sql.Tx, id, replyEventID string) (runtimereplycontext.Record, runtimereplycontext.ClaimOutcome, error) {
	record, err := loadPostgresReplyContext(ctx, tx, id, true)
	if err != nil {
		return runtimereplycontext.Record{}, "", err
	}
	return claimLoadedReplyContextTx(ctx, tx, record, replyEventID, true)
}

func (s *ReplySQLiteOwner) ClaimReplyContext(ctx context.Context, id, replyEventID string) (runtimereplycontext.Record, runtimereplycontext.ClaimOutcome, error) {
	var record runtimereplycontext.Record
	var outcome runtimereplycontext.ClaimOutcome
	err := s.backend.RunTransaction(ctx, "sqlite reply context claim", func(txctx context.Context, tx *sql.Tx) error {
		loaded, err := loadSQLiteReplyContext(txctx, tx, id)
		if err != nil {
			return err
		}
		record, outcome, err = claimLoadedReplyContextTx(txctx, tx, loaded, replyEventID, false)
		if err != nil {
			return err
		}
		effects := runforkrevision.NewEffects()
		if err := effects.Add(record.RunID, runforkrevision.FamilyReplyContexts); err != nil {
			return err
		}
		_, err = runforkrevision.FinalizeSQLite(txctx, tx, effects)
		return err
	})
	return record, outcome, err
}

func claimLoadedReplyContextTx(ctx context.Context, db *sql.Tx, record runtimereplycontext.Record, replyEventID string, postgres bool) (runtimereplycontext.Record, runtimereplycontext.ClaimOutcome, error) {
	replyEventID = strings.TrimSpace(replyEventID)
	if replyEventID == "" {
		return runtimereplycontext.Record{}, "", fmt.Errorf("reply event id is required")
	}
	var activeErr error
	if postgres {
		activeErr = storerunstate.RequirePostgresActiveTx(ctx, db, record.RunID)
	} else {
		activeErr = storerunstate.RequireSQLiteActiveTx(ctx, db, record.RunID)
	}
	if activeErr != nil {
		return runtimereplycontext.Record{}, "", fmt.Errorf("claim reply context: %w", activeErr)
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

func (s *ReplyPostgresOwner) ClaimWithinTransaction(ctx context.Context, tx *sql.Tx, command runtimereplycontext.ClaimCommand) error {
	command = command.Normalized()
	if err := command.Validate(); err != nil {
		return err
	}
	loaded, err := loadPostgresReplyContext(ctx, tx, command.Expected.ID, true)
	if err != nil {
		return err
	}
	return commitExpectedReplyContextClaim(ctx, tx, loaded, command, true)
}

func (s *ReplySQLiteOwner) ClaimWithinTransaction(ctx context.Context, tx *sql.Tx, command runtimereplycontext.ClaimCommand) error {
	command = command.Normalized()
	if err := command.Validate(); err != nil {
		return err
	}
	loaded, err := loadSQLiteReplyContext(ctx, tx, command.Expected.ID)
	if err != nil {
		return err
	}
	return commitExpectedReplyContextClaim(ctx, tx, loaded, command, false)
}

func commitExpectedReplyContextClaim(ctx context.Context, tx *sql.Tx, loaded runtimereplycontext.Record, command runtimereplycontext.ClaimCommand, postgres bool) error {
	expected := command.Expected.Normalized()
	loaded = loaded.Normalized()
	if !loaded.SameIdentity(expected) || loaded.State != expected.State || loaded.AcceptedReplyEventID != expected.AcceptedReplyEventID {
		return fmt.Errorf("reply context %s changed after publication planning", expected.ID)
	}
	_, outcome, err := claimLoadedReplyContextTx(ctx, tx, loaded, command.ReplyEventID, postgres)
	if err != nil {
		return err
	}
	switch outcome {
	case runtimereplycontext.ClaimAccepted, runtimereplycontext.ClaimIdempotent:
		return nil
	case runtimereplycontext.ClaimTerminal:
		return fmt.Errorf("reply context %s was already claimed by event %s", expected.ID, loaded.AcceptedReplyEventID)
	default:
		return fmt.Errorf("reply context %s returned invalid claim outcome %q", expected.ID, outcome)
	}
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

func sqliteTimeValue(raw any) (time.Time, bool, error) {
	switch value := raw.(type) {
	case nil:
		return time.Time{}, false, nil
	case time.Time:
		if value.IsZero() {
			return time.Time{}, false, nil
		}
		return value.UTC(), true, nil
	case string:
		return parseSQLiteTimeString(value)
	case []byte:
		return parseSQLiteTimeString(string(value))
	default:
		return time.Time{}, false, fmt.Errorf("unsupported sqlite time value %T", raw)
	}
}

func parseSQLiteTimeString(raw string) (time.Time, bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, false, nil
	}
	formats := []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999 -0700 MST",
		"2006-01-02 15:04:05.999999 -0700 MST",
		"2006-01-02 15:04:05 -0700 MST",
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05-07:00",
		"2006-01-02 15:04:05Z07:00",
		"2006-01-02 15:04:05",
	}
	var lastErr error
	for _, layout := range formats {
		parsed, err := time.Parse(layout, raw)
		if err == nil {
			return parsed.UTC(), true, nil
		}
		lastErr = err
	}
	return time.Time{}, false, fmt.Errorf("parse sqlite time %q: %w", raw, lastErr)
}
