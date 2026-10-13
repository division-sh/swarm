package replycontextstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	runtimereplycontext "github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
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

type ReplyContextStorageCounts struct {
	Total, Accepted, DistinctRequests, DistinctReplies int
}

type FirstReplyContextStorage struct {
	ContextID, RequestEventID, AcceptedReplyEventID string
}

func ReadReplyContextRecords(ctx context.Context, tx *sql.Tx, run string, postgres bool) ([]runtimereplycontext.Record, error) {
	query := sqliteReplyContextSelect
	if postgres {
		query = postgresReplyContextSelect
	}
	rows, err := tx.QueryContext(ctx, query+` WHERE run_id=$1 ORDER BY reply_context_id`, run)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var records []runtimereplycontext.Record
	for rows.Next() {
		record, err := scanReplyContext(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return records, nil
}

func ReadReplyContextRequestIDs(ctx context.Context, tx *sql.Tx, run string) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `SELECT CAST(request_event_id AS TEXT) FROM reply_contexts WHERE run_id=$1 ORDER BY request_event_id`, run)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return ids, nil
}

func ReadFirstReplyContextStorage(ctx context.Context, tx *sql.Tx, run string) (FirstReplyContextStorage, error) {
	var row FirstReplyContextStorage
	err := tx.QueryRowContext(ctx, `SELECT reply_context_id,request_event_id,accepted_reply_event_id
FROM reply_contexts WHERE run_id=$1 ORDER BY reply_context_id LIMIT 1`, run).
		Scan(&row.ContextID, &row.RequestEventID, &row.AcceptedReplyEventID)
	if err != nil {
		return FirstReplyContextStorage{}, err
	}
	return row, nil
}

func ReadReplyContextStorageCounts(ctx context.Context, tx *sql.Tx, run string) (ReplyContextStorageCounts, error) {
	var row ReplyContextStorageCounts
	err := tx.QueryRowContext(ctx, `SELECT count(*),count(accepted_reply_event_id),count(DISTINCT request_event_id),count(DISTINCT accepted_reply_event_id)
FROM reply_contexts WHERE run_id=$1`, run).Scan(&row.Total, &row.Accepted, &row.DistinctRequests, &row.DistinctReplies)
	if err != nil {
		return ReplyContextStorageCounts{}, err
	}
	return row, nil
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
	if err := requireFreshReplyContext(record); err != nil {
		return err
	}
	result := mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, nil, func(txctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
		return struct{}{}, s.CreateWithinTransaction(txctx, attempt, record)
	})
	return result.Err()
}

func createPostgresReplyContext(ctx context.Context, db *sql.Tx, attempt *mutationprotocol.Attempt, record runtimereplycontext.Record) error {
	record = record.Normalized()
	if err := record.Validate(); err != nil {
		return err
	}
	if err := storerunstate.RequirePostgresActiveTx(ctx, db, record.RunID); err != nil {
		return fmt.Errorf("create reply context: %w", err)
	}
	origin, err := record.EncodeOrigin()
	if err != nil {
		return fmt.Errorf("encode reply context origin: %w", err)
	}
	result, err := insertReplyContextRow(ctx, db, record, origin, true)
	if err != nil {
		return fmt.Errorf("create reply context: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("create reply context rows: %w", err)
	}
	if rows == 1 {
		if err := requireEnclosingReplyContext(ctx, db, record, true); err != nil {
			return err
		}
		return attempt.AddFact(record.RunID, runforkrevision.FamilyReplyContexts, record.ID)
	}
	existing, loadErr := loadPostgresReplyContext(ctx, db, record.ID, false)
	if err := resolveReplyContextCreateConflict(record, existing, loadErr); err != nil {
		return err
	}
	return attempt.AddFact(existing.RunID, runforkrevision.FamilyReplyContexts, existing.ID)
}

func (s *ReplyPostgresOwner) CreateWithinTransaction(ctx context.Context, attempt *mutationprotocol.Attempt, record runtimereplycontext.Record) error {
	return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return createPostgresReplyContext(ctx, tx, attempt, record)
	})
}

func (s *ReplySQLiteOwner) CreateReplyContext(ctx context.Context, record runtimereplycontext.Record) error {
	if err := requireFreshReplyContext(record); err != nil {
		return err
	}
	result := mutationprotocol.RunSQLite(ctx, s.backend, "sqlite reply context create", mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, nil, func(txctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
		return struct{}{}, s.CreateWithinTransaction(txctx, attempt, record)
	})
	return result.Err()
}

func requireFreshReplyContext(record runtimereplycontext.Record) error {
	record = record.Normalized()
	if err := record.Validate(); err != nil {
		return err
	}
	if record.State != runtimereplycontext.StateOpen || record.SourceRunID != record.RunID || record.SourceRequestEventID != record.RequestEventID {
		return fmt.Errorf("fresh reply creation requires its own open physical request; projection belongs to fixed-cut materialization")
	}
	return nil
}

func createSQLiteReplyContextTx(ctx context.Context, db *sql.Tx, attempt *mutationprotocol.Attempt, record runtimereplycontext.Record) error {
	record = record.Normalized()
	if err := record.Validate(); err != nil {
		return err
	}
	if err := storerunstate.RequireSQLiteActiveTx(ctx, db, record.RunID); err != nil {
		return fmt.Errorf("create sqlite reply context: %w", err)
	}
	origin, err := record.EncodeOrigin()
	if err != nil {
		return fmt.Errorf("encode reply context origin: %w", err)
	}
	result, err := insertReplyContextRow(ctx, db, record, origin, false)
	if err != nil {
		return fmt.Errorf("create sqlite reply context: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("create sqlite reply context rows: %w", err)
	}
	if rows == 1 {
		if err := requireEnclosingReplyContext(ctx, db, record, false); err != nil {
			return err
		}
		return attempt.AddFact(record.RunID, runforkrevision.FamilyReplyContexts, record.ID)
	}
	existing, loadErr := loadSQLiteReplyContext(ctx, db, record.ID)
	if err := resolveReplyContextCreateConflict(record, existing, loadErr); err != nil {
		return err
	}
	return attempt.AddFact(existing.RunID, runforkrevision.FamilyReplyContexts, existing.ID)
}

func (s *ReplySQLiteOwner) CreateWithinTransaction(ctx context.Context, attempt *mutationprotocol.Attempt, record runtimereplycontext.Record) error {
	return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return createSQLiteReplyContextTx(ctx, tx, attempt, record)
	})
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

func (s *ReplyPostgresOwner) LoadWithinTransaction(ctx context.Context, tx *sql.Tx, id string) (runtimereplycontext.Record, error) {
	if err := ctx.Err(); err != nil {
		return runtimereplycontext.Record{}, err
	}
	if tx == nil {
		return runtimereplycontext.Record{}, fmt.Errorf("load postgres reply context: transaction is required")
	}
	return loadPostgresReplyContext(ctx, tx, id, false)
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

func (s *ReplySQLiteOwner) LoadWithinTransaction(ctx context.Context, tx *sql.Tx, id string) (runtimereplycontext.Record, error) {
	if err := ctx.Err(); err != nil {
		return runtimereplycontext.Record{}, err
	}
	if tx == nil {
		return runtimereplycontext.Record{}, fmt.Errorf("load sqlite reply context: transaction is required")
	}
	return loadSQLiteReplyContext(ctx, tx, id)
}

func loadSQLiteReplyContext(ctx context.Context, db replyContextSQL, id string) (runtimereplycontext.Record, error) {
	return scanReplyContext(db.QueryRowContext(ctx, sqliteReplyContextSelect+` WHERE reply_context_id = ?`, strings.TrimSpace(id)))
}

// Request replay consumes the projected open row, not creation idempotence.
// The lock remains held through publication so a concurrent reply cannot settle
// the obligation between this check and its request delivery.
func (s *ReplyPostgresOwner) RequireOpenWithinTransaction(ctx context.Context, attempt *mutationprotocol.Attempt, expected runtimereplycontext.Record) error {
	if attempt == nil {
		return fmt.Errorf("reply continuation requires its mutation attempt")
	}
	return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		actual, err := loadPostgresReplyContext(ctx, tx, expected.ID, true)
		if err != nil {
			return err
		}
		return requireOpenReplyContinuation(expected, actual)
	})
}

func (s *ReplySQLiteOwner) RequireOpenWithinTransaction(ctx context.Context, attempt *mutationprotocol.Attempt, expected runtimereplycontext.Record) error {
	if attempt == nil {
		return fmt.Errorf("reply continuation requires its mutation attempt")
	}
	return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		actual, err := loadSQLiteReplyContext(ctx, tx, expected.ID)
		if err != nil {
			return err
		}
		return requireOpenReplyContinuation(expected, actual)
	})
}

func requireOpenReplyContinuation(expected, actual runtimereplycontext.Record) error {
	if expected.State != runtimereplycontext.StateOpen || actual.State != runtimereplycontext.StateOpen {
		return fmt.Errorf("request continuation requires its existing open reply obligation")
	}
	return actual.ValidateContinuationFrom(expected, expected.CreatedAt)
}

func (s *ReplyPostgresOwner) RequireTerminalWithinTransaction(ctx context.Context, attempt *mutationprotocol.Attempt, expected runtimereplycontext.Record) error {
	if attempt == nil {
		return fmt.Errorf("retained return requires its mutation attempt")
	}
	return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		actual, err := loadPostgresReplyContext(ctx, tx, expected.ID, true)
		if err != nil {
			return err
		}
		return requireTerminalReplyContinuation(expected, actual)
	})
}

func (s *ReplySQLiteOwner) RequireTerminalWithinTransaction(ctx context.Context, attempt *mutationprotocol.Attempt, expected runtimereplycontext.Record) error {
	if attempt == nil {
		return fmt.Errorf("retained return requires its mutation attempt")
	}
	return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		actual, err := loadSQLiteReplyContext(ctx, tx, expected.ID)
		if err != nil {
			return err
		}
		return requireTerminalReplyContinuation(expected, actual)
	})
}

func requireTerminalReplyContinuation(expected, actual runtimereplycontext.Record) error {
	if expected.State != runtimereplycontext.StateTerminal || actual.State != runtimereplycontext.StateTerminal {
		return fmt.Errorf("retained return requires its existing terminal obligation")
	}
	return actual.ValidateContinuationFrom(expected, expected.CreatedAt)
}

func (s *ReplyPostgresOwner) ClaimReplyContext(ctx context.Context, id, replyEventID string) (runtimereplycontext.Record, runtimereplycontext.ClaimOutcome, error) {
	type claimResult struct {
		record  runtimereplycontext.Record
		outcome runtimereplycontext.ClaimOutcome
	}
	result := mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, nil, func(txctx context.Context, attempt *mutationprotocol.Attempt) (claimResult, error) {
		var value claimResult
		err := attempt.WithSQL(txctx, func(ctx context.Context, tx *sql.Tx) error {
			var err error
			value.record, value.outcome, err = claimPostgresReplyContext(ctx, tx, id, replyEventID)
			return err
		})
		if err != nil {
			return claimResult{}, err
		}
		return value, attempt.AddFact(value.record.RunID, runforkrevision.FamilyReplyContexts, value.record.ID)
	})
	value, ok := result.Value()
	if !ok {
		return runtimereplycontext.Record{}, "", result.Err()
	}
	return value.record, value.outcome, result.Err()
}

func claimPostgresReplyContext(ctx context.Context, tx *sql.Tx, id, replyEventID string) (runtimereplycontext.Record, runtimereplycontext.ClaimOutcome, error) {
	record, err := loadPostgresReplyContext(ctx, tx, id, true)
	if err != nil {
		return runtimereplycontext.Record{}, "", err
	}
	return claimLoadedReplyContextTx(ctx, tx, record, replyEventID, true)
}

func (s *ReplySQLiteOwner) ClaimReplyContext(ctx context.Context, id, replyEventID string) (runtimereplycontext.Record, runtimereplycontext.ClaimOutcome, error) {
	type claimResult struct {
		record  runtimereplycontext.Record
		outcome runtimereplycontext.ClaimOutcome
	}
	result := mutationprotocol.RunSQLite(ctx, s.backend, "sqlite reply context claim", mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, nil, func(txctx context.Context, attempt *mutationprotocol.Attempt) (claimResult, error) {
		var value claimResult
		err := attempt.WithSQL(txctx, func(ctx context.Context, tx *sql.Tx) error {
			loaded, err := loadSQLiteReplyContext(ctx, tx, id)
			if err != nil {
				return err
			}
			value.record, value.outcome, err = claimLoadedReplyContextTx(ctx, tx, loaded, replyEventID, false)
			return err
		})
		if err != nil {
			return claimResult{}, err
		}
		return value, attempt.AddFact(value.record.RunID, runforkrevision.FamilyReplyContexts, value.record.ID)
	})
	value, ok := result.Value()
	if !ok {
		return runtimereplycontext.Record{}, "", result.Err()
	}
	return value.record, value.outcome, result.Err()
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
	existsQuery := `SELECT EXISTS (SELECT 1 FROM events WHERE event_id = ? AND COALESCE(run_id, '') = ?)`
	if postgres {
		existsQuery = `SELECT EXISTS (SELECT 1 FROM events WHERE event_id = $1::uuid AND run_id IS NOT DISTINCT FROM NULLIF($2, '')::uuid)`
	}
	var replyEventPersisted bool
	if err := db.QueryRowContext(ctx, existsQuery, replyEventID, record.RunID).Scan(&replyEventPersisted); err != nil {
		return runtimereplycontext.Record{}, "", fmt.Errorf("verify reply event %s persistence before claim: %w", replyEventID, err)
	}
	if !replyEventPersisted {
		return runtimereplycontext.Record{}, "", fmt.Errorf("reply event %s must be persisted in the reply context's run before terminal claim", replyEventID)
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

func (s *ReplyPostgresOwner) ClaimWithinTransaction(ctx context.Context, attempt *mutationprotocol.Attempt, command runtimereplycontext.ClaimCommand) error {
	command = command.Normalized()
	if err := command.Validate(); err != nil {
		return err
	}
	var loaded runtimereplycontext.Record
	if err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		loaded, err = loadPostgresReplyContext(ctx, tx, command.Expected.ID, true)
		if err != nil {
			return err
		}
		return commitExpectedReplyContextClaim(ctx, tx, loaded, command, true)
	}); err != nil {
		return err
	}
	return attempt.AddFact(loaded.RunID, runforkrevision.FamilyReplyContexts, loaded.ID)
}

func (s *ReplySQLiteOwner) ClaimWithinTransaction(ctx context.Context, attempt *mutationprotocol.Attempt, command runtimereplycontext.ClaimCommand) error {
	command = command.Normalized()
	if err := command.Validate(); err != nil {
		return err
	}
	var loaded runtimereplycontext.Record
	if err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		loaded, err = loadSQLiteReplyContext(ctx, tx, command.Expected.ID)
		if err != nil {
			return err
		}
		return commitExpectedReplyContextClaim(ctx, tx, loaded, command, false)
	}); err != nil {
		return err
	}
	return attempt.AddFact(loaded.RunID, runforkrevision.FamilyReplyContexts, loaded.ID)
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
	       created_at, updated_at, terminal_at,
	       COALESCE(source_run_id::text, ''), source_request_event_id::text,
	       COALESCE(enclosing_reply_context_id, '')
	FROM reply_contexts`

const sqliteReplyContextSelect = `
	SELECT reply_context_id, COALESCE(run_id, ''), request_event_id,
	       requester_flow_id, request_output_pin, reply_input_pin,
	       provider_flow_id, provider_input_pin, provider_output_pin,
	       origin_route, request_correlation_id, COALESCE(correlation_key, ''),
	       state, COALESCE(accepted_reply_event_id, ''),
	       created_at, updated_at, terminal_at,
	       COALESCE(source_run_id, ''), source_request_event_id,
	       COALESCE(enclosing_reply_context_id, '')
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
		&record.SourceRunID, &record.SourceRequestEventID, &record.EnclosingReplyContextID,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return runtimereplycontext.Record{}, runtimereplycontext.ErrNotFound
		}
		return runtimereplycontext.Record{}, fmt.Errorf("load reply context: %w", err)
	}
	if err := record.DecodeOrigin(originJSON); err != nil {
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
