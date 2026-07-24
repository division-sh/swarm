package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
	"github.com/google/uuid"
)

type pipelineClaimState struct {
	claim runtimepipelineobligation.Claim
}

type postgresPipelineClaimRegistry struct {
	mu                          sync.Mutex
	issuer                      *runtimepipelineobligation.ClaimIssuer
	claims                      map[string]*pipelineClaimState
	testBeforeClaimRegistryLock func()
	testAfterParentClaimScan    func()
}

var postgresPipelineClaimRegistries sync.Map

var (
	_ runtimepipelineobligation.Store = (*postgresPipelineObligationStore)(nil)
	_ runtimepipelineobligation.Store = (*sqlitePipelineObligationStore)(nil)
)

type postgresPipelineObligationStore struct{ *PostgresStore }
type sqlitePipelineObligationStore struct{ *SQLiteRuntimeStore }

const pipelineCandidatePageSize = 32

type pipelineCandidate struct {
	eventID       string
	attemptCount  int
	nextAttemptAt time.Time
	createdAt     time.Time
}

func (s *PostgresStore) PipelineObligations() runtimepipelineobligation.Store {
	return &postgresPipelineObligationStore{PostgresStore: s}
}

func (s *SQLiteRuntimeStore) PipelineObligations() runtimepipelineobligation.Store {
	return &sqlitePipelineObligationStore{SQLiteRuntimeStore: s}
}

func (s *PostgresStore) postgresPipelineClaims() *postgresPipelineClaimRegistry {
	if s == nil || s.DB == nil {
		return nil
	}
	created := &postgresPipelineClaimRegistry{
		issuer: runtimepipelineobligation.NewClaimIssuer(),
		claims: map[string]*pipelineClaimState{},
	}
	actual, _ := postgresPipelineClaimRegistries.LoadOrStore(s.DB, created)
	return actual.(*postgresPipelineClaimRegistry)
}

func (s *SQLiteRuntimeStore) pipelineClaimOwner() (*runtimepipelineobligation.ClaimIssuer, map[string]*pipelineClaimState) {
	if s.pipelineClaimIssuer == nil {
		s.pipelineClaimIssuer = runtimepipelineobligation.NewClaimIssuer()
	}
	if s.pipelineClaims == nil {
		s.pipelineClaims = map[string]*pipelineClaimState{}
	}
	return s.pipelineClaimIssuer, s.pipelineClaims
}

func (s *PostgresStore) requirePipelinePublicationClaimTx(
	_ context.Context,
	tx *sql.Tx,
	eventID string,
	claim runtimepipelineobligation.Claim,
) error {
	if tx == nil {
		return errors.New("PostgreSQL event commit transaction is required")
	}
	state, err := s.postgresPipelineClaimState(claim)
	if err != nil {
		return err
	}
	if state.claim.EventID() != strings.TrimSpace(eventID) || state.claim.Purpose() != runtimepipelineobligation.PurposePublication {
		return runtimepipelineobligation.ErrWrongClaim
	}
	return nil
}

func (s *SQLiteRuntimeStore) requirePipelinePublicationClaimTx(
	_ context.Context,
	tx *sql.Tx,
	eventID string,
	claim runtimepipelineobligation.Claim,
) error {
	if tx == nil {
		return errors.New("SQLite event commit transaction is required")
	}
	state, err := s.sqlitePipelineClaimState(claim)
	if err != nil {
		return err
	}
	if state.claim.EventID() != strings.TrimSpace(eventID) || state.claim.Purpose() != runtimepipelineobligation.PurposePublication {
		return runtimepipelineobligation.ErrWrongClaim
	}
	return nil
}

func (s *postgresPipelineObligationStore) ClaimPublication(ctx context.Context, eventID string) (runtimepipelineobligation.Claim, error) {
	return s.claimPostgresPipelineEvent(ctx, eventID, runtimepipelineobligation.PurposePublication)
}

func (s *sqlitePipelineObligationStore) ClaimPublication(ctx context.Context, eventID string) (runtimepipelineobligation.Claim, error) {
	return s.claimSQLitePipelineEvent(ctx, eventID, runtimepipelineobligation.PurposePublication)
}

func (s *postgresPipelineObligationStore) ClaimEvent(ctx context.Context, eventID string, purpose runtimepipelineobligation.Purpose) (runtimepipelineobligation.ClaimedWork, error) {
	claim, err := s.claimPostgresPipelineEvent(ctx, eventID, purpose)
	if err != nil {
		return runtimepipelineobligation.ClaimedWork{}, err
	}
	work, err := s.loadPostgresClaimedPipelineWork(ctx, claim)
	if err != nil {
		_ = s.Release(context.WithoutCancel(ctx), claim)
		return runtimepipelineobligation.ClaimedWork{}, err
	}
	return work, nil
}

func (s *sqlitePipelineObligationStore) ClaimEvent(ctx context.Context, eventID string, purpose runtimepipelineobligation.Purpose) (runtimepipelineobligation.ClaimedWork, error) {
	claim, err := s.claimSQLitePipelineEvent(ctx, eventID, purpose)
	if err != nil {
		return runtimepipelineobligation.ClaimedWork{}, err
	}
	work, err := s.loadSQLiteClaimedPipelineWork(ctx, claim)
	if err != nil {
		_ = s.Release(context.WithoutCancel(ctx), claim)
		return runtimepipelineobligation.ClaimedWork{}, err
	}
	return work, nil
}

func (s *postgresPipelineObligationStore) ClaimNext(ctx context.Context, query runtimepipelineobligation.ClaimQuery) (runtimepipelineobligation.ClaimedWork, bool, error) {
	if err := query.Validate(); err != nil {
		return runtimepipelineobligation.ClaimedWork{}, false, err
	}
	var after *pipelineCandidate
	for {
		candidates, err := s.postgresPipelineCandidates(ctx, query, after, pipelineCandidatePageSize)
		if err != nil {
			return runtimepipelineobligation.ClaimedWork{}, false, err
		}
		for i := range candidates {
			candidate := candidates[i]
			after = &candidate
			claim, err := s.claimPostgresPipelineEvent(ctx, candidate.eventID, query.Purpose)
			if errors.Is(err, runtimepipelineobligation.ErrBusy) || errors.Is(err, runtimepipelineobligation.ErrIneligible) {
				continue
			}
			if err != nil {
				return runtimepipelineobligation.ClaimedWork{}, false, err
			}
			work, err := s.loadPostgresClaimedPipelineWork(ctx, claim)
			if disposition, corrupt := corruptPipelineScopeDisposition(candidate.eventID, err); corrupt {
				if settleErr := s.Settle(ctx, claim, disposition); settleErr != nil {
					return runtimepipelineobligation.ClaimedWork{}, false, errors.Join(err, settleErr)
				}
				continue
			}
			if err != nil {
				_ = s.Release(context.WithoutCancel(ctx), claim)
				return runtimepipelineobligation.ClaimedWork{}, false, err
			}
			return work, true, nil
		}
		if len(candidates) < pipelineCandidatePageSize {
			return runtimepipelineobligation.ClaimedWork{}, false, nil
		}
	}
}

func (s *sqlitePipelineObligationStore) ClaimNext(ctx context.Context, query runtimepipelineobligation.ClaimQuery) (runtimepipelineobligation.ClaimedWork, bool, error) {
	if err := query.Validate(); err != nil {
		return runtimepipelineobligation.ClaimedWork{}, false, err
	}
	var after *pipelineCandidate
	for {
		candidates, err := s.sqlitePipelineCandidates(ctx, query, after, pipelineCandidatePageSize)
		if err != nil {
			return runtimepipelineobligation.ClaimedWork{}, false, err
		}
		for i := range candidates {
			candidate := candidates[i]
			after = &candidate
			claim, err := s.claimSQLitePipelineEvent(ctx, candidate.eventID, query.Purpose)
			if errors.Is(err, runtimepipelineobligation.ErrBusy) || errors.Is(err, runtimepipelineobligation.ErrIneligible) {
				continue
			}
			if err != nil {
				return runtimepipelineobligation.ClaimedWork{}, false, err
			}
			work, err := s.loadSQLiteClaimedPipelineWork(ctx, claim)
			if disposition, corrupt := corruptPipelineScopeDisposition(candidate.eventID, err); corrupt {
				if settleErr := s.Settle(ctx, claim, disposition); settleErr != nil {
					return runtimepipelineobligation.ClaimedWork{}, false, errors.Join(err, settleErr)
				}
				continue
			}
			if err != nil {
				_ = s.Release(context.WithoutCancel(ctx), claim)
				return runtimepipelineobligation.ClaimedWork{}, false, err
			}
			return work, true, nil
		}
		if len(candidates) < pipelineCandidatePageSize {
			return runtimepipelineobligation.ClaimedWork{}, false, nil
		}
	}
}

func corruptPipelineScopeDisposition(eventID string, err error) (runtimepipelineobligation.Disposition, bool) {
	code := ""
	switch {
	case errors.Is(err, runtimepipelineobligation.ErrMissingScope):
		code = "committed_pipeline_scope_missing"
	case errors.Is(err, runtimepipelineobligation.ErrInvalidScope):
		code = "committed_pipeline_scope_invalid"
	default:
		return runtimepipelineobligation.Disposition{}, false
	}
	failureErr := runtimefailures.New(
		runtimefailures.ClassSchemaInvalid,
		code,
		"pipeline-obligation-store",
		"hydrate_claimed_work",
		map[string]any{"event_id": strings.TrimSpace(eventID)},
	)
	failure, _ := runtimefailures.EnvelopeFromError(failureErr)
	return runtimepipelineobligation.Quarantined(code, &failure), true
}

func (s *postgresPipelineObligationStore) MarkDecisionProcessed(ctx context.Context, claim runtimepipelineobligation.Claim) error {
	if claim.Purpose() != runtimepipelineobligation.PurposePublication && claim.Purpose() != runtimepipelineobligation.PurposeDecisionRoute {
		return runtimepipelineobligation.ErrWrongClaim
	}
	lease, err := s.acquirePostgresPipelineMutationLease(ctx, claim)
	if err != nil {
		return err
	}
	defer func() { _ = lease.Release(context.WithoutCancel(ctx)) }()
	if _, err := s.postgresPipelineClaimState(claim); err != nil {
		return err
	}
	tx, err := lease.conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := markDecisionRouteProcessedTx(ctx, tx, claim.EventID(), true, time.Now().UTC()); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (s *sqlitePipelineObligationStore) MarkDecisionProcessed(ctx context.Context, claim runtimepipelineobligation.Claim) error {
	if claim.Purpose() != runtimepipelineobligation.PurposePublication && claim.Purpose() != runtimepipelineobligation.PurposeDecisionRoute {
		return runtimepipelineobligation.ErrWrongClaim
	}
	return s.runRuntimeMutation(ctx, "mark sqlite decision route processed", func(txctx context.Context, tx *sql.Tx) error {
		if _, err := s.sqlitePipelineClaimState(claim); err != nil {
			return err
		}
		return markDecisionRouteProcessedTx(txctx, tx, claim.EventID(), false, s.now())
	})
}

func markDecisionRouteProcessedTx(ctx context.Context, tx pipelineExecer, eventID string, postgres bool, now time.Time) error {
	pending, receiptOutcome, err := pipelineDispositionState(ctx, tx, eventID, postgres)
	if err != nil {
		return err
	}
	if !pending || receiptOutcome != "" {
		return runtimepipelineobligation.ErrIneligible
	}
	return writeExactPlatformPipelineReceipt(ctx, tx, eventID, runtimepipelineobligation.Acknowledged("decision_route_processed"), postgres, now)
}

func (s *PostgresStore) claimPostgresPipelineEvent(ctx context.Context, eventID string, purpose runtimepipelineobligation.Purpose) (runtimepipelineobligation.Claim, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimepipelineobligation.Claim{}, err
	}
	eventID = strings.TrimSpace(eventID)
	if _, err := uuid.Parse(eventID); err != nil {
		return runtimepipelineobligation.Claim{}, fmt.Errorf("claim pipeline event: %w", err)
	}
	registry := s.postgresPipelineClaims()
	if registry == nil {
		return runtimepipelineobligation.Claim{}, errors.New("PostgreSQL pipeline claim registry is required")
	}
	if registry.testBeforeClaimRegistryLock != nil {
		registry.testBeforeClaimRegistryLock()
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	for _, state := range registry.claims {
		if state != nil && state.claim.EventID() == eventID {
			return runtimepipelineobligation.Claim{}, runtimepipelineobligation.ErrBusy
		}
	}
	lease, acquired, err := acquireAdvisoryLockLease(context.WithoutCancel(ctx), s.DB, replayClaimLockKey(eventID))
	if err != nil {
		return runtimepipelineobligation.Claim{}, err
	}
	if !acquired || lease == nil {
		return runtimepipelineobligation.Claim{}, runtimepipelineobligation.ErrBusy
	}
	defer func() { _ = lease.Release(context.WithoutCancel(ctx)) }()
	if purpose != runtimepipelineobligation.PurposePublication {
		eligible, err := postgresPipelineEligible(ctx, lease.conn, eventID, purpose)
		if err != nil {
			return runtimepipelineobligation.Claim{}, err
		}
		if !eligible {
			return runtimepipelineobligation.Claim{}, runtimepipelineobligation.ErrIneligible
		}
	}
	claim, err := registry.issuer.Issue(eventID, purpose)
	if err == nil {
		token, tokenErr := registry.issuer.Token(claim)
		if tokenErr != nil {
			err = tokenErr
		} else {
			registry.claims[token] = &pipelineClaimState{claim: claim}
		}
	}
	return claim, err
}

func (s *SQLiteRuntimeStore) claimSQLitePipelineEvent(ctx context.Context, eventID string, purpose runtimepipelineobligation.Purpose) (runtimepipelineobligation.Claim, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimepipelineobligation.Claim{}, err
	}
	eventID = strings.TrimSpace(eventID)
	if _, err := uuid.Parse(eventID); err != nil {
		return runtimepipelineobligation.Claim{}, fmt.Errorf("claim sqlite pipeline event: %w", err)
	}
	if _, mutationActive := runtimepipeline.PipelineSQLTxFromContext(ctx); !mutationActive {
		s.mutationMu.Lock()
		defer s.mutationMu.Unlock()
	}
	s.pipelineClaimMu.Lock()
	issuer, claims := s.pipelineClaimOwner()
	for _, state := range claims {
		if state != nil && state.claim.EventID() == eventID {
			s.pipelineClaimMu.Unlock()
			return runtimepipelineobligation.Claim{}, runtimepipelineobligation.ErrBusy
		}
	}
	if purpose != runtimepipelineobligation.PurposePublication {
		eligible, err := sqlitePipelineEligible(ctx, s.DB, eventID, purpose)
		if err != nil {
			s.pipelineClaimMu.Unlock()
			return runtimepipelineobligation.Claim{}, err
		}
		if !eligible {
			s.pipelineClaimMu.Unlock()
			return runtimepipelineobligation.Claim{}, runtimepipelineobligation.ErrIneligible
		}
	}
	claim, err := issuer.Issue(eventID, purpose)
	if err == nil {
		token, tokenErr := issuer.Token(claim)
		if tokenErr != nil {
			err = tokenErr
		} else {
			claims[token] = &pipelineClaimState{claim: claim}
		}
	}
	s.pipelineClaimMu.Unlock()
	return claim, err
}

func (s *PostgresStore) loadPostgresClaimedPipelineWork(ctx context.Context, claim runtimepipelineobligation.Claim) (runtimepipelineobligation.ClaimedWork, error) {
	if _, err := s.postgresPipelineClaimState(claim); err != nil {
		return runtimepipelineobligation.ClaimedWork{}, err
	}
	if claim.Purpose() != runtimepipelineobligation.PurposePublication {
		eligible, err := postgresPipelineEligible(ctx, s.DB, claim.EventID(), claim.Purpose())
		if err != nil {
			return runtimepipelineobligation.ClaimedWork{}, err
		}
		if !eligible {
			return runtimepipelineobligation.ClaimedWork{}, runtimepipelineobligation.ErrIneligible
		}
	}
	return loadClaimedPipelineWork(ctx, s.DB, claim, true)
}

func (s *SQLiteRuntimeStore) loadSQLiteClaimedPipelineWork(ctx context.Context, claim runtimepipelineobligation.Claim) (runtimepipelineobligation.ClaimedWork, error) {
	if _, err := s.sqlitePipelineClaimState(claim); err != nil {
		return runtimepipelineobligation.ClaimedWork{}, err
	}
	return loadClaimedPipelineWork(ctx, s.DB, claim, false)
}

func (s *PostgresStore) commitInitialPipelineScopeTx(ctx context.Context, tx *sql.Tx, eventID string, scope runtimepipelineobligation.CommittedScope) error {
	if tx == nil {
		return errors.New("initial pipeline scope transaction is required")
	}
	if err := requireActiveRunForEvent(ctx, tx, eventID, storerunlifecycle.DialectPostgres); err != nil {
		return err
	}
	return insertCommittedPipelineScopeTx(ctx, tx, eventID, scope, true, time.Now().UTC())
}

func (s *SQLiteRuntimeStore) commitInitialPipelineScopeTx(ctx context.Context, tx *sql.Tx, eventID string, scope runtimepipelineobligation.CommittedScope) error {
	if tx == nil {
		return errors.New("initial sqlite pipeline scope transaction is required")
	}
	if err := requireActiveRunForEvent(ctx, tx, eventID, storerunlifecycle.DialectSQLite); err != nil {
		return err
	}
	return insertCommittedPipelineScopeTx(ctx, tx, eventID, scope, false, s.now())
}

func insertCommittedPipelineScopeTx(
	ctx context.Context,
	tx pipelineExecer,
	eventID string,
	scope runtimepipelineobligation.CommittedScope,
	postgres bool,
	now time.Time,
) error {
	eventID = strings.TrimSpace(eventID)
	if tx == nil {
		return errors.New("committed pipeline scope transaction is required")
	}
	if _, err := uuid.Parse(eventID); err != nil {
		return fmt.Errorf("committed pipeline scope event id: %w", err)
	}
	parsed, err := runtimepipelineobligation.ParseCommittedScope(string(scope))
	if err != nil {
		return fmt.Errorf("committed pipeline scope: %w", err)
	}
	if parsed != scope {
		return errors.New("committed pipeline scope must be canonical")
	}
	query := `
		INSERT INTO committed_replay_scopes (event_id, run_id, scope, created_at, updated_at)
		SELECT e.event_id, e.run_id, ?, ?, ? FROM events e WHERE e.event_id = ?
		ON CONFLICT(event_id) DO NOTHING`
	args := []any{string(scope), now, now, eventID}
	if postgres {
		query = `
			INSERT INTO committed_replay_scopes (event_id, run_id, scope, created_at, updated_at)
			SELECT e.event_id, e.run_id, $2, $3, $3 FROM events e WHERE e.event_id = $1::uuid
			ON CONFLICT(event_id) DO NOTHING`
		args = []any{eventID, string(scope), now}
	}
	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("insert committed pipeline scope: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read committed pipeline scope insertion: %w", err)
	}
	if rows == 1 {
		return nil
	}
	persisted, err := loadCommittedPipelineScope(ctx, tx, eventID, postgres)
	if err != nil {
		return fmt.Errorf("read committed pipeline scope duplicate: %w", err)
	}
	if persisted != scope {
		return errors.New("committed pipeline scope conflicts with persisted scope")
	}
	return nil
}

func (s *PostgresStore) commitInitialPipelineDispositionTx(
	ctx context.Context,
	tx *sql.Tx,
	eventID string,
	claim runtimepipelineobligation.Claim,
	disposition runtimepipelineobligation.Disposition,
) error {
	if tx == nil {
		return errors.New("initial pipeline disposition transaction is required")
	}
	if claim.Purpose() != runtimepipelineobligation.PurposePublication || claim.EventID() != strings.TrimSpace(eventID) {
		return runtimepipelineobligation.ErrWrongClaim
	}
	if _, err := s.postgresPipelineClaimState(claim); err != nil {
		return err
	}
	if err := disposition.ValidateFor(claim.Purpose()); err != nil {
		return err
	}
	return writePipelineDispositionTx(ctx, tx, eventID, claim.Purpose(), disposition, true, time.Now().UTC())
}

func (s *SQLiteRuntimeStore) commitInitialPipelineDispositionTx(
	ctx context.Context,
	tx *sql.Tx,
	eventID string,
	claim runtimepipelineobligation.Claim,
	disposition runtimepipelineobligation.Disposition,
) error {
	if tx == nil {
		return errors.New("initial sqlite pipeline disposition transaction is required")
	}
	if claim.Purpose() != runtimepipelineobligation.PurposePublication || claim.EventID() != strings.TrimSpace(eventID) {
		return runtimepipelineobligation.ErrWrongClaim
	}
	if _, err := s.sqlitePipelineClaimState(claim); err != nil {
		return err
	}
	if err := disposition.ValidateFor(claim.Purpose()); err != nil {
		return err
	}
	return writePipelineDispositionTx(ctx, tx, eventID, claim.Purpose(), disposition, false, s.now())
}

type pipelineQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func loadClaimedPipelineWork(ctx context.Context, q pipelineQueryer, claim runtimepipelineobligation.Claim, postgres bool) (runtimepipelineobligation.ClaimedWork, error) {
	var (
		records []events.PersistedReplayEvent
		outcome sql.NullString
		err     error
	)
	if postgres {
		records, err = hydratePostgresPersistedReplayEvents(ctx, q, []string{claim.EventID()})
	} else {
		records, err = hydrateSQLitePersistedReplayEvents(ctx, q, []string{claim.EventID()})
	}
	if err != nil {
		return runtimepipelineobligation.ClaimedWork{}, err
	}
	if len(records) != 1 {
		return runtimepipelineobligation.ClaimedWork{}, runtimepipelineobligation.ErrIneligible
	}
	scope, err := loadCommittedPipelineScope(ctx, q, claim.EventID(), postgres)
	if err != nil {
		return runtimepipelineobligation.ClaimedWork{}, err
	}
	if postgres {
		err = q.QueryRowContext(ctx, `SELECT outcome FROM event_receipts WHERE event_id = $1::uuid AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`, claim.EventID()).Scan(&outcome)
	} else {
		err = q.QueryRowContext(ctx, `SELECT outcome FROM event_receipts WHERE event_id = ? AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`, claim.EventID()).Scan(&outcome)
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return runtimepipelineobligation.ClaimedWork{}, err
	}
	work := runtimepipelineobligation.ClaimedWork{
		Event:        records[0].Event,
		Scope:        scope,
		Claim:        claim,
		Acknowledged: strings.TrimSpace(outcome.String) == "success",
	}
	if failure := records[0].ReplayFailure; failure != nil {
		reasonCode := strings.TrimSpace(failure.Detail.Code)
		if reasonCode == "" {
			reasonCode = "pipeline_recovery_event_invalid"
		}
		return runtimepipelineobligation.PreclassifiedWork(
			work,
			runtimepipelineobligation.Quarantined(reasonCode, failure),
		)
	}
	return work, nil
}

func loadCommittedPipelineScope(ctx context.Context, q rowQueryer, eventID string, postgres bool) (runtimepipelineobligation.CommittedScope, error) {
	var raw string
	query := `SELECT scope FROM committed_replay_scopes WHERE event_id = ?`
	args := []any{strings.TrimSpace(eventID)}
	if postgres {
		query = `SELECT scope FROM committed_replay_scopes WHERE event_id = $1::uuid`
	}
	if err := q.QueryRowContext(ctx, query, args...).Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", runtimepipelineobligation.ErrMissingScope
		}
		return "", err
	}
	scope, err := runtimepipelineobligation.ParseCommittedScope(raw)
	if err != nil {
		return "", fmt.Errorf("%w: %v", runtimepipelineobligation.ErrInvalidScope, err)
	}
	return scope, nil
}

func postgresPipelineEligible(ctx context.Context, q pipelineQueryer, eventID string, purpose runtimepipelineobligation.Purpose) (bool, error) {
	var eligible bool
	switch purpose {
	case runtimepipelineobligation.PurposeRecovery:
		diagnostics := diagnosticDirectReplayEventArgs()
		query := fmt.Sprintf(`
				SELECT EXISTS (
					SELECT 1
					FROM events e
				LEFT JOIN runs run ON run.run_id = e.run_id
				LEFT JOIN event_receipts receipt
				  ON receipt.event_id = e.event_id
				 AND receipt.subscriber_type = 'platform'
				 AND receipt.subscriber_id = 'pipeline'
					WHERE e.event_id = $1::uuid
					  AND (e.run_id IS NULL OR run.status IN ('running', 'paused'))
					  AND receipt.event_id IS NULL
					  AND %s
					  AND NOT EXISTS (
						SELECT 1 FROM decision_card_route_obligations route
						WHERE route.event_id = e.event_id AND route.status <> 'completed'
					  )
				)`, postgresDiagnosticDirectReplayExclusionSQL("e", 2))
		err := q.QueryRowContext(ctx, query, append([]any{eventID}, diagnostics...)...).Scan(&eligible)
		return eligible, err
	case runtimepipelineobligation.PurposeDecisionRoute:
		err := q.QueryRowContext(ctx, `
				SELECT EXISTS (
					SELECT 1
					FROM decision_card_route_obligations route
					JOIN runs run ON run.run_id = route.run_id
					WHERE route.event_id = $1::uuid
					  AND route.status = 'pending'
					  AND run.status IN ('running', 'paused')
				)`, eventID).Scan(&eligible)
		return eligible, err
	default:
		return false, fmt.Errorf("pipeline claim purpose %q cannot hydrate work", purpose)
	}
}

func sqlitePipelineEligible(ctx context.Context, q pipelineQueryer, eventID string, purpose runtimepipelineobligation.Purpose) (bool, error) {
	var eligible bool
	switch purpose {
	case runtimepipelineobligation.PurposeRecovery:
		query := `
				SELECT EXISTS (
					SELECT 1
				FROM events e
				LEFT JOIN runs run ON run.run_id = e.run_id
				LEFT JOIN event_receipts receipt
				  ON receipt.event_id = e.event_id
				 AND receipt.subscriber_type = 'platform'
				 AND receipt.subscriber_id = 'pipeline'
					WHERE e.event_id = ?
					  AND (e.run_id IS NULL OR run.status IN ('running', 'paused'))
					  AND receipt.event_id IS NULL
					  AND ` + sqliteDiagnosticDirectReplayExclusionSQL("e") + `
					  AND NOT EXISTS (
						SELECT 1 FROM decision_card_route_obligations route
						WHERE route.event_id = e.event_id AND route.status <> 'completed'
					  )
				)`
		err := q.QueryRowContext(ctx, query, append([]any{eventID}, diagnosticDirectReplayEventArgs()...)...).Scan(&eligible)
		return eligible, err
	case runtimepipelineobligation.PurposeDecisionRoute:
		err := q.QueryRowContext(ctx, `
				SELECT EXISTS (
					SELECT 1
					FROM decision_card_route_obligations route
					JOIN runs run ON run.run_id = route.run_id
					WHERE route.event_id = ?
					  AND route.status = 'pending'
					  AND run.status IN ('running', 'paused')
				)`, eventID).Scan(&eligible)
		return eligible, err
	default:
		return false, fmt.Errorf("pipeline claim purpose %q cannot hydrate work", purpose)
	}
}

func (s *PostgresStore) postgresPipelineCandidates(ctx context.Context, query runtimepipelineobligation.ClaimQuery, after *pipelineCandidate, limit int) ([]pipelineCandidate, error) {
	if query.Purpose == runtimepipelineobligation.PurposeDecisionRoute {
		whereAfter := ""
		args := []any{}
		if after != nil {
			whereAfter = `
				AND (route.attempt_count, route.next_attempt_at, route.created_at, route.event_id) >
				    ($1, $2, $3, $4::uuid)`
			args = append(args, after.attemptCount, after.nextAttemptAt, after.createdAt, after.eventID)
		}
		args = append(args, limit)
		rows, err := s.DB.QueryContext(ctx, fmt.Sprintf(`
				SELECT route.event_id::text
				     , route.attempt_count
				     , route.next_attempt_at
				     , route.created_at
				FROM decision_card_route_obligations route
				JOIN runs run ON run.run_id = route.run_id
				WHERE route.status = 'pending'
				  AND route.next_attempt_at <= now()
				  AND run.status IN ('running', 'paused')
				  %s
				ORDER BY route.attempt_count, route.next_attempt_at, route.created_at, route.event_id
				LIMIT $%d`, whereAfter, len(args)), args...)
		if err != nil {
			return nil, err
		}
		return scanPipelineCandidates(rows, true, "postgres decision-route pipeline candidates")
	}
	args := diagnosticDirectReplayEventArgs()
	whereRun := ""
	if runID := strings.TrimSpace(query.RunID); runID != "" {
		whereRun = fmt.Sprintf("AND e.run_id = $%d::uuid", len(args)+1)
		args = append(args, runID)
	}
	whereAfter := ""
	if after != nil {
		whereAfter = fmt.Sprintf("AND (e.created_at, e.event_id) > ($%d, $%d::uuid)", len(args)+1, len(args)+2)
		args = append(args, after.createdAt, after.eventID)
	}
	args = append(args, limit)
	rows, err := s.DB.QueryContext(ctx, fmt.Sprintf(`
			SELECT e.event_id::text, 0, e.created_at, e.created_at
			FROM events e
			LEFT JOIN runs run ON run.run_id = e.run_id
			LEFT JOIN event_receipts receipt
		  ON receipt.event_id = e.event_id
		 AND receipt.subscriber_type = 'platform'
		 AND receipt.subscriber_id = 'pipeline'
		WHERE receipt.event_id IS NULL
			  AND (e.run_id IS NULL OR run.status IN ('running', 'paused'))
			  %s
			  %s
			  AND NOT EXISTS (
				SELECT 1 FROM decision_card_route_obligations route
				WHERE route.event_id = e.event_id AND route.status <> 'completed'
			  )
			  AND %s
			ORDER BY e.created_at, e.event_id
			LIMIT $%d`, whereRun, whereAfter, postgresDiagnosticDirectReplayExclusionSQL("e", 1), len(args)), args...)
	if err != nil {
		return nil, err
	}
	return scanPipelineCandidates(rows, false, "postgres pipeline candidates")
}

func (s *SQLiteRuntimeStore) sqlitePipelineCandidates(ctx context.Context, query runtimepipelineobligation.ClaimQuery, after *pipelineCandidate, limit int) ([]pipelineCandidate, error) {
	if query.Purpose == runtimepipelineobligation.PurposeDecisionRoute {
		whereAfter := ""
		args := []any{time.Now().UTC()}
		if after != nil {
			whereAfter = `
				AND (route.attempt_count, route.next_attempt_at, route.created_at, route.event_id) >
				    (?, ?, ?, ?)`
			args = append(args, after.attemptCount, after.nextAttemptAt, after.createdAt, after.eventID)
		}
		args = append(args, limit)
		rows, err := s.DB.QueryContext(ctx, `
				SELECT route.event_id
				     , route.attempt_count
				     , route.next_attempt_at
				     , route.created_at
				FROM decision_card_route_obligations route
				JOIN runs run ON run.run_id = route.run_id
				WHERE route.status = 'pending'
				  AND route.next_attempt_at <= ?
				  AND run.status IN ('running', 'paused')
				  `+whereAfter+`
				ORDER BY route.attempt_count, route.next_attempt_at, route.created_at, route.event_id
				LIMIT ?`, args...)
		if err != nil {
			return nil, err
		}
		return scanPipelineCandidates(rows, true, "sqlite decision-route pipeline candidates")
	}
	args := make([]any, 0, len(diagnosticDirectReplayEventArgs())+4)
	whereRun := ""
	if runID := strings.TrimSpace(query.RunID); runID != "" {
		whereRun = "AND e.run_id = ?"
		args = append(args, runID)
	}
	whereAfter := ""
	if after != nil {
		whereAfter = "AND (e.created_at, e.event_id) > (?, ?)"
		args = append(args, after.createdAt, after.eventID)
	}
	args = append(args, diagnosticDirectReplayEventArgs()...)
	args = append(args, limit)
	rows, err := s.DB.QueryContext(ctx, `
			SELECT e.event_id, 0, e.created_at, e.created_at
			FROM events e
			LEFT JOIN runs run ON run.run_id = e.run_id
			LEFT JOIN event_receipts receipt
		  ON receipt.event_id = e.event_id
		 AND receipt.subscriber_type = 'platform'
		 AND receipt.subscriber_id = 'pipeline'
			WHERE receipt.event_id IS NULL
			  AND (e.run_id IS NULL OR run.status IN ('running', 'paused'))
			  `+whereRun+`
			  `+whereAfter+`
			  AND NOT EXISTS (
				SELECT 1 FROM decision_card_route_obligations route
				WHERE route.event_id = e.event_id AND route.status <> 'completed'
		  )
		  AND `+sqliteDiagnosticDirectReplayExclusionSQL("e")+`
		ORDER BY e.created_at, e.event_id
		LIMIT ?`, args...)
	if err != nil {
		return nil, err
	}
	return scanPipelineCandidates(rows, false, "sqlite pipeline candidates")
}

func scanPipelineCandidates(rows *sql.Rows, decisionRoute bool, operation string) ([]pipelineCandidate, error) {
	defer rows.Close()
	out := make([]pipelineCandidate, 0)
	for rows.Next() {
		var (
			candidate  pipelineCandidate
			nextRaw    any
			createdRaw any
		)
		if err := rows.Scan(&candidate.eventID, &candidate.attemptCount, &nextRaw, &createdRaw); err != nil {
			return nil, fmt.Errorf("%s: %w", operation, err)
		}
		var ok bool
		var err error
		candidate.createdAt, ok, err = sqliteTimeValue(createdRaw)
		if err != nil {
			return nil, fmt.Errorf("%s created_at: %w", operation, err)
		}
		if !ok {
			return nil, fmt.Errorf("%s created_at is missing", operation)
		}
		candidate.nextAttemptAt, ok, err = sqliteTimeValue(nextRaw)
		if err != nil {
			return nil, fmt.Errorf("%s next_attempt_at: %w", operation, err)
		}
		if !ok {
			return nil, fmt.Errorf("%s next_attempt_at is missing", operation)
		}
		candidate.eventID = strings.TrimSpace(candidate.eventID)
		if !decisionRoute {
			candidate.nextAttemptAt = candidate.createdAt
		}
		out = append(out, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: %w", operation, err)
	}
	return out, nil
}

func (s *postgresPipelineObligationStore) Settle(ctx context.Context, claim runtimepipelineobligation.Claim, disposition runtimepipelineobligation.Disposition) error {
	if err := disposition.ValidateFor(claim.Purpose()); err != nil {
		return err
	}
	lease, err := s.acquirePostgresPipelineMutationLease(ctx, claim)
	if err != nil {
		return err
	}
	leaseOpen := true
	defer func() {
		if leaseOpen {
			_ = lease.Release(context.WithoutCancel(ctx))
		}
	}()
	if _, err := s.postgresPipelineClaimState(claim); err != nil {
		return err
	}
	tx, err := lease.conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := writePipelineDispositionTx(ctx, tx, claim.EventID(), claim.Purpose(), disposition, true, time.Now().UTC()); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if err := lease.Release(context.WithoutCancel(ctx)); err != nil {
		return err
	}
	leaseOpen = false
	return s.releasePostgresPipelineClaim(context.WithoutCancel(ctx), claim)
}

func (s *sqlitePipelineObligationStore) Settle(ctx context.Context, claim runtimepipelineobligation.Claim, disposition runtimepipelineobligation.Disposition) error {
	if err := disposition.ValidateFor(claim.Purpose()); err != nil {
		return err
	}
	if err := s.runRuntimeMutation(ctx, "settle sqlite pipeline obligation", func(txctx context.Context, tx *sql.Tx) error {
		if _, err := s.sqlitePipelineClaimState(claim); err != nil {
			return err
		}
		return writePipelineDispositionTx(txctx, tx, claim.EventID(), claim.Purpose(), disposition, false, s.now())
	}); err != nil {
		return err
	}
	return s.releaseSQLitePipelineClaim(claim)
}

type pipelineExecer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (s *PostgresStore) terminalizePipelineObligationTx(
	ctx context.Context,
	tx *sql.Tx,
	eventID string,
	disposition runtimepipelineobligation.Disposition,
	at time.Time,
) error {
	if tx == nil {
		return errors.New("pipeline parent terminalization transaction is required")
	}
	if err := disposition.ValidateFor(runtimepipelineobligation.PurposeRecovery); err != nil {
		return err
	}
	registry := s.postgresPipelineClaims()
	if registry == nil {
		return errors.New("PostgreSQL pipeline claim registry is required")
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	for _, state := range registry.claims {
		if state != nil && state.claim.EventID() == strings.TrimSpace(eventID) {
			return runtimepipelineobligation.ErrBusy
		}
	}
	if registry.testAfterParentClaimScan != nil {
		registry.testAfterParentClaimScan()
	}
	var acquired bool
	if err := tx.QueryRowContext(ctx, `SELECT pg_try_advisory_xact_lock(hashtext($1))`, replayClaimLockKey(eventID)).Scan(&acquired); err != nil {
		return fmt.Errorf("fence pipeline parent terminalization: %w", err)
	}
	if !acquired {
		return runtimepipelineobligation.ErrBusy
	}
	return terminalizeUnclaimedPipelineObligationTx(ctx, tx, eventID, disposition, true, at)
}

func (s *SQLiteRuntimeStore) terminalizePipelineObligationTx(
	ctx context.Context,
	tx *sql.Tx,
	eventID string,
	disposition runtimepipelineobligation.Disposition,
	at time.Time,
) error {
	if tx == nil {
		return errors.New("sqlite pipeline parent terminalization transaction is required")
	}
	if err := disposition.ValidateFor(runtimepipelineobligation.PurposeRecovery); err != nil {
		return err
	}
	eventID = strings.TrimSpace(eventID)
	s.pipelineClaimMu.Lock()
	_, claims := s.pipelineClaimOwner()
	for _, state := range claims {
		if state != nil && state.claim.EventID() == eventID {
			s.pipelineClaimMu.Unlock()
			return runtimepipelineobligation.ErrBusy
		}
	}
	s.pipelineClaimMu.Unlock()
	return terminalizeUnclaimedPipelineObligationTx(ctx, tx, eventID, disposition, false, at)
}

func terminalizeUnclaimedPipelineObligationTx(
	ctx context.Context,
	tx pipelineExecer,
	eventID string,
	disposition runtimepipelineobligation.Disposition,
	postgres bool,
	at time.Time,
) error {
	pending, receiptOutcome, err := pipelineDispositionState(ctx, tx, eventID, postgres)
	if err != nil {
		return err
	}
	if pending && receiptOutcome == "success" && !disposition.Successful() {
		return supersedeProcessedParentDecisionRouteTx(ctx, tx, eventID, postgres, at)
	}
	exact, found, err := exactStoredPipelineDisposition(ctx, tx, eventID, disposition, postgres)
	if err != nil {
		return err
	}
	if found {
		if exact {
			return settleExactParentDecisionRouteTx(ctx, tx, eventID, disposition, postgres, at)
		}
		return runtimepipelineobligation.ErrIneligible
	}
	return writePipelineDispositionTx(ctx, tx, eventID, runtimepipelineobligation.PurposeRecovery, disposition, postgres, at)
}

func supersedeProcessedParentDecisionRouteTx(
	ctx context.Context,
	tx pipelineExecer,
	eventID string,
	postgres bool,
	at time.Time,
) error {
	query := `UPDATE decision_card_route_obligations SET status = 'superseded', superseded_at = ?, updated_at = ? WHERE event_id = ? AND status = 'pending'`
	args := []any{at, at, eventID}
	if postgres {
		query = `UPDATE decision_card_route_obligations SET status = 'superseded', superseded_at = $2, updated_at = $2 WHERE event_id = $1::uuid AND status = 'pending'`
		args = []any{eventID, at}
	}
	result, err := tx.ExecContext(ctx, query, args...)
	return requireOnePipelineMutation(result, err, "supersede processed parent decision-route obligation")
}

func settleExactParentDecisionRouteTx(
	ctx context.Context,
	tx pipelineExecer,
	eventID string,
	disposition runtimepipelineobligation.Disposition,
	postgres bool,
	at time.Time,
) error {
	pending, _, err := pipelineDispositionState(ctx, tx, eventID, postgres)
	if err != nil || !pending {
		return err
	}
	if disposition.Successful() {
		return runtimepipelineobligation.ErrIneligible
	}
	status := "quarantined"
	query := `UPDATE decision_card_route_obligations SET status = ?, quarantined_at = ?, updated_at = ? WHERE event_id = ? AND status = 'pending'`
	args := []any{status, at, at, eventID}
	if postgres {
		query = `UPDATE decision_card_route_obligations SET status = $2, quarantined_at = $3, updated_at = $3 WHERE event_id = $1::uuid AND status = 'pending'`
		args = []any{eventID, status, at}
	}
	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}
	return requireOnePipelineMutation(result, nil, "settle exact parent decision-route obligation")
}

func exactStoredPipelineDisposition(
	ctx context.Context,
	q pipelineExecer,
	eventID string,
	disposition runtimepipelineobligation.Disposition,
	postgres bool,
) (exact bool, found bool, err error) {
	expected, err := storedPipelineDispositionFor(disposition)
	if err != nil {
		return false, false, err
	}
	var (
		outcome     string
		reasonCode  string
		failureRaw  any
		sideEffects []byte
	)
	query := `
		SELECT outcome, COALESCE(reason_code, ''), failure, side_effects
		FROM event_receipts
		WHERE event_id = ? AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`
	args := []any{eventID}
	if postgres {
		query = `
			SELECT outcome, COALESCE(reason_code, ''), failure, side_effects
			FROM event_receipts
			WHERE event_id = $1::uuid AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`
	}
	if err := q.QueryRowContext(ctx, query, args...).Scan(&outcome, &reasonCode, &failureRaw, &sideEffects); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, false, nil
		}
		return false, false, err
	}
	actualFailure, err := decodeStoredFailure(failureRaw)
	if err != nil {
		return false, true, err
	}
	var actualSideEffects pipelineReceiptSideEffects
	if err := json.Unmarshal(sideEffects, &actualSideEffects); err != nil {
		return false, true, fmt.Errorf("decode pipeline receipt side effects: %w", err)
	}
	return strings.TrimSpace(outcome) == expected.outcome &&
			strings.TrimSpace(reasonCode) == expected.reasonCode &&
			samePipelineFailure(actualFailure, disposition.Failure()) &&
			actualSideEffects == expected.sideEffects,
		true,
		nil
}

func samePipelineFailure(left, right *runtimefailures.Envelope) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	leftRaw, leftErr := runtimefailures.MarshalEnvelope(*left)
	rightRaw, rightErr := runtimefailures.MarshalEnvelope(*right)
	return leftErr == nil && rightErr == nil && string(leftRaw) == string(rightRaw)
}

func writePipelineDispositionTx(ctx context.Context, tx pipelineExecer, eventID string, purpose runtimepipelineobligation.Purpose, disposition runtimepipelineobligation.Disposition, postgres bool, now time.Time) error {
	routePending, receiptOutcome, err := pipelineDispositionState(ctx, tx, eventID, postgres)
	if err != nil {
		return err
	}
	if disposition.Kind() == runtimepipelineobligation.DispositionDeferred {
		if !routePending {
			return errors.New("deferred pipeline disposition requires a pending decision-route obligation")
		}
		raw, err := json.Marshal(disposition.Failure())
		if err != nil {
			return err
		}
		query := `UPDATE decision_card_route_obligations SET attempt_count = attempt_count + 1, next_attempt_at = ?, last_failure = ?, updated_at = ? WHERE event_id = ? AND status = 'pending'`
		args := []any{disposition.RetryAt(), string(raw), now, eventID}
		if postgres {
			query = `UPDATE decision_card_route_obligations SET attempt_count = attempt_count + 1, next_attempt_at = $2, last_failure = $3::jsonb, updated_at = $4 WHERE event_id = $1::uuid AND status = 'pending'`
			args = []any{eventID, disposition.RetryAt(), string(raw), now}
		}
		result, err := tx.ExecContext(ctx, query, args...)
		return requireOnePipelineMutation(result, err, "defer decision-route obligation")
	}
	if receiptOutcome != "" {
		if !(routePending && receiptOutcome == "success" && disposition.Kind() == runtimepipelineobligation.DispositionAcknowledged) {
			return runtimepipelineobligation.ErrIneligible
		}
	} else {
		if err := writeExactPlatformPipelineReceipt(ctx, tx, eventID, disposition, postgres, now); err != nil {
			return err
		}
	}
	if !routePending {
		return nil
	}
	status := "completed"
	timeColumn := "completed_at"
	if disposition.Kind() == runtimepipelineobligation.DispositionQuarantined ||
		disposition.Kind() == runtimepipelineobligation.DispositionTerminal ||
		disposition.Kind() == runtimepipelineobligation.DispositionDeadLetter {
		status = "quarantined"
		timeColumn = "quarantined_at"
	}
	query := fmt.Sprintf(`UPDATE decision_card_route_obligations SET status = ?, %s = ?, updated_at = ? WHERE event_id = ? AND status = 'pending'`, timeColumn)
	args := []any{status, now, now, eventID}
	if postgres {
		query = fmt.Sprintf(`UPDATE decision_card_route_obligations SET status = $2, %s = $3, updated_at = $3 WHERE event_id = $1::uuid AND status = 'pending'`, timeColumn)
		args = []any{eventID, status, now}
	}
	result, err := tx.ExecContext(ctx, query, args...)
	return requireOnePipelineMutation(result, err, "settle decision-route obligation")
}

func pipelineDispositionState(ctx context.Context, tx pipelineExecer, eventID string, postgres bool) (bool, string, error) {
	var (
		routePending bool
		outcome      sql.NullString
	)
	query := `
		SELECT EXISTS (
			SELECT 1 FROM decision_card_route_obligations
			WHERE event_id = ? AND status = 'pending'
		), (
			SELECT outcome FROM event_receipts
			WHERE event_id = ? AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'
		)`
	args := []any{eventID, eventID}
	if postgres {
		query = `
			SELECT EXISTS (
				SELECT 1 FROM decision_card_route_obligations
				WHERE event_id = $1::uuid AND status = 'pending'
			), (
				SELECT outcome FROM event_receipts
				WHERE event_id = $1::uuid AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'
			)`
		args = []any{eventID}
	}
	if err := tx.QueryRowContext(ctx, query, args...).Scan(&routePending, &outcome); err != nil {
		return false, "", err
	}
	return routePending, strings.TrimSpace(outcome.String), nil
}

func writeExactPlatformPipelineReceipt(ctx context.Context, tx pipelineExecer, eventID string, disposition runtimepipelineobligation.Disposition, postgres bool, now time.Time) error {
	stored, err := storedPipelineDispositionFor(disposition)
	if err != nil {
		return err
	}
	failureJSON, err := encodeStoredFailure(disposition.Failure())
	if err != nil {
		return err
	}
	sideEffects, err := json.Marshal(stored.sideEffects)
	if err != nil {
		return err
	}
	receiptID := uuid.NewString()
	query := `
		INSERT INTO event_receipts (
			receipt_id, event_id, subscriber_type, subscriber_id, entity_id, flow_instance,
			outcome, reason_code, failure, side_effects, processed_at
		)
		SELECT ?, e.event_id, 'platform', 'pipeline', e.entity_id, e.flow_instance, ?, ?, ?, ?, ?
		FROM events e WHERE e.event_id = ?
		ON CONFLICT(event_id, subscriber_type, subscriber_id) DO NOTHING`
	args := []any{receiptID, stored.outcome, stored.reasonCode, failureJSON, string(sideEffects), now, eventID}
	if postgres {
		query = `
			INSERT INTO event_receipts (
				receipt_id, event_id, subscriber_type, subscriber_id, entity_id, flow_instance,
				outcome, reason_code, failure, side_effects, processed_at
			)
			SELECT $1::uuid, e.event_id, 'platform', 'pipeline', e.entity_id, e.flow_instance, $2, $3, $4::jsonb, $5::jsonb, $6
			FROM events e WHERE e.event_id = $7::uuid
			ON CONFLICT(event_id, subscriber_type, subscriber_id) DO NOTHING`
	}
	result, err := tx.ExecContext(ctx, query, args...)
	return requireOnePipelineMutation(result, err, "write platform pipeline acknowledgement")
}

type storedPipelineDisposition struct {
	outcome     string
	reasonCode  string
	sideEffects pipelineReceiptSideEffects
}

func storedPipelineDispositionFor(disposition runtimepipelineobligation.Disposition) (storedPipelineDisposition, error) {
	if err := disposition.ValidateFor(runtimepipelineobligation.PurposeRecovery); err != nil {
		return storedPipelineDisposition{}, err
	}
	outcome := "success"
	managerStatus := "processed"
	if disposition.Kind() != runtimepipelineobligation.DispositionAcknowledged {
		outcome = "dead_letter"
		managerStatus = "error"
		if disposition.Kind() == runtimepipelineobligation.DispositionDeadLetter {
			managerStatus = "dead_letter"
		}
	}
	reasonCode := strings.TrimSpace(disposition.ReasonCode())
	if reasonCode == "" {
		if outcome == "success" {
			reasonCode = "pipeline_persisted"
		} else {
			reasonCode = "pipeline_error"
		}
	}
	return storedPipelineDisposition{
		outcome:     outcome,
		reasonCode:  reasonCode,
		sideEffects: pipelineReceiptSideEffects{ManagerStatus: managerStatus, ReasonCode: reasonCode},
	}, nil
}

func requireOnePipelineMutation(result sql.Result, err error, operation string) error {
	if err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s: read affected rows: %w", operation, err)
	}
	if rows != 1 {
		return fmt.Errorf("%s: affected %d rows, want 1", operation, rows)
	}
	return nil
}

func (s *PostgresStore) postgresPipelineClaimState(claim runtimepipelineobligation.Claim) (*pipelineClaimState, error) {
	registry := s.postgresPipelineClaims()
	if registry == nil {
		return nil, runtimepipelineobligation.ErrStaleClaim
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	token, err := registry.issuer.Token(claim)
	if err != nil {
		return nil, err
	}
	state := registry.claims[token]
	if state == nil {
		return nil, runtimepipelineobligation.ErrStaleClaim
	}
	if err := registry.issuer.Verify(state.claim, claim.EventID(), claim.Purpose()); err != nil {
		return nil, err
	}
	return state, nil
}

func (s *PostgresStore) acquirePostgresPipelineMutationLease(ctx context.Context, claim runtimepipelineobligation.Claim) (*sqlAdvisoryLockLease, error) {
	registry := s.postgresPipelineClaims()
	if registry == nil {
		return nil, runtimepipelineobligation.ErrStaleClaim
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	token, err := registry.issuer.Token(claim)
	if err != nil {
		return nil, err
	}
	state := registry.claims[token]
	if state == nil {
		return nil, runtimepipelineobligation.ErrStaleClaim
	}
	if err := registry.issuer.Verify(state.claim, claim.EventID(), claim.Purpose()); err != nil {
		return nil, err
	}
	lease, acquired, err := acquireAdvisoryLockLease(context.WithoutCancel(ctx), s.DB, replayClaimLockKey(claim.EventID()))
	if err != nil {
		return nil, err
	}
	if !acquired || lease == nil {
		return nil, runtimepipelineobligation.ErrBusy
	}
	return lease, nil
}

func (s *SQLiteRuntimeStore) sqlitePipelineClaimState(claim runtimepipelineobligation.Claim) (*pipelineClaimState, error) {
	s.pipelineClaimMu.Lock()
	defer s.pipelineClaimMu.Unlock()
	issuer, claims := s.pipelineClaimOwner()
	token, err := issuer.Token(claim)
	if err != nil {
		return nil, err
	}
	state := claims[token]
	if state == nil {
		return nil, runtimepipelineobligation.ErrStaleClaim
	}
	if err := issuer.Verify(state.claim, claim.EventID(), claim.Purpose()); err != nil {
		return nil, err
	}
	return state, nil
}

func (s *postgresPipelineObligationStore) Release(ctx context.Context, claim runtimepipelineobligation.Claim) error {
	return s.releasePostgresPipelineClaim(ctx, claim)
}

func (s *sqlitePipelineObligationStore) Release(_ context.Context, claim runtimepipelineobligation.Claim) error {
	return s.releaseSQLitePipelineClaim(claim)
}

func (s *PostgresStore) releasePostgresPipelineClaim(ctx context.Context, claim runtimepipelineobligation.Claim) error {
	registry := s.postgresPipelineClaims()
	if registry == nil {
		return runtimepipelineobligation.ErrStaleClaim
	}
	registry.mu.Lock()
	token, err := registry.issuer.Token(claim)
	if err != nil {
		registry.mu.Unlock()
		return err
	}
	state := registry.claims[token]
	if state == nil {
		registry.mu.Unlock()
		return runtimepipelineobligation.ErrStaleClaim
	}
	delete(registry.claims, token)
	registry.mu.Unlock()
	return nil
}

func (s *SQLiteRuntimeStore) releaseSQLitePipelineClaim(claim runtimepipelineobligation.Claim) error {
	s.pipelineClaimMu.Lock()
	defer s.pipelineClaimMu.Unlock()
	issuer, claims := s.pipelineClaimOwner()
	token, err := issuer.Token(claim)
	if err != nil {
		return err
	}
	if claims[token] == nil {
		return runtimepipelineobligation.ErrStaleClaim
	}
	delete(claims, token)
	return nil
}

func (s *postgresPipelineObligationStore) GlobalWorkPresence(ctx context.Context) (runtimepipelineobligation.GlobalWorkPresence, error) {
	var out runtimepipelineobligation.GlobalWorkPresence
	args := diagnosticDirectReplayEventArgs()
	err := s.DB.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT EXISTS (
			SELECT 1 FROM events e
			LEFT JOIN runs run ON run.run_id = e.run_id
			LEFT JOIN event_receipts receipt ON receipt.event_id = e.event_id AND receipt.subscriber_type = 'platform' AND receipt.subscriber_id = 'pipeline'
			WHERE receipt.event_id IS NULL
			  AND (e.run_id IS NULL OR run.status IN ('running', 'paused'))
			  AND NOT EXISTS (SELECT 1 FROM decision_card_route_obligations route WHERE route.event_id = e.event_id AND route.status <> 'completed')
			  AND %s
		), EXISTS (
			SELECT 1 FROM decision_card_route_obligations route JOIN runs run ON run.run_id = route.run_id
				WHERE route.status = 'pending' AND route.next_attempt_at <= now() AND run.status IN ('running', 'paused')
		), COALESCE((
			SELECT MIN(e.created_at) FROM events e
				LEFT JOIN runs run ON run.run_id = e.run_id
				LEFT JOIN event_receipts receipt ON receipt.event_id = e.event_id AND receipt.subscriber_type = 'platform' AND receipt.subscriber_id = 'pipeline'
				WHERE receipt.event_id IS NULL
				  AND (e.run_id IS NULL OR run.status IN ('running', 'paused'))
				  AND NOT EXISTS (SELECT 1 FROM decision_card_route_obligations route WHERE route.event_id = e.event_id AND route.status <> 'completed')
				  AND %s
		), '0001-01-01'::timestamptz)`,
		postgresDiagnosticDirectReplayExclusionSQL("e", 1),
		postgresDiagnosticDirectReplayExclusionSQL("e", 1)), args...).Scan(&out.ProcessingEligible, &out.DecisionRouteDue, &out.OldestEligibleEvent)
	return out, err
}

func (s *sqlitePipelineObligationStore) GlobalWorkPresence(ctx context.Context) (runtimepipelineobligation.GlobalWorkPresence, error) {
	var (
		out       runtimepipelineobligation.GlobalWorkPresence
		oldestRaw any
	)
	diagnostics := diagnosticDirectReplayEventArgs()
	args := make([]any, 0, len(diagnostics)*2+1)
	args = append(args, diagnostics...)
	args = append(args, time.Now().UTC())
	args = append(args, diagnostics...)
	err := s.DB.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM events e
			LEFT JOIN runs run ON run.run_id = e.run_id
			LEFT JOIN event_receipts receipt ON receipt.event_id = e.event_id AND receipt.subscriber_type = 'platform' AND receipt.subscriber_id = 'pipeline'
			WHERE receipt.event_id IS NULL
			  AND (e.run_id IS NULL OR run.status IN ('running', 'paused'))
			  AND NOT EXISTS (SELECT 1 FROM decision_card_route_obligations route WHERE route.event_id = e.event_id AND route.status <> 'completed')
			  AND `+sqliteDiagnosticDirectReplayExclusionSQL("e")+`
		), EXISTS (
			SELECT 1 FROM decision_card_route_obligations route JOIN runs run ON run.run_id = route.run_id
				WHERE route.status = 'pending' AND route.next_attempt_at <= ? AND run.status IN ('running', 'paused')
		), (
			SELECT MIN(e.created_at) FROM events e
				LEFT JOIN runs run ON run.run_id = e.run_id
				LEFT JOIN event_receipts receipt ON receipt.event_id = e.event_id AND receipt.subscriber_type = 'platform' AND receipt.subscriber_id = 'pipeline'
				WHERE receipt.event_id IS NULL
				  AND (e.run_id IS NULL OR run.status IN ('running', 'paused'))
				  AND NOT EXISTS (SELECT 1 FROM decision_card_route_obligations route WHERE route.event_id = e.event_id AND route.status <> 'completed')
				  AND `+sqliteDiagnosticDirectReplayExclusionSQL("e")+`
			)`, args...).Scan(
		&out.ProcessingEligible, &out.DecisionRouteDue, &oldestRaw)
	if err != nil {
		return out, err
	}
	if oldest, ok, parseErr := sqliteTimeValue(oldestRaw); parseErr != nil {
		return out, fmt.Errorf("parse oldest SQLite pipeline obligation: %w", parseErr)
	} else if ok {
		out.OldestEligibleEvent = oldest
	}
	return out, nil
}

func (s *postgresPipelineObligationStore) SummarizeRun(ctx context.Context, runID string) (runtimepipelineobligation.RunSummary, error) {
	var queryer pipelineQueryer = s.DB
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		queryer = tx
	}
	return summarizePipelineRun(ctx, queryer, runID, true)
}

func (s *sqlitePipelineObligationStore) SummarizeRun(ctx context.Context, runID string) (runtimepipelineobligation.RunSummary, error) {
	var queryer pipelineQueryer = s.DB
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		queryer = tx
	}
	return summarizePipelineRun(ctx, queryer, runID, false)
}

func (s *postgresPipelineObligationStore) TerminalizeRun(
	ctx context.Context,
	runID string,
	disposition runtimepipelineobligation.Disposition,
	at time.Time,
) (int, error) {
	tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx)
	if !ok || tx == nil {
		return 0, errors.New("pipeline parent terminalization requires the named PostgreSQL transaction")
	}
	return s.terminalizePostgresPipelineRunTx(ctx, tx, runID, disposition, at)
}

func (s *sqlitePipelineObligationStore) TerminalizeRun(
	ctx context.Context,
	runID string,
	disposition runtimepipelineobligation.Disposition,
	at time.Time,
) (int, error) {
	tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx)
	if !ok || tx == nil {
		return 0, errors.New("pipeline parent terminalization requires the named SQLite transaction")
	}
	return s.terminalizeSQLitePipelineRunTx(ctx, tx, runID, disposition, at)
}

func (s *PostgresStore) terminalizePostgresPipelineRunTx(
	ctx context.Context,
	tx *sql.Tx,
	runID string,
	disposition runtimepipelineobligation.Disposition,
	at time.Time,
) (int, error) {
	if !disposition.Terminal() || disposition.Successful() {
		return 0, errors.New("parent run terminalization requires a terminal non-success pipeline disposition")
	}
	diagnostics := diagnosticDirectReplayEventArgs()
	args := append([]any{strings.TrimSpace(runID)}, diagnostics...)
	rows, err := tx.QueryContext(ctx, fmt.Sprintf(`
		SELECT e.event_id::text
		FROM events e
		LEFT JOIN event_receipts receipt
		  ON receipt.event_id = e.event_id
		 AND receipt.subscriber_type = 'platform'
		 AND receipt.subscriber_id = 'pipeline'
			WHERE e.run_id = $1::uuid
			  AND (
				receipt.event_id IS NULL
				OR (
					receipt.outcome = 'success'
					AND EXISTS (
						SELECT 1 FROM decision_card_route_obligations route
						WHERE route.event_id = e.event_id AND route.status = 'pending'
					)
				)
			  )
			  AND %s
		ORDER BY e.created_at, e.event_id`,
		postgresDiagnosticDirectReplayExclusionSQL("e", 2)), args...)
	if err != nil {
		return 0, fmt.Errorf("list PostgreSQL pipeline parent terminalization targets: %w", err)
	}
	eventIDs, err := scanOrderedEventIDs(rows, "PostgreSQL pipeline parent terminalization")
	if err != nil {
		return 0, err
	}
	for _, eventID := range eventIDs {
		if err := s.terminalizePipelineObligationTx(ctx, tx, eventID, disposition, at); err != nil {
			return 0, fmt.Errorf("terminalize PostgreSQL pipeline event %s: %w", eventID, err)
		}
	}
	return len(eventIDs), nil
}

func (s *SQLiteRuntimeStore) terminalizeSQLitePipelineRunTx(
	ctx context.Context,
	tx *sql.Tx,
	runID string,
	disposition runtimepipelineobligation.Disposition,
	at time.Time,
) (int, error) {
	if !disposition.Terminal() || disposition.Successful() {
		return 0, errors.New("parent run terminalization requires a terminal non-success pipeline disposition")
	}
	args := append([]any{strings.TrimSpace(runID)}, diagnosticDirectReplayEventArgs()...)
	rows, err := tx.QueryContext(ctx, `
		SELECT e.event_id
		FROM events e
		LEFT JOIN event_receipts receipt
		  ON receipt.event_id = e.event_id
		 AND receipt.subscriber_type = 'platform'
		 AND receipt.subscriber_id = 'pipeline'
			WHERE e.run_id = ?
			  AND (
				receipt.event_id IS NULL
				OR (
					receipt.outcome = 'success'
					AND EXISTS (
						SELECT 1 FROM decision_card_route_obligations route
						WHERE route.event_id = e.event_id AND route.status = 'pending'
					)
				)
			  )
			  AND `+sqliteDiagnosticDirectReplayExclusionSQL("e")+`
		ORDER BY e.created_at, e.event_id`, args...)
	if err != nil {
		return 0, fmt.Errorf("list SQLite pipeline parent terminalization targets: %w", err)
	}
	eventIDs, err := scanOrderedEventIDs(rows, "SQLite pipeline parent terminalization")
	if err != nil {
		return 0, err
	}
	for _, eventID := range eventIDs {
		if err := s.terminalizePipelineObligationTx(ctx, tx, eventID, disposition, at); err != nil {
			return 0, fmt.Errorf("terminalize SQLite pipeline event %s: %w", eventID, err)
		}
	}
	return len(eventIDs), nil
}

func summarizePipelineRun(ctx context.Context, q pipelineQueryer, runID string, postgres bool) (runtimepipelineobligation.RunSummary, error) {
	out := runtimepipelineobligation.RunSummary{RunID: strings.TrimSpace(runID)}
	if _, err := uuid.Parse(out.RunID); err != nil {
		return out, fmt.Errorf("pipeline run summary: %w", err)
	}
	diagnostics := diagnosticDirectReplayEventArgs()
	diagnosticPredicate := sqliteDiagnosticDirectReplayExclusionSQL("e")
	runPlaceholder := "?"
	args := make([]any, 0, len(diagnostics)+1)
	args = append(args, diagnostics...)
	args = append(args, out.RunID)
	if postgres {
		diagnosticPredicate = postgresDiagnosticDirectReplayExclusionSQL("e", 1)
		runPlaceholder = fmt.Sprintf("$%d::uuid", len(diagnostics)+1)
	} else {
		args = append(args, out.RunID)
	}
	query := fmt.Sprintf(`
			WITH classified AS (
				SELECT
					e.event_id,
					NOT (%s) AS diagnostic,
					receipt.event_id AS receipt_id,
					receipt.outcome AS receipt_outcome,
					route.event_id AS route_id,
					route.status AS route_status
				FROM events e
				LEFT JOIN event_receipts receipt
				  ON receipt.event_id = e.event_id
				 AND receipt.subscriber_type = 'platform'
				 AND receipt.subscriber_id = 'pipeline'
				LEFT JOIN decision_card_route_obligations route ON route.event_id = e.event_id
				WHERE e.run_id = %s
			)
			SELECT
				COALESCE(SUM(CASE WHEN NOT classified.diagnostic AND classified.receipt_id IS NULL AND (classified.route_id IS NULL OR classified.route_status = 'completed') THEN 1 ELSE 0 END), 0),
				COALESCE(SUM(CASE WHEN NOT classified.diagnostic AND classified.receipt_outcome = 'success' THEN 1 ELSE 0 END), 0),
				COALESCE(SUM(CASE WHEN NOT classified.diagnostic AND classified.receipt_id IS NOT NULL AND classified.receipt_outcome <> 'success' THEN 1 ELSE 0 END), 0),
				COALESCE(SUM(CASE WHEN NOT classified.diagnostic AND classified.route_id IS NOT NULL AND classified.route_status = 'pending' THEN 1 ELSE 0 END), 0),
				COALESCE(SUM(CASE WHEN NOT classified.diagnostic AND classified.route_id IS NOT NULL AND classified.route_status = 'pending' AND classified.receipt_outcome = 'success' THEN 1 ELSE 0 END), 0),
				COALESCE(SUM(CASE WHEN classified.diagnostic THEN 1 ELSE 0 END), 0),
				run.status NOT IN ('running', 'paused'),
				run.status = 'forked'
			FROM runs run
			LEFT JOIN classified ON TRUE
			WHERE run.run_id = %s
			GROUP BY run.status`,
		diagnosticPredicate, runPlaceholder, runPlaceholder)
	err := q.QueryRowContext(ctx, query, args...).Scan(
		&out.Replayable, &out.Acknowledged, &out.TerminalNonSuccess, &out.Deferred,
		&out.ProcessedDeferred, &out.DiagnosticExcluded, &out.RunInactive, &out.RunForked)
	if errors.Is(err, sql.ErrNoRows) {
		return out, fmt.Errorf("pipeline run %s not found", out.RunID)
	}
	return out, err
}
