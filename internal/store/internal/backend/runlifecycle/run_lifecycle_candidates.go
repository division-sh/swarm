package runlifecycle

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

func (s *RunLifecyclePostgresOwner) RegisterCompletionCandidateSink(
	ctx context.Context,
	scope runtimerunlifecycle.CandidateScope,
	sink runtimerunlifecycle.CandidateSink,
) (runtimerunlifecycle.CandidateRegistration, error) {
	if s == nil {
		return nil, errors.New("postgres store is required")
	}
	return s.runLifecycleCandidates.Register(ctx, scope, sink)
}

func (s *RunLifecycleSQLiteOwner) RegisterCompletionCandidateSink(
	ctx context.Context,
	scope runtimerunlifecycle.CandidateScope,
	sink runtimerunlifecycle.CandidateSink,
) (runtimerunlifecycle.CandidateRegistration, error) {
	if s == nil {
		return nil, errors.New("sqlite runtime store is required")
	}
	return s.runLifecycleCandidates.Register(ctx, scope, sink)
}

func (s *RunLifecyclePostgresOwner) RequestCompletionCandidateTx(
	ctx context.Context,
	tx *sql.Tx,
	runID string,
	dueAt *time.Time,
	handoff *CandidateHandoff,
) (runtimerunlifecycle.CandidateRequestResult, error) {
	result, err := requestPostgresCompletionCandidateTx(ctx, tx, runID, dueAt, false)
	if err != nil {
		return runtimerunlifecycle.CandidateRequestResult{}, err
	}
	if err := handoff.Prepare(s.runLifecycleCandidates, result); err != nil {
		return runtimerunlifecycle.CandidateRequestResult{}, err
	}
	return result, nil
}

func (s *RunLifecycleSQLiteOwner) RequestCompletionCandidateTx(
	ctx context.Context,
	tx *sql.Tx,
	runID string,
	dueAt *time.Time,
	handoff *CandidateHandoff,
) (runtimerunlifecycle.CandidateRequestResult, error) {
	result, err := requestSQLiteCompletionCandidateTx(ctx, tx, runID, dueAt, s.now(), false)
	if err != nil {
		return runtimerunlifecycle.CandidateRequestResult{}, err
	}
	if err := handoff.Prepare(s.runLifecycleCandidates, result); err != nil {
		return runtimerunlifecycle.CandidateRequestResult{}, err
	}
	return result, nil
}

func (s *RunLifecyclePostgresOwner) RequestCompletionCandidate(
	ctx context.Context,
	request runtimerunlifecycle.CandidateRequest,
) (runtimerunlifecycle.CandidateRequestDisposition, error) {
	if err := request.Validate(); err != nil {
		return "", err
	}
	handoff, err := ReserveCandidateHandoff(ctx)
	if err != nil {
		return "", err
	}
	defer handoff.Rollback()
	var dueAt *time.Time
	if request.Timing == runtimerunlifecycle.CandidateAt {
		dueAt = &request.DueAt
	}
	var result runtimerunlifecycle.CandidateRequestResult
	err = s.runPostgresRuntimeMutation(ctx, func(txctx context.Context, tx *sql.Tx) error {
		result, err = s.RequestCompletionCandidateTx(txctx, tx, request.RunID, dueAt, handoff)
		return err
	})
	if err != nil {
		return "", err
	}
	return result.Disposition, handoff.Commit()
}

func (s *RunLifecycleSQLiteOwner) RequestCompletionCandidate(
	ctx context.Context,
	request runtimerunlifecycle.CandidateRequest,
) (runtimerunlifecycle.CandidateRequestDisposition, error) {
	if err := request.Validate(); err != nil {
		return "", err
	}
	handoff, err := ReserveCandidateHandoff(ctx)
	if err != nil {
		return "", err
	}
	defer handoff.Rollback()
	var dueAt *time.Time
	if request.Timing == runtimerunlifecycle.CandidateAt {
		dueAt = &request.DueAt
	}
	var result runtimerunlifecycle.CandidateRequestResult
	err = s.runRuntimeMutation(ctx, "sqlite request completion candidate", func(txctx context.Context, tx *sql.Tx) error {
		result, err = s.RequestCompletionCandidateTx(txctx, tx, request.RunID, dueAt, handoff)
		return err
	})
	if err != nil {
		return "", err
	}
	return result.Disposition, handoff.Commit()
}

func requestPostgresCompletionCandidateTx(
	ctx context.Context,
	tx *sql.Tx,
	runID string,
	dueAt *time.Time,
	forceRevision bool,
) (runtimerunlifecycle.CandidateRequestResult, error) {
	runID = nullUUIDString(runID)
	if tx == nil || runID == "" {
		return runtimerunlifecycle.CandidateRequestResult{}, errors.New("completion candidate requires transaction and run_id")
	}
	var (
		state       string
		bundleHash  string
		currentDue  sql.NullTime
		currentRev  int64
		selectedNow time.Time
	)
	if err := tx.QueryRowContext(ctx, `
		SELECT LOWER(status), bundle_hash, completion_due_at, completion_revision, clock_timestamp()
		FROM runs
		WHERE run_id = $1::uuid
		FOR UPDATE
	`, runID).Scan(&state, &bundleHash, &currentDue, &currentRev, &selectedNow); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return runtimerunlifecycle.CandidateRequestResult{}, &runtimerunlifecycle.RunNotFoundError{RunID: runID}
		}
		return runtimerunlifecycle.CandidateRequestResult{}, fmt.Errorf("lock completion candidate: %w", err)
	}
	lifecycleState, err := runtimerunlifecycle.ParseState(state)
	if err != nil {
		return runtimerunlifecycle.CandidateRequestResult{}, err
	}
	if lifecycleState.Terminal() {
		return runtimerunlifecycle.CandidateRequestResult{Disposition: runtimerunlifecycle.CandidateAbsorbedTerminal}, nil
	}
	if lifecycleState == runtimerunlifecycle.StatePaused {
		return runtimerunlifecycle.CandidateRequestResult{Disposition: runtimerunlifecycle.CandidateDeferredPaused}, nil
	}
	selectedNow = runtimerunlifecycle.CanonicalTimestamp(selectedNow)
	requestedDue := selectedNow
	if dueAt != nil {
		requestedDue = runtimerunlifecycle.CanonicalTimestamp(*dueAt)
	}
	currentDueAt := runtimerunlifecycle.CanonicalTimestamp(currentDue.Time)
	currentIsImmediate := currentDue.Valid && !currentDueAt.After(selectedNow)
	sameCoordinate := currentDue.Valid && currentDueAt.Equal(requestedDue)
	if !forceRevision && (sameCoordinate || currentIsImmediate) {
		result := runtimerunlifecycle.CandidateRequestResult{
			Disposition: runtimerunlifecycle.CandidateAlreadyCurrent,
			Candidate: runtimerunlifecycle.Candidate{
				RunID: runID, BundleHash: strings.TrimSpace(bundleHash),
				Revision: currentRev, DueAt: currentDueAt,
			},
		}
		return result, result.Validate()
	}
	var candidate runtimerunlifecycle.Candidate
	if err := tx.QueryRowContext(ctx, `
		UPDATE runs
		SET completion_revision = completion_revision + 1,
		    completion_due_at = $2
		WHERE run_id = $1::uuid
		  AND status = $3
		RETURNING run_id::text, bundle_hash, completion_revision, completion_due_at
	`, runID, requestedDue, string(runtimerunlifecycle.StateRunning)).Scan(
		&candidate.RunID,
		&candidate.BundleHash,
		&candidate.Revision,
		&candidate.DueAt,
	); err != nil {
		return runtimerunlifecycle.CandidateRequestResult{}, fmt.Errorf("request completion candidate: %w", err)
	}
	candidate.DueAt = runtimerunlifecycle.CanonicalTimestamp(candidate.DueAt)
	result := runtimerunlifecycle.CandidateRequestResult{Disposition: runtimerunlifecycle.CandidateRequested, Candidate: candidate}
	return result, result.Validate()
}

func RequestPostgresCompletionCandidateTx(ctx context.Context, tx *sql.Tx, runID string, dueAt *time.Time) (runtimerunlifecycle.CandidateRequestResult, error) {
	return requestPostgresCompletionCandidateTx(ctx, tx, runID, dueAt, false)
}

func requestSQLiteCompletionCandidateTx(
	ctx context.Context,
	tx *sql.Tx,
	runID string,
	dueAt *time.Time,
	selectedNow time.Time,
	forceRevision bool,
) (runtimerunlifecycle.CandidateRequestResult, error) {
	runID = nullUUIDString(runID)
	if tx == nil || runID == "" {
		return runtimerunlifecycle.CandidateRequestResult{}, errors.New("completion candidate requires transaction and run_id")
	}
	var (
		state      string
		bundleHash string
		currentDue any
		currentRev int64
	)
	if err := tx.QueryRowContext(ctx, `
		SELECT LOWER(status), bundle_hash, completion_due_at, completion_revision
		FROM runs
		WHERE run_id = ?
	`, runID).Scan(&state, &bundleHash, &currentDue, &currentRev); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return runtimerunlifecycle.CandidateRequestResult{}, &runtimerunlifecycle.RunNotFoundError{RunID: runID}
		}
		return runtimerunlifecycle.CandidateRequestResult{}, fmt.Errorf("lock sqlite completion candidate: %w", err)
	}
	lifecycleState, err := runtimerunlifecycle.ParseState(state)
	if err != nil {
		return runtimerunlifecycle.CandidateRequestResult{}, err
	}
	if lifecycleState.Terminal() {
		return runtimerunlifecycle.CandidateRequestResult{Disposition: runtimerunlifecycle.CandidateAbsorbedTerminal}, nil
	}
	if lifecycleState == runtimerunlifecycle.StatePaused {
		return runtimerunlifecycle.CandidateRequestResult{Disposition: runtimerunlifecycle.CandidateDeferredPaused}, nil
	}
	selectedNow = runtimerunlifecycle.CanonicalTimestamp(selectedNow)
	requestedDue := selectedNow
	if dueAt != nil {
		requestedDue = runtimerunlifecycle.CanonicalTimestamp(*dueAt)
	}
	if parsed, ok, err := sqliteTimeValue(currentDue); err != nil {
		return runtimerunlifecycle.CandidateRequestResult{}, err
	} else if !forceRevision && ok && (runtimerunlifecycle.CanonicalTimestamp(parsed).Equal(requestedDue) || !runtimerunlifecycle.CanonicalTimestamp(parsed).After(selectedNow)) {
		result := runtimerunlifecycle.CandidateRequestResult{
			Disposition: runtimerunlifecycle.CandidateAlreadyCurrent,
			Candidate: runtimerunlifecycle.Candidate{
				RunID: runID, BundleHash: strings.TrimSpace(bundleHash),
				Revision: currentRev, DueAt: runtimerunlifecycle.CanonicalTimestamp(parsed),
			},
		}
		return result, result.Validate()
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE runs
		SET completion_revision = completion_revision + 1,
		    completion_due_at = ?
		WHERE run_id = ?
		  AND status = ?
	`, requestedDue, runID, string(runtimerunlifecycle.StateRunning))
	if err != nil {
		return runtimerunlifecycle.CandidateRequestResult{}, fmt.Errorf("request sqlite completion candidate: %w", err)
	}
	if rows, rowsErr := result.RowsAffected(); rowsErr != nil || rows != 1 {
		return runtimerunlifecycle.CandidateRequestResult{}, fmt.Errorf("request sqlite completion candidate affected %d rows: %w", rows, rowsErr)
	}
	candidate := runtimerunlifecycle.Candidate{
		RunID: runID, BundleHash: strings.TrimSpace(bundleHash),
		Revision: currentRev + 1, DueAt: requestedDue,
	}
	request := runtimerunlifecycle.CandidateRequestResult{Disposition: runtimerunlifecycle.CandidateRequested, Candidate: candidate}
	return request, request.Validate()
}

func RequestSQLiteCompletionCandidateTx(ctx context.Context, tx *sql.Tx, runID string, dueAt *time.Time, now time.Time) (runtimerunlifecycle.CandidateRequestResult, error) {
	return requestSQLiteCompletionCandidateTx(ctx, tx, runID, dueAt, now, false)
}

func clearPostgresCompletionCandidateTx(ctx context.Context, tx *sql.Tx, runID string) error {
	if tx == nil {
		return errors.New("clear completion candidate requires transaction")
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE runs
		SET completion_due_at = NULL
		WHERE run_id = $1::uuid
	`, nullUUIDString(runID)); err != nil {
		return fmt.Errorf("clear completion candidate: %w", err)
	}
	return nil
}

func clearSQLiteCompletionCandidateTx(ctx context.Context, tx *sql.Tx, runID string) error {
	if tx == nil {
		return errors.New("clear sqlite completion candidate requires transaction")
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE runs
		SET completion_due_at = NULL
		WHERE run_id = ?
	`, nullUUIDString(runID)); err != nil {
		return fmt.Errorf("clear sqlite completion candidate: %w", err)
	}
	return nil
}

func transitionPostgresActiveRunStateTx(
	ctx context.Context,
	tx *sql.Tx,
	runID string,
	state runtimerunlifecycle.State,
	current runtimerunlifecycle.State,
) (sql.Result, error) {
	return tx.ExecContext(ctx, `
		UPDATE runs
		SET status = $2,
		    ended_at = NULL,
		    failure = NULL,
		    completion_due_at = NULL
		WHERE run_id = $1::uuid
		  AND status = $3
	`, runID, string(state), string(current))
}

func transitionSQLiteActiveRunStateTx(
	ctx context.Context,
	tx *sql.Tx,
	runID string,
	state runtimerunlifecycle.State,
	current runtimerunlifecycle.State,
) (sql.Result, error) {
	return tx.ExecContext(ctx, `
		UPDATE runs
		SET status = ?,
		    ended_at = NULL,
		    failure = NULL,
		    completion_due_at = NULL
		WHERE run_id = ?
		  AND status = ?
	`, string(state), runID, string(current))
}

func (s *RunLifecyclePostgresOwner) ListCompletionCandidates(
	ctx context.Context,
	scope runtimerunlifecycle.CandidateScope,
	cursor runtimerunlifecycle.CandidateCursor,
	limit int,
) (runtimerunlifecycle.CandidatePage, error) {
	if s == nil || s.backend == nil {
		return runtimerunlifecycle.CandidatePage{}, errors.New("postgres store is required")
	}
	if err := scope.Validate(); err != nil {
		return runtimerunlifecycle.CandidatePage{}, err
	}
	if limit <= 0 {
		return runtimerunlifecycle.CandidatePage{}, errors.New("completion candidate page limit must be positive")
	}
	rows, err := s.backend.QueryContext(ctx, `
		SELECT run_id::text, bundle_hash, completion_revision, completion_due_at
		FROM runs
		WHERE bundle_hash = $1
		  AND completion_due_at IS NOT NULL
		  AND status = 'running'
		  AND run_id::text > $2
		ORDER BY run_id::text
		LIMIT $3
	`, strings.TrimSpace(scope.BundleHash), strings.TrimSpace(cursor.RunID), limit)
	if err != nil {
		return runtimerunlifecycle.CandidatePage{}, fmt.Errorf("list completion candidates: %w", err)
	}
	defer rows.Close()
	page := runtimerunlifecycle.CandidatePage{Candidates: make([]runtimerunlifecycle.Candidate, 0, limit)}
	for rows.Next() {
		var candidate runtimerunlifecycle.Candidate
		if err := rows.Scan(&candidate.RunID, &candidate.BundleHash, &candidate.Revision, &candidate.DueAt); err != nil {
			return runtimerunlifecycle.CandidatePage{}, fmt.Errorf("scan completion candidate: %w", err)
		}
		candidate.DueAt = runtimerunlifecycle.CanonicalTimestamp(candidate.DueAt)
		if err := candidate.Validate(); err != nil {
			return runtimerunlifecycle.CandidatePage{}, err
		}
		page.Candidates = append(page.Candidates, candidate)
		page.Next.RunID = candidate.RunID
	}
	if err := rows.Err(); err != nil {
		return runtimerunlifecycle.CandidatePage{}, fmt.Errorf("read completion candidates: %w", err)
	}
	page.Exhausted = len(page.Candidates) < limit
	return page, nil
}

func (s *RunLifecycleSQLiteOwner) ListCompletionCandidates(
	ctx context.Context,
	scope runtimerunlifecycle.CandidateScope,
	cursor runtimerunlifecycle.CandidateCursor,
	limit int,
) (runtimerunlifecycle.CandidatePage, error) {
	if s == nil || s.backend == nil {
		return runtimerunlifecycle.CandidatePage{}, errors.New("sqlite runtime store is required")
	}
	if err := scope.Validate(); err != nil {
		return runtimerunlifecycle.CandidatePage{}, err
	}
	if limit <= 0 {
		return runtimerunlifecycle.CandidatePage{}, errors.New("completion candidate page limit must be positive")
	}
	rows, err := s.backend.QueryContext(ctx, `
		SELECT run_id, bundle_hash, completion_revision, completion_due_at
		FROM runs
		WHERE bundle_hash = ?
		  AND completion_due_at IS NOT NULL
		  AND status = 'running'
		  AND run_id > ?
		ORDER BY run_id
		LIMIT ?
	`, strings.TrimSpace(scope.BundleHash), strings.TrimSpace(cursor.RunID), limit)
	if err != nil {
		return runtimerunlifecycle.CandidatePage{}, fmt.Errorf("list sqlite completion candidates: %w", err)
	}
	defer rows.Close()
	page := runtimerunlifecycle.CandidatePage{Candidates: make([]runtimerunlifecycle.Candidate, 0, limit)}
	for rows.Next() {
		var (
			candidate runtimerunlifecycle.Candidate
			dueAt     any
		)
		if err := rows.Scan(&candidate.RunID, &candidate.BundleHash, &candidate.Revision, &dueAt); err != nil {
			return runtimerunlifecycle.CandidatePage{}, fmt.Errorf("scan sqlite completion candidate: %w", err)
		}
		parsed, ok, err := sqliteTimeValue(dueAt)
		if err != nil || !ok {
			return runtimerunlifecycle.CandidatePage{}, fmt.Errorf("decode sqlite completion candidate due_at: %w", err)
		}
		candidate.DueAt = runtimerunlifecycle.CanonicalTimestamp(parsed)
		if err := candidate.Validate(); err != nil {
			return runtimerunlifecycle.CandidatePage{}, err
		}
		page.Candidates = append(page.Candidates, candidate)
		page.Next.RunID = candidate.RunID
	}
	if err := rows.Err(); err != nil {
		return runtimerunlifecycle.CandidatePage{}, fmt.Errorf("read sqlite completion candidates: %w", err)
	}
	page.Exhausted = len(page.Candidates) < limit
	return page, nil
}

func (s *RunLifecyclePostgresOwner) ExecuteCompletionCandidate(
	ctx context.Context,
	candidate runtimerunlifecycle.Candidate,
	catalog runtimerunlifecycle.TerminalCatalog,
) (runtimerunlifecycle.CompletionResult, error) {
	if err := candidate.Validate(); err != nil {
		return runtimerunlifecycle.CompletionResult{}, err
	}
	var outcome runtimerunlifecycle.CompletionResult
	effects := privaterunforkrevision.NewEffects()
	err := s.runPrivateAuthorActivityMutation(ctx, effects, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
		var err error
		outcome, err = s.executeCompletionCandidateTx(txctx, tx, story, effects, candidate, catalog)
		return err
	})
	return outcome, err
}

func (s *RunLifecycleSQLiteOwner) ExecuteCompletionCandidate(
	ctx context.Context,
	candidate runtimerunlifecycle.Candidate,
	catalog runtimerunlifecycle.TerminalCatalog,
) (runtimerunlifecycle.CompletionResult, error) {
	if err := candidate.Validate(); err != nil {
		return runtimerunlifecycle.CompletionResult{}, err
	}
	var outcome runtimerunlifecycle.CompletionResult
	effects := privaterunforkrevision.NewEffects()
	err := s.runPrivateAuthorActivityMutation(ctx, "sqlite execute run completion candidate", effects, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
		var err error
		outcome, err = s.executeCompletionCandidateTx(txctx, tx, story, effects, candidate, catalog)
		return err
	})
	return outcome, err
}

func (s *RunLifecyclePostgresOwner) executeCompletionCandidateTx(
	ctx context.Context,
	tx *sql.Tx,
	story *privateauthoractivity.Mutation,
	effects *privaterunforkrevision.Effects,
	candidate runtimerunlifecycle.Candidate,
	catalog runtimerunlifecycle.TerminalCatalog,
) (runtimerunlifecycle.CompletionResult, error) {
	var (
		state       string
		bundleHash  string
		currentRev  int64
		currentDue  sql.NullTime
		selectedNow time.Time
	)
	err := tx.QueryRowContext(ctx, `
		SELECT LOWER(status), bundle_hash, completion_revision, completion_due_at,
		       clock_timestamp()
		FROM runs
		WHERE run_id = $1::uuid
		FOR UPDATE
	`, candidate.RunID).Scan(&state, &bundleHash, &currentRev, &currentDue, &selectedNow)
	if errors.Is(err, sql.ErrNoRows) {
		return runtimerunlifecycle.CompletionResult{Outcome: runtimerunlifecycle.OutcomeExactNoop}, nil
	}
	if err != nil {
		return runtimerunlifecycle.CompletionResult{}, err
	}
	if strings.TrimSpace(bundleHash) != candidate.BundleHash || !currentDue.Valid || currentRev != candidate.Revision {
		return runtimerunlifecycle.CompletionResult{Outcome: runtimerunlifecycle.OutcomeExactNoop}, nil
	}
	storedDue := runtimerunlifecycle.CanonicalTimestamp(currentDue.Time)
	if !storedDue.Equal(candidate.DueAt) {
		return runtimerunlifecycle.CompletionResult{Outcome: runtimerunlifecycle.OutcomeExactNoop}, nil
	}
	current := candidate
	selectedNow = runtimerunlifecycle.CanonicalTimestamp(selectedNow)
	current.DueAt = storedDue
	if current.DueAt.After(selectedNow) {
		return runtimerunlifecycle.CompletionResult{Outcome: runtimerunlifecycle.OutcomeRearmAt, Candidate: current}, nil
	}
	lifecycleState, err := runtimerunlifecycle.ParseState(state)
	if err != nil {
		return runtimerunlifecycle.CompletionResult{}, err
	}
	if lifecycleState.Terminal() {
		if _, err := tx.ExecContext(ctx, `
			UPDATE runs SET completion_due_at = NULL
			WHERE run_id = $1::uuid AND completion_revision = $2
		`, candidate.RunID, candidate.Revision); err != nil {
			return runtimerunlifecycle.CompletionResult{}, err
		}
		return runtimerunlifecycle.CompletionResult{Outcome: runtimerunlifecycle.OutcomeExactNoop}, nil
	}
	if lifecycleState == runtimerunlifecycle.StatePaused {
		return runtimerunlifecycle.CompletionResult{}, errors.New("paused run carries an executable completion candidate")
	}
	standalone := false
	snapshot, loadErr := loadPostgresRunLifecycleSnapshot(ctx, tx, candidate.RunID, false)
	if loadErr != nil {
		return runtimerunlifecycle.CompletionResult{}, loadErr
	}
	if selectedNow.Before(snapshot.StartedAt) {
		return runtimerunlifecycle.CompletionResult{
			Outcome: runtimerunlifecycle.OutcomeRetryCurrent,
			Retryable: &runtimerunlifecycle.SelectedStoreBeforeRunStartError{
				RunID:      candidate.RunID,
				SelectedAt: selectedNow,
				StartedAt:  snapshot.StartedAt,
			},
		}, nil
	}
	if snapshot.Origin.Kind() == runtimerunlifecycle.OriginEvent {
		rec, found, loadErr := loadPostgresStandaloneRuntimePlatformRunRecord(ctx, tx, snapshot.Origin.EventID())
		if loadErr != nil {
			return runtimerunlifecycle.CompletionResult{}, loadErr
		}
		standalone = found && isStandaloneRuntimePlatformRunRecord(rec)
	}
	if standalone {
		summary, err := postgresDeliveryAdapter.SummarizeRun(ctx, tx, candidate.RunID)
		if err != nil {
			return runtimerunlifecycle.CompletionResult{}, err
		}
		if !summary.Settled() {
			return s.finishBlockedPostgresCandidate(ctx, tx, candidate, nil)
		}
	} else {
		if catalog.Empty() {
			return runtimerunlifecycle.CompletionResult{}, errors.New("normal run completion requires terminal catalog")
		}
		if err := s.pipeline.AdvanceFanOutDeliveryBarriersTx(ctx, tx, effects, candidate.RunID, selectedNow); err != nil {
			return runtimerunlifecycle.CompletionResult{}, fmt.Errorf("advance fan-out delivery barriers: %w", err)
		}
		summaries, err := s.loadPostgresRunCompletionOwnerSummaries(ctx, tx, candidate.RunID, selectedNow, catalog)
		if err != nil {
			return runtimerunlifecycle.CompletionResult{}, err
		}
		if summaries.blocksCompletion() {
			return s.finishBlockedPostgresCandidate(ctx, tx, candidate, optionalWake(summaries.Sessions.NextExpiry))
		}
	}
	if _, _, err := s.completeRunTx(ctx, tx, story, effects, candidate.RunID, selectedNow); err != nil {
		return runtimerunlifecycle.CompletionResult{}, err
	}
	return runtimerunlifecycle.CompletionResult{Outcome: runtimerunlifecycle.OutcomeTerminallyEligible}, nil
}

func (s *RunLifecyclePostgresOwner) finishBlockedPostgresCandidate(
	ctx context.Context,
	tx *sql.Tx,
	candidate runtimerunlifecycle.Candidate,
	nextWake *time.Time,
) (runtimerunlifecycle.CompletionResult, error) {
	if nextWake != nil {
		request, err := requestPostgresCompletionCandidateTx(ctx, tx, candidate.RunID, nextWake, true)
		if err != nil {
			return runtimerunlifecycle.CompletionResult{}, err
		}
		return runtimerunlifecycle.CompletionResult{Outcome: runtimerunlifecycle.OutcomeRearmAt, Candidate: request.Candidate}, nil
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE runs SET completion_due_at = NULL
		WHERE run_id = $1::uuid AND completion_revision = $2
	`, candidate.RunID, candidate.Revision); err != nil {
		return runtimerunlifecycle.CompletionResult{}, err
	}
	return runtimerunlifecycle.CompletionResult{Outcome: runtimerunlifecycle.OutcomeAwaitMutation}, nil
}

func (s *RunLifecycleSQLiteOwner) executeCompletionCandidateTx(
	ctx context.Context,
	tx *sql.Tx,
	story *privateauthoractivity.Mutation,
	effects *privaterunforkrevision.Effects,
	candidate runtimerunlifecycle.Candidate,
	catalog runtimerunlifecycle.TerminalCatalog,
) (runtimerunlifecycle.CompletionResult, error) {
	var (
		state      string
		bundleHash string
		currentRev int64
		currentDue any
	)
	err := tx.QueryRowContext(ctx, `
		SELECT LOWER(status), bundle_hash, completion_revision, completion_due_at
		FROM runs
		WHERE run_id = ?
	`, candidate.RunID).Scan(&state, &bundleHash, &currentRev, &currentDue)
	if errors.Is(err, sql.ErrNoRows) {
		return runtimerunlifecycle.CompletionResult{Outcome: runtimerunlifecycle.OutcomeExactNoop}, nil
	}
	if err != nil {
		return runtimerunlifecycle.CompletionResult{}, err
	}
	dueAt, duePresent, err := sqliteTimeValue(currentDue)
	if err != nil {
		return runtimerunlifecycle.CompletionResult{}, err
	}
	if strings.TrimSpace(bundleHash) != candidate.BundleHash || !duePresent || currentRev != candidate.Revision {
		return runtimerunlifecycle.CompletionResult{Outcome: runtimerunlifecycle.OutcomeExactNoop}, nil
	}
	storedDue := runtimerunlifecycle.CanonicalTimestamp(dueAt)
	if !storedDue.Equal(candidate.DueAt) {
		return runtimerunlifecycle.CompletionResult{Outcome: runtimerunlifecycle.OutcomeExactNoop}, nil
	}
	selectedNow := runtimerunlifecycle.CanonicalTimestamp(s.now())
	current := candidate
	current.DueAt = storedDue
	if current.DueAt.After(selectedNow) {
		return runtimerunlifecycle.CompletionResult{Outcome: runtimerunlifecycle.OutcomeRearmAt, Candidate: current}, nil
	}
	lifecycleState, err := runtimerunlifecycle.ParseState(state)
	if err != nil {
		return runtimerunlifecycle.CompletionResult{}, err
	}
	if lifecycleState.Terminal() {
		if _, err := tx.ExecContext(ctx, `
			UPDATE runs SET completion_due_at = NULL
			WHERE run_id = ? AND completion_revision = ?
		`, candidate.RunID, candidate.Revision); err != nil {
			return runtimerunlifecycle.CompletionResult{}, err
		}
		return runtimerunlifecycle.CompletionResult{Outcome: runtimerunlifecycle.OutcomeExactNoop}, nil
	}
	if lifecycleState == runtimerunlifecycle.StatePaused {
		return runtimerunlifecycle.CompletionResult{}, errors.New("paused run carries an executable completion candidate")
	}
	standalone := false
	snapshot, loadErr := loadSQLiteRunLifecycleSnapshot(ctx, tx, candidate.RunID)
	if loadErr != nil {
		return runtimerunlifecycle.CompletionResult{}, loadErr
	}
	if selectedNow.Before(snapshot.StartedAt) {
		return runtimerunlifecycle.CompletionResult{
			Outcome: runtimerunlifecycle.OutcomeRetryCurrent,
			Retryable: &runtimerunlifecycle.SelectedStoreBeforeRunStartError{
				RunID:      candidate.RunID,
				SelectedAt: selectedNow,
				StartedAt:  snapshot.StartedAt,
			},
		}, nil
	}
	if snapshot.Origin.Kind() == runtimerunlifecycle.OriginEvent {
		rec, found, loadErr := loadSQLiteStandaloneRuntimePlatformRunRecord(ctx, tx, snapshot.Origin.EventID())
		if loadErr != nil {
			return runtimerunlifecycle.CompletionResult{}, loadErr
		}
		standalone = found && isStandaloneRuntimePlatformRunRecord(rec)
	}
	if standalone {
		summary, err := sqliteDeliveryAdapter.SummarizeRun(ctx, tx, candidate.RunID)
		if err != nil {
			return runtimerunlifecycle.CompletionResult{}, err
		}
		if !summary.Settled() {
			return s.finishBlockedSQLiteCandidate(ctx, tx, candidate, nil, selectedNow)
		}
	} else {
		if catalog.Empty() {
			return runtimerunlifecycle.CompletionResult{}, errors.New("normal run completion requires terminal catalog")
		}
		if err := s.pipeline.AdvanceFanOutDeliveryBarriersTx(ctx, tx, effects, candidate.RunID, selectedNow); err != nil {
			return runtimerunlifecycle.CompletionResult{}, fmt.Errorf("advance sqlite fan-out delivery barriers: %w", err)
		}
		summaries, err := s.loadSQLiteRunCompletionOwnerSummaries(ctx, tx, candidate.RunID, selectedNow, catalog)
		if err != nil {
			return runtimerunlifecycle.CompletionResult{}, err
		}
		if summaries.blocksCompletion() {
			return s.finishBlockedSQLiteCandidate(ctx, tx, candidate, optionalWake(summaries.Sessions.NextExpiry), selectedNow)
		}
	}
	if _, _, err := s.completeRunTx(ctx, tx, story, effects, candidate.RunID, selectedNow); err != nil {
		return runtimerunlifecycle.CompletionResult{}, err
	}
	return runtimerunlifecycle.CompletionResult{Outcome: runtimerunlifecycle.OutcomeTerminallyEligible}, nil
}

func (s *RunLifecycleSQLiteOwner) finishBlockedSQLiteCandidate(
	ctx context.Context,
	tx *sql.Tx,
	candidate runtimerunlifecycle.Candidate,
	nextWake *time.Time,
	selectedNow time.Time,
) (runtimerunlifecycle.CompletionResult, error) {
	if nextWake != nil {
		request, err := requestSQLiteCompletionCandidateTx(ctx, tx, candidate.RunID, nextWake, selectedNow, true)
		if err != nil {
			return runtimerunlifecycle.CompletionResult{}, err
		}
		return runtimerunlifecycle.CompletionResult{Outcome: runtimerunlifecycle.OutcomeRearmAt, Candidate: request.Candidate}, nil
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE runs SET completion_due_at = NULL
		WHERE run_id = ? AND completion_revision = ?
	`, candidate.RunID, candidate.Revision); err != nil {
		return runtimerunlifecycle.CompletionResult{}, err
	}
	return runtimerunlifecycle.CompletionResult{Outcome: runtimerunlifecycle.OutcomeAwaitMutation}, nil
}

func optionalWake(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	value = value.UTC()
	return &value
}
