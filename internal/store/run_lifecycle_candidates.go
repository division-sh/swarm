package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

type runLifecycleCandidateSinkRegistry struct {
	mu    sync.Mutex
	sinks map[string]runtimerunlifecycle.CandidateSink
}

type runLifecycleCandidateRegistration struct {
	once    sync.Once
	release func()
}

type runLifecycleCandidateHandoffReservation struct {
	lease      *worklifetime.Lease
	handoffs   []runLifecycleCandidateHandoff
	identities map[string]struct{}
	settled    bool
}

type runLifecycleCandidateHandoff struct {
	admission runtimerunlifecycle.CandidateAdmission
	candidate runtimerunlifecycle.Candidate
}

func detachedRunLifecycleCandidateContext(ctx context.Context) context.Context {
	ctx = context.WithoutCancel(ctx)
	ctx = runtimepipeline.WithoutPipelineSQLTxContext(ctx)
	return runtimepipeline.WithoutPipelineSQLConnContext(ctx)
}

func reserveRunLifecycleCandidateHandoff(ctx context.Context) (*runLifecycleCandidateHandoffReservation, error) {
	owner, ok := worklifetime.OccurrenceFromContext(ctx)
	if !ok {
		return &runLifecycleCandidateHandoffReservation{}, nil
	}
	lease, err := owner.Begin(detachedRunLifecycleCandidateContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("reserve completion candidate handoff: %w", err)
	}
	return &runLifecycleCandidateHandoffReservation{lease: lease}, nil
}

func (r *runLifecycleCandidateHandoffReservation) prepare(
	sinks *runLifecycleCandidateSinkRegistry,
	result runtimerunlifecycle.CandidateRequestResult,
) error {
	if r == nil || r.lease == nil || result.Disposition == runtimerunlifecycle.CandidateAbsorbedTerminal {
		return nil
	}
	if err := result.Candidate.Validate(); err != nil {
		return err
	}
	sink := sinks.load(result.Candidate.BundleHash)
	if sink == nil {
		// No runtime generation is currently serving this selected store.
		// The durable candidate remains authoritative and startup enumeration
		// will represent it when a generation registers.
		return nil
	}
	identity := fmt.Sprintf("%s/%d", result.Candidate.RunID, result.Candidate.Revision)
	if r.identities == nil {
		r.identities = make(map[string]struct{})
	}
	if _, exists := r.identities[identity]; exists {
		return nil
	}
	admission, err := sink.ReserveCompletionCandidate(detachedRunLifecycleCandidateContext(r.lease.Context()))
	if err != nil {
		return fmt.Errorf("reserve completion candidate executor admission: %w", err)
	}
	r.identities[identity] = struct{}{}
	r.handoffs = append(r.handoffs, runLifecycleCandidateHandoff{
		admission: admission, candidate: result.Candidate,
	})
	return nil
}

func (r *runLifecycleCandidateHandoffReservation) commit() error {
	if r == nil || r.settled {
		return nil
	}
	r.settled = true
	var submitErr error
	for _, handoff := range r.handoffs {
		submitErr = errors.Join(
			submitErr,
			handoff.admission.Submit(handoff.candidate),
		)
	}
	if r.lease != nil {
		submitErr = errors.Join(submitErr, r.lease.Done())
	}
	return submitErr
}

func (r *runLifecycleCandidateHandoffReservation) rollback() {
	if r == nil || r.settled {
		return
	}
	r.settled = true
	for _, handoff := range r.handoffs {
		_ = handoff.admission.Cancel()
	}
	if r.lease != nil {
		_ = r.lease.Done()
	}
}

func (r *runLifecycleCandidateRegistration) Release() {
	if r != nil {
		r.once.Do(r.release)
	}
}

func (r *runLifecycleCandidateSinkRegistry) register(
	scope runtimerunlifecycle.CandidateScope,
	sink runtimerunlifecycle.CandidateSink,
) (runtimerunlifecycle.CandidateRegistration, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	if sink == nil {
		return nil, errors.New("completion candidate sink is required")
	}
	bundleHash := strings.TrimSpace(scope.BundleHash)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sinks == nil {
		r.sinks = make(map[string]runtimerunlifecycle.CandidateSink)
	}
	if _, exists := r.sinks[bundleHash]; exists {
		return nil, fmt.Errorf("completion candidate sink already registered for bundle_hash %s", bundleHash)
	}
	r.sinks[bundleHash] = sink
	return &runLifecycleCandidateRegistration{release: func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.sinks[bundleHash] == sink {
			delete(r.sinks, bundleHash)
		}
	}}, nil
}

func (r *runLifecycleCandidateSinkRegistry) load(bundleHash string) runtimerunlifecycle.CandidateSink {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.sinks[strings.TrimSpace(bundleHash)]
}

func (s *PostgresStore) RegisterCompletionCandidateSink(
	scope runtimerunlifecycle.CandidateScope,
	sink runtimerunlifecycle.CandidateSink,
) (runtimerunlifecycle.CandidateRegistration, error) {
	if s == nil {
		return nil, errors.New("postgres store is required")
	}
	return s.runLifecycleSinks.register(scope, sink)
}

func (s *SQLiteRuntimeStore) RegisterCompletionCandidateSink(
	scope runtimerunlifecycle.CandidateScope,
	sink runtimerunlifecycle.CandidateSink,
) (runtimerunlifecycle.CandidateRegistration, error) {
	if s == nil {
		return nil, errors.New("sqlite runtime store is required")
	}
	return s.runLifecycleSinks.register(scope, sink)
}

func queueCompletionCandidateSignal(
	ctx context.Context,
	sinks *runLifecycleCandidateSinkRegistry,
	candidate runtimerunlifecycle.Candidate,
) error {
	if err := candidate.Validate(); err != nil {
		return err
	}
	sink := sinks.load(candidate.BundleHash)
	if sink == nil {
		// A selected store may be mutated without a live runtime generation
		// (operator/test setup). Startup enumeration owns the later handoff.
		return nil
	}
	admission, err := sink.ReserveCompletionCandidate(detachedRunLifecycleCandidateContext(ctx))
	if err != nil {
		return fmt.Errorf("reserve completion candidate executor admission: %w", err)
	}
	if !runtimepipeline.QueueRunLifecycleCandidateHandoff(ctx, admission, candidate) {
		_ = admission.Cancel()
		return errors.New("completion candidate handoff could not reserve runtime work before commit")
	}
	return nil
}

func (s *PostgresStore) requestCompletionCandidateTx(
	ctx context.Context,
	tx *sql.Tx,
	runID string,
	dueAt *time.Time,
) (runtimerunlifecycle.CandidateRequestResult, error) {
	result, err := requestPostgresCompletionCandidateTx(ctx, tx, runID, dueAt, false)
	if err != nil {
		return runtimerunlifecycle.CandidateRequestResult{}, err
	}
	if result.Disposition != runtimerunlifecycle.CandidateAbsorbedTerminal {
		if err := queueCompletionCandidateSignal(ctx, &s.runLifecycleSinks, result.Candidate); err != nil {
			return runtimerunlifecycle.CandidateRequestResult{}, err
		}
	}
	return result, nil
}

func (s *SQLiteRuntimeStore) requestCompletionCandidateTx(
	ctx context.Context,
	tx *sql.Tx,
	runID string,
	dueAt *time.Time,
) (runtimerunlifecycle.CandidateRequestResult, error) {
	result, err := requestSQLiteCompletionCandidateTx(ctx, tx, runID, dueAt, s.now(), false)
	if err != nil {
		return runtimerunlifecycle.CandidateRequestResult{}, err
	}
	if result.Disposition != runtimerunlifecycle.CandidateAbsorbedTerminal {
		if err := queueCompletionCandidateSignal(ctx, &s.runLifecycleSinks, result.Candidate); err != nil {
			return runtimerunlifecycle.CandidateRequestResult{}, err
		}
	}
	return result, nil
}

func (s *PostgresStore) RequestCompletionCandidate(
	ctx context.Context,
	request runtimerunlifecycle.CandidateRequest,
) (runtimerunlifecycle.CandidateRequestDisposition, error) {
	if err := request.Validate(); err != nil {
		return "", err
	}
	tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx)
	if !ok || tx == nil {
		return "", errors.New("completion candidate request requires the current named mutation")
	}
	var dueAt *time.Time
	if request.Timing == runtimerunlifecycle.CandidateAt {
		dueAt = &request.DueAt
	}
	result, err := s.requestCompletionCandidateTx(ctx, tx, request.RunID, dueAt)
	if err != nil {
		return "", err
	}
	return result.Disposition, nil
}

func (s *SQLiteRuntimeStore) RequestCompletionCandidate(
	ctx context.Context,
	request runtimerunlifecycle.CandidateRequest,
) (runtimerunlifecycle.CandidateRequestDisposition, error) {
	if err := request.Validate(); err != nil {
		return "", err
	}
	tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx)
	if !ok || tx == nil {
		return "", errors.New("completion candidate request requires the current named mutation")
	}
	var dueAt *time.Time
	if request.Timing == runtimerunlifecycle.CandidateAt {
		dueAt = &request.DueAt
	}
	result, err := s.requestCompletionCandidateTx(ctx, tx, request.RunID, dueAt)
	if err != nil {
		return "", err
	}
	return result.Disposition, nil
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
		SELECT LOWER(status), bundle_hash, completion_due_at, completion_revision, CURRENT_TIMESTAMP
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
	requestedDue := selectedNow.UTC()
	if dueAt != nil {
		requestedDue = dueAt.UTC()
	}
	currentDueAt := currentDue.Time.UTC()
	currentIsImmediate := currentDue.Valid && !currentDueAt.After(selectedNow.UTC())
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
		  AND status IN (`+runLifecycleActiveStateSQLValues+`)
		RETURNING run_id::text, bundle_hash, completion_revision, completion_due_at
	`, runID, requestedDue).Scan(&candidate.RunID, &candidate.BundleHash, &candidate.Revision, &candidate.DueAt); err != nil {
		return runtimerunlifecycle.CandidateRequestResult{}, fmt.Errorf("request completion candidate: %w", err)
	}
	candidate.DueAt = candidate.DueAt.UTC()
	result := runtimerunlifecycle.CandidateRequestResult{Disposition: runtimerunlifecycle.CandidateRequested, Candidate: candidate}
	return result, result.Validate()
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
	requestedDue := selectedNow.UTC()
	if dueAt != nil {
		requestedDue = dueAt.UTC()
	}
	if parsed, ok, err := sqliteTimeValue(currentDue); err != nil {
		return runtimerunlifecycle.CandidateRequestResult{}, err
	} else if !forceRevision && ok && (parsed.UTC().Equal(requestedDue) || !parsed.UTC().After(selectedNow.UTC())) {
		result := runtimerunlifecycle.CandidateRequestResult{
			Disposition: runtimerunlifecycle.CandidateAlreadyCurrent,
			Candidate: runtimerunlifecycle.Candidate{
				RunID: runID, BundleHash: strings.TrimSpace(bundleHash),
				Revision: currentRev, DueAt: parsed.UTC(),
			},
		}
		return result, result.Validate()
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE runs
		SET completion_revision = completion_revision + 1,
		    completion_due_at = ?
		WHERE run_id = ?
		  AND status IN (`+runLifecycleActiveStateSQLValues+`)
	`, requestedDue, runID)
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

func (s *PostgresStore) ListCompletionCandidates(
	ctx context.Context,
	scope runtimerunlifecycle.CandidateScope,
	cursor runtimerunlifecycle.CandidateCursor,
	limit int,
) (runtimerunlifecycle.CandidatePage, error) {
	if s == nil || s.DB == nil {
		return runtimerunlifecycle.CandidatePage{}, errors.New("postgres store is required")
	}
	if err := scope.Validate(); err != nil {
		return runtimerunlifecycle.CandidatePage{}, err
	}
	if limit <= 0 {
		return runtimerunlifecycle.CandidatePage{}, errors.New("completion candidate page limit must be positive")
	}
	rows, err := s.DB.QueryContext(ctx, `
		SELECT run_id::text, bundle_hash, completion_revision, completion_due_at
		FROM runs
		WHERE bundle_hash = $1
		  AND completion_due_at IS NOT NULL
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
		candidate.DueAt = candidate.DueAt.UTC()
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

func (s *SQLiteRuntimeStore) ListCompletionCandidates(
	ctx context.Context,
	scope runtimerunlifecycle.CandidateScope,
	cursor runtimerunlifecycle.CandidateCursor,
	limit int,
) (runtimerunlifecycle.CandidatePage, error) {
	if s == nil || s.DB == nil {
		return runtimerunlifecycle.CandidatePage{}, errors.New("sqlite runtime store is required")
	}
	if err := scope.Validate(); err != nil {
		return runtimerunlifecycle.CandidatePage{}, err
	}
	if limit <= 0 {
		return runtimerunlifecycle.CandidatePage{}, errors.New("completion candidate page limit must be positive")
	}
	rows, err := s.DB.QueryContext(ctx, `
		SELECT run_id, bundle_hash, completion_revision, completion_due_at
		FROM runs
		WHERE bundle_hash = ?
		  AND completion_due_at IS NOT NULL
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
		candidate.DueAt = parsed.UTC()
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

func (s *PostgresStore) ExecuteCompletionCandidate(
	ctx context.Context,
	candidate runtimerunlifecycle.Candidate,
	catalog runtimerunlifecycle.TerminalCatalog,
) (runtimerunlifecycle.CompletionResult, error) {
	if err := candidate.Validate(); err != nil {
		return runtimerunlifecycle.CompletionResult{}, err
	}
	var outcome runtimerunlifecycle.CompletionResult
	err := s.runAuthorActivityMutation(ctx, "postgres execute run completion candidate", func(txctx context.Context, tx *sql.Tx) error {
		var err error
		outcome, err = s.executeCompletionCandidateTx(txctx, tx, candidate, catalog)
		return err
	})
	return outcome, err
}

func (s *SQLiteRuntimeStore) ExecuteCompletionCandidate(
	ctx context.Context,
	candidate runtimerunlifecycle.Candidate,
	catalog runtimerunlifecycle.TerminalCatalog,
) (runtimerunlifecycle.CompletionResult, error) {
	if err := candidate.Validate(); err != nil {
		return runtimerunlifecycle.CompletionResult{}, err
	}
	var outcome runtimerunlifecycle.CompletionResult
	err := s.runAuthorActivityMutation(ctx, "sqlite execute run completion candidate", func(txctx context.Context, tx *sql.Tx) error {
		var err error
		outcome, err = s.executeCompletionCandidateTx(txctx, tx, candidate, catalog)
		return err
	})
	return outcome, err
}

func (s *PostgresStore) executeCompletionCandidateTx(
	ctx context.Context,
	tx *sql.Tx,
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
		       CURRENT_TIMESTAMP
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
	current := candidate
	current.DueAt = currentDue.Time.UTC()
	if current.DueAt.After(selectedNow.UTC()) {
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
		sessions, err := summarizeSessionRun(ctx, tx, candidate.RunID, selectedNow.UTC(), true)
		if err != nil {
			return runtimerunlifecycle.CompletionResult{}, err
		}
		return s.finishBlockedPostgresCandidate(ctx, tx, candidate, optionalWake(sessions.NextExpiry))
	}
	standalone := false
	snapshot, loadErr := loadPostgresRunLifecycleSnapshot(ctx, tx, candidate.RunID, false)
	if loadErr != nil {
		return runtimerunlifecycle.CompletionResult{}, loadErr
	}
	if snapshot.Origin.Kind() == runtimerunlifecycle.OriginEvent {
		rec, found, loadErr := loadStandaloneRuntimePlatformRunRecord(ctx, tx, snapshot.Origin.EventID())
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
		summaries, err := loadPostgresRunCompletionOwnerSummaries(ctx, tx, candidate.RunID, selectedNow.UTC(), catalog)
		if err != nil {
			return runtimerunlifecycle.CompletionResult{}, err
		}
		if summaries.blocksCompletion() {
			return s.finishBlockedPostgresCandidate(ctx, tx, candidate, optionalWake(summaries.Sessions.NextExpiry))
		}
	}
	if _, _, err := s.completeRunTx(ctx, tx, candidate.RunID, selectedNow.UTC()); err != nil {
		return runtimerunlifecycle.CompletionResult{}, err
	}
	return runtimerunlifecycle.CompletionResult{Outcome: runtimerunlifecycle.OutcomeTerminallyEligible}, nil
}

func (s *PostgresStore) finishBlockedPostgresCandidate(
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

func (s *SQLiteRuntimeStore) executeCompletionCandidateTx(
	ctx context.Context,
	tx *sql.Tx,
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
	selectedNow := s.now()
	current := candidate
	current.DueAt = dueAt.UTC()
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
		sessions, err := summarizeSessionRun(ctx, tx, candidate.RunID, selectedNow, false)
		if err != nil {
			return runtimerunlifecycle.CompletionResult{}, err
		}
		return s.finishBlockedSQLiteCandidate(ctx, tx, candidate, optionalWake(sessions.NextExpiry), selectedNow)
	}
	standalone := false
	snapshot, loadErr := loadSQLiteRunLifecycleSnapshot(ctx, tx, candidate.RunID)
	if loadErr != nil {
		return runtimerunlifecycle.CompletionResult{}, loadErr
	}
	if snapshot.Origin.Kind() == runtimerunlifecycle.OriginEvent {
		rec, found, loadErr := sqliteLoadStandaloneRuntimePlatformRunRecord(ctx, tx, snapshot.Origin.EventID())
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
		summaries, err := loadSQLiteRunCompletionOwnerSummaries(ctx, tx, candidate.RunID, selectedNow, catalog)
		if err != nil {
			return runtimerunlifecycle.CompletionResult{}, err
		}
		if summaries.blocksCompletion() {
			return s.finishBlockedSQLiteCandidate(ctx, tx, candidate, optionalWake(summaries.Sessions.NextExpiry), selectedNow)
		}
	}
	if _, _, err := s.completeRunTx(ctx, tx, candidate.RunID, selectedNow); err != nil {
		return runtimerunlifecycle.CompletionResult{}, err
	}
	return runtimerunlifecycle.CompletionResult{Outcome: runtimerunlifecycle.OutcomeTerminallyEligible}, nil
}

func (s *SQLiteRuntimeStore) finishBlockedSQLiteCandidate(
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

func eventRunIDForCompletionCandidateTx(ctx context.Context, tx *sql.Tx, eventID string, postgres bool) (string, error) {
	if tx == nil {
		return "", errors.New("completion candidate event lookup requires transaction")
	}
	var runID string
	query := `SELECT COALESCE(run_id, '') FROM events WHERE event_id = ?`
	if postgres {
		query = `SELECT COALESCE(run_id::text, '') FROM events WHERE event_id = $1::uuid`
	}
	if err := tx.QueryRowContext(ctx, query, strings.TrimSpace(eventID)).Scan(&runID); err != nil {
		return "", fmt.Errorf("load completion candidate event run: %w", err)
	}
	runID = strings.TrimSpace(runID)
	return runID, nil
}
