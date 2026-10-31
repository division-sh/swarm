package runlifecycle

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
)

func ReadStandaloneCompletionCandidateTx(ctx context.Context, tx *sql.Tx, postgres bool, eventID string) (runtimerunlifecycle.Candidate, error) {
	if tx == nil {
		return runtimerunlifecycle.Candidate{}, fmt.Errorf("standalone candidate evidence requires its selected read transaction")
	}
	var candidate runtimerunlifecycle.Candidate
	query := `SELECT CAST(r.run_id AS TEXT),r.bundle_hash,COALESCE(c.revision,0),` + completionEvidenceDue + `
FROM runs r ` + completionEvidenceJoin + ` JOIN events e ON e.run_id=r.run_id WHERE e.event_id=$1`
	if postgres {
		if err := tx.QueryRowContext(ctx, query, eventID).Scan(&candidate.RunID, &candidate.BundleHash, &candidate.Revision, &candidate.DueAt); err != nil {
			return runtimerunlifecycle.Candidate{}, err
		}
	} else {
		var due any
		if err := tx.QueryRowContext(ctx, query, eventID).Scan(&candidate.RunID, &candidate.BundleHash, &candidate.Revision, &due); err != nil {
			return runtimerunlifecycle.Candidate{}, err
		}
		parsed, found, err := sqliteTimeValue(due)
		if err != nil {
			return runtimerunlifecycle.Candidate{}, err
		}
		if !found {
			return runtimerunlifecycle.Candidate{}, fmt.Errorf("standalone candidate has no stored due time")
		}
		candidate.DueAt = parsed
	}
	return candidate, nil
}

func (s *RunLifecyclePostgresOwner) ReadSelectedControlAwaitingMutationTx(ctx context.Context, tx *sql.Tx, runID, loadedBundleHash string) (bool, error) {
	return readSelectedControlAwaitingMutationTx(ctx, tx, runID, loadedBundleHash)
}

func (s *RunLifecycleSQLiteOwner) ReadSelectedControlAwaitingMutationTx(ctx context.Context, tx *sql.Tx, runID, loadedBundleHash string) (bool, error) {
	return readSelectedControlAwaitingMutationTx(ctx, tx, runID, loadedBundleHash)
}

func readSelectedControlAwaitingMutationTx(ctx context.Context, tx *sql.Tx, runID, loadedBundleHash string) (bool, error) {
	var awaiting bool
	err := tx.QueryRowContext(ctx, `SELECT r.status='running' AND (r.bundle_hash <> $2 OR `+completionEvidenceDue+` IS NULL)
		FROM runs r `+completionEvidenceJoin+` WHERE r.run_id=$1`, runID, loadedBundleHash).Scan(&awaiting)
	return awaiting, err
}

func (s *RunLifecyclePostgresOwner) ReadRunDebugStorageTx(ctx context.Context, tx *sql.Tx, runID string) ([]string, string, error) {
	return readRunDebugStorageTx(ctx, tx, true, runID)
}

func (s *RunLifecycleSQLiteOwner) ReadRunDebugStorageTx(ctx context.Context, tx *sql.Tx, runID string) ([]string, string, error) {
	return readRunDebugStorageTx(ctx, tx, false, runID)
}

func readRunDebugStorageTx(ctx context.Context, tx *sql.Tx, postgres bool, runID string) ([]string, string, error) {
	query := `SELECT r.status, COALESCE(c.revision,0), COALESCE(` + completionEvidenceDue + `, ''), r.bundle_hash FROM runs r ` + completionEvidenceJoin + ` WHERE r.run_id = ?`
	if postgres {
		query = `SELECT r.status, COALESCE(c.revision,0), COALESCE((` + completionEvidenceDue + `)::text, ''), r.bundle_hash FROM runs r ` + completionEvidenceJoin + ` WHERE r.run_id = $1::uuid`
	}
	rows, err := tx.QueryContext(ctx, query, runID)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, " columns", err
	}
	var out []string
	for rows.Next() {
		values := make([]sql.NullString, len(columns))
		scan := make([]any, len(values))
		for i := range values {
			scan[i] = &values[i]
		}
		if err := rows.Scan(scan...); err != nil {
			return nil, " scan", err
		}
		cols := make([]string, len(values))
		for i, value := range values {
			if value.Valid {
				cols[i] = value.String
			}
		}
		out = append(out, fmt.Sprintf("%v", cols))
	}
	if err := rows.Err(); err != nil {
		return nil, " rows", err
	}
	return out, "", nil
}

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

// WriteCompletionCandidateTx performs only the durable request. The mutation
// protocol owns the matching attempt-local handoff reservation.
func (s *RunLifecyclePostgresOwner) WriteCompletionCandidateTx(
	ctx context.Context,
	attempt *mutationprotocol.Attempt,
	runID string,
	dueAt *time.Time,
) (runtimerunlifecycle.CandidateRequestResult, error) {
	var selectedNow time.Time
	var result runtimerunlifecycle.CandidateRequestResult
	err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) (err error) {
		result, err = requestPostgresCompletionCandidateTx(ctx, tx, runID, dueAt, false, &selectedNow)
		return err
	})
	if err != nil || result.Disposition != runtimerunlifecycle.CandidateRetainedLive {
		return result, err
	}
	result.GenericScheduleActivations, err = s.pipeline.AdvanceFanOutDeliveryBarriersTx(ctx, attempt, runID, selectedNow)
	return result, err
}

func (s *RunLifecycleSQLiteOwner) WriteCompletionCandidateTx(
	ctx context.Context,
	attempt *mutationprotocol.Attempt,
	runID string,
	dueAt *time.Time,
) (runtimerunlifecycle.CandidateRequestResult, error) {
	selectedNow := s.now()
	var result runtimerunlifecycle.CandidateRequestResult
	err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) (err error) {
		result, err = requestSQLiteCompletionCandidateTx(ctx, tx, runID, dueAt, selectedNow, false)
		return err
	})
	if err != nil || result.Disposition != runtimerunlifecycle.CandidateRetainedLive {
		return result, err
	}
	result.GenericScheduleActivations, err = s.pipeline.AdvanceFanOutDeliveryBarriersTx(ctx, attempt, runID, selectedNow)
	return result, err
}

func (s *RunLifecyclePostgresOwner) RequestCompletionCandidate(
	ctx context.Context,
	request runtimerunlifecycle.CandidateRequest,
) (runtimerunlifecycle.CandidateRequestDisposition, error) {
	if err := request.Validate(); err != nil {
		return "", err
	}
	var dueAt *time.Time
	if request.Timing == runtimerunlifecycle.CandidateAt {
		dueAt = &request.DueAt
	}
	if err := s.requireCurrentSchema(); err != nil {
		return "", err
	}
	result := mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, s.runLifecycleCandidates, func(txctx context.Context, attempt *mutationprotocol.Attempt) (runtimerunlifecycle.CandidateRequestResult, error) {
		return attempt.RequestCompletion(txctx, s, request.RunID, dueAt)
	})
	value, acknowledged := result.Value()
	if !acknowledged {
		return "", result.Err()
	}
	return value.Disposition, result.Err()
}

func (s *RunLifecycleSQLiteOwner) RequestCompletionCandidate(
	ctx context.Context,
	request runtimerunlifecycle.CandidateRequest,
) (runtimerunlifecycle.CandidateRequestDisposition, error) {
	if err := request.Validate(); err != nil {
		return "", err
	}
	var dueAt *time.Time
	if request.Timing == runtimerunlifecycle.CandidateAt {
		dueAt = &request.DueAt
	}
	if err := s.requireCurrentSchema(); err != nil {
		return "", err
	}
	result := mutationprotocol.RunSQLite(ctx, s.backend, "sqlite request completion candidate", mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, s.runLifecycleCandidates, func(txctx context.Context, attempt *mutationprotocol.Attempt) (runtimerunlifecycle.CandidateRequestResult, error) {
		return attempt.RequestCompletion(txctx, s, request.RunID, dueAt)
	})
	value, acknowledged := result.Value()
	if !acknowledged {
		return "", result.Err()
	}
	return value.Disposition, result.Err()
}

func requestPostgresCompletionCandidateTx(ctx context.Context, tx *sql.Tx, runID string, dueAt *time.Time, forceRevision bool, observedAt *time.Time) (runtimerunlifecycle.CandidateRequestResult, error) {
	return requestCompletionEvidenceTx(ctx, tx, true, runID, dueAt, time.Time{}, forceRevision, observedAt)
}

func RequestPostgresCompletionCandidateTx(ctx context.Context, tx *sql.Tx, runID string, dueAt *time.Time) (runtimerunlifecycle.CandidateRequestResult, error) {
	return requestPostgresCompletionCandidateTx(ctx, tx, runID, dueAt, false, nil)
}

func requestSQLiteCompletionCandidateTx(ctx context.Context, tx *sql.Tx, runID string, dueAt *time.Time, now time.Time, forceRevision bool) (runtimerunlifecycle.CandidateRequestResult, error) {
	return requestCompletionEvidenceTx(ctx, tx, false, runID, dueAt, now, forceRevision, nil)
}

func RequestSQLiteCompletionCandidateTx(ctx context.Context, tx *sql.Tx, runID string, dueAt *time.Time, now time.Time) (runtimerunlifecycle.CandidateRequestResult, error) {
	return requestSQLiteCompletionCandidateTx(ctx, tx, runID, dueAt, now, false)
}

func clearPostgresCompletionCandidateTx(ctx context.Context, tx *sql.Tx, runID string) error {
	return clearCompletionEvidenceTx(ctx, tx, nullUUIDString(runID), 0)
}

func clearSQLiteCompletionCandidateTx(ctx context.Context, tx *sql.Tx, runID string) error {
	return clearCompletionEvidenceTx(ctx, tx, nullUUIDString(runID), 0)
}

func transitionPostgresActiveRunStateTx(
	ctx context.Context,
	tx *sql.Tx,
	runID string,
	state runtimerunlifecycle.State,
	current runtimerunlifecycle.State,
) (sql.Result, error) {
	if err := clearPostgresCompletionCandidateTx(ctx, tx, runID); err != nil {
		return nil, err
	}
	return tx.ExecContext(ctx, `
		UPDATE runs
		SET status = $2,
		    ended_at = NULL,
		    failure = NULL
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
	if err := clearSQLiteCompletionCandidateTx(ctx, tx, runID); err != nil {
		return nil, err
	}
	return tx.ExecContext(ctx, `
		UPDATE runs
		SET status = ?,
		    ended_at = NULL,
		    failure = NULL
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
	var page runtimerunlifecycle.CandidatePage
	err := s.runRead(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		page, err = listCompletionCandidatesTx(txctx, tx, true, scope, cursor, limit)
		return err
	})
	return page, err
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
	var page runtimerunlifecycle.CandidatePage
	err := s.runRead(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		page, err = listCompletionCandidatesTx(txctx, tx, false, scope, cursor, limit)
		return err
	})
	return page, err
}

func (s *RunLifecyclePostgresOwner) ExecuteCompletionCandidate(
	ctx context.Context,
	candidate runtimerunlifecycle.Candidate,
	catalog runtimerunlifecycle.FinalCatalog,
) (runtimerunlifecycle.CompletionResult, error) {
	if err := candidate.Validate(); err != nil {
		return runtimerunlifecycle.CompletionResult{}, err
	}
	if err := s.requireCurrentSchema(); err != nil {
		return runtimerunlifecycle.CompletionResult{}, err
	}
	var changes ChannelCardChanges
	result := mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, func(txctx context.Context, attempt *mutationprotocol.Attempt) (runtimerunlifecycle.CompletionResult, error) {
		transactiontest.Mark(txctx, transactiontest.RunCompletionCandidate)
		var outcome runtimerunlifecycle.CompletionResult
		err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) (err error) {
			if _, err := mutationprotocol.RunRowLock(txctx, tx, candidate.RunID, true); err != nil {
				return err
			}
			outcome, err = s.executeCompletionCandidateTx(txctx, tx, attempt, candidate, catalog, &changes)
			return err
		})
		return outcome, err
	})
	outcome, committed := result.Value()
	if !committed {
		return runtimerunlifecycle.CompletionResult{}, result.Err()
	}
	outcome.Committed = true
	return outcome, errors.Join(result.Err(), s.publishChannelChanges(committed, changes))
}

func (s *RunLifecycleSQLiteOwner) ExecuteCompletionCandidate(
	ctx context.Context,
	candidate runtimerunlifecycle.Candidate,
	catalog runtimerunlifecycle.FinalCatalog,
) (runtimerunlifecycle.CompletionResult, error) {
	if err := candidate.Validate(); err != nil {
		return runtimerunlifecycle.CompletionResult{}, err
	}
	if err := s.requireCurrentSchema(); err != nil {
		return runtimerunlifecycle.CompletionResult{}, err
	}
	var changes ChannelCardChanges
	result := mutationprotocol.RunSQLite(ctx, s.backend, "sqlite execute run completion candidate", mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, func(txctx context.Context, attempt *mutationprotocol.Attempt) (runtimerunlifecycle.CompletionResult, error) {
		transactiontest.Mark(txctx, transactiontest.RunCompletionCandidate)
		var outcome runtimerunlifecycle.CompletionResult
		err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) (err error) {
			if _, err := mutationprotocol.RunRowLock(txctx, tx, candidate.RunID, true); err != nil {
				return err
			}
			outcome, err = s.executeCompletionCandidateTx(txctx, tx, attempt, candidate, catalog, &changes)
			return err
		})
		return outcome, err
	})
	outcome, committed := result.Value()
	if !committed {
		return runtimerunlifecycle.CompletionResult{}, result.Err()
	}
	outcome.Committed = true
	return outcome, errors.Join(result.Err(), s.publishChannelChanges(committed, changes))
}

func (s *RunLifecyclePostgresOwner) executeCompletionCandidateTx(
	ctx context.Context,
	tx *sql.Tx,
	attempt *mutationprotocol.Attempt,
	candidate runtimerunlifecycle.Candidate,
	catalog runtimerunlifecycle.FinalCatalog,
	changes *ChannelCardChanges,
) (runtimerunlifecycle.CompletionResult, error) {
	var (
		state       string
		bundleHash  string
		selectedNow time.Time
	)
	err := tx.QueryRowContext(ctx, `
		SELECT LOWER(status), bundle_hash, clock_timestamp()
		FROM runs
		WHERE run_id = $1::uuid
		FOR UPDATE
	`, candidate.RunID).Scan(&state, &bundleHash, &selectedNow)
	if errors.Is(err, sql.ErrNoRows) {
		return runtimerunlifecycle.CompletionResult{Outcome: runtimerunlifecycle.OutcomeExactNoop}, nil
	}
	if err != nil {
		return runtimerunlifecycle.CompletionResult{}, err
	}
	if err := requireCompletionCandidateAuthorityTx(ctx, tx, true, candidate, bundleHash, s.runAuthority); err != nil {
		return runtimerunlifecycle.CompletionResult{}, err
	}
	currentRev, currentDue, err := readCompletionEvidenceTx(ctx, tx, candidate.RunID)
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
		if err := clearCompletionEvidenceTx(ctx, tx, candidate.RunID, candidate.Revision); err != nil {
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
		barrierActivations, err := s.pipeline.AdvanceFanOutDeliveryBarriersTx(ctx, attempt, candidate.RunID, selectedNow)
		if err != nil {
			return runtimerunlifecycle.CompletionResult{}, fmt.Errorf("advance fan-out delivery barriers: %w", err)
		}
		allowEmptyEntities := snapshot.Origin.Kind() == runtimerunlifecycle.OriginDeployment
		pending, wake, err := s.pendingPostgresRunCompletionWork(ctx, tx, candidate.RunID, selectedNow, catalog, allowEmptyEntities)
		if err != nil {
			return runtimerunlifecycle.CompletionResult{}, err
		}
		if pending {
			result, err := s.finishBlockedPostgresCandidate(ctx, tx, candidate, wake)
			result.GenericScheduleActivations = barrierActivations
			return result, err
		}
		summaries, err := s.loadPostgresRunCompletionOwnerSummaries(ctx, tx, candidate.RunID, selectedNow, catalog)
		if err != nil {
			return runtimerunlifecycle.CompletionResult{}, err
		}
		if summaries.blocksCompletion(allowEmptyEntities) {
			result, err := s.finishBlockedPostgresCandidate(ctx, tx, candidate, optionalWake(summaries.Sessions.NextExpiry))
			result.GenericScheduleActivations = barrierActivations
			return result, err
		}
		if len(barrierActivations) != 0 {
			return runtimerunlifecycle.CompletionResult{}, errors.New("new fan-out barrier completion did not block run completion")
		}
	}
	if _, _, err := s.completeRunTx(ctx, tx, attempt, candidate.RunID, selectedNow, changes); err != nil {
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
		request, err := requestPostgresCompletionCandidateTx(ctx, tx, candidate.RunID, nextWake, true, nil)
		if err != nil {
			return runtimerunlifecycle.CompletionResult{}, err
		}
		return runtimerunlifecycle.CompletionResult{Outcome: runtimerunlifecycle.OutcomeRearmAt, Candidate: request.Candidate}, nil
	}
	if err := clearCompletionEvidenceTx(ctx, tx, candidate.RunID, candidate.Revision); err != nil {
		return runtimerunlifecycle.CompletionResult{}, err
	}
	return runtimerunlifecycle.CompletionResult{Outcome: runtimerunlifecycle.OutcomeAwaitMutation}, nil
}

func (s *RunLifecycleSQLiteOwner) executeCompletionCandidateTx(
	ctx context.Context,
	tx *sql.Tx,
	attempt *mutationprotocol.Attempt,
	candidate runtimerunlifecycle.Candidate,
	catalog runtimerunlifecycle.FinalCatalog,
	changes *ChannelCardChanges,
) (runtimerunlifecycle.CompletionResult, error) {
	var (
		state      string
		bundleHash string
	)
	err := tx.QueryRowContext(ctx, `
		SELECT LOWER(status), bundle_hash
		FROM runs
		WHERE run_id = ?
	`, candidate.RunID).Scan(&state, &bundleHash)
	if errors.Is(err, sql.ErrNoRows) {
		return runtimerunlifecycle.CompletionResult{Outcome: runtimerunlifecycle.OutcomeExactNoop}, nil
	}
	if err != nil {
		return runtimerunlifecycle.CompletionResult{}, err
	}
	if err := requireCompletionCandidateAuthorityTx(ctx, tx, false, candidate, bundleHash, s.runAuthority); err != nil {
		return runtimerunlifecycle.CompletionResult{}, err
	}
	currentRev, currentDue, err := readCompletionEvidenceTx(ctx, tx, candidate.RunID)
	if err != nil {
		return runtimerunlifecycle.CompletionResult{}, err
	}
	if strings.TrimSpace(bundleHash) != candidate.BundleHash || !currentDue.Valid || currentRev != candidate.Revision {
		return runtimerunlifecycle.CompletionResult{Outcome: runtimerunlifecycle.OutcomeExactNoop}, nil
	}
	storedDue := currentDue.Time
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
		if err := clearCompletionEvidenceTx(ctx, tx, candidate.RunID, candidate.Revision); err != nil {
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
		barrierActivations, err := s.pipeline.AdvanceFanOutDeliveryBarriersTx(ctx, attempt, candidate.RunID, selectedNow)
		if err != nil {
			return runtimerunlifecycle.CompletionResult{}, fmt.Errorf("advance sqlite fan-out delivery barriers: %w", err)
		}
		allowEmptyEntities := snapshot.Origin.Kind() == runtimerunlifecycle.OriginDeployment
		pending, wake, err := s.pendingSQLiteRunCompletionWork(ctx, tx, candidate.RunID, selectedNow, catalog, allowEmptyEntities)
		if err != nil {
			return runtimerunlifecycle.CompletionResult{}, err
		}
		if pending {
			result, err := s.finishBlockedSQLiteCandidate(ctx, tx, candidate, wake, selectedNow)
			result.GenericScheduleActivations = barrierActivations
			return result, err
		}
		summaries, err := s.loadSQLiteRunCompletionOwnerSummaries(ctx, tx, candidate.RunID, selectedNow, catalog)
		if err != nil {
			return runtimerunlifecycle.CompletionResult{}, err
		}
		if summaries.blocksCompletion(allowEmptyEntities) {
			result, err := s.finishBlockedSQLiteCandidate(ctx, tx, candidate, optionalWake(summaries.Sessions.NextExpiry), selectedNow)
			result.GenericScheduleActivations = barrierActivations
			return result, err
		}
		if len(barrierActivations) != 0 {
			return runtimerunlifecycle.CompletionResult{}, errors.New("new sqlite fan-out barrier completion did not block run completion")
		}
	}
	if _, _, err := s.completeRunTx(ctx, tx, attempt, candidate.RunID, selectedNow, changes); err != nil {
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
	if err := clearCompletionEvidenceTx(ctx, tx, candidate.RunID, candidate.Revision); err != nil {
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
