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
)

// A clear consumes one exact request, without overwriting either its identity
// or a newer request. The run row remains the lifecycle admission fence only.
const completionEvidenceJoin = ` LEFT JOIN run_completion_evidence c ON c.run_id=r.run_id
	AND c.record_kind='request' AND c.revision=(SELECT MAX(e.revision) FROM run_completion_evidence e WHERE e.run_id=r.run_id AND e.record_kind='request')
	LEFT JOIN run_completion_evidence consumed ON consumed.run_id=c.run_id AND consumed.revision=c.revision AND consumed.record_kind='clear' `

const completionEvidenceDue = `CASE WHEN consumed.run_id IS NULL THEN c.due_at ELSE NULL END`

func readCompletionEvidenceTx(ctx context.Context, tx *sql.Tx, runID string) (int64, sql.NullTime, error) {
	var revision int64
	var raw any
	err := tx.QueryRowContext(ctx, `SELECT COALESCE(c.revision,0), `+completionEvidenceDue+`
		FROM runs r `+completionEvidenceJoin+` WHERE r.run_id=$1`, runID).Scan(&revision, &raw)
	if err != nil {
		return 0, sql.NullTime{}, err
	}
	due, present, err := sqliteTimeValue(raw)
	return revision, sql.NullTime{Time: runtimerunlifecycle.CanonicalTimestamp(due), Valid: present}, err
}

func clearCompletionEvidenceTx(ctx context.Context, tx *sql.Tx, runID string, revision int64) error {
	if tx == nil {
		return errors.New("clear completion evidence requires transaction")
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO run_completion_evidence (run_id, revision, record_kind, due_at)
		SELECT run_id, revision, 'clear', NULL FROM run_completion_evidence
		WHERE run_id=$1 AND record_kind='request' AND revision=(
			SELECT MAX(revision) FROM run_completion_evidence WHERE run_id=$1 AND record_kind='request')
		AND (CAST($2 AS BIGINT)=0 OR revision=CAST($2 AS BIGINT)) ON CONFLICT (run_id, revision, record_kind) DO NOTHING`, runID, revision)
	return err
}

func requestCompletionEvidenceTx(ctx context.Context, tx *sql.Tx, postgres bool, runID string, dueAt *time.Time, selectedNow time.Time, force bool, observedAt *time.Time) (runtimerunlifecycle.CandidateRequestResult, error) {
	runID = nullUUIDString(runID)
	if tx == nil || runID == "" {
		return runtimerunlifecycle.CandidateRequestResult{}, errors.New("completion candidate requires transaction and run_id")
	}
	exclusive, err := mutationprotocol.RunRowLock(ctx, tx, runID, false)
	if err != nil {
		return runtimerunlifecycle.CandidateRequestResult{}, err
	}
	query := `SELECT LOWER(status), bundle_hash FROM runs WHERE run_id=$1`
	var state, bundleHash string
	if postgres {
		lock := ` FOR SHARE`
		if exclusive {
			lock = ` FOR UPDATE`
		}
		err = tx.QueryRowContext(ctx, `SELECT LOWER(status), bundle_hash, clock_timestamp() FROM runs WHERE run_id=$1::uuid`+lock, runID).Scan(&state, &bundleHash, &selectedNow)
	} else {
		err = tx.QueryRowContext(ctx, query, runID).Scan(&state, &bundleHash)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return runtimerunlifecycle.CandidateRequestResult{}, &runtimerunlifecycle.RunNotFoundError{RunID: runID}
	}
	if err != nil {
		return runtimerunlifecycle.CandidateRequestResult{}, fmt.Errorf("read completion lifecycle: %w", err)
	}
	lifecycleState, err := runtimerunlifecycle.ParseState(state)
	if err != nil {
		return runtimerunlifecycle.CandidateRequestResult{}, err
	}
	switch {
	case lifecycleState.Terminal():
		return runtimerunlifecycle.CandidateRequestResult{Disposition: runtimerunlifecycle.CandidateAbsorbedTerminal}, nil
	case lifecycleState == runtimerunlifecycle.StatePaused:
		return runtimerunlifecycle.CandidateRequestResult{Disposition: runtimerunlifecycle.CandidateDeferredPaused}, nil
	case lifecycleState == runtimerunlifecycle.StateLive:
		if observedAt != nil {
			*observedAt = runtimerunlifecycle.CanonicalTimestamp(selectedNow)
		}
		return runtimerunlifecycle.CandidateRequestResult{Disposition: runtimerunlifecycle.CandidateRetainedLive,
			Candidate: runtimerunlifecycle.Candidate{RunID: runID, BundleHash: bundleHash}}, nil
	}
	selectedRunID, err := selectedRunBinding(ctx, tx, postgres, runID)
	if err != nil {
		return runtimerunlifecycle.CandidateRequestResult{}, err
	}
	selectedNow = runtimerunlifecycle.CanonicalTimestamp(selectedNow)
	requestedDue := selectedNow
	if dueAt != nil {
		requestedDue = runtimerunlifecycle.CanonicalTimestamp(*dueAt)
	}
	for {
		currentRev, currentDue, err := readCompletionEvidenceTx(ctx, tx, runID)
		if err != nil {
			return runtimerunlifecycle.CandidateRequestResult{}, err
		}
		candidate := runtimerunlifecycle.Candidate{RunID: runID, BundleHash: strings.TrimSpace(bundleHash),
			Revision: currentRev, DueAt: currentDue.Time, SelectedForkRunID: selectedRunID}
		if !force && currentDue.Valid && (currentDue.Time.Equal(requestedDue) || !currentDue.Time.After(selectedNow)) {
			result := runtimerunlifecycle.CandidateRequestResult{Disposition: runtimerunlifecycle.CandidateAlreadyCurrent, Candidate: candidate}
			return result, result.Validate()
		}
		candidate.Revision, candidate.DueAt = currentRev+1, requestedDue
		result, err := tx.ExecContext(ctx, `INSERT INTO run_completion_evidence (run_id, revision, record_kind, due_at)
			VALUES ($1,$2,'request',$3) ON CONFLICT (run_id, revision, record_kind) DO NOTHING`, runID, candidate.Revision, candidate.DueAt)
		if err != nil {
			return runtimerunlifecycle.CandidateRequestResult{}, fmt.Errorf("insert completion request: %w", err)
		}
		inserted, err := result.RowsAffected()
		if err != nil {
			return runtimerunlifecycle.CandidateRequestResult{}, err
		}
		if inserted == 0 {
			// Another request won this revision. Re-fold it in this same
			// transaction; never replay the domain callback or its effects.
			continue
		}
		request := runtimerunlifecycle.CandidateRequestResult{Disposition: runtimerunlifecycle.CandidateRequested, Candidate: candidate}
		return request, request.Validate()
	}
}
