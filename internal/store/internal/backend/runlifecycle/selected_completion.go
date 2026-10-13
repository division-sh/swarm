package runlifecycle

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle/sourceadmission"
)

// Binding membership, not the run's origin or the caller's context, determines
// which executor may consume this durable completion coordinate.
func selectedRunBinding(ctx context.Context, q sourceadmission.RowQueryer, postgres bool, runID string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if q == nil || runID == "" {
		return "", errors.New("completion mode requires transaction and run_id")
	}
	query := `SELECT CAST(fork_run_id AS TEXT) FROM run_fork_selected_contract_bindings WHERE fork_run_id = ?`
	if postgres {
		query = `SELECT fork_run_id::text FROM run_fork_selected_contract_bindings WHERE fork_run_id = $1::uuid`
	}
	var selectedRunID string
	if err := q.QueryRowContext(ctx, query, runID).Scan(&selectedRunID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", fmt.Errorf("read completion candidate mode: %w", err)
	}
	if selectedRunID != runID {
		return "", errors.New("completion binding contradicts exact run identity")
	}
	return selectedRunID, nil
}

func requireCompletionCandidateAuthorityTx(ctx context.Context, tx *sql.Tx, postgres bool, candidate runtimerunlifecycle.Candidate, bundleHash string, owner runAuthorityOwner) error {
	selectedRunID, err := selectedRunBinding(ctx, tx, postgres, candidate.RunID)
	if err != nil {
		return err
	}
	if candidate.SelectedForkRunID != selectedRunID {
		return completionAuthorityRefusal(candidate, "candidate execution mode differs from current binding", nil)
	}
	if selectedRunID == "" {
		if err := requireRunlessExecution(ctx); err != nil {
			return completionAuthorityRefusal(candidate, "selected authority cannot execute an ordinary candidate", err)
		}
		return nil
	}
	source, admitted := runtimecorrelation.SourceArtifactFactFromContext(ctx)
	if !admitted || source.BundleHash() != candidate.BundleHash || bundleHash != candidate.BundleHash {
		return completionAuthorityRefusal(candidate, "selected completion requires the exact current run bundle", nil)
	}
	if err := requireSelectedRunAuthorityTx(ctx, tx, selectedRunID, bundleHash, owner); err != nil {
		if errors.Is(err, runtimerunlifecycle.ErrRunExecutionAuthority) {
			return completionAuthorityRefusal(candidate, "exact current selected execution authority is required", err)
		}
		if envelope, typed := runtimefailures.EnvelopeFromError(err); typed && envelope.Class == runtimefailures.ClassSupersededGeneration {
			return completionAuthorityRefusal(candidate, "selected completion authority is stale", err)
		}
		return err
	}
	return nil
}

func completionAuthorityRefusal(candidate runtimerunlifecycle.Candidate, reason string, cause error) error {
	return fmt.Errorf("%w: completion candidate %s: %s", errors.Join(runtimerunlifecycle.ErrCompletionAuthority, cause), candidate.RunID, reason)
}

func listCompletionCandidatesTx(ctx context.Context, tx *sql.Tx, postgres bool, scope runtimerunlifecycle.CandidateScope, cursor runtimerunlifecycle.CandidateCursor, limit int) (runtimerunlifecycle.CandidatePage, error) {
	if err := ctx.Err(); err != nil {
		return runtimerunlifecycle.CandidatePage{}, err
	}
	if tx == nil {
		return runtimerunlifecycle.CandidatePage{}, errors.New("completion listing requires transaction")
	}
	query, args := completionCandidatesQuery(postgres, scope, cursor, limit)
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return runtimerunlifecycle.CandidatePage{}, fmt.Errorf("list completion candidates: %w", err)
	}
	defer rows.Close()
	page := runtimerunlifecycle.CandidatePage{Candidates: make([]runtimerunlifecycle.Candidate, 0, limit)}
	for rows.Next() {
		var candidate runtimerunlifecycle.Candidate
		var dueAt any
		if err := rows.Scan(&candidate.RunID, &candidate.BundleHash, &candidate.Revision, &dueAt, &candidate.SelectedForkRunID); err != nil {
			return runtimerunlifecycle.CandidatePage{}, fmt.Errorf("scan completion candidate: %w", err)
		}
		parsed, present, err := sqliteTimeValue(dueAt)
		if err != nil {
			return runtimerunlifecycle.CandidatePage{}, fmt.Errorf("decode completion candidate due_at: %w", err)
		}
		if !present {
			return runtimerunlifecycle.CandidatePage{}, errors.New("completion candidate due_at is required")
		}
		candidate.DueAt = runtimerunlifecycle.CanonicalTimestamp(parsed)
		if err := candidate.Validate(); err != nil {
			return runtimerunlifecycle.CandidatePage{}, err
		}
		if !scope.MatchesCandidate(candidate) {
			return runtimerunlifecycle.CandidatePage{}, errors.New("completion candidate differs from exact listing scope")
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

func completionCandidatesQuery(postgres bool, scope runtimerunlifecycle.CandidateScope, cursor runtimerunlifecycle.CandidateCursor, limit int) (string, []any) {
	query := `SELECT r.run_id, r.bundle_hash, r.completion_revision, r.completion_due_at, COALESCE(b.fork_run_id, '')
		FROM runs r LEFT JOIN run_fork_selected_contract_bindings b ON b.fork_run_id = r.run_id
		WHERE r.bundle_hash = ? AND r.completion_due_at IS NOT NULL AND r.status = 'running' AND r.run_id > ?`
	if postgres {
		query = `SELECT r.run_id::text, r.bundle_hash, r.completion_revision, r.completion_due_at, COALESCE(b.fork_run_id::text, '')
			FROM runs r LEFT JOIN run_fork_selected_contract_bindings b ON b.fork_run_id = r.run_id
			WHERE r.bundle_hash = $1 AND r.completion_due_at IS NOT NULL AND r.status = 'running' AND r.run_id::text > $2`
	}
	args := []any{strings.TrimSpace(scope.BundleHash), strings.TrimSpace(cursor.RunID)}
	if scope.SelectedForkRunID == "" {
		query += ` AND b.fork_run_id IS NULL`
	} else {
		if postgres {
			query += ` AND r.run_id = $3::uuid AND b.fork_run_id = r.run_id`
		} else {
			query += ` AND r.run_id = ? AND b.fork_run_id = r.run_id`
		}
		args = append(args, scope.SelectedForkRunID)
	}
	if postgres {
		query += fmt.Sprintf(` ORDER BY r.run_id::text LIMIT $%d`, len(args)+1)
	} else {
		query += ` ORDER BY r.run_id LIMIT ?`
	}
	return query, append(args, limit)
}
