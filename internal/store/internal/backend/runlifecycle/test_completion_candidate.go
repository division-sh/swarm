package runlifecycle

import (
	"context"
	"database/sql"
	"strings"
	"time"

	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

// The clear and replacement request stay in the caller's original story
// transaction. No later transaction can observe or mint the replacement.
func ForcePostgresCompletionCandidateForTest(ctx context.Context, tx *sql.Tx, runID string, dueAt *time.Time) (runtimerunlifecycle.CandidateRequestResult, error) {
	if err := clearCompletionCandidateFixtureTx(ctx, tx, runID, true); err != nil {
		return runtimerunlifecycle.CandidateRequestResult{}, err
	}
	return RequestPostgresCompletionCandidateTx(ctx, tx, runID, dueAt)
}

func ForceSQLiteCompletionCandidateForTest(ctx context.Context, tx *sql.Tx, runID string, dueAt *time.Time, now time.Time) (runtimerunlifecycle.CandidateRequestResult, error) {
	if err := clearCompletionCandidateFixtureTx(ctx, tx, runID, false); err != nil {
		return runtimerunlifecycle.CandidateRequestResult{}, err
	}
	return RequestSQLiteCompletionCandidateTx(ctx, tx, runID, dueAt, now)
}

func (s *RunLifecyclePostgresOwner) ClearCompletionCandidateForTest(ctx context.Context, runID string) error {
	return s.backend.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return clearCompletionCandidateFixtureTx(ctx, tx, runID, true)
	})
}

func (s *RunLifecycleSQLiteOwner) ClearCompletionCandidateForTest(ctx context.Context, runID string) error {
	return s.backend.RunTransaction(ctx, "unrevisioned event fixture", func(ctx context.Context, tx *sql.Tx) error {
		return clearCompletionCandidateFixtureTx(ctx, tx, runID, false)
	})
}

func clearCompletionCandidateFixtureTx(ctx context.Context, tx *sql.Tx, runID string, postgres bool) error {
	query := `INSERT INTO run_completion_evidence (run_id, revision, record_kind, due_at)
		SELECT run_id,MAX(revision),'clear',NULL FROM run_completion_evidence WHERE run_id=? AND record_kind='request'
		GROUP BY run_id ON CONFLICT (run_id,revision,record_kind) DO NOTHING`
	if postgres {
		query = `INSERT INTO run_completion_evidence (run_id, revision, record_kind, due_at)
		SELECT run_id,MAX(revision),'clear',NULL FROM run_completion_evidence WHERE run_id=$1::uuid AND record_kind='request'
		GROUP BY run_id ON CONFLICT (run_id,revision,record_kind) DO NOTHING`
	}
	_, err := tx.ExecContext(ctx, query, strings.TrimSpace(runID))
	return err
}
