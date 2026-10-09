package runforkrevision

import (
	"context"
	"database/sql"
	"errors"

	"github.com/google/uuid"
)

// This fixed physical witness retains every timer revision, not only the
// latest projection. It exposes neither ledger bodies nor caller SQL.
func CountWorkflowTimerRevisionFactsForTest(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	if _, err := uuid.Parse(runID); err != nil {
		return 0, err
	}
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_fact_revisions WHERE run_id=$1 AND family=$2`, runID, FamilyTimers).Scan(&count)
	return count, err
}

type WorkflowMetadataRevisionEvidence struct {
	EntityID string
	Revision int64
	Fact     []byte
}

// A closed physical witness of one run's metadata copies, not a history fold.
func ObserveWorkflowMetadataRevisionsForTest(ctx context.Context, tx *sql.Tx, runID string) ([]WorkflowMetadataRevisionEvidence, error) {
	if _, err := uuid.Parse(runID); err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT fact_key,revision,fact,present FROM run_fork_fact_revisions WHERE run_id=$1 AND family=$2`, runID, FamilyEntityMetadata)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []WorkflowMetadataRevisionEvidence
	for rows.Next() {
		var row WorkflowMetadataRevisionEvidence
		var present bool
		if err := rows.Scan(&row.EntityID, &row.Revision, &row.Fact, &present); err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		if present {
			out = append(out, row)
		}
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	return out, nil
}
