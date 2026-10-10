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

type H2CounterCommitEvidence struct {
	MutationID, EntityID, EventID, Path string
	Revision                            int64
}

// This physical witness binds existing counter mutations to commit order,
// without granting historical execution or inferring order from timestamps.
func ObserveH2CounterCommitsForTest(ctx context.Context, tx *sql.Tx, runID string) ([]H2CounterCommitEvidence, error) {
	if _, err := uuid.Parse(runID); err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT m.mutation_id,m.entity_id,m.caused_by_event,m.path,r.revision
		FROM entity_mutations m LEFT JOIN run_fork_fact_revisions r
		ON r.run_id=m.run_id AND r.family=$2 AND r.fact_key=CAST(m.mutation_id AS TEXT) AND r.present=TRUE
		WHERE m.run_id=$1 AND m.domain='authored_field' AND m.path IN ('count','c1','c2')`, runID, FamilyEntityMutations)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []H2CounterCommitEvidence
	for rows.Next() {
		var row H2CounterCommitEvidence
		var event sql.NullString
		var revision sql.NullInt64
		if err := rows.Scan(&row.MutationID, &row.EntityID, &event, &row.Path, &revision); err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		row.EventID, row.Revision = event.String, revision.Int64
		out = append(out, row)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	return out, nil
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
