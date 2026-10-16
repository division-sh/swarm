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

type H2CommittedFactEvidence struct {
	Family   string
	Key      string
	Revision int64
	Present  bool
	Fact     []byte
}

type H2CommittedHistoryEvidence struct {
	RunID     string
	Facts     []H2CommittedFactEvidence
	Complete  bool
	Truncated bool
	Bytes     int
}

// Failure-only, closed-family evidence. Tombstones are retained so a later
// deletion cannot resurrect an older header, event or timer at a selected cut.
func ObserveH2CommittedHistoryForTest(ctx context.Context, tx *sql.Tx, runID string) (H2CommittedHistoryEvidence, error) {
	if _, err := uuid.Parse(runID); err != nil {
		return H2CommittedHistoryEvidence{}, err
	}
	out := H2CommittedHistoryEvidence{RunID: runID}
	rows, err := tx.QueryContext(ctx, `SELECT family,fact_key,revision,present,fact
		FROM run_fork_fact_revisions WHERE run_id=$1 AND family IN ($2,$3,$4)
		ORDER BY revision,family,fact_key LIMIT 32769`, runID, FamilyTimers, FamilyEntityMetadata, FamilyEvents)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var row H2CommittedFactEvidence
		if err := rows.Scan(&row.Family, &row.Key, &row.Revision, &row.Present, &row.Fact); err != nil {
			return out, errors.Join(err, rows.Close())
		}
		if len(out.Facts) == 32768 || out.Bytes+len(row.Fact) > 32<<20 {
			out.Truncated = true
			return out, rows.Close()
		}
		out.Bytes += len(row.Fact)
		out.Facts = append(out.Facts, row)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return out, err
	}
	out.Complete = true
	return out, nil
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
