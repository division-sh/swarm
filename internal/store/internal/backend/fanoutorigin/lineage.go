package fanoutorigin

import (
	"context"
	"database/sql"
	"fmt"
)

type Queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// SourceRunInLineage proves ancestry, not permission to replay or publish.
func SourceRunInLineage(ctx context.Context, q Queryer, postgres bool, runID, sourceRunID string) (bool, error) {
	query := `WITH RECURSIVE lineage(run_id) AS (
		SELECT $1 UNION SELECT runs.forked_from_run_id FROM runs
		JOIN lineage ON runs.run_id=lineage.run_id WHERE runs.forked_from_run_id IS NOT NULL
	) SELECT EXISTS(SELECT 1 FROM lineage WHERE run_id=$2)`
	if postgres {
		query = `WITH RECURSIVE lineage(run_id) AS (
			SELECT $1::uuid UNION SELECT runs.forked_from_run_id FROM runs
			JOIN lineage ON runs.run_id=lineage.run_id WHERE runs.forked_from_run_id IS NOT NULL
		) SELECT EXISTS(SELECT 1 FROM lineage WHERE run_id=$2::uuid)`
	}
	var found bool
	if err := q.QueryRowContext(ctx, query, runID, sourceRunID).Scan(&found); err != nil {
		return false, fmt.Errorf("validate fan-out source run lineage: %w", err)
	}
	return found, nil
}
