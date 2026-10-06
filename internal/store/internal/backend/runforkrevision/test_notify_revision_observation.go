package runforkrevision

import (
	"context"
	"database/sql"
)

// Physical cardinality is not historical payload admission. The caller retains
// its original selected read snapshot; no ledger body or query selector escapes.
func CountNotifyFanOutRevisionStorageForTest(ctx context.Context, tx *sql.Tx, runID string) (revisions, facts int64, err error) {
	err = tx.QueryRowContext(ctx, `SELECT
		(SELECT COUNT(*) FROM run_fork_revisions WHERE run_id=$1),
		(SELECT COUNT(*) FROM run_fork_fact_revisions WHERE run_id=$1)`, runID).Scan(&revisions, &facts)
	if err != nil {
		return 0, 0, err
	}
	return revisions, facts, nil
}
