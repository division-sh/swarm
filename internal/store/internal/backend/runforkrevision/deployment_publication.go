package runforkrevision

import (
	"context"
	"database/sql"
	"fmt"
)

// FirstDeploymentFeedRevisionTx selects the immutable insertion cut, not the
// feed's current cursor or a later publication of the same declaration.
func FirstDeploymentFeedRevisionTx(ctx context.Context, tx *sql.Tx, runID, feedID string) (int64, error) {
	var revision sql.NullInt64
	err := tx.QueryRowContext(ctx, `SELECT MIN(revision) FROM run_fork_fact_revisions
		WHERE run_id=$1 AND family='fan_out_obligations' AND present=TRUE
		AND fact ->> 'fact_kind'='intent' AND fact ->> 'origin_kind'='deployment'
		AND fact ->> 'deployment_feed_id'=$2`, runID, feedID).Scan(&revision)
	if err != nil {
		return 0, fmt.Errorf("read publication insertion revision: %w", err)
	}
	if !revision.Valid || revision.Int64 <= 0 {
		return 0, fmt.Errorf("publication feed has no committed insertion revision")
	}
	return revision.Int64, nil
}
