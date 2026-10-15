package agentpersistence

import (
	"context"
	"database/sql"
)

func CountRunNamedAgentStorageTx(ctx context.Context, tx *sql.Tx, runID, agentID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM agents WHERE run_id=$1 AND agent_id=$2`, runID, agentID).Scan(&count)
	return count, err
}
