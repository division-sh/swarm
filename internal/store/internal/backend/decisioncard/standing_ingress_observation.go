package decisionstore

import (
	"context"
	"database/sql"
)

func CountPendingStandingRetirementCardsStorageTx(ctx context.Context, tx *sql.Tx, postgres bool, entityID string) (int, error) {
	query := `SELECT COUNT(*) FROM decision_cards WHERE anchor_kind = 'stage_gate'
		AND json_extract(anchor, '$.entity_id') = ? AND status = 'pending' AND json_extract(snapshot, '$.decision') = 'retire_service'`
	if postgres {
		query = `SELECT COUNT(*) FROM decision_cards WHERE anchor_kind = 'stage_gate'
			AND anchor->>'entity_id' = $1 AND status = 'pending' AND snapshot->>'decision' = 'retire_service'`
	}
	var count int
	err := tx.QueryRowContext(ctx, query, entityID).Scan(&count)
	return count, err
}
