package decisionstore

import (
	"context"
	"database/sql"
)

type GateCompletionCardStorage struct{ Status, Verdict, DecidedBy, DecisionEventID, Anchor string }

func ReadGateCompletionCardStorageTx(ctx context.Context, tx *sql.Tx, cardID string) (GateCompletionCardStorage, error) {
	var out GateCompletionCardStorage
	err := tx.QueryRowContext(ctx, `SELECT status,COALESCE(verdict,''),COALESCE(decided_by,''),COALESCE(CAST(decision_event_id AS TEXT),''),CAST(anchor AS TEXT)
		FROM decision_cards WHERE card_id=$1`, cardID).Scan(&out.Status, &out.Verdict, &out.DecidedBy, &out.DecisionEventID, &out.Anchor)
	return out, err
}

func ReadGateCompletionChangesStorageTx(ctx context.Context, tx *sql.Tx, cardID string) ([][3]string, error) {
	rows, err := tx.QueryContext(ctx, `SELECT CAST(change_id AS TEXT),change_type,CAST(payload AS TEXT)
		FROM decision_card_changes WHERE card_id=$1 ORDER BY change_id`, cardID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out [][3]string
	for rows.Next() {
		var row [3]string
		if err := rows.Scan(&row[0], &row[1], &row[2]); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}
