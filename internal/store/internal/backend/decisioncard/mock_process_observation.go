package decisionstore

import (
	"context"
	"database/sql"
)

type MockDecisionCardStorage struct {
	CardID, Snapshot, Status string
}

func ReadMockDecisionCardsStorageTx(ctx context.Context, tx *sql.Tx) ([]MockDecisionCardStorage, error) {
	rows, err := tx.QueryContext(ctx, `SELECT card_id, CAST(snapshot AS TEXT), status FROM decision_cards WHERE execution_mode = 'mock' ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []MockDecisionCardStorage
	for rows.Next() {
		var row MockDecisionCardStorage
		if err := rows.Scan(&row.CardID, &row.Snapshot, &row.Status); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func CountMockTelegramDecisionCardsStorageTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM decision_cards WHERE execution_mode = 'mock' AND CAST(snapshot AS TEXT) LIKE '%send_telegram_message%'`).Scan(&count)
	return count, err
}
