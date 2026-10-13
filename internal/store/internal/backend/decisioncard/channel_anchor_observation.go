package decisionstore

import (
	"context"
	"database/sql"
)

type ChannelAnchorCardStorage struct {
	CardID, AnchorJSON string
}

func ReadChannelAnchorCardsStorageTx(ctx context.Context, tx *sql.Tx, runID, kind, status string) ([]ChannelAnchorCardStorage, error) {
	rows, err := tx.QueryContext(ctx, `SELECT card_id,CAST(anchor AS TEXT) FROM decision_cards WHERE run_id=$1 AND anchor_kind=$2 AND ($3='' OR status=$3)`, runID, kind, status)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []ChannelAnchorCardStorage
	for rows.Next() {
		var row ChannelAnchorCardStorage
		if err := rows.Scan(&row.CardID, &row.AnchorJSON); err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

type ChannelAnchorDecisionStorage struct {
	Status, Verdict string
}

func ReadChannelAnchorDecisionStorageTx(ctx context.Context, tx *sql.Tx, cardID string) (ChannelAnchorDecisionStorage, error) {
	var result ChannelAnchorDecisionStorage
	err := tx.QueryRowContext(ctx, `SELECT status,COALESCE(verdict,'') FROM decision_cards WHERE card_id=$1`, cardID).Scan(&result.Status, &result.Verdict)
	return result, err
}
