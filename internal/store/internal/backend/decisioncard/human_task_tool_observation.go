package decisionstore

import (
	"context"
	"database/sql"
)

func CountHumanTaskToolCardStorageTx(ctx context.Context, tx *sql.Tx, cardID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM decision_cards WHERE card_id=$1`, cardID).Scan(&count)
	return count, err
}

func CountHumanTaskToolContinuationStorageTx(ctx context.Context, tx *sql.Tx, cardID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM human_task_continuations WHERE card_id=$1`, cardID).Scan(&count)
	return count, err
}

func CountAllHumanTaskToolCardsStorageTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM decision_cards WHERE anchor_kind='human_task'`).Scan(&count)
	return count, err
}
