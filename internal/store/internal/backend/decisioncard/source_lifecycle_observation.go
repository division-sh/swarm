package decisionstore

import (
	"context"
	"database/sql"
)

func CountSourceRetiredCardStorageTx(ctx context.Context, tx *sql.Tx, run, card string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM decision_cards WHERE run_id=$1 AND card_id=$2 AND status='superseded'`, run, card).Scan(&count)
	return count, err
}

func ReadSourceParentCardClockStorageTx(ctx context.Context, tx *sql.Tx, card string) ([3]string, error) {
	var state [3]string
	err := tx.QueryRowContext(ctx, `SELECT status,COALESCE(verdict,''),CAST(updated_at AS TEXT) FROM decision_cards WHERE card_id=$1`, card).Scan(&state[0], &state[1], &state[2])
	return state, err
}

func CountSourceCardDraftsStorageTx(ctx context.Context, tx *sql.Tx, card string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM decision_card_input_drafts WHERE card_id=$1`, card).Scan(&count)
	return count, err
}
