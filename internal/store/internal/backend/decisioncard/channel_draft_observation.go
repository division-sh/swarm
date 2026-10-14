package decisionstore

import (
	"context"
	"database/sql"
)

func ReadChannelDraftStatusStorageTx(ctx context.Context, tx *sql.Tx, cardID string) (string, error) {
	var status string
	err := tx.QueryRowContext(ctx, `SELECT status FROM decision_card_input_drafts WHERE card_id=$1`, cardID).Scan(&status)
	return status, err
}

func ReadChannelDraftCardPhysicalStorageTx(ctx context.Context, tx *sql.Tx, card string) ([7]string, error) {
	var result [7]string
	err := tx.QueryRowContext(ctx, `SELECT d.status,CAST(d.input_fields AS TEXT),CAST(d.next_field_index AS TEXT),
		CAST(d.updated_at AS TEXT),c.status,COALESCE(c.verdict,''),CAST(c.updated_at AS TEXT)
		FROM decision_card_input_drafts d JOIN decision_cards c ON c.card_id=d.card_id WHERE d.card_id=$1`, card).Scan(
		&result[0], &result[1], &result[2], &result[3], &result[4], &result[5], &result[6])
	return result, err
}

func CountDecidedCardsStorageTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM decision_cards WHERE status='decided'`).Scan(&count)
	return count, err
}

func CountThreeDecidedCardsStorageTx(ctx context.Context, tx *sql.Tx, first, second, third string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM decision_cards WHERE card_id IN ($1,$2,$3) AND status='decided'`, first, second, third).Scan(&count)
	return count, err
}

func ReadCardRunStorageTx(ctx context.Context, tx *sql.Tx, cardID string) (string, error) {
	var runID string
	err := tx.QueryRowContext(ctx, `SELECT run_id FROM decision_cards WHERE card_id=$1`, cardID).Scan(&runID)
	return runID, err
}

func ReadCardSnapshotStorageTx(ctx context.Context, tx *sql.Tx, cardID string) (string, error) {
	var snapshot string
	err := tx.QueryRowContext(ctx, `SELECT CAST(snapshot AS TEXT) FROM decision_cards WHERE card_id=$1`, cardID).Scan(&snapshot)
	return snapshot, err
}
