package channeldelivery

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/google/uuid"
)

func CurrentCardChangeCursor(ctx context.Context, tx *sql.Tx) (int64, bool, error) {
	if tx == nil {
		return 0, false, fmt.Errorf("channel card change cursor requires selected transaction")
	}
	var cursor int64
	err := tx.QueryRowContext(ctx, `SELECT card_change_cursor FROM channel_delivery_defaults WHERE singleton_id=1 AND state='current'`).Scan(&cursor)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	if cursor < 0 {
		return 0, false, fmt.Errorf("channel card change cursor is negative")
	}
	return cursor, true, nil
}

// PlanChangedCardTx commits the open-card responsibility before acknowledging
// its source change. Retrying after rollback is harmless and never skips work.
func PlanChangedCardTx(ctx context.Context, tx *sql.Tx, sequence int64, cardID string, postgres bool) (bool, error) {
	if tx == nil || sequence <= 0 || uuid.Validate(cardID) != nil {
		return false, fmt.Errorf("channel card change requires transaction, positive sequence and card id")
	}
	query := `SELECT card_id FROM decision_card_changes WHERE change_id=?`
	if postgres {
		query = `SELECT card_id::text FROM decision_card_changes WHERE change_id=$1`
	}
	var actual string
	if err := tx.QueryRowContext(ctx, query, sequence).Scan(&actual); err != nil {
		return false, fmt.Errorf("load exact channel card change: %w", err)
	}
	if actual != cardID {
		return false, fmt.Errorf("channel card change %d belongs to another card", sequence)
	}
	created, err := PlanOpenCardTx(ctx, tx, cardID, postgres)
	if err != nil {
		return false, err
	}
	query = `UPDATE channel_delivery_plans SET action_page_index=0
		WHERE source_kind='card' AND source_id=? AND action_page_index<>0
		AND EXISTS (SELECT 1 FROM channel_delivery_renders r
			WHERE r.render_id=channel_delivery_plans.current_render_id AND r.source_revision<?)`
	if postgres {
		query = `UPDATE channel_delivery_plans SET action_page_index=0
			WHERE source_kind='card' AND source_id=$1::uuid AND action_page_index<>0
			AND EXISTS (SELECT 1 FROM channel_delivery_renders r
				WHERE r.render_id=channel_delivery_plans.current_render_id AND r.source_revision<$2)`
	}
	pages, err := tx.ExecContext(ctx, query, cardID, sequence)
	if err != nil {
		return false, err
	}
	pageCount, err := pages.RowsAffected()
	if err != nil {
		return false, err
	}
	query = `UPDATE channel_delivery_defaults SET card_change_cursor=? WHERE singleton_id=1 AND state='current' AND card_change_cursor < ?`
	if postgres {
		query = `UPDATE channel_delivery_defaults SET card_change_cursor=$1 WHERE singleton_id=1 AND state='current' AND card_change_cursor < $2`
	}
	cursor, err := tx.ExecContext(ctx, query, sequence, sequence)
	if err != nil {
		return false, err
	}
	cursorCount, err := cursor.RowsAffected()
	return created || pageCount > 0 || cursorCount > 0, err
}
