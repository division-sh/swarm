package channeldelivery

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/google/uuid"
)

// CurrentRenderCorruption retains one exact existing row, not render authority.
type CurrentRenderCorruption struct {
	card, plan, render, run, sourceRevision, version, input, hash, created, status string
}

func CaptureCurrentRenderCorruptionTx(ctx context.Context, tx *sql.Tx, card string) (*CurrentRenderCorruption, error) {
	if tx == nil || uuid.Validate(card) != nil {
		return nil, errors.New("current render fault requires its exact native card")
	}
	rows, err := tx.QueryContext(ctx, `SELECT CAST(p.delivery_id AS TEXT),CAST(r.render_id AS TEXT),CAST(c.run_id AS TEXT),
		CAST(r.source_revision AS TEXT),r.projection_version,CAST(r.render_input AS TEXT),r.render_hash,CAST(r.created_at AS TEXT),c.status
		FROM channel_delivery_plans p JOIN channel_delivery_renders r ON r.render_id=p.current_render_id AND r.delivery_id=p.delivery_id
		JOIN decision_cards c ON c.card_id=p.source_id
		WHERE p.source_kind='card' AND p.source_id=$1`, card)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return nil, sql.ErrNoRows
	}
	row := &CurrentRenderCorruption{card: card}
	if err := rows.Scan(&row.plan, &row.render, &row.run, &row.sourceRevision, &row.version, &row.input, &row.hash, &row.created, &row.status); err != nil {
		return nil, err
	}
	if rows.Next() {
		return nil, errors.New("current render fault refuses multiple source plans")
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if row.input == "{}" || row.status != "pending" {
		return nil, errors.New("current render fault requires an intact pending native card")
	}
	return row, rows.Close()
}

func (r *CurrentRenderCorruption) InstallTx(ctx context.Context, tx *sql.Tx) error {
	if r == nil {
		return errors.New("current render fault requires its captured native row")
	}
	return r.replaceTx(ctx, tx, r.input, "{}")
}

func (r *CurrentRenderCorruption) RestoreTx(ctx context.Context, tx *sql.Tx) error {
	if r == nil || tx == nil {
		return errors.New("current render fault requires its captured native row")
	}
	// The transaction may have rolled back, or a previous restore may have
	// committed without acknowledgement. Retained history is safe to leave intact
	// even after the current plan advances; restoring still requires the live CAS.
	var original bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM channel_delivery_renders
		WHERE render_id=$1 AND delivery_id=$2 AND CAST(render_input AS TEXT)=$3
		AND CAST(source_revision AS TEXT)=$4 AND projection_version=$5 AND render_hash=$6 AND CAST(created_at AS TEXT)=$7)`,
		r.render, r.plan, r.input, r.sourceRevision, r.version, r.hash, r.created).Scan(&original)
	if err != nil || original {
		return err
	}
	return r.replaceTx(ctx, tx, "{}", r.input)
}

func (r *CurrentRenderCorruption) replaceTx(ctx context.Context, tx *sql.Tx, before, after string) error {
	if r == nil || tx == nil || uuid.Validate(r.card) != nil || uuid.Validate(r.render) != nil {
		return errors.New("current render fault requires its captured native row")
	}
	result, err := tx.ExecContext(ctx, `UPDATE channel_delivery_renders SET render_input=$1
		WHERE render_id=$2 AND delivery_id=$3 AND CAST(render_input AS TEXT)=$4
		AND CAST(source_revision AS TEXT)=$5 AND projection_version=$6 AND render_hash=$7 AND CAST(created_at AS TEXT)=$8
		AND EXISTS (SELECT 1 FROM channel_delivery_plans p JOIN decision_cards c ON c.card_id=p.source_id
			WHERE p.delivery_id=$3 AND p.current_render_id=$2 AND p.source_kind='card' AND p.source_id=$9
			AND c.run_id=$10 AND c.status=$11)`, after, r.render, r.plan, before, r.sourceRevision, r.version, r.hash, r.created, r.card, r.run, r.status)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return fmt.Errorf("current render changed after capture: affected=%d", count)
	}
	return nil
}
