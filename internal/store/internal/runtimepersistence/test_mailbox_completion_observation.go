package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
)

// This fixed physical witness excludes only the two diagnostic event classes.
// Callers first join publication handoff; no row fields or clocks are projected away.
func ReadMailboxCompletionRunEffectsForTest(ctx context.Context, selected any, runID string) ([]string, error) {
	if err := validateSelectedForkStorageIdentity(runID); err != nil {
		return nil, err
	}
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	var out []string
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		for _, query := range []string{
			`SELECT * FROM decision_cards WHERE run_id=$1 ORDER BY card_id`,
			`SELECT d.* FROM decision_card_input_drafts d JOIN decision_cards c ON c.card_id=d.card_id WHERE c.run_id=$1 ORDER BY d.input_draft_id`,
			`SELECT d.* FROM decision_card_changes d JOIN decision_cards c ON c.card_id=d.card_id WHERE c.run_id=$1 ORDER BY d.change_id`,
			`SELECT * FROM human_task_continuations WHERE run_id=$1 ORDER BY card_id`,
			`SELECT * FROM proposed_effect_continuations WHERE run_id=$1 ORDER BY card_id`,
			`SELECT * FROM events WHERE run_id=$1 AND event_class NOT IN ('runtime_diagnostic','diagnostic_direct') ORDER BY event_id`,
		} {
			rows, err := readMailboxCompletionEffectsRows(ctx, tx, query, runID)
			if err != nil {
				return err
			}
			out = append(out, rows...)
		}
		rows, err := delivery.ReadMailboxCompletionDeliveryEffectsTx(ctx, tx, runID)
		if err != nil {
			return err
		}
		out = append(out, rows...)
		for _, query := range []string{
			`SELECT * FROM entity_state WHERE run_id=$1 ORDER BY entity_id,flow_instance`,
			`SELECT * FROM flow_instances WHERE run_id=$1 ORDER BY instance_path`,
		} {
			rows, err := readMailboxCompletionEffectsRows(ctx, tx, query, runID)
			if err != nil {
				return err
			}
			out = append(out, rows...)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func readMailboxCompletionEffectsRows(ctx context.Context, tx *sql.Tx, query, runID string) ([]string, error) {
	rows, err := tx.QueryContext(ctx, query, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var out []string
	for rows.Next() {
		values, pointers := make([]any, len(columns)), make([]any, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			return nil, err
		}
		raw, err := json.Marshal(values)
		if err != nil {
			return nil, err
		}
		out = append(out, query+":"+string(raw))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return out, nil
}
