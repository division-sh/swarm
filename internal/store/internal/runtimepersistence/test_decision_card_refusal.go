package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/gateruntime"
)

// SupersedeDecisionCardAnchorForTest pins a hostile pending-card/superseded-anchor
// cut under the selected writer. It is not a production lifecycle operation.
func SupersedeDecisionCardAnchorForTest(ctx context.Context, selected any, card decisioncard.Card, now time.Time) error {
	fact, ok := correlation.SourceArtifactFactFromContext(ctx)
	if !ok || fact.BundleHash() != card.BundleHash || card.Status != decisioncard.StatusPending {
		return fmt.Errorf("superseded anchor fixture requires an exact pending card/source")
	}
	persist := func(ctx context.Context, tx *sql.Tx, postgres bool) error {
		var query string
		switch card.Anchor.Kind() {
		case decisioncard.AnchorKindHumanTask:
			query = `UPDATE human_task_continuations SET state='superseded' WHERE card_id=$1 AND run_id=$2`
		case decisioncard.AnchorKindProposedEffect:
			query = `UPDATE proposed_effect_continuations SET state='superseded', superseded_reason='owner_replaced' WHERE card_id=$1 AND run_id=$2`
		case decisioncard.AnchorKindStageGate:
			anchor, err := card.Anchor.StageGate()
			if err != nil {
				return err
			}
			var raw string
			if err := tx.QueryRowContext(ctx, `SELECT CAST(accumulator AS TEXT) FROM entity_state WHERE run_id=$1 AND entity_id=$2`, card.RunID, anchor.EntityID).Scan(&raw); err != nil {
				return err
			}
			var buckets map[string]map[string]any
			decoder := json.NewDecoder(strings.NewReader(raw))
			decoder.UseNumber()
			if err := decoder.Decode(&buckets); err != nil {
				return err
			}
			gate, found, err := gateruntime.Load(buckets, anchor.FlowID, card.Snapshot.Decision)
			if err != nil {
				return err
			}
			if !found || gate.CardID != card.CardID || !gate.Supersede("stage_exited", now) {
				return fmt.Errorf("superseded anchor fixture requires the exact open gate")
			}
			if err := gateruntime.Store(buckets, gate); err != nil {
				return err
			}
			updated, err := json.Marshal(buckets)
			if err != nil {
				return err
			}
			query = `UPDATE entity_state SET accumulator=$1 WHERE run_id=$2 AND entity_id=$3`
			if postgres {
				query = `UPDATE entity_state SET accumulator=$1::jsonb WHERE run_id=$2 AND entity_id=$3`
			}
			result, err := tx.ExecContext(ctx, query, string(updated), card.RunID, anchor.EntityID)
			return requireSupersededAnchorFixtureRow(result, err)
		default:
			return fmt.Errorf("superseded anchor fixture requires a registered anchor")
		}
		result, err := tx.ExecContext(ctx, query, card.CardID, card.RunID)
		return requireSupersededAnchorFixtureRow(result, err)
	}
	switch store := selected.(type) {
	case *PostgresStore:
		if store == nil || store.backend == nil {
			return fmt.Errorf("postgres fixture store is required")
		}
		return store.backend.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error { return persist(ctx, tx, true) })
	case *SQLiteRuntimeStore:
		if store == nil || store.backend == nil {
			return fmt.Errorf("sqlite fixture store is required")
		}
		return store.backend.RunTransaction(ctx, "decision-card superseded anchor fixture", func(ctx context.Context, tx *sql.Tx) error { return persist(ctx, tx, false) })
	default:
		return fmt.Errorf("unsupported decision-card fixture store %T", selected)
	}
}

func requireSupersededAnchorFixtureRow(result sql.Result, err error) error {
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return fmt.Errorf("superseded anchor fixture affected %d rows", affected)
	}
	return nil
}
