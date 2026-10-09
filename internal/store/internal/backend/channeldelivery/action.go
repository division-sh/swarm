package channeldelivery

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/division-sh/swarm/internal/operatorchannel"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/google/uuid"
)

func requireCurrentActionPageTx(ctx context.Context, tx *sql.Tx, resolved render.ResolvedAction, plan Plan, postgres bool) error {
	stored, found, err := LoadRender(ctx, tx, resolved.RenderID, postgres)
	if err != nil {
		return err
	}
	if !found || stored.Frozen.Hash != resolved.RenderHash || stored.Frozen.ActionPage == nil ||
		stored.Frozen.ActionPage.Index != plan.ActionPageIndex ||
		stored.Frozen.Bounds != plan.Bounds {
		return fmt.Errorf("card action page contradicts current frozen render")
	}
	pages, err := render.ActionPageCount(stored.Frozen)
	if err != nil {
		return err
	}
	if pages < 2 || plan.ActionPageIndex == math.MaxInt64 {
		return fmt.Errorf("card has no additional controls")
	}
	return nil
}

// AdvanceActionPageTx moves one verified tap to the next immutable page
// and settles that tap atomically with the selected plan pointer.
func AdvanceActionPageTx(ctx context.Context, tx *sql.Tx, action operatorchannel.InboundAction,
	expected render.ResolvedAction, mode render.ControlPageMode, postgres bool) (bool, error) {
	if tx == nil || expected.Action.Kind != "more_controls" ||
		expected.Action.Token != action.Token || (mode != render.ControlPageEdit && mode != render.ControlPageFreshCopy) {
		return false, fmt.Errorf("card action page requires an exact verified control")
	}
	if err := LockPrincipalTx(ctx, tx, expected.PrincipalID, postgres); err != nil {
		return false, err
	}
	state, err := RequireActionIntentTx(ctx, tx, action, postgres, true)
	if err != nil {
		return false, err
	}
	if state != "pending" {
		return false, fmt.Errorf("card action page tap is already settled")
	}
	resolved, found, err := ResolveActionFactForMutationTx(ctx, tx, action.ActionFact, postgres)
	if err != nil {
		return false, err
	}
	if !found || !resolved.CurrentRender || resolved != expected {
		return false, fmt.Errorf("card action page tap is no longer current")
	}
	plan, found, err := LoadDestinationCurrentPlan(ctx, tx, resolved.DeliveryID, postgres)
	if err != nil {
		return false, err
	}
	if !found || !exactSentActionPlan(plan, resolved) {
		return false, fmt.Errorf("card action page has no exact sent plan")
	}
	if err := requireCurrentActionPageTx(ctx, tx, resolved, plan, postgres); err != nil {
		return false, err
	}
	if mode == render.ControlPageFreshCopy {
		if err := planRequestedControlCopyTx(ctx, tx, action, resolved, plan, postgres); err != nil {
			if errors.Is(err, errControlCopySuperseded) {
				changed, err := SettleUnappliedActionIntentTx(ctx, tx, action, render.ActionStale, postgres)
				return changed, err
			}
			return false, err
		}
		return settleControlNavigationTx(ctx, tx, action, postgres)
	}
	next := plan.ActionPageIndex + 1
	query := `UPDATE channel_delivery_plans SET action_page_index=?
		WHERE delivery_id=? AND current_render_id=? AND current_receipt_operation_id=?
		AND action_page_index=? AND state='sent'`
	if postgres {
		query = `UPDATE channel_delivery_plans SET action_page_index=$1
			WHERE delivery_id=$2::uuid AND current_render_id=$3::uuid AND current_receipt_operation_id=$4::uuid
			AND action_page_index=$5 AND state='sent'`
	}
	result, err := tx.ExecContext(ctx, query, next, plan.DeliveryID, plan.CurrentRenderID, plan.CurrentReceiptID, plan.ActionPageIndex)
	if err != nil {
		return false, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	if rows != 1 {
		return false, fmt.Errorf("card action page changed before navigation")
	}
	plan.ActionPageIndex = next
	frozen, err := FreezeCurrentSourceTx(ctx, tx, plan, postgres)
	if err != nil {
		return false, err
	}
	renderID, _, err := PersistRenderTx(ctx, tx, plan.DeliveryID, frozen, postgres)
	if err != nil {
		return false, err
	}
	if _, err := EnsureRenderActionsTx(ctx, tx, renderID, frozen, postgres); err != nil {
		return false, err
	}
	return settleControlNavigationTx(ctx, tx, action, postgres)
}

func exactSentActionPlan(plan Plan, resolved render.ResolvedAction) bool {
	return plan.SourceKind == resolved.SourceKind && plan.State == "sent" &&
		plan.CurrentRenderID == resolved.RenderID && plan.CurrentReceiptID == resolved.ReceiptOperationID
}

func settleControlNavigationTx(ctx context.Context, tx *sql.Tx, action operatorchannel.InboundAction, postgres bool) (bool, error) {
	query := `UPDATE operator_channel_action_intents SET state='settled', disposition='navigation', settled_at=?
		WHERE publication_id=? AND state='pending'`
	if postgres {
		query = `UPDATE operator_channel_action_intents SET state='settled', disposition='navigation', settled_at=$1
			WHERE publication_id=$2::uuid AND state='pending'`
	}
	result, err := tx.ExecContext(ctx, query, time.Now().UTC(), action.PublicationID)
	if err != nil {
		return false, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	if rows != 1 {
		return false, fmt.Errorf("card action page intent did not settle with navigation")
	}
	return true, nil
}

func EnsureRenderActionsTx(ctx context.Context, tx *sql.Tx, renderID string, frozen render.Frozen, postgres bool) ([]render.Action, error) {
	if tx == nil || uuid.Validate(renderID) != nil {
		return nil, fmt.Errorf("channel actions require a transaction and render id")
	}
	if err := frozen.Validate(); err != nil {
		return nil, err
	}
	desired, err := actionsForFrozen(frozen)
	if err != nil {
		return nil, err
	}
	for index := range desired {
		position := index + 1
		query := `INSERT INTO channel_delivery_actions
			(action_token, render_id, action_position, action_kind, verdict, label, created_at)
			VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (render_id, action_position) DO NOTHING`
		if postgres {
			query = `INSERT INTO channel_delivery_actions
				(action_token, render_id, action_position, action_kind, verdict, label, created_at)
				VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7)
				ON CONFLICT (render_id, action_position) DO NOTHING`
		}
		var verdict any
		if desired[index].Kind == "verdict" {
			verdict = desired[index].Verdict
		}
		if _, err := tx.ExecContext(ctx, query, uuid.NewString(), renderID, position,
			desired[index].Kind, verdict, desired[index].Label, time.Now().UTC()); err != nil {
			return nil, fmt.Errorf("persist channel action: %w", err)
		}
		query = `SELECT action_token, action_kind, verdict, label FROM channel_delivery_actions
			WHERE render_id=? AND action_position=?`
		if postgres {
			query = `SELECT action_token::text, action_kind, verdict, label FROM channel_delivery_actions
				WHERE render_id=$1::uuid AND action_position=$2`
		}
		var actual render.Action
		var storedVerdict sql.NullString
		if err := tx.QueryRowContext(ctx, query, renderID, position).Scan(
			&actual.Token, &actual.Kind, &storedVerdict, &actual.Label); err != nil {
			return nil, err
		}
		if storedVerdict.Valid {
			actual.Verdict = storedVerdict.String
		}
		if actual.Kind != desired[index].Kind || actual.Verdict != desired[index].Verdict ||
			actual.Label != desired[index].Label || uuid.Validate(actual.Token) != nil {
			return nil, fmt.Errorf("existing channel action contradicts immutable render")
		}
		actual.DraftID = desired[index].DraftID
		actual.CardID = desired[index].CardID
		actual.TextPublicationID = desired[index].TextPublicationID
		actual.RecoveryDeliveryID = desired[index].RecoveryDeliveryID
		actual.ParentReceiptOperationID = desired[index].ParentReceiptOperationID
		desired[index] = actual
	}
	query := `SELECT COUNT(*) FROM channel_delivery_actions WHERE render_id=?`
	if postgres {
		query = `SELECT COUNT(*) FROM channel_delivery_actions WHERE render_id=$1::uuid`
	}
	var count int
	if err := tx.QueryRowContext(ctx, query, renderID).Scan(&count); err != nil {
		return nil, err
	}
	if count != len(desired) {
		return nil, fmt.Errorf("stored channel actions contradict frozen render")
	}
	return desired, nil
}

func actionsForFrozen(frozen render.Frozen) ([]render.Action, error) {
	return render.ControlsForRender(frozen)
}
