package channeldelivery

import (
	"context"
	"database/sql"
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
	expected render.ResolvedAction, postgres bool) error {
	if tx == nil || expected.Action.Kind != "more_controls" ||
		expected.Action.Token != action.Token {
		return fmt.Errorf("card action page requires an exact verified control")
	}
	if err := LockPrincipalTx(ctx, tx, expected.PrincipalID, postgres); err != nil {
		return err
	}
	state, err := RequireActionIntentTx(ctx, tx, action, postgres, true)
	if err != nil {
		return err
	}
	if state != "pending" {
		return fmt.Errorf("card action page tap is already settled")
	}
	resolved, found, err := ResolveActionFactForMutationTx(ctx, tx, action.ActionFact, postgres)
	if err != nil {
		return err
	}
	if !found || !resolved.CurrentRender || resolved != expected {
		return fmt.Errorf("card action page tap is no longer current")
	}
	plan, found, err := LoadCurrentPlan(ctx, tx, resolved.DeliveryID, postgres)
	if err != nil {
		return err
	}
	if !found || plan.SourceKind != expected.SourceKind || plan.State != "sent" ||
		plan.CurrentRenderID != resolved.RenderID || plan.CurrentReceiptID != resolved.ReceiptOperationID {
		return fmt.Errorf("card action page has no exact sent plan")
	}
	if err := requireCurrentActionPageTx(ctx, tx, resolved, plan, postgres); err != nil {
		return err
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
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return fmt.Errorf("card action page changed before navigation")
	}
	plan.ActionPageIndex = next
	frozen, err := FreezeCurrentSourceTx(ctx, tx, plan, postgres)
	if err != nil {
		return err
	}
	renderID, _, err := PersistRenderTx(ctx, tx, plan.DeliveryID, frozen, postgres)
	if err != nil {
		return err
	}
	if _, err := EnsureRenderActionsTx(ctx, tx, renderID, frozen, postgres); err != nil {
		return err
	}
	query = `UPDATE operator_channel_action_intents SET state='settled', disposition='navigation', settled_at=?
		WHERE publication_id=? AND state='pending'`
	if postgres {
		query = `UPDATE operator_channel_action_intents SET state='settled', disposition='navigation', settled_at=$1
			WHERE publication_id=$2::uuid AND state='pending'`
	}
	result, err = tx.ExecContext(ctx, query, time.Now().UTC(), action.PublicationID)
	if err != nil {
		return err
	}
	rows, err = result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return fmt.Errorf("card action page intent did not settle with navigation")
	}
	return nil
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

func semanticActionsForFrozen(frozen render.Frozen) []render.Action {
	desired := make([]render.Action, 0, len(frozen.Choices)+len(frozen.DraftChoices)+3)
	for _, choice := range frozen.Choices {
		desired = append(desired, render.Action{Kind: "verdict", Verdict: choice.Verdict, Label: choice.Label})
	}
	if frozen.Prompt != nil {
		desired = append(desired, render.Action{Kind: "cancel_input", DraftID: frozen.Prompt.DraftID, Label: "Cancel input"})
		if frozen.Prompt.Optional {
			desired = append(desired, render.Action{Kind: "skip_input", DraftID: frozen.Prompt.DraftID, Label: "Skip field"})
		}
	}
	if frozen.DraftChooser != nil {
		for _, choice := range frozen.DraftChoices {
			desired = append(desired, render.Action{Kind: "select_draft", DraftID: choice.DraftID, CardID: choice.CardID,
				TextPublicationID: frozen.DraftChooser.TextPublicationID, Label: choice.Label})
		}
	}
	if frozen.Recovery != nil {
		for _, choice := range frozen.RecoveryChoices {
			desired = append(desired, render.Action{Kind: "resend", RecoveryDeliveryID: choice.DeliveryID, Label: choice.Label})
		}
	}
	if frozen.SourceKind == PlanSummary {
		desired = append(desired, render.Action{Kind: "open_inbox", Label: "Open inbox"})
	}
	if frozen.SourceKind == PlanNotice && !frozen.NoticeAcknowledged {
		desired = append(desired, render.Action{Kind: "acknowledge_notice", Label: "Acknowledge"})
	}
	if frozen.Page != nil && frozen.Page.Index+1 < frozen.Page.Count {
		desired = append(desired, render.Action{Kind: "next_page", Label: "Next page"})
	}
	return desired
}

func actionsForFrozen(frozen render.Frozen) ([]render.Action, error) {
	if err := frozen.Validate(); err != nil {
		return nil, err
	}
	desired := semanticActionsForFrozen(frozen)
	_, truncated, err := render.PresentationText(frozen)
	if err != nil {
		return nil, err
	}
	if truncated {
		desired = append([]render.Action{{Kind: "view_full", Label: "View full"}}, desired...)
	}
	if page := frozen.ActionPage; page != nil {
		if len(desired) > page.Capacity {
			pageSize := page.Capacity - 1
			pages, err := render.ActionPageCount(frozen)
			if err != nil {
				return nil, err
			}
			start := (page.Index % pages) * pageSize
			end := start + pageSize
			if end > len(desired) {
				end = len(desired)
			}
			if start >= end {
				return nil, fmt.Errorf("channel action page has no controls")
			}
			paged := append([]render.Action(nil), desired[start:end]...)
			desired = append(paged, render.Action{Kind: "more_controls", Label: "More choices"})
		}
	}
	for index := range desired {
		desired[index].Label = frozen.Bounds.Label(desired[index].Label)
	}
	return desired, nil
}
