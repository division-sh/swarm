package channeldelivery

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/google/uuid"
)

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

func actionsForFrozen(frozen render.Frozen) ([]render.Action, error) {
	if err := frozen.Validate(); err != nil {
		return nil, err
	}
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
		start := frozen.DraftChooser.PageIndex * render.DraftChooserPageSize
		end := start + render.DraftChooserPageSize
		if end > len(frozen.DraftChoices) {
			end = len(frozen.DraftChoices)
		}
		for _, choice := range frozen.DraftChoices[start:end] {
			desired = append(desired, render.Action{Kind: "select_draft", DraftID: choice.DraftID, CardID: choice.CardID,
				TextPublicationID: frozen.DraftChooser.TextPublicationID, Label: choice.Label})
		}
		if end < len(frozen.DraftChoices) {
			desired = append(desired, render.Action{Kind: "next_draft_page", Label: "More choices"})
		}
	}
	if frozen.Recovery != nil {
		start := frozen.Recovery.PageIndex * render.DraftChooserPageSize
		end := start + render.DraftChooserPageSize
		if end > len(frozen.RecoveryChoices) {
			end = len(frozen.RecoveryChoices)
		}
		for _, choice := range frozen.RecoveryChoices[start:end] {
			desired = append(desired, render.Action{Kind: "resend", RecoveryDeliveryID: choice.DeliveryID, Label: choice.Label})
		}
		if end < len(frozen.RecoveryChoices) {
			desired = append(desired, render.Action{Kind: "next_recovery_page", Label: "More uncertain"})
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
	_, truncated, err := render.PresentationText(frozen)
	if err != nil {
		return nil, err
	}
	if truncated {
		desired = append(desired, render.Action{Kind: "view_full", Label: "View full"})
	}
	return desired, nil
}
