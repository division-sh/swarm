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
	desired := make([]render.Action, 0, len(frozen.Choices)+2)
	for _, choice := range frozen.Choices {
		desired = append(desired, render.Action{Kind: "verdict", Verdict: choice.Verdict, Label: choice.Label})
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
