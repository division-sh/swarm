package channeldelivery

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/channeldelivery"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/store/internal/backend/decisionpersistence"
)

// FreezeCurrentSourceTx derives presentation only from the committed source
// under the same transaction that admits its destination and render.
func FreezeCurrentSourceTx(ctx context.Context, tx *sql.Tx, plan Plan, postgres bool) (channeldelivery.Frozen, error) {
	if tx == nil {
		return channeldelivery.Frozen{}, fmt.Errorf("channel render source transaction is required")
	}
	if err := plan.Validate(); err != nil {
		return channeldelivery.Frozen{}, err
	}
	audience := channeldelivery.Audience{
		PrincipalID: plan.PrincipalID, InterfaceKey: plan.InterfaceKey, DeliveryEpoch: plan.DeliveryEpoch,
		ExternalAccountRef: plan.ExternalAccountRef, ConversationRef: plan.ConversationRef,
		ConversationScope: plan.ConversationScope,
	}
	switch plan.SourceKind {
	case PlanResponse:
		if plan.CurrentRenderID == "" {
			return channeldelivery.Frozen{}, fmt.Errorf("channel response has no committed render")
		}
		stored, found, err := LoadRender(ctx, tx, plan.CurrentRenderID, postgres)
		if err != nil {
			return channeldelivery.Frozen{}, err
		}
		if !found {
			return channeldelivery.Frozen{}, fmt.Errorf("channel response render is absent")
		}
		return channeldelivery.WithPresentation(stored.Frozen, plan.Bounds, plan.ActionPageIndex)
	case PlanSummary:
		frozen, err := channeldelivery.FreezeSummary(plan.SourceID, plan.SummaryCount, audience)
		if err != nil {
			return channeldelivery.Frozen{}, err
		}
		return channeldelivery.WithPresentation(frozen, plan.Bounds, plan.ActionPageIndex)
	case PlanNotice:
		query := `SELECT item_type, COALESCE(summary, ''), COALESCE(severity, 'normal'), COALESCE(notified, false),
			COALESCE(payload, '{}'), COALESCE(from_agent, ''), COALESCE(entity_id, ''), COALESCE(flow_instance, '')
			FROM mailbox WHERE item_id=?`
		if postgres {
			query = `SELECT item_type, COALESCE(summary, ''), COALESCE(severity, 'normal'), COALESCE(notified, false),
				COALESCE(payload, '{}'), COALESCE(from_agent, ''), COALESCE(entity_id::text, ''), COALESCE(flow_instance, '')
				FROM mailbox WHERE item_id=$1::uuid FOR UPDATE`
		}
		notice := channeldelivery.Notice{ID: plan.SourceID}
		var payload any
		if err := tx.QueryRowContext(ctx, query, plan.SourceID).Scan(&notice.Type, &notice.Summary,
			&notice.Priority, &notice.Acknowledged, &payload, &notice.FromAgent, &notice.EntityID, &notice.FlowInstance); err != nil {
			return channeldelivery.Frozen{}, fmt.Errorf("load exact channel notice: %w", err)
		}
		switch value := payload.(type) {
		case string:
			notice.Context = []byte(value)
		case []byte:
			notice.Context = value
		default:
			return channeldelivery.Frozen{}, fmt.Errorf("stored channel notice context has unsupported type %T", payload)
		}
		frozen, err := channeldelivery.FreezeNotice(notice, audience)
		if err != nil {
			return channeldelivery.Frozen{}, err
		}
		return channeldelivery.WithPresentation(frozen, plan.Bounds, plan.ActionPageIndex)
	case PlanCard:
		card, err := decisionpersistence.LoadDecisionCardInTx(ctx, tx, plan.SourceID, postgres)
		if err != nil {
			return channeldelivery.Frozen{}, fmt.Errorf("load exact channel card: %w", err)
		}
		query := `SELECT COALESCE(MAX(change_id), 0) FROM decision_card_changes WHERE card_id=?`
		if postgres {
			query = `SELECT COALESCE(MAX(change_id), 0) FROM decision_card_changes WHERE card_id=$1::uuid`
		}
		var revision int64
		if err := tx.QueryRowContext(ctx, query, plan.SourceID).Scan(&revision); err != nil {
			return channeldelivery.Frozen{}, err
		}
		dispatch := ""
		if card.Anchor.Kind() == decisioncard.AnchorKindProposedEffect {
			readback, err := decisionpersistence.ProposedEffectReadbackInTx(ctx, tx, plan.SourceID, postgres)
			if err != nil {
				return channeldelivery.Frozen{}, fmt.Errorf("load exact card dispatch state: %w", err)
			}
			dispatch = readback.DispatchState
		}
		prompt := channeldelivery.DraftPrompt{}
		if card.Status == decisioncard.StatusPending {
			query = `SELECT input_draft_id, verdict, next_field_index, expires_at
				FROM decision_card_input_drafts WHERE card_id=? AND principal_id=? AND status='active' LIMIT 2`
			if postgres {
				query = `SELECT input_draft_id::text, verdict, next_field_index, expires_at
					FROM decision_card_input_drafts WHERE card_id=$1::uuid AND principal_id=$2 AND status='active' LIMIT 2`
			}
			rows, err := tx.QueryContext(ctx, query, plan.SourceID, plan.PrincipalID)
			if err != nil {
				return channeldelivery.Frozen{}, err
			}
			defer rows.Close()
			for rows.Next() {
				if prompt.DraftID != "" {
					return channeldelivery.Frozen{}, fmt.Errorf("card has multiple active channel input drafts")
				}
				var expires any
				if err := rows.Scan(&prompt.DraftID, &prompt.Verdict, &prompt.NextFieldIndex, &expires); err != nil {
					return channeldelivery.Frozen{}, err
				}
				prompt.ExpiresAt, err = decodeActionTime(expires)
				if err != nil {
					return channeldelivery.Frozen{}, err
				}
			}
			if err := rows.Err(); err != nil {
				return channeldelivery.Frozen{}, err
			}
		}
		frozen, err := channeldelivery.FreezeCard(card, revision, dispatch, audience, prompt)
		if err != nil {
			return channeldelivery.Frozen{}, err
		}
		return channeldelivery.WithPresentation(frozen, plan.Bounds, plan.ActionPageIndex)
	default:
		return channeldelivery.Frozen{}, fmt.Errorf("unsupported channel render source %q", plan.SourceKind)
	}
}

func requireExactSourceRenderTx(ctx context.Context, tx *sql.Tx, plan Plan, frozen channeldelivery.Frozen, postgres bool) error {
	current, err := FreezeCurrentSourceTx(ctx, tx, plan, postgres)
	if err != nil {
		return err
	}
	if current.Hash != frozen.Hash || !bytes.Equal(current.Input, frozen.Input) {
		return fmt.Errorf("channel render does not match the current committed source")
	}
	return nil
}
