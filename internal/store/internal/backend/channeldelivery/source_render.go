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
		return stored.Frozen, nil
	case PlanSummary:
		return channeldelivery.FreezeSummary(plan.SourceID, plan.SummaryCount, audience)
	case PlanNotice:
		query := `SELECT item_type, COALESCE(summary, ''), COALESCE(severity, 'normal'),
			COALESCE(payload, '{}'), COALESCE(from_agent, ''), COALESCE(entity_id, ''), COALESCE(flow_instance, '')
			FROM mailbox WHERE item_id=?`
		if postgres {
			query = `SELECT item_type, COALESCE(summary, ''), COALESCE(severity, 'normal'),
				COALESCE(payload, '{}'), COALESCE(from_agent, ''), COALESCE(entity_id::text, ''), COALESCE(flow_instance, '')
				FROM mailbox WHERE item_id=$1::uuid FOR UPDATE`
		}
		notice := channeldelivery.Notice{ID: plan.SourceID}
		var payload any
		if err := tx.QueryRowContext(ctx, query, plan.SourceID).Scan(&notice.Type, &notice.Summary,
			&notice.Priority, &payload, &notice.FromAgent, &notice.EntityID, &notice.FlowInstance); err != nil {
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
		return channeldelivery.FreezeNotice(notice, audience)
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
			query = `SELECT state FROM proposed_effect_continuations WHERE card_id=?`
			if postgres {
				query = `SELECT state FROM proposed_effect_continuations WHERE card_id=$1::uuid FOR UPDATE`
			}
			if err := tx.QueryRowContext(ctx, query, plan.SourceID).Scan(&dispatch); err != nil {
				return channeldelivery.Frozen{}, fmt.Errorf("load exact card dispatch state: %w", err)
			}
		}
		return channeldelivery.FreezeCard(card, revision, dispatch, audience)
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
