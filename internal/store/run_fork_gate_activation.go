package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/gateruntime"
)

func materializeRunForkDecisionCards(ctx context.Context, tx *sql.Tx, forkRunID, entityID string, bindings []runForkGateActivationBinding, now time.Time) error {
	for _, binding := range bindings {
		sourceCard, err := loadDecisionCard(ctx, tx, binding.Source.CardID, true, false)
		if err != nil {
			return fmt.Errorf("load source decision card %s for fork: %w", binding.Source.CardID, err)
		}
		sourceVerdict := sourceCard.Verdict
		sourceFields := sourceCard.Fields
		sourceActor := sourceCard.DecidedBy
		sourceReceipt := sourceCard.DeliveryReceiptID
		sourceRenderHash := sourceCard.DeliveryRenderHash

		forkCard := sourceCard
		forkCard.CardID = binding.Fork.CardID
		forkCard.RunID = strings.TrimSpace(forkRunID)
		forkCard.EntityID = strings.TrimSpace(entityID)
		forkCard.StageActivationID = binding.Fork.ActivationID
		forkCard.Status = decisioncard.StatusPending
		forkCard.Verdict = ""
		forkCard.Fields = nil
		forkCard.DecidedBy = ""
		forkCard.DecidedAt = time.Time{}
		forkCard.DeferredUntil = time.Time{}
		forkCard.DecisionEventID = ""
		forkCard.DeliveryReceiptID = ""
		forkCard.DeliveryRenderHash = ""
		forkCard.SupersededReason = ""
		forkCard.CreatedAt = now.UTC()
		forkCard.UpdatedAt = now.UTC()
		forkCard.Provenance = cloneDecisionCardMap(sourceCard.Provenance)
		forkCard.Provenance["forked_from_card_id"] = sourceCard.CardID
		forkCard.Provenance["forked_from_stage_activation_id"] = binding.Source.ActivationID
		forkCard, err = decisioncard.New(forkCard)
		if err != nil {
			return fmt.Errorf("construct fork decision card: %w", err)
		}
		if err := insertDecisionCard(ctx, tx, forkCard, true); err != nil {
			return fmt.Errorf("insert fork decision card: %w", err)
		}
		switch binding.Fork.Status {
		case gateruntime.StatusOpen:
		case gateruntime.StatusDecisionCommitted, gateruntime.StatusRouted:
			if strings.TrimSpace(sourceVerdict) == "" || strings.TrimSpace(binding.Fork.DecisionEventID) == "" {
				return fmt.Errorf("source decision card %s lacks committed verdict evidence", sourceCard.CardID)
			}
			if _, err := decideDecisionCard(ctx, tx, decisioncard.DecideRequest{
				CardID: forkCard.CardID, Verdict: sourceVerdict, Fields: sourceFields, ActorTokenID: sourceActor,
				ObservedContentHash: forkCard.CardContentHash, DeliveryReceiptID: sourceReceipt, DeliveryRenderHash: sourceRenderHash,
				DecisionEventID: binding.Fork.DecisionEventID, Now: now,
			}, true); err != nil {
				return fmt.Errorf("restore committed fork decision card: %w", err)
			}
		case gateruntime.StatusSuperseded:
			if err := supersedeDecisionCardsForStage(ctx, tx, forkRunID, entityID, binding.Fork.ActivationID, binding.Fork.SupersededReason, now, true); err != nil {
				return fmt.Errorf("restore superseded fork decision card: %w", err)
			}
		}
	}
	return nil
}

func cloneDecisionCardMap(input map[string]any) map[string]any {
	out := make(map[string]any, len(input)+2)
	for key, value := range input {
		out[key] = value
	}
	return out
}
