package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/gateruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
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
		forkCard.Fields = semanticvalue.EmptyObject()
		forkCard.DecidedBy = ""
		forkCard.DecidedAt = time.Time{}
		forkCard.DeferredUntil = time.Time{}
		forkCard.DecisionEventID = ""
		forkCard.DeliveryReceiptID = ""
		forkCard.DeliveryRenderHash = ""
		forkCard.SupersededReason = ""
		forkCard.CreatedAt = now.UTC()
		forkCard.UpdatedAt = now.UTC()
		forkedFromCardID, err := semanticvalue.String(sourceCard.CardID)
		if err != nil {
			return fmt.Errorf("admit source decision card identity: %w", err)
		}
		forkCard.Provenance, err = sourceCard.Provenance.With("forked_from_card_id", forkedFromCardID)
		if err != nil {
			return fmt.Errorf("extend fork decision card provenance: %w", err)
		}
		forkedFromActivationID, err := semanticvalue.String(binding.Source.ActivationID)
		if err != nil {
			return fmt.Errorf("admit source gate activation identity: %w", err)
		}
		forkCard.Provenance, err = forkCard.Provenance.With("forked_from_stage_activation_id", forkedFromActivationID)
		if err != nil {
			return fmt.Errorf("extend fork decision card provenance: %w", err)
		}
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
