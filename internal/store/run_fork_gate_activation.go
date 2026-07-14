package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
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
		sourceAnchor, err := sourceCard.Anchor.StageGate()
		if err != nil {
			return fmt.Errorf("source decision card %s anchor: %w", sourceCard.CardID, err)
		}
		forkCard.Anchor, err = decisioncard.NewStageGateAnchor(decisioncard.StageGateAnchor{
			FlowInstance: sourceAnchor.FlowInstance, FlowID: sourceAnchor.FlowID,
			EntityID: strings.TrimSpace(entityID), Stage: sourceAnchor.Stage,
			StageActivationID: binding.Fork.ActivationID,
		})
		if err != nil {
			return fmt.Errorf("construct fork stage_gate anchor: %w", err)
		}
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

func materializeRunForkProposedEffectCards(ctx context.Context, tx *sql.Tx, sourceRunID, forkRunID, entityID string, forkPoint RunForkPoint, now time.Time) error {
	rows, err := tx.QueryContext(ctx, `
		SELECT p.card_id
		FROM proposed_effect_continuations p
		JOIN decision_cards c ON c.card_id = p.card_id
		WHERE p.run_id = $1::uuid
		  AND p.effect->>'entity_id' = $2
		  AND c.created_at <= $3
		ORDER BY c.created_at, p.card_id
		FOR UPDATE OF p, c
	`, strings.TrimSpace(sourceRunID), strings.TrimSpace(entityID), forkPoint.Timestamp.UTC())
	if err != nil {
		return fmt.Errorf("load source proposed effects for fork: %w", err)
	}
	var cardIDs []string
	for rows.Next() {
		var cardID string
		if err := rows.Scan(&cardID); err != nil {
			_ = rows.Close()
			return err
		}
		cardIDs = append(cardIDs, cardID)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(cardIDs) == 0 {
		return nil
	}
	forkGenerations, err := loadRunForkEntityGenerations(ctx, tx, forkRunID, entityID)
	if err != nil {
		return err
	}
	for _, cardID := range cardIDs {
		sourceCard, err := loadDecisionCard(ctx, tx, cardID, true, false)
		if err != nil {
			return fmt.Errorf("load source proposed-effect card %s: %w", cardID, err)
		}
		pendingAtFork := sourceCard.Status == decisioncard.StatusPending
		if !sourceCard.DecidedAt.IsZero() {
			pendingAtFork = sourceCard.DecidedAt.After(forkPoint.Timestamp)
		} else if sourceCard.Status == decisioncard.StatusSuperseded {
			pendingAtFork = sourceCard.UpdatedAt.After(forkPoint.Timestamp)
		}
		if !pendingAtFork {
			continue
		}
		sourceContinuation, err := loadProposedEffectContinuation(ctx, tx, cardID, true, false)
		if err != nil {
			return fmt.Errorf("load source proposed-effect continuation %s: %w", cardID, err)
		}
		forkCard, forkContinuation, err := forkPendingProposedEffect(sourceCard, sourceContinuation, forkRunID, forkGenerations, now)
		if err != nil {
			return err
		}
		if err := insertProposedEffectCard(ctx, tx, forkCard, forkContinuation, true); err != nil {
			return fmt.Errorf("insert fork-local proposed effect: %w", err)
		}
	}
	return nil
}

func forkPendingProposedEffect(sourceCard decisioncard.Card, source decisioncard.ProposedEffectContinuation, forkRunID string, forkGenerations []attemptgeneration.Generation, now time.Time) (decisioncard.Card, decisioncard.ProposedEffectContinuation, error) {
	source = source.Canonical()
	fork := source
	fork.RunID = strings.TrimSpace(forkRunID)
	fork.SourceRunID = fork.RunID
	fork.ReplyContextID = ""
	fork.SourceEventID = activityidentity.ForkLineageEventID(fork.RunID, source.SourceEventID)
	if source.ParentEventID != "" {
		fork.ParentEventID = activityidentity.ForkLineageEventID(fork.RunID, source.ParentEventID)
	}
	if source.Generation.Valid() {
		matched := false
		for _, generation := range forkGenerations {
			if strings.TrimSpace(generation.LoopID) == strings.TrimSpace(source.Generation.LoopID) {
				fork.Generation = generation.Normalize()
				matched = true
				break
			}
		}
		if !matched {
			return decisioncard.Card{}, decisioncard.ProposedEffectContinuation{}, fmt.Errorf("fork proposed effect %s has no fork-local loop generation", source.ActivityID)
		}
	}
	fact := activityidentity.Fact{
		RunID: fork.RunID, SourceEventID: fork.SourceEventID, ParentEventID: fork.ParentEventID,
		EntityID: fork.EntityID, FlowID: fork.FlowID, NodeID: fork.NodeID,
		HandlerEventKey: fork.HandlerEventKey, ActivityID: fork.ActivityID, Tool: fork.Tool,
		Attempt: fork.Attempt, RevisionID: fork.Generation.RevisionID,
	}
	fork.RequestEventID = activityidentity.RequestEventID(fact)
	sourceAnchor, err := sourceCard.Anchor.ProposedEffect()
	if err != nil {
		return decisioncard.Card{}, decisioncard.ProposedEffectContinuation{}, err
	}
	fork.CardID = decisioncard.ProposedEffectCardID(fork.RequestEventID, sourceAnchor.Decision)
	fork.State = decisioncard.ProposedEffectPending
	fork.Verdict = ""
	fork.DecisionEventID = ""
	fork.RouteEventID = ""
	fork.SupersededReason = ""
	fork.CreatedAt = now.UTC()
	fork.UpdatedAt = now.UTC()
	effect, err := fork.EffectValue()
	if err != nil {
		return decisioncard.Card{}, decisioncard.ProposedEffectContinuation{}, err
	}
	fork.EffectContentHash, err = canonicaljson.HashValue(effect)
	if err != nil {
		return decisioncard.Card{}, decisioncard.ProposedEffectContinuation{}, err
	}
	scope := sourceAnchor.Scope
	scope.FlowInstance = strings.Trim(strings.TrimSpace(scope.FlowInstance), "/")
	scope.EntityID = strings.TrimSpace(scope.EntityID)
	scope.EntityID = fork.EntityID
	anchor, err := decisioncard.NewProposedEffectAnchor(decisioncard.ProposedEffectAnchor{
		RequestEventID: fork.RequestEventID, ActivityID: fork.ActivityID, Decision: sourceAnchor.Decision, Scope: scope,
	})
	if err != nil {
		return decisioncard.Card{}, decisioncard.ProposedEffectContinuation{}, err
	}
	forkCard := sourceCard
	forkCard.CardID = fork.CardID
	forkCard.RunID = fork.RunID
	forkCard.Anchor = anchor
	forkCard.EffectContentHash = fork.EffectContentHash
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
	forkedFromCardID, _ := semanticvalue.String(sourceCard.CardID)
	forkCard.Provenance, err = sourceCard.Provenance.With("forked_from_card_id", forkedFromCardID)
	if err != nil {
		return decisioncard.Card{}, decisioncard.ProposedEffectContinuation{}, err
	}
	forkedFromRequestID, _ := semanticvalue.String(source.RequestEventID)
	forkCard.Provenance, err = forkCard.Provenance.With("forked_from_request_event_id", forkedFromRequestID)
	if err != nil {
		return decisioncard.Card{}, decisioncard.ProposedEffectContinuation{}, err
	}
	forkCard, err = decisioncard.New(forkCard)
	if err != nil {
		return decisioncard.Card{}, decisioncard.ProposedEffectContinuation{}, err
	}
	if err := fork.Validate(forkCard); err != nil {
		return decisioncard.Card{}, decisioncard.ProposedEffectContinuation{}, err
	}
	return forkCard, fork, nil
}
