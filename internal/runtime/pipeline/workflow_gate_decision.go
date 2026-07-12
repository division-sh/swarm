package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/gateruntime"
	"github.com/google/uuid"
)

const workflowGateDecisionEventType events.EventType = "mailbox.card_decided"

func (pc *PipelineCoordinator) handleWorkflowGateDecisionEvent(ctx context.Context, evt events.Event) ([]events.Event, error) {
	if pc == nil || pc.decisionCards == nil || pc.workflowStore == nil {
		return nil, fmt.Errorf("gate decision runtime is not configured")
	}
	payload := parsePayloadMap(evt.Payload())
	cardID := strings.TrimSpace(asString(payload["card_id"]))
	if cardID == "" {
		return nil, fmt.Errorf("mailbox.card_decided card_id is required")
	}
	card, err := pc.decisionCards.GetDecisionCard(ctx, cardID)
	if err != nil {
		return nil, err
	}
	if card.Status != decisioncard.StatusDecided || card.DecisionEventID != evt.ID() {
		return nil, fmt.Errorf("mailbox.card_decided does not match the authoritative card decision")
	}
	outcome, ok := card.Snapshot.Outcomes[card.Verdict]
	if !ok {
		return nil, fmt.Errorf("card verdict %s is absent from the frozen outcome plan", card.Verdict)
	}
	if err := pc.routeWorkflowGateDecision(ctx, card, evt, outcome); err != nil {
		return nil, err
	}
	emitted, err := pc.workflowGateOutcomeEvents(card, evt, outcome)
	if err != nil {
		return nil, err
	}
	return emitted, nil
}

func (pc *PipelineCoordinator) routeWorkflowGateDecision(ctx context.Context, card decisioncard.Card, evt events.Event, outcome runtimecontracts.WorkflowGateOutcomePlan) error {
	return pc.workflowStore.RunPipelineMutation(ctx, func(txctx context.Context) error {
		if err := pc.workflowStore.RequireGateRouteAdmitted(txctx, card.RunID); err != nil {
			return err
		}
		currentStage := ""
		if err := pc.workflowStore.MutateE(txctx, card.EntityID, func(instance *WorkflowInstance) error {
			currentStage = strings.TrimSpace(instance.CurrentState)
			if currentStage != card.Stage {
				return fmt.Errorf("decision card stage is no longer current")
			}
			carrier, err := runtimeengine.StateCarrierFromPersisted(instance.Metadata, instance.StateBuckets)
			if err != nil {
				return err
			}
			activation, found, err := gateruntime.Load(carrier.StateBuckets, card.FlowID, card.DecisionID)
			if err != nil {
				return err
			}
			if !found || activation.ActivationID != card.StageActivationID || activation.CardID != card.CardID {
				return fmt.Errorf("decision card activation is no longer authoritative")
			}
			if err := activation.Route(evt.ID(), evt.CreatedAt()); err != nil {
				return err
			}
			if err := gateruntime.Store(carrier.StateBuckets, activation); err != nil {
				return err
			}
			next := strings.TrimSpace(outcome.AdvancesTo)
			instance.CurrentState = next
			instance.EnteredStageAt = time.Now().UTC()
			instance.TransitionHistory = append(instance.TransitionHistory, workflowTransitionRecord(pc.WorkflowDefinition(), currentStage, next, evt.ID()))
			instance.StateBuckets = carrier.PersistedStateBuckets()
			return nil
		}); err != nil {
			return err
		}
		pc.notifyTestEntityStateUpdated(card.EntityID, outcome.AdvancesTo)
		if err := pc.reconcileWorkflowStageTimers(txctx, card.EntityID, currentStage, outcome.AdvancesTo, evt.ID()); err != nil {
			return err
		}
		if err := pc.applyWorkflowJoinIntents(txctx, card.EntityID, currentStage, outcome.AdvancesTo); err != nil {
			return err
		}
		return pc.applyWorkflowGateIntents(txctx, card.EntityID, currentStage, outcome.AdvancesTo, evt.ID())
	})
}

func (pc *PipelineCoordinator) workflowGateOutcomeEvents(card decisioncard.Card, parent events.Event, outcome runtimecontracts.WorkflowGateOutcomePlan) ([]events.Event, error) {
	if outcome.Emit.Empty() || strings.TrimSpace(outcome.Emit.Event) == "" {
		return nil, nil
	}
	payload := map[string]any{}
	for field, expression := range outcome.Emit.Fields {
		value, err := decisionCardEmissionValue(expression, card.Fields)
		if err != nil {
			return nil, fmt.Errorf("gate outcome field %s: %w", field, err)
		}
		payload[field] = value
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	eventType := strings.TrimSpace(outcome.Emit.Event)
	if pc.SemanticSource() != nil {
		eventType = pc.SemanticSource().ResolveFlowEventReference(card.FlowID, eventType)
	}
	envelope := events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, card.EntityID), card.FlowInstance)
	produced := events.NewChildEvent(uuid.NewString(), events.EventType(eventType), runtimeWorkflowID, "", raw, parent.ChainDepth()+1, parent, envelope, time.Now().UTC())
	return []events.Event{produced}, nil
}

func decisionCardEmissionValue(expression runtimecontracts.ExpressionValue, fields map[string]any) (any, error) {
	if expression.HasLiteralValue() {
		return expression.Literal, nil
	}
	raw := strings.TrimSpace(firstNonEmptyString(expression.Ref, expression.CEL))
	if !strings.HasPrefix(raw, "decision.") || strings.Count(raw, ".") != 1 {
		return nil, fmt.Errorf("only exact decision.<field> references are supported")
	}
	field := strings.TrimPrefix(raw, "decision.")
	value, ok := fields[field]
	if !ok {
		return nil, fmt.Errorf("decision field %s is absent", field)
	}
	return value, nil
}
