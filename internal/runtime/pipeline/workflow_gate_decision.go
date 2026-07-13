package pipeline

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/gateruntime"
	"github.com/google/uuid"
)

const workflowGateDecisionEventType events.EventType = "mailbox.card_decided"

func (pc *PipelineCoordinator) handleWorkflowGateDecisionEvent(ctx context.Context, evt events.Event) ([]events.Event, error) {
	if pc == nil || pc.decisionCards == nil || pc.workflowStore == nil {
		return nil, fmt.Errorf("gate decision runtime is not configured")
	}
	payload, err := canonicaljson.Decode(evt.Payload())
	if err != nil {
		return nil, fmt.Errorf("decode mailbox.card_decided payload: %w", err)
	}
	cardIDValue, ok := payload.Lookup("card_id")
	if !ok {
		return nil, fmt.Errorf("mailbox.card_decided card_id is required")
	}
	cardID, ok := cardIDValue.String()
	if !ok {
		return nil, fmt.Errorf("mailbox.card_decided card_id must be a string")
	}
	cardID = strings.TrimSpace(cardID)
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
	if current := workflowGateBundleHash(ctx, pc); current == "" || current != card.BundleHash {
		return nil, DeferPipelineReceipt(runtimefailures.New(
			runtimefailures.ClassDependencyUnavailable,
			"decision_card_bundle_unavailable",
			runtimeWorkflowID,
			"route_gate_decision",
			map[string]any{"card_id": card.CardID, "required_bundle_hash": card.BundleHash, "current_bundle_hash": current},
		))
	}
	emitted, err := workflowGateOutcomeEvent(card, evt, outcome)
	if err != nil {
		return nil, err
	}
	if err := pc.routeWorkflowGateDecision(ctx, card, evt, outcome, emitted); err != nil {
		return nil, err
	}
	return nil, nil
}

func (pc *PipelineCoordinator) routeWorkflowGateDecision(ctx context.Context, card decisioncard.Card, evt events.Event, outcome decisioncard.FrozenOutcome, emitted *events.Event) error {
	return pc.workflowStore.RunPipelineMutation(ctx, func(txctx context.Context) error {
		if err := pc.workflowStore.RequireGateRouteAdmitted(txctx, card.RunID); err != nil {
			return err
		}
		currentStage := ""
		alreadyRouted := false
		if err := pc.workflowStore.MutateE(txctx, card.EntityID, func(instance *WorkflowInstance) error {
			currentStage = strings.TrimSpace(instance.CurrentState)
			carrier, err := runtimeengine.StateCarrierFromPersisted(instance.Metadata, instance.StateBuckets)
			if err != nil {
				return err
			}
			activation, found, err := gateruntime.Load(carrier.StateBuckets, card.FlowID, card.DecisionID)
			if err != nil {
				return err
			}
			if found && activation.ActivationID == card.StageActivationID && activation.CardID == card.CardID && activation.Status == gateruntime.StatusRouted && activation.DecisionEventID == evt.ID() {
				if currentStage != strings.TrimSpace(outcome.AdvancesTo) {
					return fmt.Errorf("routed decision card state does not match its frozen outcome")
				}
				alreadyRouted = true
				return nil
			}
			if currentStage != card.Stage {
				return fmt.Errorf("decision card stage is no longer current")
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
		if alreadyRouted {
			return nil
		}
		pc.notifyTestEntityStateUpdated(card.EntityID, outcome.AdvancesTo)
		if err := pc.reconcileWorkflowStageTimers(txctx, card.EntityID, currentStage, outcome.AdvancesTo, evt.ID()); err != nil {
			return err
		}
		if err := pc.applyWorkflowJoinIntents(txctx, card.EntityID, currentStage, outcome.AdvancesTo); err != nil {
			return err
		}
		if err := pc.applyWorkflowGateIntents(txctx, card.EntityID, currentStage, outcome.AdvancesTo, evt.ID()); err != nil {
			return err
		}
		if emitted != nil {
			publisher, ok := pc.bus.(workflowGateMutationPublisher)
			if !ok || publisher == nil {
				return fmt.Errorf("transactional event publisher is required for gate outcome")
			}
			if err := publisher.PublishInMutation(txctx, *emitted); err != nil {
				return fmt.Errorf("publish frozen gate outcome: %w", err)
			}
		}
		return nil
	})
}

func workflowGateOutcomeEvent(card decisioncard.Card, parent events.Event, outcome decisioncard.FrozenOutcome) (*events.Event, error) {
	if outcome.Emit.Empty() || strings.TrimSpace(outcome.Emit.Event) == "" {
		return nil, nil
	}
	payload, err := decisioncard.BuildOutcomePayload(outcome, card.Fields)
	if err != nil {
		return nil, err
	}
	raw, err := canonicaljson.Encode(payload)
	if err != nil {
		return nil, err
	}
	eventType := strings.TrimSpace(outcome.Emit.Event)
	envelope := events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, card.EntityID), card.FlowInstance)
	identity := strings.Join([]string{card.CardID, card.DecisionEventID, card.Verdict, eventType}, "\x00")
	createdAt := card.DecidedAt
	if createdAt.IsZero() {
		createdAt = parent.CreatedAt()
	}
	produced := events.NewChildEvent(uuid.NewSHA1(uuid.NameSpaceOID, []byte("swarm.gate.outcome.v1\x00"+identity)).String(), events.EventType(eventType), runtimeWorkflowID, "", raw, parent.ChainDepth()+1, parent, envelope, createdAt.UTC())
	return &produced, nil
}
