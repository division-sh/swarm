package pipeline

import (
	"context"
	"fmt"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/gateruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func (r pipelineEngineStateRepo) validateMutationTransition(ctx context.Context, current WorkflowInstance, mutation runtimeengine.StateMutation, flowID string) error {
	cause := mutation.Transition
	if cause == nil || r.coordinator == nil || r.coordinator.SemanticSource() == nil {
		return fmt.Errorf("workflow transition requires selected source and admitted carrier evidence")
	}
	if err := cause.Validate(); err != nil {
		return err
	}
	graph, ok := semanticview.WorkflowStageTopology(r.coordinator.SemanticSource(), flowID)
	if !ok || cause.FlowID() != graph.FlowID {
		return fmt.Errorf("workflow transition evidence belongs to another flow")
	}
	compiled, hasCompiled := cause.Compiled()
	if !hasCompiled || compiled.Edge().Source != "gate" {
		if err := cause.ValidateAgainst(graph); err != nil {
			return err
		}
		if node, event, hasHandler := cause.HandlerOrigin(); hasHandler {
			handler, found := r.coordinator.SemanticSource().ExecutableNodeEventHandlers(node)[event]
			if !found {
				return fmt.Errorf("transition has no exact compiled handler")
			}
			if err := cause.ValidateHandlerEvidence(handler); err != nil {
				return err
			}
			accepted, hasAccepted := runtimecorrelation.InboundEventFromContext(ctx)
			if !hasAccepted || accepted.ID() == "" || accepted.Type() == "" || accepted.CreatedAt().IsZero() {
				return fmt.Errorf("transition requires its accepted execution event")
			}
			if application, present := deliveryTargetApplicationFromContext(ctx); present {
				if err := application.Validate(); err != nil {
					return err
				}
				applied := application.Event()
				if accepted.ID() != applied.ID() || accepted.Type() != applied.Type() || !accepted.CreatedAt().Equal(applied.CreatedAt()) {
					return fmt.Errorf("transition execution event contradicts its admitted delivery application")
				}
			}
			if mutation.TriggerEventID != accepted.ID() || mutation.TriggerEventType != string(accepted.Type()) || !mutation.TriggeredAt.Equal(accepted.CreatedAt()) {
				return fmt.Errorf("transition trigger contradicts its accepted execution event")
			}
			if delivery, present := workflowNodeDeliveryRoute(ctx); present {
				recipient, isNode := delivery.Recipient.Node()
				if !isNode || !recipient.Equal(node) {
					return fmt.Errorf("transition handler contradicts its admitted delivery recipient")
				}
			}
			// Protocol joins and compiled connects can select a handler whose
			// declaration key differs from the accepted event type.
			resolved := workflowNodeEventHandlerResolutionForDeliveryContext(ctx, r.coordinator.SemanticSource(), node, accepted)
			if !resolved.Matched || resolved.HandlerEventKey != event || resolved.FlowID != flowID {
				return fmt.Errorf("transition handler contradicts its accepted execution selection")
			}
			return nil
		}
		return nil
	}

	// Frozen gate continuations, not the ambient graph, own verdict evidence.
	// This is the existing decision/card authority; no route is reconstructed.
	edge := compiled.Edge()
	carrier, err := workflowInstanceStateCarrier(current)
	if err != nil {
		return err
	}
	activation, found, err := gateruntime.Load(carrier.StateBuckets, flowID, edge.DecisionID)
	if err != nil {
		return err
	}
	if !found || r.coordinator.decisionCards == nil {
		return fmt.Errorf("gate transition has no authoritative activation/card")
	}
	card, err := r.coordinator.decisionCards.GetDecisionCard(ctx, activation.CardID)
	if err != nil {
		return err
	}
	if card.Status != decisioncard.StatusDecided || card.DecisionEventID != mutation.TriggerEventID || card.Verdict != edge.Verdict || mutation.TriggerEventType != string(workflowGateDecisionEventType) {
		return fmt.Errorf("gate transition contradicts the accepted decision")
	}
	if pin := workflowGateBundleHash(ctx, r.coordinator); pin == "" || pin != card.BundleHash {
		return fmt.Errorf("gate transition requires its recorded bundle pin")
	}
	anchor, err := card.Anchor.StageGate()
	if err != nil {
		return err
	}
	if err := validateStageGateInstanceOwner(anchor, current, activation); err != nil {
		return err
	}
	if edge.From != anchor.Stage || activation.ActivationID != anchor.StageActivationID || activation.CardID != card.CardID {
		return fmt.Errorf("gate transition contradicts its stage activation")
	}
	route, err := gateruntime.RouteFor(activation.RoutesJSON, card.Verdict)
	if err != nil {
		return err
	}
	if route.Transition != compiled {
		return fmt.Errorf("gate transition contradicts its frozen carrier")
	}
	return nil
}
