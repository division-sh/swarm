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

const (
	workflowGateDecisionEventType events.EventType = "mailbox.card_decided"
	decisionCardDeferredEventType events.EventType = "mailbox.card_deferred"
	decisionCardExpiredEventType  events.EventType = "mailbox.card_expired"
)

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
	switch card.Anchor.Kind() {
	case decisioncard.AnchorKindStageGate:
		return pc.handleStageGateDecisionCard(ctx, evt, card)
	case decisioncard.AnchorKindHumanTask:
		return pc.handleHumanTaskDecisionCard(ctx, evt, card)
	default:
		return nil, fmt.Errorf("decision-card anchor kind %q is not registered", card.Anchor.Kind())
	}
}

func (pc *PipelineCoordinator) handleDecisionCardDeferredEvent(ctx context.Context, evt events.Event) ([]events.Event, error) {
	if pc == nil || pc.decisionCards == nil || pc.workflowStore == nil {
		return nil, fmt.Errorf("decision-card runtime is not configured")
	}
	cardID, err := decisionCardLifecycleEventCardID(evt)
	if err != nil {
		return nil, err
	}
	card, err := pc.decisionCards.GetDecisionCard(ctx, cardID)
	if err != nil {
		return nil, err
	}
	if card.Status != decisioncard.StatusPending || card.DeferredUntil.IsZero() {
		return nil, fmt.Errorf("mailbox.card_deferred does not match the authoritative card state")
	}
	if card.Anchor.Kind() != decisioncard.AnchorKindHumanTask {
		return nil, nil
	}
	store, ok := pc.decisionCards.(decisioncard.HumanTaskStore)
	if !ok || store == nil {
		return nil, fmt.Errorf("human-task continuation store is not configured")
	}
	anchor, err := card.Anchor.HumanTask()
	if err != nil {
		return nil, err
	}
	continuation, err := store.LoadHumanTaskContinuation(ctx, card.CardID)
	if err != nil {
		return nil, err
	}
	if continuation.State != decisioncard.HumanTaskContinuationPending || !continuation.DeferredUntil.Equal(card.DeferredUntil) {
		return nil, fmt.Errorf("mailbox.card_deferred does not match the authoritative human-task continuation")
	}
	payload, err := canonicaljson.Bytes(map[string]any{
		"card_id": card.CardID, "status": "deferred", "cause": continuation.DeferCause,
		"resume_at": continuation.DeferredUntil.UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return nil, err
	}
	productID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("swarm.human-task.deferred.v1\x00"+card.CardID+"\x00"+evt.ID())).String()
	product := events.NewChildEvent(productID, "human_task.deferred", runtimeWorkflowID, "", payload, evt.ChainDepth()+1, evt,
		humanTaskRequesterOutcomeEnvelope(continuation), evt.CreatedAt().UTC())
	return nil, pc.workflowStore.RunPipelineMutation(ctx, func(txctx context.Context) error {
		if continuation.ReplyContextID != "" {
			delivery := events.DeliveryContext{Reply: &events.ReplyContextRef{ID: continuation.ReplyContextID}}
			product = product.WithDeliveryContext(delivery)
			txctx = events.WithDeliveryContext(txctx, delivery)
		}
		publisher, ok := pc.bus.(decisionCardDirectMutationPublisher)
		if !ok || publisher == nil {
			return fmt.Errorf("transactional direct event publisher is required for human-task defer")
		}
		return publisher.PublishDirectInMutation(txctx, product, []string{anchor.RequesterAgentID})
	})
}

func (pc *PipelineCoordinator) handleDecisionCardExpiredEvent(ctx context.Context, evt events.Event) ([]events.Event, error) {
	if pc == nil || pc.decisionCards == nil || pc.workflowStore == nil {
		return nil, fmt.Errorf("decision-card runtime is not configured")
	}
	cardID, err := decisionCardLifecycleEventCardID(evt)
	if err != nil {
		return nil, err
	}
	card, err := pc.decisionCards.GetDecisionCard(ctx, cardID)
	if err != nil {
		return nil, err
	}
	if card.Anchor.Kind() != decisioncard.AnchorKindHumanTask || card.Status != decisioncard.StatusExpired {
		return nil, fmt.Errorf("mailbox.card_expired does not match an authoritative expired human-task card")
	}
	store, ok := pc.decisionCards.(decisioncard.HumanTaskStore)
	if !ok || store == nil {
		return nil, fmt.Errorf("human-task continuation store is not configured")
	}
	anchor, err := card.Anchor.HumanTask()
	if err != nil {
		return nil, err
	}
	continuation, err := store.LoadHumanTaskContinuation(ctx, card.CardID)
	if err != nil {
		return nil, err
	}
	if continuation.OutcomeEventID != evt.ID() || (continuation.State != decisioncard.HumanTaskContinuationExpired && continuation.State != decisioncard.HumanTaskContinuationOutcomeDispatched) {
		return nil, fmt.Errorf("mailbox.card_expired does not match the authoritative human-task continuation")
	}
	payload, err := canonicaljson.Bytes(map[string]any{
		"card_id": card.CardID, "status": "expired", "cause": "deadline_elapsed",
		"deadline_at": continuation.DeadlineAt.UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return nil, err
	}
	productID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("swarm.human-task.expiry-outcome.v1\x00"+card.CardID+"\x00"+evt.ID())).String()
	product := events.NewChildEvent(productID, "human_task.expired", runtimeWorkflowID, "", payload, evt.ChainDepth()+1, evt,
		humanTaskRequesterOutcomeEnvelope(continuation), card.DecidedAt.UTC())
	return nil, pc.workflowStore.RunPipelineMutation(ctx, func(txctx context.Context) error {
		if _, err := store.CompleteHumanTaskOutcome(txctx, card.CardID, evt.ID(), card.DecidedAt); err != nil {
			return err
		}
		if continuation.ReplyContextID != "" {
			delivery := events.DeliveryContext{Reply: &events.ReplyContextRef{ID: continuation.ReplyContextID}}
			product = product.WithDeliveryContext(delivery)
			txctx = events.WithDeliveryContext(txctx, delivery)
		}
		publisher, ok := pc.bus.(decisionCardDirectMutationPublisher)
		if !ok || publisher == nil {
			return fmt.Errorf("transactional direct event publisher is required for human-task expiry")
		}
		return publisher.PublishDirectInMutation(txctx, product, []string{anchor.RequesterAgentID})
	})
}

func decisionCardLifecycleEventCardID(evt events.Event) (string, error) {
	payload, err := canonicaljson.Decode(evt.Payload())
	if err != nil {
		return "", fmt.Errorf("decode %s payload: %w", evt.Type(), err)
	}
	value, ok := payload.Lookup("card_id")
	if !ok {
		return "", fmt.Errorf("%s card_id is required", evt.Type())
	}
	cardID, ok := value.String()
	if !ok || strings.TrimSpace(cardID) == "" {
		return "", fmt.Errorf("%s card_id must be a non-empty string", evt.Type())
	}
	return strings.TrimSpace(cardID), nil
}

func humanTaskRequesterOutcomeEnvelope(continuation decisioncard.HumanTaskContinuation) events.EventEnvelope {
	if route := continuation.RequesterRoute.Normalized(); !route.Empty() {
		return events.EnvelopeForTargetRoute(events.EventEnvelope{}, route)
	}
	return events.EventEnvelope{}
}

func (pc *PipelineCoordinator) handleStageGateDecisionCard(ctx context.Context, evt events.Event, card decisioncard.Card) ([]events.Event, error) {
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

func (pc *PipelineCoordinator) handleHumanTaskDecisionCard(ctx context.Context, evt events.Event, card decisioncard.Card) ([]events.Event, error) {
	store, ok := pc.decisionCards.(decisioncard.HumanTaskStore)
	if !ok || store == nil {
		return nil, fmt.Errorf("human-task continuation store is not configured")
	}
	anchor, err := card.Anchor.HumanTask()
	if err != nil {
		return nil, err
	}
	var eventType events.EventType
	switch card.Verdict {
	case "approve":
		eventType = "human_task.approved"
	case "reject":
		eventType = "human_task.rejected"
	default:
		return nil, fmt.Errorf("human-task card verdict %q is unsupported", card.Verdict)
	}
	payload, err := canonicaljson.Bytes(map[string]any{
		"card_id": card.CardID, "requester_agent_id": anchor.RequesterAgentID,
		"status": strings.TrimPrefix(string(eventType), "human_task."),
		"fields": card.Fields.Interface(), "decided_by": card.DecidedBy,
		"decided_at": card.DecidedAt.UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return nil, err
	}
	return nil, pc.workflowStore.RunPipelineMutation(ctx, func(txctx context.Context) error {
		continuation, err := store.CompleteHumanTaskOutcome(txctx, card.CardID, evt.ID(), card.DecidedAt)
		if err != nil {
			return err
		}
		productEventID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("swarm.human-task.outcome.v1\x00"+card.CardID+"\x00"+evt.ID())).String()
		product := events.NewChildEvent(productEventID, eventType, runtimeWorkflowID, "", payload, evt.ChainDepth()+1, evt,
			humanTaskRequesterOutcomeEnvelope(continuation), card.DecidedAt.UTC())
		if continuation.ReplyContextID != "" {
			delivery := events.DeliveryContext{Reply: &events.ReplyContextRef{ID: continuation.ReplyContextID}}
			product = product.WithDeliveryContext(delivery)
			txctx = events.WithDeliveryContext(txctx, delivery)
		}
		publisher, ok := pc.bus.(decisionCardDirectMutationPublisher)
		if !ok || publisher == nil {
			return fmt.Errorf("transactional direct event publisher is required for human-task outcome")
		}
		return publisher.PublishDirectInMutation(txctx, product, []string{anchor.RequesterAgentID})
	})
}

func (pc *PipelineCoordinator) routeWorkflowGateDecision(ctx context.Context, card decisioncard.Card, evt events.Event, outcome decisioncard.FrozenOutcome, emitted *events.Event) error {
	anchor, err := card.Anchor.StageGate()
	if err != nil {
		return err
	}
	return pc.workflowStore.RunPipelineMutation(ctx, func(txctx context.Context) error {
		if err := pc.workflowStore.RequireGateRouteAdmitted(txctx, card.RunID); err != nil {
			return err
		}
		currentStage := ""
		alreadyRouted := false
		if err := pc.workflowStore.MutateE(txctx, anchor.EntityID, func(instance *WorkflowInstance) error {
			currentStage = strings.TrimSpace(instance.CurrentState)
			carrier, err := runtimeengine.StateCarrierFromPersisted(instance.Metadata, instance.StateBuckets)
			if err != nil {
				return err
			}
			activation, found, err := gateruntime.Load(carrier.StateBuckets, anchor.FlowID, card.Snapshot.Decision)
			if err != nil {
				return err
			}
			if found && activation.ActivationID == anchor.StageActivationID && activation.CardID == card.CardID && activation.Status == gateruntime.StatusRouted && activation.DecisionEventID == evt.ID() {
				if currentStage != strings.TrimSpace(outcome.AdvancesTo) {
					return fmt.Errorf("routed decision card state does not match its frozen outcome")
				}
				alreadyRouted = true
				return nil
			}
			if currentStage != anchor.Stage {
				return fmt.Errorf("decision card stage is no longer current")
			}
			if !found || activation.ActivationID != anchor.StageActivationID || activation.CardID != card.CardID {
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
		pc.notifyTestEntityStateUpdated(anchor.EntityID, outcome.AdvancesTo)
		if err := pc.reconcileWorkflowStageTimers(txctx, anchor.EntityID, currentStage, outcome.AdvancesTo, evt.ID()); err != nil {
			return err
		}
		if err := pc.applyWorkflowJoinIntents(txctx, anchor.EntityID, currentStage, outcome.AdvancesTo); err != nil {
			return err
		}
		if err := pc.applyWorkflowGateIntents(txctx, anchor.EntityID, currentStage, outcome.AdvancesTo, evt.ID()); err != nil {
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
	anchor, err := card.Anchor.StageGate()
	if err != nil {
		return nil, err
	}
	envelope := events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, anchor.EntityID), anchor.FlowInstance)
	identity := strings.Join([]string{card.CardID, card.DecisionEventID, card.Verdict, eventType}, "\x00")
	createdAt := card.DecidedAt
	if createdAt.IsZero() {
		createdAt = parent.CreatedAt()
	}
	produced := events.NewChildEvent(uuid.NewSHA1(uuid.NameSpaceOID, []byte("swarm.gate.outcome.v1\x00"+identity)).String(), events.EventType(eventType), runtimeWorkflowID, "", raw, parent.ChainDepth()+1, parent, envelope, createdAt.UTC())
	return &produced, nil
}
