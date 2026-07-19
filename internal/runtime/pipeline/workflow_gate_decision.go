package pipeline

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
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
	case decisioncard.AnchorKindProposedEffect:
		return pc.handleProposedEffectDecisionCard(ctx, evt, card)
	default:
		return nil, fmt.Errorf("decision-card anchor kind %q is not registered", card.Anchor.Kind())
	}
}

func (pc *PipelineCoordinator) handleProposedEffectDecisionCard(ctx context.Context, evt events.Event, card decisioncard.Card) ([]events.Event, error) {
	store, ok := pc.decisionCards.(decisioncard.ProposedEffectStore)
	if !ok || store == nil {
		return nil, fmt.Errorf("proposed-effect continuation store is not configured")
	}
	continuation, err := store.LoadProposedEffectContinuation(ctx, card.CardID)
	if err != nil {
		return nil, err
	}
	if continuation.DecisionEventID != evt.ID() || continuation.Verdict != card.Verdict {
		return nil, fmt.Errorf("mailbox.card_decided does not match the authoritative proposed-effect continuation")
	}
	var released []runtimeengine.EmitIntent
	err = pc.workflowStore.RunPipelineMutation(ctx, func(txctx context.Context) error {
		continuation, err := store.LoadProposedEffectContinuation(txctx, card.CardID)
		if err != nil {
			return err
		}
		if continuation.RouteEventID == evt.ID() && (continuation.State == decisioncard.ProposedEffectRequestReleased || continuation.State == decisioncard.ProposedEffectOutcomeDispatched) {
			_, err := store.CompleteProposedEffectRoute(txctx, card.CardID, evt.ID(), card.DecidedAt)
			return err
		}
		if continuation.State != decisioncard.ProposedEffectDecisionCommitted {
			return fmt.Errorf("proposed-effect continuation is not ready to route")
		}
		if current := workflowGateBundleHash(txctx, pc); current == "" || current != card.BundleHash {
			return DeferPipelineReceipt(runtimefailures.New(
				runtimefailures.ClassDependencyUnavailable,
				"decision_card_bundle_unavailable",
				runtimeWorkflowID,
				"route_proposed_effect_decision",
				map[string]any{"card_id": card.CardID, "required_bundle_hash": card.BundleHash, "current_bundle_hash": current},
			))
		}
		switch card.Verdict {
		case "approve":
			request, err := activityRequestEmitIntent(activityIntentFromProposedEffect(continuation))
			if err != nil {
				return err
			}
			outbox := pc.bus.EngineOutbox()
			if outbox == nil {
				return fmt.Errorf("approved activity release requires pipeline outbox")
			}
			if err := outbox.WriteOutbox(txctx, []runtimeengine.EmitIntent{request}); err != nil {
				return err
			}
			released = []runtimeengine.EmitIntent{request}
		case "revise", "reject":
			product, err := proposedEffectOutcomeEvent(card, evt, continuation)
			if err != nil {
				return err
			}
			publisher, ok := pc.bus.(workflowGateMutationPublisher)
			if !ok || publisher == nil {
				return fmt.Errorf("transactional event publisher is required for proposed-effect outcome")
			}
			if continuation.ReplyContextID != "" {
				delivery := events.DeliveryContext{Reply: &events.ReplyContextRef{ID: continuation.ReplyContextID}}
				txctx = events.WithDeliveryContext(txctx, delivery)
			}
			if err := publisher.PublishInMutation(txctx, product); err != nil {
				return err
			}
		default:
			return fmt.Errorf("proposed-effect verdict %q is unsupported", card.Verdict)
		}
		_, err = store.CompleteProposedEffectRoute(txctx, card.CardID, evt.ID(), card.DecidedAt)
		return err
	})
	if err != nil || len(released) == 0 {
		return nil, err
	}
	dispatcher := pc.bus.EngineDispatcher()
	if dispatcher == nil {
		return nil, fmt.Errorf("approved activity release requires post-commit dispatcher")
	}
	if err := dispatcher.DispatchPostCommit(context.WithoutCancel(ctx), released); err != nil {
		return nil, err
	}
	return nil, nil
}

func activityIntentFromProposedEffect(continuation decisioncard.ProposedEffectContinuation) runtimeengine.ActivityIntent {
	continuation = continuation.Canonical()
	intent := runtimeengine.ActivityIntent{
		Context: events.DeliveryContext{}, ActivityID: continuation.ActivityID, Tool: continuation.Tool,
		BundleHash: continuation.BundleHash, WorkflowVersion: continuation.WorkflowVersion,
		Input: continuation.Input, EffectClass: continuation.EffectClass,
		SuccessEvent: continuation.SuccessEvent, FailureEvent: continuation.FailureEvent,
		RevisionEvent: continuation.RevisionEvent, RejectedEvent: continuation.RejectedEvent,
		RetryMaxAttempts: continuation.RetryMaxAttempts, RetryBackoff: continuation.RetryBackoff, ForkPolicy: continuation.ForkPolicy,
		EntityID: identity.NormalizeEntityID(continuation.EntityID), NodeID: identity.NormalizeNodeID(continuation.NodeID),
		FlowID: identity.NormalizeFlowID(continuation.FlowID), FlowInstance: continuation.FlowInstance,
		HandlerEventKey: continuation.HandlerEventKey, SourceEventID: continuation.SourceEventID,
		SourceRunID: continuation.SourceRunID, SourceTaskID: continuation.SourceTaskID,
		ParentEventID: continuation.ParentEventID, ChainDepth: continuation.ChainDepth,
		Attempt: continuation.Attempt, Generation: continuation.Generation, LoopStage: continuation.LoopStage,
		ExecutionMode: continuation.ExecutionMode,
	}
	if continuation.ReplyContextID != "" {
		intent.Context = events.DeliveryContext{Reply: &events.ReplyContextRef{ID: continuation.ReplyContextID}}
	}
	return intent.Normalized()
}

func proposedEffectOutcomeEvent(card decisioncard.Card, parent events.Event, continuation decisioncard.ProposedEffectContinuation) (events.Event, error) {
	var noEvent events.Event
	eventType := continuation.RevisionEvent
	payloadValues := map[string]any{
		"card_id": card.CardID, "activity_id": continuation.ActivityID,
		"tool": continuation.Tool, "effect_class": string(continuation.EffectClass),
		"effect_content_hash": continuation.EffectContentHash,
		"decided_by":          card.DecidedBy, "decided_at": card.DecidedAt.UTC().Format(time.RFC3339Nano),
	}
	switch card.Verdict {
	case "revise":
		feedback, ok := card.Fields.Lookup("feedback")
		if !ok {
			return noEvent, fmt.Errorf("revision feedback is required")
		}
		payloadValues["feedback"] = feedback.Interface()
	case "reject":
		eventType = continuation.RejectedEvent
		if reason, ok := card.Fields.Lookup("reason"); ok {
			payloadValues["reason"] = reason.Interface()
		}
	default:
		return noEvent, fmt.Errorf("proposed-effect outcome verdict %q is unsupported", card.Verdict)
	}
	raw, err := canonicaljson.Bytes(payloadValues)
	if err != nil {
		return noEvent, err
	}
	if strings.TrimSpace(eventType) == "" {
		return noEvent, fmt.Errorf("proposed-effect %s event is missing", card.Verdict)
	}
	envelope := events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, continuation.EntityID), continuation.FlowInstance)
	eventID := decisioncard.ProposedEffectOutcomeEventID(card.CardID, parent.ID(), card.Verdict)
	return newWorkflowChildEvent(eventID, events.EventType(eventType), continuation.SourceTaskID, raw, parent.ChainDepth()+1, parent, envelope, card.DecidedAt.UTC())
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
	product, err := newWorkflowChildEvent(productID, "human_task.deferred", "", payload, evt.ChainDepth()+1, evt,
		humanTaskRequesterOutcomeEnvelope(continuation), evt.CreatedAt().UTC())
	if err != nil {
		return nil, err
	}
	return nil, pc.workflowStore.RunPipelineMutation(ctx, func(txctx context.Context) error {
		if continuation.ReplyContextID != "" {
			delivery := events.DeliveryContext{Reply: &events.ReplyContextRef{ID: continuation.ReplyContextID}}
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
	product, err := newWorkflowChildEvent(productID, "human_task.expired", "", payload, evt.ChainDepth()+1, evt,
		humanTaskRequesterOutcomeEnvelope(continuation), card.DecidedAt.UTC())
	if err != nil {
		return nil, err
	}
	return nil, pc.workflowStore.RunPipelineMutation(ctx, func(txctx context.Context) error {
		if continuation.ReplyContextID != "" {
			delivery := events.DeliveryContext{Reply: &events.ReplyContextRef{ID: continuation.ReplyContextID}}
			txctx = events.WithDeliveryContext(txctx, delivery)
		}
		publisher, ok := pc.bus.(decisionCardDirectMutationPublisher)
		if !ok || publisher == nil {
			return fmt.Errorf("transactional direct event publisher is required for human-task expiry")
		}
		if err := publisher.PublishDirectInMutation(txctx, product, []string{anchor.RequesterAgentID}); err != nil {
			return err
		}
		_, err = store.CompleteHumanTaskOutcome(txctx, card.CardID, evt.ID(), card.DecidedAt)
		return err
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
	if current := workflowGateBundleHash(ctx, pc); current == "" || current != card.BundleHash {
		return nil, DeferPipelineReceipt(runtimefailures.New(
			runtimefailures.ClassDependencyUnavailable,
			"decision_card_bundle_unavailable",
			runtimeWorkflowID,
			"route_gate_decision",
			map[string]any{"card_id": card.CardID, "required_bundle_hash": card.BundleHash, "current_bundle_hash": current},
		))
	}
	route, err := pc.loadStageGateRoute(ctx, card)
	if err != nil {
		return nil, err
	}
	emitted, err := workflowGateOutcomeEvent(card, evt, route)
	if err != nil {
		return nil, err
	}
	if err := pc.routeWorkflowGateDecision(ctx, card, evt, route, emitted); err != nil {
		return nil, err
	}
	return nil, nil
}

func (pc *PipelineCoordinator) loadStageGateRoute(ctx context.Context, card decisioncard.Card) (gateruntime.Route, error) {
	anchor, err := card.Anchor.StageGate()
	if err != nil {
		return gateruntime.Route{}, err
	}
	instance, found, err := pc.workflowStore.Load(ctx, anchor.EntityID)
	if err != nil {
		return gateruntime.Route{}, err
	}
	if !found {
		return gateruntime.Route{}, fmt.Errorf("decision card workflow instance is missing")
	}
	carrier, err := runtimeengine.StateCarrierFromPersisted(instance.Metadata, instance.StateBuckets)
	if err != nil {
		return gateruntime.Route{}, err
	}
	activation, found, err := gateruntime.Load(carrier.StateBuckets, anchor.FlowID, card.Snapshot.Decision)
	if err != nil {
		return gateruntime.Route{}, err
	}
	if !found || activation.ActivationID != anchor.StageActivationID || activation.CardID != card.CardID {
		return gateruntime.Route{}, fmt.Errorf("decision card activation is no longer authoritative")
	}
	return gateruntime.RouteFor(activation.RoutesJSON, card.Verdict)
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
		continuation, err := store.LoadHumanTaskContinuation(txctx, card.CardID)
		if err != nil {
			return err
		}
		productEventID := decisioncard.HumanTaskOutcomeEventID(card.CardID, evt.ID())
		product, err := newWorkflowChildEvent(productEventID, eventType, "", payload, evt.ChainDepth()+1, evt,
			humanTaskRequesterOutcomeEnvelope(continuation), card.DecidedAt.UTC())
		if err != nil {
			return err
		}
		if continuation.ReplyContextID != "" {
			delivery := events.DeliveryContext{Reply: &events.ReplyContextRef{ID: continuation.ReplyContextID}}
			txctx = events.WithDeliveryContext(txctx, delivery)
		}
		publisher, ok := pc.bus.(decisionCardDirectMutationPublisher)
		if !ok || publisher == nil {
			return fmt.Errorf("transactional direct event publisher is required for human-task outcome")
		}
		if err := publisher.PublishDirectInMutation(txctx, product, []string{anchor.RequesterAgentID}); err != nil {
			return err
		}
		_, err = store.CompleteHumanTaskOutcome(txctx, card.CardID, evt.ID(), card.DecidedAt)
		return err
	})
}

func (pc *PipelineCoordinator) routeWorkflowGateDecision(ctx context.Context, card decisioncard.Card, evt events.Event, route gateruntime.Route, emitted *events.Event) error {
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
				if currentStage != strings.TrimSpace(route.AdvancesTo) {
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
			next := strings.TrimSpace(route.AdvancesTo)
			instance.CurrentState = next
			instance.EnteredStageAt = evt.CreatedAt().UTC()
			instance.TransitionHistory = append(instance.TransitionHistory, workflowTransitionRecord(pc.WorkflowDefinition(), currentStage, next, evt.ID(), string(evt.Type()), evt.CreatedAt()))
			instance.StateBuckets = carrier.PersistedStateBuckets()
			return nil
		}); err != nil {
			return err
		}
		if alreadyRouted {
			return nil
		}
		pc.notifyTestEntityStateUpdated(anchor.EntityID, route.AdvancesTo)
		cause := workflowTimerCause{
			Kind:         workflowTimerCauseTransition,
			EventID:      evt.ID(),
			EventType:    strings.TrimSpace(string(evt.Type())),
			OccurredAt:   evt.CreatedAt(),
			TransitionID: workflowTransitionIdentity(pc.WorkflowDefinition(), currentStage, route.AdvancesTo, string(evt.Type())),
			FromState:    currentStage,
			ToState:      route.AdvancesTo,
		}
		if err := pc.workflowTimers.Reconcile(txctx, anchor.EntityID, currentStage, route.AdvancesTo, cause); err != nil {
			return err
		}
		if err := pc.applyWorkflowJoinIntents(txctx, anchor.EntityID, currentStage, route.AdvancesTo); err != nil {
			return err
		}
		if err := pc.applyWorkflowGateIntents(txctx, anchor.EntityID, currentStage, route.AdvancesTo, string(evt.Type())); err != nil {
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

func workflowGateOutcomeEvent(card decisioncard.Card, parent events.Event, route gateruntime.Route) (*events.Event, error) {
	if route.Emit.Empty() || strings.TrimSpace(route.Emit.Event) == "" {
		return nil, nil
	}
	payload, err := gateruntime.BuildRoutePayload(route, card.Fields)
	if err != nil {
		return nil, err
	}
	raw, err := canonicaljson.Encode(payload)
	if err != nil {
		return nil, err
	}
	eventType := strings.TrimSpace(route.Emit.Event)
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
	produced, err := newWorkflowChildEvent(uuid.NewSHA1(uuid.NameSpaceOID, []byte("swarm.gate.outcome.v1\x00"+identity)).String(), events.EventType(eventType), "", raw, parent.ChainDepth()+1, parent, envelope, createdAt.UTC())
	if err != nil {
		return nil, err
	}
	return &produced, nil
}

func newWorkflowChildEvent(id string, eventType events.EventType, taskID string, payload []byte, chainDepth int, parent events.Event, envelope events.EventEnvelope, createdAt time.Time) (events.Event, error) {
	return events.NewChildEvent(events.ChildEventInput{
		Facts: events.EventFacts{
			ID: id, Type: eventType,
			Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: runtimeWorkflowID},
			TaskID:   taskID, Payload: payload, ChainDepth: chainDepth,
			Envelope: envelope, CreatedAt: createdAt,
		},
		Lineage: events.LineageFromEvent(parent),
	})
}
