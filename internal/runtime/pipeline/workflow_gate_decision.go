package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/gateruntime"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimeworkflowlifecycle "github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	"github.com/google/uuid"
)

const (
	workflowGateDecisionEventType events.EventType = "mailbox.card_decided"
	decisionCardDeferredEventType events.EventType = "mailbox.card_deferred"
	decisionCardExpiredEventType  events.EventType = "mailbox.card_expired"
)

func (pc *PipelineCoordinator) handleWorkflowGateDecisionEvent(ctx context.Context, evt events.Event) ([]events.Event, runtimepipelineobligation.ExecutionOutcome, error) {
	if pc == nil || pc.decisionCards == nil || pc.workflowStore == nil {
		return nil, runtimepipelineobligation.Continue(), fmt.Errorf("gate decision runtime is not configured")
	}
	payload, err := canonicaljson.Decode(evt.Payload())
	if err != nil {
		return nil, runtimepipelineobligation.Continue(), fmt.Errorf("decode mailbox.card_decided payload: %w", err)
	}
	cardIDValue, ok := payload.Lookup("card_id")
	if !ok {
		return nil, runtimepipelineobligation.Continue(), fmt.Errorf("mailbox.card_decided card_id is required")
	}
	cardID, ok := cardIDValue.String()
	if !ok {
		return nil, runtimepipelineobligation.Continue(), fmt.Errorf("mailbox.card_decided card_id must be a string")
	}
	cardID = strings.TrimSpace(cardID)
	if cardID == "" {
		return nil, runtimepipelineobligation.Continue(), fmt.Errorf("mailbox.card_decided card_id is required")
	}
	card, err := pc.decisionCards.GetDecisionCard(ctx, cardID)
	if err != nil {
		return nil, runtimepipelineobligation.Continue(), err
	}
	if card.Status != decisioncard.StatusDecided || card.DecisionEventID != evt.ID() {
		return nil, runtimepipelineobligation.Continue(), fmt.Errorf("mailbox.card_decided does not match the authoritative card decision")
	}
	switch card.Anchor.Kind() {
	case decisioncard.AnchorKindStageGate:
		return pc.handleStageGateDecisionCard(ctx, evt, card)
	case decisioncard.AnchorKindHumanTask:
		emitted, err := pc.handleHumanTaskDecisionCard(ctx, evt, card)
		return emitted, runtimepipelineobligation.Continue(), err
	case decisioncard.AnchorKindProposedEffect:
		return pc.handleProposedEffectDecisionCard(ctx, evt, card)
	default:
		return nil, runtimepipelineobligation.Continue(), fmt.Errorf("decision-card anchor kind %q is not registered", card.Anchor.Kind())
	}
}

func (pc *PipelineCoordinator) handleProposedEffectDecisionCard(ctx context.Context, evt events.Event, card decisioncard.Card) ([]events.Event, runtimepipelineobligation.ExecutionOutcome, error) {
	store := pc.proposedEffects
	if store == nil {
		return nil, runtimepipelineobligation.Continue(), fmt.Errorf("proposed-effect continuation store is not configured")
	}
	continuation, err := store.LoadProposedEffectContinuation(ctx, card.CardID)
	if err != nil {
		return nil, runtimepipelineobligation.Continue(), err
	}
	if err := continuation.Validate(card); err != nil {
		return nil, runtimepipelineobligation.Continue(), err
	}
	if continuation.DecisionEventID != evt.ID() || continuation.Verdict != card.Verdict {
		return nil, runtimepipelineobligation.Continue(), fmt.Errorf("mailbox.card_decided does not match the authoritative proposed-effect continuation")
	}
	executionSource, err := card.Anchor.ExecutionRoutingSource()
	if err != nil {
		return nil, runtimepipelineobligation.Continue(), fmt.Errorf("load proposed-effect execution source: %w", err)
	}
	if current := workflowGateBundleHash(ctx, pc); current == "" || current != card.BundleHash {
		failure := runtimefailures.Normalize(runtimefailures.New(
			runtimefailures.ClassDependencyUnavailable,
			"decision_card_bundle_unavailable",
			runtimeWorkflowID,
			"route_proposed_effect_decision",
			map[string]any{"card_id": card.CardID, "required_bundle_hash": card.BundleHash, "current_bundle_hash": current},
		), runtimeWorkflowID, "route_proposed_effect_decision")
		return nil, runtimepipelineobligation.DeferExecution("decision_card_bundle_unavailable", time.Now().UTC().Add(runtimepipelineobligation.DecisionRouteRetryDelay), &failure), nil
	}
	if pc.workflowStore.decisionRoutes == nil {
		return nil, runtimepipelineobligation.Continue(), fmt.Errorf("proposed-effect route requires the selected-store route owner")
	}
	var intent runtimeengine.EmitIntent
	switch card.Verdict {
	case "approve":
		var activityIntent runtimeengine.ActivityIntent
		activityIntent, err = activityIntentFromProposedEffect(continuation, executionSource)
		if err == nil {
			intent, err = activityRequestEmitIntentFromAdmittedSource(activityIntent)
		}
	case "revise", "reject":
		var product events.Event
		product, err = proposedEffectOutcomeEvent(card, evt, continuation)
		intent = runtimeengine.EmitIntent{Event: product}
		if continuation.ReplyContextID != "" {
			intent.Context = events.DeliveryContext{Reply: &events.ReplyContextRef{ID: continuation.ReplyContextID}}
		}
	default:
		return nil, runtimepipelineobligation.Continue(), fmt.Errorf("proposed-effect verdict %q is unsupported", card.Verdict)
	}
	if err != nil {
		return nil, runtimepipelineobligation.Continue(), err
	}
	planner, ok := pc.bus.(EnginePublicationPlanner)
	if !ok {
		return nil, runtimepipelineobligation.Continue(), fmt.Errorf("proposed-effect route requires the publication planner")
	}
	plans, err := planner.PrepareEnginePublications(ctx, []runtimeengine.EmitIntent{intent})
	if err != nil {
		return nil, runtimepipelineobligation.Continue(), err
	}
	if len(plans) != 1 {
		releaseErr := planner.ReleaseEnginePublications(context.WithoutCancel(ctx), plans)
		return nil, runtimepipelineobligation.Continue(), errors.Join(fmt.Errorf("proposed-effect route planner returned %d plans", len(plans)), releaseErr)
	}
	committed, err := pc.workflowStore.decisionRoutes.CommitProposedEffectRoute(ctx, ProposedEffectRouteCommand{
		CardID: card.CardID, RouteEventID: evt.ID(), OccurredAt: card.DecidedAt, Publication: plans[0],
	})
	if err != nil {
		return nil, runtimepipelineobligation.Continue(), errors.Join(err, planner.ReleaseEnginePublications(context.WithoutCancel(ctx), plans))
	}
	if err := planner.FinalizeEnginePublications(ctx, []runtimeengine.CommittedDurablePublication{committed.Publication}); err != nil {
		return nil, runtimepipelineobligation.Continue(), err
	}
	dispatcher := pc.bus.EngineDispatcher()
	if dispatcher == nil {
		return nil, runtimepipelineobligation.Continue(), fmt.Errorf("proposed-effect route requires post-commit dispatcher")
	}
	if err := dispatcher.DispatchPostCommit(context.WithoutCancel(ctx), []runtimeengine.EmitIntent{intent}); err != nil {
		return nil, runtimepipelineobligation.Continue(), err
	}
	return nil, runtimepipelineobligation.Continue(), nil
}

func activityIntentFromProposedEffect(continuation decisioncard.ProposedEffectContinuation, source events.RoutingSource) (runtimeengine.ActivityIntent, error) {
	continuation = continuation.Canonical()
	owner, err := activityidentity.ParseOwnerKey(continuation.NodeID)
	if err != nil {
		return runtimeengine.ActivityIntent{}, fmt.Errorf("proposed-effect continuation owner identity: %w", err)
	}
	intent := runtimeengine.ActivityIntent{
		Context: events.DeliveryContext{}, ActivityID: continuation.ActivityID, Tool: continuation.Tool,
		BundleHash: continuation.BundleHash, WorkflowVersion: continuation.WorkflowVersion,
		Input: continuation.Input, EffectClass: continuation.EffectClass,
		SuccessEvent: continuation.SuccessEvent, FailureEvent: continuation.FailureEvent,
		RevisionEvent: continuation.RevisionEvent, RejectedEvent: continuation.RejectedEvent,
		RetryMaxAttempts: continuation.RetryMaxAttempts, RetryBackoff: continuation.RetryBackoff, ForkPolicy: continuation.ForkPolicy,
		EntityID: identity.NormalizeEntityID(continuation.EntityID), Owner: owner,
		ExecutionFlowID: identity.NormalizeFlowID(continuation.FlowID), FlowInstance: continuation.FlowInstance,
		RoutingSource:   source,
		HandlerEventKey: continuation.HandlerEventKey, SourceEventID: continuation.SourceEventID,
		SourceRunID: continuation.SourceRunID, SourceTaskID: continuation.SourceTaskID,
		ParentEventID: continuation.ParentEventID, ChainDepth: continuation.ChainDepth,
		Attempt: continuation.Attempt, Generation: continuation.Generation, LoopStage: continuation.LoopStage,
		ExecutionMode: continuation.ExecutionMode,
	}
	if continuation.ReplyContextID != "" {
		intent.Context = events.DeliveryContext{Reply: &events.ReplyContextRef{ID: continuation.ReplyContextID}}
	}
	return intent.Normalized(), nil
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
	source, err := card.Anchor.ExecutionRoutingSource()
	if err != nil {
		return noEvent, err
	}
	return newWorkflowChildEvent(eventID, events.EventType(eventType), continuation.SourceTaskID, raw, parent.ChainDepth()+1, source, parent, envelope, card.DecidedAt.UTC())
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
	store := pc.humanTasks
	if store == nil {
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
	if err := continuation.Validate(card); err != nil {
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
	product, err := newWorkflowChildEvent(productID, "human_task.deferred", "", payload, evt.ChainDepth()+1, anchor.Source, evt,
		humanTaskRequesterOutcomeEnvelope(continuation), evt.CreatedAt().UTC())
	if err != nil {
		return nil, err
	}
	return nil, pc.commitHumanTaskRoute(ctx, product, []string{anchor.RequesterAgentID}, continuation.ReplyContextID, card.CardID, evt.ID(), evt.CreatedAt(), false)
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
	store := pc.humanTasks
	if store == nil {
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
	if err := continuation.Validate(card); err != nil {
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
	product, err := newWorkflowChildEvent(productID, "human_task.expired", "", payload, evt.ChainDepth()+1, anchor.Source, evt,
		humanTaskRequesterOutcomeEnvelope(continuation), card.DecidedAt.UTC())
	if err != nil {
		return nil, err
	}
	return nil, pc.commitHumanTaskRoute(ctx, product, []string{anchor.RequesterAgentID}, continuation.ReplyContextID, card.CardID, evt.ID(), card.DecidedAt, true)
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

func (pc *PipelineCoordinator) handleStageGateDecisionCard(ctx context.Context, evt events.Event, card decisioncard.Card) ([]events.Event, runtimepipelineobligation.ExecutionOutcome, error) {
	if current := workflowGateBundleHash(ctx, pc); current == "" || current != card.BundleHash {
		failure := runtimefailures.Normalize(runtimefailures.New(
			runtimefailures.ClassDependencyUnavailable,
			"decision_card_bundle_unavailable",
			runtimeWorkflowID,
			"route_gate_decision",
			map[string]any{"card_id": card.CardID, "required_bundle_hash": card.BundleHash, "current_bundle_hash": current},
		), runtimeWorkflowID, "route_gate_decision")
		return nil, runtimepipelineobligation.DeferExecution("decision_card_bundle_unavailable", time.Now().UTC().Add(runtimepipelineobligation.DecisionRouteRetryDelay), &failure), nil
	}
	route, err := pc.loadStageGateRoute(ctx, card)
	if err != nil {
		return nil, runtimepipelineobligation.Continue(), err
	}
	emitted, err := workflowGateOutcomeEvent(card, evt, route)
	if err != nil {
		return nil, runtimepipelineobligation.Continue(), err
	}
	if err := pc.routeWorkflowGateDecision(ctx, card, evt, route, emitted); err != nil {
		return nil, runtimepipelineobligation.Continue(), err
	}
	return nil, runtimepipelineobligation.Continue(), nil
}

func (pc *PipelineCoordinator) loadStageGateRoute(ctx context.Context, card decisioncard.Card) (gateruntime.Route, error) {
	anchor, err := card.Anchor.StageGate()
	if err != nil {
		return gateruntime.Route{}, err
	}
	instance, found, err := pc.workflowStore.Load(ctx, anchor.Route)
	if err != nil {
		return gateruntime.Route{}, err
	}
	if !found {
		return gateruntime.Route{}, fmt.Errorf("decision card workflow instance is missing")
	}
	carrier, err := workflowInstanceStateCarrier(instance)
	if err != nil {
		return gateruntime.Route{}, err
	}
	activation, found, err := gateruntime.Load(carrier.StateBuckets, anchor.FlowID, card.Snapshot.Decision)
	if err != nil {
		return gateruntime.Route{}, err
	}
	if !found {
		return gateruntime.Route{}, fmt.Errorf("decision card activation is no longer authoritative")
	}
	if err := validateStageGateInstanceOwner(anchor, instance, activation); err != nil {
		return gateruntime.Route{}, err
	}
	if activation.ActivationID != anchor.StageActivationID || activation.CardID != card.CardID {
		return gateruntime.Route{}, fmt.Errorf("decision card activation is no longer authoritative")
	}
	return gateruntime.RouteFor(activation.RoutesJSON, card.Verdict)
}

func validateStageGateInstanceOwner(anchor decisioncard.StageGateAnchor, instance WorkflowInstance, activation gateruntime.Activation) error {
	if strings.TrimSpace(anchor.FlowID) != strings.TrimSpace(activation.FlowID) {
		return fmt.Errorf("stage_gate anchor does not match its authoritative workflow instance")
	}
	if _, err := requireWorkflowInstanceIdentity(anchor.Route, identity.NormalizeEntityID(anchor.EntityID), instance); err != nil {
		return fmt.Errorf("stage_gate anchor does not match its authoritative workflow instance: %w", err)
	}
	return nil
}

func (pc *PipelineCoordinator) handleHumanTaskDecisionCard(ctx context.Context, evt events.Event, card decisioncard.Card) ([]events.Event, error) {
	store := pc.humanTasks
	if store == nil {
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
	continuation, err := store.LoadHumanTaskContinuation(ctx, card.CardID)
	if err != nil {
		return nil, err
	}
	productEventID := decisioncard.HumanTaskOutcomeEventID(card.CardID, evt.ID())
	product, err := newWorkflowChildEvent(productEventID, eventType, "", payload, evt.ChainDepth()+1, anchor.Source, evt,
		humanTaskRequesterOutcomeEnvelope(continuation), card.DecidedAt.UTC())
	if err != nil {
		return nil, err
	}
	return nil, pc.commitHumanTaskRoute(ctx, product, []string{anchor.RequesterAgentID}, continuation.ReplyContextID, card.CardID, evt.ID(), card.DecidedAt, true)
}

func (pc *PipelineCoordinator) commitHumanTaskRoute(
	ctx context.Context,
	product events.Event,
	recipients []string,
	replyContextID string,
	cardID string,
	routeEventID string,
	occurredAt time.Time,
	completeOutcome bool,
) error {
	if pc == nil || pc.workflowStore == nil || pc.workflowStore.decisionRoutes == nil {
		return fmt.Errorf("human-task route requires the selected-store decision route owner")
	}
	planner, ok := pc.bus.(EnginePublicationPlanner)
	if !ok {
		return fmt.Errorf("human-task route requires the publication planner")
	}
	intent := runtimeengine.EmitIntent{Event: product, Recipients: append([]string(nil), recipients...)}
	if strings.TrimSpace(replyContextID) != "" {
		intent.Context = events.DeliveryContext{Reply: &events.ReplyContextRef{ID: strings.TrimSpace(replyContextID)}}
	}
	plans, err := planner.PrepareEnginePublications(ctx, []runtimeengine.EmitIntent{intent})
	if err != nil {
		return err
	}
	if len(plans) != 1 {
		releaseErr := planner.ReleaseEnginePublications(context.WithoutCancel(ctx), plans)
		return errors.Join(fmt.Errorf("human-task route planner returned %d plans", len(plans)), releaseErr)
	}
	var committed CommittedHumanTaskRoute
	if completeOutcome {
		committed, err = pc.workflowStore.decisionRoutes.CommitHumanTaskOutcomeRoute(ctx, HumanTaskOutcomeRouteCommand{
			CardID: cardID, RouteEventID: routeEventID, OccurredAt: occurredAt, Publication: plans[0],
		})
	} else {
		committed, err = pc.workflowStore.decisionRoutes.CommitHumanTaskDeferredRoute(ctx, HumanTaskDeferredRouteCommand{
			CardID: cardID, RouteEventID: routeEventID, OccurredAt: occurredAt, Publication: plans[0],
		})
	}
	if err != nil {
		return errors.Join(err, planner.ReleaseEnginePublications(context.WithoutCancel(ctx), plans))
	}
	if err := planner.FinalizeEnginePublications(ctx, []runtimeengine.CommittedDurablePublication{committed.Publication}); err != nil {
		return err
	}
	dispatcher := pc.bus.EngineDispatcher()
	if dispatcher == nil {
		return fmt.Errorf("human-task route requires post-commit dispatcher")
	}
	return dispatcher.DispatchPostCommit(context.WithoutCancel(ctx), []runtimeengine.EmitIntent{intent})
}

func (pc *PipelineCoordinator) routeWorkflowGateDecision(ctx context.Context, card decisioncard.Card, evt events.Event, route gateruntime.Route, emitted *events.Event) error {
	anchor, err := card.Anchor.StageGate()
	if err != nil {
		return err
	}
	if pc.workflowStore == nil || pc.workflowStore.engineMutations == nil {
		return fmt.Errorf("gate route requires the selected workflow engine mutation owner")
	}
	instanceRoute := anchor.Route
	instance, found, err := pc.workflowStore.Load(ctx, instanceRoute)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("decision card workflow instance is missing")
	}
	currentStage := strings.TrimSpace(instance.CurrentState)
	carrier, err := workflowInstanceStateCarrier(instance)
	if err != nil {
		return err
	}
	activation, found, err := gateruntime.Load(carrier.StateBuckets, anchor.FlowID, card.Snapshot.Decision)
	if err != nil {
		return err
	}
	nextStage := strings.TrimSpace(route.AdvancesTo)
	if found && activation.ActivationID == anchor.StageActivationID && activation.CardID == card.CardID && activation.Status == gateruntime.StatusRouted && activation.DecisionEventID == evt.ID() {
		if currentStage != nextStage {
			return fmt.Errorf("routed decision card state does not match its frozen outcome")
		}
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
	address := runtimeengine.StateAddress{
		FlowID: identity.NormalizeFlowID(anchor.FlowID), Route: instanceRoute,
		EntityID: identity.NormalizeEntityID(anchor.EntityID),
	}
	preparedState, err := (pipelineEngineStateRepo{coordinator: pc}).prepareMutation(ctx, address, runtimeengine.StateMutation{
		NextState: nextStage, TriggerEventID: evt.ID(), TriggerEventType: string(evt.Type()),
		TriggeredAt: evt.CreatedAt(), StateCarrier: carrier,
	})
	if err != nil {
		return err
	}
	effect, err := (pipelineWorkflowLifecycleOwner{coordinator: pc}).AcceptedEventEffect(instanceRoute, address.EntityID, evt, currentStage, nextStage)
	if err != nil {
		return err
	}
	lifecycle, err := pc.prepareWorkflowLifecycleMutation(ctx, &preparedState.instance, []runtimeworkflowlifecycle.Effect{effect}, true)
	if err != nil {
		return err
	}
	state, err := preparedState.record()
	if err != nil {
		return err
	}
	var intents []runtimeengine.EmitIntent
	var publications []runtimeengine.DurablePublicationPlan
	if emitted != nil {
		intents = []runtimeengine.EmitIntent{{Event: *emitted}}
		planner, ok := pc.bus.(EnginePublicationPlanner)
		if !ok {
			return fmt.Errorf("gate route requires the publication planner")
		}
		publications, err = planner.PrepareEnginePublications(ctx, intents)
		if err != nil {
			return err
		}
		if len(publications) != 1 {
			releaseErr := planner.ReleaseEnginePublications(context.WithoutCancel(ctx), publications)
			return errors.Join(fmt.Errorf("gate route planner returned %d plans", len(publications)), releaseErr)
		}
	}
	committed, err := pc.workflowStore.engineMutations.CommitWorkflowEngineMutation(ctx, WorkflowEngineMutationCommand{
		State: state, GateRouteAdmissionRunID: card.RunID,
		Lifecycle: lifecycle.Commit, Publications: publications,
	})
	if err != nil {
		if planner, ok := pc.bus.(EnginePublicationPlanner); ok {
			err = errors.Join(err, planner.ReleaseEnginePublications(context.WithoutCancel(ctx), publications))
		}
		return err
	}
	if planner, ok := pc.bus.(EnginePublicationPlanner); ok {
		if err := planner.FinalizeEnginePublications(ctx, committed.Publications); err != nil {
			return err
		}
	}
	if err := pc.finalizeWorkflowLifecycleMutation(ctx, committed.Lifecycle); err != nil {
		return err
	}
	if len(intents) > 0 {
		dispatcher := pc.bus.EngineDispatcher()
		if dispatcher == nil {
			return fmt.Errorf("gate route requires the post-commit dispatcher")
		}
		if err := dispatcher.DispatchPostCommit(context.WithoutCancel(ctx), intents); err != nil {
			return err
		}
	}
	pc.notifyTestEntityStateUpdated(anchor.EntityID, nextStage)
	return nil
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
	envelope := events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, anchor.EntityID), anchor.Route.InstancePath)
	identity := strings.Join([]string{card.CardID, card.DecisionEventID, card.Verdict, eventType}, "\x00")
	createdAt := card.DecidedAt
	if createdAt.IsZero() {
		createdAt = parent.CreatedAt()
	}
	produced, err := newWorkflowChildEvent(uuid.NewSHA1(uuid.NameSpaceOID, []byte("swarm.gate.outcome.v1\x00"+identity)).String(), events.EventType(eventType), "", raw, parent.ChainDepth()+1, anchor.Source, parent, envelope, createdAt.UTC())
	if err != nil {
		return nil, err
	}
	return &produced, nil
}

func newWorkflowChildEvent(id string, eventType events.EventType, taskID string, payload []byte, chainDepth int, source events.RoutingSource, parent events.Event, envelope events.EventEnvelope, createdAt time.Time) (events.Event, error) {
	return events.NewChildEvent(events.ChildEventInput{
		Facts: events.EventFacts{
			ID: id, Type: eventType,
			Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: runtimeWorkflowID},
			TaskID:   taskID, Payload: payload, ChainDepth: chainDepth, RoutingSource: source,
			Envelope: envelope, CreatedAt: createdAt,
		},
		Lineage: events.LineageFromEvent(parent),
	})
}
