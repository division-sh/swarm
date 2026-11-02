package bus

import (
	"context"
	"fmt"
	"slices"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

type PlatformLiveRunUnavailableError struct {
	FlowPath string
	Event    events.EventType
}

func (e *PlatformLiveRunUnavailableError) Error() string {
	return fmt.Sprintf("platform event %s refused: flow %s has no live run", e.Event, e.FlowPath)
}

func (eb *EventBus) planPlatformLivePublication(ctx context.Context, event events.Event, diagnostic bool, planner deliveryPlanner) (RoutePlan, error) {
	if eb.routeTable == nil {
		return RoutePlan{}, fmt.Errorf("platform publication requires its compiled subscriptions")
	}
	fact, admitted := correlation.SourceArtifactFactFromContext(ctx)
	if !admitted {
		return RoutePlan{}, fmt.Errorf("platform publication requires its admitted source")
	}
	keys := routedEventKeysForPlan(event)
	flows := make(map[string]bool)
	for _, flow := range eb.semanticSource.FlowScopes() {
		flowKeys := append(append([]string(nil), keys...), flow.Path+"/"+string(event.Type()))
		subscribers, err := eb.routeTable.PubsubDeclarationDefinitions(flow.ID, flowKeys)
		if err != nil {
			return RoutePlan{}, err
		}
		if len(subscribers) != 0 {
			flows[flow.ID] = true
		}
	}
	for _, plan := range planner.connectPlanner.matchedPlans(ctx, event) {
		flows[plan.ReceiverEndpoint().Readback().FlowID] = true
	}
	ordered := make([]string, 0, len(flows))
	for flow := range flows {
		ordered = append(ordered, flow)
	}
	slices.Sort(ordered)
	if len(ordered) != 0 && eb.durable.LandingRuns == nil {
		return RoutePlan{}, fmt.Errorf("platform publication requires its native landing-run reader")
	}
	var internal []deliveryRecipientCandidate
	if planner.routeResolver.resolveSubscribedRecipients != nil {
		for _, key := range keys {
			for _, candidate := range planner.routeResolver.resolveSubscribedRecipients(key) {
				if !candidate.PersistAsDelivery {
					internal = append(internal, candidate)
				}
			}
		}
	}
	combined := routePlanFromManifest(event, deliveryRecipientManifest{LiveRecipients: internal})
	var unavailable []string
	for _, flow := range ordered {
		plan, missing, err := eb.planPlatformSubscriber(ctx, event, flow, fact, planner)
		if err != nil {
			return RoutePlan{}, err
		}
		if missing != "" {
			unavailable = append(unavailable, missing)
			continue
		}
		combined.AuthorityOwner = plan.AuthorityOwner
		combined.AddLiveRecipients(plan.LiveRecipients...)
		combined.AddDeliveryIntents(plan.DeliveryIntents...)
		combined.RoutedRecipients = append(combined.RoutedRecipients, plan.RoutedRecipients...)
		combined.SubscribedRecipients = append(combined.SubscribedRecipients, plan.SubscribedRecipients...)
		combined.ActivationPlans = append(combined.ActivationPlans, plan.ActivationPlans...)
		combined.ReplyCreations = append(combined.ReplyCreations, plan.ReplyCreations...)
		combined.ReplyContinuations = append(combined.ReplyContinuations, plan.ReplyContinuations...)
		combined.ReplyReturns = append(combined.ReplyReturns, plan.ReplyReturns...)
		combined.ReplyClaims = append(combined.ReplyClaims, plan.ReplyClaims...)
		combined.JoinAdmissionFences = append(combined.JoinAdmissionFences, plan.JoinAdmissionFences...)
		if err := mergeConnectEvaluation(&combined.ConnectEvaluation, plan.ConnectEvaluation); err != nil {
			return RoutePlan{}, err
		}
	}
	if len(unavailable) != 0 {
		combined.TargetFailure = pinrouting.FailureTargetUnreachableTerminated
		combined.ExtraDetail = map[string]any{"unavailable_flow_paths": unavailable}
	}
	combined = combined.Normalized()
	if diagnostic {
		eb.recordPublishDiagnostic(ctx, event, combined)
	}
	return combined, combined.ValidatePersistentDeliveries()
}

func (eb *EventBus) planPlatformSubscriber(ctx context.Context, event events.Event, flow string, fact correlation.SourceArtifactFact, planner deliveryPlanner) (RoutePlan, string, error) {
	scope := runlifecycle.LiveRunScope{FlowPath: flowidentity.ScopeKey(eb.semanticSource, flow)}
	landing, found, err := eb.durable.LandingRuns.LoadLandingRun(ctx, scope)
	if err != nil {
		return RoutePlan{}, "", err
	}
	if !found || (landing.State != runlifecycle.StateLive && landing.State != runlifecycle.StatePaused) {
		return RoutePlan{}, scope.FlowPath, nil
	}
	live := landing.LiveRun
	if live.Scope != scope || !live.Source.Matches(fact) {
		return RoutePlan{}, "", runlifecycle.ErrLiveRunChanged
	}
	view, err := events.NewDeliveryEvent(event, events.DeliveryRoute{Context: events.DeliveryContext{RunID: live.RunID}})
	if err != nil {
		return RoutePlan{}, "", err
	}
	bounded := planner
	bounded.connectPlanner.publicationFlow = flow
	bounded.routeResolver.resolveRoutedSubscribers = bounded.connectPlanner.resolvePubsubSubscribers
	bounded.preparedOwners = &publicationTargetOwnerEvidence{event: view.Event(), planner: bounded}
	plan, err := eb.planSubscribedRoutePlanWithPlanner(correlation.WithRunID(ctx, live.RunID), view.Event(), false, bounded)
	if err != nil {
		return RoutePlan{}, "", err
	}
	if !plan.TargetFailure.Empty() {
		return RoutePlan{}, "", (PublishRecipientPlan{TargetFailure: plan.TargetFailure.Code(), targetFailureCause: plan.unavailableReceiverError}).TargetFailureError("eventbus", "platform_publish", map[string]any{"flow_path": scope.FlowPath})
	}
	for index := range plan.DeliveryIntents {
		plan.DeliveryIntents[index].Context.RunID = live.RunID
	}
	return plan, "", nil
}
