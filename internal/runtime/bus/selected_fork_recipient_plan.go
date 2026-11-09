package bus

import (
	"context"
	"fmt"
	"reflect"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/forkrecipient"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
)

// This is a narrowed resolution input, not execution authority. The selected
// owner and final exact recipient guard remain required for publication.
type selectedForkRecipientPlan struct {
	eventID       string
	lineage       events.SelectedForkLineage
	recipients    []forkrecipient.Evidence
	contexts      map[events.DeliveryRouteIdentity]events.DeliveryContext
	contextRoutes []events.DeliveryRoute
	requests      []replycontext.Record
	returnReply   *replycontext.Record
	returnRoutes  []events.DeliveryRoute
}

type selectedForkRecipientPlanKey struct{}

func WithSelectedForkRecipientPlan(ctx context.Context, event events.Event, recipients []forkrecipient.Evidence) (context.Context, error) {
	lineage, ok := event.SelectedForkLineage()
	if !ok || event.AdmissionClass() != events.EventAdmissionSelectedForkReplay || event.ID() == "" {
		return nil, fmt.Errorf("selected recipient plan requires its exact child publication")
	}
	canonical, err := forkrecipient.CanonicalSet(recipients)
	if err != nil || len(canonical) == 0 {
		return nil, fmt.Errorf("selected recipient plan requires its complete unfinished partition: %v", err)
	}
	return context.WithValue(ctx, selectedForkRecipientPlanKey{}, selectedForkRecipientPlan{
		eventID: event.ID(), lineage: lineage, recipients: canonical,
	}), nil
}

func selectedForkRecipientPlanFromContext(ctx context.Context, event events.Event) (selectedForkRecipientPlan, bool) {
	plan, ok := ctx.Value(selectedForkRecipientPlanKey{}).(selectedForkRecipientPlan)
	lineage, selected := event.SelectedForkLineage()
	return plan, ok && selected && event.ID() == plan.eventID && lineage == plan.lineage
}

func WithSelectedForkRequestReplies(ctx context.Context, event events.Event, records []replycontext.Record) (context.Context, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	plan, ok := selectedForkRecipientPlanFromContext(ctx, event)
	if !ok || len(records) == 0 || plan.requests != nil || plan.returnReply != nil {
		return nil, fmt.Errorf("inherited request requires its exact unfinished publication scope")
	}
	seen := make(map[string]bool, len(records))
	for _, record := range records {
		if err := record.Validate(); err != nil {
			return nil, err
		}
		if record.RunID != event.RunID() || record.State != replycontext.StateOpen || seen[record.ID] {
			return nil, fmt.Errorf("inherited request has contradictory retained reply evidence")
		}
		seen[record.ID] = true
		plan.requests = append(plan.requests, record.Normalized())
	}
	return context.WithValue(ctx, selectedForkRecipientPlanKey{}, plan), nil
}

func selectedForkRequestReply(ctx context.Context, event events.Event, draft replycontext.Record) (replycontext.Record, bool, error) {
	plan, selected := selectedForkRecipientPlanFromContext(ctx, event)
	if !selected || plan.requests == nil {
		return replycontext.Record{}, false, nil
	}
	if err := ctx.Err(); err != nil {
		return replycontext.Record{}, false, err
	}
	var matched replycontext.Record
	for _, record := range plan.requests {
		if record.RequesterFlowID != draft.RequesterFlowID || record.RequestOutputPin != draft.RequestOutputPin || record.ProviderFlowID != draft.ProviderFlowID {
			continue
		}
		if matched.ID != "" {
			return replycontext.Record{}, false, fmt.Errorf("inherited request has ambiguous retained reply bindings")
		}
		if err := record.ValidateInheritedRequestPublication(draft); err != nil {
			return replycontext.Record{}, false, err
		}
		matched = record.Normalized()
	}
	if matched.ID == "" {
		return replycontext.Record{}, false, fmt.Errorf("inherited request lacks its retained paired binding")
	}
	return matched, true, nil
}

func WithSelectedForkReplyReturn(ctx context.Context, event events.Event, record replycontext.Record) (context.Context, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	plan, ok := selectedForkRecipientPlanFromContext(ctx, event)
	if !ok || plan.returnReply != nil || len(plan.requests) != 0 {
		return nil, fmt.Errorf("retained return requires its exact unfinished publication scope")
	}
	if err := record.ValidateContinuationFrom(record, record.CreatedAt); err != nil {
		return nil, err
	}
	if record.RunID != event.RunID() || record.State != replycontext.StateTerminal {
		return nil, fmt.Errorf("retained return requires its own terminal child context")
	}
	record = record.Normalized()
	plan.returnReply = &record
	ctx = context.WithValue(ctx, selectedForkRecipientPlanKey{}, plan)
	return events.WithDeliveryContext(ctx, events.DeliveryContext{Reply: &events.ReplyContextRef{ID: record.ID}}), nil
}

func selectedForkReplyReturn(ctx context.Context, event events.Event, record replycontext.Record) (bool, error) {
	plan, selected := selectedForkRecipientPlanFromContext(ctx, event)
	if !selected || plan.returnReply == nil {
		return false, nil
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if record.State != replycontext.StateTerminal {
		return false, fmt.Errorf("retained return lost its terminal child context")
	}
	if err := record.ValidateContinuationFrom(*plan.returnReply, plan.returnReply.CreatedAt); err != nil {
		return false, err
	}
	return true, nil
}

// Captured context is data, not recipient or execution authority. Resolution
// must produce the same exact routes before this owner can restore their context.
func WithSelectedForkRecipientContexts(ctx context.Context, event events.Event, routes []events.DeliveryRoute) (context.Context, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	plan, ok := selectedForkRecipientPlanFromContext(ctx, event)
	if !ok || len(routes) == 0 {
		return nil, fmt.Errorf("selected receiver context requires its exact unfinished publication")
	}
	if plan.contexts != nil {
		return nil, fmt.Errorf("selected receiver context is already bound")
	}
	if err := events.ValidateDeliveryRoutes(routes); err != nil {
		return nil, err
	}
	plan.contexts = make(map[events.DeliveryRouteIdentity]events.DeliveryContext, len(routes))
	plan.contextRoutes = append([]events.DeliveryRoute(nil), routes...)
	if plan.returnReply != nil {
		plan.returnRoutes = append([]events.DeliveryRoute(nil), routes...)
	}
	for index, route := range routes {
		plan.contextRoutes[index].Context = route.Context.Normalized()
		key, err := selectedForkContextRouteIdentity(route)
		if err != nil {
			return nil, err
		}
		if _, duplicate := plan.contexts[key]; duplicate {
			return nil, fmt.Errorf("selected receiver context has contradictory route bindings")
		}
		plan.contexts[key] = route.Context.Normalized()
	}
	return context.WithValue(ctx, selectedForkRecipientPlanKey{}, plan), nil
}

func selectedForkContextRouteIdentity(route events.DeliveryRoute) (events.DeliveryRouteIdentity, error) {
	route.Context = events.DeliveryContext{}
	return route.Identity()
}

// An inherited recipient's projected execution life is part of its full claim,
// so it must be available before hashing. Accept only an exact compiled claim
// and context; the later route/partition checks remain independent admission.
func selectedForkExecutionLife(ctx context.Context, event events.Event, compiled pinrouting.ConnectRoutePlan, route pinrouting.ConnectDeliveryRoute) (*events.InstanceLife, error) {
	selected, found := selectedForkRecipientPlanFromContext(ctx, event)
	if !found || selected.contexts == nil {
		return route.Context.Life, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var matched *events.InstanceLife
	found = false
	for _, retained := range selected.contextRoutes {
		expected := retained.ConnectClaim
		if selected.requests != nil {
			var continuation *pinrouting.ReplyRequestContinuation
			for _, record := range selected.requests {
				if retained.Context.ReplyContextID() == record.ID && compiled.MatchesReplyRecord(record) {
					if continuation != nil {
						return nil, fmt.Errorf("selected request has ambiguous compiled reply bindings")
					}
					value := pinrouting.ReplyRequestContinuation{Record: record, Plan: compiled}
					continuation = &value
				}
			}
			if continuation == nil {
				continue
			}
			var err error
			expected, err = continuation.ClaimForDelivery(retained)
			if err != nil {
				return nil, err
			}
		}
		candidate := route
		candidate.Context.Life = retained.Context.Life
		claim, err := pinrouting.ConnectExecutionClaim(compiled, candidate)
		if err != nil {
			return nil, err
		}
		if !claim.Equal(expected) || !reflect.DeepEqual(candidate.Context.Normalized(), retained.Context.Normalized()) {
			continue
		}
		if found {
			return nil, fmt.Errorf("selected recipient has ambiguous exact execution-life claims")
		}
		matched, found = retained.Context.Life, true
	}
	if !found {
		return nil, fmt.Errorf("selected recipient execution life lacks its exact compiled claim")
	}
	return matched, nil
}

func restoreSelectedForkRecipientContexts(ctx context.Context, event events.Event, routes RoutePlan) (RoutePlan, error) {
	if err := ctx.Err(); err != nil {
		return RoutePlan{}, err
	}
	selected, ok := selectedForkRecipientPlanFromContext(ctx, event)
	if !ok || selected.contexts == nil {
		return routes, nil
	}
	routes = routes.Normalized()
	if selected.returnReply != nil {
		contexts, err := selectedForkReturnContexts(selected, routes.ReplyReturns)
		if err != nil {
			return RoutePlan{}, err
		}
		selected.contexts = contexts
	} else if selected.requests != nil {
		contexts, err := selectedForkRequestContexts(selected, routes.ReplyContinuations)
		if err != nil {
			return RoutePlan{}, err
		}
		selected.contexts = contexts
	}
	seen := make(map[events.DeliveryRouteIdentity]bool, len(selected.contexts))
	for i, intent := range routes.DeliveryIntents {
		if !intent.Persist {
			continue
		}
		key, err := selectedForkContextRouteIdentity(intent.deliveryRoute())
		if err != nil {
			return RoutePlan{}, err
		}
		retained, found := selected.contexts[key]
		if !found {
			return RoutePlan{}, fmt.Errorf("selected receiver context does not match the resolved route")
		}
		seen[key] = true
		routes.DeliveryIntents[i].Context = retained.Normalized()
	}
	if len(seen) != len(selected.contexts) {
		return RoutePlan{}, fmt.Errorf("selected receiver context is missing a resolved recipient")
	}
	return routes.Normalized(), nil
}

func selectedForkRequestContexts(selected selectedForkRecipientPlan, continuations []pinrouting.ReplyRequestContinuation) (map[events.DeliveryRouteIdentity]events.DeliveryContext, error) {
	contexts := make(map[events.DeliveryRouteIdentity]events.DeliveryContext, len(selected.contextRoutes))
	for _, route := range selected.contextRoutes {
		matched := false
		for _, continuation := range continuations {
			if route.Context.ReplyContextID() != continuation.Record.ID {
				continue
			}
			if matched {
				return nil, fmt.Errorf("retained request context has ambiguous compiled bindings")
			}
			claim, err := continuation.ClaimForDelivery(route)
			if err != nil {
				return nil, err
			}
			route.ConnectClaim, matched = claim, true
		}
		key, err := selectedForkContextRouteIdentity(route)
		if err != nil {
			return nil, err
		}
		if _, duplicate := contexts[key]; duplicate {
			return nil, fmt.Errorf("retained request has duplicate projected recipient claims")
		}
		contexts[key] = route.Context.Normalized()
	}
	return contexts, nil
}

func selectedForkReturnContexts(selected selectedForkRecipientPlan, returns []pinrouting.ReplyReturnContinuation) (map[events.DeliveryRouteIdentity]events.DeliveryContext, error) {
	if len(returns) != 1 || returns[0].Validate() != nil || returns[0].Record.ValidateContinuationFrom(*selected.returnReply, selected.returnReply.CreatedAt) != nil {
		return nil, fmt.Errorf("retained return context restoration lacks its exact response plan")
	}
	contexts := make(map[events.DeliveryRouteIdentity]events.DeliveryContext, len(selected.returnRoutes))
	for _, route := range selected.returnRoutes {
		if returns[0].MatchesResponseRecipient(route) {
			claim, err := returns[0].ClaimForDelivery(route)
			if err != nil {
				return nil, err
			}
			route.ConnectClaim = claim
		} else if claim, matched, err := returns[0].OrdinaryClaimForDelivery(route); err != nil {
			return nil, err
		} else if matched {
			route.ConnectClaim = claim
		} else if !route.ConnectClaim.Empty() {
			return nil, fmt.Errorf("retained ordinary context lacks its compiled connection")
		}
		key, err := selectedForkContextRouteIdentity(route)
		if err != nil {
			return nil, err
		}
		if _, duplicate := contexts[key]; duplicate {
			return nil, fmt.Errorf("retained return has duplicate projected recipient claims")
		}
		contexts[key] = route.Context.Normalized()
	}
	return contexts, nil
}

func (p selectedForkRecipientPlan) allows(actual forkrecipient.Evidence) bool {
	for _, expected := range p.recipients {
		if equal, err := forkrecipient.Equal(expected, actual); err == nil && equal {
			return true
		}
	}
	return false
}

func (p selectedForkRecipientPlan) allowsSubscriber(subscriber Subscriber, event events.Event) bool {
	if subscriber.Recipient.IsNode() && subscriber.handlerNode.FlowPath() == "." {
		subscriber.Path = "."
	}
	actual, err := subscriber.SelectedRecipient(event.Type())
	return err == nil && p.allows(actual)
}

func (p selectedForkRecipientPlan) allowsConnectPlan(plan pinrouting.ConnectRoutePlan) bool {
	identity, err := pinrouting.ConnectPlanIdentity(plan)
	if err != nil {
		return false
	}
	for _, recipient := range p.recipients {
		connection, receiver, ok := recipient.Connect()
		if ok && connection == identity && receiver == plan.ReceiverPinIdentity().EvidenceIdentity() {
			return true
		}
	}
	return false
}

func selectedForkConnectPlans(ctx context.Context, event events.Event, plans []pinrouting.ConnectRoutePlan) []pinrouting.ConnectRoutePlan {
	selected, ok := selectedForkRecipientPlanFromContext(ctx, event)
	if !ok {
		return plans
	}
	included := make([]pinrouting.ConnectRoutePlan, 0, len(plans))
	for _, plan := range plans {
		if selected.allowsConnectPlan(plan) {
			included = append(included, plan)
		}
	}
	return included
}

func (p selectedForkRecipientPlan) allowsRegistration(plan pinrouting.ConnectRoutePlan, registration pinrouting.ConnectRecipientRegistration) bool {
	identity, err := pinrouting.ConnectPlanIdentity(plan)
	if err != nil {
		return false
	}
	recipient, projected := registration.RecipientForPlan(plan)
	if !projected {
		return false
	}
	node := recipient.Handler().Node()
	var typed events.DeliveryRecipient
	if recipient.Kind() == pinrouting.ConnectRecipientAgent {
		typed = events.MustAgentDeliveryRecipient(recipient.ID())
	} else {
		typed = events.MustNodeDeliveryRecipient(node)
	}
	path := recipient.Path()
	if typed.IsNode() && node.FlowPath() == "." && path == p.lineage.DestinationRunID() {
		path = "."
	}
	actual, err := forkrecipient.NewConnect(forkrecipient.Input{
		Recipient: typed, Path: path, AgentPlan: recipient.AgentPlan(),
		HandlerNode: node, HandlerEvent: recipient.HandlerEvent(),
	}, identity, registration.ReceiverIdentity())
	return err == nil && p.allows(actual)
}

func selectedForkRecipientPlanForInbound(ctx context.Context) (selectedForkRecipientPlan, bool) {
	event, ok := correlation.InboundEventFromContext(ctx)
	if !ok {
		return selectedForkRecipientPlan{}, false
	}
	return selectedForkRecipientPlanFromContext(ctx, event)
}
