package bus

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/forkrecipient"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/correlation"
)

// This is a narrowed resolution input, not execution authority. The selected
// owner and final exact recipient guard remain required for publication.
type selectedForkRecipientPlan struct {
	eventID    string
	lineage    events.SelectedForkLineage
	recipients []forkrecipient.Evidence
	contexts   map[events.DeliveryRouteIdentity]events.DeliveryContext
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
	for _, route := range routes {
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

func restoreSelectedForkRecipientContexts(ctx context.Context, event events.Event, routes RoutePlan) (RoutePlan, error) {
	if err := ctx.Err(); err != nil {
		return RoutePlan{}, err
	}
	selected, ok := selectedForkRecipientPlanFromContext(ctx, event)
	if !ok || selected.contexts == nil {
		return routes, nil
	}
	routes = routes.Normalized()
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
	actual, err := forkrecipient.NewConnect(forkrecipient.Input{
		Recipient: typed, Path: recipient.Path(), AgentPlan: recipient.AgentPlan(),
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
