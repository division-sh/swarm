package bus

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type selectedRunTargetOwnerProjection struct {
	agents           map[agentidentity.Identity]ActiveAgentDescriptor
	agentsAvailable  bool
	descriptors      []ActiveTargetDescriptor
	targetsAvailable bool
	structural       events.DeliveryTargetOwnership
	source           semanticview.Source
	required         bool
}

func (p selectedRunTargetOwnerProjection) resolveRoutePlan(plan RoutePlan) (RoutePlan, error) {
	plan = plan.Normalized()
	for index := range plan.DeliveryIntents {
		intent := &plan.DeliveryIntents[index]
		if intent.Recipient.IsAgent() {
			if intent.TargetBlueprint.Empty() {
				if err := intent.AgentIdentity.Validate(); err != nil {
					return RoutePlan{}, fmt.Errorf("validate agent delivery owner for %s: %w", intent.Recipient.ID(), err)
				}
				continue
			}
			owner, err := p.resolveSelectedRoute(intent.TargetBlueprint, intent.AllowStructuralOwner)
			if err != nil {
				return RoutePlan{}, fmt.Errorf("resolve delivery target for %s: %w", intent.Recipient.ID(), err)
			}
			intent.TargetOwnership = owner
			continue
		}
		handler, err := deliveryIntentTargetHandler(*intent, plan.RoutedRecipients, plan.Event.Type())
		if err != nil {
			return RoutePlan{}, fmt.Errorf("resolve delivery target handler for %s: %w", intent.Recipient.ID(), err)
		}
		intent.Handler = handler
		owner, err := runtimepipeline.ClassifyDeliveryTargetOwnership(runtimepipeline.DeliveryTargetOwnershipRequest{
			Source: p.source, Event: plan.Event, Recipient: intent.Recipient, Blueprint: intent.TargetBlueprint,
			Handler: handler, Candidates: p.targetOwnerCandidates(), StructuralOwner: p.structural,
			AllowStructuralOwner: intent.AllowStructuralOwner,
		})
		if err != nil {
			return RoutePlan{}, fmt.Errorf("resolve delivery target for %s: %w", intent.Recipient.ID(), err)
		}
		intent.TargetOwnership = owner
	}
	ledger, err := p.resolveConnectEvaluation(plan.ConnectEvaluation, plan.DeliveryIntents)
	if err != nil {
		return RoutePlan{}, err
	}
	plan.ConnectEvaluation = ledger
	return plan.Normalized(), nil
}

func deliveryIntentTargetHandler(intent RoutePlanDeliveryIntent, routed []Subscriber, eventType events.EventType) (runtimepipeline.DeliveryTargetHandler, error) {
	if !intent.Handler.Empty() {
		return intent.Handler, nil
	}
	var selected runtimepipeline.DeliveryTargetHandler
	for _, subscriber := range routed {
		if subscriber.Recipient != intent.Recipient || subscriber.targetHandler.Empty() {
			continue
		}
		candidate := subscriber.targetHandler
		if localized := strings.TrimSpace(subscriber.LocalizedEvent); localized != "" {
			candidate = candidate.ForEvent(events.EventType(localized))
		} else {
			candidate = candidate.ForEvent(eventType)
		}
		if selected.Empty() {
			selected = candidate
			continue
		}
		if !selected.Equal(candidate) {
			return runtimepipeline.DeliveryTargetHandler{}, fmt.Errorf("multiple authored handlers reach the same recipient route")
		}
	}
	if selected.Empty() {
		return runtimepipeline.DeliveryTargetHandler{}, fmt.Errorf("no admitted authored handler reaches the recipient route")
	}
	return selected, nil
}

func (p selectedRunTargetOwnerProjection) resolveConnectEvaluation(ledger events.ConnectEvaluationLedger, intents []RoutePlanDeliveryIntent) (events.ConnectEvaluationLedger, error) {
	if !ledger.Present() {
		return ledger, nil
	}
	plans := ledger.Plans()
	resolved := make([]events.ConnectPlanEvaluation, 0, len(plans))
	for _, plan := range plans {
		targets := plan.Targets()
		for index, target := range targets {
			if target.Empty() {
				continue
			}
			owners := map[events.RouteIdentity]struct{}{}
			matchedIntents := 0
			for _, intent := range intents {
				blueprint := intent.TargetBlueprint.Normalized()
				if blueprint.FlowInstance != target.FlowInstance || (target.EntityID != "" && blueprint.EntityID != "" && blueprint.EntityID != target.EntityID) {
					continue
				}
				matchedIntents++
				if intent.TargetOwnership.Empty() {
					continue
				}
				owners[intent.TargetOwnership.Route()] = struct{}{}
			}
			if matchedIntents == 0 {
				continue
			}
			if len(owners) != 1 {
				return events.ConnectEvaluationLedger{}, fmt.Errorf("resolve connect evaluation target %q from exact admitted recipients: found %d owners", target.FlowInstance, len(owners))
			}
			for owner := range owners {
				targets[index] = owner
			}
		}
		projected, err := events.NewConnectPlanEvaluation(
			plan.PlanIdentity(), plan.Resolution(), targets, plan.Candidates(),
		)
		if err != nil {
			return events.ConnectEvaluationLedger{}, fmt.Errorf("project connect evaluation target owner: %w", err)
		}
		resolved = append(resolved, projected)
	}
	projected, err := events.NewConnectEvaluationLedger(resolved)
	if err != nil {
		return events.ConnectEvaluationLedger{}, fmt.Errorf("build projected connect evaluation: %w", err)
	}
	return projected, nil
}

func (p selectedRunTargetOwnerProjection) withActivationPlans(plans []runtimepipeline.FlowInstanceActivationPlan) (selectedRunTargetOwnerProjection, error) {
	for _, plan := range plans {
		normalized, err := plan.Normalized()
		if err != nil {
			return selectedRunTargetOwnerProjection{}, fmt.Errorf("normalize selected-run activation owner: %w", err)
		}
		if err := normalized.Validate(); err != nil {
			return selectedRunTargetOwnerProjection{}, fmt.Errorf("validate selected-run activation owner: %w", err)
		}
		p.descriptors = appendActiveTargetDescriptor(p.descriptors, ActiveTargetDescriptor{
			ID:            normalized.Identity.InstanceID,
			FlowInstance:  normalized.Identity.InstancePath,
			EntityID:      normalized.Identity.EntityID,
			Materializing: true,
		})
		p.targetsAvailable = true
	}
	return p, nil
}

type selectedRunTargetOwnerProjectionKey struct{}

func withSelectedRunTargetOwnerProjection(ctx context.Context, projection selectedRunTargetOwnerProjection) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, selectedRunTargetOwnerProjectionKey{}, projection)
}

func selectedRunTargetOwnerProjectionFromContext(ctx context.Context) (selectedRunTargetOwnerProjection, bool) {
	if ctx == nil {
		return selectedRunTargetOwnerProjection{}, false
	}
	projection, ok := ctx.Value(selectedRunTargetOwnerProjectionKey{}).(selectedRunTargetOwnerProjection)
	return projection, ok
}

func (p deliveryRecipientPolicy) loadSelectedRunTargetOwnerProjection(ctx context.Context) (selectedRunTargetOwnerProjection, error) {
	agents, agentsAvailable, err := p.loadActiveAgentDescriptors(ctx)
	if err != nil {
		return selectedRunTargetOwnerProjection{}, err
	}
	var descriptors []ActiveTargetDescriptor
	targetsAvailable := false
	if p.loadActiveTargetDescriptors != nil {
		loaded, available, err := p.loadActiveTargetDescriptors(ctx)
		if err != nil {
			return selectedRunTargetOwnerProjection{}, err
		}
		targetsAvailable = targetsAvailable || available
		for _, descriptor := range loaded {
			descriptors = appendActiveTargetDescriptor(descriptors, descriptor)
		}
	}
	projection := selectedRunTargetOwnerProjection{
		agents: agents, agentsAvailable: agentsAvailable,
		descriptors: descriptors, targetsAvailable: targetsAvailable, source: p.semanticSource, required: p.requireTargetOwners,
	}
	if route, ok := runtimedelivery.RouteFromContext(ctx); ok {
		projection.structural = route.Target
	}
	if projection.required {
		if err := projection.validate(); err != nil {
			return selectedRunTargetOwnerProjection{}, err
		}
	}
	return projection, nil
}

func (p selectedRunTargetOwnerProjection) validate() error {
	for _, descriptor := range p.descriptors {
		descriptor = descriptor.Normalized()
		if descriptor.EntityID == "" {
			return fmt.Errorf("selected-run target owner %q for flow instance %q is missing exact entity identity", descriptor.ID, descriptor.FlowInstance)
		}
	}
	return nil
}

func (p selectedRunTargetOwnerProjection) pinRoutingDescriptors(plans []runtimepinrouting.ConnectRoutePlan) []runtimepinrouting.Descriptor {
	out := make([]runtimepinrouting.Descriptor, 0, len(p.descriptors)+len(plans))
	for _, descriptor := range p.descriptors {
		descriptor = descriptor.Normalized()
		out = append(out, runtimepinrouting.Descriptor{
			ID: descriptor.ID, EntityID: descriptor.EntityID, FlowInstance: descriptor.FlowInstance,
			AddressFields: normalizeDescriptorAddressFields(descriptor.AddressFields),
		})
	}
	for _, plan := range plans {
		if !plan.StructuralTargetOwnerEligible() {
			continue
		}
		for _, target := range plan.Readback().Targets {
			if descriptor, ok := p.structuralDescriptor(target); ok {
				out = append(out, runtimepinrouting.Descriptor{
					ID: descriptor.ID, EntityID: descriptor.EntityID, FlowInstance: descriptor.FlowInstance,
				})
			}
		}
	}
	return out
}

func (p selectedRunTargetOwnerProjection) resolveSelectedRoute(blueprint events.RouteIdentity, allowStructural bool) (events.DeliveryTargetOwnership, error) {
	blueprint = blueprint.Normalized()
	if blueprint.Empty() {
		if !p.required {
			return events.DeliveryTargetOwnership{}, nil
		}
		return events.DeliveryTargetOwnership{}, fmt.Errorf("receiver target blueprint is required")
	}
	owners := make(map[events.DeliveryTargetOwnership]struct{})
	for _, descriptor := range p.descriptors {
		descriptor = descriptor.Normalized()
		if blueprint.FlowInstance != "" && descriptor.FlowInstance != blueprint.FlowInstance {
			continue
		}
		if blueprint.EntityID != "" && descriptor.EntityID != blueprint.EntityID {
			continue
		}
		owner := blueprint
		owner.EntityID = descriptor.EntityID
		ownership, err := deliveryTargetOwnershipFromDescriptor(owner, descriptor)
		if err != nil {
			return events.DeliveryTargetOwnership{}, err
		}
		owners[ownership] = struct{}{}
	}
	if len(owners) == 0 && allowStructural {
		if blueprint.EntityID == "" {
			if descriptor, ok := p.structuralDescriptor(blueprint); ok {
				owner := blueprint
				owner.EntityID = descriptor.EntityID
				var ownership events.DeliveryTargetOwnership
				var err error
				if p.structural.MaterializingEntity() {
					ownership, err = events.NewMaterializingEntityTarget(owner)
				} else {
					ownership, err = events.NewExistingEntityTarget(owner)
				}
				if err != nil {
					return events.DeliveryTargetOwnership{}, err
				}
				owners[ownership] = struct{}{}
			}
		}
	}
	if len(owners) != 1 {
		if len(owners) == 0 && !p.required {
			if blueprint.EntityID != "" {
				return events.NewExistingEntityTarget(blueprint)
			}
			return events.NewEntitylessReceiverTarget(blueprint)
		}
		candidates := make([]string, 0, len(owners))
		for owner := range owners {
			route := owner.Route()
			candidates = append(candidates, fmt.Sprintf("%s:%s/%s", owner.Code(), route.FlowInstance, route.EntityID))
		}
		sort.Strings(candidates)
		if len(candidates) == 0 {
			return events.DeliveryTargetOwnership{}, fmt.Errorf("receiver target owner is missing for flow instance %q", blueprint.FlowInstance)
		}
		return events.DeliveryTargetOwnership{}, fmt.Errorf("receiver target owner is ambiguous for flow instance %q; candidates: %s", blueprint.FlowInstance, strings.Join(candidates, ", "))
	}
	for owner := range owners {
		return owner, nil
	}
	return events.DeliveryTargetOwnership{}, fmt.Errorf("receiver target owner resolution failed")
}

func deliveryTargetOwnershipFromDescriptor(route events.RouteIdentity, descriptor ActiveTargetDescriptor) (events.DeliveryTargetOwnership, error) {
	if descriptor.Materializing {
		return events.NewMaterializingEntityTarget(route)
	}
	return events.NewExistingEntityTarget(route)
}

func (p selectedRunTargetOwnerProjection) targetOwnerCandidates() []runtimepipeline.DeliveryTargetOwnerCandidate {
	out := make([]runtimepipeline.DeliveryTargetOwnerCandidate, 0, len(p.descriptors))
	for _, descriptor := range p.descriptors {
		descriptor = descriptor.Normalized()
		out = append(out, runtimepipeline.DeliveryTargetOwnerCandidate{
			Route:         events.RouteIdentity{FlowInstance: descriptor.FlowInstance, EntityID: descriptor.EntityID},
			Materializing: descriptor.Materializing,
		})
	}
	return out
}

func (p selectedRunTargetOwnerProjection) structuralDescriptor(blueprint events.RouteIdentity) (ActiveTargetDescriptor, bool) {
	blueprint = blueprint.Normalized()
	structural := p.structural.Route()
	if blueprint.FlowInstance == "" || structural.EntityID == "" {
		return ActiveTargetDescriptor{}, false
	}
	return ActiveTargetDescriptor{
		ID:       "structural:" + blueprint.FlowInstance,
		EntityID: structural.EntityID, FlowInstance: blueprint.FlowInstance,
	}.Normalized(), true
}
