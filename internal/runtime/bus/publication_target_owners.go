package bus

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
)

// This evidence ends at publication preparation. Neither RoutePlan nor the
// persisted/dispatch command carries its lookup context or planner.
type closedPublicationRoutes struct {
	plan   RoutePlan
	owners *publicationTargetOwnerEvidence
}

type publicationTargetOwnerEvidence struct {
	event      events.Event
	projection selectedRunTargetOwnerProjection
	planner    deliveryPlanner
	present    bool
	origins    map[string]events.RouteIdentity
}

func (p deliveryPlanner) resolvePreparedRoutePlan(projection selectedRunTargetOwnerProjection, plan RoutePlan) (RoutePlan, error) {
	var err error
	if p.preparedOwners != nil && len(plan.ActivationPlans) > 0 {
		projection, err = projection.withActivationPlans(plan.ActivationPlans)
		if err != nil {
			return RoutePlan{}, err
		}
	}
	plan, err = projection.resolveRoutePlan(plan)
	if err != nil {
		return RoutePlan{}, err
	}
	if p.preparedOwners != nil {
		var origins map[string]events.RouteIdentity
		if len(plan.ReplyCreations) > 0 {
			origins = make(map[string]events.RouteIdentity, len(plan.ReplyCreations))
		}
		for _, record := range plan.ReplyCreations {
			if err := record.Validate(); err != nil {
				return RoutePlan{}, err
			}
			if record.RequestEventID != p.preparedOwners.event.ID() || record.RunID != p.preparedOwners.event.RunID() {
				return RoutePlan{}, fmt.Errorf("reply creation crosses its admitted publication")
			}
			origins[record.ID] = record.Origin
		}
		p.preparedOwners.projection = projection
		p.preparedOwners.present = true
		p.preparedOwners.origins = origins
	}
	return plan, nil
}

func (p *publicationTargetOwnerEvidence) validate(ctx context.Context, event events.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if p == nil || !p.present || p.projection.context == nil {
		return fmt.Errorf("reply admission requires its publication-local target owners")
	}
	if err := p.projection.context.Err(); err != nil {
		return err
	}
	if event.ID() != p.event.ID() || event.RunID() != p.event.RunID() ||
		event.AdmissionClass() != p.event.AdmissionClass() || event.ExecutionMode() != p.event.ExecutionMode() ||
		event.Type() != p.event.Type() || event.Producer() != p.event.Producer() ||
		event.ParentEventID() != p.event.ParentEventID() ||
		event.RoutingSource() != p.event.RoutingSource() || event.SourceRoute() != p.event.SourceRoute() {
		return fmt.Errorf("reply target owners cross their admitted publication")
	}
	fact, present := correlation.SourceArtifactFactFromContext(ctx)
	captured, admitted := correlation.SourceArtifactFactFromContext(p.projection.context)
	if !present || !admitted || !fact.Matches(captured) {
		return fmt.Errorf("reply target owners cross their admitted source")
	}
	scope, scoped := p.projection.context.Value(selectedTargetOwnerLookupScopeKey{}).(selectedTargetOwnerLookupScope)
	if !scoped || !scope.lookup.Valid() || scope.lookup.RunID() != event.RunID() || !scope.lookup.SourceFact().Matches(fact) {
		return fmt.Errorf("reply target owners require their compiled instance lookup scope")
	}
	return nil
}

func (p *publicationTargetOwnerEvidence) returnOwner(ctx context.Context, event events.Event, plan *RoutePlan, record replycontext.Record) (events.DeliveryTargetOwnership, error) {
	if err := p.validate(ctx, event); err != nil {
		return events.DeliveryTargetOwnership{}, err
	}
	origin, captured := p.origins[record.ID]
	if !captured || origin != record.Origin || record.RunID != event.RunID() || record.RequestEventID != event.ID() {
		return events.DeliveryTargetOwnership{}, fmt.Errorf("reply origin differs from its prepared publication")
	}
	coordinates, err := p.planner.connectPlanner.exactLookupCoordinates(event.RunID(), []events.RouteIdentity{origin})
	if err != nil {
		return events.DeliveryTargetOwnership{}, err
	}
	if len(coordinates) != 1 {
		return events.DeliveryTargetOwnership{}, fmt.Errorf("reply origin requires one exact compiled coordinate")
	}
	scope := p.projection.context.Value(selectedTargetOwnerLookupScopeKey{}).(selectedTargetOwnerLookupScope)
	covered := false
	for _, flow := range scope.lookup.FlowIDs() {
		covered = covered || flow == coordinates[0].Route.ScopeKey
	}
	for _, coordinate := range scope.lookup.Coordinates() {
		covered = covered || coordinate == coordinates[0]
	}
	if !covered {
		flows := make(map[string]struct{})
		for _, flow := range scope.lookup.FlowIDs() {
			flows[flow] = struct{}{}
		}
		coordinates = append(scope.lookup.Coordinates(), coordinates...)
		scope, err = p.planner.connectPlanner.lookupTargetScope(p.projection.context, event.RunID(), flows, coordinates)
		if err != nil {
			return events.DeliveryTargetOwnership{}, err
		}
		bounded := withSelectedTargetOwnerLookupScope(p.projection.context, scope)
		p.projection, err = p.planner.recipientPolicy.loadSelectedRunTargetOwnerProjection(bounded)
		if err != nil {
			return events.DeliveryTargetOwnership{}, err
		}
	}
	p.projection, err = p.projection.withActivationPlans(plan.ActivationPlans)
	if err != nil {
		return events.DeliveryTargetOwnership{}, err
	}
	return p.projection.resolveSelectedRoute(origin)
}
