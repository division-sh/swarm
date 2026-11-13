package bus

import (
	"context"
	"errors"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
)

// requesterReturnRelation is the source-owned built-in edge. A binding supplies
// its native continuation endpoint, never a subscription or caller recipient set.
type requesterReturnRelation struct{ compiled bool }

func (r requesterReturnRelation) bind(ctx context.Context, p deliveryPlanner, evt events.Event, binding decisioncard.HumanTaskReturn) (RoutePlan, error) {
	if !r.compiled {
		return RoutePlan{}, errors.New("human-task requester-return relation is not compiled")
	}
	if err := binding.ValidateEvent(evt, events.DeliveryContextFromContext(ctx)); err != nil {
		return RoutePlan{}, err
	}
	p.recipientPolicy.instances = p.connectPlanner.lifecycle.index
	ctx = runtimecorrelation.WithInboundEvent(ctx, evt)
	scope, bounded, err := p.connectPlanner.selectedTargetScope(ctx, evt)
	if err != nil {
		return RoutePlan{}, err
	}
	if bounded {
		ctx = withSelectedTargetOwnerLookupScope(ctx, scope)
	}
	projection, err := p.recipientPolicy.loadPublicationTargetOwners(ctx)
	if err != nil {
		return RoutePlan{}, err
	}
	ctx = withPublicationTargetOwners(ctx, projection)
	manifest, err := p.recipientPolicy.evaluate(ctx, evt, agentDeliveryRecipientCandidates([]string{binding.RequesterID()}), ordinaryPublicationSource{})
	if err != nil {
		return RoutePlan{}, err
	}
	identities := map[agentidentity.Identity]struct{}{}
	for _, recipient := range manifest.LiveRecipients {
		if recipient.ID != binding.RequesterID() || recipient.AgentIdentity.IsZero() {
			return RoutePlan{}, errors.New("human-task return contains a foreign requester")
		}
		identities[recipient.AgentIdentity.Normalize()] = struct{}{}
	}
	if len(identities) != 1 || len(manifest.DeliveryRoutes) != 1 {
		return RoutePlan{}, errors.New("human-task requester is missing, unavailable or ambiguous")
	}
	plan := routePlanFromManifest(evt, manifest)
	plan.MarkCanonicalRouteMatched(routeIntentProducerOrdinary)
	return p.resolvePreparedRoutePlan(projection, plan)
}
