package bus

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
)

func (eb *EventBus) recordedReplayPlan(ctx context.Context, evt events.Event, selection operatorread.ReplaySelection) (RoutePlan, error) {
	if err := selection.ValidateEvent(evt); err != nil {
		return RoutePlan{}, err
	}
	ctx = runtimecorrelation.WithInboundEvent(ctx, evt)
	agents, available, err := eb.deliveryPlanner.recipientPolicy.loadActiveAgentDescriptors(ctx)
	if err != nil {
		return RoutePlan{}, err
	}
	routes := selection.Routes()
	manifest := deliveryRecipientManifest{}
	var missing []agentidentity.Identity
	for _, route := range routes {
		identity := route.AgentIdentity.Normalize()
		if available {
			if descriptor, found := agents[identity]; !found || descriptor.Normalized().Identity != identity {
				missing = append(missing, identity)
				continue
			}
		}
		manifest.LiveRecipients = append(manifest.LiveRecipients, deliveryRecipientCandidate{
			ID: route.Recipient.ID(), AgentIdentity: identity, PersistAsDelivery: true,
		})
	}
	if len(missing) > 0 {
		return RoutePlan{}, &exactDirectRecipientsUnavailableError{identities: missing}
	}
	plan := routePlanFromManifest(evt, manifest)
	plan.DeliveryIntents = routePlanDeliveryIntentsFromAdmittedRoutes(routes)
	if err := plan.ValidatePersistentDeliveries(); err != nil {
		return RoutePlan{}, fmt.Errorf("validate recorded replay delivery routes: %w", err)
	}
	return plan.WithDefaultDeliveryContext(events.DeliveryContextFromContext(ctx)), nil
}
