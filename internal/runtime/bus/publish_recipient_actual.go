package bus

import (
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

// PublishRecipientActual preserves execution authority which the diagnostic
// subscriber projection and durable DeliveryRoute alone cannot express.
type PublishRecipientActual struct {
	route       events.DeliveryRoute
	handler     runtimepipeline.DeliveryTargetHandler
	connectPlan events.ConnectPlanIdentity
}

func (a PublishRecipientActual) Route() events.DeliveryRoute {
	route := a.route
	route.Context = route.Context.Normalized()
	return route
}

func (a PublishRecipientActual) Handler() runtimepipeline.DeliveryTargetHandler { return a.handler }

func (a PublishRecipientActual) ConnectPlan() (events.ConnectPlanIdentity, bool) {
	return a.connectPlan, !a.connectPlan.Empty()
}

// RecipientActuals returns the effective persistent intents, not a reconstruction
// from recipient IDs or diagnostic provenance.
func (p PublishRecipientPlan) RecipientActuals() ([]PublishRecipientActual, error) {
	if p.actualErr != nil {
		return nil, p.actualErr
	}
	if !p.actualPresent {
		return nil, fmt.Errorf("publish plan lacks canonical recipient intent evidence")
	}
	return append([]PublishRecipientActual(nil), p.actuals...), nil
}

func (p RoutePlan) recipientActuals() ([]PublishRecipientActual, error) {
	out := make([]PublishRecipientActual, 0, len(p.DeliveryIntents))
	for index, intent := range p.DeliveryIntents {
		if !intent.Persist {
			continue
		}
		if err := validateRecipientIntentProducer(intent); err != nil {
			return nil, fmt.Errorf("recipient intent %d: %w", index, err)
		}
		route := intent.deliveryRoute()
		if err := events.ValidateDeliveryRoutes([]events.DeliveryRoute{route}); err != nil {
			return nil, fmt.Errorf("recipient intent %d: %w", index, err)
		}
		if route.Recipient.IsNode() {
			node, _ := route.Recipient.Node()
			if intent.Handler.Empty() || !intent.Handler.Node().Equal(node) {
				return nil, fmt.Errorf("recipient intent %d lacks its exact node handler", index)
			}
			if _, present := intent.Handler.EventOverride(); !present {
				return nil, fmt.Errorf("recipient intent %d lacks its handler-local event", index)
			}
		}
		out = append(out, PublishRecipientActual{route: route, handler: intent.Handler, connectPlan: intent.ConnectPlan})
	}
	return out, nil
}

func validateRecipientIntentProducer(intent RoutePlanDeliveryIntent) error {
	if intent.Producer.Empty() {
		return fmt.Errorf("recipient intent lacks its semantic producer")
	}
	if intent.Producer == routeIntentProducerConnectRoutePlan {
		if intent.ConnectPlan.Empty() || intent.ConnectClaim.Empty() {
			return fmt.Errorf("connect recipient intent lacks plan or execution claim")
		}
	} else if !intent.ConnectPlan.Empty() || !intent.ConnectClaim.Empty() {
		return fmt.Errorf("local recipient intent carries competing connect authority")
	}
	return nil
}
