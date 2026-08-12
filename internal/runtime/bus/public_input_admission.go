package bus

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type publicInputAdmissionContextKey struct{}

type publicInputAdmission struct {
	endpointID string
	flowID     string
	pinName    string
	eventType  events.EventType
	plan       runtimepinrouting.ConnectRoutePlan
}

func newPublicInputAdmission(source semanticview.Source, endpoint semanticview.AuthoredEventEndpoint) (publicInputAdmission, error) {
	plan, issue := runtimepinrouting.LowerPublicInputRoutePlan(source, endpoint)
	if !issue.Failure.Empty() {
		return publicInputAdmission{}, fmt.Errorf("public input route %s.%s: %s (%s)",
			strings.TrimSpace(endpoint.FlowID), strings.TrimSpace(endpoint.PinName), issue.Failure.Code(), strings.TrimSpace(issue.Detail))
	}
	readback := plan.ReceiverEndpoint().Readback()
	eventType := events.EventType(strings.TrimSpace(readback.ResolvedEvent))
	if eventType == "" {
		return publicInputAdmission{}, fmt.Errorf("public input route %s.%s has no resolved event identity", endpoint.FlowID, endpoint.PinName)
	}
	return publicInputAdmission{
		endpointID: strings.TrimSpace(endpoint.ID),
		flowID:     strings.TrimSpace(endpoint.FlowID),
		pinName:    strings.TrimSpace(endpoint.PinName),
		eventType:  eventType,
		plan:       plan,
	}, nil
}

func (a publicInputAdmission) validateEvent(evt events.Event) error {
	if a.endpointID == "" || a.flowID == "" || a.pinName == "" || a.eventType == "" {
		return fmt.Errorf("public input admission is incomplete")
	}
	if evt.AdmissionClass() != events.EventAdmissionRootIngress {
		return fmt.Errorf("public input admission requires a new-run root ingress event")
	}
	if evt.Type() != a.eventType {
		return fmt.Errorf("public input endpoint %s.%s resolves %s, not %s", a.flowID, a.pinName, a.eventType, evt.Type())
	}
	return nil
}

func withPublicInputAdmission(ctx context.Context, admission publicInputAdmission) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, publicInputAdmissionContextKey{}, admission)
}

func publicInputAdmissionFromContext(ctx context.Context) (publicInputAdmission, bool) {
	if ctx == nil {
		return publicInputAdmission{}, false
	}
	admission, ok := ctx.Value(publicInputAdmissionContextKey{}).(publicInputAdmission)
	return admission, ok
}

func requirePublicInputRoutePlan(ctx context.Context, routePlan RoutePlan) error {
	admission, required := publicInputAdmissionFromContext(ctx)
	if !required {
		return nil
	}
	if !routePlan.TargetFailure.Empty() {
		return fmt.Errorf("public input endpoint %s.%s is not routable: %s", admission.flowID, admission.pinName, routePlan.TargetFailure.Code())
	}
	if !routePlan.CanonicalRouteOwnerMatched() || routePlan.AuthorityOwner != routePlanSourceConnectRoutePlan {
		return fmt.Errorf("public input endpoint %s.%s did not select the canonical connect route owner", admission.flowID, admission.pinName)
	}
	if len(routePlan.DeliveryRoutes()) == 0 {
		return fmt.Errorf("public input endpoint %s.%s selected zero durable deliveries", admission.flowID, admission.pinName)
	}
	return nil
}

// PublishPublicInputAcknowledged admits one census-proven public template input
// through the canonical route/lifecycle transaction before acknowledging it.
func (eb *EventBus) PublishPublicInputAcknowledged(ctx context.Context, evt events.Event, endpoint semanticview.AuthoredEventEndpoint) error {
	if eb == nil {
		return fmt.Errorf("event bus is required")
	}
	admission, err := newPublicInputAdmission(eb.semanticSource, endpoint)
	if err != nil {
		return err
	}
	if err := admission.validateEvent(evt); err != nil {
		return err
	}
	return eb.PublishAcknowledged(withPublicInputAdmission(ctx, admission), evt)
}
