package bus

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// The creating root input prepares the whole keyless tree before recipient
// classification. Publication commits that tree and its deliveries atomically.
func (p deliveryPlanner) prepareRootConstruction(ctx context.Context, event events.Event, projection selectedRunTargetOwnerProjection) ([]pipeline.FlowInstanceActivationPlan, error) {
	if event.RoutingSource().Kind() != events.RoutingSourceRoot {
		return nil, nil
	}
	switch event.AdmissionClass() {
	case events.EventAdmissionRootIngress, events.EventAdmissionOperatorInjected:
	default:
		return nil, nil
	}
	source := p.recipientPolicy.semanticSource
	pin, admitted := semanticview.SelectedRootInputPin(source, string(event.Type()))
	if !admitted {
		return nil, nil
	}
	flowID := semanticview.RootExecutionFlowID(source)
	schema, found := source.FlowSchemaByID(flowID)
	if !found || !projection.targetsAvailable {
		return nil, fmt.Errorf("root construction requires the admitted schema and selected-store ownership snapshot")
	}
	identity := flowidentity.Stored(source, flowID, event.RunID(), event.RunID(), "", "")
	for _, descriptor := range projection.descriptors {
		if descriptor.FlowInstance != identity.InstancePath {
			continue
		}
		if descriptor.EntityID != identity.EntityID {
			return nil, fmt.Errorf("root construction identity contradicts its persisted owner")
		}
		if err := descriptor.Availability.Validate(source, flowID); err != nil {
			return nil, err
		}
		return nil, nil
	}
	if p.connectPlanner.lifecycle.plan == nil {
		return nil, fmt.Errorf("root construction requires its canonical activation planner")
	}
	request := pipeline.FlowInstanceActivationRequest{
		Context: events.DeliveryContextFromContext(ctx), ContractBundle: source,
		Instance: identity, TriggerEvent: event, OccurredAt: event.CreatedAt(),
	}
	if !schema.Instance.Empty() {
		request.ConstructorInput = pin.EventType()
		var payload map[string]any
		if err := canonicaljson.DecodePreservingNumberLexemes(event.Payload(), &payload); err != nil {
			return nil, fmt.Errorf("root constructor payload: %w", err)
		}
		request.ResolvedKey = payload[schema.Instance.Path()]
	}
	plan, err := p.connectPlanner.lifecycle.plan.PrepareFlowInstanceActivation(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("prepare root constructor: %w", err)
	}
	return []pipeline.FlowInstanceActivationPlan{plan}, nil
}
