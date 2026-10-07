package bus

import (
	"context"
	"fmt"
	"reflect"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/core/provideroutput"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// The creating root input prepares the whole keyless tree before recipient
// classification. Publication commits that tree and its deliveries atomically.
func (p deliveryPlanner) prepareRootConstruction(ctx context.Context, event events.Event, projection selectedRunTargetOwnerProjection) ([]pipeline.FlowInstanceActivationPlan, error) {
	sourceKind := event.RoutingSource().Kind()
	if sourceKind != events.RoutingSourceRoot && sourceKind != events.RoutingSourceExternalIngress {
		return nil, nil
	}
	switch event.AdmissionClass() {
	case events.EventAdmissionRootIngress, events.EventAdmissionOperatorInjected:
	default:
		return nil, nil
	}
	source := p.recipientPolicy.semanticSource
	if sourceKind == events.RoutingSourceExternalIngress && event.RoutingSource().Route().FlowID != semanticview.RootExecutionFlowID(source) {
		return nil, nil
	}
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
		if !schema.Instance.Empty() {
			if err := p.validateRootConstructionReuse(ctx, event, identity, schema.Instance.Path()); err != nil {
				return nil, err
			}
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
		constructor, err := pipeline.CompileFlowConstructor(source, flowID, request.ConstructorInput)
		if err != nil {
			return nil, err
		}
		if admission, authenticated := authenticatedProviderPublicationForEvent(ctx, event); authenticated && admission.kind == provideroutput.KindRaw &&
			!constructor.Eligible() && !pinrouting.ClassifyRoutingSourceOutputConsumer(source, string(event.Type()), event.RoutingSource()).HasRuntimeConsumer() {
			return nil, nil
		}
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

func (p deliveryPlanner) validateRootConstructionReuse(ctx context.Context, event events.Event, instance flowidentity.Instance, key string) error {
	reader, ok := p.connectPlanner.lifecycle.plan.(pipeline.FlowConstructionPublicationReader)
	if !ok {
		return fmt.Errorf("root reuse requires its immutable construction receipt owner")
	}
	owner, err := flowidentity.NewRunScopedFlowInstance(event.RunID(), instance.Route())
	if err != nil {
		return err
	}
	receipt, err := reader.LoadFlowConstructionPublication(ctx, owner, instance.EntityID)
	if err != nil {
		return fmt.Errorf("root reuse construction receipt: %w", err)
	}
	contract, found := entityruntime.ResolveForFlow(p.recipientPolicy.semanticSource, instance.TemplateID)
	if !found || receipt.CreatingInput.EventID == "" || receipt.Fields[key] == nil {
		return fmt.Errorf("keyed root reuse requires its exact immutable creating input and key")
	}
	stored, err := entityruntime.NormalizeFieldValue(contract, key, receipt.Fields[key])
	if err != nil {
		return fmt.Errorf("root construction receipt key: %w", err)
	}
	var payload map[string]any
	if err := canonicaljson.DecodePreservingNumberLexemes(event.Payload(), &payload); err != nil {
		return err
	}
	if supplied, present := payload[key]; present {
		candidate, err := entityruntime.NormalizeFieldValue(contract, key, supplied)
		if err != nil || !reflect.DeepEqual(candidate, stored) {
			return fmt.Errorf("root input key %s contradicts its immutable constructor key", key)
		}
	}
	return nil
}
