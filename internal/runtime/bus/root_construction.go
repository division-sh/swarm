package bus

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/core/provideroutput"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// The creating root input prepares the whole keyless tree before recipient
// classification. Publication commits that tree and its deliveries atomically.
func (p deliveryPlanner) prepareRootConstruction(ctx context.Context, event events.Event) ([]pipeline.FlowInstanceActivationPlan, error) {
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
	if !found {
		return nil, fmt.Errorf("root construction requires its admitted schema")
	}
	fact, present := correlation.SourceArtifactFactFromContext(ctx)
	if !present || p.connectPlanner.lifecycle.index == nil {
		return nil, fmt.Errorf("root construction requires its admitted source and native index")
	}
	owner, err := flowidentity.NewRunScopedFlowInstance(event.RunID(), flowidentity.StoredRoute(flowidentity.ScopeKey(source, flowID), event.RunID(), event.RunID()))
	if err != nil {
		return nil, err
	}
	lookup, err := pipeline.NewExactFlowInstanceLookup(source, fact, owner)
	if err != nil {
		return nil, err
	}
	_, err = p.connectPlanner.prospectiveConnectInstances(ctx, event.RunID(), fact)
	if err != nil {
		return nil, err
	}
	for _, tree := range preparedConnectPlans(ctx) {
		if tree.Identity.Route() != owner.Route {
			continue
		}
		if err := p.validateRootConstructorKey(event, schema.Instance.Path(), tree.Instance.InstanceKey); err != nil {
			return nil, err
		}
		if err := selectConnectionConstruction(ctx, tree.Identity); err != nil {
			return nil, err
		}
		return nil, nil
	}
	var observed pipeline.FlowInstanceObservation
	var exists bool
	if proposal := p.connectPlanner.lifecycle.runProposal; proposal.Present() {
		if err := proposal.Validate(event.RunID(), fact); err != nil {
			return nil, err
		}
	} else {
		observed, exists, err = p.connectPlanner.lifecycle.index.LookupFlowInstance(ctx, lookup)
		if err != nil {
			return nil, err
		}
	}
	if exists {
		if err := observed.ValidateSelection(lookup); err != nil {
			return nil, err
		}
		identity := observed.Identity()
		if err := p.validateRootConstructionReuse(ctx, event, observed, schema.Instance.Path()); err != nil {
			return nil, err
		}
		if err := selectConnectionConstruction(ctx, identity); err != nil {
			return nil, err
		}
		return nil, nil
	}
	identity := flowidentity.Stored(source, flowID, event.RunID(), event.RunID(), "", "")
	if p.connectPlanner.lifecycle.plan == nil {
		return nil, fmt.Errorf("root construction requires its canonical activation planner")
	}
	request := pipeline.FlowInstanceActivationRequest{
		Context: events.DeliveryContextFromContext(ctx), ContractBundle: source,
		Instance: identity, TriggerEvent: event, OccurredAt: event.CreatedAt(),
	}
	request, transportOnly, err := prepareRootConstructorArguments(ctx, request, pin.EventType(), schema.Instance.Path())
	if err != nil || transportOnly {
		return nil, err
	}
	plan, err := p.connectPlanner.lifecycle.plan.PrepareFlowInstanceActivation(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("prepare root constructor: %w", err)
	}
	return []pipeline.FlowInstanceActivationPlan{plan}, nil
}

func prepareRootConstructorArguments(ctx context.Context, request pipeline.FlowInstanceActivationRequest, input, key string) (pipeline.FlowInstanceActivationRequest, bool, error) {
	if key == "" {
		return request, false, nil
	}
	request.ConstructorInput = input
	event := request.TriggerEvent
	if admission, authenticated := authenticatedProviderPublicationForEvent(ctx, event); authenticated && admission.kind == provideroutput.KindRaw &&
		!pinrouting.ClassifyRoutingSourceOutputConsumer(request.ContractBundle, string(event.Type()), event.RoutingSource()).HasRuntimeConsumer() {
		return request, true, nil
	}
	if _, err := pipeline.CompileFlowConstructor(request.ContractBundle, request.Instance.TemplateID, input); err != nil {
		return request, false, err
	}
	var payload map[string]any
	if err := canonicaljson.DecodePreservingNumberLexemes(event.Payload(), &payload); err != nil {
		return request, false, fmt.Errorf("root constructor payload: %w", err)
	}
	request.ResolvedKey = payload[key]
	return request, false, nil
}

func (p deliveryPlanner) validateRootConstructionReuse(ctx context.Context, event events.Event, observed pipeline.FlowInstanceObservation, key string) error {
	instance, err := observed.WorkflowInstance()
	if err != nil {
		return err
	}
	stage := ""
	if instance.StageDefined {
		stage = instance.CurrentState
	}
	if err := pipeline.NewDeliveryTargetAvailability(stage, instance.Status, !instance.TerminatedAt.IsZero()).Validate(p.recipientPolicy.semanticSource, observed.Identity().TemplateID); err != nil {
		return err
	}
	return p.validateRootConstructorKey(event, key, observed.InstanceKey())
}

func (p deliveryPlanner) validateRootConstructorKey(event events.Event, key, instanceKey string) error {
	if key == "" {
		return nil
	}
	flowID := semanticview.RootExecutionFlowID(p.recipientPolicy.semanticSource)
	contract, found := entityruntime.ResolveForFlow(p.recipientPolicy.semanticSource, flowID)
	if !found || instanceKey == "" {
		return fmt.Errorf("keyed root reuse requires its admitted immutable key")
	}
	var payload map[string]any
	if err := canonicaljson.DecodePreservingNumberLexemes(event.Payload(), &payload); err != nil {
		return err
	}
	if supplied, present := payload[key]; present {
		candidate, err := entityruntime.NormalizeFieldValue(contract, key, supplied)
		if err != nil {
			return err
		}
		keys, err := pipeline.AdmitFlowInstanceKeyMaterial(p.recipientPolicy.semanticSource, flowID, candidate)
		if err != nil || len(keys) != 1 || keys[0].Value != instanceKey {
			return fmt.Errorf("root input key %s contradicts its immutable constructor key", key)
		}
	}
	return nil
}
