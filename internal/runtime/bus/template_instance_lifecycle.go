package bus

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type TemplateInstanceLifecycleAction uint8

const (
	templateInstanceLifecycleActionNone TemplateInstanceLifecycleAction = iota
	templateInstanceLifecycleActionCreated
	templateInstanceLifecycleActionPreviewCreate
	templateInstanceLifecycleActionReused
	templateInstanceLifecycleActionSelectedExisting
)

func templateInstanceLifecycleActionCode(a TemplateInstanceLifecycleAction) string {
	switch a {
	case templateInstanceLifecycleActionCreated:
		return "created"
	case templateInstanceLifecycleActionPreviewCreate:
		return "would_create"
	case templateInstanceLifecycleActionReused:
		return "reused"
	case templateInstanceLifecycleActionSelectedExisting:
		return "selected_existing"
	default:
		return ""
	}
}

type templateInstanceLifecyclePreviewKey struct{}

type templateInstanceLifecycleOwner struct {
	source     semanticview.Source
	routeTable *RouteTable
	plan       runtimepipeline.FlowInstanceActivationPlanner
}

type TemplateInstanceLifecycleDecision struct {
	Action        TemplateInstanceLifecycleAction
	InstanceID    string
	InstancePath  string
	EntityID      string
	KeyDigest     string
	KeyMaterial   []runtimecontracts.TemplateInstanceKeyValue
	SourceEventID string
	Activation    *runtimepipeline.FlowInstanceActivationPlan
	configuration map[string]any
	receiver      runtimepinrouting.ConnectRoutePlanEndpoint
}

func newTemplateInstanceLifecycleOwner(source semanticview.Source, routeTable *RouteTable, planner runtimepipeline.FlowInstanceActivationPlanner) templateInstanceLifecycleOwner {
	return templateInstanceLifecycleOwner{
		source:     source,
		routeTable: routeTable,
		plan:       planner,
	}
}

func (d TemplateInstanceLifecycleDecision) Empty() bool {
	return d.Action == templateInstanceLifecycleActionNone
}

func (d TemplateInstanceLifecycleDecision) Detail() map[string]any {
	if d.Empty() {
		return nil
	}
	receiver := d.receiver.Readback()
	return map[string]any{
		"action":          templateInstanceLifecycleActionCode(d.Action),
		"receiver_flow":   receiver.FlowID,
		"instance_id":     strings.TrimSpace(d.InstanceID),
		"instance_path":   strings.Trim(strings.TrimSpace(d.InstancePath), "/"),
		"entity_id":       strings.TrimSpace(d.EntityID),
		"key_digest":      strings.TrimSpace(d.KeyDigest),
		"key_material":    templateInstanceLifecycleKeyMaterialDetail(d.KeyMaterial),
		"source_event_id": strings.TrimSpace(d.SourceEventID),
	}
}

func (d TemplateInstanceLifecycleDecision) Route() runtimeflowidentity.Route {
	scope := runtimeflowidentity.SemanticScopeFromInstancePath(d.InstancePath)
	if scope == "" {
		scope = d.receiver.Readback().FlowID
	}
	return runtimeflowidentity.StoredRoute(scope, d.InstanceID, d.InstancePath)
}

func (d TemplateInstanceLifecycleDecision) ActivationVariables() map[string]string {
	if d.Activation != nil {
		return cloneRouteActivationVariables(d.Activation.ActivationVariables)
	}
	out := map[string]string{}
	for name, value := range d.configuration {
		out[name] = fmt.Sprint(value)
	}
	for _, key := range d.KeyMaterial {
		field := key.Field.Path()
		value := strings.TrimSpace(key.Value)
		if field != "" && value != "" {
			out[field] = value
		}
	}
	setTemplateInstanceLifecycleVariable(out, "entity_id", d.EntityID)
	setTemplateInstanceLifecycleVariable(out, "instance_id", d.InstanceID)
	setTemplateInstanceLifecycleVariable(out, "template_id", d.receiver.Readback().FlowID)
	if route := d.Route(); route.Valid() {
		setTemplateInstanceLifecycleVariable(out, "flow_scope_key", route.ScopeKey)
		setTemplateInstanceLifecycleVariable(out, "flow_instance_path", route.InstancePath)
	}
	return out
}

func setTemplateInstanceLifecycleVariable(vars map[string]string, key, value string) {
	value = strings.TrimSpace(value)
	if value == "" {
		return
	}
	vars[key] = value
}

func withTemplateInstanceLifecyclePreview(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, templateInstanceLifecyclePreviewKey{}, true)
}

func templateInstanceLifecyclePreview(ctx context.Context) bool {
	enabled, _ := ctx.Value(templateInstanceLifecyclePreviewKey{}).(bool)
	return enabled
}

func (o templateInstanceLifecycleOwner) Materialize(ctx context.Context, evt events.Event, plan runtimepinrouting.ConnectRoutePlan, values map[string]string, descriptors []runtimepinrouting.Descriptor) (runtimepinrouting.ConnectRoutePlanMaterialization, TemplateInstanceLifecycleDecision, bool, error) {
	if plan.ResolutionKind() != runtimepinrouting.ConnectResolutionInstanceKey || plan.InstanceKey() == nil {
		return runtimepinrouting.ConnectRoutePlanMaterialization{}, TemplateInstanceLifecycleDecision{}, false, nil
	}
	material, failure := instanceKeyMaterialForTemplateLifecycle(evt, plan, values)
	if !failure.Empty() {
		return runtimepinrouting.ConnectRoutePlanMaterialization{Failure: failure}, TemplateInstanceLifecycleDecision{}, true, nil
	}
	instanceContract, failure := o.resolveInstanceContract(plan, material)
	if !failure.Empty() {
		return runtimepinrouting.ConnectRoutePlanMaterialization{Failure: failure}, TemplateInstanceLifecycleDecision{}, true, nil
	}
	keyMaterial := append([]runtimecontracts.TemplateInstanceKeyValue{}, material.Keys...)
	if canonical, err := instanceContract.CanonicalKeyMaterial(material.CanonicalValues()); err == nil {
		keyMaterial = canonical
	} else {
		return runtimepinrouting.ConnectRoutePlanMaterialization{Failure: runtimepinrouting.ConnectFailureInstanceSourceValueMissing}, TemplateInstanceLifecycleDecision{}, true, nil
	}
	mode := plan.InstanceKey().Mode()
	switch mode {
	case runtimecontracts.FlowInputResolutionModeCreate,
		runtimecontracts.FlowInputResolutionModeSelect,
		runtimecontracts.FlowInputResolutionModeSelectOrCreate:
	default:
		return runtimepinrouting.ConnectRoutePlanMaterialization{Failure: runtimepinrouting.ConnectFailureInstanceResolutionInvalid}, TemplateInstanceLifecycleDecision{}, true, nil
	}
	matches := runtimepinrouting.InstanceKeyDescriptorRoutesForConnectRoutePlan(plan, keyMaterial, descriptors)
	if len(matches) > 1 {
		return runtimepinrouting.ConnectRoutePlanMaterialization{Failure: runtimepinrouting.ConnectFailureTargetAmbiguous}, TemplateInstanceLifecycleDecision{}, true, nil
	}
	if len(matches) == 1 {
		if mode == runtimecontracts.FlowInputResolutionModeCreate {
			return runtimepinrouting.ConnectRoutePlanMaterialization{Failure: runtimepinrouting.ConnectFailureInstanceConflict}, TemplateInstanceLifecycleDecision{}, true, nil
		}
		return templateInstanceLifecycleMaterialization(plan, matches), o.decision(plan, evt, keyMaterial, matches[0], templateInstanceLifecycleExistingAction(mode)), true, nil
	}
	instanceID := templateInstanceLifecycleInstanceID(plan, keyMaterial)
	instance := plan.DeriveReceiverIdentity(o.source, instanceID)
	derivedRoute := plan.ReceiverRoute(instance.InstancePath, instance.EntityID)
	if templateInstanceLifecycleMatchIsRoutable(o.routeTable, evt.RunID(), plan, derivedRoute) {
		if mode == runtimecontracts.FlowInputResolutionModeCreate {
			return runtimepinrouting.ConnectRoutePlanMaterialization{Failure: runtimepinrouting.ConnectFailureInstanceConflict}, TemplateInstanceLifecycleDecision{}, true, nil
		}
		decision := o.decision(plan, evt, keyMaterial, derivedRoute, templateInstanceLifecycleExistingAction(mode))
		return templateInstanceLifecycleMaterialization(plan, []events.RouteIdentity{derivedRoute}), decision, true, nil
	}
	if mode == runtimecontracts.FlowInputResolutionModeSelect {
		return runtimepinrouting.ConnectRoutePlanMaterialization{Failure: runtimepinrouting.ConnectFailureTargetUnresolved}, TemplateInstanceLifecycleDecision{}, true, nil
	}
	if o.plan == nil {
		return runtimepinrouting.ConnectRoutePlanMaterialization{Failure: runtimepinrouting.ConnectFailureLifecycleUnavailable}, TemplateInstanceLifecycleDecision{}, true, nil
	}
	req, decision, err := o.activationRequest(evt, plan, instanceContract, keyMaterial)
	if err != nil {
		return runtimepinrouting.ConnectRoutePlanMaterialization{}, TemplateInstanceLifecycleDecision{}, true, err
	}
	activation, err := o.plan.PrepareFlowInstanceActivation(ctx, req)
	if err != nil {
		return runtimepinrouting.ConnectRoutePlanMaterialization{}, TemplateInstanceLifecycleDecision{}, true, fmt.Errorf("plan connect-time template instance %s: %w", req.Instance.InstancePath, err)
	}
	decision.Activation = &activation
	decision.Action = templateInstanceLifecycleActionPreviewCreate
	route := plan.ReceiverRoute(req.Instance.InstancePath, req.Instance.EntityID)
	return templateInstanceLifecycleMaterialization(plan, []events.RouteIdentity{route}), decision, true, nil
}

func instanceKeyMaterialForTemplateLifecycle(evt events.Event, plan runtimepinrouting.ConnectRoutePlan, values map[string]string) (runtimepinrouting.ConnectRoutePlanInstanceKeyMaterial, runtimepinrouting.ConnectRoutePlanFailure) {
	if plan.InstanceKey() != nil && plan.InstanceKey().RequiresDeliveryProjection() {
		return runtimepinrouting.EventSourcedInstanceKeyMaterialForConnectRoutePlan(plan, evt.ID())
	}
	return runtimepinrouting.InstanceKeyMaterialForConnectRoutePlan(plan, runtimepinrouting.AdmitConnectRouteMatchValues(values))
}

func (o templateInstanceLifecycleOwner) resolveInstanceContract(plan runtimepinrouting.ConnectRoutePlan, material runtimepinrouting.ConnectRoutePlanInstanceKeyMaterial) (runtimecontracts.TemplateInstanceContract, runtimepinrouting.ConnectRoutePlanFailure) {
	bundle, ok := semanticview.Bundle(o.source)
	if !ok || bundle == nil {
		return runtimecontracts.TemplateInstanceContract{}, runtimepinrouting.ConnectFailureLifecycleUnavailable
	}
	instance, err := plan.ReceiverTemplate(bundle)
	if err != nil {
		return runtimecontracts.TemplateInstanceContract{}, runtimepinrouting.ConnectFailureLifecycleUnavailable
	}
	if _, err := instance.CanonicalKeyMaterial(material.CanonicalValues()); err != nil {
		return runtimecontracts.TemplateInstanceContract{}, runtimepinrouting.ConnectFailureInstanceSourceValueMissing
	}
	return instance, 0
}

func (o templateInstanceLifecycleOwner) activationRequest(evt events.Event, plan runtimepinrouting.ConnectRoutePlan, instanceContract runtimecontracts.TemplateInstanceContract, keyMaterial []runtimecontracts.TemplateInstanceKeyValue) (runtimepipeline.FlowInstanceActivationRequest, TemplateInstanceLifecycleDecision, error) {
	instanceID := templateInstanceLifecycleInstanceID(plan, keyMaterial)
	if instanceID == "" {
		return runtimepipeline.FlowInstanceActivationRequest{}, TemplateInstanceLifecycleDecision{}, fmt.Errorf("receiver instance source value is missing")
	}
	instance := plan.DeriveReceiverIdentity(o.source, instanceID)
	instance.ParentRoute = templateInstanceLifecycleParentRoute(evt, plan)
	instance.ParentEntityID = instance.ParentRoute.EntityID
	var payload map[string]any
	if err := canonicaljson.DecodePreservingNumberLexemes(evt.Payload(), &payload); err != nil {
		return runtimepipeline.FlowInstanceActivationRequest{}, TemplateInstanceLifecycleDecision{}, fmt.Errorf("receiver initialization payload: %w", err)
	}
	config, err := plan.ReceiverInitializationConfig(o.source, payload, evt.ID())
	if err != nil {
		return runtimepipeline.FlowInstanceActivationRequest{}, TemplateInstanceLifecycleDecision{}, err
	}
	fields := map[string]any{instanceContract.Field.Path(): config[instanceContract.Field.Path()]}
	bookkeeping := map[string]any{"last_source_event": strings.TrimSpace(evt.ID())}
	decision := TemplateInstanceLifecycleDecision{
		Action:        templateInstanceLifecycleActionCreated,
		InstanceID:    instance.InstanceID,
		InstancePath:  instance.InstancePath,
		EntityID:      instance.EntityID,
		KeyDigest:     plan.ReceiverKeyDigest(keyMaterial),
		KeyMaterial:   append([]runtimecontracts.TemplateInstanceKeyValue{}, keyMaterial...),
		SourceEventID: strings.TrimSpace(evt.ID()),
		receiver:      plan.ReceiverEndpoint(),
		configuration: config,
	}
	return runtimepipeline.FlowInstanceActivationRequest{
		ContractBundle: o.source,
		Instance:       instance,
		Config:         config,
		Fields:         fields,
		Bookkeeping:    bookkeeping,
		TriggerEvent:   evt,
	}, decision, nil
}

func templateInstanceLifecycleMaterialization(plan runtimepinrouting.ConnectRoutePlan, routes []events.RouteIdentity) runtimepinrouting.ConnectRoutePlanMaterialization {
	routes = templateInstanceLifecycleRoutes(routes)
	if len(routes) == 0 {
		return runtimepinrouting.ConnectRoutePlanMaterialization{Failure: runtimepinrouting.ConnectFailureTargetUnresolved}
	}
	if len(routes) > 1 {
		return runtimepinrouting.ConnectRoutePlanMaterialization{Failure: runtimepinrouting.ConnectFailureTargetAmbiguous}
	}
	switch plan.TargetKind() {
	case runtimepinrouting.ConnectTargetKindTarget:
		return runtimepinrouting.ConnectRoutePlanMaterialization{Target: routes[0]}
	case runtimepinrouting.ConnectTargetKindTargetSet:
		return runtimepinrouting.ConnectRoutePlanMaterialization{TargetSet: routes}
	default:
		return runtimepinrouting.ConnectRoutePlanMaterialization{Failure: runtimepinrouting.ConnectFailureDeliveryTopologyInvalid}
	}
}

func templateInstanceLifecycleRoutes(in []events.RouteIdentity) []events.RouteIdentity {
	if len(in) == 0 {
		return nil
	}
	out := make([]events.RouteIdentity, 0, len(in))
	seen := map[events.RouteIdentity]struct{}{}
	for _, route := range in {
		route = route.Normalized()
		if route.Empty() {
			continue
		}
		if _, ok := seen[route]; ok {
			continue
		}
		seen[route] = struct{}{}
		out = append(out, route)
	}
	return out
}

func (o templateInstanceLifecycleOwner) decision(plan runtimepinrouting.ConnectRoutePlan, evt events.Event, keyMaterial []runtimecontracts.TemplateInstanceKeyValue, route events.RouteIdentity, action TemplateInstanceLifecycleAction) TemplateInstanceLifecycleDecision {
	return TemplateInstanceLifecycleDecision{
		Action:        action,
		InstanceID:    runtimeflowidentity.LogicalInstanceID(route.FlowInstance),
		InstancePath:  strings.Trim(strings.TrimSpace(route.FlowInstance), "/"),
		EntityID:      strings.TrimSpace(route.EntityID),
		KeyDigest:     plan.ReceiverKeyDigest(keyMaterial),
		KeyMaterial:   append([]runtimecontracts.TemplateInstanceKeyValue{}, keyMaterial...),
		SourceEventID: strings.TrimSpace(evt.ID()),
		receiver:      plan.ReceiverEndpoint(),
	}
}

func templateInstanceLifecycleExistingAction(mode runtimecontracts.FlowInputResolutionMode) TemplateInstanceLifecycleAction {
	if mode == runtimecontracts.FlowInputResolutionModeSelectOrCreate {
		return templateInstanceLifecycleActionReused
	}
	return templateInstanceLifecycleActionSelectedExisting
}

func templateInstanceLifecycleMatchIsRoutable(routeTable *RouteTable, runID string, plan runtimepinrouting.ConnectRoutePlan, target events.RouteIdentity) bool {
	if routeTable == nil || strings.TrimSpace(runID) == "" {
		return false
	}
	target = target.Normalized()
	if target.Empty() {
		return false
	}
	return len(routeTable.evaluateConnectPlan(runID, plan, []events.RouteIdentity{target}).Recipients()) > 0
}

func templateInstanceLifecycleParentRoute(evt events.Event, plan runtimepinrouting.ConnectRoutePlan) runtimeflowidentity.ParentRoute {
	sourceEvent, err := runtimepinrouting.SourceEventFromEvent(evt)
	if err != nil {
		return runtimeflowidentity.ParentRoute{}
	}
	return plan.SourceParentRoute(sourceEvent)
}

func templateInstanceLifecycleInstanceID(plan runtimepinrouting.ConnectRoutePlan, keyMaterial []runtimecontracts.TemplateInstanceKeyValue) string {
	digest := plan.ReceiverKeyDigest(keyMaterial)
	if digest == "" {
		return ""
	}
	if len(digest) > 24 {
		digest = digest[:24]
	}
	return "ti-" + digest
}

func templateInstanceLifecycleKeyMaterialDetail(keyMaterial []runtimecontracts.TemplateInstanceKeyValue) []map[string]string {
	if len(keyMaterial) == 0 {
		return nil
	}
	out := make([]map[string]string, 0, len(keyMaterial))
	for _, key := range keyMaterial {
		field := key.Field.Path()
		value := strings.TrimSpace(key.Value)
		if field == "" || value == "" {
			continue
		}
		out = append(out, map[string]string{"field": field, "value": value})
	}
	return out
}
