package bus

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const (
	templateInstanceLifecycleActionCreated          = "created"
	templateInstanceLifecycleActionPreviewCreate    = "would_create"
	templateInstanceLifecycleActionReused           = "reused"
	templateInstanceLifecycleActionSelectedExisting = "selected_existing"
)

type templateInstanceLifecyclePreviewKey struct{}

type templateInstanceLifecycleOwner struct {
	source          semanticview.Source
	loadDescriptors connectRoutePlanDescriptorLoader
	activate        runtimepipeline.FlowInstanceActivator
}

type TemplateInstanceLifecycleDecision struct {
	Action        string
	ReceiverFlow  string
	InstanceID    string
	InstancePath  string
	EntityID      string
	KeyDigest     string
	KeyMaterial   []runtimecontracts.TemplateInstanceKeyValue
	OnMissing     string
	OnConflict    string
	SourceEventID string
}

func newTemplateInstanceLifecycleOwner(source semanticview.Source, loadDescriptors connectRoutePlanDescriptorLoader, activate runtimepipeline.FlowInstanceActivator) templateInstanceLifecycleOwner {
	return templateInstanceLifecycleOwner{
		source:          source,
		loadDescriptors: loadDescriptors,
		activate:        activate,
	}
}

func (d TemplateInstanceLifecycleDecision) Empty() bool {
	return strings.TrimSpace(d.Action) == ""
}

func (d TemplateInstanceLifecycleDecision) Detail() map[string]any {
	if d.Empty() {
		return nil
	}
	return map[string]any{
		"action":          strings.TrimSpace(d.Action),
		"receiver_flow":   strings.TrimSpace(d.ReceiverFlow),
		"instance_id":     strings.TrimSpace(d.InstanceID),
		"instance_path":   strings.Trim(strings.TrimSpace(d.InstancePath), "/"),
		"entity_id":       strings.TrimSpace(d.EntityID),
		"key_digest":      strings.TrimSpace(d.KeyDigest),
		"key_material":    templateInstanceLifecycleKeyMaterialDetail(d.KeyMaterial),
		"on_missing":      strings.TrimSpace(d.OnMissing),
		"on_conflict":     strings.TrimSpace(d.OnConflict),
		"source_event_id": strings.TrimSpace(d.SourceEventID),
	}
}

func (d TemplateInstanceLifecycleDecision) Route() runtimeflowidentity.Route {
	scope := runtimeflowidentity.SemanticScopeFromInstancePath(d.InstancePath)
	if scope == "" {
		scope = strings.TrimSpace(d.ReceiverFlow)
	}
	return runtimeflowidentity.StoredRoute(scope, d.InstanceID, d.InstancePath)
}

func (d TemplateInstanceLifecycleDecision) ActivationVariables() map[string]string {
	out := map[string]string{}
	for _, key := range d.KeyMaterial {
		field := strings.TrimSpace(key.Field)
		value := strings.TrimSpace(key.Value)
		if field != "" && value != "" {
			out[field] = value
		}
	}
	setTemplateInstanceLifecycleVariable(out, "entity_id", d.EntityID)
	setTemplateInstanceLifecycleVariable(out, "instance_id", d.InstanceID)
	setTemplateInstanceLifecycleVariable(out, "template_id", d.ReceiverFlow)
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
	if plan.ResolutionKind != runtimepinrouting.ConnectResolutionInstanceKey || plan.InstanceKey == nil {
		return runtimepinrouting.ConnectRoutePlanMaterialization{}, TemplateInstanceLifecycleDecision{}, false, nil
	}
	material, failure := instanceKeyMaterialForTemplateLifecycle(evt, plan, values)
	if failure != "" {
		return runtimepinrouting.ConnectRoutePlanMaterialization{Failure: failure}, TemplateInstanceLifecycleDecision{}, true, nil
	}
	instanceContract, failure := o.resolveInstanceContract(plan, material)
	if failure != "" {
		return runtimepinrouting.ConnectRoutePlanMaterialization{Failure: failure}, TemplateInstanceLifecycleDecision{}, true, nil
	}
	keyMaterial := append([]runtimecontracts.TemplateInstanceKeyValue{}, material.Keys...)
	if canonical, err := instanceContract.CanonicalKeyMaterial(material.Values); err == nil {
		keyMaterial = canonical
	} else {
		return runtimepinrouting.ConnectRoutePlanMaterialization{Failure: runtimepinrouting.ConnectFailureAddressValueMissing}, TemplateInstanceLifecycleDecision{}, true, nil
	}
	onMissing := strings.TrimSpace(instanceContract.OnMissing)
	onConflict := strings.TrimSpace(instanceContract.OnConflict)
	if plan.InstanceKey != nil {
		switch strings.TrimSpace(plan.InstanceKey.Mode) {
		case runtimecontracts.FlowInputResolutionModeCreate:
			onMissing = "create"
			onConflict = "reuse"
		case runtimecontracts.FlowInputResolutionModeSelect:
			onMissing = "reject"
			onConflict = "reject"
		}
	}
	matches := runtimepinrouting.InstanceKeyDescriptorRoutesForConnectRoutePlan(plan, keyMaterial, descriptors)
	if len(matches) > 1 {
		return runtimepinrouting.ConnectRoutePlanMaterialization{Failure: runtimepinrouting.ConnectFailureTargetAmbiguous}, TemplateInstanceLifecycleDecision{}, true, nil
	}
	if len(matches) == 1 {
		if onMissing == "create" && onConflict == "reject" {
			return runtimepinrouting.ConnectRoutePlanMaterialization{Failure: runtimepinrouting.ConnectFailureInstanceConflict}, TemplateInstanceLifecycleDecision{}, true, nil
		}
		return templateInstanceLifecycleMaterialization(plan, matches), o.decision(plan, evt, keyMaterial, onMissing, onConflict, matches[0], templateInstanceLifecycleExistingAction(onMissing, onConflict)), true, nil
	}
	if onMissing != "create" {
		return runtimepinrouting.ConnectRoutePlanMaterialization{Failure: runtimepinrouting.ConnectFailureTargetUnresolved}, TemplateInstanceLifecycleDecision{}, true, nil
	}
	if o.activate == nil {
		return runtimepinrouting.ConnectRoutePlanMaterialization{Failure: runtimepinrouting.ConnectFailureLifecycleUnavailable}, TemplateInstanceLifecycleDecision{}, true, nil
	}
	if templateInstanceLifecyclePreview(ctx) {
		req, decision, failure := o.activationRequest(evt, plan, instanceContract, keyMaterial, onMissing, onConflict)
		if failure != "" {
			return runtimepinrouting.ConnectRoutePlanMaterialization{Failure: failure}, TemplateInstanceLifecycleDecision{}, true, nil
		}
		decision.Action = templateInstanceLifecycleActionPreviewCreate
		route := events.RouteIdentity{FlowID: plan.Receiver.FlowID, FlowInstance: req.Instance.InstancePath, EntityID: req.Instance.EntityID}.Normalized()
		return templateInstanceLifecycleMaterialization(plan, []events.RouteIdentity{route}), decision, true, nil
	}
	req, decision, failure := o.activationRequest(evt, plan, instanceContract, keyMaterial, onMissing, onConflict)
	if failure != "" {
		return runtimepinrouting.ConnectRoutePlanMaterialization{Failure: failure}, TemplateInstanceLifecycleDecision{}, true, nil
	}
	if err := o.activate(ctx, req); err != nil {
		return runtimepinrouting.ConnectRoutePlanMaterialization{}, TemplateInstanceLifecycleDecision{}, true, fmt.Errorf("activate connect-time template instance %s: %w", req.Instance.InstancePath, err)
	}
	refreshed, err := o.reloadDescriptors(ctx)
	if err != nil {
		return runtimepinrouting.ConnectRoutePlanMaterialization{}, TemplateInstanceLifecycleDecision{}, true, err
	}
	matches = runtimepinrouting.InstanceKeyDescriptorRoutesForConnectRoutePlan(plan, keyMaterial, refreshed)
	if len(matches) == 0 {
		return runtimepinrouting.ConnectRoutePlanMaterialization{Failure: runtimepinrouting.ConnectFailureTargetUnresolved}, decision, true, nil
	}
	if len(matches) > 1 {
		return runtimepinrouting.ConnectRoutePlanMaterialization{Failure: runtimepinrouting.ConnectFailureTargetAmbiguous}, decision, true, nil
	}
	decision.InstancePath = matches[0].FlowInstance
	decision.EntityID = matches[0].EntityID
	return templateInstanceLifecycleMaterialization(plan, matches), decision, true, nil
}

func instanceKeyMaterialForTemplateLifecycle(evt events.Event, plan runtimepinrouting.ConnectRoutePlan, values map[string]string) (runtimepinrouting.ConnectRoutePlanInstanceKeyMaterial, runtimepinrouting.ConnectRoutePlanFailure) {
	if plan.InstanceKey != nil && strings.TrimSpace(plan.InstanceKey.Mint) != "" {
		return runtimepinrouting.MintedInstanceKeyMaterialForConnectRoutePlan(plan, evt.ID())
	}
	return runtimepinrouting.InstanceKeyMaterialForConnectRoutePlan(plan, values)
}

func (o templateInstanceLifecycleOwner) resolveInstanceContract(plan runtimepinrouting.ConnectRoutePlan, material runtimepinrouting.ConnectRoutePlanInstanceKeyMaterial) (runtimecontracts.TemplateInstanceContract, runtimepinrouting.ConnectRoutePlanFailure) {
	bundle, ok := semanticview.Bundle(o.source)
	if !ok || bundle == nil {
		return runtimecontracts.TemplateInstanceContract{}, runtimepinrouting.ConnectFailureLifecycleUnavailable
	}
	instance, err := bundle.ResolveFlowTemplateInstance(plan.Receiver.FlowID)
	if err != nil {
		return runtimecontracts.TemplateInstanceContract{}, runtimepinrouting.ConnectFailureLifecycleUnavailable
	}
	if _, err := instance.CanonicalKeyMaterial(material.Values); err != nil {
		return runtimecontracts.TemplateInstanceContract{}, runtimepinrouting.ConnectFailureAddressValueMissing
	}
	return instance, ""
}

func (o templateInstanceLifecycleOwner) activationRequest(evt events.Event, plan runtimepinrouting.ConnectRoutePlan, instanceContract runtimecontracts.TemplateInstanceContract, keyMaterial []runtimecontracts.TemplateInstanceKeyValue, onMissing, onConflict string) (runtimepipeline.FlowInstanceActivationRequest, TemplateInstanceLifecycleDecision, runtimepinrouting.ConnectRoutePlanFailure) {
	instanceID := templateInstanceLifecycleInstanceID(plan.Receiver.FlowID, keyMaterial)
	if instanceID == "" {
		return runtimepipeline.FlowInstanceActivationRequest{}, TemplateInstanceLifecycleDecision{}, runtimepinrouting.ConnectFailureAddressValueMissing
	}
	instance := runtimeflowidentity.Derive(o.source, plan.Receiver.FlowID, instanceID)
	instance.ParentRoute = templateInstanceLifecycleParentRoute(evt, plan)
	instance.ParentEntityID = instance.ParentRoute.EntityID
	config := templateInstanceLifecycleKeyMap(keyMaterial)
	metadata := templateInstanceLifecycleKeyMap(keyMaterial)
	metadata["entity_type"] = strings.TrimSpace(instanceContract.PrimaryEntity.EntityType)
	metadata["instance_kind"] = "template"
	metadata["last_source_event"] = strings.TrimSpace(evt.ID())
	config["template_instance_key"] = templateInstanceLifecycleKeyDigest(plan.Receiver.FlowID, keyMaterial)
	config["template_instance_source_event"] = strings.TrimSpace(evt.ID())
	config["template_instance_on_missing"] = onMissing
	config["template_instance_on_conflict"] = onConflict
	decision := TemplateInstanceLifecycleDecision{
		Action:        templateInstanceLifecycleActionCreated,
		ReceiverFlow:  strings.TrimSpace(plan.Receiver.FlowID),
		InstanceID:    instance.InstanceID,
		InstancePath:  instance.InstancePath,
		EntityID:      instance.EntityID,
		KeyDigest:     templateInstanceLifecycleKeyDigest(plan.Receiver.FlowID, keyMaterial),
		KeyMaterial:   append([]runtimecontracts.TemplateInstanceKeyValue{}, keyMaterial...),
		OnMissing:     onMissing,
		OnConflict:    onConflict,
		SourceEventID: strings.TrimSpace(evt.ID()),
	}
	return runtimepipeline.FlowInstanceActivationRequest{
		ContractBundle: o.source,
		Instance:       instance,
		Config:         config,
		Metadata:       metadata,
		TriggerEvent:   evt,
	}, decision, ""
}

func (o templateInstanceLifecycleOwner) reloadDescriptors(ctx context.Context) ([]runtimepinrouting.Descriptor, error) {
	if o.loadDescriptors == nil {
		return nil, nil
	}
	return o.loadDescriptors(ctx)
}

func templateInstanceLifecycleMaterialization(plan runtimepinrouting.ConnectRoutePlan, routes []events.RouteIdentity) runtimepinrouting.ConnectRoutePlanMaterialization {
	routes = templateInstanceLifecycleRoutes(routes)
	if len(routes) == 0 {
		return runtimepinrouting.ConnectRoutePlanMaterialization{Failure: runtimepinrouting.ConnectFailureTargetUnresolved}
	}
	if len(routes) > 1 {
		return runtimepinrouting.ConnectRoutePlanMaterialization{Failure: runtimepinrouting.ConnectFailureTargetAmbiguous}
	}
	switch plan.TargetKind {
	case runtimepinrouting.ConnectTargetKindTarget, runtimepinrouting.ConnectTargetKindReply:
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
	seen := map[string]struct{}{}
	for _, route := range in {
		route = route.Normalized()
		if route.Empty() {
			continue
		}
		key := strings.TrimSpace(route.FlowID) + "\x00" + strings.Trim(route.FlowInstance, "/") + "\x00" + strings.TrimSpace(route.EntityID)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, route)
	}
	return out
}

func (o templateInstanceLifecycleOwner) decision(plan runtimepinrouting.ConnectRoutePlan, evt events.Event, keyMaterial []runtimecontracts.TemplateInstanceKeyValue, onMissing, onConflict string, route events.RouteIdentity, action string) TemplateInstanceLifecycleDecision {
	return TemplateInstanceLifecycleDecision{
		Action:        action,
		ReceiverFlow:  strings.TrimSpace(plan.Receiver.FlowID),
		InstanceID:    runtimeflowidentity.LogicalInstanceID(route.FlowInstance),
		InstancePath:  strings.Trim(strings.TrimSpace(route.FlowInstance), "/"),
		EntityID:      strings.TrimSpace(route.EntityID),
		KeyDigest:     templateInstanceLifecycleKeyDigest(plan.Receiver.FlowID, keyMaterial),
		KeyMaterial:   append([]runtimecontracts.TemplateInstanceKeyValue{}, keyMaterial...),
		OnMissing:     strings.TrimSpace(onMissing),
		OnConflict:    strings.TrimSpace(onConflict),
		SourceEventID: strings.TrimSpace(evt.ID()),
	}
}

func templateInstanceLifecycleExistingAction(onMissing, onConflict string) string {
	if strings.TrimSpace(onMissing) == "create" && strings.TrimSpace(onConflict) == "reuse" {
		return templateInstanceLifecycleActionReused
	}
	return templateInstanceLifecycleActionSelectedExisting
}

func templateInstanceLifecycleParentRoute(evt events.Event, plan runtimepinrouting.ConnectRoutePlan) runtimeflowidentity.ParentRoute {
	envelope := evt.NormalizedEnvelope()
	parent := runtimeflowidentity.ParentRoute{
		FlowID:       strings.TrimSpace(plan.Source.FlowID),
		FlowInstance: strings.Trim(strings.TrimSpace(evt.FlowInstance()), "/"),
		EntityID:     strings.TrimSpace(evt.EntityID()),
	}
	if parent.FlowInstance == "" {
		parent.FlowInstance = strings.Trim(strings.TrimSpace(envelope.Source.FlowInstance), "/")
	}
	if parent.EntityID == "" {
		parent.EntityID = strings.TrimSpace(envelope.Source.EntityID)
	}
	if parent.FlowID == "" {
		parent.FlowID = strings.TrimSpace(envelope.Source.FlowID)
	}
	if parent.FlowInstance == "" {
		parent.FlowInstance = strings.Trim(strings.TrimSpace(plan.Source.FlowPath), "/")
	}
	return parent.Normalized()
}

func templateInstanceLifecycleInstanceID(receiverFlow string, keyMaterial []runtimecontracts.TemplateInstanceKeyValue) string {
	digest := templateInstanceLifecycleKeyDigest(receiverFlow, keyMaterial)
	if digest == "" {
		return ""
	}
	if len(digest) > 24 {
		digest = digest[:24]
	}
	return "ti-" + digest
}

func templateInstanceLifecycleKeyDigest(receiverFlow string, keyMaterial []runtimecontracts.TemplateInstanceKeyValue) string {
	receiverFlow = strings.TrimSpace(receiverFlow)
	if receiverFlow == "" || len(keyMaterial) == 0 {
		return ""
	}
	h := sha256.New()
	_, _ = h.Write([]byte(receiverFlow))
	_, _ = h.Write([]byte{0})
	for _, key := range keyMaterial {
		_, _ = h.Write([]byte(strings.TrimSpace(key.Field)))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(strings.TrimSpace(key.Value)))
		_, _ = h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func templateInstanceLifecycleKeyMap(keyMaterial []runtimecontracts.TemplateInstanceKeyValue) map[string]any {
	out := make(map[string]any, len(keyMaterial))
	for _, key := range keyMaterial {
		field := strings.TrimSpace(key.Field)
		value := strings.TrimSpace(key.Value)
		if field == "" || value == "" {
			continue
		}
		out[field] = value
	}
	return out
}

func templateInstanceLifecycleKeyMaterialDetail(keyMaterial []runtimecontracts.TemplateInstanceKeyValue) []map[string]string {
	if len(keyMaterial) == 0 {
		return nil
	}
	out := make([]map[string]string, 0, len(keyMaterial))
	for _, key := range keyMaterial {
		field := strings.TrimSpace(key.Field)
		value := strings.TrimSpace(key.Value)
		if field == "" || value == "" {
			continue
		}
		out = append(out, map[string]string{"field": field, "value": value})
	}
	return out
}
