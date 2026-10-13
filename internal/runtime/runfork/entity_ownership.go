package runfork

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
)

type EntityIdentity struct {
	EntityID     string
	FlowInstance string
}

type EntityProjection struct {
	Source EntityIdentity
	Fork   EntityIdentity
}

// ProjectExecutionRoute preserves declaration scope and only replaces the exact
// admitted root execution coordinate. Entity and producer ownership are separate.
func ProjectExecutionRoute(sourceRunID, forkRunID, flowID string, route flowidentity.Route) (flowidentity.Route, error) {
	if sourceRunID == "" || forkRunID == "" || sourceRunID == forkRunID || !route.Valid() || route.ScopeKey != flowID {
		return route, fmt.Errorf("fork execution route requires exact runs and declaration scope")
	}
	if flowID != "." {
		if route.InstancePath == sourceRunID || route.InstancePath == forkRunID {
			return route, fmt.Errorf("non-root execution route cannot use root ownership")
		}
		return route, nil
	}
	if (route.InstancePath != sourceRunID && route.InstancePath != forkRunID) || route.InstanceID != route.InstancePath {
		return route, fmt.Errorf("root execution route contradicts the admitted runs")
	}
	route.InstanceID, route.InstancePath = forkRunID, forkRunID
	return route, nil
}

// ProjectProducerOwnership consumes typed source authority without interpreting
// receiver state or immutable source lineage.
func ProjectProducerOwnership(sourceRunID, forkRunID string, source events.RoutingSource) (events.RoutingSource, error) {
	if sourceRunID == "" || forkRunID == "" || sourceRunID == forkRunID {
		return source, fmt.Errorf("producer projection requires distinct source and child runs")
	}
	if source.Empty() {
		return source, nil
	}
	route := source.Route()
	if source.Kind() == events.RoutingSourceStaticFlow && route.FlowID == "." {
		if route.EntityID != "" && route.EntityID != route.FlowInstance {
			return source, fmt.Errorf("root execution producer entity contradicts its exact run coordinate")
		}
		projected, err := ProjectExecutionRoute(sourceRunID, forkRunID, ".", flowidentity.StoredRoute(".", route.FlowInstance, route.FlowInstance))
		if err != nil {
			return source, err
		}
		route.FlowInstance = projected.InstancePath
		if route.EntityID != "" {
			route.EntityID = forkRunID
		}
		return events.RestoreRoutingSource(source.Kind().StorageCode(), route, source.Authority().StorageCode())
	}
	root := rootProducerSource(source, sourceRunID, forkRunID)
	if root {
		if route.EntityID != sourceRunID && route.EntityID != forkRunID {
			return source, fmt.Errorf("root producer belongs to neither admitted run")
		}
		route.EntityID = forkRunID
		return events.RestoreRoutingSource(source.Kind().StorageCode(), route, source.Authority().StorageCode())
	}
	if route.EntityID == sourceRunID || route.EntityID == forkRunID || route.FlowInstance == sourceRunID || route.FlowInstance == forkRunID {
		return source, fmt.Errorf("non-root producer cannot own the root entity")
	}
	return source, nil
}

func rootProducerSource(source events.RoutingSource, sourceRunID, forkRunID string) bool {
	route := source.Route()
	return source.Kind() == events.RoutingSourceRoot ||
		(source.Kind() == events.RoutingSourceStaticFlow && route.FlowID == ".") ||
		(source.Kind() == events.RoutingSourceExternalIngress && (route.EntityID == sourceRunID || route.EntityID == forkRunID))
}

// ProjectEntityOwnership changes only the canonical root coordinate. A copied
// non-root entity retains its owner; selected recipients cannot rehome it.
func ProjectEntityOwnership(sourceRunID, forkRunID, entityID, flowInstance string) (EntityProjection, error) {
	sourceRunID = strings.TrimSpace(sourceRunID)
	forkRunID = strings.TrimSpace(forkRunID)
	identity := EntityIdentity{
		EntityID: strings.TrimSpace(entityID), FlowInstance: strings.Trim(strings.TrimSpace(flowInstance), "/"),
	}
	if sourceRunID == "" || forkRunID == "" || sourceRunID == forkRunID || identity.EntityID == "" || identity.FlowInstance == "" {
		return EntityProjection{}, fmt.Errorf("fork entity identity requires distinct source/fork runs, entity, and flow instance")
	}
	projection := EntityProjection{Source: identity, Fork: identity}
	if identity.EntityID != sourceRunID {
		if identity.FlowInstance == sourceRunID {
			return EntityProjection{}, fmt.Errorf("fork source entity %s uses root flow_instance %s without canonical root entity identity", identity.EntityID, sourceRunID)
		}
		return projection, nil
	}
	if identity.FlowInstance != sourceRunID {
		return EntityProjection{}, fmt.Errorf("fork root entity %s has flow_instance %q; want source run identity", identity.EntityID, identity.FlowInstance)
	}
	projection.Fork = EntityIdentity{EntityID: forkRunID, FlowInstance: forkRunID}
	return projection, nil
}

// ProjectParentRoute preserves recorded construction context, remapping only
// the canonical root through the same owner as entity materialization.
func ProjectParentRoute(sourceRunID, forkRunID string, parent flowidentity.ParentRoute) (flowidentity.ParentRoute, error) {
	if parent.Empty() {
		return parent, nil
	}
	if !parent.Complete() || parent != parent.Normalized() {
		return flowidentity.ParentRoute{}, fmt.Errorf("fork parent projection requires exact recorded construction context")
	}
	projected, err := ProjectEntityOwnership(sourceRunID, forkRunID, parent.EntityID, parent.FlowInstance)
	if err != nil {
		return flowidentity.ParentRoute{}, err
	}
	parent.FlowInstance, parent.EntityID = projected.Fork.FlowInstance, projected.Fork.EntityID
	return parent, nil
}

// ProjectConstructionIdentity retains the recorded declaration and ancestry.
// Only the admitted root coordinates change; a path tail is not an identity.
func ProjectConstructionIdentity(sourceRunID, forkRunID string, source flowidentity.Instance) (flowidentity.Instance, error) {
	if !source.HasStoredPath || source.TemplateID == "" || source.ScopeKey == "" || source.InstanceID == "" ||
		source.EntityID == "" || !source.Route().Valid() || source.Route().InstanceID != source.InstanceID {
		return flowidentity.Instance{}, fmt.Errorf("fork construction requires complete recorded identity")
	}
	owner, err := ProjectEntityOwnership(sourceRunID, forkRunID, source.EntityID, source.InstancePath)
	if err != nil {
		return flowidentity.Instance{}, err
	}
	route, err := ProjectExecutionRoute(sourceRunID, forkRunID, source.ScopeKey, source.Route())
	if err != nil {
		return flowidentity.Instance{}, err
	}
	if route.InstancePath != owner.Fork.FlowInstance {
		return flowidentity.Instance{}, fmt.Errorf("fork construction route contradicts admitted ownership")
	}
	parent, err := ProjectParentRoute(sourceRunID, forkRunID, source.ParentRoute)
	if err != nil {
		return flowidentity.Instance{}, err
	}
	if source.ParentEntityID != source.ParentRoute.EntityID {
		return flowidentity.Instance{}, fmt.Errorf("fork construction parent contradicts recorded ancestry")
	}
	child := source
	child.EntityID, child.InstanceID, child.InstancePath = owner.Fork.EntityID, route.InstanceID, route.InstancePath
	child.ParentRoute, child.ParentEntityID = parent, parent.EntityID
	return child, nil
}

// ProjectSelectedContractSourceEvent is shared by persistent preparation and the
// runtime container. It projects producer coordinates, never receiver state.
// Calling it on an already child-projected event validates without reminting.
func ProjectSelectedContractSourceEvent(sourceRunID, forkRunID string, event RunForkSelectedContractSourceEvent) (RunForkSelectedContractSourceEvent, error) {
	sourceRunID = strings.TrimSpace(sourceRunID)
	forkRunID = strings.TrimSpace(forkRunID)
	if sourceRunID == "" || forkRunID == "" || sourceRunID == forkRunID {
		return event, fmt.Errorf("selected-contract source projection requires distinct source and child runs")
	}
	if event.SourceEventID == "" {
		return event, fmt.Errorf("selected-contract source event requires exact event identity")
	}
	if event.RoutingSource.Empty() {
		if event.EventName == RunForkSelectedContractPlatformActivityEvent {
			return event, fmt.Errorf("selected-contract activity requires persisted producer routing authority")
		}
		return event, nil
	}
	source, err := ProjectProducerOwnership(sourceRunID, forkRunID, event.RoutingSource)
	if err != nil {
		return event, err
	}
	event.RoutingSource = source
	route := source.Route()
	root := rootProducerSource(source, sourceRunID, forkRunID)
	if event.EventName != RunForkSelectedContractPlatformActivityEvent {
		return event, nil
	}
	// Activity entity_id/flow_instance are execution coordinates, not arbitrary
	// business payload. Preserve every other payload field and its numeric bytes.
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(event.Payload, &payload); err != nil || payload == nil {
		return event, fmt.Errorf("selected-contract activity %s requires an object payload", event.SourceEventID)
	}
	var activityFlow string
	if raw, ok := payload["flow_instance"]; !ok || json.Unmarshal(raw, &activityFlow) != nil {
		return event, fmt.Errorf("selected-contract activity %s requires an exact flow_instance", event.SourceEventID)
	}
	wantFlow := route.FlowInstance
	if root {
		wantFlow = forkRunID
		if activityFlow == sourceRunID {
			activityFlow = forkRunID
		}
	}
	if wantFlow == "" || activityFlow != wantFlow {
		return event, fmt.Errorf("selected-contract activity %s contradicts its producer flow", event.SourceEventID)
	}
	var activityEntity string
	if raw, ok := payload["entity_id"]; !ok || json.Unmarshal(raw, &activityEntity) != nil || activityEntity == "" {
		return event, fmt.Errorf("selected-contract activity %s requires an exact entity_id", event.SourceEventID)
	}
	if root && activityEntity == sourceRunID {
		activityEntity = route.EntityID
	}
	if activityEntity != route.EntityID {
		return event, fmt.Errorf("selected-contract activity %s contradicts its producer entity", event.SourceEventID)
	}
	payload["entity_id"], _ = json.Marshal(activityEntity)
	payload["flow_instance"], _ = json.Marshal(activityFlow)
	raw, err := json.Marshal(payload)
	if err != nil {
		return event, err
	}
	event.Payload = raw
	return event, nil
}
