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
	root := source.Kind() == events.RoutingSourceRoot ||
		(source.Kind() == events.RoutingSourceExternalIngress && (route.EntityID == sourceRunID || route.EntityID == forkRunID))
	if root {
		if route.EntityID != sourceRunID && route.EntityID != forkRunID {
			return source, fmt.Errorf("root producer belongs to neither admitted run")
		}
		route.EntityID = forkRunID
		return events.RestoreRoutingSource(source.Kind().StorageCode(), route, source.Authority().StorageCode())
	}
	if route.EntityID == sourceRunID || route.EntityID == forkRunID {
		return source, fmt.Errorf("non-root producer cannot own the root entity")
	}
	return source, nil
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
	root := source.Kind() == events.RoutingSourceRoot ||
		(source.Kind() == events.RoutingSourceExternalIngress && (route.EntityID == sourceRunID || route.EntityID == forkRunID))
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
