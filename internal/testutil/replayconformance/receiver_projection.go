package replayconformance

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/division-sh/swarm/internal/events"
)

// Generated event UUIDs differ across clean executions. Validate the complete
// supplier/dependency relation first, then project its publication references
// to the same causal key used for the containing event. No business field or
// ownership evidence is discarded by this comparison-only projection.
func projectReceiverRoutes(event events.Event, routes []events.DeliveryRoute, eventKey string) ([]json.RawMessage, error) {
	if err := events.ValidateReceiverMaterializations(event, routes); err != nil {
		return nil, err
	}
	objects := make([]map[string]json.RawMessage, len(routes))
	references := make(map[string]string, len(routes))
	for i, route := range routes {
		base := route
		base.Materialization = events.ReceiverMaterializationPlan{}
		identity, err := base.Identity()
		if err != nil {
			return nil, err
		}
		raw, err := json.Marshal(base)
		if err != nil {
			return nil, err
		}
		var object map[string]json.RawMessage
		if err := json.Unmarshal(raw, &object); err != nil {
			return nil, err
		}
		if !route.Initialization.Empty() {
			var supplier map[string]json.RawMessage
			if err := json.Unmarshal(object["receiver_initialization"], &supplier); err != nil {
				return nil, err
			}
			supplier["event_id"], _ = json.Marshal(eventKey)
			object["receiver_initialization"], err = json.Marshal(supplier)
			if err != nil {
				return nil, err
			}
		}
		canonical, err := json.Marshal(object)
		if err != nil {
			return nil, err
		}
		sum := sha256.Sum256(canonical)
		references[events.EncodeDeliveryRouteIdentity(identity)] = "route:" + hex.EncodeToString(sum[:])
		objects[i] = object
	}
	result := make([]json.RawMessage, len(routes))
	for i, route := range routes {
		if !route.Materialization.Empty() {
			raw, err := json.Marshal(route.Materialization)
			if err != nil {
				return nil, err
			}
			var dependency map[string]json.RawMessage
			if err := json.Unmarshal(raw, &dependency); err != nil {
				return nil, err
			}
			materializer := references[events.EncodeDeliveryRouteIdentity(route.Materialization.Materializer())]
			if materializer == "" {
				return nil, fmt.Errorf("receiver projection has no exact materializer")
			}
			dependents := make([]string, 0)
			for _, identity := range route.Materialization.Dependents() {
				key := references[events.EncodeDeliveryRouteIdentity(identity)]
				if key == "" {
					return nil, fmt.Errorf("receiver projection has no exact dependent")
				}
				dependents = append(dependents, key)
			}
			sort.Strings(dependents)
			dependency["event_id"], _ = json.Marshal(eventKey)
			dependency["materializer_route_identity"], _ = json.Marshal(materializer)
			dependency["dependent_route_identities"], _ = json.Marshal(dependents)
			objects[i]["receiver_materialization_plan"], err = json.Marshal(dependency)
			if err != nil {
				return nil, err
			}
		}
		raw, err := json.Marshal(objects[i])
		if err != nil {
			return nil, err
		}
		result[i] = raw
	}
	return result, nil
}
