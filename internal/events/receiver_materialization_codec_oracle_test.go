package events

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
)

// Frozen pre-optimization codecs. Do not route these through the new private
// helpers: the differential tests must retain independent wire admission.
type receiverInitializationOracle ReceiverInitialization

type receiverMaterializationRecordOracle struct {
	Initialization *receiverInitializationOracle `json:"initialization"`
	Dependency     json.RawMessage               `json:"dependency"`
}

func (s *receiverInitializationOracle) UnmarshalJSON(raw []byte) error {
	decoded, err := canonicaljson.Decode(raw)
	if err != nil {
		return err
	}
	object, ok := decoded.ObjectMap()
	if !ok {
		return fmt.Errorf("receiver initialization requires an object")
	}
	var wire receiverInitializationWire
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&wire); err != nil {
		return err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("receiver initialization requires one object")
	}
	value := ReceiverInitialization{runID: wire.RunID, eventID: wire.EventID, target: wire.Target}
	switch wire.Kind {
	case "flow_lifecycle":
		if _, present := object["node"]; present {
			return fmt.Errorf("flow initialization forbids node evidence")
		}
		value.kind = receiverInitializationFlow
	case "node_delivery":
		value.kind = receiverInitializationNode
	default:
		return fmt.Errorf("unknown receiver initialization supplier %q", wire.Kind)
	}
	if wire.Node != nil {
		value.node = *wire.Node
	}
	if err := value.validate(); err != nil {
		return err
	}
	*s = receiverInitializationOracle(value)
	return nil
}

func restoreReceiverMaterializationRecordOracle(route DeliveryRoute, raw []byte) (DeliveryRoute, error) {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		if !route.Initialization.Empty() || !route.Materialization.Empty() {
			return DeliveryRoute{}, fmt.Errorf("cannot erase receiver initialization record")
		}
		return route, nil
	}
	if _, err := canonicaljson.Decode(raw); err != nil {
		return DeliveryRoute{}, err
	}
	var record receiverMaterializationRecordOracle
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&record); err != nil {
		return DeliveryRoute{}, err
	}
	if record.Initialization == nil || len(record.Dependency) == 0 {
		return DeliveryRoute{}, fmt.Errorf("receiver materialization record requires supplier and dependency fields")
	}
	if !route.Initialization.Empty() && !route.Initialization.Equal(ReceiverInitialization(*record.Initialization)) {
		return DeliveryRoute{}, fmt.Errorf("cannot replace admitted receiver initialization")
	}
	route.Initialization = ReceiverInitialization(*record.Initialization)
	if err := route.Initialization.ValidateRoute(route); err != nil {
		return DeliveryRoute{}, err
	}
	return restoreDeliveryMaterializationOracle(route, record.Dependency)
}

func restoreDeliveryMaterializationOracle(route DeliveryRoute, raw []byte) (DeliveryRoute, error) {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		if !route.Materialization.Empty() {
			return DeliveryRoute{}, fmt.Errorf("cannot erase a receiver dependency")
		}
		return route, nil
	}
	var wire receiverMaterializationWire
	if _, err := canonicaljson.Decode(raw); err != nil {
		return DeliveryRoute{}, fmt.Errorf("receiver dependency JSON: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil {
		return DeliveryRoute{}, fmt.Errorf("decode receiver dependency: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return DeliveryRoute{}, fmt.Errorf("receiver dependency must be one object")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || len(fields) != 6 {
		return DeliveryRoute{}, fmt.Errorf("receiver dependency requires every binding field")
	}
	for _, field := range []string{"run_id", "event_id", "routing_source", "target", "materializer_route_identity", "dependent_route_identities"} {
		if value := fields[field]; len(value) == 0 || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return DeliveryRoute{}, fmt.Errorf("receiver dependency missing %s", field)
		}
	}
	if canonicalMaterializationUUID(wire.RunID) != nil || canonicalMaterializationUUID(wire.EventID) != nil || !wire.Target.MaterializingEntity() || wire.Target.Validate() != nil {
		return DeliveryRoute{}, fmt.Errorf("receiver dependency has invalid publication or future target")
	}
	node, err := ParseDeliveryRouteIdentity(wire.Materializer)
	if err != nil || EncodeDeliveryRouteIdentity(node) != wire.Materializer {
		return DeliveryRoute{}, fmt.Errorf("receiver dependency materializer identity is invalid")
	}
	if len(wire.Dependents) == 0 {
		return DeliveryRoute{}, fmt.Errorf("receiver dependency has no agents")
	}
	ids := make([]DeliveryRouteIdentity, len(wire.Dependents))
	for i, value := range wire.Dependents {
		id, err := ParseDeliveryRouteIdentity(value)
		if err != nil || EncodeDeliveryRouteIdentity(id) != value || id == node || (i > 0 && value <= wire.Dependents[i-1]) {
			return DeliveryRoute{}, fmt.Errorf("receiver dependency agent bindings are invalid or noncanonical")
		}
		ids[i] = id
	}
	plan := ReceiverMaterializationPlan{runID: wire.RunID, eventID: wire.EventID, source: wire.Source, target: wire.Target, materializer: node, dependents: ids}
	return plan.BindDependent(route)
}
