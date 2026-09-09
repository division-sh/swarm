package events

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"sort"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/google/uuid"
)

// ReceiverMaterializationPlan binds future ownership to an exact publication
// and its deliveries. It is not evidence that the node has executed, that an
// entity exists, or that an agent has readiness or execution authority.
type ReceiverMaterializationPlan struct {
	runID        string
	eventID      string
	source       RoutingSource
	target       DeliveryTargetOwnership
	materializer DeliveryRouteIdentity
	dependents   []DeliveryRouteIdentity
}

// AdmitReceiverMaterializationPlan consumes the complete admitted route set,
// not a descriptor assembled from a speculative node result. The caller must
// have classified the node's acquisition through the canonical handler owner.
// This owner then requires exact compiled receiver agreement and rejects a
// second materializing node rather than choosing the first one encountered.
func AdmitReceiverMaterializationPlan(event Event, materializer DeliveryRoute, dependents, publication []DeliveryRoute) (ReceiverMaterializationPlan, error) {
	if err := canonicalMaterializationUUID(event.RunID()); err != nil {
		return ReceiverMaterializationPlan{}, fmt.Errorf("receiver materialization run: %w", err)
	}
	if err := canonicalMaterializationUUID(event.ID()); err != nil {
		return ReceiverMaterializationPlan{}, fmt.Errorf("receiver materialization event: %w", err)
	}
	if err := ValidateDeliveryRoutes(publication); err != nil {
		return ReceiverMaterializationPlan{}, fmt.Errorf("receiver materialization publication: %w", err)
	}
	publication = NormalizeDeliveryRoutes(publication)
	materializer = materializer.Normalized()
	if !materializer.Recipient.IsNode() || !materializer.Target.MaterializingEntity() {
		return ReceiverMaterializationPlan{}, fmt.Errorf("receiver materializer requires a node with exact future ownership")
	}
	materializerID, err := materializer.Identity()
	if err != nil {
		return ReceiverMaterializationPlan{}, err
	}
	pin, ok := materializer.ConnectClaim.ReceiverIdentity()
	if !ok || materializer.ConnectClaim.digest == ([sha256.Size]byte{}) || materializer.ConnectClaim.receiverPinDigest == ([sha256.Size]byte{}) {
		return ReceiverMaterializationPlan{}, fmt.Errorf("receiver materializer requires compiled receiver evidence")
	}
	handler, handlerEvent, ok := materializer.ConnectClaim.NodeHandlerOwner()
	if !ok || handler.Key() != materializer.Recipient.ID() {
		return ReceiverMaterializationPlan{}, fmt.Errorf("receiver materializer requires exact node handler evidence")
	}
	if len(dependents) == 0 {
		return ReceiverMaterializationPlan{}, fmt.Errorf("receiver materialization requires dependent agents")
	}
	available := make(map[DeliveryRouteIdentity]struct{}, len(publication))
	for _, route := range publication {
		id, err := route.identity(false)
		if err != nil {
			return ReceiverMaterializationPlan{}, err
		}
		available[id] = struct{}{}
		otherPin, sameReceiver := route.ConnectClaim.ReceiverIdentity()
		if route.Recipient.IsNode() && SameDeliveryTargetOwnership(route.Target, materializer.Target) && sameReceiver && otherPin == pin && id != materializerID {
			return ReceiverMaterializationPlan{}, fmt.Errorf("receiver materialization has ambiguous materializing node deliveries")
		}
	}
	if _, ok := available[materializerID]; !ok {
		return ReceiverMaterializationPlan{}, fmt.Errorf("receiver materializer is not an exact publication delivery")
	}
	ids := make([]DeliveryRouteIdentity, 0, len(dependents))
	seen := make(map[DeliveryRouteIdentity]struct{}, len(dependents))
	for _, dependent := range dependents {
		dependent = dependent.Normalized()
		id, err := dependent.identity(false)
		if err != nil {
			return ReceiverMaterializationPlan{}, err
		}
		if !dependent.Recipient.IsAgent() || !SameDeliveryTargetOwnership(materializer.Target, dependent.Target) {
			return ReceiverMaterializationPlan{}, fmt.Errorf("receiver dependency requires an agent with the exact materializing target")
		}
		if dependent.AgentIdentity.RunID != event.RunID() {
			return ReceiverMaterializationPlan{}, fmt.Errorf("receiver dependency agent belongs to another run")
		}
		_, _, instance, err := dependent.AgentIdentity.ExecutionCoordinates()
		if err != nil {
			return ReceiverMaterializationPlan{}, fmt.Errorf("receiver dependency agent coordinates: %w", err)
		}
		if instance != dependent.Target.Route().FlowInstance {
			return ReceiverMaterializationPlan{}, fmt.Errorf("receiver dependency agent instance contradicts its exact target")
		}
		dependentPin, ok := dependent.ConnectClaim.ReceiverIdentity()
		dependentEvent, hasEvent := dependent.ConnectClaim.ReceiverEvent()
		if !ok || dependentPin != pin || !hasEvent || dependentEvent != handlerEvent || dependent.ConnectClaim.digest == ([sha256.Size]byte{}) {
			return ReceiverMaterializationPlan{}, fmt.Errorf("receiver dependency lacks exact compiled receiver agreement")
		}
		if _, ok := available[id]; !ok {
			return ReceiverMaterializationPlan{}, fmt.Errorf("receiver dependent is not an exact publication delivery")
		}
		if _, duplicate := seen[id]; duplicate {
			return ReceiverMaterializationPlan{}, fmt.Errorf("receiver dependency repeats an agent delivery")
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	for _, route := range publication {
		otherPin, ok := route.ConnectClaim.ReceiverIdentity()
		if !route.Recipient.IsAgent() || !SameDeliveryTargetOwnership(route.Target, materializer.Target) || !ok || otherPin != pin {
			continue
		}
		id, err := route.identity(false)
		if err != nil {
			return ReceiverMaterializationPlan{}, err
		}
		if _, ok := seen[id]; !ok {
			return ReceiverMaterializationPlan{}, fmt.Errorf("receiver materialization omits a dependent publication agent")
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].value < ids[j].value })
	return ReceiverMaterializationPlan{
		runID: event.RunID(), eventID: event.ID(), source: event.RoutingSource(),
		target: materializer.Target, materializer: materializerID, dependents: ids,
	}, nil
}

func (p ReceiverMaterializationPlan) Empty() bool { return p.eventID == "" }

func (p ReceiverMaterializationPlan) RunID() string { return p.runID }

func (p ReceiverMaterializationPlan) EventID() string { return p.eventID }

func (p ReceiverMaterializationPlan) Target() DeliveryTargetOwnership { return p.target }

func (p ReceiverMaterializationPlan) Materializer() DeliveryRouteIdentity { return p.materializer }

func (p ReceiverMaterializationPlan) Dependents() []DeliveryRouteIdentity {
	return append([]DeliveryRouteIdentity(nil), p.dependents...)
}

// ValidatePublication re-admits the exact relation, including all competing
// materializers. A matching event ID or target alone cannot validate a plan.
func (p ReceiverMaterializationPlan) ValidatePublication(event Event, publication []DeliveryRoute) error {
	if p.Empty() || event.ID() != p.eventID || event.RunID() != p.runID || event.RoutingSource() != p.source {
		return fmt.Errorf("receiver materialization publication identity disagrees")
	}
	if err := ValidateDeliveryRoutes(publication); err != nil {
		return err
	}
	publication = NormalizeDeliveryRoutes(publication)
	var materializer DeliveryRoute
	dependents := make([]DeliveryRoute, 0, len(p.dependents))
	wanted := make(map[DeliveryRouteIdentity]struct{}, len(p.dependents))
	for _, id := range p.dependents {
		wanted[id] = struct{}{}
	}
	for _, route := range publication {
		id, err := route.identity(false)
		if err != nil {
			return err
		}
		if id == p.materializer {
			materializer = route
		}
		if _, ok := wanted[id]; ok {
			dependents = append(dependents, route)
		}
	}
	if len(dependents) != len(p.dependents) || !SameDeliveryTargetOwnership(materializer.Target, p.target) {
		return fmt.Errorf("receiver materialization delivery evidence is missing or contradictory")
	}
	admitted, err := AdmitReceiverMaterializationPlan(event, materializer, dependents, publication)
	if err != nil {
		return err
	}
	if !p.Equal(admitted) {
		return fmt.Errorf("receiver materialization plan disagrees with publication")
	}
	return nil
}

// BindDependent attaches only a previously admitted exact relation. It never
// resolves an entity or chooses a materializer from current descriptors.
func (p ReceiverMaterializationPlan) BindDependent(route DeliveryRoute) (DeliveryRoute, error) {
	if err := p.validateDependent(route); err != nil {
		return DeliveryRoute{}, err
	}
	if !route.Materialization.Empty() && !route.Materialization.Equal(p) {
		return DeliveryRoute{}, fmt.Errorf("receiver dependency cannot replace an admitted plan")
	}
	route = route.Normalized()
	route.Materialization = p
	return route, nil
}

func (p ReceiverMaterializationPlan) validateDependent(route DeliveryRoute) error {
	if p.Empty() || !route.Recipient.IsAgent() || route.AgentIdentity.RunID != p.runID || !SameDeliveryTargetOwnership(p.target, route.Target) {
		return fmt.Errorf("receiver dependency contradicts its agent route")
	}
	id, err := route.identity(false)
	if err != nil {
		return err
	}
	for _, dependent := range p.dependents {
		if dependent == id {
			return nil
		}
	}
	return fmt.Errorf("receiver dependency does not bind this exact agent execution")
}

func (p ReceiverMaterializationPlan) ValidateEvent(event Event) error {
	if p.Empty() || event.ID() != p.eventID || event.RunID() != p.runID || event.RoutingSource() != p.source {
		return fmt.Errorf("receiver dependency publication identity disagrees")
	}
	return nil
}

// ValidateReceiverMaterializations is the aggregate admission/readback owner.
// Every dependent of a plan must retain it, not just the first matching agent.
func ValidateReceiverMaterializations(event Event, routes []DeliveryRoute) error {
	for _, route := range routes {
		plan := route.Materialization
		if plan.Empty() {
			if route.Recipient.IsAgent() && route.Target.MaterializingEntity() {
				pin, present := route.ConnectClaim.ReceiverIdentity()
				for _, candidate := range routes {
					otherPin, otherPresent := candidate.ConnectClaim.ReceiverIdentity()
					if present && otherPresent && pin == otherPin && candidate.Recipient.IsNode() && SameDeliveryTargetOwnership(route.Target, candidate.Target) {
						return fmt.Errorf("materializing agent omitted its publication dependency")
					}
				}
			}
			continue
		}
		if err := plan.ValidatePublication(event, routes); err != nil {
			return err
		}
		for _, other := range routes {
			id, err := other.identity(false)
			if err != nil {
				return err
			}
			for _, dependent := range plan.dependents {
				if id == dependent && !other.Materialization.Equal(plan) {
					return fmt.Errorf("publication omitted or contradicted a receiver dependency")
				}
			}
		}
	}
	return nil
}

// RestoreDeliveryMaterialization is a strict record codec, not publication
// admission. Complete aggregate validation and materializer/readiness checks
// remain mandatory before a restored dependent can acquire execution.
func RestoreDeliveryMaterialization(route DeliveryRoute, raw []byte) (DeliveryRoute, error) {
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

func (p ReceiverMaterializationPlan) Equal(other ReceiverMaterializationPlan) bool {
	if p.runID != other.runID || p.eventID != other.eventID || p.source != other.source ||
		!SameDeliveryTargetOwnership(p.target, other.target) || p.materializer != other.materializer || len(p.dependents) != len(other.dependents) {
		return false
	}
	for i := range p.dependents {
		if p.dependents[i] != other.dependents[i] {
			return false
		}
	}
	return true
}

// MarshalJSON is a read projection only. Durable hydration must re-admit the
// relation against its event and routes; decoding this object is not authority.
func (p ReceiverMaterializationPlan) MarshalJSON() ([]byte, error) {
	if p.Empty() {
		return nil, fmt.Errorf("cannot encode an empty receiver materialization plan")
	}
	dependents := make([]string, len(p.dependents))
	for i, id := range p.dependents {
		dependents[i] = EncodeDeliveryRouteIdentity(id)
	}
	return json.Marshal(receiverMaterializationWire{p.runID, p.eventID, p.source, p.target, EncodeDeliveryRouteIdentity(p.materializer), dependents})
}

type receiverMaterializationWire struct {
	RunID        string                  `json:"run_id"`
	EventID      string                  `json:"event_id"`
	Source       RoutingSource           `json:"routing_source"`
	Target       DeliveryTargetOwnership `json:"target"`
	Materializer string                  `json:"materializer_route_identity"`
	Dependents   []string                `json:"dependent_route_identities"`
}

func canonicalMaterializationUUID(raw string) error {
	id, err := uuid.Parse(raw)
	if err != nil || id == uuid.Nil || id.String() != raw {
		return fmt.Errorf("canonical non-zero UUID required")
	}
	return nil
}
