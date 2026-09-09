package events

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"

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
		id, err := route.Identity()
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
		id, err := dependent.Identity()
		if err != nil {
			return ReceiverMaterializationPlan{}, err
		}
		if !dependent.Recipient.IsAgent() || !SameDeliveryTargetOwnership(materializer.Target, dependent.Target) {
			return ReceiverMaterializationPlan{}, fmt.Errorf("receiver dependency requires an agent with the exact materializing target")
		}
		if dependent.AgentIdentity.RunID != event.RunID() {
			return ReceiverMaterializationPlan{}, fmt.Errorf("receiver dependency agent belongs to another run")
		}
		if dependent.AgentIdentity.FlowInstance() != dependent.Target.Route().FlowInstance {
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
		id, err := route.Identity()
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
		id, err := route.Identity()
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
	return json.Marshal(struct {
		RunID        string                  `json:"run_id"`
		EventID      string                  `json:"event_id"`
		Source       RoutingSource           `json:"routing_source"`
		Target       DeliveryTargetOwnership `json:"target"`
		Materializer string                  `json:"materializer_route_identity"`
		Dependents   []string                `json:"dependent_route_identities"`
	}{p.runID, p.eventID, p.source, p.target, EncodeDeliveryRouteIdentity(p.materializer), dependents})
}

func canonicalMaterializationUUID(raw string) error {
	id, err := uuid.Parse(raw)
	if err != nil || id == uuid.Nil || id.String() != raw {
		return fmt.Errorf("canonical non-zero UUID required")
	}
	return nil
}
