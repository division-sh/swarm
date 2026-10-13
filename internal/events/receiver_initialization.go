package events

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/google/uuid"
)

// ReceiverInitialization binds a delivery to its selected flow's construction,
// independently of the recipient's state ownership. The store verifies the
// original receipt and exact attachment authority before execution.
type ReceiverInitialization struct {
	runID, eventID string
	target         DeliveryTargetOwnership
	receiver       agentidentity.Route
	historical     HistoricalReceiverConstruction
}

type HistoricalReceiverConstruction struct {
	SourceRunID    string              `json:"source_run_id"`
	SourceRevision int64               `json:"source_revision"`
	SourceReceiver agentidentity.Route `json:"source_receiver"`
}

func AdmitFlowReceiverInitialization(event Event, target DeliveryTargetOwnership, receiver agentidentity.Route) (ReceiverInitialization, error) {
	if !target.MaterializingEntity() {
		return ReceiverInitialization{}, fmt.Errorf("new receiver construction requires a materializing flow target")
	}
	s := ReceiverInitialization{runID: event.RunID(), eventID: event.ID(), target: target, receiver: receiver}
	return s, s.validate()
}

func AdmitStoredFlowReceiverInitialization(runID, creatingEventID string, target DeliveryTargetOwnership, receiver agentidentity.Route) (ReceiverInitialization, error) {
	if !target.ExistingEntity() {
		return ReceiverInitialization{}, fmt.Errorf("stored receiver construction requires an existing flow target")
	}
	s := ReceiverInitialization{runID: runID, eventID: creatingEventID, target: target, receiver: receiver}
	return s, s.validate()
}

func AdmitHistoricalFlowReceiverInitialization(runID string, target DeliveryTargetOwnership, receiver agentidentity.Route, historical HistoricalReceiverConstruction) (ReceiverInitialization, error) {
	if !target.ExistingEntity() || historical == (HistoricalReceiverConstruction{}) {
		return ReceiverInitialization{}, fmt.Errorf("historical receiver construction requires its exact fixed cut")
	}
	s := ReceiverInitialization{runID: runID, target: target, receiver: receiver, historical: historical}
	return s, s.validate()
}

func (s ReceiverInitialization) Empty() bool                                 { return s.runID == "" }
func (s ReceiverInitialization) CreatingEventID() string                     { return s.eventID }
func (s ReceiverInitialization) ConstructionTarget() DeliveryTargetOwnership { return s.target }
func (s ReceiverInitialization) ReceiverRoute() agentidentity.Route          { return s.receiver }
func (s ReceiverInitialization) FlowLifecycle() bool                         { return !s.Empty() }
func (s ReceiverInitialization) Equal(other ReceiverInitialization) bool     { return s == other }
func (s ReceiverInitialization) HistoricalConstruction() (HistoricalReceiverConstruction, bool) {
	return s.historical, s.historical != (HistoricalReceiverConstruction{})
}

func (s ReceiverInitialization) validate() error {
	if canonicalMaterializationUUID(s.runID) != nil ||
		(s.eventID != "" && canonicalMaterializationUUID(s.eventID) != nil) ||
		(s.target.MaterializingEntity() && s.eventID == "") ||
		(!s.target.MaterializingEntity() && !s.target.ExistingEntity()) || s.target.Validate() != nil {
		return fmt.Errorf("receiver initialization requires exact construction publication and target")
	}
	if s.receiver != s.receiver.Normalize() || s.receiver.Validate() != nil {
		return fmt.Errorf("receiver initialization requires canonical receiver coordinates")
	}
	path := s.receiver.InstancePath
	if s.receiver.Presence == agentidentity.RouteRoot {
		path = s.runID
	}
	if path != s.target.Route().FlowInstance {
		return fmt.Errorf("receiver initialization coordinates contradict construction target")
	}
	if historical, present := s.HistoricalConstruction(); present {
		if s.eventID != "" || !s.target.ExistingEntity() ||
			canonicalMaterializationUUID(historical.SourceRunID) != nil || historical.SourceRunID == s.runID || historical.SourceRevision <= 0 ||
			historical.SourceReceiver != s.receiver || historical.SourceReceiver.Validate() != nil {
			return fmt.Errorf("historical receiver initialization contradicts its captured source and fixed cut")
		}
	}
	return nil
}

func (s ReceiverInitialization) ValidateEvent(event Event) error {
	return s.ValidatePublication(event.RunID(), event.ID())
}

func (s ReceiverInitialization) ValidatePublication(runID, eventID string) error {
	if err := s.validate(); err != nil {
		return err
	}
	if s.runID != runID || canonicalMaterializationUUID(eventID) != nil ||
		s.target.MaterializingEntity() && s.eventID != eventID {
		return fmt.Errorf("receiver initialization publication mismatch")
	}
	return nil
}

func (s ReceiverInitialization) ValidateRoute(route DeliveryRoute) error {
	if err := s.validate(); err != nil {
		return err
	}
	if route.Recipient.IsAgent() {
		if route.AgentIdentity.Validate() != nil || route.AgentIdentity.RunID != s.runID {
			return fmt.Errorf("receiver initialization agent run mismatch")
		}
		construction, actor := s.target.Route(), route.Target.Route()
		if route.Target.MaterializingEntity() && !SameDeliveryTargetOwnership(s.target, route.Target) {
			return fmt.Errorf("materializing actor contradicts its construction target")
		}
		if route.Target.Empty() || route.Target.Validate() != nil || construction.FlowID != actor.FlowID || construction.FlowInstance != actor.FlowInstance {
			return fmt.Errorf("receiver initialization actor flow mismatch")
		}
		if route.AgentIdentity.Route != s.receiver {
			return fmt.Errorf("receiver initialization agent coordinates mismatch")
		}
	} else if !SameDeliveryTargetOwnership(s.target, route.Target) {
		return fmt.Errorf("receiver initialization target mismatch")
	}
	return nil
}

type receiverInitializationWire struct {
	Kind       string                          `json:"kind"`
	RunID      string                          `json:"run_id"`
	EventID    string                          `json:"event_id"`
	Target     DeliveryTargetOwnership         `json:"target"`
	Receiver   agentidentity.Route             `json:"receiver_route"`
	Historical *HistoricalReceiverConstruction `json:"historical,omitempty"`
}

type historicalReceiverInitializationWire struct {
	Kind       string                         `json:"kind"`
	RunID      string                         `json:"run_id"`
	Target     DeliveryTargetOwnership        `json:"target"`
	Receiver   agentidentity.Route            `json:"receiver_route"`
	Historical HistoricalReceiverConstruction `json:"historical"`
}

func (s ReceiverInitialization) MarshalJSON() ([]byte, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	if historical, present := s.HistoricalConstruction(); present {
		return json.Marshal(historicalReceiverInitializationWire{Kind: "flow_historical", RunID: s.runID, Target: s.target, Receiver: s.receiver, Historical: historical})
	}
	return json.Marshal(receiverInitializationWire{Kind: "flow_lifecycle", RunID: s.runID, EventID: s.eventID, Target: s.target, Receiver: s.receiver})
}

func (s *ReceiverInitialization) UnmarshalJSON(raw []byte) error {
	decoded, err := canonicaljson.Decode(raw)
	if err != nil {
		return err
	}
	value, err := decodeReceiverInitialization(raw, decoded)
	if err != nil {
		return err
	}
	*s = value
	return nil
}

func decodeReceiverInitialization(raw []byte, decoded semanticvalue.Value) (ReceiverInitialization, error) {
	if decoded.Kind() != semanticvalue.KindObject || decoded.Len() != 5 {
		return ReceiverInitialization{}, fmt.Errorf("receiver initialization requires every construction binding field")
	}
	kind, _ := decoded.Lookup("kind")
	kindText, _ := kind.String()
	fields := []string{"kind", "run_id", "event_id", "target", "receiver_route"}
	if kindText == "flow_historical" {
		fields[2] = "historical"
		if err := validateHistoricalReceiverInitialization(decoded); err != nil {
			return ReceiverInitialization{}, err
		}
	}
	for _, field := range fields {
		if value, present := decoded.Lookup(field); !present || value.Kind() == semanticvalue.KindNull {
			return ReceiverInitialization{}, fmt.Errorf("receiver initialization missing %s", field)
		}
	}
	if err := validateReceiverInitializationCoordinates(decoded, "receiver_route"); err != nil {
		return ReceiverInitialization{}, err
	}
	var wire receiverInitializationWire
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&wire); err != nil {
		return ReceiverInitialization{}, err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return ReceiverInitialization{}, fmt.Errorf("receiver initialization requires one object")
	}
	if wire.Kind != "flow_lifecycle" && wire.Kind != "flow_historical" {
		return ReceiverInitialization{}, fmt.Errorf("unknown receiver initialization authority %q", wire.Kind)
	}
	value := ReceiverInitialization{runID: wire.RunID, eventID: wire.EventID, target: wire.Target, receiver: wire.Receiver}
	if wire.Kind == "flow_historical" {
		if wire.Historical == nil {
			return ReceiverInitialization{}, fmt.Errorf("historical receiver initialization lacks fixed-cut evidence")
		}
		return AdmitHistoricalFlowReceiverInitialization(wire.RunID, wire.Target, wire.Receiver, *wire.Historical)
	}
	return value, value.validate()
}

func validateReceiverInitializationCoordinates(decoded semanticvalue.Value, field string) error {
	receiver, _ := decoded.Lookup(field)
	if receiver.Kind() != semanticvalue.KindObject || receiver.Len() != 4 {
		return fmt.Errorf("receiver initialization requires complete captured coordinates")
	}
	for _, key := range []string{"presence", "scope_key", "instance_id", "instance_path"} {
		if value, present := receiver.Lookup(key); !present || value.Kind() != semanticvalue.KindString {
			return fmt.Errorf("receiver initialization has invalid %s", key)
		}
	}
	return nil
}

func validateHistoricalReceiverInitialization(decoded semanticvalue.Value) error {
	historical, _ := decoded.Lookup("historical")
	if historical.Kind() != semanticvalue.KindObject || historical.Len() != 3 {
		return fmt.Errorf("historical receiver initialization requires every fixed-cut coordinate")
	}
	for _, field := range []string{"source_run_id", "source_revision", "source_receiver"} {
		if value, present := historical.Lookup(field); !present || value.Kind() == semanticvalue.KindNull {
			return fmt.Errorf("historical receiver initialization missing %s", field)
		}
	}
	return validateReceiverInitializationCoordinates(historical, "source_receiver")
}

type receiverInitializationRecord struct {
	Initialization *ReceiverInitialization `json:"initialization"`
}

func EncodeReceiverMaterializationRecord(route DeliveryRoute) ([]byte, error) {
	if route.Initialization.Empty() {
		return []byte("null"), nil
	}
	if err := route.Initialization.ValidateRoute(route); err != nil {
		return nil, err
	}
	return json.Marshal(receiverInitializationRecord{Initialization: &route.Initialization})
}

func RestoreReceiverMaterializationRecord(route DeliveryRoute, raw []byte) (DeliveryRoute, error) {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		if !route.Initialization.Empty() {
			return DeliveryRoute{}, fmt.Errorf("cannot erase receiver construction receipt")
		}
		return route, nil
	}
	decoded, err := canonicaljson.Decode(raw)
	if err != nil {
		return DeliveryRoute{}, err
	}
	value, present := decoded.Lookup("initialization")
	if decoded.Kind() != semanticvalue.KindObject || decoded.Len() != 1 || !present || value.Kind() == semanticvalue.KindNull {
		return DeliveryRoute{}, fmt.Errorf("receiver initialization record requires only its construction receipt")
	}
	var wire struct {
		Initialization json.RawMessage `json:"initialization"`
	}
	if err := json.Unmarshal(raw, &wire); err != nil {
		return DeliveryRoute{}, err
	}
	initialization, err := decodeReceiverInitialization(wire.Initialization, value)
	if err != nil {
		return DeliveryRoute{}, err
	}
	if !route.Initialization.Empty() && !route.Initialization.Equal(initialization) {
		return DeliveryRoute{}, fmt.Errorf("cannot replace admitted receiver construction receipt")
	}
	route.Initialization = initialization
	if err := route.Initialization.ValidateRoute(route); err != nil {
		return DeliveryRoute{}, err
	}
	return route, nil
}

func ValidateReceiverMaterializations(event Event, routes []DeliveryRoute) error {
	for _, route := range routes {
		if route.Initialization.Empty() {
			if route.Target.MaterializingEntity() {
				return fmt.Errorf("constructing receiver omitted canonical construction receipt")
			}
			continue
		}
		if err := route.Initialization.ValidateEvent(event); err != nil {
			return err
		}
		if err := route.Initialization.ValidateRoute(route); err != nil {
			return err
		}
	}
	return nil
}

func canonicalMaterializationUUID(raw string) error {
	id, err := uuid.Parse(raw)
	if err != nil || id == uuid.Nil || id.String() != raw {
		return fmt.Errorf("canonical non-zero UUID required")
	}
	return nil
}
