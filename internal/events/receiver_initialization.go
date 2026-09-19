package events

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
)

type receiverInitializationKind uint8

const (
	receiverInitializationAbsent receiverInitializationKind = iota
	receiverInitializationFlow
	receiverInitializationNode
)

// ReceiverInitialization records who supplies a future receiver's state. It is
// separate from ownership of that state and grants no execution authority.
type ReceiverInitialization struct {
	kind           receiverInitializationKind
	runID, eventID string
	target         DeliveryTargetOwnership
	node           identity.ExecutableNode
}

// AdmitNodeReceiverInitialization is consumed only by the canonical handler
// classifier's publication projection. The node is the initializer, not merely
// another recipient carrying the same future target.
func AdmitNodeReceiverInitialization(event Event, target DeliveryTargetOwnership, node identity.ExecutableNode) (ReceiverInitialization, error) {
	s := ReceiverInitialization{kind: receiverInitializationNode, runID: event.RunID(), eventID: event.ID(), target: target, node: node}
	return s, s.validate()
}

// AdmitFlowReceiverInitialization projects an admitted flow activation. Store
// consumers still require its exact materialized state and runtime readiness.
func AdmitFlowReceiverInitialization(event Event, target DeliveryTargetOwnership) (ReceiverInitialization, error) {
	s := ReceiverInitialization{kind: receiverInitializationFlow, runID: event.RunID(), eventID: event.ID(), target: target}
	return s, s.validate()
}

func (s ReceiverInitialization) Empty() bool                             { return s.kind == receiverInitializationAbsent }
func (s ReceiverInitialization) FlowLifecycle() bool                     { return s.kind == receiverInitializationFlow }
func (s ReceiverInitialization) NodeDelivery() bool                      { return s.kind == receiverInitializationNode }
func (s ReceiverInitialization) Equal(other ReceiverInitialization) bool { return s == other }
func (s ReceiverInitialization) IsInitializer(route DeliveryRoute) bool {
	return s.NodeDelivery() && route.Recipient.IsNode() && route.Recipient.ID() == s.node.Key() && SameDeliveryTargetOwnership(route.Target, s.target)
}

func (s ReceiverInitialization) validate() error {
	if canonicalMaterializationUUID(s.runID) != nil || canonicalMaterializationUUID(s.eventID) != nil || !s.target.MaterializingEntity() || s.target.Validate() != nil {
		return fmt.Errorf("receiver initialization requires exact publication and future target")
	}
	switch s.kind {
	case receiverInitializationNode:
		if !s.node.Valid() {
			return fmt.Errorf("receiver initialization requires an exact node")
		}
	case receiverInitializationFlow:
		if !s.node.Empty() || s.target.Route().FlowID == "" || s.target.Route().FlowID == "." {
			return fmt.Errorf("flow initialization requires a flow target and no node")
		}
	default:
		return fmt.Errorf("invalid receiver initialization supplier")
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
	if s.runID != runID || s.eventID != eventID {
		return fmt.Errorf("receiver initialization publication mismatch")
	}
	return nil
}

func (s ReceiverInitialization) ValidateRoute(route DeliveryRoute) error {
	if err := s.validate(); err != nil {
		return err
	}
	if !SameDeliveryTargetOwnership(s.target, route.Target) {
		return fmt.Errorf("receiver initialization target mismatch")
	}
	if route.Recipient.IsAgent() && route.AgentIdentity.RunID != s.runID {
		return fmt.Errorf("receiver initialization agent run mismatch")
	}
	return nil
}

type receiverInitializationWire struct {
	Kind    string                   `json:"kind"`
	RunID   string                   `json:"run_id"`
	EventID string                   `json:"event_id"`
	Target  DeliveryTargetOwnership  `json:"target"`
	Node    *identity.ExecutableNode `json:"node,omitempty"`
}

func (s ReceiverInitialization) MarshalJSON() ([]byte, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	wire := receiverInitializationWire{RunID: s.runID, EventID: s.eventID, Target: s.target}
	if s.FlowLifecycle() {
		wire.Kind = "flow_lifecycle"
	} else {
		wire.Kind = "node_delivery"
		node := s.node
		wire.Node = &node
	}
	return json.Marshal(wire)
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

// decoded is the canonical admission of these same bytes, either standalone
// or as the exact subtree of the enclosing materialization record.
func decodeReceiverInitialization(raw []byte, decoded semanticvalue.Value) (ReceiverInitialization, error) {
	if decoded.Kind() != semanticvalue.KindObject {
		return ReceiverInitialization{}, fmt.Errorf("receiver initialization requires an object")
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
	value := ReceiverInitialization{runID: wire.RunID, eventID: wire.EventID, target: wire.Target}
	switch wire.Kind {
	case "flow_lifecycle":
		if _, present := decoded.Lookup("node"); present {
			return ReceiverInitialization{}, fmt.Errorf("flow initialization forbids node evidence")
		}
		value.kind = receiverInitializationFlow
	case "node_delivery":
		value.kind = receiverInitializationNode
	default:
		return ReceiverInitialization{}, fmt.Errorf("unknown receiver initialization supplier %q", wire.Kind)
	}
	if wire.Node != nil {
		value.node = *wire.Node
	}
	if err := value.validate(); err != nil {
		return ReceiverInitialization{}, err
	}
	return value, nil
}

// The existing durable materialization column stores both supplier evidence
// and the optional node dependency as one strict record. Neither can be lost
// by reading only the other projection.
type receiverMaterializationRecord struct {
	Initialization *ReceiverInitialization `json:"initialization"`
	Dependency     json.RawMessage         `json:"dependency"`
}

func EncodeReceiverMaterializationRecord(route DeliveryRoute) ([]byte, error) {
	if route.Initialization.Empty() && route.Materialization.Empty() {
		return []byte("null"), nil
	}
	if err := route.Initialization.ValidateRoute(route); err != nil {
		return nil, err
	}
	record := receiverMaterializationRecord{Initialization: &route.Initialization, Dependency: json.RawMessage("null")}
	if !route.Materialization.Empty() {
		if err := route.Materialization.validateDependent(route); err != nil {
			return nil, err
		}
		raw, err := json.Marshal(route.Materialization)
		if err != nil {
			return nil, err
		}
		record.Dependency = raw
	}
	return json.Marshal(record)
}

func RestoreReceiverMaterializationRecord(route DeliveryRoute, raw []byte) (DeliveryRoute, error) {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		if !route.Initialization.Empty() || !route.Materialization.Empty() {
			return DeliveryRoute{}, fmt.Errorf("cannot erase receiver initialization record")
		}
		return route, nil
	}
	decoded, err := canonicaljson.Decode(raw)
	if err != nil {
		return DeliveryRoute{}, err
	}
	record, dependency, err := decodeReceiverMaterializationRecord(raw, decoded)
	if err != nil {
		return DeliveryRoute{}, err
	}
	if record.Initialization == nil || len(record.Dependency) == 0 {
		return DeliveryRoute{}, fmt.Errorf("receiver materialization record requires supplier and dependency fields")
	}
	if !route.Initialization.Empty() && !route.Initialization.Equal(*record.Initialization) {
		return DeliveryRoute{}, fmt.Errorf("cannot replace admitted receiver initialization")
	}
	route.Initialization = *record.Initialization
	if err := route.Initialization.ValidateRoute(route); err != nil {
		return DeliveryRoute{}, err
	}
	return restoreDeliveryMaterialization(route, record.Dependency, dependency)
}

func decodeReceiverMaterializationRecord(raw []byte, decoded semanticvalue.Value) (receiverMaterializationRecord, semanticvalue.Value, error) {
	var record receiverMaterializationRecord
	var dependency semanticvalue.Value
	d := json.NewDecoder(bytes.NewReader(raw))
	if decoded.Kind() != semanticvalue.KindObject {
		// Retain the original typed decoder's non-object error.
		err := d.Decode(&record)
		return record, dependency, err
	}
	if _, err := d.Token(); err != nil {
		return record, dependency, err
	}
	var unknown error
	for d.More() {
		token, err := d.Token()
		if err != nil {
			return record, dependency, err
		}
		key, ok := token.(string)
		if !ok {
			return record, dependency, fmt.Errorf("receiver materialization field must be a string")
		}
		var field json.RawMessage
		if err := d.Decode(&field); err != nil {
			return record, dependency, err
		}
		value, found := decoded.Lookup(key)
		if !found {
			return record, dependency, fmt.Errorf("receiver materialization field missing from canonical object")
		}
		switch {
		case strings.EqualFold(key, "initialization"):
			// Decode every case alias in wire order, as encoding/json does.
			// A later alias must not hide an earlier invalid supplier.
			if value.Kind() == semanticvalue.KindNull {
				record.Initialization = nil
				continue
			}
			initialization, err := decodeReceiverInitialization(field, value)
			if err != nil {
				return record, dependency, err
			}
			record.Initialization = &initialization
		case strings.EqualFold(key, "dependency"):
			record.Dependency, dependency = field, value
		default:
			if unknown == nil {
				unknown = fmt.Errorf("json: unknown field %q", key)
			}
		}
	}
	if _, err := d.Token(); err != nil {
		return record, dependency, err
	}
	return record, dependency, unknown
}
