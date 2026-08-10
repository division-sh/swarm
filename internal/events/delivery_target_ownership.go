package events

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

type deliveryTargetOwnershipKind uint8

const (
	deliveryTargetExistingEntity deliveryTargetOwnershipKind = iota + 1
	deliveryTargetMaterializingEntity
	deliveryTargetEntitylessReceiver
)

// DeliveryTargetOwnership is the complete receiver-owned target fact for one
// node delivery. Its zero value is reserved for untargeted agent delivery.
type DeliveryTargetOwnership struct {
	kind  deliveryTargetOwnershipKind
	route RouteIdentity
}

func NewExistingEntityTarget(route RouteIdentity) (DeliveryTargetOwnership, error) {
	return newDeliveryTargetOwnership(deliveryTargetExistingEntity, route)
}

func NewMaterializingEntityTarget(route RouteIdentity) (DeliveryTargetOwnership, error) {
	return newDeliveryTargetOwnership(deliveryTargetMaterializingEntity, route)
}

func NewEntitylessReceiverTarget(route RouteIdentity) (DeliveryTargetOwnership, error) {
	return newDeliveryTargetOwnership(deliveryTargetEntitylessReceiver, route)
}

func MustExistingEntityTarget(route RouteIdentity) DeliveryTargetOwnership {
	owner, err := NewExistingEntityTarget(route)
	if err != nil {
		panic(err)
	}
	return owner
}

func MustMaterializingEntityTarget(route RouteIdentity) DeliveryTargetOwnership {
	owner, err := NewMaterializingEntityTarget(route)
	if err != nil {
		panic(err)
	}
	return owner
}

func MustEntitylessReceiverTarget(route RouteIdentity) DeliveryTargetOwnership {
	owner, err := NewEntitylessReceiverTarget(route)
	if err != nil {
		panic(err)
	}
	return owner
}

func newDeliveryTargetOwnership(kind deliveryTargetOwnershipKind, route RouteIdentity) (DeliveryTargetOwnership, error) {
	owner := DeliveryTargetOwnership{kind: kind, route: route.Normalized()}
	if err := owner.Validate(); err != nil {
		return DeliveryTargetOwnership{}, err
	}
	return owner, nil
}

func (o DeliveryTargetOwnership) Empty() bool {
	return o.kind == 0 && o.route.Normalized().Empty()
}

func (o DeliveryTargetOwnership) Route() RouteIdentity {
	return o.route.Normalized()
}

func (o DeliveryTargetOwnership) ExistingEntity() bool {
	return o.kind == deliveryTargetExistingEntity
}

func (o DeliveryTargetOwnership) MaterializingEntity() bool {
	return o.kind == deliveryTargetMaterializingEntity
}

func (o DeliveryTargetOwnership) EntitylessReceiver() bool {
	return o.kind == deliveryTargetEntitylessReceiver
}

func (o DeliveryTargetOwnership) Code() string {
	switch o.kind {
	case deliveryTargetExistingEntity:
		return "existing_entity"
	case deliveryTargetMaterializingEntity:
		return "materializing_entity"
	case deliveryTargetEntitylessReceiver:
		return "entityless_receiver"
	default:
		return ""
	}
}

func (o DeliveryTargetOwnership) Validate() error {
	route := o.route.Normalized()
	if o.Empty() {
		return nil
	}
	if o.Code() == "" {
		return fmt.Errorf("delivery target ownership kind is invalid")
	}
	if route.FlowInstance == "" {
		return fmt.Errorf("delivery target ownership requires exact flow instance")
	}
	switch o.kind {
	case deliveryTargetExistingEntity, deliveryTargetMaterializingEntity:
		if route.EntityID == "" {
			return fmt.Errorf("delivery target ownership %s requires exact entity identity", o.Code())
		}
	case deliveryTargetEntitylessReceiver:
		if route.EntityID != "" {
			return fmt.Errorf("delivery target ownership entityless_receiver prohibits entity identity")
		}
	}
	return nil
}

type deliveryTargetOwnershipWire struct {
	Kind  string        `json:"kind"`
	Route RouteIdentity `json:"route"`
}

func (o DeliveryTargetOwnership) MarshalJSON() ([]byte, error) {
	if o.Empty() {
		return []byte(`{}`), nil
	}
	if err := o.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(deliveryTargetOwnershipWire{Kind: o.Code(), Route: o.Route()})
}

func (o *DeliveryTargetOwnership) UnmarshalJSON(raw []byte) error {
	if o == nil {
		return fmt.Errorf("delivery target ownership destination is nil")
	}
	if strings.TrimSpace(string(raw)) == "{}" {
		*o = DeliveryTargetOwnership{}
		return nil
	}
	var wire deliveryTargetOwnershipWire
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil {
		return fmt.Errorf("decode delivery target ownership: %w", err)
	}
	if err := requireDeliveryTargetJSONEOF(decoder); err != nil {
		return fmt.Errorf("decode delivery target ownership: %w", err)
	}
	var kind deliveryTargetOwnershipKind
	switch wire.Kind {
	case "existing_entity":
		kind = deliveryTargetExistingEntity
	case "materializing_entity":
		kind = deliveryTargetMaterializingEntity
	case "entityless_receiver":
		kind = deliveryTargetEntitylessReceiver
	default:
		return fmt.Errorf("delivery target ownership kind %q is invalid", wire.Kind)
	}
	restored, err := newDeliveryTargetOwnership(kind, wire.Route)
	if err != nil {
		return err
	}
	*o = restored
	return nil
}

func requireDeliveryTargetJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("unexpected trailing JSON value")
		}
		return err
	}
	return nil
}
