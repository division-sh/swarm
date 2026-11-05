package replycontext

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
)

// Return admission is part of the existing reply obligation, not authority
// inherited by unrelated outputs from the provider.
type replyOrigin struct {
	events.RouteIdentity
	Life  int64                         `json:"life"`
	Joins []events.JoinAdmissionReceipt `json:"join_admissions,omitempty"`
}

func (r Record) EncodeOrigin() ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(replyOrigin{RouteIdentity: r.Origin, Life: r.OriginLife, Joins: r.ReturnJoins})
}

func (r *Record) DecodeOrigin(raw []byte) error {
	value, err := canonicaljson.Decode(raw)
	if err != nil {
		return fmt.Errorf("decode reply origin: %w", err)
	}
	fields, object := value.ObjectMap()
	if !object {
		return fmt.Errorf("reply origin must be an object")
	}
	for name := range fields {
		switch name {
		case "flow_instance", "entity_id", "flow_id", "life", "join_admissions":
		default:
			return fmt.Errorf("reply origin has undeclared field %q", name)
		}
	}
	var origin replyOrigin
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&origin); err != nil {
		return fmt.Errorf("decode reply origin: %w", err)
	}
	if err := (events.DeliveryContext{Joins: origin.Joins}).Validate(); err != nil {
		return err
	}
	if origin.Life <= 0 {
		return fmt.Errorf("reply origin requires its positive captured life")
	}
	r.Origin, r.OriginLife, r.ReturnJoins = origin.RouteIdentity, origin.Life, origin.Joins
	return nil
}

func (r Record) sameReturnJoins(other Record) bool {
	return reflect.DeepEqual(
		(events.DeliveryContext{Joins: r.ReturnJoins}).Normalized().Joins,
		(events.DeliveryContext{Joins: other.ReturnJoins}).Normalized().Joins,
	)
}
