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
	Joins []events.JoinAdmissionReceipt `json:"join_admissions,omitempty"`
}

func (r Record) EncodeOrigin() ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(replyOrigin{RouteIdentity: r.Origin, Joins: r.ReturnJoins})
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
		case "flow_instance", "entity_id", "flow_id", "join_admissions":
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
	r.Origin, r.ReturnJoins = origin.RouteIdentity, origin.Joins
	return nil
}

func (r Record) sameReturnJoins(other Record) bool {
	return reflect.DeepEqual(
		(events.DeliveryContext{Joins: r.ReturnJoins}).Normalized().Joins,
		(events.DeliveryContext{Joins: other.ReturnJoins}).Normalized().Joins,
	)
}
