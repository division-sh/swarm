package replycontext

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
)

// AwaitBinding is immutable attribution, never a claim or execution permit.
type AwaitBinding struct {
	Version             string `json:"version"`
	ReplyContextID      string `json:"reply_context_id"`
	RequestEventID      string `json:"request_event_id"`
	RunID               string `json:"run_id"`
	OriginLife          int64  `json:"origin_life"`
	DeliveryID          string `json:"delivery_id"`
	ClaimVersion        int64  `json:"claim_version"`
	ExecutionOwner      string `json:"execution_owner"`
	OperationID         string `json:"operation_id"`
	AttemptID           string `json:"attempt_id"`
	BundleHash          string `json:"bundle_hash"`
	PlanFingerprint     string `json:"plan_fingerprint"`
	FrameID             string `json:"frame_id"`
	ResponseFingerprint string `json:"response_fingerprint"`
	CallID              string `json:"call_id"`
	CallIndex           int    `json:"call_index"`
	CompletedResults    string `json:"completed_results"`
}

func (b AwaitBinding) Validate() error {
	if b.Version != "reply-await.v1" || b.ReplyContextID == "" || b.RequestEventID == "" || b.RunID == "" || b.OriginLife <= 0 ||
		b.DeliveryID == "" || b.ClaimVersion <= 0 || b.ExecutionOwner == "" ||
		b.OperationID == "" || b.AttemptID == "" || b.BundleHash == "" || b.PlanFingerprint == "" ||
		b.FrameID == "" || b.ResponseFingerprint == "" || b.CallID == "" || b.CallIndex < 0 {
		return fmt.Errorf("reply await binding requires exact delivery, completion, frame, and call evidence")
	}
	value, err := canonicaljson.Decode([]byte(b.CompletedResults))
	if err != nil {
		return err
	}
	if value.Kind() != semanticvalue.KindArray || value.Len() != b.CallIndex {
		return fmt.Errorf("reply await binding requires the exact completed tool prefix")
	}
	return nil
}

func (r Record) EncodeAwaitBinding() (any, error) {
	if r.Await == nil {
		return nil, nil
	}
	if err := r.Await.Validate(); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(r.Await)
	return string(raw), err
}

func (r *Record) DecodeAwaitBinding(raw []byte) error {
	if len(raw) == 0 {
		r.Await = nil
		return nil
	}
	value, err := canonicaljson.Decode(raw)
	if err != nil {
		return err
	}
	fields, object := value.ObjectMap()
	if !object {
		return fmt.Errorf("reply await binding must be an object")
	}
	typeOf := reflect.TypeOf(AwaitBinding{})
	allowed := make(map[string]bool, typeOf.NumField())
	for i := 0; i < typeOf.NumField(); i++ {
		allowed[typeOf.Field(i).Tag.Get("json")] = true
	}
	if len(fields) != len(allowed) {
		return fmt.Errorf("reply await binding is missing required evidence")
	}
	if index, number := fields["call_index"].Number(); !number || index < 0 || index != float64(int(index)) {
		return fmt.Errorf("reply await call index requires a nonnegative integer")
	}
	for name := range fields {
		if !allowed[name] {
			return fmt.Errorf("reply await binding has undeclared field %q", name)
		}
	}
	var binding AwaitBinding
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&binding); err != nil {
		return err
	}
	if err := binding.Validate(); err != nil {
		return err
	}
	r.Await = &binding
	return nil
}
