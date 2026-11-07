package channelnative

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/google/uuid"
)

// TransportParent identifies existing registration and binding rows, never a
// replacement activation or an inferred owner for an absent bundle.
type TransportParent struct {
	Kind                     string `json:"kind"`
	ID                       string `json:"id"`
	Revision                 int64  `json:"revision"`
	BindingOperationID       string `json:"binding_operation_id"`
	BindingOperationRevision int64  `json:"binding_operation_revision"`
	ProofID                  string `json:"proof_id,omitempty"`
	ProofRevision            int64  `json:"proof_revision,omitempty"`
}

func (p TransportParent) Valid() bool {
	_, registration := uuid.Parse(p.ID)
	_, binding := uuid.Parse(p.BindingOperationID)
	_, proof := uuid.Parse(p.ProofID)
	return p.Kind == "deployment_registration" && registration == nil && binding == nil &&
		p.Revision > 0 && p.BindingOperationRevision > 0 &&
		((p.ProofID == "" && p.ProofRevision == 0) || (proof == nil && p.ProofRevision > 0))
}

func DecodeTransportParent(raw []byte) (TransportParent, error) {
	var parent TransportParent
	if _, err := canonicaljson.Decode(raw); err != nil {
		return parent, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return parent, err
	}
	for _, value := range fields {
		if bytes.Equal(value, []byte("null")) {
			return parent, fmt.Errorf("transport parent members cannot be null")
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&parent); err != nil {
		return parent, err
	}
	if !parent.Valid() {
		return TransportParent{}, fmt.Errorf("invalid deployment transport parent")
	}
	return parent, nil
}
