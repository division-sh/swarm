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
const (
	TransportRuntimeActivation      = "runtime_activation"
	TransportDeploymentRegistration = "deployment_registration"
)

type TransportParent struct {
	Kind                     string `json:"kind"`
	ID                       string `json:"id"`
	Revision                 int64  `json:"revision"`
	BindingOperationID       string `json:"binding_operation_id,omitempty"`
	BindingOperationRevision int64  `json:"binding_operation_revision,omitempty"`
	ProofID                  string `json:"proof_id,omitempty"`
	ProofRevision            int64  `json:"proof_revision,omitempty"`
}

func (p TransportParent) Valid() bool {
	_, registration := uuid.Parse(p.ID)
	if p.Kind == TransportRuntimeActivation {
		return registration == nil && p.Revision > 0 && p.BindingOperationID == "" &&
			p.BindingOperationRevision == 0 && p.ProofID == "" && p.ProofRevision == 0
	}
	_, binding := uuid.Parse(p.BindingOperationID)
	_, proof := uuid.Parse(p.ProofID)
	return p.Kind == "deployment_registration" && registration == nil && binding == nil &&
		p.Revision > 0 && p.BindingOperationRevision > 0 &&
		((p.ProofID == "" && p.ProofRevision == 0) || (proof == nil && p.ProofRevision > 0))
}

func (p TransportParent) Validate() error {
	if !p.Valid() {
		return fmt.Errorf("invalid channel transport parent")
	}
	return nil
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
	_, proofID := fields["proof_id"]
	_, proofRevision := fields["proof_revision"]
	if proofID != proofRevision || proofID && (parent.ProofID == "" || parent.ProofRevision < 1) {
		return TransportParent{}, fmt.Errorf("transport parent proof must be an exact existing pair")
	}
	if !parent.Valid() {
		return TransportParent{}, fmt.Errorf("invalid deployment transport parent")
	}
	return parent, nil
}
