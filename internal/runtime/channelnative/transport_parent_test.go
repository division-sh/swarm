package channelnative

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
)

func TestTransportParentClosedCodec(t *testing.T) {
	parent := TransportParent{Kind: "deployment_registration", ID: uuid.NewString(), Revision: 2,
		BindingOperationID: uuid.NewString(), BindingOperationRevision: 3}
	raw, err := json.Marshal(parent)
	if err != nil {
		t.Fatal(err)
	}
	if decoded, err := DecodeTransportParent(raw); err != nil || decoded != parent {
		t.Fatal("closed parent failed", decoded, err)
	}
	for name, raw := range map[string]string{
		"null": "null", "array": "[]", "missing": "{}",
		"trailing": string(raw) + "{}", "unknown": string(raw[:len(raw)-1]) + `,"extra":1}`,
		"duplicate":  string(raw[:len(raw)-1]) + `,"revision":2}`,
		"null proof": string(raw[:len(raw)-1]) + `,"proof_id":null,"proof_revision":null}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeTransportParent([]byte(raw)); err == nil {
				t.Fatal("invalid parent admitted", raw)
			}
		})
	}
	for _, mutate := range []func(*TransportParent){
		func(p *TransportParent) { p.Kind = "runtime" }, func(p *TransportParent) { p.ID = "" },
		func(p *TransportParent) { p.Revision = 0 }, func(p *TransportParent) { p.BindingOperationID = "no" },
		func(p *TransportParent) { p.BindingOperationRevision = -1 }, func(p *TransportParent) { p.ProofID = uuid.NewString() },
		func(p *TransportParent) { p.ProofRevision = 1 },
	} {
		changed := parent
		mutate(&changed)
		if changed.Valid() {
			t.Fatal("invalid parent accepted", changed)
		}
	}
}
