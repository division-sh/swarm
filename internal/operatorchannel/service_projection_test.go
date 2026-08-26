package operatorchannel

import "testing"

func TestUnboundBindingProjectionRetainsIndependentActiveProof(t *testing.T) {
	read := Readback{ProofID: "proof-a", ProofRevision: 2, ProofStatus: ProofActive}
	projectBindingReadback(&read, Binding{Revision: 5, Status: BindingUnbound})
	if read.ProofID != "proof-a" || read.ProofRevision != 2 || read.ProofStatus != ProofActive {
		t.Fatalf("unbound projection discarded retained proof: %#v", read)
	}
}
