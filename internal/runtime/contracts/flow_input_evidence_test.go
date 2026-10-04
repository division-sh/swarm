package contracts

import "testing"

func TestRootPublicEligibilityDoesNotCompeteWithBoundaryBindings(t *testing.T) {
	public := FlowInputProducerEvidence{Kind: FlowInputProducerBoundaryExternalIngress, EventType: "work.ready"}
	provider := FlowInputProducerEvidence{Kind: FlowInputProducerBoundaryIntrinsicIngress, FlowID: ".", EventType: "work.ready"}
	connection := FlowInputProducerEvidence{Kind: FlowInputProducerBoundaryParentConnect, FlowID: "producer", Pin: "work.ready", EventType: "work.ready"}
	for _, tc := range []struct {
		name     string
		evidence []FlowInputProducerEvidence
	}{
		{name: "public alone", evidence: []FlowInputProducerEvidence{public}},
		{name: "public and provider", evidence: []FlowInputProducerEvidence{public, provider}},
		{name: "public and connection", evidence: []FlowInputProducerEvidence{public, connection}},
		{name: "independent bindings coexist", evidence: []FlowInputProducerEvidence{public, provider, connection}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resolution := FlowInputProducerResolution{FlowID: ".", EventType: "work.ready", Evidence: tc.evidence}
			if !resolution.HasEvidence() {
				t.Fatal("accepted producer evidence was lost")
			}
		})
	}
}
