package contracts

import "testing"

func TestRootPublicEligibilityDoesNotCompeteWithBoundaryBindings(t *testing.T) {
	public := FlowInputProducerEvidence{Kind: FlowInputProducerBoundaryExternalIngress, EventType: "work.ready"}
	provider := FlowInputProducerEvidence{Kind: FlowInputProducerBoundaryIntrinsicIngress, FlowID: ".", EventType: "work.ready"}
	connection := FlowInputProducerEvidence{Kind: FlowInputProducerBoundaryParentConnect, FlowID: "producer", Pin: "work.ready", EventType: "work.ready"}
	for _, tc := range []struct {
		name            string
		evidence        []FlowInputProducerEvidence
		harnessConflict bool
	}{
		{name: "public alone", evidence: []FlowInputProducerEvidence{public}},
		{name: "public and provider", evidence: []FlowInputProducerEvidence{public, provider}},
		{name: "public and connection", evidence: []FlowInputProducerEvidence{public, connection}},
		{name: "independent bindings coexist", evidence: []FlowInputProducerEvidence{public, provider, connection}},
		{name: "harness and provider conflict", evidence: []FlowInputProducerEvidence{provider, {Kind: FlowInputProducerBoundaryHarnessInjection}}, harnessConflict: true},
		{name: "harness and connection conflict", evidence: []FlowInputProducerEvidence{connection, {Kind: FlowInputProducerBoundaryHarnessInjection}}, harnessConflict: true},
		{name: "harness remains exclusive", evidence: []FlowInputProducerEvidence{public, {Kind: FlowInputProducerBoundaryHarnessInjection, FlowID: ".", EventType: "work.ready"}}, harnessConflict: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resolution := FlowInputProducerResolution{FlowID: ".", EventType: "work.ready", Evidence: tc.evidence}
			if !resolution.HasEvidence() {
				t.Fatal("accepted producer evidence was lost")
			}
			if got := resolution.HasConflictingHarnessEvidence(); got != tc.harnessConflict {
				t.Fatalf("harness conflict=%v want=%v", got, tc.harnessConflict)
			}
		})
	}
}
