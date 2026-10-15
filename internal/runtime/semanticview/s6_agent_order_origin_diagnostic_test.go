package semanticview

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestS6CompiledNonAuthoredAgentOriginDiagnostic(t *testing.T) {
	source := loadLifecycleStaticSource(t, canonicalrouting.CopySelectedForkReadiness(t, 1, "node"))
	const local = "worker.inspect.requested"
	proof := ResolveFlowEventProof(source, "worker-flow", "worker-flow/worker-001/"+local)
	if !proof.HasSchema || proof.IsAuthored(source) {
		t.Fatalf("compiled renamed receiver is not a non-authored event: %+v", proof)
	}
	admission, err := AdmitFlowOwnedAgentSubscriptions(source, FlowOwnedAgentSubscriptionRequest{
		AgentID: "worker-agent", FlowID: "worker-flow", FlowPath: "worker-flow/worker-001", Subscriptions: []string{local},
	})
	if err != nil || !admission.ValidForAgent("worker-agent") {
		t.Fatalf("non-authored compiled receiver refused agent eligibility: %+v err=%v", admission, err)
	}
	patterns := admission.RoutePatterns()
	if len(patterns) != 1 || patterns[0] != proof.Canonical {
		t.Fatalf("agent admission and non-authored occurrence disagree: patterns=%v proof=%+v", patterns, proof)
	}
	t.Log("canonical compiled-schema and agent-subscription owners admit a non-authored receiver occurrence; not a served-publication qualification")
}
