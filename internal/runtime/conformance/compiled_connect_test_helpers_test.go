package conformance

import (
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"testing"
)

func compiledConnectPlans(source semanticview.Source) ([]runtimepinrouting.ConnectRoutePlan, []runtimepinrouting.ConnectRoutePlanIssue) {
	graph := runtimepinrouting.CompileConnectGraph(source)
	return graph.Plans(), graph.Issues()
}

func requireRootInputConnections(t *testing.T, plans []runtimepinrouting.ConnectRoutePlan, receiver string, events ...string) []runtimepinrouting.ConnectRoutePlan {
	t.Helper()
	remaining := make(map[string]bool, len(events))
	for _, event := range events {
		remaining[event] = true
	}
	var private []runtimepinrouting.ConnectRoutePlan
	for _, plan := range plans {
		if !plan.SourceEndpoint().IsRoot() {
			private = append(private, plan)
			continue
		}
		source, target := plan.SourceEndpoint().Readback(), plan.ReceiverEndpoint().Readback()
		if !remaining[source.LocalEvent] || target.FlowID != receiver || target.LocalEvent != source.LocalEvent || source.PinDigest == "" || target.PinDigest == "" || plan.ResolutionKind() != runtimepinrouting.ConnectResolutionStatic {
			t.Fatalf("unexpected root connection: %#v", plan.Readback())
		}
		delete(remaining, source.LocalEvent)
	}
	if len(remaining) != 0 {
		t.Fatalf("missing root connections: %#v", remaining)
	}
	return private
}
