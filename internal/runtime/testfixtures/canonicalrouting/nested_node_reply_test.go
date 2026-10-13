package canonicalrouting

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type nestedNodeReplyGuardEdge struct {
	from, output, to, input string
}

func TestNestedNodeReplyFixturesPassBootAndRetainExactPairedGraph(t *testing.T) {
	chain := []pinrouting.ConnectRoutePlanReplyReadback{
		{RequesterFlowID: "provider", RequestOutputPin: "t.requested", ReplyInputPin: "t.replied", ProviderFlowID: "t", ProviderInputPin: "t.requested", ProviderOutputPin: "t.replied", CorrelationKey: "request_id"},
		{RequesterFlowID: "t", RequestOutputPin: "u.requested", ReplyInputPin: "u.replied", ProviderFlowID: "u", ProviderInputPin: "u.requested", ProviderOutputPin: "u.replied", CorrelationKey: "request_id"},
	}
	for _, tc := range []struct {
		name             string
		copy             func(testing.TB) string
		outer            []pinrouting.ConnectRoutePlanReplyReadback
		ordinary         []nestedNodeReplyGuardEdge
		requesters       []string
		innerCorrelation string
		inputs           map[string]bool
	}{
		{
			name:             "depth3",
			copy:             CopyNestedNodeReplyDepth3,
			innerCorrelation: "request_id",
			inputs:           map[string]bool{"request.started": true, "request.stop": true},
			outer: []pinrouting.ConnectRoutePlanReplyReadback{
				{RequesterFlowID: ".", RequestOutputPin: "provider.requested", ReplyInputPin: "provider.replied", ProviderFlowID: "provider", ProviderInputPin: "provider.requested", ProviderOutputPin: "provider.replied", CorrelationKey: "request_id"},
			},
		},
		{
			name: "two_requesters",
			copy: CopyNestedNodeReplyTwoRequesters,
			outer: []pinrouting.ConnectRoutePlanReplyReadback{
				{RequesterFlowID: "r1", RequestOutputPin: "provider.requested", ReplyInputPin: "provider.replied", ProviderFlowID: "provider", ProviderInputPin: "r1.requested", ProviderOutputPin: "provider.replied", CorrelationKey: "request_id"},
				{RequesterFlowID: "r2", RequestOutputPin: "provider.requested", ReplyInputPin: "provider.replied", ProviderFlowID: "provider", ProviderInputPin: "r2.requested", ProviderOutputPin: "provider.replied", CorrelationKey: "request_id"},
			},
			ordinary: []nestedNodeReplyGuardEdge{
				{".", "r1.dispatched", "r1", "request.dispatched"},
				{".", "r2.dispatched", "r2", "request.dispatched"},
				{"r1", "request.finished", ".", "r1.finished"},
				{"r2", "request.finished", ".", "r2.finished"},
			},
			requesters: []string{"r1", "r2"},
			inputs:     map[string]bool{"r1.started": true, "r2.started": true, "request.stop": true},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root, repo := tc.copy(t), RepoRoot(t)
			bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, root, contracts.DefaultPlatformSpecFile(repo))
			if err != nil {
				t.Fatal(err)
			}
			source := semanticview.Wrap(bundle)
			if findings := bootverify.Run(context.Background(), source, bootverify.Options{}).HardInvalidities(); len(findings) != 0 {
				t.Fatalf("nested node reply fixture must pass complete boot: %#v", findings)
			}
			if len(semanticview.AgentDeclarations(source)) != 0 {
				t.Fatal("nested node reply proof must have no agents")
			}
			graph := pinrouting.CompileConnectGraph(source)
			if len(graph.Issues()) != 0 {
				t.Fatalf("paired graph refused: %+v", graph.Issues())
			}
			wantEdges := map[nestedNodeReplyGuardEdge]int{}
			wantReplies := map[pinrouting.ConnectRoutePlanReplyReadback]int{}
			for i, pair := range append(tc.outer, chain...) {
				if i >= len(tc.outer) {
					pair.CorrelationKey = tc.innerCorrelation
				}
				wantEdges[nestedNodeReplyGuardEdge{pair.RequesterFlowID, pair.RequestOutputPin, pair.ProviderFlowID, pair.ProviderInputPin}]++
				wantEdges[nestedNodeReplyGuardEdge{pair.ProviderFlowID, pair.ProviderOutputPin, pair.RequesterFlowID, pair.ReplyInputPin}]++
				for _, role := range []string{"request", "response"} {
					pair.Role = role
					wantReplies[pair]++
				}
			}
			for _, edge := range tc.ordinary {
				wantEdges[edge]++
			}
			gotEdges := map[nestedNodeReplyGuardEdge]int{}
			gotReplies := map[pinrouting.ConnectRoutePlanReplyReadback]int{}
			for _, plan := range graph.Plans() {
				readback := plan.Readback()
				gotEdges[nestedNodeReplyGuardEdge{readback.Source.FlowID, readback.Source.Pin, readback.Receiver.FlowID, readback.Receiver.Pin}]++
				if reply := plan.ReplyResolution(); reply != nil {
					gotReplies[reply.Readback()]++
				}
				for _, endpoint := range []pinrouting.ConnectRoutePlanEndpointReadback{readback.Source, readback.Receiver} {
					field, ok := semanticview.ResolveEventSchema(source, endpoint.FlowID, endpoint.Pin).Field("request_id")
					if !ok || field.IsOptional || field.Type.Kind != contracts.CatalogTypeText {
						t.Fatalf("%s.%s requires request_id: text", endpoint.FlowID, endpoint.Pin)
					}
				}
			}
			if !reflect.DeepEqual(gotEdges, wantEdges) || !reflect.DeepEqual(gotReplies, wantReplies) {
				t.Fatalf("exact nested graph changed: edges=%+v want=%+v replies=%+v want=%+v", gotEdges, wantEdges, gotReplies, wantReplies)
			}
			inputs := map[string]bool{}
			for _, pin := range source.FlowInputEventPins(".") {
				inputs[pin.EventType()] = true
			}
			outputs := map[string]bool{}
			for _, pin := range source.FlowOutputEventPins(".") {
				outputs[pin.EventType()] = true
			}
			if !reflect.DeepEqual(inputs, tc.inputs) || !reflect.DeepEqual(outputs, map[string]bool{"request.finished": true}) {
				t.Fatalf("nested proof leaked public pins: inputs=%+v outputs=%+v", inputs, outputs)
			}
			rootScope, ok := semanticview.FlowScopeByID(source, ".")
			if !ok || rootScope.Nodes["stop-node"].EventHandlers["request.stop"].AdvancesTo != "done" {
				t.Fatal("external stop must be the final transition")
			}
			for _, scope := range semanticview.FlowScopes(source) {
				for name, node := range scope.Nodes {
					for event, handler := range node.EventHandlers {
						if handler.AdvancesTo != "" && (scope.ID != "." || name != "stop-node" || event != "request.stop") {
							t.Fatalf("%s.%s/%s ends the proof before external stop", scope.ID, name, event)
						}
					}
				}
			}
			for _, outer := range tc.outer {
				requireNestedNodeReplyGuardEmit(t, source, "provider", "provider-node", outer.ProviderInputPin, "t.requested")
			}
			requireNestedNodeReplyGuardEmit(t, source, "provider", "provider-node", "t.replied", "provider.replied")
			requireNestedNodeReplyGuardEmit(t, source, "t", "t-node", "t.requested", "u.requested")
			requireNestedNodeReplyGuardEmit(t, source, "t", "t-node", "u.replied", "t.replied")
			requireNestedNodeReplyGuardEmit(t, source, "u", "u-node", "u.requested", "u.replied")
			for _, flow := range append([]string{"provider", "t", "u"}, tc.requesters...) {
				for _, file := range []string{"entities.yaml", "agents.yaml"} {
					path := filepath.Join(root, flow, file)
					if _, err := os.Stat(path); !os.IsNotExist(err) {
						t.Fatalf("payload-only receiver must omit %s: %v", path, err)
					}
				}
			}
			if entities, err := os.ReadFile(filepath.Join(root, "entities.yaml")); err != nil || string(entities) != "request_state: {}\n" {
				t.Fatalf("inherited root state changed: %q, %v", entities, err)
			}
		})
	}
}

func requireNestedNodeReplyGuardEmit(t testing.TB, source semanticview.Source, flow, node, input, output string) {
	t.Helper()
	scope, ok := semanticview.FlowScopeByID(source, flow)
	if !ok {
		t.Fatalf("missing concrete flow %s", flow)
	}
	handler, ok := scope.Nodes[node].EventHandlers[input]
	if !ok || handler.Emit.EventType() != output {
		t.Fatalf("%s.%s/%s must emit only %s, got %+v", flow, node, input, output, handler)
	}
}
