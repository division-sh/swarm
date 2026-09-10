package runtime_test

import (
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/providerconnectors"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestBoundedConnectorSourceRetainsAdmittedStageDeclarations(t *testing.T) {
	source := telegramConnectorSupportedSurfaceSource(t, "http://127.0.0.1", boundedProviderFlowID)
	graph, ok := semanticview.WorkflowStageTopology(source, boundedProviderFlowID)
	if !ok || graph.FlowID != boundedProviderFlowID {
		t.Fatalf("missing selected connector topology: %#v", graph)
	}
	// The shared inbound fixture materializes this authored stage before dispatch.
	if graph.InitialStage != "active" || !reflect.DeepEqual(graph.Stages, []string{"active"}) {
		t.Fatalf("connector source lost its admitted stage declarations: %#v", graph)
	}
}

func TestConnectorPackSourcePreservesCompiledStatelessTopology(t *testing.T) {
	bundle := loadRuntimeTempBundle(t, map[string]string{
		"schema.yaml":       "name: stateless-connector\nimports:\n  connector_packs:\n    - provider: telegram\n      tool: telegram.send_message\n",
		"child/schema.yaml": "name: child\nmode: static\nstages: []\n",
	})
	base := semanticview.Wrap(bundle)
	imported, err := providerconnectors.SourceWithConnectorPackImports(base, telegramConnectorSupportedSurfacePackRegistry(t, "http://127.0.0.1"))
	if err != nil {
		t.Fatal(err)
	}
	for _, flow := range []string{".", "child"} {
		want, ok := semanticview.WorkflowStageTopology(base, flow)
		if !ok || want.FlowID != flow || want.InitialStage != "" || len(want.Stages) != 0 || len(want.Edges) != 0 {
			t.Fatalf("loaded stateless flow %s lacks canonical empty topology: %#v", flow, want)
		}
		got, ok := semanticview.WorkflowStageTopology(imported, flow)
		if !ok || !reflect.DeepEqual(got, want) {
			t.Fatalf("connector overlay lost stateless topology for %s: %#v", flow, got)
		}
	}
	if _, ok := semanticview.WorkflowStageTopology(imported, "missing"); ok {
		t.Fatal("undeclared flow received a stateless fallback topology")
	}
}
