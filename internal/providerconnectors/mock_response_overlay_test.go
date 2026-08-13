package providerconnectors

import (
	"encoding/json"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/scenarioexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestOverlayMockResponsePlanUsesCanonicalToolAdmission(t *testing.T) {
	emptyObject, err := runtimecontracts.NewToolInputSchema(runtimecontracts.ToolSchemaObject)
	if err != nil {
		t.Fatal(err)
	}
	boolean, err := runtimecontracts.NewToolInputSchema(runtimecontracts.ToolSchemaBoolean)
	if err != nil {
		t.Fatal(err)
	}
	output, err := runtimecontracts.NewToolInputSchema(runtimecontracts.ToolSchemaObject,
		runtimecontracts.ToolSchemaProperties(map[string]runtimecontracts.ToolInputSchema{"ok": boolean}),
		runtimecontracts.ToolSchemaRequired("ok"),
	)
	if err != nil {
		t.Fatal(err)
	}
	tool, err := runtimecontracts.NewToolSchemaEntry(
		runtimecontracts.WithToolCategory("provider_connector"),
		runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerHTTP),
		runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{Method: "POST", URL: "https://example.test"}),
		runtimecontracts.WithToolSchemas(emptyObject, output),
	)
	if err != nil {
		t.Fatal(err)
	}
	source, err := semanticview.WithRuntimeTools(emptyMockResponseSource{}, map[string]runtimecontracts.ToolSchemaEntry{"provider.send": tool})
	if err != nil {
		t.Fatal(err)
	}
	digest, err := tool.OutputSchema().CanonicalHash()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := OverlayMockResponsePlan(nil, source, []scenarioexecution.ConnectorResponse{{
		ToolID: "provider.send", OutputSchemaDigest: digest, Response: json.RawMessage(`{"ok":true}`),
	}})
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := plan.Admit("provider.send", tool)
	if err != nil {
		t.Fatal(err)
	}
	got, err := admitted.Materialize()
	if err != nil || got.(map[string]any)["ok"] != true {
		t.Fatalf("materialize = %#v, %v", got, err)
	}
	_, err = OverlayMockResponsePlan(plan, source, []scenarioexecution.ConnectorResponse{{
		ToolID: "provider.send", OutputSchemaDigest: "sha256:" + strings.Repeat("0", 64), Response: json.RawMessage(`{"ok":false}`),
	}})
	if err == nil || !strings.Contains(err.Error(), "output_schema digest mismatch") {
		t.Fatalf("digest mismatch error = %v", err)
	}
}

type emptyMockResponseSource struct{ semanticview.Source }

func (emptyMockResponseSource) SemanticCapabilities() semanticview.Capabilities {
	return semanticview.Capabilities{}
}

func (emptyMockResponseSource) ToolEntries() map[string]runtimecontracts.ToolSchemaEntry {
	return map[string]runtimecontracts.ToolSchemaEntry{}
}
