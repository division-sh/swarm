package tools

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestPrivateToolNameInventoryAdmissionAndCanonicalModules(t *testing.T) {
	if len(RuntimeAvailableToolNamesForSource(nil)) == 0 {
		t.Fatal("ordinary platform inventory disappeared")
	}
	invalid := semanticview.Wrap(&contracts.WorkflowContractBundle{Tools: map[string]contracts.ToolSchemaEntry{"Read": inProcessSendEntry()}})
	if names := RuntimeAvailableToolNamesForSource(invalid); len(names) != 0 {
		t.Errorf("unqualified private source advertised alternate inventory: %v", names)
	}
	for _, kind := range []string{"wasm", "python"} {
		t.Run(kind, func(t *testing.T) {
			abi, entry, handler := "core-json-v1", "compute", contracts.ToolHandlerWasm
			if kind == "python" {
				abi, entry, handler = "python-json-v1", "handle", contracts.ToolHandlerPython
			}
			tool := contracts.MustToolSchemaEntry(contracts.WithToolHandler(handler), contracts.WithToolModule(contracts.PolicyModule{
				Kind: kind, Path: "modules/pinned.bin", ABI: abi, Entry: entry, Digest: "sha256:" + strings.Repeat("0", 64),
				InputSchema:  map[string]any{"type": "object", "properties": map[string]any{"value": map[string]any{"type": "integer"}}},
				OutputSchema: map[string]any{"type": "object", "properties": map[string]any{"value": map[string]any{"type": "integer"}}},
				Limits:       contracts.PolicyModuleLimits{Gas: 100, MemoryPages: 16, OutputBytes: 1024}}))
			source := semanticview.Wrap(&contracts.WorkflowContractBundle{Tools: map[string]contracts.ToolSchemaEntry{"schedule": tool}})
			for _, name := range RuntimeAvailableToolNamesForSource(source) {
				if name == "schedule" {
					t.Fatal("canonical private module resurrected a builtin in inventory")
				}
			}
		})
	}
}
