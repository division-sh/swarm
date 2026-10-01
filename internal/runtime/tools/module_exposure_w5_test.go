package tools

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	contracts "github.com/division-sh/swarm/internal/runtime/contracts"
	actors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimemcp "github.com/division-sh/swarm/internal/runtime/mcp"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/yamlsource"
)

func TestW5ModulesCannotAcquireAgentAuthorityThroughGrantsCandidatesOrNativeFallback(t *testing.T) {
	const schema = "{type: object, properties: {value: {type: integer}}}"
	for _, kind := range []string{"wasm", "python"} {
		for _, name := range []string{"compute_only", "bash", "read_file"} {
			t.Run(kind+"/"+name, func(t *testing.T) {
				body := name + ":\n  handler_type: " + kind + "\n  path: modules/pinned.bin\n  abi: core-json-v1\n  entry: compute\n  digest: sha256:" + strings.Repeat("0", 64) + "\n  input_schema: " + schema + "\n  output_schema: " + schema + "\n  limits: {gas: 100, memory_pages: 16, output_bytes: 1024}\n"
				if kind == "python" {
					body = strings.ReplaceAll(strings.ReplaceAll(body, "core-json-v1", "python-json-v1"), "entry: compute", "entry: handle")
				}
				snapshot, err := yamlsource.Load([]byte(body))
				if err != nil {
					t.Fatal(err)
				}
				entries, err := contracts.AdmitToolDeclarationsValue(snapshot.Document("tools.yaml").Root())
				if err != nil {
					t.Fatal(err)
				}
				bundle := &contracts.WorkflowContractBundle{Tools: entries}
				source := wrapRootAgentBundle(bundle)
				actor := actors.AgentConfig{ID: "worker", FlowID: ".", NativeTools: actors.NativeToolConfig{Bash: true, FileIO: true}}
				discovered := map[string]runtimemcp.DiscoveredTool{name: {
					Name: name,
					Contract: contracts.MustToolSchemaEntry(
						contracts.WithToolHandler(contracts.ToolHandlerMCP),
						contracts.WithToolSchemas(contracts.MustToolInputSchema(contracts.ToolSchemaObject), contracts.MustToolInputSchema(contracts.ToolSchemaObject)),
						contracts.WithToolMCP(contracts.MustToolMCPBinding("hostile", name)),
					),
				}}
				if _, err := executionToolsForActor(source, actor, discovered); err == nil || !strings.Contains(err.Error(), "cannot acquire a discovered") {
					t.Fatalf("discovery resurrected module: %v", err)
				}
				unprojected := semanticview.Wrap(&contracts.WorkflowContractBundle{Tools: entries})
				if _, ok, err := resolveExecutionToolForActor(unprojected, actor, name, nil); err != nil || ok {
					t.Fatalf("unprojected actor resurrected module/native binding: %t %v", ok, err)
				}
				toolTestDeclareAgent(t, bundle, actor.ID, ".")
				bundle.FlowTree.Root.Tools = entries
				for _, definitions := range []func() error{
					func() error {
						defs, err := toolDefinitionsForRuntime(source, nil)
						for _, def := range defs {
							if def.Name == name {
								t.Fatal("module in runtime tools/list")
							}
						}
						return err
					},
					func() error {
						defs, err := toolDefinitionsForActor(source, actor, nil)
						for _, def := range defs {
							if def.Name == name {
								t.Fatal("module in actor tools/list")
							}
						}
						return err
					},
				} {
					if err := definitions(); err != nil {
						t.Fatal(err)
					}
				}
				if _, ok, err := resolveExecutionToolForActor(source, actor, name, nil); err != nil || ok {
					t.Fatalf("module resolved for direct call: %t %v", ok, err)
				}
				var effects atomic.Int32
				dispatcher := NewToolDispatcher(nil, func(_ context.Context, a actors.AgentConfig, n string) (ExecutionTool, bool, error) {
					return resolveExecutionToolForActor(source, a, n, nil)
				}, nil, nil, nil, nil, map[string]ToolHandler{name: func(context.Context, actors.AgentConfig, any) (any, error) { effects.Add(1); return nil, nil }})
				if _, err := dispatcher.Dispatch(context.Background(), actor, name, map[string]any{}); err == nil {
					t.Fatal("module dispatched to agent handler")
				}
				actor.Tools = []string{name}
				if _, err := executionToolsForActor(source, actor, nil); err == nil || !strings.Contains(err.Error(), "cannot be granted") {
					t.Fatalf("forged grant: %v", err)
				}
				if _, err := dispatcher.Dispatch(context.Background(), actor, name, map[string]any{}); err == nil {
					t.Fatal("forged grant dispatched")
				}
				if effects.Load() != 0 {
					t.Fatal("module reached agent executor")
				}
			})
		}
	}
}

func TestW5ModuleAncestorShadowsGlobalAndNativeAgentBindings(t *testing.T) {
	module := contracts.PolicyModule{Kind: "wasm", Path: "pinned.wasm", ABI: "core-json-v1", Entry: "compute", Digest: "sha256:" + strings.Repeat("0", 64), InputSchema: map[string]any{"type": "object", "properties": map[string]any{"n": map[string]any{"type": "integer"}}}, OutputSchema: map[string]any{"type": "object", "properties": map[string]any{"n": map[string]any{"type": "integer"}}}, Limits: contracts.PolicyModuleLimits{Gas: 10, MemoryPages: 1, OutputBytes: 1024}}
	entry := contracts.MustToolSchemaEntry(contracts.WithToolHandler(contracts.ToolHandlerWasm), contracts.WithToolModule(module))
	root := &contracts.FlowContractView{Paths: contracts.FlowContractPaths{FlowPath: "."}, Tools: map[string]contracts.ToolSchemaEntry{"bash": entry}, Children: []contracts.FlowContractView{{Paths: contracts.FlowContractPaths{FlowPath: "child"}, Agents: map[string]contracts.AgentRegistryEntry{"worker": {}}}}}
	bundle := &contracts.WorkflowContractBundle{FlowTree: contracts.FlowTree{Root: root}}
	// The global projection deliberately contains an ordinary same-name binding;
	// nearest scoped declarations must win before native fallback or kind checks.
	bundle.Tools = map[string]contracts.ToolSchemaEntry{"bash": contracts.MustToolSchemaEntry(contracts.WithToolHandler(contracts.ToolHandlerPlatformBuiltin), contracts.WithToolSchemas(contracts.MustToolInputSchema(contracts.ToolSchemaObject), contracts.MustToolInputSchema(contracts.ToolSchemaObject)))}
	toolTestDeclareAgent(t, bundle, "worker", "child")
	source := semanticview.Wrap(bundle)
	actor := actors.AgentConfig{ID: "worker", FlowID: "child", NativeTools: actors.NativeToolConfig{Bash: true}}
	if _, ok, err := resolveExecutionToolForActor(source, actor, "bash", nil); err != nil || ok {
		t.Fatalf("ancestor module resurrected native binding: %t %v", ok, err)
	}
}
