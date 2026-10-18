package tools

import (
	"context"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimemcp "github.com/division-sh/swarm/internal/runtime/mcp"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestInProcessToolRuntimeProjectionKeepsTargetWithoutFallback(t *testing.T) {
	entry := contracts.MustToolSchemaEntry(contracts.WithToolCategory("provider_connector"),
		contracts.WithToolHandler(contracts.ToolHandlerInProcess), contracts.WithToolEffect(contracts.ActivityEffectClassNonIdempotentWrite),
		contracts.WithToolSchemas(contracts.MustToolInputSchema(contracts.ToolSchemaObject), contracts.MustToolInputSchema(contracts.ToolSchemaObject)),
		contracts.WithToolInProcessTarget(contracts.ToolInProcessWhatsAppSendText))
	tool, found := executionToolFromAdmitted("send", entry)
	target, native := tool.InProcess()
	if found || !native || target != contracts.ToolInProcessWhatsAppSendText {
		t.Fatal("runtime view dropped compiled target")
	}
	if _, http := tool.HTTPExecution(); http {
		t.Fatal("native tool gained an HTTP execution recipe")
	}
	source := semanticview.Wrap(&contracts.WorkflowContractBundle{Tools: map[string]contracts.ToolSchemaEntry{"send": entry}})
	if _, err := ValidateToolImplementations(source); err == nil {
		t.Fatal("intent-only declaration advertised an installed execution owner")
	}
	calls := 0
	dispatcher := NewToolDispatcher(nil,
		func(context.Context, actors.AgentConfig, string) (ExecutionTool, bool, error) { return tool, true, nil },
		func(context.Context, actors.AgentConfig, ExecutionTool, any) (any, error) { calls++; return nil, nil },
		nil, nil, nil,
		map[string]ToolHandler{"send": func(context.Context, actors.AgentConfig, any) (any, error) { calls++; return nil, nil }})
	if _, err := dispatcher.Dispatch(context.Background(), actors.AgentConfig{ID: "caller"}, "send", map[string]any{}); err == nil || calls != 0 {
		t.Fatal("uninstalled native operation fell back to HTTP or a named callback", err)
	}
}

func inProcessSendEntry() contracts.ToolSchemaEntry {
	return contracts.MustToolSchemaEntry(contracts.WithToolCategory("provider_connector"),
		contracts.WithToolHandler(contracts.ToolHandlerInProcess), contracts.WithToolEffect(contracts.ActivityEffectClassNonIdempotentWrite),
		contracts.WithToolSchemas(contracts.MustToolInputSchema(contracts.ToolSchemaObject), contracts.MustToolInputSchema(contracts.ToolSchemaObject)),
		contracts.WithToolInProcessTarget(contracts.ToolInProcessWhatsAppSendText))
}

func TestInProcessRuntimeDiscoveryAndRealResolverRefuseFallback(t *testing.T) {
	for _, name := range []string{"send", "read_file", "write_file", " read_file ", "READ_FILE"} {
		for _, scope := range []string{"source_only", "root", "child", "inherited"} {
			t.Run(scope+"/"+name, func(t *testing.T) {
				name := strings.TrimSpace(name)
				bundle := &contracts.WorkflowContractBundle{Tools: map[string]contracts.ToolSchemaEntry{name: inProcessSendEntry()}}
				actor := actors.AgentConfig{ID: "caller", FlowID: ".", NativeTools: actors.NativeToolConfig{FileIO: true}}
				if scope != "source_only" {
					if scope != "root" {
						actor.FlowID = "child"
						bundle.FlowTree.Root = &contracts.FlowContractView{Paths: contracts.FlowContractPaths{FlowPath: "."},
							Children: []contracts.FlowContractView{{Paths: contracts.FlowContractPaths{FlowPath: "child"}}}}
					}
					toolTestDeclareAgent(t, bundle, actor.ID, actor.FlowID)
					bundle.FlowTree.Root.Tools = bundle.Tools
					if scope == "child" {
						bundle.FlowTree.Root.Children[0].Tools = bundle.Tools
						bundle.FlowTree.Root.Tools = nil
						bundle.Tools = nil
					}
				}
				source := semanticview.Wrap(bundle)
				if _, err := ValidateToolImplementations(source); err == nil && scope != "child" {
					t.Fatal("uninstalled root target passed qualification")
				}
				for _, defs := range []func() error{
					func() error {
						definitions, err := ContractDefinitionsForSource(source)
						for _, def := range definitions {
							if def.Name == name && err == nil {
								t.Fatal("private native intent in source definitions")
							}
						}
						return err
					},
					func() error {
						definitions, err := toolDefinitionsForActor(source, actor, nil)
						for _, def := range definitions {
							if def.Name == name && err == nil {
								t.Fatal("private native intent in actor definitions")
							}
						}
						return err
					},
				} {
					_ = defs() // An explicit qualification refusal is also closed.
				}
				for _, candidate := range RuntimeAvailableToolNamesForSource(source) {
					if candidate == name {
						t.Fatal("private native intent in available-name projection")
					}
				}
				calls := 0
				resolver := func(_ context.Context, actor actors.AgentConfig, name string) (ExecutionTool, bool, error) {
					return resolveExecutionToolForActor(source, actor, name, nil)
				}
				dispatcher := NewToolDispatcher(nil, resolver, nil, nil, nil, nil,
					map[string]ToolHandler{name: func(context.Context, actors.AgentConfig, any) (any, error) { calls++; return nil, nil }})
				if _, err := dispatcher.Dispatch(context.Background(), actor, name, map[string]any{}); err == nil || calls != 0 {
					t.Fatalf("native target replaced by callback: calls=%d err=%v", calls, err)
				}
				actor.Tools = []string{name}
				if _, err := executionToolsForActor(source, actor, nil); err == nil {
					t.Fatal("native provider target granted direct agent execution")
				}
			})
		}
	}
}

func TestInProcessDiscoveredMCPDoesNotReplaceDeclaration(t *testing.T) {
	const name = "provider.send"
	source := semanticview.Wrap(&contracts.WorkflowContractBundle{Tools: map[string]contracts.ToolSchemaEntry{name: inProcessSendEntry()}})
	discovered := map[string]runtimemcp.DiscoveredTool{name: {Name: name, Contract: contracts.MustToolSchemaEntry(
		contracts.WithToolHandler(contracts.ToolHandlerMCP),
		contracts.WithToolSchemas(contracts.MustToolInputSchema(contracts.ToolSchemaObject), contracts.MustToolInputSchema(contracts.ToolSchemaObject)),
		contracts.WithToolMCP(contracts.MustToolMCPBinding("other-server", "send")))}}
	if _, err := executionToolsForRuntime(source, discovered); err == nil {
		t.Fatal("discovered transport adopted private native declaration")
	}
}

func TestInProcessScopedOwnerBlocksGlobalWrongKindAndPermissionGrants(t *testing.T) {
	root := &contracts.FlowContractView{Paths: contracts.FlowContractPaths{FlowPath: "."},
		Tools:    map[string]contracts.ToolSchemaEntry{"read_file": inProcessSendEntry()},
		Children: []contracts.FlowContractView{{Paths: contracts.FlowContractPaths{FlowPath: "child"}}}}
	object := contracts.MustToolInputSchema(contracts.ToolSchemaObject)
	bundle := &contracts.WorkflowContractBundle{FlowTree: contracts.FlowTree{Root: root},
		Tools: map[string]contracts.ToolSchemaEntry{"read_file": contracts.MustToolSchemaEntry(contracts.WithToolHandler(contracts.ToolHandlerPlatformBuiltin), contracts.WithToolSchemas(object, object))}}
	toolTestDeclareAgent(t, bundle, "caller", "child")
	source := semanticview.Wrap(bundle)
	actor := actors.AgentConfig{ID: "caller", FlowID: "child", NativeTools: actors.NativeToolConfig{FileIO: true}}
	if _, found, err := resolveExecutionToolForActor(source, actor, "read_file", nil); err == nil || found {
		t.Fatalf("global wrong-kind projection replaced nearest private target: found=%t %v", found, err)
	}
	if err := agentModuleGrantError(source, "child", "read_file"); err == nil || !strings.Contains(err.Error(), "private activities") {
		t.Fatalf("permission admission lost the native private-activity teaching error: %v", err)
	}
	actor.Tools = []string{"read_file"}
	if _, err := executionToolsForActor(source, actor, nil); err == nil {
		t.Fatal("ancestor private native target granted through wrong-kind global entry")
	}
}
