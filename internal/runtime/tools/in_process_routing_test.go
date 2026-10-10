package tools

import (
	"context"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/toolidentity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/yamlsource"
)

// Preserve the reviewer's admitted-YAML reproduction, not a preselected resolver.
func TestInProcessCanonicalAliasCannotResurrectBuiltin(t *testing.T) {
	pairs := [][2]string{{"Read", "read_file"}, {"Write", "write_file"}, {"Edit", "write_file"}, {"Bash", "bash"}, {"WebSearch", "web_search"}, {"WebFetch", "web_search"}}
	for _, pair := range append(append([][2]string{}, pairs...), [2]string{"read_file", "read_file"}, [2]string{"write_file", "write_file"}, [2]string{"bash", "bash"}, [2]string{"web_search", "web_search"}) {
		pairs = append(pairs, [2]string{toolidentity.RuntimeToolsMCPPrefix + pair[0], pair[1]})
	}
	pairs = append(pairs, [2]string{"emit_probe", "emit_probe"}, [2]string{toolidentity.RuntimeToolsMCPPrefix + "emit_probe", "emit_probe"})
	for _, pair := range pairs {
		t.Run(pair[0], func(t *testing.T) {
			snapshot, err := yamlsource.Load([]byte(pair[0] + ":\n  category: provider_connector\n  handler_type: in_process\n  effect_class: non_idempotent_write\n  in_process: whatsapp.send_text\n"))
			if err != nil {
				t.Fatal(err)
			}
			entries, err := contracts.AdmitToolDeclarationsValue(snapshot.Document("tools.yaml").Root())
			if err == nil || !strings.Contains(err.Error(), pair[0]) || !strings.Contains(err.Error(), "private tool declaration") {
				t.Fatalf("ambiguous private source reached routing instead of exact admission refusal: %v", err)
			}
			// A typed reconstruction cannot bypass the same declaration owner.
			entries = map[string]contracts.ToolSchemaEntry{pair[0]: inProcessSendEntry()}
			bundle := &contracts.WorkflowContractBundle{Tools: entries}
			toolTestDeclareAgent(t, bundle, "caller", ".")
			bundle.FlowTree.Root.Tools = bundle.Tools
			source := semanticview.Wrap(bundle)
			if _, err := ValidateToolImplementations(source); err == nil || !strings.Contains(err.Error(), pair[0]) || !strings.Contains(err.Error(), "private tool declaration") {
				t.Fatalf("runtime qualification lost exact private-name admission: %v", err)
			}
			actor := actors.AgentConfig{ID: "caller", FlowID: ".", NativeTools: actors.NativeToolConfig{FileIO: true, Bash: true, WebSearch: true}}
			calls := 0
			d := NewToolDispatcher(func(context.Context, actors.AgentConfig, string, any) (any, error) { calls++; return nil, nil }, func(_ context.Context, actor actors.AgentConfig, name string) (ExecutionTool, bool, error) {
				return resolveExecutionToolForActor(source, actor, name, nil)
			}, nil, nil, nil, nil, map[string]ToolHandler{pair[1]: func(context.Context, actors.AgentConfig, any) (any, error) { calls++; return nil, nil }})
			for _, requested := range []string{pair[0], pair[1]} {
				_, err = d.Dispatch(context.Background(), actor, requested, map[string]any{})
				if err == nil || calls != 0 {
					t.Fatalf("private declared name %q dispatched alternate handler %q via %q: calls=%d err=%v", pair[0], pair[1], requested, calls, err)
				}
			}
		})
	}
}

func TestInProcessRoutingScopesPublicWrapperAndGrants(t *testing.T) {
	for _, name := range []string{"Read", "emit_probe", "mcp__runtime-tools__Read", "mcp__runtime-tools__emit_probe"} {
		for _, scope := range []string{"root", "child", "inherited", "wrong_kind_global"} {
			t.Run(scope+"/"+name, func(t *testing.T) {
				entries := map[string]contracts.ToolSchemaEntry{name: inProcessSendEntry()}
				root := &contracts.FlowContractView{Paths: contracts.FlowContractPaths{FlowPath: "."}, Tools: entries,
					Children: []contracts.FlowContractView{{Paths: contracts.FlowContractPaths{FlowPath: "child"}}}}
				bundle := &contracts.WorkflowContractBundle{Tools: entries, FlowTree: contracts.FlowTree{Root: root}}
				actor := actors.AgentConfig{ID: "caller", FlowID: ".", NativeTools: actors.NativeToolConfig{FileIO: true}}
				if scope != "root" {
					actor.FlowID = "child"
				}
				if scope == "child" {
					root.Children[0].Tools, root.Tools, bundle.Tools = entries, nil, nil
				}
				if scope == "wrong_kind_global" {
					object := contracts.MustToolInputSchema(contracts.ToolSchemaObject)
					bundle.Tools = map[string]contracts.ToolSchemaEntry{name: contracts.MustToolSchemaEntry(
						contracts.WithToolHandler(contracts.ToolHandlerPlatformBuiltin), contracts.WithToolSchemas(object, object))}
				}
				toolTestDeclareAgent(t, bundle, actor.ID, actor.FlowID)
				source := semanticview.Wrap(bundle)
				executor := NewExecutorWithOptions(nil, ExecutorOptions{WorkflowSource: source})
				calls := 0
				executor.dispatcher.emitHandler = func(context.Context, actors.AgentConfig, string, any) (any, error) {
					calls++
					return nil, nil
				}
				executor.dispatcher.handlers[toolidentity.CanonicalName(name)] = func(context.Context, actors.AgentConfig, any) (any, error) {
					calls++
					return nil, nil
				}
				for _, requested := range []string{name, toolidentity.RuntimeToolsMCPPrefix + name} {
					if _, err := executor.Execute(WithActor(context.Background(), actor), requested, map[string]any{}); err == nil || !strings.Contains(err.Error(), "private tool") || calls != 0 {
						t.Fatalf("public wrapper lost %s/%s declaration: calls=%d err=%v", scope, requested, calls, err)
					}
					if err := agentModuleGrantError(source, actor.FlowID, requested); err == nil {
						t.Fatal("permission admission lost private declaration", requested)
					}
					granted := actor
					granted.Tools = []string{requested}
					if _, err := executionToolsForActor(source, granted, nil); err == nil {
						t.Fatal("aliased grant acquired an executable catalog entry", requested)
					}
				}
				for _, def := range executor.ToolDefinitionsForActor(actor) {
					if def.Name == name {
						t.Fatal("private declaration advertised by public discovery", name)
					}
				}
			})
		}
	}
}
