package tools

import (
	"context"
	"errors"
	"reflect"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/workspace"
)

func TestActorToolAdmissionMatchesExecutorWithoutConstructingOne(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"owner_probe": runtimecontracts.MustToolSchemaEntry(
				runtimecontracts.WithToolDescription("immutable owner probe"),
				runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerHTTP),
				runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject), runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject)),
				runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{Method: "POST", URL: "https://never-called.invalid"}),
			),
		},
	})
	for _, permissions := range [][]string{nil, {"human_communication"}} {
		actor := models.AgentConfig{ID: "worker", ExecutionMode: "live", Tools: []string{"owner_probe"}, Permissions: permissions}
		definitions, capabilities, err := PrepareActorToolAdmission(context.Background(), source, actor, nil, NativeToolAdmissionOptions{})
		if err != nil {
			t.Fatal(err)
		}
		// The production constructor is a comparison witness only. Admission
		// above needs neither this executor nor an authority/presentation.
		executor := NewExecutorWithOptions(nil, ExecutorOptions{WorkflowSource: source})
		if actual := executor.ToolDefinitionsForActor(actor); !reflect.DeepEqual(definitions, actual) {
			t.Fatalf("static and production definition owners disagree: static=%v production=%v", toolDefinitionNames(definitions), toolDefinitionNames(actual))
		}
		if !containsToolName(toolDefinitionNames(definitions), "owner_probe") {
			t.Fatal("explicitly granted source tool missing")
		}
		if actual := executor.ToolCapabilitiesForActor(actor, toolDefinitionNames(definitions), nil); !reflect.DeepEqual(capabilities, actual) {
			t.Fatalf("static and production grant owners disagree: static=%+v production=%+v", capabilities, actual)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := PrepareActorToolAdmission(ctx, source, models.AgentConfig{}, nil, NativeToolAdmissionOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("caller cancellation lost: %v", err)
	}
	if _, _, err := PrepareActorToolAdmission(context.Background(), nil, models.AgentConfig{}, nil, NativeToolAdmissionOptions{}); err == nil {
		t.Fatal("missing source became empty catalog success")
	}
}

func TestActorToolAdmissionUsesCapabilityResolverForNativeFallback(t *testing.T) {
	source := wrapRootAgentBundle(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{"worker": {ID: "worker", NativeTools: map[string]any{"file_io": true}}},
	})
	actor := models.AgentConfig{ID: "worker", ExecutionMode: "live", NativeTools: models.NativeToolConfig{FileIO: true}}
	for _, contract := range []llm.ProviderContract{llm.AnthropicAPIProviderContract(), llm.ClaudeCLIProviderContract()} {
		t.Run(contract.RuntimeMode, func(t *testing.T) {
			regular, admission := 0, 0
			resolver := nativeCapabilityAdmissionWorkspace{regularCalls: &regular, admissionCalls: &admission, target: &workspace.Target{Backend: workspace.BackendHost, Workdir: t.TempDir()}}
			definitions, capabilities, err := PrepareActorToolAdmission(context.Background(), source, actor, nil, NativeToolAdmissionOptions{ProviderContract: contract, Workspaces: resolver})
			if err != nil {
				t.Fatal(err)
			}
			planned, err := llm.CompileManagedCapabilityAdmission(actor, contract, definitions, capabilities)
			if err != nil {
				t.Fatal(err)
			}
			if regular != 0 {
				t.Fatalf("admission resolved an execution workspace %d times", regular)
			}
			if contract.RuntimeMode == llm.AnthropicAPIProviderContract().RuntimeMode && (admission == 0 || !containsToolName(toolDefinitionNames(definitions), "read_file") || !containsToolName(toolDefinitionNames(definitions), "write_file")) {
				t.Fatalf("fallback capability lost: inspections=%d names=%v", admission, toolDefinitionNames(definitions))
			}
			found := map[string]bool{}
			for _, tool := range planned {
				found[tool.Name] = tool.Capability.Visible && tool.Capability.Callable
			}
			if !found["read_file"] || !found["write_file"] {
				t.Fatalf("native/fallback capability input not admitted: %+v", planned)
			}
		})
	}
}
