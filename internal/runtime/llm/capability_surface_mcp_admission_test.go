package llm

import (
	"context"
	"encoding/json"
	"testing"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/toolgateway"
	"github.com/division-sh/swarm/internal/runtime/workspace"
)

func TestWorkspaceMCPDefinitionAdmissionRejectsEveryChangedPlannedDefinition(t *testing.T) {
	tool := ToolDefinition{Name: "emit_done", Description: "Exact planned output", Schema: map[string]any{"type": "object", "additionalProperties": false}}
	_, plan := testManagedCLISurfaceContext(t, models.AgentConfig{ID: "gateway-agent"}, []ToolDefinition{tool})
	exact := toolgateway.ListedDefinition{Name: tool.Name, Description: tool.Description, InputSchema: json.RawMessage(`{"additionalProperties":false,"type":"object"}`)}
	for _, tc := range []struct {
		name        string
		definitions []toolgateway.ListedDefinition
		valid       bool
	}{
		{"exact_reordered_schema", []toolgateway.ListedDefinition{exact}, true},
		{"missing", nil, false},
		{"duplicate", []toolgateway.ListedDefinition{exact, exact}, false},
		{"foreign", []toolgateway.ListedDefinition{exact, {Name: "foreign", InputSchema: json.RawMessage(`{}`)}}, false},
		{"changed_description", []toolgateway.ListedDefinition{{Name: tool.Name, Description: "Changed", InputSchema: exact.InputSchema}}, false},
		{"changed_schema", []toolgateway.ListedDefinition{{Name: tool.Name, Description: tool.Description, InputSchema: json.RawMessage(`{"type":"object","additionalProperties":true}`)}}, false},
		{"duplicate_schema_key", []toolgateway.ListedDefinition{{Name: tool.Name, Description: tool.Description, InputSchema: json.RawMessage(`{"type":"object","type":"string"}`)}}, false},
		{"absent_schema", []toolgateway.ListedDefinition{{Name: tool.Name, Description: tool.Description}}, false},
		{"null_schema", []toolgateway.ListedDefinition{{Name: tool.Name, Description: tool.Description, InputSchema: json.RawMessage(`null`)}}, false},
		{"scalar_schema", []toolgateway.ListedDefinition{{Name: tool.Name, Description: tool.Description, InputSchema: json.RawMessage(`"object"`)}}, false},
		{"provider_alias", []toolgateway.ListedDefinition{{Name: "mcp__runtime-tools__emit_done", Description: tool.Description, InputSchema: exact.InputSchema}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, err := range []error{validatePlannedWorkspaceMCPDefinitions(plan, tc.definitions), validateWorkspaceMCPDefinitions([]ToolDefinition{tool}, tc.definitions)} {
				if tc.valid {
					if err != nil {
						t.Fatal(err)
					}
					continue
				}
				failure, ok := runtimefailures.As(err)
				if !ok || failure.Failure.Detail.Code != "managed_capability_mcp_definition_mismatch" || failure.Failure.Class == runtimefailures.ClassDependencyUnavailable {
					t.Fatalf("reachable changed definition was not an exact mismatch: %v", err)
				}
			}
		})
	}
}

func TestCLIPlannedMCPAdmissionRequiresConnectedExactProviderEvidence(t *testing.T) {
	_, plan := testManagedCLISurfaceContext(t, models.AgentConfig{ID: "gateway-agent"}, []ToolDefinition{
		{Name: "emit_done", Schema: map[string]any{"type": "object"}},
	})
	for _, tc := range []struct {
		name, inventory, status, code string
	}{
		{"connected_exact", `["mcp__runtime-tools__emit_done"]`, "connected", ""},
		{"missing_server", `["mcp__runtime-tools__emit_done"]`, "", "workspace_gateway_unreachable"},
		{"failed_server", `["mcp__runtime-tools__emit_done"]`, "failed", "workspace_gateway_unreachable"},
		{"connected_missing_tool", `[]`, "connected", "managed_capability_mcp_definition_mismatch"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response := cliInventoryResponseForTest(tc.inventory)
			if tc.status != "" {
				response.MCPServers = map[string]string{"runtime-tools": tc.status}
			}
			observed, err := ObserveCLIResponseCapabilitySurface(plan, response)
			if err != nil {
				t.Fatal(err)
			}
			err = ValidateCLIProviderCapabilitySurface(observed, response)
			if tc.code == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			failure, ok := runtimefailures.As(err)
			if !ok || failure.Failure.Detail.Code != tc.code {
				t.Fatalf("admission = %v, want typed %s", err, tc.code)
			}
			if failure.Failure.Class == runtimefailures.ClassOutcomeUncertain {
				t.Fatal("pre-model admission was classified outcome-uncertain")
			}
		})
	}
}

func TestWorkspaceMCPDisabledBridgeCannotAuthorizePlannedTools(t *testing.T) {
	t.Setenv("SWARM_CLAUDE_USE_MCP", "0")
	tools := []ToolDefinition{{Name: "emit_done", Schema: map[string]any{"type": "object"}}}
	ctx, _ := testManagedCLISurfaceContext(t, models.AgentConfig{ID: "gateway-agent"}, tools)
	target := &workspace.Target{Backend: workspace.BackendHost, Workdir: t.TempDir()}
	_, err := probeWorkspaceMCP(ctx, nil, nil, &Session{AgentID: "gateway-agent", Tools: tools}, toolgateway.Binding{}, target)
	failure, ok := runtimefailures.As(err)
	if !ok || failure.Failure.Detail.Code != "workspace_mcp_transport_required" {
		t.Fatalf("disabled planned bridge admitted a model: %v", err)
	}
	// A native-only/empty plan does not fabricate a required MCP tool.
	nativeCtx, _ := testManagedCLISurfaceContext(t, models.AgentConfig{ID: "native-agent"}, nil)
	if _, err := probeWorkspaceMCP(nativeCtx, nil, nil, &Session{AgentID: "native-agent"}, toolgateway.Binding{}, target); err != nil {
		t.Fatalf("empty plan gained a tool requirement: %v", err)
	}
	if _, err := probeWorkspaceMCP(context.Background(), nil, nil, &Session{}, toolgateway.Binding{}, target); err != nil {
		t.Fatalf("empty unmanaged plan gained a tool requirement: %v", err)
	}
}

func TestCLINativeOnlyAdmissionDoesNotRequireMCP(t *testing.T) {
	_, plan := testManagedCLISurfaceContext(t, models.AgentConfig{ID: "native-agent", NativeTools: models.NativeToolConfig{WebSearch: true}}, nil)
	response := cliInventoryResponseForTest(`["WebFetch","WebSearch"]`)
	observed, err := ObserveCLIResponseCapabilitySurface(plan, response)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateCLIProviderCapabilitySurface(observed, response); err != nil {
		t.Fatal(err)
	}
}
