package runtime

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	llm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimemcp "github.com/division-sh/swarm/internal/runtime/mcp"
)

func TestStartupProbeToolsCallAcceptsOnlyClosedTypedCombinations(t *testing.T) {
	validation := startupFailureEnvelope(t, runtimefailures.New(runtimefailures.ClassSchemaInvalid, "invalid_tool_input", "test", "startup", nil))
	internal := startupFailureEnvelope(t, runtimefailures.New(runtimefailures.ClassInternalFailure, "startup_execution_failed", "test", "startup", nil))
	tests := []struct {
		name      string
		result    map[string]any
		wantOK    bool
		wantClass runtimefailures.Class
	}{
		{"success omitted isError", startupProbeWireResult(runtimemcp.StartupProbeOutcomeSuccess, nil, nil), true, ""},
		{"success explicit false", startupProbeWireResult(runtimemcp.StartupProbeOutcomeSuccess, false, nil), true, ""},
		{"success cannot claim error", startupProbeWireResult(runtimemcp.StartupProbeOutcomeSuccess, true, nil), false, ""},
		{"success cannot carry runtime error", startupProbeWireResult(runtimemcp.StartupProbeOutcomeSuccess, false, validation), false, ""},
		{"validation exact pair", startupProbeWireResult(runtimemcp.StartupProbeOutcomeValidationOnly, true, validation), true, ""},
		{"validation missing typed failure", startupProbeWireResult(runtimemcp.StartupProbeOutcomeValidationOnly, true, nil), false, ""},
		{"validation wrong typed failure", startupProbeWireResult(runtimemcp.StartupProbeOutcomeValidationOnly, true, internal), false, ""},
		{"execution typed failure", startupProbeWireResult(runtimemcp.StartupProbeOutcomeExecutionFailure, true, internal), false, runtimefailures.ClassInternalFailure},
		{"execution cannot use validation pair", startupProbeWireResult(runtimemcp.StartupProbeOutcomeExecutionFailure, true, validation), false, ""},
		{"execution missing typed failure", startupProbeWireResult(runtimemcp.StartupProbeOutcomeExecutionFailure, true, nil), false, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"jsonrpc": "2.0",
					"id":      "startup-tools-call",
					"result":  tt.result,
				})
			}))
			defer server.Close()
			ctx, binding := startupFailureTestBinding(t, server.URL)
			err := startupProbeMCPToolsCall(ctx, server.Client(), binding, "health_check")
			if tt.wantOK {
				if err != nil {
					t.Fatalf("startupProbeMCPToolsCall: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("invalid startup outcome combination was accepted")
			}
			if tt.wantClass != "" {
				failure, ok := runtimefailures.As(err)
				if !ok || failure.Failure.Class != tt.wantClass {
					t.Fatalf("error = %v, want preserved class %s", err, tt.wantClass)
				}
			}
		})
	}
}

func TestStartupCallRejectsManualOrMutatedTransportBinding(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": "startup-tools-call", "result": map[string]any{}})
	}))
	defer server.Close()
	ctx, trusted := startupFailureTestBinding(t, server.URL)

	manual := llm.MCPHTTPBinding{URL: trusted.URL, Headers: trusted.Headers, ContextToken: trusted.ContextToken}
	if _, err := startupCallMCP(ctx, server.Client(), manual, runtimemcp.RPCRequest{JSONRPC: "2.0", Method: "tools/call", ID: "startup-tools-call"}); err == nil || !strings.Contains(err.Error(), "construction provenance") {
		t.Fatalf("manual binding error = %v", err)
	}

	trusted.Headers = map[string]string{
		"Authorization":         "Bearer copied-token",
		"X-SWARM-Context-Token": trusted.ContextToken,
	}
	if _, err := startupCallMCP(ctx, server.Client(), trusted, runtimemcp.RPCRequest{JSONRPC: "2.0", Method: "tools/call", ID: "startup-tools-call"}); err == nil || !strings.Contains(err.Error(), "construction provenance") {
		t.Fatalf("mutated binding error = %v", err)
	}
}

func startupFailureTestBinding(t *testing.T, serverURL string) (context.Context, llm.MCPHTTPBinding) {
	t.Helper()
	t.Setenv("SWARM_CLAUDE_USE_MCP", "1")
	ctx := runtimeactors.WithActor(context.Background(), runtimeactors.AgentConfig{ExecutionMode: "live", ID: "startup-agent"})
	turns := runtimemcp.NewTurnContextRegistry(runtimeactors.ActorFromContext)
	binding, enabled, err := llm.BuildMCPHTTPBinding(
		ctx,
		&config.Config{},
		turns,
		&llm.Session{AgentID: "startup-agent", Tools: []llm.ToolDefinition{{Name: "health_check"}}},
		testToolGatewayBinding(serverURL, serverURL, "gateway-token"),
		llm.MCPGatewayHostEndpoint,
	)
	if err != nil || !enabled || !binding.IsRuntimeOwned() {
		t.Fatalf("BuildMCPHTTPBinding = %#v, %t, %v", binding, enabled, err)
	}
	return ctx, binding
}

func startupFailureEnvelope(t *testing.T, err error) map[string]any {
	t.Helper()
	envelope, ok := runtimefailures.EnvelopeFromError(err)
	if !ok {
		t.Fatalf("missing envelope: %v", err)
	}
	return map[string]any{"failure": envelope}
}

func startupProbeWireResult(outcome runtimemcp.StartupProbeOutcome, isError any, runtimeError any) map[string]any {
	result := map[string]any{
		"content": []any{map[string]any{"type": "text", "text": "presentation only"}},
		"swarmStartupProbe": map[string]any{
			"contract":  runtimemcp.StartupProbeContractManagedAgentCallable,
			"outcome":   outcome,
			"tool_name": "health_check",
		},
	}
	if isError != nil {
		result["isError"] = isError
	}
	if runtimeError != nil {
		result["runtimeError"] = runtimeError
	}
	return result
}
