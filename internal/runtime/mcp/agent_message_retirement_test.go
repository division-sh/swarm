package mcp

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
)

func TestAgentMessageRetirementMCPNormalAndSelectedFork(t *testing.T) {
	for _, kind := range []managedcapabilities.ExecutionKind{managedcapabilities.ExecutionNormalAgent, managedcapabilities.ExecutionSelectedContractFork} {
		for _, name := range []string{"agent_message", "mcp__runtime-tools__agent_message"} {
			t.Run(string(kind)+"/"+name, func(t *testing.T) {
				ctx, surface, harness := managedClaudeProviderTurnTestContext(t, kind)
				registry := NewTurnContextRegistry(models.ActorFromContext)
				token := registry.RegisterTurnContextWithCapabilitySurface(ctx, time.Minute, surface)
				if token == "" {
					t.Fatal("register production-shaped turn")
				}
				executorCalls := 0
				gateway := NewGateway(testToolExecutor(func(context.Context, string, any) (any, error) {
					executorCalls++
					return map[string]any{"ok": true}, nil
				}), testGatewayToken, managedCLIGatewayHooks(registry))
				listed := callMCPGateway(t, gateway, token, RPCRequest{JSONRPC: "2.0", Method: "tools/list", ID: "list-retirement", Params: map[string]any{}})
				body, err := json.Marshal(listed.Result)
				if err != nil || listed.Error != nil || strings.Contains(string(body), "agent_message") || !strings.Contains(string(body), "write_file") {
					t.Fatalf("MCP list lost active tool or published messaging: %+v, %v", listed, err)
				}
				response := callMCPGateway(t, gateway, token, RPCRequest{JSONRPC: "2.0", Method: "tools/call", ID: "call-retirement", Params: map[string]any{
					"name": name, "arguments": map[string]any{}, "_meta": map[string]any{claudeCodeToolUseIDMetaKey: "toolu-retirement"},
				}})
				if result, ok := response.Result.(map[string]any); response.Error == nil && (!ok || result["isError"] != true) {
					t.Fatalf("messaging call admitted: %+v", response)
				}
				if executorCalls != 0 || len(harness.Attempts) != 0 {
					t.Fatalf("retired call reached executor=%d effect_attempts=%d", executorCalls, len(harness.Attempts))
				}
				turn, ok := registry.ResolveTurnContext(token)
				if !ok || turn.CapabilitySurface == nil || !turn.CapabilitySurface.HasMismatch() || len(turn.CapabilitySurface.EffectiveNames()) != 0 {
					t.Fatalf("retired call did not invalidate the exact surface: %+v", turn.CapabilitySurface)
				}
			})
		}
	}
}
