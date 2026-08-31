package mcp_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimemcp "github.com/division-sh/swarm/internal/runtime/mcp"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/google/uuid"
)

func TestSelectedRetiredDynamicAgentToolsNeverReachCLIOrMCPListProjection(t *testing.T) {
	for _, name := range []string{"agent_hire", "agent_fire", "agent_reconfigure"} {
		t.Run(name, func(t *testing.T) {
			source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
				Agents: map[string]runtimecontracts.AgentRegistryEntry{
					"worker": {ID: "worker", Tools: []string{name}},
				},
				Tools: map[string]runtimecontracts.ToolSchemaEntry{
					name: runtimecontracts.MustToolSchemaEntry(
						runtimecontracts.WithToolDescription("hostile selected-source fixture"),
						runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerHTTP),
						runtimecontracts.WithToolSchemas(
							runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject),
							runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject),
						),
						runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{Method: "POST", URL: "https://example.invalid"}),
					),
				},
			})
			executor := runtimetools.NewExecutorWithOptions(nil, runtimetools.ExecutorOptions{WorkflowSource: source})
			actor := models.AgentConfig{ID: "worker", ExecutionMode: "live", Tools: []string{name}}
			registry := runtimemcp.NewTurnContextRegistry(models.ActorFromContext)
			gateway := runtimemcp.NewGateway(executor, "retired-tool-test", runtimemcp.GatewayHooks{
				WithActor:          models.WithActor,
				ActorFromContext:   models.ActorFromContext,
				ResolveTurnContext: registry.ResolveTurnContext,
			})
			ctx := models.WithActor(context.Background(), actor)
			ctx = runtimeeffects.WithAuthority(ctx, retiredToolConversationForkAuthority())
			token := registry.RegisterConversationForkSandboxTurnContext(ctx, time.Minute, []string{name})
			if token == "" {
				t.Fatal("register CLI/MCP turn context")
			}

			body := `{"jsonrpc":"2.0","id":"list","method":"tools/list","params":{}}`
			req := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(body))
			req.Header.Set("Authorization", "Bearer retired-tool-test")
			req.Header.Set("X-SWARM-Context-Token", token)
			rec := httptest.NewRecorder()
			gateway.Handler().ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("tools/list status = %d body=%s", rec.Code, rec.Body.String())
			}
			var response struct {
				Result struct {
					Tools []runtimemcp.ToolDef `json:"tools"`
				} `json:"result"`
				Error *runtimemcp.RPCError `json:"error"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
				t.Fatalf("decode tools/list response: %v", err)
			}
			if response.Error != nil {
				t.Fatalf("tools/list error = %#v", response.Error)
			}
			if len(response.Result.Tools) != 0 {
				t.Fatalf("CLI/MCP tools/list projection retained %s: %#v", name, response.Result.Tools)
			}
		})
	}
}

func retiredToolConversationForkAuthority() runtimeeffects.Authority {
	forkTurnID := uuid.NewString()
	return runtimeeffects.Authority{
		Kind:            runtimeeffects.AuthorityConversationForkChat,
		ID:              forkTurnID,
		ExecutionOwner:  "retired-tool-test",
		LeaseExpiresAt:  time.Now().UTC().Add(time.Minute),
		FenceGeneration: 1,
		ExecutionMode:   runtimeeffects.ExecutionModeLive,
		ForkChat: runtimeeffects.ConversationForkChatAuthority{
			ForkTurnID:          forkTurnID,
			ForkID:              uuid.NewString(),
			BundleHash:          "bundle-v2:sha256:" + strings.Repeat("a", 64),
			ActorTokenID:        "retired-tool-test",
			RequestOccurrenceID: uuid.NewString(),
			RequestHash:         "retired-tool-test",
		},
	}
}
