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
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
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
			actor.Identity = agentidentitytest.RootRuntime(t, actor.ID, "retired-tool-test")
			registry := runtimemcp.NewTurnContextRegistry(models.ActorFromContext)
			gateway := runtimemcp.NewGateway(executor, "retired-tool-test", runtimemcp.GatewayHooks{
				WithActor:          models.WithActor,
				ActorFromContext:   models.ActorFromContext,
				ResolveTurnContext: registry.ResolveTurnContext,
			})
			ctx := models.WithActor(context.Background(), actor)
			if definitions := executor.ToolDefinitionsForActor(actor); len(definitions) != 0 {
				t.Fatalf("selected catalog retained %s: %+v", name, definitions)
			}
			// The selected workflow catalogue is a managed-agent projection, not
			// the forensic fork sandbox's separate canonical stub policy.
			plan, err := actor.Identity.Plan()
			if err != nil {
				t.Fatal(err)
			}
			surface, err := managedcapabilities.New(managedcapabilities.Plan{
				ActorPlan: plan, RuntimeMode: "startup_probe", Provider: "claude", Transport: "cli", ProviderContract: "retired-tool-test",
				Authority: managedcapabilities.Authority{
					Kind: managedcapabilities.AuthorityStartupProbe, ID: uuid.NewString(),
					ExecutionKind: managedcapabilities.ExecutionNormalAgent, ExecutionAuthorityID: actor.ID,
					StartupOwnerID: "retired-tool-test", StartupGeneration: 1,
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			token := registry.RegisterTurnContextWithCapabilitySurface(ctx, time.Minute, surface)
			if token == "" {
				t.Fatal("register CLI/MCP turn context")
			}
			defer registry.UnregisterTurnContext(token)

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
