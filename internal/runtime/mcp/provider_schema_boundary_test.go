package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/toolgateway"
	"github.com/google/uuid"
)

func TestProviderSchemaGatewayAdmission(t *testing.T) {
	bad := llm.ToolDefinition{Name: "write_file", Schema: map[string]any{"type": "unsupported-provider-type"}}
	good := llm.ToolDefinition{Name: "read_file", Description: " exact ", Schema: map[string]any{"type": "object", "properties": map[string]any{}}}
	for _, kind := range []managedcapabilities.ExecutionKind{managedcapabilities.ExecutionNormalAgent, managedcapabilities.ExecutionSelectedContractFork} {
		t.Run("managed/"+string(kind), func(t *testing.T) {
			ctx, surface := providerSchemaGatewaySurface(t, kind, []llm.ToolDefinition{good, bad}, false)
			actor, _ := models.ActorFromContext(ctx)
			calls := 0
			executor := &leasedCatalogExecutorStub{actorScopedToolExecutorStub: actorScopedToolExecutorStub{callCount: &calls}, leasedDefinitions: []llm.ToolDefinition{good, bad}}
			gateway := NewGateway(executor, testGatewayToken, GatewayHooks{})
			tools, evidence, mismatches, err := gateway.mcpToolsForCapabilitySurface(ctx, TurnContext{Actor: actor, CapabilitySurface: &surface})
			cause := errors.Unwrap(err)
			if cause == nil || !strings.Contains(cause.Error(), "write_file.input_schema.type") || tools != nil || evidence != nil || len(mismatches) != 0 || calls != 0 {
				t.Fatalf("invalid selected catalog escaped: tools=%v evidence=%v mismatches=%v calls=%d err=%v", tools, evidence, mismatches, calls, err)
			}
			if executor.acquired != 1 || executor.released != 1 {
				t.Fatalf("refusal lease=%d/%d, want 1/1", executor.acquired, executor.released)
			}
		})
	}
	for _, startup := range []bool{false, true} {
		name := "http/normal"
		if startup {
			name = "http/startup"
		}
		t.Run(name, func(t *testing.T) {
			ctx, surface := providerSchemaGatewaySurface(t, managedcapabilities.ExecutionNormalAgent, []llm.ToolDefinition{bad}, startup)
			registry := NewTurnContextRegistry(models.ActorFromContext)
			token := registry.RegisterTurnContextWithCapabilitySurface(ctx, time.Minute, surface)
			if token == "" {
				t.Fatal("earlier exact-context admission refused")
			}
			defer registry.UnregisterTurnContext(token)
			calls := 0
			gateway := NewGateway(actorScopedToolExecutorStub{actorDefs: []llm.ToolDefinition{bad}, callCount: &calls}, testGatewayToken, managedCLIGatewayHooks(registry))
			response := callMCPGateway(t, gateway, token, RPCRequest{JSONRPC: "2.0", Method: "tools/list", ID: "schema-list"})
			if response.Error == nil || response.Result != nil || calls != 0 {
				t.Fatalf("tools/list admitted invalid schema: %+v calls=%d", response, calls)
			}
			after, ok := registry.ResolveManagedCapabilitySurface(token)
			if !ok || after.Tools[0].EffectiveVisible || after.Tools[0].EffectiveCallable {
				t.Fatalf("refusal published confirmation: %+v", after)
			}
		})
	}
	t.Run("managed/selection-and-identity", func(t *testing.T) {
		ctx, surface := providerSchemaGatewaySurface(t, managedcapabilities.ExecutionNormalAgent, []llm.ToolDefinition{good}, false)
		actor, _ := models.ActorFromContext(ctx)
		executor := &leasedCatalogExecutorStub{leasedDefinitions: []llm.ToolDefinition{bad, good}}
		gateway := NewGateway(executor, testGatewayToken, GatewayHooks{})
		tools, evidence, _, err := gateway.mcpToolsForCapabilitySurface(ctx, TurnContext{Actor: actor, CapabilitySurface: &surface})
		if err != nil || len(tools) != 1 || len(evidence) != 1 || tools[0].Name != good.Name || executor.released != 1 {
			t.Fatalf("unselected malformed entry changed selected list: tools=%v evidence=%v err=%v", tools, evidence, err)
		}
		want := `[{"name":"read_file","description":"exact","inputSchema":{"properties":{},"type":"object"}}]`
		if raw, _ := json.Marshal(tools); string(raw) != want {
			t.Fatalf("valid selected bytes=%s, want %s", raw, want)
		}
		executor.leasedDefinitions = []llm.ToolDefinition{{Name: good.Name, Schema: bad.Schema}}
		tools, evidence, mismatches, err := gateway.mcpToolsForCapabilitySurface(ctx, TurnContext{Actor: actor, CapabilitySurface: &surface})
		if err == nil || len(mismatches) != 1 || mismatches[0].Kind != "mcp_definition_identity_mismatch" || tools != nil || evidence != nil || executor.released != 2 {
			t.Fatalf("earlier identity precedence changed: tools=%v evidence=%v mismatch=%v err=%v", tools, evidence, mismatches, err)
		}
	})
	t.Run("actor/refusal", func(t *testing.T) {
		executor := &leasedCatalogExecutorStub{leasedDefinitions: []llm.ToolDefinition{good, bad}}
		gateway := NewGateway(executor, testGatewayToken, GatewayHooks{})
		tools, err := gateway.mcpToolsForActorInContext(context.Background(), models.AgentConfig{ID: "actor"}, nil, true)
		cause := errors.Unwrap(err)
		if cause == nil || !strings.Contains(cause.Error(), "write_file.input_schema.type") || tools != nil || executor.acquired != 1 || executor.released != 1 {
			t.Fatalf("actor projection escaped or leaked: tools=%v err=%v lease=%d/%d", tools, err, executor.acquired, executor.released)
		}
	})
	t.Run("actor/selection-and-defaults", func(t *testing.T) {
		executor := &leasedCatalogExecutorStub{leasedDefinitions: []llm.ToolDefinition{bad, {Name: "read_file"}, {Name: "object", Schema: map[string]any{}}}}
		gateway := NewGateway(executor, testGatewayToken, GatewayHooks{})
		allowed := map[string]struct{}{"read_file": {}, "object": {}}
		tools, err := gateway.mcpToolsForActorInContext(context.Background(), models.AgentConfig{ID: "actor"}, allowed, true)
		if err != nil || executor.released != 1 {
			t.Fatalf("hidden malformed schema invalidated list: %v", err)
		}
		want := `[{"name":"read_file","description":"Runtime tool","inputSchema":{"additionalProperties":true,"properties":{},"type":"object"}},{"name":"object","description":"Runtime tool","inputSchema":{}}]`
		if raw, _ := json.Marshal(tools); string(raw) != want {
			t.Fatalf("actor bytes=%s, want %s", raw, want)
		}
	})
	for _, tc := range []struct {
		name   string
		schema any
	}{
		{"padded_type", map[string]any{"type": " object "}},
		{"padded_nested", map[string]any{"properties": map[string]any{"q": map[string]any{"type": " string "}}}},
		{"typed_nil_root", map[string]any(nil)},
		{"nil_properties", map[string]any{"properties": map[string]any(nil)}},
		{"nil_nested_schema", map[string]any{"items": map[string]any(nil)}},
		{"nested_types", map[string]any{"type": []any{[]any{"object"}}}},
		{"duplicate_types", map[string]any{"type": []any{"object", "object"}}},
		{"nil_types", map[string]any{"type": []any(nil)}},
		{"duplicate_required", map[string]any{"required": []any{"q", "q"}}},
		{"nil_required", map[string]any{"required": []any(nil)}},
		{"nil_enum", map[string]any{"enum": []any(nil)}},
	} {
		t.Run("wire/"+tc.name, func(t *testing.T) {
			definition := llm.ToolDefinition{Name: "read_file", Schema: tc.schema}
			want := llm.ValidateProviderToolSchema(definition.Name, tc.schema)
			if want == nil {
				t.Fatal("invalid wire fixture escaped canonical admission")
			}
			ctx, surface := providerSchemaGatewaySurface(t, managedcapabilities.ExecutionNormalAgent, []llm.ToolDefinition{definition}, false)
			actor, _ := models.ActorFromContext(ctx)
			for _, managed := range []bool{true, false} {
				executor := &leasedCatalogExecutorStub{leasedDefinitions: []llm.ToolDefinition{definition}}
				gateway := NewGateway(executor, testGatewayToken, GatewayHooks{})
				var tools []ToolDef
				var err error
				if managed {
					var evidence []managedcapabilities.DeliveryEvidence
					tools, evidence, _, err = gateway.mcpToolsForCapabilitySurface(ctx, TurnContext{Actor: actor, CapabilitySurface: &surface})
					if evidence != nil {
						t.Fatal("invalid wire published list evidence")
					}
				} else {
					tools, err = gateway.mcpToolsForActorInContext(ctx, actor, nil, true)
				}
				cause := errors.Unwrap(err)
				if cause == nil || cause.Error() != want.Error() || tools != nil || executor.acquired != 1 || executor.released != 1 {
					t.Fatalf("invalid wire admitted/leaked, managed=%v: tools=%v err=%v lease=%d/%d", managed, tools, err, executor.acquired, executor.released)
				}
			}
		})
	}
	t.Run("fork/frozen", testProviderSchemaFrozenForkCatalog)
}

func testProviderSchemaFrozenForkCatalog(t *testing.T) {
	actor := models.AgentConfig{ID: "schema-fork-agent"}
	forkTurnID := uuid.NewString()
	authority := runtimeeffects.Authority{
		Kind: runtimeeffects.AuthorityConversationForkChat, ID: forkTurnID, ExecutionOwner: "schema-owner",
		LeaseExpiresAt: time.Now().Add(time.Minute), FenceGeneration: 1, ExecutionMode: runtimeeffects.ExecutionModeLive,
		ForkChat: runtimeeffects.ConversationForkChatAuthority{ForkTurnID: forkTurnID, ForkID: uuid.NewString(),
			SourceRunID: uuid.NewString(), BundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64),
			ActorTokenID: "schema-actor", RequestOccurrenceID: uuid.NewString(), RequestHash: "schema-request"},
	}
	ctx := models.WithActor(runtimeeffects.WithAuthority(context.Background(), authority), actor)
	definition := llm.ToolDefinition{Name: "read_file", Description: "frozen description", Schema: map[string]any{"type": "object", "properties": map[string]any{}}}
	sandboxCalls, liveCalls := 0, 0
	sandbox := actorScopedToolExecutorStub{actorDefs: []llm.ToolDefinition{definition}, callCount: &sandboxCalls}
	runtime := &projectionForkRuntime{}
	conversation, err := llm.NewForkChatConversation(actor.ID, "", "", sandbox.actorDefs, agentmemory.Plan{}, 2, runtime)
	if err != nil {
		t.Fatal(err)
	}
	conversation.SetToolExecutor(sandbox)
	if _, err := conversation.RunForkChat(ctx, "capture frozen schema dispatch"); err != nil {
		t.Fatal(err)
	}
	registry := NewTurnContextRegistry(models.ActorFromContext)
	token := registry.RegisterConversationForkSandboxTurnContext(runtime.ctx, time.Minute, []string{definition.Name})
	if token == "" {
		t.Fatal("exact fork authority did not register its frozen dispatch")
	}
	defer registry.UnregisterTurnContext(token)
	definition.Schema.(map[string]any)["type"] = "unsupported-provider-type"
	live := &leasedCatalogExecutorStub{actorScopedToolExecutorStub: actorScopedToolExecutorStub{callCount: &liveCalls}, leasedDefinitions: []llm.ToolDefinition{definition}}
	server := httptest.NewServer(NewGateway(live, testGatewayToken, GatewayHooks{
		ResolveTurnContext: registry.ResolveTurnContext, WithActor: models.WithActor, ActorFromContext: models.ActorFromContext,
	}).Handler())
	defer server.Close()
	tools, err := (toolgateway.HTTPObservation{URL: server.URL + "/mcp", Headers: map[string]string{
		"Authorization": "Bearer " + testGatewayToken, "X-SWARM-Context-Token": token,
	}}).Probe(ctx)
	if err != nil || len(tools) != 1 {
		t.Fatalf("frozen tools/list failed: tools=%v err=%v", tools, err)
	}
	want := `{"properties":{},"type":"object"}`
	if tools[0].Name != definition.Name || tools[0].Description != "frozen description" || string(tools[0].InputSchema) != want || sandboxCalls != 0 || liveCalls != 0 || live.acquired != 0 {
		t.Fatalf("frozen list used mutable/live authority: tools=%v sandbox=%d live=%d leases=%d", tools, sandboxCalls, liveCalls, live.acquired)
	}
}

func providerSchemaGatewaySurface(t testing.TB, kind managedcapabilities.ExecutionKind, definitions []llm.ToolDefinition, startup bool) (context.Context, managedcapabilities.Surface) {
	t.Helper()
	ctx, original, _ := managedClaudeProviderTurnTestContext(t, kind)
	actor, _ := models.ActorFromContext(ctx)
	caps := make([]toolcapabilities.Capability, 0, len(definitions))
	for _, definition := range definitions {
		caps = append(caps, toolcapabilities.Capability{Name: definition.Name, Visible: true, Callable: true})
	}
	tools, err := llm.CompileManagedCapabilityAdmission(actor, llm.ClaudeCLIProviderContract(), definitions, toolcapabilities.NewSet(caps))
	if err != nil {
		t.Fatal(err)
	}
	plan := managedcapabilities.Plan{ActorIdentity: actor.Identity, RuntimeMode: original.RuntimeMode, Provider: original.Provider,
		Transport: original.Transport, ProviderContract: original.ProviderContract, Authority: original.Authority, Tools: tools, CreatedAt: time.Now().UTC()}
	if startup {
		plan.ActorPlan, err = actor.Identity.Plan()
		if err != nil {
			t.Fatal(err)
		}
		plan.ActorIdentity = models.AgentConfig{}.Identity
		plan.RuntimeMode = "startup_probe"
		plan.Authority = managedcapabilities.Authority{Kind: managedcapabilities.AuthorityStartupProbe, ID: uuid.NewString(),
			ExecutionKind: managedcapabilities.ExecutionNormalAgent, ExecutionAuthorityID: "schema-startup", StartupOwnerID: "schema-owner", StartupGeneration: 1}
		ctx = models.WithActor(context.Background(), models.AgentConfig{ID: actor.ID, FlowPath: actor.FlowPath})
	}
	surface, err := managedcapabilities.New(plan)
	if err != nil {
		t.Fatal(err)
	}
	return managedcapabilities.WithContext(ctx, surface), surface
}
