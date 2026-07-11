package mcp

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
)

func TestTurnContextRegistry_ResetIsScopedToRegistry(t *testing.T) {
	registryA := NewTurnContextRegistry(nil)
	registryB := NewTurnContextRegistry(nil)

	registryA.PutTurnContextForTest("ctx-shared", TurnContext{
		Actor:     models.AgentConfig{ID: "agent-a"},
		CreatedAt: time.Now().UTC(),
		ExpiresAt: time.Now().UTC().Add(time.Hour),
	})
	registryB.PutTurnContextForTest("ctx-shared", TurnContext{
		Actor:     models.AgentConfig{ID: "agent-b"},
		CreatedAt: time.Now().UTC(),
		ExpiresAt: time.Now().UTC().Add(time.Hour),
	})

	registryA.Reset()

	if _, ok := registryA.ResolveTurnContext("ctx-shared"); ok {
		t.Fatal("registryA should be empty after reset")
	}
	turn, ok := registryB.ResolveTurnContext("ctx-shared")
	if !ok {
		t.Fatal("registryB should retain its turn context")
	}
	if turn.Actor.ID != "agent-b" {
		t.Fatalf("registryB actor id = %q, want agent-b", turn.Actor.ID)
	}
}

func TestTurnContextRegistryPreservesManagedEffectAuthority(t *testing.T) {
	harness := effecttest.New()
	registry := NewTurnContextRegistry(models.ActorFromContext)
	ctx := models.WithActor(harness.Context("gateway-turn"), models.AgentConfig{ID: harness.Token.AgentID})
	token := registry.RegisterTurnContext(ctx)
	turn, ok := registry.ResolveTurnContext(token)
	if !ok {
		t.Fatal("ResolveTurnContext returned false")
	}
	if !turn.HasLifecycleToken || turn.LifecycleToken != harness.Token || turn.EffectController == nil {
		t.Fatalf("managed effect authority was not preserved: %#v", turn)
	}
	if !turn.HasLogicalIdentity || turn.LogicalIdentity != "gateway-turn" {
		t.Fatalf("managed logical identity was not preserved: %#v", turn)
	}
	base := (&Gateway{}).baseContextForResolvedTurn(context.Background(), turn)
	if restored, ok := runtimeeffects.LifecycleTokenFromContext(base); !ok || restored != harness.Token {
		t.Fatalf("restored lifecycle token = %+v ok=%v", restored, ok)
	}
	if _, ok := runtimeeffects.ControllerFromContext(base); !ok {
		t.Fatal("restored effect controller is missing")
	}
	if identity, ok := runtimeeffects.LogicalOperationIdentityFromContext(base); !ok || identity != "gateway-turn" {
		t.Fatalf("restored logical identity = %q ok=%v", identity, ok)
	}
}

func TestTurnContextRegistryPreservesSiblingLogicalIdentityAndIgnoresMCPTransportID(t *testing.T) {
	harness := effecttest.New()
	registry := NewTurnContextRegistry(models.ActorFromContext)
	register := func(identity string) TurnContext {
		ctx := runtimeeffects.WithLogicalOperationIdentity(harness.Context("inbound-event"), identity)
		ctx = models.WithActor(ctx, models.AgentConfig{ID: harness.Token.AgentID})
		token := registry.RegisterTurnContext(ctx)
		turn, ok := registry.ResolveTurnContext(token)
		if !ok {
			t.Fatalf("resolve %s turn context", identity)
		}
		return turn
	}

	requestOne := RPCRequest{JSONRPC: "2.0", Method: "tools/call", ID: float64(1), Params: map[string]any{
		"name": "write_file", "arguments": map[string]any{"path": "/workspace/result.txt", "content": "x"},
		"_meta": map[string]any{claudeCodeToolUseIDMetaKey: "toolu-call-1", "progressToken": float64(1)},
	}}
	requestReplay := requestOne
	requestReplay.ID = "replacement-transport-id"
	requestReplay.Params = map[string]any{
		"name": "write_file", "arguments": map[string]any{"path": "/workspace/result.txt", "content": "x"},
		"_meta": map[string]any{claudeCodeToolUseIDMetaKey: "toolu-call-1", "progressToken": "replacement-progress-token"},
	}
	managedCtx := harness.Context("provider-turn")
	firstSegment, err := mcpToolCallLogicalIdentitySegment(managedCtx, requestOne)
	if err != nil {
		t.Fatal(err)
	}
	replaySegment, err := mcpToolCallLogicalIdentitySegment(managedCtx, requestReplay)
	if err != nil {
		t.Fatal(err)
	}
	if firstSegment != replaySegment {
		t.Fatalf("transport correlation changed provider call identity: %q != %q", firstSegment, replaySegment)
	}
	requestSibling := requestOne
	requestSibling.Params = map[string]any{
		"name": "write_file", "arguments": map[string]any{"path": "/workspace/result.txt", "content": "x"},
		"_meta": map[string]any{claudeCodeToolUseIDMetaKey: "toolu-call-2", "progressToken": float64(2)},
	}
	siblingSegment, err := mcpToolCallLogicalIdentitySegment(managedCtx, requestSibling)
	if err != nil {
		t.Fatal(err)
	}
	if firstSegment == siblingSegment {
		t.Fatal("identical same-turn calls with distinct provider tool-use IDs collapsed")
	}

	gateway := &Gateway{}
	begin := func(turn TurnContext, segment string) error {
		ctx := gateway.baseContextForResolvedTurn(context.Background(), turn)
		ctx = runtimeeffects.WithLogicalOperationIdentitySegment(ctx, segment)
		_, err := runtimeeffects.Begin(ctx, "authored_http_tool", []byte("request"), map[string]string{"tool": "write_file"})
		return err
	}
	turnOne := register("provider-turn-1")
	turnTwo := register("provider-turn-2")
	if err := begin(turnOne, firstSegment); err != nil {
		t.Fatalf("first provider-turn child: %v", err)
	}
	if err := begin(turnTwo, firstSegment); err != nil {
		t.Fatalf("sibling provider-turn child collided: %v", err)
	}
	if err := begin(turnOne, replaySegment); err == nil {
		t.Fatal("changed MCP transport ID redispatched the same semantic child")
	}
	if len(harness.Attempts) != 2 {
		t.Fatalf("managed attempts = %d, want two siblings and no replay attempt", len(harness.Attempts))
	}
}

func TestMCPToolCallLogicalIdentityRequiresProviderCallCoordinateForManagedTurns(t *testing.T) {
	harness := effecttest.New()
	req := RPCRequest{JSONRPC: "2.0", Method: "tools/call", ID: float64(1), Params: map[string]any{
		"name": "write_file", "arguments": map[string]any{"path": "/workspace/result.txt"},
	}}
	if _, err := mcpToolCallLogicalIdentitySegment(harness.Context("provider-turn"), req); err == nil {
		t.Fatal("managed MCP call without provider call coordinate was accepted")
	}
	unmanaged := runtimeeffects.WithDifferentOwner(context.Background(), runtimeeffects.OwnerRuntimeDependency)
	if segment, err := mcpToolCallLogicalIdentitySegment(unmanaged, req); err != nil || segment != "" {
		t.Fatalf("different-owner call identity = %q err=%v, want empty identity", segment, err)
	}
}

func TestTurnContextRegistry_PreservesTypedRuntimeLineage(t *testing.T) {
	registry := NewTurnContextRegistry(models.ActorFromContext)
	ctx := models.WithActor(unmanagedMCPTestContext(), models.AgentConfig{ID: "selected-agent"})
	ctx = runtimecorrelation.WithRuntimeLineage(ctx, runtimecorrelation.RuntimeLineage{
		Owner:               "runtime.run_fork.selected_contract_execution.fork_local_runtime_typed_lineage",
		RunID:               "9b06692c-353c-4479-8e92-70927f5e4937",
		SubjectEventID:      "4078d35c-3a8a-40ea-a5f5-01b35a9ff59a",
		SubjectEventType:    "validation/validation.package_ready",
		ParentEventID:       "4078d35c-3a8a-40ea-a5f5-01b35a9ff59a",
		RowCategory:         runtimecorrelation.RuntimeLineageRowCategoryRuntimeContainer,
		SelectedForkOwner:   "runtime.run_fork.selected_contract_execution.fork_local_runtime_container",
		Classification:      runtimecorrelation.RuntimeLineageClassificationForkLocal,
		SelectedForkContext: true,
	})
	ctx = runtimebus.WithInboundEvent(ctx, eventtest.RootIngress("4078d35c-3a8a-40ea-a5f5-01b35a9ff59a",
		events.EventType("validation/validation.package_ready"), "", "", nil, 0, "9b06692c-353c-4479-8e92-70927f5e4937", "", events.EventEnvelope{}, time.Time{}))

	token := registry.RegisterTurnContext(ctx)
	if token == "" {
		t.Fatal("RegisterTurnContext returned empty token")
	}

	turn, ok := registry.ResolveTurnContext(token)
	if !ok {
		t.Fatal("ResolveTurnContext returned false")
	}
	if !turn.HasRuntimeLineage {
		t.Fatal("turn context did not preserve runtime lineage")
	}
	if got := turn.RuntimeLineage.Owner; got != "runtime.run_fork.selected_contract_execution.fork_local_runtime_typed_lineage" {
		t.Fatalf("owner = %q", got)
	}
	if got := turn.RuntimeLineage.ParentEventID; got != "4078d35c-3a8a-40ea-a5f5-01b35a9ff59a" {
		t.Fatalf("parent event = %q", got)
	}
	if got := turn.RuntimeLineage.RowCategory; got != runtimecorrelation.RuntimeLineageRowCategoryRuntimeContainer {
		t.Fatalf("row category = %q", got)
	}
}
