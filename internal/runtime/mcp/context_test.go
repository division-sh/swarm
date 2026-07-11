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
	base := (&Gateway{}).baseContextForResolvedTurn(context.Background(), turn)
	if restored, ok := runtimeeffects.LifecycleTokenFromContext(base); !ok || restored != harness.Token {
		t.Fatalf("restored lifecycle token = %+v ok=%v", restored, ok)
	}
	if _, ok := runtimeeffects.ControllerFromContext(base); !ok {
		t.Fatal("restored effect controller is missing")
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
