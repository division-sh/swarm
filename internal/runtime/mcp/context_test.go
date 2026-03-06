package mcp

import (
	"context"
	"testing"
	"time"

	"empireai/internal/models"
	runtimebus "empireai/internal/runtime/bus"
)

func TestTurnRegistry_ExpiresEntriesByTTL(t *testing.T) {
	reg := newMCPTurnRegistry()
	now := time.Now().UTC()
	reg.put("expired", TurnContext{
		Actor:     models.AgentConfig{ID: "a1"},
		CreatedAt: now.Add(-2 * time.Hour),
		ExpiresAt: now.Add(-1 * time.Minute),
	})
	reg.put("active", TurnContext{
		Actor:     models.AgentConfig{ID: "a2"},
		CreatedAt: now,
		ExpiresAt: now.Add(20 * time.Minute),
	})

	reg.mu.Lock()
	reg.pruneLocked(now)
	reg.mu.Unlock()

	if _, ok := reg.get("expired"); ok {
		t.Fatal("expected expired token to be pruned")
	}
	if _, ok := reg.get("active"); !ok {
		t.Fatal("expected active token to remain")
	}
}

func TestRegisterTurnContextWithTTL_RespectsCustomTTL(t *testing.T) {
	ResetTurnContexts()
	defer ResetTurnContexts()
	SetActorResolver(func(context.Context) (models.AgentConfig, bool) {
		return models.AgentConfig{ID: "agent-test", Role: "analysis-agent", Mode: "factory"}, true
	})

	ctx := runtimebus.WithRuntimeEpoch(context.Background(), runtimebus.CurrentRuntimeEpoch())
	token := RegisterTurnContextWithTTL(ctx, 50*time.Millisecond)
	if token == "" {
		t.Fatal("expected token")
	}
	if _, ok := ResolveTurnContext(token); !ok {
		t.Fatal("expected token to resolve immediately")
	}

	PruneTurnContextsBefore(time.Now().UTC().Add(75 * time.Millisecond))
	if _, ok := ResolveTurnContext(token); ok {
		t.Fatal("expected token to expire after custom TTL")
	}
}

func TestTurnRegistry_UnregisterAndReset(t *testing.T) {
	ResetTurnContexts()
	defer ResetTurnContexts()
	SetActorResolver(func(context.Context) (models.AgentConfig, bool) {
		return models.AgentConfig{ID: "agent-cleanup", Role: "analysis-agent", Mode: "factory"}, true
	})

	token1 := RegisterTurnContextWithTTL(context.Background(), 2*time.Minute)
	token2 := RegisterTurnContextWithTTL(context.Background(), 2*time.Minute)
	if token1 == "" || token2 == "" {
		t.Fatalf("expected non-empty tokens, got token1=%q token2=%q", token1, token2)
	}

	UnregisterTurnContext(token1)
	if _, ok := ResolveTurnContext(token1); ok {
		t.Fatal("expected unregister to remove token1")
	}
	if _, ok := ResolveTurnContext(token2); !ok {
		t.Fatal("expected token2 to remain before reset")
	}

	ResetTurnContexts()
	if _, ok := ResolveTurnContext(token2); ok {
		t.Fatal("expected reset to clear all tokens")
	}
}
