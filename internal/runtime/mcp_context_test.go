package runtime

import (
	"context"
	"testing"
	"time"

	"empireai/internal/models"
)

func TestMCPTurnRegistry_ExpiresEntriesByTTL(t *testing.T) {
	reg := newMCPTurnRegistry()
	now := time.Now().UTC()
	reg.put("expired", mcpTurnContext{
		Actor:     models.AgentConfig{ID: "a1"},
		CreatedAt: now.Add(-2 * time.Hour),
		ExpiresAt: now.Add(-1 * time.Minute),
	})
	reg.put("active", mcpTurnContext{
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

func TestRegisterMCPTurnContextWithTTL_RespectsCustomTTL(t *testing.T) {
	resetMCPTurnContexts()
	defer resetMCPTurnContexts()

	ctx := WithActor(context.Background(), models.AgentConfig{
		ID:   "agent-test",
		Role: "analysis-agent",
		Mode: "factory",
	})

	token := registerMCPTurnContextWithTTL(ctx, 50*time.Millisecond)
	if token == "" {
		t.Fatal("expected token")
	}
	if _, ok := resolveMCPTurnContext(token); !ok {
		t.Fatal("expected token to resolve immediately")
	}

	globalMCPTurnRegistry.mu.Lock()
	globalMCPTurnRegistry.pruneLocked(time.Now().UTC().Add(75 * time.Millisecond))
	globalMCPTurnRegistry.mu.Unlock()
	if _, ok := resolveMCPTurnContext(token); ok {
		t.Fatal("expected token to expire after custom TTL")
	}
}

func TestMCPTurnRegistry_UnregisterAndReset(t *testing.T) {
	resetMCPTurnContexts()
	defer resetMCPTurnContexts()

	ctx := WithActor(context.Background(), models.AgentConfig{
		ID:   "agent-cleanup",
		Role: "analysis-agent",
		Mode: "factory",
	})

	token1 := registerMCPTurnContextWithTTL(ctx, 2*time.Minute)
	token2 := registerMCPTurnContextWithTTL(ctx, 2*time.Minute)
	if token1 == "" || token2 == "" {
		t.Fatalf("expected non-empty tokens, got token1=%q token2=%q", token1, token2)
	}

	unregisterMCPTurnContext(token1)
	if _, ok := resolveMCPTurnContext(token1); ok {
		t.Fatal("expected unregister to remove token1")
	}
	if _, ok := resolveMCPTurnContext(token2); !ok {
		t.Fatal("expected token2 to remain before reset")
	}

	resetMCPTurnContexts()
	if _, ok := resolveMCPTurnContext(token2); ok {
		t.Fatal("expected reset to clear all tokens")
	}
}
