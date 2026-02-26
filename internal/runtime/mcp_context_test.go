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

	time.Sleep(75 * time.Millisecond)
	if _, ok := resolveMCPTurnContext(token); ok {
		t.Fatal("expected token to expire after custom TTL")
	}
}
