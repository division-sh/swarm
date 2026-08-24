package llm

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/sessions"
)

func TestPrepareManagedSessionRotatesExactCompletedRootBeforeNextFrame(t *testing.T) {
	registry := sessions.NewInMemoryRegistry(time.Second)
	identity := testMemoryIdentity("agent-1", "support/instance-1")
	ctx := agentmemory.WithExecution(context.Background(), testMemory(), identity)
	lease, err := registry.Acquire(ctx, identity, "worker-1")
	if err != nil {
		t.Fatal(err)
	}
	oldSessionID := lease.SessionID
	if err := registry.IncrementTurn(ctx, identity, oldSessionID); err != nil {
		t.Fatal(err)
	}
	if err := registry.Release(ctx, lease); err != nil {
		t.Fatal(err)
	}
	session := &Session{
		ID: oldSessionID, AgentID: identity.AgentID(), Memory: testMemory(), MemoryIdentity: identity,
		TurnCount: 1, Messages: []Message{{Role: "user", Content: "first"}, {Role: "assistant", Content: "done"}},
	}
	if err := prepareManagedSessionForTurn(ctx, session, registry, "worker-1", 1, nil); err != nil {
		t.Fatal(err)
	}
	if session.ID == oldSessionID || session.TurnCount != 0 || len(session.Messages) != 1 ||
		session.Messages[0].Role != "system" || !strings.Contains(session.Messages[0].Content, "turn_limit_reached:1") {
		t.Fatalf("rotated session=%#v old_session_id=%s", session, oldSessionID)
	}
	current, err := registry.Acquire(ctx, identity, "worker-1")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = registry.Release(context.Background(), current) }()
	if current.SessionID != session.ID || current.RetriesFromSessionID != oldSessionID {
		t.Fatalf("current lease=%#v session=%#v", current, session)
	}
}

func TestPrepareManagedSessionRejectsStaleSessionWithoutRotation(t *testing.T) {
	registry := sessions.NewInMemoryRegistry(time.Second)
	identity := testMemoryIdentity("agent-1", "support/instance-1")
	ctx := agentmemory.WithExecution(context.Background(), testMemory(), identity)
	lease, err := registry.Acquire(ctx, identity, "worker-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Release(ctx, lease); err != nil {
		t.Fatal(err)
	}
	session := &Session{
		ID: "stale-session", AgentID: identity.AgentID(), Memory: testMemory(), MemoryIdentity: identity,
		TurnCount: 1, Messages: []Message{{Role: "assistant", Content: "done"}},
	}
	if err := prepareManagedSessionForTurn(ctx, session, registry, "worker-1", 1, nil); err == nil || !strings.Contains(err.Error(), "changed before rotation") {
		t.Fatalf("stale preparation error=%v", err)
	}
	current, err := registry.Acquire(ctx, identity, "worker-1")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = registry.Release(context.Background(), current) }()
	if current.SessionID != lease.SessionID || session.ID != "stale-session" {
		t.Fatalf("stale preparation mutated current=%#v session=%#v", current, session)
	}
}
