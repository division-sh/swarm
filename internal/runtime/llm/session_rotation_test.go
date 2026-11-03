package llm

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/sessions"
)

func TestPrepareManagedSessionRejectsStaleSessionWithoutRotation(t *testing.T) {
	registry := sessions.NewInMemoryRegistry(time.Second)
	identity := testMemoryIdentity("agent-1", "support/instance-1")
	ctx := agentmemory.WithExecution(context.Background(), testMemory(), identity)
	lease, err := registry.Acquire(ctx, identity, "worker-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.ReleaseOutcome(ctx, lease); err != nil {
		t.Fatal(err)
	}
	session := &Session{
		ID: "stale-session", AgentID: identity.AgentID(), Memory: testMemory(), MemoryIdentity: identity,
		TurnCount: 1, Messages: []Message{{Role: "assistant", Content: "done"}},
	}
	if err := prepareManagedSessionForTurn(ctx, session, registry, newTransientLiveSessionAcquirer(registry), "worker-1", nil); err == nil || !strings.Contains(err.Error(), "managed_capability_turn_identity_mismatch") {
		t.Fatalf("stale preparation error=%v", err)
	}
	current, err := registry.Acquire(ctx, identity, "worker-1")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = registry.ReleaseOutcome(context.Background(), current) }()
	if current.SessionID != lease.SessionID || session.ID != current.SessionID {
		t.Fatalf("stale preparation mutated current=%#v session=%#v", current, session)
	}
}
