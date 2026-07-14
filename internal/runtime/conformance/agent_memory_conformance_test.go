package conformance

import (
	"context"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
)

func TestAgentMemoryConformance_ExactIdentityOwnsReuseAndIsolation(t *testing.T) {
	t.Parallel()

	registry := runtimesessions.NewInMemoryRegistry(0)
	base := agentmemory.Identity{
		RunID:        "11111111-1111-1111-1111-111111111111",
		AgentID:      "support-agent",
		FlowInstance: "support/chat-a",
	}

	first := acquireAndReleaseMemory(t, registry, base, "first")
	if got := acquireAndReleaseMemory(t, registry, base, "same-identity"); got != first {
		t.Fatalf("same exact identity session = %q, want %q", got, first)
	}

	differentRun := base
	differentRun.RunID = "22222222-2222-2222-2222-222222222222"
	if got := acquireAndReleaseMemory(t, registry, differentRun, "different-run"); got == first {
		t.Fatalf("different run reused session %q", got)
	}

	differentFlow := base
	differentFlow.FlowInstance = "support/chat-b"
	if got := acquireAndReleaseMemory(t, registry, differentFlow, "different-flow"); got == first {
		t.Fatalf("different flow instance reused session %q", got)
	}
}

func TestAgentMemoryConformance_IdentityIsCompleteBeforeAcquire(t *testing.T) {
	t.Parallel()

	base := agentmemory.Identity{
		RunID:        "11111111-1111-1111-1111-111111111111",
		AgentID:      "support-agent",
		FlowInstance: "support/chat-a",
	}
	tests := []struct {
		name string
		edit func(*agentmemory.Identity)
		want string
	}{
		{name: "run", edit: func(id *agentmemory.Identity) { id.RunID = "" }, want: "run_id is required"},
		{name: "agent", edit: func(id *agentmemory.Identity) { id.AgentID = "" }, want: "agent_id is required"},
		{name: "flow", edit: func(id *agentmemory.Identity) { id.FlowInstance = "" }, want: "flow_instance is required"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			identity := base
			tc.edit(&identity)
			_, err := runtimesessions.NewInMemoryRegistry(0).Acquire(context.Background(), identity, "proof")
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Acquire error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestAgentMemoryConformance_RootMemoryFailsClosed(t *testing.T) {
	t.Parallel()

	if err := agentmemory.ValidateFlowOwnership(agentmemory.PlatformDefault(), ""); err != nil {
		t.Fatalf("stateless root: %v", err)
	}
	if err := agentmemory.ValidateFlowOwnership(agentmemory.Authored(true), ""); err == nil || !strings.Contains(err.Error(), "flow-instance owner") {
		t.Fatalf("remembered root error = %v", err)
	}
}

func acquireAndReleaseMemory(t *testing.T, registry runtimesessions.Registry, identity agentmemory.Identity, owner string) string {
	t.Helper()
	lease, err := registry.Acquire(context.Background(), identity, owner)
	if err != nil {
		t.Fatalf("Acquire(%+v): %v", identity, err)
	}
	if err := registry.Release(context.Background(), lease); err != nil {
		t.Fatalf("Release(%+v): %v", identity, err)
	}
	return lease.SessionID
}
