package llm

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/google/uuid"
)

func TestNoopRuntimeStartSessionPreservesExactExecutionIdentity(t *testing.T) {
	identity := testMemoryIdentity("agent-1", "support/instance-1")
	for _, plan := range []agentmemory.Plan{testMemory(), agentmemory.Authored(false)} {
		ctx := agentmemory.WithExecution(context.Background(), plan, identity)
		session, err := NewNoopRuntime(MockProviderContract()).StartSession(ctx, identity.AgentID(), "system", nil)
		if err != nil {
			t.Fatalf("StartSession(%+v): %v", plan, err)
		}
		if session.Memory != plan || session.MemoryIdentity != identity {
			t.Fatalf("session execution = (%+v, %+v), want (%+v, %+v)", session.Memory, session.MemoryIdentity, plan, identity)
		}
		if _, err := uuid.Parse(session.ID); err != nil {
			t.Fatalf("session id %q is not a UUID: %v", session.ID, err)
		}
	}
}
