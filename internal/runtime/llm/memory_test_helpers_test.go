package llm

import (
	"context"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
)

const testMemoryRunID = "11111111-1111-1111-1111-111111111111"

func testMemory() agentmemory.Plan {
	return agentmemory.Authored(true)
}

func testMemoryIdentity(agentID, flowInstance string) agentmemory.Identity {
	return agentmemory.Identity{RunID: testMemoryRunID, AgentID: agentID, FlowInstance: flowInstance}
}

func withTestMemory(ctx context.Context, agentID, flowInstance string) context.Context {
	return agentmemory.WithExecution(ctx, testMemory(), testMemoryIdentity(agentID, flowInstance))
}

func withTestStatelessMemory(ctx context.Context) context.Context {
	ctx = runtimecorrelation.WithRunID(ctx, testMemoryRunID)
	return agentmemory.WithExecution(ctx, agentmemory.Authored(false), agentmemory.Identity{})
}
