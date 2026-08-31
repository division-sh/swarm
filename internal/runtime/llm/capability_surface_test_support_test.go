package llm

import (
	"context"
	"fmt"

	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
)

func managedCapabilityPlanForTest(ctx context.Context, runtime Runtime, runtimeMode string, tools []ToolDefinition, capabilities toolcapabilities.Set, authority managedcapabilities.Authority) (managedcapabilities.Surface, error) {
	if authority.Kind != managedcapabilities.AuthorityStartupProbe {
		return managedCapabilityPlan(ctx, runtime, runtimeMode, tools, capabilities, authority)
	}
	actor, ok := runtimeactors.ActorFromContext(ctx)
	if !ok {
		return managedcapabilities.Surface{}, fmt.Errorf("startup capability test actor is required")
	}
	identity, err := actor.ConcreteIdentity()
	if err != nil {
		return managedcapabilities.Surface{}, err
	}
	plan, err := identity.Plan()
	if err != nil {
		return managedcapabilities.Surface{}, err
	}
	if authority.ExecutionKind == managedcapabilities.ExecutionNormalAgent {
		actor.Identity = runtimeagentidentity.Identity{}
		ctx = runtimeactors.WithActor(ctx, actor)
	}
	return ManagedCapabilitySurfaceForStartup(ctx, plan, runtime, tools, capabilities, authority)
}
