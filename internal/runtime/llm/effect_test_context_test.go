package llm

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
)

func unmanagedLLMTestContext() context.Context {
	return runtimeeffects.WithDifferentOwner(context.Background(), runtimeeffects.OwnerBuildTestInfrastructure)
}

func managedProviderTestContext(t *testing.T, ctx context.Context, runtime Runtime, session *Session, tools []ToolDefinition) context.Context {
	t.Helper()
	var err error
	ctx, _, err = withProviderTurnAuthority(ctx, session)
	if err != nil {
		t.Fatalf("withProviderTurnAuthority: %v", err)
	}
	caps := make([]toolcapabilities.Capability, 0, len(tools))
	for _, tool := range tools {
		caps = append(caps, toolcapabilities.Capability{Name: tool.Name, Visible: true, Callable: true})
	}
	surface, err := managedCapabilityPlanForTurn(ctx, runtime, session, tools, toolcapabilities.NewSet(caps))
	if err != nil {
		t.Fatalf("managedCapabilityPlanForTurn: %v", err)
	}
	return managedcapabilities.WithContext(ctx, surface)
}
