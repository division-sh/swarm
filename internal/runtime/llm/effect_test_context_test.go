package llm

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
)

func unmanagedLLMTestContext() context.Context {
	return runtimeeffects.WithDifferentOwner(context.Background(), runtimeeffects.OwnerBuildTestInfrastructure)
}

func llmTestWorkContext(t testing.TB, ctx context.Context) context.Context {
	t.Helper()
	process := worklifetime.NewProcess()
	owner, err := process.NewRuntime(ctx, worklifetime.RuntimeIdentity{
		RuntimeInstanceID: "llm-test-runtime",
		BundleHash:        "llm-test-bundle",
	})
	if err != nil {
		t.Fatalf("create LLM test runtime occurrence: %v", err)
	}
	t.Cleanup(func() {
		waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := owner.RetireAndWait(waitCtx); err != nil {
			t.Errorf("retire LLM test runtime occurrence: %v", err)
			return
		}
		if _, err := process.Join(waitCtx); err != nil {
			t.Errorf("join LLM test process owner: %v", err)
		}
	})
	return worklifetime.WithOccurrence(ctx, owner)
}

func managedProviderTestContext(t *testing.T, ctx context.Context, runtime Runtime, session *Session, tools []ToolDefinition) context.Context {
	t.Helper()
	ctx = llmTestWorkContext(t, ctx)
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
