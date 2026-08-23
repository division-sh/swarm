package llm

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/agentframe"
	"github.com/division-sh/swarm/internal/runtime/agentintent"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
)

func unmanagedLLMTestContext() context.Context {
	return runtimeeffects.WithDifferentOwner(context.Background(), runtimeeffects.OwnerBuildTestInfrastructure)
}

func liveLLMTestContext() context.Context {
	return runtimeeffects.WithExecutionMode(unmanagedLLMTestContext(), runtimeeffects.ExecutionModeLive)
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
	return worklifetime.WithOccurrence(worklifetime.WithProcess(ctx, process), owner)
}

func managedProviderTestContext(t *testing.T, ctx context.Context, runtime Runtime, session *Session, tools []ToolDefinition) context.Context {
	t.Helper()
	actor, ok := models.ActorFromContext(ctx)
	if !ok {
		t.Fatal("managed provider test context requires actor")
	}
	if actor.Identity.IsZero() {
		actor.Identity = session.MemoryIdentity.Agent
		if actor.Identity.IsZero() {
			if token, hasToken := runtimeeffects.LifecycleTokenFromContext(ctx); hasToken {
				actor.Identity = token.Identity
			}
		}
		actor.ID = actor.Identity.AgentID()
		actor.FlowPath = actor.Identity.FlowInstance()
		ctx = models.WithActor(ctx, actor)
	}
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

func managedProviderCallForEffectTest(t testing.TB, ctx context.Context) *managedProviderCall {
	t.Helper()
	surface, ok := managedcapabilities.FromContext(ctx)
	if !ok {
		t.Fatal("effect test requires managed capability surface")
	}
	intent, err := agentintent.Resolve(agentintent.SourceInline, "inline", "agents.yaml#agents.effect-test.intent", "Process the admitted effect test.")
	if err != nil {
		t.Fatal(err)
	}
	prompt, err := agentintent.IntentOnlyPrompt(intent)
	if err != nil {
		t.Fatal(err)
	}
	providerPrompt, err := agentintent.AssembleProviderPrompt(intent, nil, prompt, agentintent.RuntimeEnvironmentContext())
	if err != nil {
		t.Fatal(err)
	}
	mode, ok := runtimeeffects.ExecutionModeFromContext(ctx)
	if !ok {
		mode = runtimeeffects.ExecutionModeLive
	}
	event := eventtest.RunCreatingRootIngressWithMode(
		"66666666-6666-4666-8666-666666666666", "effect.test.requested", "operator", "effect-test",
		json.RawMessage(`{ "effect": "test" }`), 0, surface.Authority.RunID, "",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "77777777-7777-4777-8777-777777777777"), time.Unix(1, 0).UTC(), mode,
	)
	frame, err := agentframe.Complete(agentframe.SessionSeed{
		AgentIdentity: surface.ActorIdentity, Role: "effect-test", Intent: intent, ProviderPrompt: providerPrompt,
		RuntimeMode: surface.RuntimeMode, Provider: surface.Provider, Transport: surface.Transport,
		ModelAlias: "regular", Model: "effect-test-model",
	}, agentframe.TurnDraft{Kind: agentframe.TurnInitial, Event: event}, agentframe.Completion{
		BundleHash:   "bundle-v1:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
		BundleSource: "persisted", Surface: surface,
	})
	if err != nil {
		t.Fatalf("complete effect-test frame: %v", err)
	}
	return &managedProviderCall{frame: frame, provider: frame.Session.Provider}
}

func beginManagedTestCompletion(t testing.TB, ctx context.Context, adapter string, request []byte) (*runtimeeffects.Handle, error) {
	t.Helper()
	return runtimeeffects.BeginManagedCompletion(ctx, adapter, request, managedProviderCallForEffectTest(t, ctx).frame, nil)
}
