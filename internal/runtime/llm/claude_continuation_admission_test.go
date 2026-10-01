package llm

import (
	"context"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	actors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/runtime/workspace"
)

func TestClaudeContinuationAdmissionPrecedence(t *testing.T) {
	for _, tc := range []struct {
		name, prompt, want string
		turns, failures    int
	}{
		{"empty_prompt_before_missing_head", "  ", "empty_agent_prompt", 1, 1},
		{"missing_confirmed_head", "hello", "claude_provider_head_missing", 1, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			harness := effecttest.New()
			ctx := managedEffectHarnessContext(t, harness, tc.name)
			surface, _ := managedcapabilities.FromContext(ctx)
			actor := actors.AgentConfig{ID: surface.ActorIdentity.AgentID(), Identity: surface.ActorIdentity, FlowPath: surface.ActorIdentity.FlowInstance(), Model: llmselection.ModelAliasRegular}
			ctx = actors.WithActor(ctx, actor)
			ctx = correlation.WithRunID(ctx, actor.Identity.RunID)
			cfg := &config.Config{LLM: config.LLMConfig{Models: llmselection.ModelAliases{llmselection.ModelAliasRegular: {llmselection.BackendClaudeCLI: "test-model"}}}}
			runtime := NewClaudeCLIRuntime(cfg, sessions.NewInMemoryRegistry(0), "admission", workspaceResolverStub{target: &workspace.Target{Container: "admission", Workdir: "/workspace"}}, nil, nil)
			session, err := runtime.StartSession(ctx, actor.ID, "admitted intent", nil)
			if err != nil {
				t.Fatal(err)
			}
			session.MemoryIdentity = actor.Identity
			ctx = managedProviderTestContext(t, ctx, runtime, session, nil)
			managed := managedProviderCallForEffectTest(t, ctx)
			session.TurnCount = tc.turns
			var lease *sessions.Lease
			response, err := runtime.continueClaudeTurn(ctx, session, Message{Content: tc.prompt}, managed, actor, "", resolvedMemoryExecution{}, &lease)
			if response != nil || err == nil || !strings.Contains(err.Error(), tc.want) || session.ParseFailures != tc.failures {
				t.Fatalf("response=%+v err=%v parse failures=%d", response, err, session.ParseFailures)
			}
		})
	}
	runtime := NewClaudeCLIRuntime(&config.Config{}, sessions.NewInMemoryRegistry(0), "admission", nil, nil, nil)
	if result, err := runtime.continueSession(context.Background(), nil, Message{}, nil); result != nil || err == nil || err.Error() != "nil session" {
		t.Fatalf("nil session: result=%+v err=%v", result, err)
	}
	if result, err := runtime.ContinueManagedSession(context.Background(), &Session{}, ManagedCall{}); result != nil || err == nil {
		t.Fatalf("unvalidated managed call: result=%+v err=%v", result, err)
	}
}
