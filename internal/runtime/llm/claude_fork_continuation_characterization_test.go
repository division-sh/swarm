package llm

import (
	"context"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	"github.com/division-sh/swarm/internal/runtime/engine"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
)

func TestClaudeForkChatContinuationCharacterization(t *testing.T) {
	for _, scenario := range []string{"success", "acknowledged_cleanup", "process_failure"} {
		t.Run(scenario, func(t *testing.T) {
			harness := effecttest.New()
			provider, invoked := originTestRuntime(t, "claude", harness, &eventPublisherStub{})
			runtime := provider.(*ClaudeCLIRuntime)
			runtime.cfg.LLM.Models = llmselection.ModelAliases{llmselection.ModelAliasRegular: {llmselection.BackendClaudeCLI: "test-model"}}
			cleanup := errors.New("fork completion cleanup after acknowledgment")
			probe := &acknowledgedForkChatCompletionProbe{&committedCleanupCompletionProbe{Harness: harness}}
			if scenario == "acknowledged_cleanup" {
				probe.cleanupErr = cleanup
			}
			if scenario == "process_failure" {
				t.Setenv("CLAUDE_CHARACTERIZATION_MODE", "failure")
			}
			runtime.completionController = liveTestCompletionController(probe, probe, probe, probe)
			conversation, err := NewForkChatConversation("fork-agent", "fork", "Inspect this fork only.", nil, agentmemory.Plan{}, 2, runtime)
			if err != nil {
				t.Fatal(err)
			}
			authority := testConversationForkAuthority()
			identity := testMemoryIdentity("fork-agent", "fork/one")
			identity.RunID = authority.ForkChat.SourceRunID
			actor := actors.AgentConfig{ID: "fork-agent", Identity: identity, ExecutionMode: effects.ExecutionModeLive, Model: llmselection.ModelAliasRegular}
			ctx := actors.WithActor(effects.WithAuthority(context.Background(), authority), actor)
			ctx = agentmemory.WithExecution(ctx, agentmemory.Plan{}, identity)
			ctx = effects.WithExecutionMode(ctx, effects.ExecutionModeLive)
			ctx = effects.WithLogicalOperationIdentity(ctx, authority.ForkChat.RequestOccurrenceID)
			ctx = WithConversationForkSandboxInvocationPolicy(ctx, nil)
			ctx = llmTestWorkContext(t, ctx)
			response, err := conversation.RunForkChat(ctx, "Inspect the exact fork.")
			if scenario == "process_failure" {
				if response != nil || err == nil || invoked() != 1 || engine.FailureDispositionFor(err) != engine.FailureDispositionTerminal || conversation.TurnCount != 0 {
					t.Fatalf("fork failure response=%+v err=%v calls=%d turns=%d", response, err, invoked(), conversation.TurnCount)
				}
				if err := harness.RequireState("claude_cli", effects.StateOutcomeUncertain); err != nil {
					t.Fatal(err)
				}
				return
			}
			if response == nil || response.Message.Content != "characterized" || invoked() != 1 || conversation.TurnCount != 1 || len(conversation.Messages) != 2 {
				t.Fatalf("fork response=%+v err=%v calls=%d turns=%d messages=%v", response, err, invoked(), conversation.TurnCount, conversation.Messages)
			}
			if scenario == "acknowledged_cleanup" {
				var acknowledged *AcknowledgedForkChatCompletionError
				if !errors.Is(err, cleanup) || !errors.As(err, &acknowledged) || !response.ForkChatCompletionAcknowledged() {
					t.Fatalf("lost acknowledged fork response/error: %+v/%v", response, err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if err := harness.RequireState("claude_cli", effects.StateSettled); err != nil {
				t.Fatal(err)
			}
			for _, attempt := range harness.Attempts {
				if attempt.Authority.Kind != effects.AuthorityConversationForkChat || attempt.Origin.Kind != "" {
					t.Fatalf("fork authority was replaced by a normal origin: %+v", attempt)
				}
			}
		})
	}
}
