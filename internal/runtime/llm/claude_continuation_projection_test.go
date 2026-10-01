package llm

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/agentframe"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/workspace"
)

type claudeProjectionTestStore struct {
	*effecttest.Harness
	lost atomic.Bool
}

func (s *claudeProjectionTestStore) IsExternalEffectAuthorityCurrent(ctx context.Context, authority effects.Authority) (bool, error) {
	if s.lost.Load() {
		return false, nil
	}
	return s.Harness.IsExternalEffectAuthorityCurrent(ctx, authority)
}

type claudeProjectionTestState struct {
	claudeStateStub
	check func(string) error
}

func (s claudeProjectionTestState) CheckHead(_ context.Context, head string) error {
	return s.check(head)
}

type claudeProjectionTestResolver struct {
	workspaceResolverStub
	state workspace.ClaudeState
}

func (r claudeProjectionTestResolver) ResolveClaudeWorkspace(ctx context.Context, actor actors.AgentConfig, request workspace.ClaudeStateRequest, head string) (*workspace.Target, error) {
	target, err := r.workspaceResolverStub.ResolveClaudeWorkspace(ctx, actor, request, head)
	if err == nil {
		target.ClaudeState = r.state
	}
	return target, err
}

func TestClaudeContinuationCandidateBackingAndProjectionRefusal(t *testing.T) {
	for _, scenario := range []string{"missing_backing", "projection_lost_after_backing"} {
		t.Run(scenario, func(t *testing.T) {
			harness := effecttest.New()
			provider, invoked := originTestRuntime(t, "claude", harness, &eventPublisherStub{markChanged: true})
			runtime := provider.(*ClaudeCLIRuntime)
			store := &claudeProjectionTestStore{Harness: harness}
			runtime.completionController = liveTestCompletionController(store, harness, harness, harness)
			backingFailure := errors.New("candidate transcript is missing")
			checks := 0
			runtime.workspaces = claudeProjectionTestResolver{
				workspaceResolverStub: workspaceResolverStub{target: &workspace.Target{Container: "projection-test", Workdir: "/workspace"}},
				state: claudeProjectionTestState{check: func(head string) error {
					checks++
					if head == "" {
						t.Fatal("candidate validation did not receive the attempt head")
					}
					if scenario == "missing_backing" {
						return backingFailure
					}
					store.lost.Store(true)
					return nil
				}},
			}
			ctx := testManagedConversationContext(t, harness, "projection-agent", "support/one", "reader")
			actor, _ := actors.ActorFromContext(ctx)
			conversation := newTestManagedConversation(t, actor.ID, actor.Identity.FlowInstance(), "reader", nil, testMemory(), 1, runtime)
			conversation.SetToolExecutor(openAIToolExecutor{})
			response, err := conversation.RunManaged(ctx, agentframe.TurnDraft{Kind: agentframe.TurnInitial, Event: testManagedEvent(actor.ID)})
			if response != nil || err == nil || invoked() != 1 || checks != 1 || engine.FailureDispositionFor(err) != engine.FailureDispositionTerminal {
				t.Fatalf("response=%+v err=%v invocations=%d checks=%d", response, err, invoked(), checks)
			}
			if scenario == "missing_backing" && !errors.Is(err, backingFailure) {
				t.Fatalf("backing cause lost: %v", err)
			}
			if conversation.Session.ProviderSessionID != "" || conversation.Session.TurnCount != 0 || len(conversation.Session.Messages) != 0 {
				t.Fatalf("refused candidate projected mutable state: %+v", conversation.Session)
			}
			settlements := harness.CompletionSettlementsForAdapter("claude_cli")
			if len(settlements) != 1 || settlements[0].Settlement.State != effects.StateOutcomeUncertain || settlements[0].ProviderHead != nil {
				t.Fatalf("refused candidate settlement=%+v", settlements)
			}
		})
	}
}
