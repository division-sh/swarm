package llm

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/mockperformance"
	"github.com/division-sh/swarm/internal/runtime/sessions"
)

type acknowledgedForkChatCompletionProbe struct {
	*committedCleanupCompletionProbe
}

func (p *acknowledgedForkChatCompletionProbe) AuthorizeExternalAttempt(ctx context.Context, authority runtimeeffects.Authority, req runtimeeffects.AuthorizeRequest) (runtimeeffects.Attempt, error) {
	attempt, err := p.Harness.AuthorizeExternalAttempt(ctx, authority, req)
	attempt.AuthorizationAcknowledged = err == nil
	return attempt, err
}

func (*acknowledgedForkChatCompletionProbe) IsExternalEffectAuthorityCurrent(_ context.Context, authority runtimeeffects.Authority) (bool, error) {
	return authority.Valid(), nil
}

func TestForkChatRetainsCommittedAssistantThroughCleanupError(t *testing.T) {
	source := []byte(`def handle(input):
    return {"text": "exact fork assistant result", "usage": {"input_tokens": 5, "output_tokens": 4}}
`)
	harness := effecttest.New()
	cleanup := errors.New("completion cleanup failed after commit")
	probe := &acknowledgedForkChatCompletionProbe{&committedCleanupCompletionProbe{Harness: harness, cleanupErr: cleanup}}
	runtime := NewMockRuntime(&config.Config{LLM: config.LLMConfig{Models: llmselection.ModelAliases{
		llmselection.ModelAliasRegular: {llmselection.BackendMock: "mock-regular"},
	}}}, sessions.NewInMemoryRegistry(time.Minute), "worker-1", nil, nil,
		liveTestCompletionController(probe, probe, probe, probe))
	conversation, err := NewForkChatConversation("fork-agent", "fork", "inspect", nil, agentmemory.PlatformDefault(), 2, runtime)
	if err != nil {
		t.Fatal(err)
	}
	authority := testConversationForkAuthority()
	authority.ExecutionMode = runtimeeffects.ExecutionModeMock
	actor := runtimeactors.AgentConfig{ID: "fork-agent", ExecutionMode: runtimeeffects.ExecutionModeMock, Model: llmselection.ModelAliasRegular}
	actor.Mock = mockperformance.Performance{Kind: "python", SourcePath: "mocks/agent.py", Source: source, Digest: pythonSourceDigest(source)}
	ctx := runtimeactors.WithActor(runtimeeffects.WithAuthority(context.Background(), authority), actor)
	ctx = runtimeeffects.WithExecutionMode(ctx, runtimeeffects.ExecutionModeMock)
	ctx = runtimeeffects.WithLogicalOperationIdentity(ctx, authority.ForkChat.RequestOccurrenceID)
	ctx = llmTestWorkContext(t, ctx)

	response, err := conversation.RunForkChat(ctx, "inspect")
	var acknowledged *AcknowledgedForkChatCompletionError
	if !errors.Is(err, cleanup) || !errors.As(err, &acknowledged) {
		t.Fatalf("completion error = %v, want acknowledged cleanup error", err)
	}
	if response == nil || response.Message.Content != "exact fork assistant result" || !response.forkChatCompletionAcknowledged {
		t.Fatalf("acknowledged assistant result lost: %+v", response)
	}
	if conversation.TurnCount != 1 || len(conversation.Messages) != 2 {
		t.Fatalf("conversation missed committed turn: count=%d messages=%+v", conversation.TurnCount, conversation.Messages)
	}
	if err := harness.RequireState("mock_python", runtimeeffects.StateSettled); err != nil {
		t.Fatal(err)
	}
	if got := len(harness.CompletionSettlementsForAdapter("mock_python")); got != 1 {
		t.Fatalf("completion settlements = %d, want 1", got)
	}
}

func TestForkChatDoesNotAcknowledgeResponseWithoutSettlement(t *testing.T) {
	failure := errors.New("provider failed before completion settlement")
	runtime := testConversationRuntimeAdapter{
		Runtime: &fakeRuntime{},
		continueTest: func(context.Context, *Session, Message) (*Response, error) {
			return &Response{Message: Message{Role: "assistant", Content: "uncommitted"}}, failure
		},
	}
	conversation, err := NewForkChatConversation("fork-agent", "fork", "inspect", nil, agentmemory.PlatformDefault(), 2, runtime)
	if err != nil {
		t.Fatal(err)
	}
	response, err := conversation.RunForkChat(runtimeeffects.WithAuthority(context.Background(), testConversationForkAuthority()), "inspect")
	var acknowledged *AcknowledgedForkChatCompletionError
	if !errors.Is(err, failure) || errors.As(err, &acknowledged) || response == nil || response.ForkChatCompletionAcknowledged() {
		t.Fatalf("uncommitted response incorrectly acknowledged: response=%+v err=%v", response, err)
	}
	if conversation.TurnCount != 0 {
		t.Fatalf("uncommitted response advanced conversation: %d", conversation.TurnCount)
	}
}
