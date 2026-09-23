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

type turnIncrementFaultRegistry struct {
	sessions.Registry
	fault          error
	unacknowledged bool
	calls          int
}

func (r *turnIncrementFaultRegistry) IncrementTurnOutcome(ctx context.Context, identity agentmemory.Identity, sessionID string) (sessions.TurnIncrementResult, error) {
	r.calls++
	if r.unacknowledged {
		return sessions.TurnIncrementResult{}, r.fault
	}
	result, err := r.Registry.IncrementTurnOutcome(ctx, identity, sessionID)
	return result, errors.Join(err, r.fault)
}

func TestCompletedSessionTurnIncrementRejectsUnacknowledgedMutation(t *testing.T) {
	identity := testMemoryIdentity("agent-1", "support/inst-1")
	base := sessions.NewInMemoryRegistry(time.Minute)
	lease, err := base.Acquire(context.Background(), identity, "worker-1")
	if err != nil {
		t.Fatal(err)
	}
	fault := errors.New("turn increment not committed")
	registry := &turnIncrementFaultRegistry{Registry: base, fault: fault, unacknowledged: true}
	publisher := &eventPublisherStub{}
	err = incrementCompletedSessionTurn(context.Background(), registry, identity, lease.SessionID, identity.AgentID(), publisher)
	if !errors.Is(err, fault) || registry.calls != 1 || len(publisher.runtimeLogs) != 0 {
		t.Fatalf("unacknowledged turn projected: err=%v calls=%d logs=%+v", err, registry.calls, publisher.runtimeLogs)
	}
	record, found := base.Snapshot(identity)
	if !found || record.TurnCount != 0 {
		t.Fatalf("unacknowledged turn advanced count: record=%+v found=%v", record, found)
	}
}

func TestForkChatRetainsExactResponseAfterAcknowledgedTurnIncrementError(t *testing.T) {
	source := []byte(`def handle(input):
    return {"text": "exact assistant result", "usage": {"input_tokens": 5, "output_tokens": 4}}
`)
	harness := effecttest.New()
	probe := &acknowledgedForkChatCompletionProbe{&committedCleanupCompletionProbe{Harness: harness}}
	fault := errors.New("turn increment postcommit cleanup failed")
	registry := &turnIncrementFaultRegistry{Registry: sessions.NewInMemoryRegistry(time.Minute), fault: fault}
	publisher := &eventPublisherStub{}
	runtime := NewMockRuntime(&config.Config{LLM: config.LLMConfig{Models: llmselection.ModelAliases{
		llmselection.ModelAliasRegular: {llmselection.BackendMock: "mock-regular"},
	}}}, registry, "worker-1", nil, publisher, liveTestCompletionController(probe, probe, probe, probe))
	memory := agentmemory.Authored(true)
	conversation, err := NewForkChatConversation("fork-agent", "fork", "inspect", nil, memory, 2, runtime)
	if err != nil {
		t.Fatal(err)
	}
	authority := testConversationForkAuthority()
	authority.ExecutionMode = runtimeeffects.ExecutionModeMock
	identity := testMemoryIdentity("fork-agent", "fork/inst-1")
	identity.RunID = authority.ForkChat.SourceRunID
	actor := runtimeactors.AgentConfig{ID: "fork-agent", Identity: identity, ExecutionMode: runtimeeffects.ExecutionModeMock, Model: llmselection.ModelAliasRegular, Memory: memory}
	actor.Mock = mockperformance.Performance{Kind: "python", SourcePath: "mocks/agent.py", Source: source, Digest: pythonSourceDigest(source)}
	ctx := runtimeactors.WithActor(runtimeeffects.WithAuthority(context.Background(), authority), actor)
	ctx = agentmemory.WithExecution(ctx, memory, identity)
	ctx = runtimeeffects.WithExecutionMode(ctx, runtimeeffects.ExecutionModeMock)
	ctx = runtimeeffects.WithLogicalOperationIdentity(ctx, authority.ForkChat.RequestOccurrenceID)
	ctx = llmTestWorkContext(t, ctx)

	response, err := conversation.RunForkChat(ctx, "inspect")
	if err != nil || response == nil || response.Message.Content != "exact assistant result" {
		t.Fatalf("acknowledged turn lost response: response=%+v err=%v", response, err)
	}
	if conversation.TurnCount != 1 || len(conversation.Messages) != 2 || registry.calls != 1 {
		t.Fatalf("conversation count=%d messages=%+v increments=%d", conversation.TurnCount, conversation.Messages, registry.calls)
	}
	record, found := registry.Registry.(*sessions.InMemoryRegistry).Snapshot(identity)
	if !found || record.TurnCount != 1 {
		t.Fatalf("durable turn not projected once: record=%+v found=%v", record, found)
	}
	if len(publisher.runtimeLogs) == 0 || publisher.runtimeLogs[len(publisher.runtimeLogs)-1].Action != "session_turn_increment_postcommit_failed" {
		t.Fatalf("turn postcommit error not diagnosed: %+v", publisher.runtimeLogs)
	}
	if got := len(harness.CompletionSettlementsForAdapter("mock_python")); got != 1 {
		t.Fatalf("provider settlements=%d, want one", got)
	}
}
