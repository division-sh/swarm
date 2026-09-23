package apiv1

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/mockperformance"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/google/uuid"
)

type committedForkChatCompletionProbe struct {
	*effecttest.Harness
	cleanupErr error
}

func (p *committedForkChatCompletionProbe) AuthorizeExternalAttempt(ctx context.Context, authority runtimeeffects.Authority, req runtimeeffects.AuthorizeRequest) (runtimeeffects.Attempt, error) {
	attempt, err := p.Harness.AuthorizeExternalAttempt(ctx, authority, req)
	attempt.AuthorizationAcknowledged = err == nil
	return attempt, err
}

func (*committedForkChatCompletionProbe) IsExternalEffectAuthorityCurrent(_ context.Context, authority runtimeeffects.Authority) (bool, error) {
	return authority.Valid(), nil
}

func (p *committedForkChatCompletionProbe) SettleCompletion(ctx context.Context, attempt runtimeeffects.Attempt, settlement runtimeeffects.CompletionSettlement) (runtimeeffects.CompletionSettlementResult, error) {
	result, err := p.Harness.SettleCompletion(ctx, attempt, settlement)
	if result.Committed {
		err = errors.Join(err, p.cleanupErr)
	}
	return result, err
}

type exactAssistantForkLifecycle struct {
	*fakeConversationForkLifecycleStore
}

type unacknowledgedResponseForkExecutor struct{ err error }

func (e unacknowledgedResponseForkExecutor) ExecuteForkChat(_ context.Context, prepared runfork.ConversationForkChatPrepared, _ string) (runfork.ConversationForkChatExecution, error) {
	return runfork.ConversationForkChatExecution{
		AssistantMessage: "uncommitted assistant text", ExecutionOwner: prepared.ExecutionOwner,
		FenceGeneration: prepared.FenceGeneration,
	}, e.err
}

func (s *exactAssistantForkLifecycle) RecordOperatorConversationForkChat(ctx context.Context, req runfork.ConversationForkChatRecordRequest) (runfork.ConversationForkChatResult, error) {
	payload, err := json.Marshal(map[string]string{"message": req.Execution.AssistantMessage})
	if err != nil {
		return runfork.ConversationForkChatResult{}, err
	}
	s.recordResult = runfork.ConversationForkChatResult{
		ForkID: req.ForkID,
		Turn: operatorread.OperatorConversationTurn{
			TurnID: req.Prepared.ForkTurnID, ParseOK: true, ResponsePayload: payload,
		},
	}
	return s.fakeConversationForkLifecycleStore.RecordOperatorConversationForkChat(ctx, req)
}

func TestForkChatCommittedAssistantSurvivesCleanupErrorThroughAPI(t *testing.T) {
	const assistant = "exact committed fork assistant result"
	source := []byte(`def handle(input):
    if input["round"] == 1:
        return {"calls": [{"name": "fork_snapshot_read_entities", "arguments": {}}], "usage": {"input_tokens": 3, "output_tokens": 2}}
    return {"text": "exact committed fork assistant result", "usage": {"input_tokens": 5, "output_tokens": 5}}
`)
	harness := effecttest.New()
	probe := &committedForkChatCompletionProbe{Harness: harness, cleanupErr: errors.New("completion cleanup failed after commit")}
	runtime := runtimellm.NewMockRuntime(&config.Config{LLM: config.LLMConfig{Models: llmselection.ModelAliases{
		llmselection.ModelAliasRegular: {llmselection.BackendMock: "mock-regular"},
	}}}, sessions.NewInMemoryRegistry(time.Minute), "worker-1", nil, nil,
		runtimeeffects.NewCompletionController(probe, probe, probe, probe).WithExecutionPosture(executionposture.Live))

	forkID, forkTurnID, sourceRunID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	bundleHash := "bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	actor := runtimeactors.AgentConfig{ID: "fork-agent", ExecutionMode: runtimeeffects.ExecutionModeMock, Model: llmselection.ModelAliasRegular, Memory: agentmemory.PlatformDefault()}
	actor.Mock = mockperformance.Performance{Kind: mockperformance.KindPython, SourcePath: "mocks/agent.py", Source: source, Digest: "sha256:" + runtimeeffects.Fingerprint(source)}
	policy := runfork.CanonicalConversationForkSandboxPolicy()
	prepared := runfork.ConversationForkChatPrepared{
		Fork: runfork.OperatorConversationForkSession{ForkID: forkID, SourceRunID: sourceRunID, SourceAgentID: actor.ID},
		Snapshot: runfork.ConversationForkSnapshot{
			ForkID: forkID, SourceRunID: sourceRunID, SourceAgentID: actor.ID,
			SourceAgent: actor, SnapshotOwner: runfork.ConversationForkChatSnapshotOwner,
		},
		SandboxPolicy: policy, AvailableTools: policy.AvailableToolNames(),
		ForkTurnID: forkTurnID, SourceBundleHash: bundleHash, RequestOccurrenceID: uuid.NewString(),
		RequestHash: "request-hash", ActorTokenID: "token", ExecutionOwner: "forkchat-test-owner",
		LeaseExpiresAt: time.Now().UTC().Add(time.Minute), FenceGeneration: 1,
	}
	lifecycle := &exactAssistantForkLifecycle{&fakeConversationForkLifecycleStore{prepareResult: prepared}}
	idempotency := newMutatingProbeIdempotencyStore()
	process := worklifetime.NewProcess()
	ctx := worklifetime.WithProcess(testAuthorActivityRuntimeContext(context.Background()), process)
	owner, err := process.NewRuntime(ctx, worklifetime.RuntimeIdentity{RuntimeInstanceID: uuid.NewString(), BundleHash: bundleHash})
	if err != nil {
		t.Fatal(err)
	}
	ctx = worklifetime.WithOccurrence(ctx, owner)
	t.Cleanup(func() {
		waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := owner.RetireAndWait(waitCtx); err != nil {
			t.Error(err)
		}
		if _, err := process.Join(waitCtx); err != nil {
			t.Error(err)
		}
	})
	req := Request{Method: "conversation.fork_chat", Params: map[string]any{
		"fork_id": forkID, "message": "inspect", "idempotency_key": "chat-1",
	}, ActorTokenID: "token", RequestHash: "request-hash"}
	opts := ConversationForkHandlerOptions{Lifecycle: lifecycle, Chat: NewLLMForkChatExecutor(staticForkChatRuntimeResolver{runtime: runtime}), Idempotency: idempotency, ExecutionPosture: executionposture.Live}
	for replayIndex := 0; replayIndex < 2; replayIndex++ {
		value, err := executeConversationForkChat(ctx, req, opts, time.Now().UTC())
		if err != nil {
			t.Fatalf("fork chat attempt %d: %v", replayIndex, err)
		}
		result, ok := value.(runfork.ConversationForkChatResult)
		if !ok || !result.Turn.ParseOK || string(result.Turn.ResponsePayload) != `{"message":"`+assistant+`"}` || result.IdempotencyReplayed != (replayIndex == 1) {
			t.Fatalf("fork chat attempt %d result = %+v", replayIndex, value)
		}
	}
	if lifecycle.recordCalls != 1 || lifecycle.failCalls != 0 || lifecycle.lastRecord.Execution.AssistantMessage != assistant || !lifecycle.lastRecord.Execution.AssistantCompletionAcknowledged {
		t.Fatalf("committed assistant was failed or lost: record=%d fail=%d execution=%+v", lifecycle.recordCalls, lifecycle.failCalls, lifecycle.lastRecord.Execution)
	}
	if got := lifecycle.lastRecord.Execution.ToolCalls; len(got) != 1 || got[0].Name != "fork_snapshot_read_entities" {
		t.Fatalf("acknowledged tool round lost: %+v", got)
	}
	if err := harness.RequireState("mock_python", runtimeeffects.StateSettled); err != nil {
		t.Fatal(err)
	}
	if got := len(harness.CompletionSettlementsForAdapter("mock_python")); got != 2 {
		t.Fatalf("completion settlements = %d, want 2", got)
	}
}

func TestForkChatAPIRejectsResponseWithoutCompletionAcknowledgement(t *testing.T) {
	forkID := uuid.NewString()
	prepared := runfork.ConversationForkChatPrepared{
		Fork: runfork.OperatorConversationForkSession{ForkID: forkID}, ForkTurnID: uuid.NewString(),
		RequestOccurrenceID: uuid.NewString(), RequestHash: "request-hash", ActorTokenID: testToken,
		ExecutionOwner: "forkchat-owner", LeaseExpiresAt: time.Now().UTC().Add(time.Minute), FenceGeneration: 1,
	}
	lifecycle := &fakeConversationForkLifecycleStore{prepareResult: prepared}
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: testOperatorHandlers(testOperatorCapabilities{
			ConversationForkLifecycle: lifecycle,
			ForkChatExecutor:          unacknowledgedResponseForkExecutor{err: errors.New("provider failed before commit")},
			Idempotency:               newMutatingProbeIdempotencyStore(),
		}),
	})
	response := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"chat","method":"conversation.fork_chat","params":{"fork_id":"`+forkID+`","message":"inspect"}}`)
	if response.Error == nil || lifecycle.recordCalls != 0 || lifecycle.failCalls != 1 {
		t.Fatalf("unacknowledged response was recorded: error=%+v record=%d fail=%d", response.Error, lifecycle.recordCalls, lifecycle.failCalls)
	}
}
