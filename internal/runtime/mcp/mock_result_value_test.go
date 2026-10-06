package mcp

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/mockperformance"
	"github.com/division-sh/swarm/internal/runtime/toolgateway"
	"github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/google/uuid"
)

func (w preparedContextWorkspace) ResolveForkChatWorkspace(context.Context, models.AgentConfig) (*workspace.Target, error) {
	return w.target, nil
}

type resultForkEffectHarness struct{ *effecttest.Harness }

func (*resultForkEffectHarness) IsExternalEffectAuthorityCurrent(_ context.Context, authority effects.Authority) (bool, error) {
	return authority.Valid() && authority.Kind == effects.AuthorityConversationForkChat, nil
}

func TestMockGatewayWorkerConversationPreservesResultValue(t *testing.T) {
	for _, test := range []struct {
		name  string
		value any
	}{
		{"plain_text", "hello"}, {"nil", nil}, {"json_looking_text", `{"is_still":"text"}`},
		{"structured", map[string]any{"items": []any{"text", true, nil, json.Number("7")}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			expected, err := json.Marshal(test.value)
			if err != nil {
				t.Fatal(err)
			}
			source := []byte(fmt.Sprintf(`import json
EXPECTED = json.loads(%q)
def handle(input):
    if input["messages"][-1]["role"] != "tool":
        return {"calls": [{"name": "query_entities", "arguments": {}}]}
    feedback = json.loads(input["messages"][-1]["content"])
    assert len(feedback) == 1 and feedback[0]["ok"] is True
    assert feedback[0]["result"] == EXPECTED
    return {"text": "exact type preserved"}
`, string(expected)))
			calls, liveCalls := 0, 0
			sandbox := testToolExecutor(func(context.Context, string, any) (any, error) { calls++; return test.value, nil })
			registry := NewTurnContextRegistry(models.ActorFromContext)
			server := httptest.NewServer(NewGateway(testToolExecutor(func(context.Context, string, any) (any, error) {
				liveCalls++
				return nil, fmt.Errorf("live executor must not run")
			}), testGatewayToken, GatewayHooks{ResolveTurnContext: registry.ResolveTurnContext, WithActor: models.WithActor, ActorFromContext: models.ActorFromContext}).Handler())
			defer server.Close()
			binding, err := toolgateway.NewRuntimeOwnedBinding(toolgateway.TransportHTTP, server.URL, server.URL, testGatewayToken, toolgateway.LifecycleOwnerServeBoot, toolgateway.SourceBoundMCPListener)
			if err != nil {
				t.Fatal(err)
			}
			harness := effecttest.New()
			forkHarness := &resultForkEffectHarness{harness}
			controller := effects.NewCompletionController(forkHarness, harness, harness, harness).WithExecutionPosture(executionposture.Live)
			runtime := llm.NewMockRuntime(&config.Config{LLM: config.LLMConfig{Models: selection.ModelAliases{
				selection.ModelAliasRegular: {selection.BackendMock: "mock-regular"},
			}}}, nil, "result-owner", nil, nil, controller, llm.MockRuntimeOptions{
				Workspaces: preparedContextWorkspace{target: &workspace.Target{Backend: workspace.BackendHost, Workdir: t.TempDir()}}, MCPTurns: registry, ToolGateway: binding,
			})
			actor := models.AgentConfig{ID: "result-agent", ExecutionMode: effects.ExecutionModeMock, Model: selection.ModelAliasRegular,
				Mock: mockperformance.Performance{Kind: "python", SourcePath: "mocks/result.py", Source: source, Digest: fmt.Sprintf("sha256:%x", sha256.Sum256(source))}}
			turnID := uuid.NewString()
			authority := effects.Authority{Kind: effects.AuthorityConversationForkChat, ID: turnID, ExecutionOwner: "result-owner",
				LeaseExpiresAt: time.Now().Add(time.Minute), FenceGeneration: 1, ExecutionMode: effects.ExecutionModeMock,
				ForkChat: effects.ConversationForkChatAuthority{ForkTurnID: turnID, ForkID: uuid.NewString(), SourceRunID: uuid.NewString(),
					BundleHash: "bundle-v2:sha256:" + fmt.Sprintf("%064x", 1), ActorTokenID: "result-actor", RequestOccurrenceID: uuid.NewString(), RequestHash: "result-request"}}
			ctx := models.WithActor(effects.WithAuthority(context.Background(), authority), actor)
			ctx = effects.WithExecutionMode(effects.WithLogicalOperationIdentity(ctx, authority.ForkChat.RequestOccurrenceID), effects.ExecutionModeMock)
			ctx = llm.WithConversationForkSandboxInvocationPolicy(ctx, []string{"query_entities"})
			process := worklifetime.NewProcess()
			owner, err := process.NewRuntime(ctx, worklifetime.RuntimeIdentity{RuntimeInstanceID: uuid.NewString(), BundleHash: authority.ForkChat.BundleHash})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if _, err := owner.RetireAndWait(context.Background()); err != nil {
					t.Error(err)
				}
				if _, err := process.Join(context.Background()); err != nil {
					t.Error(err)
				}
			}()
			ctx = worklifetime.WithOccurrence(worklifetime.WithProcess(ctx, process), owner)
			definition := llm.ToolDefinition{Name: "query_entities", Description: "Runtime tool", Schema: map[string]any{"type": "object", "properties": map[string]any{}, "additionalProperties": true}}
			conversation, err := llm.NewForkChatConversation(actor.ID, "", "", []llm.ToolDefinition{definition}, agentmemory.Plan{}, 3, runtime)
			if err != nil {
				t.Fatal(err)
			}
			conversation.SetToolExecutor(sandbox)
			response, err := conversation.RunForkChat(ctx, "preserve the actual result type")
			if err != nil || response == nil || response.Message.Content != "exact type preserved" || calls != 1 || liveCalls != 0 {
				t.Fatalf("actual gateway/worker/conversation result=%+v err=%v calls=%d live=%d", response, err, calls, liveCalls)
			}
			if len(harness.CompletionSettlementsForAdapter("mock_python")) != 2 {
				t.Fatal("result did not reach the next real model round")
			}
		})
	}
}
