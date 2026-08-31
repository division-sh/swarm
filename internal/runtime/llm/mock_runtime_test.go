package llm

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime/agentframe"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/mockperformance"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/google/uuid"
)

func TestObserveMockRuntimeCapabilitySurfaceBindsExactInterpreterInput(t *testing.T) {
	tool := ToolDefinition{Name: "echo", Description: "Echo text", Schema: map[string]any{"type": "object"}}
	actor := runtimeactors.AgentConfig{ID: "mock-agent", ExecutionMode: runtimeeffects.ExecutionModeMock}
	actor.Identity = testAgentIdentity(actor.ID, "")
	ctx := runtimeactors.WithActor(context.Background(), actor)
	surface, err := managedCapabilityPlanForTest(ctx, &MockRuntime{}, "mock", []ToolDefinition{tool}, toolcapabilities.NewSet([]toolcapabilities.Capability{{
		Name: tool.Name, Visible: true, Callable: true,
	}}), managedcapabilities.Authority{
		Kind: managedcapabilities.AuthorityProviderTurn, ID: uuid.NewString(), ExecutionKind: managedcapabilities.ExecutionNormalAgent,
		ExecutionAuthorityID: uuid.NewString(), SessionID: uuid.NewString(), TurnOrdinal: 1,
	})
	if err != nil {
		t.Fatalf("plan mock capability surface: %v", err)
	}
	if got := surface.PlannedBindingNames(managedcapabilities.BindingLocalRuntime); !slices.Equal(got, []string{"echo"}) {
		t.Fatalf("local-runtime bindings = %v", got)
	}
	if got := surface.EffectiveNames(); len(got) != 0 {
		t.Fatalf("effective tools before interpreter observation = %v", got)
	}
	observed, err := ObserveMockRuntimeCapabilitySurface(surface, []ToolDefinition{tool}, "sha256:module")
	if err != nil {
		t.Fatalf("observe mock capability surface: %v", err)
	}
	if got := observed.EffectiveNames(); !slices.Equal(got, []string{"echo"}) {
		t.Fatalf("effective tools after interpreter observation = %v", got)
	}
}

func TestExecuteMockCompletionUsesPythonAndCanonicalCompletionAuthority(t *testing.T) {
	source := []byte(`
import json

def handle(input):
    assert input["round"] == 1
    frame = json.loads(input["messages"][-1]["content"])
    assert frame["event"]["type"] == "message.received"
    assert input["tools"][0]["name"] == "notify_human"
    return {"calls": [{"name": "notify_human", "arguments": {"summary": "Strong match found"}}], "usage": {"input_tokens": 7, "output_tokens": 3}}
`)
	harness := effecttest.New()
	ctx := llmTestWorkContext(t, managedEffectHarnessContext(t, harness, "mock-turn"))
	ctx = runtimeeffects.WithExecutionMode(ctx, runtimeeffects.ExecutionModeMock)
	actor := runtimeactors.AgentConfig{
		ID: "effect-test-agent", ExecutionMode: runtimeeffects.ExecutionModeMock,
		Mock: mockperformance.Performance{Kind: "python", SourcePath: "mocks/assistant.py", Source: source, Digest: pythonSourceDigest(source)},
	}
	request := []byte(`{"messages":[{"role":"user","content":"{\"event\":{\"type\":\"message.received\"}}"}],"tools":[{"name":"notify_human","schema":{"type":"object","required":["summary"],"properties":{"summary":{"type":"string"},"context":{}},"additionalProperties":false}}],"tool_results":[],"round":1}`)
	response, _, usage, _, err := executeMockCompletion(ctx, actor, []ToolDefinition{notifyHumanTestToolDefinition()}, request, llmselection.ResolvedModel{ModelAlias: "regular", ConcreteModel: "mock-frame-model"}, false, managedProviderCallForEffectTest(t, ctx))
	if err != nil {
		t.Fatalf("execute mock completion: %v", err)
	}
	if len(response.ToolCalls) != 1 || response.ToolCalls[0].Name != "notify_human" || usage.InputTokens == nil || *usage.InputTokens != 7 {
		t.Fatalf("response=%#v usage=%#v", response, usage)
	}
	if err := harness.RequireState("mock_python", runtimeeffects.StateResponseObserved); err != nil {
		t.Fatal(err)
	}
}

func TestMockManagedRequestConsumesCanonicalExecutionFrame(t *testing.T) {
	source := []byte(`
import json

def handle(input):
    frame = json.loads(input["messages"][-1]["content"])
    assert frame["kind"] == "initial"
    assert frame["event"]["type"] == "work.requested"
    assert "event" not in input
    return {"text": "done", "usage": {"input_tokens": 5, "output_tokens": 2}}
`)
	harness := effecttest.New()
	registry := sessions.NewInMemoryRegistry(time.Second)
	runtime := NewMockRuntime(&config.Config{LLM: config.LLMConfig{Models: llmselection.ModelAliases{
		"hostile-alias": {llmselection.BackendMock: "hostile-config-model"},
	}}}, registry, "worker-1", nil, nil, liveTestCompletionController(harness, harness, harness, harness))
	ctx := testManagedConversationContext(t, harness, "mock-agent", "mock/inst-1", "worker")
	actor, _ := runtimeactors.ActorFromContext(ctx)
	actor.ExecutionMode = runtimeeffects.ExecutionModeMock
	actor.Model = "hostile-alias"
	actor.ResolvedModel = "hostile-actor-model"
	actor.ResolvedLLMBackend = "hostile-backend"
	actor.ResolvedLLMProvider = "hostile-provider"
	actor.ResolvedLLMTransport = "hostile-transport"
	actor.Mock = mockperformance.Performance{Kind: "python", SourcePath: "mocks/agent.py", Source: source, Digest: pythonSourceDigest(source)}
	ctx = runtimeactors.WithActor(ctx, actor)
	ctx = runtimeeffects.WithExecutionMode(ctx, runtimeeffects.ExecutionModeMock)
	conversation := newTestManagedConversation(t, "mock-agent", "mock/inst-1", "worker", nil, testMemory(), 2, runtime)
	conversation.SetToolExecutor(openAIToolExecutor{})
	response, err := conversation.RunManaged(ctx, agentframe.TurnDraft{Kind: agentframe.TurnInitial, Event: testManagedEventWithMode("mock-agent", runtimeeffects.ExecutionModeMock)})
	if err != nil {
		t.Fatalf("RunManaged: %v", err)
	}
	if response.Message.Content != "done" {
		t.Fatalf("response = %#v, want done", response)
	}
	settlements := harness.CompletionSettlementsForAdapter("mock_python")
	requireManagedSettlementProviderSelection(t, settlements)
	var input mockCompletionInput
	if len(settlements) == 1 && settlements[0].AgentTurn != nil {
		_ = json.Unmarshal(settlements[0].AgentTurn.RequestPayload, &input)
	}
	if len(settlements) != 1 || settlements[0].AgentTurn == nil || len(input.Messages) != 1 || !strings.Contains(input.Messages[0].Content, `"kind":"initial"`) {
		t.Fatalf("mock settlements = %#v, want one canonical frame request", settlements)
	}
	if settlements[0].Usage.ResolvedModel != "test-model" || settlements[0].Spend.ResolvedModel != "test-model" {
		t.Fatalf("mock settlement = %#v, want sealed frame model", settlements[0])
	}
}

func TestMockProviderTailLatencyShapesSelfTerminalization(t *testing.T) {
	t.Run("zero", func(t *testing.T) {
		if err := waitMockPostToolTail(context.Background(), mockperformance.Performance{}, true); err != nil {
			t.Fatalf("zero tail: %v", err)
		}
	})
	t.Run("non_tool_round", func(t *testing.T) {
		if err := waitMockPostToolTail(context.Background(), mockperformance.Performance{PostToolTailLatencyMS: 60_000}, false); err != nil {
			t.Fatalf("non-tool round: %v", err)
		}
	})
	t.Run("nonzero", func(t *testing.T) {
		started := time.Now()
		if err := waitMockPostToolTail(context.Background(), mockperformance.Performance{PostToolTailLatencyMS: 15}, true); err != nil {
			t.Fatalf("nonzero tail: %v", err)
		}
		if elapsed := time.Since(started); elapsed < 15*time.Millisecond {
			t.Fatalf("nonzero tail elapsed=%s, want at least 15ms", elapsed)
		}
	})
	t.Run("long_cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(context.Background())
		cause := errors.New("retire mock tail")
		cancel(cause)
		if err := waitMockPostToolTail(ctx, mockperformance.Performance{PostToolTailLatencyMS: 60_000}, true); !errors.Is(err, cause) {
			t.Fatalf("cancelled long tail error=%v, want %v", err, cause)
		}
	})
}

func TestParseMockCompletionOutputFailsClosed(t *testing.T) {
	tools := []ToolDefinition{{Name: "echo", Schema: map[string]any{"type": "object", "required": []any{"text"}, "properties": map[string]any{"text": map[string]any{"type": "string"}}, "additionalProperties": false}}}
	for name, tc := range map[string]struct {
		raw  string
		want string
	}{
		"empty":         {raw: `{}`, want: "produced no text or tool calls"},
		"unknown field": {raw: `{"text":"ok","fixture":"hidden"}`, want: "unknown field"},
		"hidden tool":   {raw: `{"calls":[{"name":"network","arguments":{}}]}`, want: "not visible"},
		"bad arguments": {raw: `{"calls":[{"name":"echo","arguments":{}}]}`, want: "is required"},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := parseMockCompletionOutput([]byte(tc.raw), nil, tools, "mock-regular")
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestMockEffectFenceRejectsEveryExternalAdapterBeforeAuthorization(t *testing.T) {
	harness := effecttest.New()
	ctx := harness.Context("mock-effect-fence")
	ctx = runtimeeffects.WithExecutionMode(ctx, runtimeeffects.ExecutionModeMock)
	ctx = agentmemory.WithExecution(ctx, agentmemory.PlatformDefault(), agentmemory.Identity{})
	for _, registration := range runtimeeffects.Registrations() {
		if registration.Adapter == "mock_python" {
			continue
		}
		if _, err := runtimeeffects.Begin(ctx, registration.Adapter, []byte("request"), nil); err == nil || !strings.Contains(err.Error(), "mock_external_effect_forbidden") {
			t.Fatalf("adapter %s fence error = %v", registration.Adapter, err)
		}
	}
	if len(harness.Attempts) != 0 {
		t.Fatalf("effect fence authorized attempts: %#v", harness.Attempts)
	}
}

func pythonSourceDigest(source []byte) string {
	return "sha256:" + runtimeeffects.Fingerprint(source)
}
