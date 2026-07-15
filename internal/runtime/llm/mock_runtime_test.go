package llm

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	"github.com/division-sh/swarm/internal/runtime/mockperformance"
)

func TestExecuteMockCompletionUsesPythonAndCanonicalCompletionAuthority(t *testing.T) {
	source := []byte(`
def handle(input):
    assert input["round"] == 1
    assert input["event"]["type"] == "message.received"
    assert input["tools"][0]["name"] == "echo"
    return {"calls": [{"name": "echo", "arguments": {"text": "hello"}}], "usage": {"input_tokens": 7, "output_tokens": 3}}
`)
	harness := effecttest.New()
	ctx := harness.CompletionContext("mock-turn")
	ctx = runtimeeffects.WithExecutionMode(ctx, runtimeeffects.ExecutionModeMock)
	actor := runtimeactors.AgentConfig{
		ID: "effect-test-agent", ExecutionMode: runtimeeffects.ExecutionModeMock,
		Mock: mockperformance.Performance{Kind: "python", SourcePath: "mocks/assistant.py", Source: source, Digest: pythonSourceDigest(source)},
	}
	request := []byte(`{"event":{"type":"message.received"},"messages":[],"tools":[{"name":"echo","schema":{"type":"object","required":["text"],"properties":{"text":{"type":"string"}},"additionalProperties":false}}],"tool_results":[],"round":1}`)
	response, _, usage, _, err := executeMockCompletion(ctx, actor, []ToolDefinition{{
		Name: "echo", Schema: map[string]any{"type": "object", "required": []any{"text"}, "properties": map[string]any{"text": map[string]any{"type": "string"}}, "additionalProperties": false},
	}}, request)
	if err != nil {
		t.Fatalf("execute mock completion: %v", err)
	}
	if len(response.ToolCalls) != 1 || response.ToolCalls[0].Name != "echo" || usage.InputTokens == nil || *usage.InputTokens != 7 {
		t.Fatalf("response=%#v usage=%#v", response, usage)
	}
	if err := harness.RequireState("mock_python", runtimeeffects.StateResponseObserved); err != nil {
		t.Fatal(err)
	}
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
