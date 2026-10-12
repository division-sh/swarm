package llm

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/mockperformance"
	"github.com/division-sh/swarm/internal/runtime/sessions"
)

func TestProviderSchemaFactoringConsumers(t *testing.T) {
	t.Run("compatible", func(t *testing.T) { testProviderSchemaHTTPConstructor(t, false) })
	t.Run("responses", func(t *testing.T) { testProviderSchemaHTTPConstructor(t, true) })
	t.Run("mock", testProviderSchemaMockAdmission)
	t.Run("forkchat", testProviderSchemaForkChatAdmission)
}

func testProviderSchemaHTTPConstructor(t *testing.T, responses bool) {
	var sent atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		sent.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	compatible := NewOpenAICompatibleRuntime(openAICompatibleTestConfig(server.URL), nil, "owner", nil, nil)
	compatible.credentialCache.value = "test-key"
	responseConfig := openAIResponsesTestConfig(server.URL)
	responseConfig.LLM.Models = map[string]map[string]string{"regular": {"openai_responses": "test-model"}}
	responseRuntime := NewOpenAIResponsesRuntime(responseConfig, nil, "owner", nil, nil)
	responseRuntime.credentialCache.value = "test-key"
	build := func(s *Session, input Message, model string) (any, error) {
		if responses {
			return responseRuntime.buildRequest(s, input, model)
		}
		return compatible.buildRequest(s, input, model)
	}
	schema := map[string]any{"type": "object", "properties": map[string]any{"q": map[string]any{"type": " string "}}}
	session := &Session{ID: "session", AgentID: "agent", SystemPrompt: "system", Tools: []ToolDefinition{
		{Name: " inspect ", Description: " description ", Schema: schema},
		{Name: "empty", Schema: nil},
	}}
	before := providerSchemaJSON(t, session.Tools)
	request, err := build(session, Message{Role: "user", Content: "hello"}, " model ")
	requireProviderSchemaError(t, err, "")
	want := `{"model":"model","messages":[{"role":"system","content":"system"},{"role":"user","content":"hello"}],"tools":[{"type":"function","function":{"name":"inspect","description":"description","parameters":{"properties":{"q":{"type":" string "}},"type":"object"}}},{"type":"function","function":{"name":"empty","parameters":{"properties":{},"type":"object"}}}]}`
	if responses {
		want = `{"model":"model","instructions":"system","input":[{"role":"user","content":"hello"}],"tools":[{"type":"function","name":"inspect","description":"description","parameters":{"properties":{"q":{"type":" string "}},"type":"object"}},{"type":"function","name":"empty","parameters":{"properties":{},"type":"object"}}]}`
	}
	if got := providerSchemaJSON(t, request); got != want {
		t.Fatalf("request bytes = %s, want %s", got, want)
	}
	if after := providerSchemaJSON(t, session.Tools); after != before {
		t.Fatalf("constructor changed definitions: before=%s after=%s", before, after)
	}
	session.Tools = []ToolDefinition{
		{Name: " first ", Schema: map[string]any{"properties": map[string]any{"z": false, "a": map[string]any{"type": "bad"}}}},
		{Name: "last", Schema: false},
	}
	wantError := "provider tool schema first: first.input_schema.properties.a.type has unsupported JSON Schema type \"bad\""
	_, err = build(session, Message{Role: "user", Content: "hello"}, "")
	requireProviderSchemaError(t, err, wantError)
	// Execute the real continuation up to schema admission, not just its builder.
	ctx := runtimeactors.WithActor(context.Background(), runtimeactors.AgentConfig{ID: "agent", Model: "regular"})
	if responses {
		_, err = responseRuntime.continueSession(ctx, session, Message{Role: "user", Content: "hello"}, nil)
	} else {
		_, err = compatible.continueSession(ctx, session, Message{Role: "user", Content: "hello"}, nil)
	}
	requireProviderSchemaError(t, err, wantError)
	if got := sent.Load(); got != 0 {
		t.Fatalf("malformed schema dispatched %d HTTP requests", got)
	}
	_, err = build(&Session{Tools: session.Tools}, Message{}, "")
	wantError = "at least one message is required"
	if responses {
		wantError = "at least one input item is required"
	}
	requireProviderSchemaError(t, err, wantError)
}

type providerSchemaMemoryProbe struct{ calls int }

func (p *providerSchemaMemoryProbe) AcquireLiveSession(context.Context, agentmemory.Identity, string) (*sessions.Lease, ConversationRecord, error) {
	p.calls++
	return nil, ConversationRecord{}, errors.New("characterization memory boundary")
}

func testProviderSchemaMockAdmission(t *testing.T) {
	source := []byte("def handle(input):\n    return {'text': 'unused'}\n")
	actor := runtimeactors.AgentConfig{ID: "mock-agent", ExecutionMode: runtimeeffects.ExecutionModeMock,
		Mock: mockperformance.Performance{Kind: mockperformance.KindPython, Source: source, Digest: pythonSourceDigest(source)}}
	ctx := runtimeactors.WithActor(context.Background(), actor)
	probe := &providerSchemaMemoryProbe{}
	runtime := &MockRuntime{liveSessions: probe, lockOwner: "owner"}
	bad := []ToolDefinition{{Name: "bad", Schema: map[string]any{"oneOf": []any{nil}}}}
	_, err := runtime.StartSession(context.Background(), actor.ID, "system", bad)
	requireProviderSchemaError(t, err, "mock runtime requires the exact executing agent descriptor")
	foreign := actor
	foreign.ID = "foreign"
	_, err = runtime.StartSession(runtimeactors.WithActor(ctx, foreign), actor.ID, "system", bad)
	requireProviderSchemaError(t, err, "mock runtime requires the exact executing agent descriptor")
	live := actor
	live.ExecutionMode = runtimeeffects.ExecutionModeLive
	_, err = runtime.StartSession(runtimeactors.WithActor(ctx, live), actor.ID, "system", bad)
	requireProviderSchemaError(t, err, "agent mock-agent is not authorized for mock execution")
	missing := actor
	missing.Mock = mockperformance.Performance{}
	_, err = runtime.StartSession(runtimeactors.WithActor(ctx, missing), actor.ID, "system", bad)
	requireProviderSchemaError(t, err, "agent mock-agent selects mock execution but has no compiled Python performance; add mock.kind: python and mock.module below the contracts root")
	memoryCtx := agentmemory.WithExecution(ctx, testMemory(), testMemoryIdentity(actor.ID, "test/instance"))
	_, err = runtime.StartSession(memoryCtx, actor.ID, "system", bad)
	requireProviderSchemaError(t, err, "provider tool schema bad: bad.input_schema.oneOf[0] must be a JSON object schema")
	if probe.calls != 0 {
		t.Fatalf("schema refusal reached memory %d times", probe.calls)
	}
	good := []ToolDefinition{{Name: " inspect ", Description: "unchanged", Schema: map[string]any{"type": " string "}}}
	_, err = runtime.StartSession(memoryCtx, actor.ID, "system", good)
	requireProviderSchemaError(t, err, "characterization memory boundary")
	if probe.calls != 1 {
		t.Fatalf("valid schema reached memory %d times, want 1", probe.calls)
	}
	before := providerSchemaJSON(t, good)
	session, err := runtime.StartSession(ctx, actor.ID, "system", good)
	requireProviderSchemaError(t, err, "")
	if session.ID == "" || session.AgentID != actor.ID || session.SystemPrompt != "system" || providerSchemaJSON(t, session.Tools) != before || providerSchemaJSON(t, good) != before {
		t.Fatalf("mock session changed admitted definitions/prompt: %+v", session)
	}
}

func testProviderSchemaForkChatAdmission(t *testing.T) {
	actor := runtimeactors.AgentConfig{ID: "fork-agent"}
	ctx := runtimeactors.WithActor(runtimeeffects.WithAuthority(context.Background(), testConversationForkAuthority()), actor)
	executor := &fakeToolExec{}
	bad := []ToolDefinition{{Name: "bad", Schema: map[string]any{"enum": []string{"bad-shape"}}}}
	for _, tc := range []struct {
		name string
		ctx  context.Context
		exec CapabilityAwareToolExecutor
	}{
		{"missing-authority", runtimeactors.WithActor(context.Background(), actor), executor},
		{"missing-actor", runtimeeffects.WithAuthority(context.Background(), testConversationForkAuthority()), executor},
		{"missing-executor", ctx, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := newForkChatToolDispatch(tc.ctx, tc.exec, bad)
			requireProviderSchemaError(t, err, "forkchat tool transport requires its exact conversation authority and executor")
		})
	}
	dispatch, err := newForkChatToolDispatch(ctx, executor, bad)
	requireProviderSchemaError(t, err, "provider tool schema bad: bad.input_schema.enum must be an array, got []string")
	if dispatch != nil || executor.calls != 0 {
		t.Fatalf("schema refusal froze/dispatched work: dispatch=%v calls=%d", dispatch, executor.calls)
	}
	good := []ToolDefinition{{Name: "inspect", Description: "snapshot only", Schema: map[string]any{"type": []any{"object", "null"}}}}
	before := providerSchemaJSON(t, good)
	dispatch, err = newForkChatToolDispatch(ctx, executor, good)
	requireProviderSchemaError(t, err, "")
	if string(dispatch.definitions) != before || providerSchemaJSON(t, dispatch.ToolDefinitionsForActor(actor)) != before {
		t.Fatalf("frozen definition bytes differ: got=%s want=%s", dispatch.definitions, before)
	}
	good[0].Schema.(map[string]any)["type"] = "bad"
	if got := providerSchemaJSON(t, dispatch.ToolDefinitionsForActor(actor)); got != before {
		t.Fatalf("frozen definitions changed with caller input: %s", got)
	}
	if executor.calls != 0 {
		t.Fatalf("freezing launched %d tool calls", executor.calls)
	}
}
