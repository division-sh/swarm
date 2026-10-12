package llm

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/toolgateway"
	"github.com/google/uuid"
)

func TestProviderSchemaBoundaryAdmission(t *testing.T) {
	invalid := []struct {
		name   string
		schema any
	}{
		{"unsupported", map[string]any{"type": "unsupported-provider-type"}},
		{"properties", map[string]any{"properties": map[string]any{"z": false, "a": map[string]any{"type": "bad"}}}},
		{"items", map[string]any{"items": false}},
		{"propertyNames", map[string]any{"propertyNames": nil}},
		{"oneOf", map[string]any{"oneOf": []any{nil}}},
		{"union", map[string]any{"type": []any{"string", false}}},
		{"additionalProperties", map[string]any{"additionalProperties": nil}},
		{"required", map[string]any{"required": []any{1}}},
		{"enum", map[string]any{"enum": []string{"bad-shape"}}},
	}
	for _, tc := range invalid {
		t.Run("anthropic/"+tc.name, func(t *testing.T) {
			session := &Session{Tools: []ToolDefinition{{Name: "bad", Schema: tc.schema}, {Name: "later", Schema: false}}}
			want := ValidateProviderToolSchema("bad", tc.schema).Error()
			request, err := (&AnthropicAPIRuntime{}).buildRequest(session, Message{Role: "user", Content: "hello"}, "")
			requireProviderSchemaError(t, err, want)
			if len(request.Tools) != 0 {
				t.Fatal("refusal returned a partial request")
			}
			_, err = (&AnthropicAPIRuntime{}).buildRequest(session, Message{}, "")
			requireProviderSchemaError(t, err, "at least one message is required")
		})
		t.Run("mock/"+tc.name, func(t *testing.T) {
			session := &Session{Tools: []ToolDefinition{{Name: "bad", Schema: tc.schema}}}
			request, err := buildMockRequest(context.Background(), session, Message{Content: "hello"})
			requireProviderSchemaError(t, err, ValidateProviderToolSchema("bad", tc.schema).Error())
			if len(request.Tools) != 0 || len(request.Messages) != 0 {
				t.Fatal("refusal returned a partial mock input")
			}
		})
	}
	t.Run("anthropic/valid", testProviderSchemaAnthropicBytes)
	t.Run("mock/valid", testProviderSchemaMockBytes)
	t.Run("mock/continued", func(t *testing.T) {
		source := []byte("def handle(input):\n    return {'text': 'unused'}\n")
		actor := runtimeactors.AgentConfig{ID: "mock-agent", ExecutionMode: "mock"}
		actor.Mock.Kind, actor.Mock.Source, actor.Mock.Digest = "python", source, pythonSourceDigest(source)
		ctx := runtimeactors.WithActor(context.Background(), actor)
		session, err := (&MockRuntime{}).StartSession(ctx, actor.ID, "system", []ToolDefinition{{Name: "bad", Schema: map[string]any{"type": "object"}}})
		if err != nil {
			t.Fatal(err)
		}
		session.Tools[0].Schema.(map[string]any)["type"] = "unsupported-provider-type"
		_, err = buildMockRequest(ctx, session, Message{Content: "second turn"})
		requireProviderSchemaError(t, err, ValidateProviderToolDefinitions(session.Tools).Error())
	})
	t.Run("anthropic/managed-http", testProviderSchemaManagedHTTPRefusal)
}

func testProviderSchemaAnthropicBytes(t *testing.T) {
	session := &Session{SystemPrompt: "system", Tools: []ToolDefinition{
		{Name: " inspect ", Description: " description ", Schema: map[string]any{"type": " object ", "properties": map[string]any{"q": map[string]any{"type": "string"}}}},
		{Name: "empty"}, {Name: "typed-nil", Schema: map[string]any(nil)},
	}}
	before := providerSchemaJSON(t, session.Tools)
	request, err := (&AnthropicAPIRuntime{}).buildRequest(session, Message{Role: "user", Content: "hello"}, " model ")
	requireProviderSchemaError(t, err, "")
	want := `{"model":"model","max_tokens":1024,"system":"system","messages":[{"role":"user","content":"hello"}],"tools":[{"name":" inspect ","description":"description","input_schema":{"properties":{"q":{"type":"string"}},"type":" object "}},{"name":"empty","input_schema":{"properties":{},"type":"object"}},{"name":"typed-nil","input_schema":null}]}`
	if got := providerSchemaJSON(t, request); got != want {
		t.Fatalf("request bytes=%s, want %s", got, want)
	}
	if got := providerSchemaJSON(t, session.Tools); got != before {
		t.Fatalf("request mutated schemas: %s != %s", got, before)
	}
	_, err = (&AnthropicAPIRuntime{}).buildRequest(session, Message{Content: "hello"}, "")
	requireProviderSchemaError(t, err, "anthropic managed provider model is required")
}

func testProviderSchemaMockBytes(t *testing.T) {
	session := &Session{SystemPrompt: "system", TurnCount: 4,
		Messages: []Message{{Role: "user", Content: "first"}},
		Tools:    []ToolDefinition{{Name: "inspect", Schema: map[string]any{"type": "string"}}, {Name: "empty"}},
	}
	before := providerSchemaJSON(t, session)
	message := Message{Role: "tool", Content: "result"}
	request, err := buildMockRequest(context.Background(), session, message)
	requireProviderSchemaError(t, err, "")
	want := mockCompletionInput{SystemPrompt: session.SystemPrompt, Round: 5, Tools: session.Tools,
		Messages: append(append([]Message(nil), session.Messages...), message), ToolResults: []Message{message}}
	if got := providerSchemaJSON(t, request); got != providerSchemaJSON(t, want) || providerSchemaJSON(t, session) != before {
		t.Fatalf("valid mock input changed bytes: %s", got)
	}
}

func testProviderSchemaManagedHTTPRefusal(t *testing.T) {
	var sends atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		sends.Add(1)
		_, _ = w.Write([]byte(`{"model":"effect-test-model","usage":{"input_tokens":1,"output_tokens":1},"content":[{"type":"text","text":"done"}]}`))
	}))
	defer server.Close()
	harness := effecttest.New()
	runtime := NewAnthropicAPIRuntime(&config.Config{}, nil, "schema-owner", nil, nil)
	runtime.apiURL, runtime.credentialCache.value = server.URL, "local-key"
	runtime.completionController = liveTestCompletionController(harness, harness, harness, harness)
	ctx := testManagedConversationContext(t, harness, "schema-agent", "schema/instance", "worker")
	actor, _ := runtimeactors.ActorFromContext(ctx)
	ctx = agentmemory.WithExecution(runtimecorrelation.WithRunID(ctx, actor.Identity.RunID), agentmemory.Plan{}, actor.Identity)
	session := &Session{ID: uuid.NewString(), AgentID: actor.ID, MemoryIdentity: actor.Identity,
		Tools: []ToolDefinition{{Name: "bad", Schema: map[string]any{"type": "bad"}}}}
	ctx = managedProviderTestContext(t, ctx, runtime, session, session.Tools)
	response, err := runtime.continueSession(ctx, session, Message{Role: "user", Content: "test"}, managedProviderCallForEffectTest(t, ctx))
	if response != nil || err == nil || !strings.Contains(err.Error(), "bad.input_schema.type") || sends.Load() != 0 {
		t.Fatalf("schema admission failed: response=%v err=%v sends=%d", response, err, sends.Load())
	}
}

func TestProviderSchemaWorkspaceObservation(t *testing.T) {
	for _, schema := range []map[string]any{
		{"type": "unsupported-provider-type"}, {"properties": map[string]any{"bad": false}}, {"oneOf": []any{nil}},
	} {
		t.Run(providerSchemaJSON(t, schema), func(t *testing.T) {
			tool := ToolDefinition{Name: "bad", Schema: schema}
			raw, _ := json.Marshal(schema)
			err := compareWorkspaceMCPDefinitions(map[string]string{tool.Name: ToolDefinitionIdentity(tool)}, []toolgateway.ListedDefinition{{Name: tool.Name, InputSchema: raw}})
			failure, ok := failures.EnvelopeFromError(err)
			cause := errors.Unwrap(err)
			if !ok || failure.Detail.Code != "managed_capability_mcp_definition_mismatch" || failure.Detail.Attributes["reason"] != "invalid_input_schema" || cause == nil || cause.Error() != ValidateProviderToolSchema(tool.Name, schema).Error() {
				t.Fatalf("matching invalid schema confirmed or lost cause: %v", err)
			}
		})
	}
	valid := ToolDefinition{Name: "inspect", Description: "exact", Schema: map[string]any{"type": "object", "enum": []any{1.25}, "properties": map[string]any{}}}
	raw, _ := json.Marshal(valid.Schema)
	definition := toolgateway.ListedDefinition{Name: valid.Name, Description: valid.Description, InputSchema: raw}
	expected := map[string]string{valid.Name: ToolDefinitionIdentity(valid)}
	if err := compareWorkspaceMCPDefinitions(expected, []toolgateway.ListedDefinition{definition}); err != nil {
		t.Fatal(err)
	}
	if err := compareWorkspaceMCPDefinitions(nil, nil); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		defs   []toolgateway.ListedDefinition
		reason string
	}{
		{"missing", nil, "missing_definition"},
		{"null", []toolgateway.ListedDefinition{{Name: valid.Name, InputSchema: []byte(`null`)}}, "malformed_or_duplicate_definition"},
		{"array", []toolgateway.ListedDefinition{{Name: valid.Name, InputSchema: []byte(`[]`)}}, "malformed_or_duplicate_definition"},
		{"noncanonical", []toolgateway.ListedDefinition{{Name: " inspect ", InputSchema: raw}}, "malformed_or_duplicate_definition"},
		{"duplicate", []toolgateway.ListedDefinition{definition, definition}, "malformed_or_duplicate_definition"},
		{"changed", []toolgateway.ListedDefinition{{Name: valid.Name, InputSchema: raw}}, "unplanned_or_changed_definition"},
		{"foreign", []toolgateway.ListedDefinition{{Name: "foreign", InputSchema: raw}}, "unplanned_or_changed_definition"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := compareWorkspaceMCPDefinitions(expected, tc.defs)
			failure, ok := failures.EnvelopeFromError(err)
			if !ok || failure.Detail.Attributes["reason"] != tc.reason {
				t.Fatalf("earlier observation gate changed: %v", err)
			}
		})
	}
}
