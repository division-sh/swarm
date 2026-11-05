package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"

	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/tests/support/testutil/llmapi"
)

func TestProviderToolWireNamesAcrossAPIEncoders(t *testing.T) {
	for _, protocol := range []llmapi.Protocol{llmapi.OpenAICompatible, llmapi.OpenAIResponses, llmapi.Anthropic} {
		t.Run(string(protocol), func(t *testing.T) {
			session := &Session{Tools: []ToolDefinition{{Name: "partner.ping", Description: "Server's exact description", Schema: map[string]any{"type": "object"}}},
				Messages: []Message{
					{Role: "assistant", Content: "prior reply", ToolCalls: []ToolCall{{ID: "prior-call", Name: "partner.ping", Arguments: map[string]any{"text": "prior"}}}},
					{Role: "tool", Content: `[{"tool_call_id":"prior-call","ok":true,"result":"prior result","provider_content":"prior result"}]`},
				}}
			before, _ := json.Marshal(session)
			names, err := admitProviderToolNames(session.Tools)
			if err != nil {
				t.Fatal(err)
			}
			var request any
			switch protocol {
			case llmapi.OpenAICompatible:
				request, err = (&OpenAICompatibleRuntime{}).buildRequest(session, Message{Role: "user", Content: "current"}, "model")
			case llmapi.OpenAIResponses:
				request, err = (&OpenAIResponsesRuntime{}).buildRequest(session, Message{Role: "user", Content: "current"}, "model")
			case llmapi.Anthropic:
				request, err = (&AnthropicAPIRuntime{}).buildRequest(session, Message{Role: "user", Content: "current"}, "model")
			}
			if err != nil {
				t.Fatal(err)
			}
			server := llmapi.New(t, protocol, "/provider", "synthetic-key", func(request llmapi.Request) (llmapi.Reply, error) {
				if bytes.Contains(request.Body, []byte(`"name":"partner.ping"`)) {
					return llmapi.Reply{StatusCode: 400, ErrorBody: "invalid function name"}, nil
				}
				if !bytes.Contains(request.Body, []byte(`"name":"partner_ping"`)) || !bytes.Contains(request.Body, []byte("Server's exact description")) {
					t.Error("request lost the wire name or changed the server description")
				}
				if bytes.Count(request.Body, []byte(`"name":"partner_ping"`)) != 2 {
					t.Error("replayed assistant call did not use the same wire name as its definition")
				}
				return llmapi.Reply{ID: "response", Model: "model", Tools: []llmapi.ToolCall{{ID: "wire-call", OutputID: "output-item", Name: "partner_ping", Arguments: json.RawMessage(`{"text":"value"}`)}}}, nil
			})
			payload, _ := json.Marshal(request)
			httpRequest, err := http.NewRequest(http.MethodPost, server.URL+"/provider", bytes.NewReader(payload))
			if err != nil {
				t.Fatal(err)
			}
			httpRequest.Header.Set("Authorization", "Bearer synthetic-key")
			httpRequest.Header.Set("x-api-key", "synthetic-key")
			result, err := server.Client().Do(httpRequest)
			if err != nil {
				t.Fatal(err)
			}
			defer result.Body.Close()
			body, err := io.ReadAll(result.Body)
			if err != nil || result.StatusCode != http.StatusOK {
				t.Fatalf("mock provider HTTP=%d err=%v: %s", result.StatusCode, err, body)
			}
			var response Response
			switch protocol {
			case llmapi.OpenAICompatible:
				var parsed openAICompatibleResponse
				err = json.Unmarshal(body, &parsed)
				if err == nil {
					response, err = convertOpenAICompatibleResponse(parsed, names)
				}
			case llmapi.OpenAIResponses:
				var parsed openAIResponsesResponse
				err = json.Unmarshal(body, &parsed)
				if err == nil {
					response, err = convertOpenAIResponsesResponse(parsed, names)
				}
			case llmapi.Anthropic:
				var parsed anthropicResponse
				err = json.Unmarshal(body, &parsed)
				response = convertAnthropicResponse(parsed, names)
			}
			if err != nil || len(response.ToolCalls) != 1 || response.ToolCalls[0].ID != "wire-call" || response.ToolCalls[0].Name != "partner.ping" || response.Message.ToolCalls[0].ID != "wire-call" || response.Message.ToolCalls[0].Name != "partner.ping" || !reflect.DeepEqual(response.ToolCalls[0].Arguments, map[string]any{"text": "value"}) {
				t.Fatalf("exact reverse resolution: response=%+v err=%v", response, err)
			}
			after, _ := json.Marshal(session)
			if !bytes.Equal(before, after) {
				t.Fatal("wire rendering mutated canonical definitions or history")
			}
		})
	}
}

func TestProviderToolNameAdmissionRefusesCollisionsAndLength(t *testing.T) {
	for _, tc := range []struct {
		name  string
		tools []ToolDefinition
		code  string
	}{
		{"maximum", []ToolDefinition{{Name: strings.Repeat("a", providerToolNameLimit)}}, ""},
		{"overlong", []ToolDefinition{{Name: strings.Repeat("a", providerToolNameLimit+1)}}, "provider_tool_name_invalid"},
		{"empty", []ToolDefinition{{Name: " "}}, "provider_tool_name_invalid"},
		{"collision", []ToolDefinition{{Name: "partner.ping"}, {Name: "partner_ping"}}, "provider_tool_name_collision"},
		{"duplicate", []ToolDefinition{{Name: "partner.ping"}, {Name: "partner.ping"}}, "provider_tool_name_collision"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := admitProviderToolNames(tc.tools)
			if tc.code == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			for _, contract := range []ProviderContract{OpenAICompatibleProviderContract(), OpenAIResponsesProviderContract(), (&AnthropicAPIRuntime{}).ProviderContract()} {
				_, err := CompileManagedCapabilityAdmission(runtimeactors.AgentConfig{ID: "name-admission"}, contract, tc.tools, toolcapabilities.NewSet(nil))
				failure := runtimefailures.Normalize(err, "test", "admit")
				if err == nil || failure.Class != runtimefailures.ClassSchemaInvalid || failure.Detail.Code != tc.code {
					t.Fatalf("%s admission error=%v failure=%+v", contract.RuntimeMode, err, failure)
				}
			}
			for _, runtime := range []Runtime{&OpenAICompatibleRuntime{}, &OpenAIResponsesRuntime{}, &AnthropicAPIRuntime{}} {
				_, err := runtime.StartSession(context.Background(), "name-admission", "", tc.tools)
				if err == nil || runtimefailures.Normalize(err, "test", "start").Detail.Code != tc.code {
					t.Fatalf("name refusal must precede session acquisition: %v", err)
				}
			}
		})
	}
}

func TestProviderToolWireInspectionKeepsCanonicalAuthority(t *testing.T) {
	actor := runtimeactors.AgentConfig{ID: "wire-inspection"}
	actor.Identity = testAgentIdentity(actor.ID, "")
	definition := ToolDefinition{Name: "partner.ping", Description: "unchanged", Schema: map[string]any{"type": "object"}}
	surface, err := managedCapabilityPlanForTest(runtimeactors.WithActor(context.Background(), actor), &OpenAICompatibleRuntime{}, "live", []ToolDefinition{definition},
		toolcapabilities.NewSet([]toolcapabilities.Capability{{Name: definition.Name, Visible: true, Callable: true}}), nativeCapabilityTestAuthority())
	if err != nil {
		t.Fatal(err)
	}
	if got := surface.PlannedBindingNames(managedcapabilities.BindingAPIDefinition); !reflect.DeepEqual(got, []string{"partner_ping"}) {
		t.Fatalf("inspection wire bindings=%v", got)
	}
	if _, found := surface.Capability("partner_ping"); found {
		t.Fatal("wire name became a canonical capability")
	}
	definition.Name = "partner_ping"
	if _, err := ObserveAPIRequestCapabilitySurface(surface, []ToolDefinition{definition}); err != nil {
		t.Fatal(err)
	}
	definition.Name = "partner.ping"
	if _, err := ObserveAPIRequestCapabilitySurface(surface, []ToolDefinition{definition}); err == nil {
		t.Fatal("canonical name passed as an observed wire definition")
	}
}

func TestProviderToolUnknownNamesRetainMessageButNoExecutionName(t *testing.T) {
	names, err := admitProviderToolNames([]ToolDefinition{{Name: "partner.ping"}, {Name: "read_file"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, wire := range []string{"partner.ping", "Read", "mcp__runtime-tools__read_file", "missing"} {
		call := ToolCall{ID: "original-call", Name: wire, Arguments: map[string]any{"text": "value"}}
		response := Response{ToolCalls: []ToolCall{call}, Message: Message{Role: "assistant", ToolCalls: []ToolCall{call}}}
		resolveProviderToolNames(&response, names)
		if response.ToolCalls[0].Name != "" || response.ToolCalls[0].ID != call.ID || !reflect.DeepEqual(response.Message.ToolCalls[0], call) {
			t.Fatalf("unknown %q gained an execution identity or lost replay: %+v", wire, response)
		}
		encoded, _ := json.Marshal(response)
		var recovered Response
		if err := json.Unmarshal(encoded, &recovered); err != nil || recovered.ToolCalls[0].Name != "" || recovered.Message.ToolCalls[0].Name != wire || recovered.ToolCalls[0].ID != call.ID {
			t.Fatalf("unknown resolution changed on recovery: %+v err=%v", recovered, err)
		}
	}
}
