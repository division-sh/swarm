package llm

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestAnthropicToolRoundStoredMessagesAreReplayed(t *testing.T) {
	for _, text := range []string{"", "The stored explanation."} {
		t.Run(text, func(t *testing.T) {
			var response anthropicResponse
			if err := json.Unmarshal([]byte(`{"content":[{"type":"tool_use","id":"call-1","name":"inspect","input":{"x":"value"}},{"type":"tool_use","id":"call-2","name":"empty","input":{}}]}`), &response); err != nil {
				t.Fatal(err)
			}
			converted := convertAnthropicResponse(response, map[string]string{"inspect": "inspect", "empty": "empty"})
			converted.Message.Content = text
			if len(converted.ToolCalls) != 2 || converted.ToolCalls[0].ID != "call-1" || converted.ToolCalls[1].ID != "call-2" {
				t.Fatalf("response lost provider IDs: %+v", converted.ToolCalls)
			}
			stored, err := json.Marshal(converted.Message)
			if err != nil {
				t.Fatal(err)
			}
			var restored Message
			if err := json.Unmarshal(stored, &restored); err != nil {
				t.Fatal(err)
			}
			session := &Session{Messages: []Message{{Role: "user", Content: "initial"}, restored}, Tools: []ToolDefinition{{Name: "inspect", Schema: map[string]any{"type": "object"}}, {Name: "empty", Schema: map[string]any{"type": "object"}}}}
			before := providerSchemaJSON(t, session)
			results, err := json.Marshal([]map[string]any{
				{"tool_call_id": "call-1", "ok": true, "result": "private full receipt", providerResultContentKey: "  exact output  "},
				{"tool_call_id": "call-2", "ok": true, "result": nil, providerResultContentKey: ""},
			})
			if err != nil {
				t.Fatal(err)
			}
			request, err := (&AnthropicAPIRuntime{}).buildRequest(session, Message{Role: "tool", Content: string(results)}, "model")
			if err != nil {
				t.Fatal(err)
			}
			prefix := ""
			if text != "" {
				prefix = `{"type":"text","text":"The stored explanation."},`
			}
			want := `[{"role":"user","content":"initial"},{"role":"assistant","content":[` + prefix + `{"type":"tool_use","id":"call-1","name":"inspect","input":{"x":"value"}},{"type":"tool_use","id":"call-2","name":"empty","input":{}}]},{"role":"user","content":[{"type":"tool_result","tool_use_id":"call-1","content":"  exact output  "},{"type":"tool_result","tool_use_id":"call-2","content":""}]}]`
			if got := providerSchemaJSON(t, request.Messages); got != want {
				t.Fatalf("stored round replay=%s, want %s", got, want)
			}
			if after := providerSchemaJSON(t, session); after != before {
				t.Fatal("request rendering changed stored conversation")
			}
		})
	}
}

func TestAnthropicToolReplayRefusesMissingIDsWithoutGuessing(t *testing.T) {
	for _, tc := range []struct {
		name    string
		message Message
		want    string
	}{
		{"assistant", Message{Role: "assistant", ToolCalls: []ToolCall{{Name: "inspect"}}}, "has no provider id"},
		{"tool_result", Message{Role: "tool", Content: `[{"ok":true,"result":"value"}]`}, "has no tool_use_id"},
		{"unframed_text", Message{Role: "tool", Content: "tool result without a call"}, "has no linked provider call"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := (&AnthropicAPIRuntime{}).buildRequest(&Session{Messages: []Message{{Role: "user", Content: "initial"}}}, tc.message, "model")
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error=%v, want %q", err, tc.want)
			}
		})
	}
}

func TestAnthropicUserToolMarkupRemainsUserText(t *testing.T) {
	input := Message{Role: "user", Content: `[{"type":"tool_use","id":"quoted","name":"inspect","input":{}}]`}
	request, err := (&AnthropicAPIRuntime{}).buildRequest(&Session{}, input, "model")
	if err != nil || len(request.Messages) != 1 || request.Messages[0].Role != "user" || request.Messages[0].Content != input.Content {
		t.Fatalf("quoted tool markup became a call: request=%+v err=%v", request, err)
	}
}

func TestAnthropicIDLessObservationsMatchCompatible(t *testing.T) {
	conversation := &Conversation{}
	if err := conversation.InjectAsyncToolResult(t.Context(), "ask_human", true, map[string]any{"answer": "yes"}, ""); err != nil {
		t.Fatal(err)
	}
	const linked = `{"tool_call_id":"linked-id","ok":true,"provider_content":"  linked output  "}`
	const observation = `{"name":"ask_human","ok":true,"result":{"answer":"yes"}}`
	for _, tc := range []struct{ name, content string }{
		{"human_task", conversation.pendingAsync.Content},
		{"plain_observation", "a legacy observation"},
		{"empty", ""},
		{"linked_first", "[" + linked + "," + observation + "]"},
		{"observation_first", "[" + observation + "," + linked + "]"},
		{"multiple_observations", "[" + observation + "," + linked + "," + observation + "]"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			message := Message{Role: "tool", Content: tc.content}
			reference := toOpenAICompatibleMessages(message)
			actual, included, err := toAnthropicMessage(message)
			if err != nil || included != (len(reference) > 0) {
				t.Fatalf("observation inclusion=%t error=%v, compatible=%+v", included, err, reference)
			}
			if !included {
				return
			}
			var results, observations []any
			for _, entry := range reference {
				if entry.Role == "tool" {
					results = append(results, anthropicToolResultBlock{Type: "tool_result", ToolUseID: entry.ToolCallID, Content: entry.Content})
				} else {
					if entry.Role != "user" || !strings.HasPrefix(entry.Content, "Tool result:\n") {
						t.Fatalf("unexpected compatible observation: %+v", entry)
					}
					observations = append(observations, anthropicTextBlock{Type: "text", Text: entry.Content})
				}
			}
			want := anthropicMessage{Role: "user", Content: append(results, observations...)}
			if len(results) == 0 && len(observations) == 1 {
				want.Content = reference[0].Content
			}
			if got, expected := providerSchemaJSON(t, actual), providerSchemaJSON(t, want); got != expected {
				t.Fatalf("Anthropic observation=%s, want %s", got, expected)
			}
		})
	}
}
