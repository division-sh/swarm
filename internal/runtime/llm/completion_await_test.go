package llm

import (
	"encoding/json"
	"testing"
	"time"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
)

// The serial cursor is checked against retained response data, without a tool invocation.
func TestCompletionAwaitCallRetainsExactSerialPrefix(t *testing.T) {
	calls := []ToolCall{{ID: "first", Name: "echo"}, {ID: "second", Name: "request"}, {ID: "third", Name: "echo"}}
	message := Message{Role: "assistant", Content: "tools", ToolCalls: calls}
	document := completionContinuationEnvelope{Version: completionContinuationVersion, Adapter: "mock_python",
		Response: Response{Message: message, ToolCalls: calls, Raw: json.RawMessage(`{"response":"tools"}`),
			ToolOutputAuthority: &ToolOutputAuthority{ProviderOperationID: "11111111-1111-4111-8111-111111111111", SettledAt: time.Unix(10, 0).UTC()}},
		Usage:      runtimeeffects.CompletionUsage{Exactness: runtimeeffects.CompletionUsageUnavailable},
		Projection: completionProjection{SessionID: "session", TurnCount: 1, FrameID: "frame", Messages: []Message{{Role: "user", Content: "start"}, message}}}
	payload, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, prefix string
		index        int
		want         string
	}{
		{"empty", `[]`, 0, "first"},
		{"prefix", `[{"tool_call_id":"first","name":"echo","ok":true,"result":{"value":"retained"}}]`, 1, "second"},
		{"failed_sibling", `[{"tool_call_id":"first","name":"echo","ok":false,"failure":{"reason":"retained"}}]`, 1, "second"},
		{"suffix_omitted", `[]`, 1, ""},
		{"unknown_call", `[{"tool_call_id":"other","name":"echo","ok":true,"result":{}}]`, 1, ""},
		{"wrong_name", `[{"tool_call_id":"first","name":"other","ok":true,"result":{}}]`, 1, ""},
		{"missing_outcome", `[{"tool_call_id":"first","name":"echo","ok":true}]`, 1, ""},
		{"both_outcomes", `[{"tool_call_id":"first","name":"echo","ok":true,"result":{},"failure":{}}]`, 1, ""},
		{"no_ok", `[{"tool_call_id":"first","name":"echo","result":{}}]`, 1, ""},
		{"null", `null`, 0, ""},
		{"object", `{}`, 0, ""},
		{"duplicate", `[{"tool_call_id":"first","tool_call_id":"other","name":"echo","ok":true,"result":{}}]`, 1, ""},
		{"past_end", `[]`, 3, ""},
		{"negative", `[]`, -1, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			frame, call, err := CompletionAwaitCall(payload, "mock_python", tc.index, []byte(tc.prefix))
			if tc.want == "" {
				if err == nil {
					t.Fatalf("invalid prefix admitted: frame=%s call=%s", frame, call)
				}
			} else if err != nil || frame != "frame" || call != tc.want {
				t.Fatalf("exact prefix refused: frame=%s call=%s err=%v", frame, call, err)
			}
		})
	}
	document.Response.ToolCalls[2].ID = "first"
	payload, err = json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := CompletionAwaitCall(payload, "mock_python", 0, []byte(`[]`)); err == nil {
		t.Fatal("duplicate assistant call identity admitted")
	}
}
