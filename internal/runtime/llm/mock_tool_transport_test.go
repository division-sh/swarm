package llm

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/failures"
)

func TestMockConversationTransportUncertaintyCannotBecomeToolFeedback(t *testing.T) {
	for _, test := range []struct {
		name, reply string
		uncertain   bool
	}{
		{"committed_call_lost_response", "", true},
		{"committed_call_malformed_result", `{"jsonrpc":"2.0","id":1,"result":{"content":null}}`, true},
		{"committed_call_malformed_value", `{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"not JSON"}]}}`, true},
		{"observed_tool_failure", `{"jsonrpc":"2.0","id":1,"result":{"isError":true,"content":[{"type":"text","text":"known tool failure"}]}}`, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request struct {
					Method string `json:"method"`
					Params struct {
						Name string `json:"name"`
					} `json:"params"`
				}
				if r.Header.Get("Authorization") != "Bearer uncertain-call-proof" || r.Header.Get("X-SWARM-Context-Token") != "exact-turn" ||
					json.NewDecoder(r.Body).Decode(&request) != nil || request.Method != "tools/call" || request.Params.Name != "commit_once" {
					http.Error(w, "foreign proof request", http.StatusBadRequest)
					return
				}
				calls.Add(1)
				if test.reply == "" {
					conn, _, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Error(err)
						return
					}
					_ = conn.Close()
					return
				}
				_, _ = w.Write([]byte(test.reply))
			}))
			defer server.Close()
			registered, released := 0, 0
			options := mockHostRuntimeOptions(t)
			options.ToolGateway = testToolGatewayBinding(server.URL, server.URL, "uncertain-call-proof")
			options.MCPTurns = mcpTurnContextStoreStub{
				register:   func(context.Context, time.Duration, []string) string { registered++; return "exact-turn" },
				unregister: func(string) { released++ },
			}
			runtime := NewMockRuntime(&config.Config{}, nil, "uncertain-call-owner", nil, nil, nil, options)
			tools := []ToolDefinition{{Name: "commit_once"}, {Name: "emit_after_commit"}}
			conversation, err := NewForkChatConversation("uncertain-call-agent", "", "", tools, testMemory(), 4, runtime)
			if err != nil {
				t.Fatal(err)
			}
			conversation.Session = &Session{ID: "exact-session", AgentID: conversation.AgentID, Tools: tools}
			local := &selectiveToolExec{}
			conversation.SetToolExecutor(local)
			ctx := testConversationForkInvocationContext(tools)
			ctx = models.WithActor(effects.WithExecutionMode(ctx, effects.ExecutionModeMock), models.AgentConfig{ID: conversation.AgentID, ExecutionMode: effects.ExecutionModeMock})
			payload, _, err := conversation.executeToolResponse(ctx, &Response{ToolCalls: []ToolCall{
				{ID: "one-call", Name: "commit_once", Arguments: map[string]any{}},
				{ID: "forbidden-successor", Name: "emit_after_commit", Arguments: map[string]any{}},
			}})
			if calls.Load() != 1 || registered != 1 || released != 1 || len(local.calls) != 0 {
				t.Fatalf("transport replay/redispatch/leak: calls=%d tokens=%d/%d local=%v", calls.Load(), registered, released, local.calls)
			}
			if test.uncertain {
				failure, ok := failures.EnvelopeFromError(err)
				if !ok || failure.Class != failures.ClassOutcomeUncertain || payload != "" {
					t.Fatalf("possibly committed call became model feedback: payload=%s err=%v", payload, err)
				}
				return
			}
			var feedback []struct {
				OK bool `json:"ok"`
			}
			if err != nil || json.Unmarshal([]byte(payload), &feedback) != nil || len(feedback) != 1 || feedback[0].OK {
				t.Fatalf("known tool failure lost ordinary feedback: payload=%s err=%v", payload, err)
			}
		})
	}
}
