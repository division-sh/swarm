package llm

import (
	"encoding/json"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/division-sh/swarm/internal/runtime/agentframe"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
	"github.com/google/cel-go/cel"
)

func TestProviderTurnRendering(t *testing.T) {
	t.Run("admitted payload projection", func(t *testing.T) {
		harness := effecttest.New()
		runtime := &managedRoundRuntime{harness: harness}
		seed := testManagedSessionSeed(t, testAgentIdentity("effect-test-agent", "effect-test/instance"), "analysis", runtime)
		ctx := testManagedConversationContext(t, harness, "effect-test-agent", "effect-test/instance", "analysis")
		surface, ok := managedcapabilities.FromContext(ctx)
		if !ok {
			t.Fatal("missing genuine managed surface")
		}
		event := testManagedEvent("effect-test-agent")
		frame, err := agentframe.Complete(seed, agentframe.TurnDraft{Kind: agentframe.TurnInitial, Event: event}, agentframe.Completion{BundleHash: testAgentFrameBundleHash, Surface: surface})
		if err != nil {
			t.Fatal(err)
		}
		for _, test := range []struct{ expression, want string }{
			{"", `{"work":"admitted"}`},
			{"payload.work", "admitted"},
			{`"  " + payload.work + "  "`, "  admitted  "},
			{`{"text": payload.work, "at": event.at}`, `{"at":"1970-01-01T00:00:01Z","text":"admitted"}`},
		} {
			seed.Content = test.expression
			if test.expression != "" {
				typ := contracts.ResolvedCatalogType{Kind: contracts.CatalogTypeObject, Fields: []contracts.ResolvedCatalogField{{Name: "work", Type: contracts.ResolvedCatalogType{Kind: contracts.CatalogTypeText}}}}
				program, err := workflowexpr.PrepareAgentContentExpression(test.expression, &typ)
				if err != nil {
					t.Fatal(err)
				}
				seed.ContentPrograms = map[string]cel.Program{string(event.Type()): program}
			}
			message, err := renderManagedMessage(frame, seed, event)
			if err != nil || message.Role != "user" || message.Content != test.want || message.Name != "" {
				t.Fatalf("expression %q: message=%#v err=%v", test.expression, message, err)
			}
			wire, err := json.Marshal(toOpenAICompatibleMessages(message))
			if err != nil || strings.Contains(string(wire), "bundle_hash") || strings.Contains(string(wire), "routing_source") {
				t.Fatalf("provider copy leaked frame internals: %s %v", wire, err)
			}
		}
		if err := frame.Validate(); err != nil || frame.Turn.Event.ID != event.ID() {
			t.Fatalf("projection changed durable frame: %v", err)
		}
		seed.Content = "event.unknown"
		seed.ContentPrograms = nil
		if _, err := renderManagedMessage(frame, seed, event); err == nil {
			t.Fatal("unknown event field admitted")
		}
	})
	t.Run("text and typed failure", func(t *testing.T) {
		for _, test := range []struct {
			value any
			emit  bool
			want  string
		}{
			{"  exact output  ", false, "  exact output  "},
			{map[string]any{"value": "business"}, false, `{"value":"business"}`},
			{map[string]any{"dispatch_id": "private", "event_id": "private"}, true, "ok"},
		} {
			got, err := renderToolOutput(test.value, test.emit)
			if err != nil || got != test.want {
				t.Fatalf("got=%q err=%v", got, err)
			}
		}
		failure := runtimefailures.Normalize(runtimefailures.New(runtimefailures.ClassAuthorizationDenied, "tool_not_granted", "test", "invoke", map[string]any{"action": "invoke", "tool": "example"}), "test", "invoke")
		got, err := renderToolFailure(failure)
		if err != nil || !strings.HasPrefix(got, "error: tool_not_granted: ") {
			t.Fatalf("failure=%q err=%v", got, err)
		}
	})
	t.Run("bounded head and tail with complete receipt", func(t *testing.T) {
		original := "HEAD" + strings.Repeat("\u00e9", 27000) + "TAIL"
		entries := []map[string]any{{"name": "read_example", "tool_call_id": "call-1", "ok": true, "result": original}}
		raw, err := attachProviderToolContents(entries, []string{truncateProviderToolOutput(original)}, maxToolMessageBytes)
		if err != nil || len(raw) > maxToolMessageBytes {
			t.Fatalf("batch bytes=%d err=%v", len(raw), err)
		}
		if entries[0]["result"] != original {
			t.Fatal("complete internal receipt changed")
		}
		results := providerToolResults(string(raw))
		if len(results) != 1 || results[0].CallID != "call-1" || !utf8.ValidString(results[0].Payload) || !strings.HasPrefix(results[0].Payload, "HEAD") || !strings.HasSuffix(results[0].Payload, "TAIL") || !strings.Contains(results[0].Payload, "[... tool output truncated ...]") {
			t.Fatalf("bounded provider projection=%#v", results)
		}
		legacy := `{"kind":"tool_continuation","tool_result":[{"tool_call_id":"old","ok":true,"result":"old"}]}`
		if got := providerToolResults(legacy); len(got) != 1 || got[0].Payload != legacy {
			t.Fatal("old stored rendering changed")
		}
	})
	t.Run("named Chat Completions user only", func(t *testing.T) {
		message := Message{Role: "user", Name: "request_gate", Content: "  hello  "}
		wire, err := json.Marshal(toOpenAICompatibleMessages(message))
		if err != nil || !strings.Contains(string(wire), `"name":"request_gate"`) || !strings.Contains(string(wire), `"content":"  hello  "`) {
			t.Fatalf("wire=%s err=%v", wire, err)
		}
		for _, value := range []any{toOpenAIResponsesInputItems(message)} {
			raw, _ := json.Marshal(value)
			if strings.Contains(string(raw), `"name"`) {
				t.Fatal("Chat Completions name invented on another adapter")
			}
		}
	})
	t.Run("environment follows actual grant once", func(t *testing.T) {
		for _, test := range []struct {
			name, class    string
			callable, want bool
		}{
			{"emit_reply", "emit_allowed", true, false}, {"bash", "native_tool", false, false}, {"bash_like", "native_tool", true, false},
			{"bash", "actor_config", true, false}, {"web_search", "native_tool", true, false}, {"bash", "native_tool", true, true},
		} {
			c := &Conversation{Session: &Session{}}
			surface := managedcapabilities.Surface{Tools: []managedcapabilities.Tool{{Capability: toolcapabilities.Capability{Name: test.name, AuthorizationClass: test.class, Visible: true, Callable: test.callable}}}}
			if err := prepareEnvironmentFrame(c.Session, surface); err != nil {
				t.Fatal(err)
			}
			if (len(c.Session.Messages) == 1) != test.want {
				t.Fatalf("grant=%#v messages=%#v", test, c.Session.Messages)
			}
			if test.want {
				if c.Session.Messages[0].RuntimeContext != "environment" || !strings.Contains(c.Session.Messages[0].Content, "/workspace (read-write)") || strings.Contains(c.Session.Messages[0].Content, "Docker") {
					t.Fatal("wrong runtime environment")
				}
				if err := prepareEnvironmentFrame(c.Session, surface); err != nil || len(c.Session.Messages) != 1 {
					t.Fatal("environment repeated")
				}
			}
			c.Session.Messages = []Message{{Role: "user", Content: "retained old frame"}}
			if err := prepareEnvironmentFrame(c.Session, surface); err != nil || c.Session.Messages[0].Content != "retained old frame" {
				t.Fatal("restored conversation reprojected")
			}
		}
	})
}
