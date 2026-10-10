package llm

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/toolidentity"
	"github.com/division-sh/swarm/internal/yamlsource"
)

func privateNameContinuation(t testing.TB, name, form string) *Response {
	t.Helper()
	block := map[string]any{"type": "tool_use", "id": "original-call", "name": name, "input": map[string]any{}}
	encode := func(value any) []byte {
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	switch form {
	case "content":
		return parseCLIResponse(encode(map[string]any{"content": []any{block}}))
	case "tool_calls":
		return parseCLIResponse(encode(map[string]any{"tool_calls": []any{map[string]any{"name": name, "arguments": map[string]any{}}}}))
	default:
		accumulator := newCLIStreamAccumulator()
		if form == "assistant" {
			accumulator.AddLine(encode(map[string]any{"type": "assistant", "message": map[string]any{"content": []any{block}}}))
		} else {
			accumulator.AddLine(encode(map[string]any{"type": "stream_event", "event": map[string]any{"type": "content_block_start", "index": 0, "content_block": block}}))
			accumulator.AddLine(encode(map[string]any{"type": "stream_event", "event": map[string]any{"type": "content_block_stop", "index": 0}}))
		}
		return accumulator.Response()
	}
}

func TestPrivateToolAdmissionPrecedesCLIExecutableContinuation(t *testing.T) {
	for _, name := range []string{"Read", "Write", "Edit", "Bash", "WebSearch", "WebFetch"} {
		for _, form := range []string{"content", "tool_calls", "assistant", "stream_event"} {
			t.Run(form+"/"+name, func(t *testing.T) {
				snapshot, err := yamlsource.Load([]byte(name + ":\n  category: provider_connector\n  handler_type: in_process\n  effect_class: non_idempotent_write\n  in_process: whatsapp.send_text\n"))
				if err != nil {
					t.Fatal(err)
				}
				executor := &selectiveToolExec{}
				_, admissionErr := contracts.AdmitToolDeclarationsValue(snapshot.Document("tools.yaml").Root())
				if admissionErr == nil {
					conversation := &Conversation{toolExecutor: executor}
					_, _, err = conversation.executeToolResponse(context.Background(), privateNameContinuation(t, name, form))
				}
				if admissionErr == nil || !strings.Contains(admissionErr.Error(), name) || !strings.Contains(admissionErr.Error(), "private tool declaration") || len(executor.calls) != 0 {
					t.Fatalf("formerly admitted private source reached continuation: admission=%v executed=%v execution=%v", admissionErr, executor.calls, err)
				}
			})
		}
	}
}

func TestPublicAliasesKeepCLIExecutableContinuations(t *testing.T) {
	for _, name := range []string{"Read", "Write", "Edit", "Bash", "WebSearch", "WebFetch"} {
		for _, form := range []string{"content", "tool_calls", "assistant", "stream_event"} {
			t.Run(form+"/"+name, func(t *testing.T) {
				snapshot, err := yamlsource.Load([]byte(name + ":\n  handler_type: http\n  http: {method: GET, url: 'https://example.invalid'}\n"))
				if err != nil {
					t.Fatal(err)
				}
				entries, err := contracts.AdmitToolDeclarationsValue(snapshot.Document("tools.yaml").Root())
				if err != nil || !entries[name].AgentExposable() {
					t.Fatalf("public source alias changed: %v", err)
				}
				executor := &selectiveToolExec{}
				conversation := &Conversation{toolExecutor: executor}
				_, _, err = conversation.executeToolResponse(context.Background(), privateNameContinuation(t, name, form))
				if err != nil || len(executor.calls) != 1 || executor.calls[0] != toolidentity.CanonicalName(name) {
					t.Fatalf("ordinary public alias changed: executed=%v err=%v", executor.calls, err)
				}
			})
		}
	}
}
