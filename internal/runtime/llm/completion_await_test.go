package llm

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/tests/support/runtime/effects/effecttest"
)

// The serial cursor is checked against retained response data, without a tool invocation.
func TestCompletionAwaitCallRetainsExactSerialPrefix(t *testing.T) {
	harness := effecttest.New()
	ctx := managedEffectHarnessContext(t, harness, t.Name())
	base, ok := managedcapabilities.FromContext(ctx)
	if !ok {
		t.Fatal("missing admitted fixture surface")
	}
	tools := []managedcapabilities.PlannedTool{}
	for _, name := range []string{"echo", "request", "read"} {
		kind := toolcapabilities.KindEmit
		if name == "read" {
			kind = toolcapabilities.KindStandard
		}
		tools = append(tools, managedcapabilities.PlannedTool{Name: name, DefinitionHash: strings.Repeat("a", 64),
			Capability: toolcapabilities.Capability{Name: name, Kind: kind, Visible: true, Callable: true},
			Bindings:   []managedcapabilities.DeliveryBinding{{Kind: managedcapabilities.BindingAPIDefinition, ExactName: name, RequiredEvidenceKind: "api_definition"}}})
	}
	surface, err := managedcapabilities.New(managedcapabilities.Plan{ActorIdentity: base.ActorIdentity, RuntimeMode: base.RuntimeMode,
		Provider: base.Provider, Transport: base.Transport, ProviderContract: base.ProviderContract, Authority: base.Authority, CreatedAt: base.CreatedAt, Tools: tools})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"echo", "request", "read"} {
		surface, err = surface.Observe(managedcapabilities.DeliveryEvidence{BindingKind: managedcapabilities.BindingAPIDefinition, ExactName: name, Kind: "api_definition", Status: managedcapabilities.EvidenceConfirmed})
		if err != nil {
			t.Fatal(err)
		}
	}
	calls := []ToolCall{{ID: "first", Name: "echo"}, {ID: "second", Name: "request"}, {ID: "third", Name: "read"}}
	message := Message{Role: "assistant", Content: "tools", ToolCalls: calls}
	document := completionContinuationEnvelope{Version: completionContinuationVersion, Adapter: "mock_python",
		Response: Response{Message: message, ToolCalls: calls, Raw: json.RawMessage(`{"response":"tools"}`),
			ToolOutputAuthority: &ToolOutputAuthority{ProviderOperationID: "11111111-1111-4111-8111-111111111111", SettledAt: time.Unix(10, 0).UTC()}},
		Usage: runtimeeffects.CompletionUsage{Exactness: runtimeeffects.CompletionUsageUnavailable, Evidence: runtimeeffects.CompletionEvidence{
			Request:        runtimeeffects.CompletionRequest{ModelAlias: "regular", RequestedModel: "frozen", BackendProfile: "anthropic", Provider: "anthropic", Transport: "api"},
			CostProvenance: runtimeeffects.CompletionCostUnavailable}.Canonical()},
		Projection: completionProjection{SessionID: "session", TurnCount: 1, FrameID: "frame", Identity: base.ActorIdentity, Messages: []Message{{Role: "user", Content: "start"}, message}}}
	payload, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := completionContinuationForPayload(payload, "mock_python"); err != nil {
		t.Fatalf("fixture must pass the existing envelope admission before any discriminator: %v", err)
	}
	operation := document.Response.ToolOutputAuthority.ProviderOperationID
	candidates, err := ReadCompletionOutputCandidates(payload, "mock_python", operation, surface)
	if err != nil || len(candidates) != 2 {
		t.Fatalf("fixture must retain one exact output per callable emit: candidates=%v err=%v", candidates, err)
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
		{"callable_non_emit", `[{"tool_call_id":"first","name":"echo","ok":true,"result":{}},{"tool_call_id":"second","name":"request","ok":true,"result":{}}]`, 2, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requestID := candidates[0].EventID
			if tc.index >= 0 && tc.index < len(candidates) {
				requestID = candidates[tc.index].EventID
			}
			frame, call, err := CompletionAwaitCall(payload, "mock_python", tc.index, []byte(tc.prefix), operation, requestID, surface)
			if tc.want == "" {
				if err == nil {
					t.Fatalf("invalid prefix admitted: frame=%s call=%s", frame, call)
				}
			} else if err != nil || frame != "frame" || call != tc.want {
				t.Fatalf("exact prefix refused: frame=%s call=%s err=%v", frame, call, err)
			}
		})
	}
	for name, args := range map[string]struct {
		operation, request string
		surface            managedcapabilities.Surface
	}{
		"different_same_run_request": {operation, "22222222-2222-4222-8222-222222222222", surface},
		"other_call_output":          {operation, candidates[1].EventID, surface},
		"foreign_operation":          {"22222222-2222-4222-8222-222222222222", candidates[0].EventID, surface},
		"not_callable_emit":          {operation, candidates[0].EventID, base},
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := CompletionAwaitCall(payload, "mock_python", 0, []byte(`[]`), args.operation, args.request, args.surface); err == nil {
				t.Fatal("unrelated request/call/capability was accepted")
			}
		})
	}
	standard := document.Response.ToolCalls[2]
	standardOutput, err := document.Response.ToolOutputAuthority.eventIdentity(completionToolCallIdentity(document.Projection.TurnCount, 2, standard))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := CompletionAwaitCall(payload, "mock_python", 2, []byte(`[{"tool_call_id":"first","name":"echo","ok":true,"result":{}},{"tool_call_id":"second","name":"request","ok":true,"result":{}}]`), operation, standardOutput.EventID(), surface); err == nil {
		t.Fatal("correctly derived output coordinate cannot make a callable non-emit into a request")
	}
	document.Response.ToolCalls[2].ID = "first"
	payload, err = json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := CompletionAwaitCall(payload, "mock_python", 0, []byte(`[]`), operation, candidates[0].EventID, surface); err == nil {
		t.Fatal("duplicate assistant call identity admitted")
	}
}
