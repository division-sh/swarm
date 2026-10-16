package mcp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/failures"
)

func TestGatewayToolUncertaintySeparatesAgentTextAndMachineEvidence(t *testing.T) {
	for _, class := range []failures.Class{failures.ClassOutcomeUncertain, failures.ClassSchemaInvalid} {
		t.Run(string(class), func(t *testing.T) {
			failure := failures.New(class, "presentation_control", "tool-executor", "execute", map[string]any{"attempt": "original"})
			canonical := failures.Normalize(failure, "test", "control")
			registry := newTestTurnContextRegistry()
			calls := 0
			gateway := NewGateway(testToolExecutor(func(context.Context, string, any) (any, error) {
				calls++
				return nil, failure
			}), testGatewayToken, GatewayHooks{ResolveTurnContext: registry.ResolveTurnContext})
			actor := models.AgentConfig{ID: "presentation-agent", Role: "analysis"}
			putTestTurnContext(t, registry, "presentation-context", TurnContext{Actor: actor, CapabilitySurface: testCapabilitySurface(t, actor, "query_entities"), CreatedAt: time.Now().UTC(), ExpiresAt: time.Now().UTC().Add(time.Hour)})
			request := withContextToken(httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(`{"id":"one-call","method":"tools/call","params":{"name":"query_entities","arguments":{}}}`)), "presentation-context")
			authorizeGatewayRequest(request)
			recorder := httptest.NewRecorder()
			gateway.Handler().ServeHTTP(recorder, request)
			response := mustRPCResponse(t, recorder)
			result, ok := response.Result.(map[string]any)
			if !ok || response.Error != nil || result["isError"] != true || calls != 1 {
				t.Fatalf("tool error result changed: calls=%d body=%s", calls, recorder.Body.String())
			}
			machine, err := DecodeRuntimeErrorPayload(result["runtimeError"])
			if err != nil || machine.Failure == nil || !reflect.DeepEqual(*machine.Failure, canonical) {
				t.Fatalf("machine decoder lost canonical evidence: %+v %v", machine, err)
			}
			text, err := json.Marshal(result["content"])
			if err != nil {
				t.Fatal(err)
			}
			if class == failures.ClassOutcomeUncertain {
				if !strings.Contains(string(text), "result unknown: the call may or may not have reached the system; it will not be retried automatically") || strings.Contains(strings.ToLower(string(text)), "reconcile") {
					t.Fatalf("model-text projection did not explain uncertainty: %s", text)
				}
			} else if !strings.Contains(string(text), canonical.Message) {
				t.Fatalf("ordinary error text changed: %s", text)
			}
		})
	}
}
