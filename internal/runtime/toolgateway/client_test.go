package toolgateway

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/failures"
)

func TestHTTPObservationUsesAuthenticatedBoundedListWithoutCallingTools(t *testing.T) {
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer private-boot" || r.Header.Get("X-SWARM-Context-Token") != "private-turn" {
			t.Error("probe lost exact boot or turn authorization")
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		var request struct {
			Method string `json:"method"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatal(err)
		}
		methods = append(methods, request.Method)
		var result any
		switch request.Method {
		case "initialize":
			result = map[string]any{"protocolVersion": "2025-03-26"}
		case "tools/list":
			result = map[string]any{"tools": []ListedDefinition{{Name: "emit_event", InputSchema: json.RawMessage(`{"type":"object"}`)}}}
		default:
			t.Errorf("probe called business method %q", request.Method)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "result": result})
	}))
	defer server.Close()
	o := testObservation(server.URL)
	definitions, err := o.Probe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(definitions) != 1 || definitions[0].Name != "emit_event" || !reflect.DeepEqual(methods, []string{"initialize", "tools/list"}) {
		t.Fatalf("definitions=%v methods=%v", definitions, methods)
	}
}

func TestHTTPObservationRefusalsStayTypedAndRedacted(t *testing.T) {
	for _, tc := range []struct {
		name, response, code string
		status               int
	}{
		{"auth", `private-server-explanation`, "workspace_gateway_unreachable", http.StatusUnauthorized},
		{"auth_rpc", `{"jsonrpc":"2.0","id":null,"error":{"code":-32001,"message":"private"}}`, "workspace_gateway_unreachable", http.StatusOK},
		{"context_auth_rpc", `{"jsonrpc":"2.0","id":1,"error":{"code":-32003,"data":{"runtimeError":{"protocol_error":{"code":"mcp_context_token_not_found","message":"private"}}}}}`, "workspace_gateway_unreachable", http.StatusOK},
		{"context_authority_rpc", `{"jsonrpc":"2.0","id":1,"error":{"code":-32003,"data":{"runtimeError":{"protocol_error":{"code":"mcp_actor_missing","message":"private"}}}}}`, "managed_capability_mcp_definition_mismatch", http.StatusOK},
		{"malformed", `{`, "managed_capability_mcp_definition_mismatch", http.StatusOK},
		{"duplicate", `{"jsonrpc":"2.0","id":2,"id":1,"result":{}}`, "managed_capability_mcp_definition_mismatch", http.StatusOK},
		{"foreign_id", `{"jsonrpc":"2.0","id":2,"result":{}}`, "managed_capability_mcp_definition_mismatch", http.StatusOK},
		{"protocol", `{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"old"}}`, "managed_capability_mcp_definition_mismatch", http.StatusOK},
		{"context", `{"jsonrpc":"2.0","id":1,"error":{"code":-32002}}`, "managed_capability_mcp_definition_mismatch", http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.response))
			}))
			defer server.Close()
			_, err := testObservation(server.URL + "/mcp?private-query").Probe(context.Background())
			assertObservationFailure(t, err, tc.code)
		})
	}
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	server.Close()
	_, err := testObservation(server.URL).Probe(context.Background())
	assertObservationFailure(t, err, "workspace_gateway_unreachable")
}

func TestHTTPObservationDistinguishesValidEmptyFromAbsentInvalidInventory(t *testing.T) {
	for _, inventory := range []string{`{"tools":[]}`, `{}`, `{"tools":null}`, `{"tools":{}}`, `{"tools":[],"nextCursor":"more"}`} {
		t.Run(inventory, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request struct {
					Method string `json:"method"`
				}
				_ = json.NewDecoder(r.Body).Decode(&request)
				result := inventory
				if request.Method == "initialize" {
					result = `{"protocolVersion":"2025-03-26"}`
				}
				_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":` + result + `}`))
			}))
			defer server.Close()
			got, err := testObservation(server.URL).Probe(context.Background())
			if inventory == `{"tools":[]}` {
				if err != nil || len(got) != 0 {
					t.Fatalf("valid empty: %v %v", got, err)
				}
				return
			}
			assertObservationFailure(t, err, "managed_capability_mcp_definition_mismatch")
		})
	}
}

func TestHTTPObservationPreservesCallOccurrenceAndGracefulCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Method string `json:"method"`
			Params struct {
				Name string            `json:"name"`
				Meta map[string]string `json:"_meta"`
			} `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&request)
		if request.Method != "tools/call" || request.Params.Name != "emit_event" || request.Params.Meta["claudecode/toolUseId"] != "same-occurrence" {
			t.Error("lost exact call coordinate")
		}
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"content":[]}}`))
	}))
	defer server.Close()
	if _, err := testObservation(server.URL).Call(context.Background(), "emit_event", map[string]any{}, "same-occurrence"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := testObservation(server.URL).Probe(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation=%v", err)
	}
	ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	_, err = testObservation(server.URL).Probe(ctx)
	assertObservationFailure(t, err, "workspace_gateway_unreachable")
}

func TestHTTPObservationLostCallReplyIsUncertainNotPreModelRefusal(t *testing.T) {
	for _, test := range []struct {
		name    string
		respond func(http.ResponseWriter, *http.Request)
	}{
		{"disconnect_after_effect", func(w http.ResponseWriter, _ *http.Request) {
			connection, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			_ = connection.Close()
		}},
		{"malformed_reply", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`private-response`)) }},
		{"foreign_reply", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":2,"result":{}}`))
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				test.respond(w, r)
			}))
			defer server.Close()
			_, err := testObservation(server.URL+"/mcp?private-query").Call(context.Background(), "emit_event", map[string]any{}, "same-occurrence")
			assertObservationFailure(t, err, "workspace_tool_outcome_uncertain")
			failure := failures.Normalize(err, "test", "execute")
			if failure.Class != failures.ClassOutcomeUncertain || failure.Retryable || calls.Load() != 1 {
				t.Fatalf("post-call outcome = %+v, calls=%d", failure, calls.Load())
			}
		})
	}
}

func testObservation(endpoint string) HTTPObservation {
	return HTTPObservation{URL: endpoint, Headers: map[string]string{"Authorization": "Bearer private-boot", "X-SWARM-Context-Token": "private-turn"}}
}

func assertObservationFailure(t *testing.T, err error, code string) {
	t.Helper()
	if err == nil {
		t.Fatal("accepted refusal")
	}
	failure := failures.Normalize(err, "test", "observe")
	if failure.Detail.Code != code {
		t.Fatalf("failure=%+v", failure)
	}
	bytes, _ := json.Marshal(failure)
	if strings.Contains(string(bytes), "private") || strings.Contains(err.Error(), "private") {
		t.Fatalf("private evidence leaked: %s", bytes)
	}
}
