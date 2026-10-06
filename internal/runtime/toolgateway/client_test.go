package toolgateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
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
		{"server_failure", `private-server-explanation`, "workspace_gateway_unreachable", http.StatusServiceUnavailable},
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

func TestHTTPObservationDNSFailureRemainsTypedAndHasNoFallback(t *testing.T) {
	var resolutions, calls atomic.Int32
	previous := net.DefaultResolver
	// Keep DNS failure hermetic. This package does not run parallel tests; the
	// numeric-address control must still use the actual HTTP client below.
	net.DefaultResolver = &net.Resolver{PreferGo: true, Dial: func(context.Context, string, string) (net.Conn, error) {
		resolutions.Add(1)
		return nil, errors.New("private DNS refusal")
	}}
	t.Cleanup(func() { net.DefaultResolver = previous })
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-03-26"}}`))
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := testObservation("http://missing-workspace-gateway.invalid/mcp?private-query").Probe(ctx)
	assertObservationFailure(t, err, "workspace_gateway_unreachable")
	if resolutions.Load() == 0 || ctx.Err() != nil || calls.Load() != 0 {
		t.Fatalf("DNS failure did not discriminate resolution from timeout/fallback: resolutions=%d calls=%d ctx=%v", resolutions.Load(), calls.Load(), ctx.Err())
	}
	if err := testObservation(server.URL).Initialize(ctx); err != nil || calls.Load() != 1 {
		t.Fatalf("numeric-address HTTP control failed: calls=%d err=%v", calls.Load(), err)
	}
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
		{"server_failure_after_effect", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("private-server-explanation"))
		}},
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
		{"foreign_auth_error", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":2,"error":{"code":-32001,"message":"private"}}`))
		}},
		{"foreign_context_error", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":2,"error":{"code":-32003,"data":{"runtimeError":{"protocol_error":{"code":"mcp_context_token_not_found"}}}}}`))
		}},
		{"null_auth_error", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":null,"error":{"code":-32001}}`))
		}},
		{"unclassified_context_error", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32003}}`))
		}},
		{"ambiguous_http_unauthorized", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32001}}`))
		}},
		{"ambiguous_http_forbidden", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusForbidden)
		}},
		{"ambiguous_result_and_error", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{},"error":{"code":-32001}}`))
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

func TestHTTPObservationOnlyAttributablePreExecutionCallRefusalIsKnown(t *testing.T) {
	for _, reply := range []string{
		`{"jsonrpc":"2.0","id":1,"error":{"code":-32001}}`,
		`{"jsonrpc":"2.0","id":1,"error":{"code":-32003,"data":{"runtimeError":{"protocol_error":{"code":"mcp_context_token_not_found"}}}}}`,
	} {
		var calls atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			calls.Add(1)
			_, _ = w.Write([]byte(reply))
		}))
		_, err := testObservation(server.URL).Call(context.Background(), "emit_event", map[string]any{}, "one-call")
		server.Close()
		assertObservationFailure(t, err, "workspace_gateway_unreachable")
		if failure := failures.Normalize(err, "test", "execute"); failure.Class != failures.ClassDependencyUnavailable || calls.Load() != 1 {
			t.Fatalf("attributable pre-execution refusal changed: %+v calls=%d", failure, calls.Load())
		}
	}
}

func TestHTTPObservationBusinessCallOutlivesDiscoveryDeadline(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var request struct {
			Method string `json:"method"`
			Params struct {
				Name string            `json:"name"`
				Meta map[string]string `json:"_meta"`
			} `json:"params"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		if r.Header.Get("Authorization") != "Bearer private-boot" || r.Header.Get("X-SWARM-Context-Token") != "private-turn" || request.Method != "tools/call" || request.Params.Name != "emit_event" || request.Params.Meta["claudecode/toolUseId"] != "same-occurrence" {
			t.Error("slow call lost exact authorization or occurrence")
			return
		}
		timer := time.NewTimer(6 * time.Second)
		defer timer.Stop()
		select {
		case <-timer.C:
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"content":[]}}`))
		case <-r.Context().Done():
			t.Error("business call inherited the discovery timeout")
		}
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	result, err := testObservation(server.URL).Call(ctx, "emit_event", map[string]any{}, "same-occurrence")
	if err != nil || ctx.Err() != nil || string(result) != `{"content":[]}` || calls.Load() != 1 {
		t.Fatalf("slow business result=%s calls=%d caller=%v error=%v", result, calls.Load(), ctx.Err(), err)
	}
}

func TestHTTPObservationDiscoveryRetainsItsOwnDeadline(t *testing.T) {
	for _, method := range []string{"initialize", "tools/list"} {
		t.Run(method, func(t *testing.T) {
			t.Parallel()
			joined := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request struct{ Method string }
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
					return
				}
				if request.Method != method {
					_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-03-26"}}`))
					return
				}
				<-r.Context().Done()
				close(joined)
			}))
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			_, err := testObservation(server.URL).Probe(ctx)
			assertObservationFailure(t, err, "workspace_gateway_unreachable")
			if ctx.Err() != nil {
				t.Fatalf("discovery relied on the caller deadline: %v", ctx.Err())
			}
			select {
			case <-joined:
			case <-ctx.Done():
				t.Fatal("timed-out observation left its HTTP request running")
			}
		})
	}
}

func TestHTTPObservationCallerCancellationAfterDispatchRemainsUncertain(t *testing.T) {
	entered, joined := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		close(entered)
		<-r.Context().Done()
		close(joined)
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := testObservation(server.URL).Call(ctx, "emit_event", map[string]any{}, "same-occurrence")
		result <- err
	}()
	<-entered
	cancel()
	err := <-result
	assertObservationFailure(t, err, "workspace_tool_outcome_uncertain")
	failure := failures.Normalize(err, "test", "execute")
	if !errors.Is(err, context.Canceled) || failure.Class != failures.ClassOutcomeUncertain || failure.Retryable || calls.Load() != 1 {
		t.Fatalf("post-dispatch cancellation=%+v calls=%d error=%v", failure, calls.Load(), err)
	}
	select {
	case <-joined:
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled caller left its HTTP request running")
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
