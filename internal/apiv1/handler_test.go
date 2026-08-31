package apiv1

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/durabledata"
	operatorread "github.com/division-sh/swarm/internal/operatorread"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/gorilla/websocket"
)

const testToken = "test-v1-token"

func mustEventRunOrigin(t testing.TB, eventID, eventType string) runtimerunlifecycle.RunOrigin {
	t.Helper()
	origin, err := runtimerunlifecycle.EventRunOrigin(eventID, eventType)
	if err != nil {
		t.Fatalf("construct event run origin: %v", err)
	}
	return origin
}

func TestRegistryMethodNamesMatchGeneratedOpenRPC(t *testing.T) {
	registry := testRegistry(t)
	openRPCNames, err := OpenRPCMethodNames(complianceOpenRPCPath(repoRoot(t)))
	if err != nil {
		t.Fatalf("OpenRPCMethodNames() error = %v", err)
	}
	if got := registry.MethodNames(); !reflect.DeepEqual(got, openRPCNames) {
		t.Fatalf("registry method names drifted from generated OpenRPC:\nregistry=%v\nopenrpc=%v", got, openRPCNames)
	}
	if len(openRPCNames) != 73 {
		t.Fatalf("method count = %d, want 73", len(openRPCNames))
	}
	if _, ok := registry.Method("test.setup_entities"); !ok {
		t.Fatal("test.setup_entities missing from generated registry")
	}
	if _, ok := registry.Method("run.fork"); !ok {
		t.Fatal("run.fork missing from generated registry")
	}
	if _, ok := registry.Method("bundle.register"); ok {
		t.Fatal("retired bundle.register remains in generated registry")
	}
	if _, ok := registry.Method("rpc.unsubscribe"); !ok {
		t.Fatal("rpc.unsubscribe missing from generated registry")
	}
	if _, ok := registry.Method("runtime.nuke"); !ok {
		t.Fatal("runtime.nuke missing from generated registry")
	}
}

func TestRPCMessageBudgetBoundaries(t *testing.T) {
	calls := 0
	handler := testHandler(t, Options{AuthTokens: []string{testToken}, Handlers: map[string]MethodHandler{
		"health.ping": func(context.Context, Request) (any, error) {
			calls++
			return map[string]any{"ok": true}, nil
		},
	}})
	base := []byte(`{"jsonrpc":"2.0","id":"budget","method":"health.ping","params":{}}`)
	callSized := func(t *testing.T, size int) rpcResponse {
		t.Helper()
		if size < len(base) {
			t.Fatalf("request size %d is below base envelope %d", size, len(base))
		}
		raw := append(append([]byte(nil), base...), bytes.Repeat([]byte(" "), size-len(base))...)
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/v1/rpc", bytes.NewReader(raw))
		request.Header.Set("Authorization", "Bearer "+testToken)
		handler.ServeHTTP(recorder, testAuthorActivityRequest(request))
		if recorder.Body.Len() > durabledata.MaxRPCMessageBytes {
			t.Fatalf("response bytes = %d, exceeds %d", recorder.Body.Len(), durabledata.MaxRPCMessageBytes)
		}
		var response rpcResponse
		if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
			t.Fatalf("decode response: %v body=%s", err, recorder.Body.String())
		}
		return response
	}
	for _, size := range []int{durabledata.MaxRPCMessageBytes - 1, durabledata.MaxRPCMessageBytes} {
		response := callSized(t, size)
		if response.Error != nil {
			t.Fatalf("size %d rejected: %#v", size, response.Error)
		}
	}
	before := calls
	response := callSized(t, durabledata.MaxRPCMessageBytes+1)
	if calls != before {
		t.Fatalf("oversized request dispatched: calls %d -> %d", before, calls)
	}
	data := asMap(t, response.Error.Data)
	if data["code"] != "MESSAGE_BUDGET_EXCEEDED" {
		t.Fatalf("oversized response error = %#v", response.Error)
	}
	details := asMap(t, data["details"])
	if details["boundary"] != "rpc_message" || details["method"] != "health.ping" || details["receipt_created"] != false {
		t.Fatalf("oversized details = %#v", details)
	}

	large := testHandler(t, Options{AuthTokens: []string{testToken}, Handlers: map[string]MethodHandler{
		"health.ping": func(context.Context, Request) (any, error) {
			return map[string]any{"value": strings.Repeat("x", durabledata.MaxRPCMessageBytes)}, nil
		},
	}})
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/rpc", bytes.NewReader(base))
	request.Header.Set("Authorization", "Bearer "+testToken)
	large.ServeHTTP(recorder, testAuthorActivityRequest(request))
	if recorder.Body.Len() > durabledata.MaxRPCMessageBytes {
		t.Fatalf("bounded replacement bytes = %d", recorder.Body.Len())
	}
	var bounded rpcResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &bounded); err != nil {
		t.Fatal(err)
	}
	if asMap(t, bounded.Error.Data)["code"] != "MESSAGE_BUDGET_EXCEEDED" {
		t.Fatalf("oversized output response = %#v", bounded)
	}
}

func TestNewHandlerRejectsHandlersOutsideCanonicalCatalog(t *testing.T) {
	_, err := NewHandler(Options{
		Registry:   testRegistry(t),
		AuthTokens: []string{testToken},
		Handlers: map[string]MethodHandler{
			"not.in.catalog": func(context.Context, Request) (any, error) {
				return nil, nil
			},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "not in the canonical method catalog") {
		t.Fatalf("NewHandler() error = %v, want canonical catalog rejection", err)
	}
}

func TestDefaultLoopbackAPITokenAllowedHost(t *testing.T) {
	for _, host := range []string{"127.0.0.1", "127.42.0.7", "::1", "[::1]"} {
		if !DefaultLoopbackAPITokenAllowedHost(host) {
			t.Fatalf("host %q rejected, want numeric loopback accepted", host)
		}
	}
	for _, host := range []string{"", "localhost", "0.0.0.0", "::", "192.168.1.10", "example.test"} {
		if DefaultLoopbackAPITokenAllowedHost(host) {
			t.Fatalf("host %q accepted, want non-loopback/DNS rejected", host)
		}
	}
}

func TestLegacyEnvironmentTokensDoNotAuthorizeV1Transports(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "")
	t.Setenv("SWARM_BUILDER_AUTH_TOKEN", "legacy-builder")
	t.Setenv("SWARM_OPERATOR_AUTH_TOKEN", "legacy-operator")

	handler := testHandler(t, Options{AuthTokens: []string{DefaultLoopbackAPIToken}})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/rpc", strings.NewReader(`{"jsonrpc":"2.0","id":"auth","method":"rpc.unsubscribe","params":{"subscription_id":"sub-1"}}`))
	req.Header.Set("Authorization", "Bearer legacy-builder")
	handler.ServeHTTP(rec, testAuthorActivityRequest(req))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("/v1/rpc status = %d, want 401 with only the default canonical token configured body=%s", rec.Code, rec.Body.String())
	}

	server := httptest.NewServer(handler)
	defer server.Close()
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/ws"
	_, resp, err := websocket.DefaultDialer.Dial(wsURL, http.Header{"Authorization": []string{"Bearer legacy-operator"}})
	if err == nil {
		t.Fatal("expected websocket auth failure")
	}
	if resp == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("/v1/ws response = %#v, want 401 with only the default canonical token configured", resp)
	}
}

func TestHandlerHTTPAuthBoundary(t *testing.T) {
	cases := []struct {
		name       string
		tokens     []string
		authHeader string
		wantStatus int
		wantWWW    bool
	}{
		{name: "auth not configured", tokens: nil, authHeader: "Bearer " + testToken, wantStatus: http.StatusServiceUnavailable},
		{name: "missing auth", tokens: []string{testToken}, wantStatus: http.StatusUnauthorized, wantWWW: true},
		{name: "malformed auth", tokens: []string{testToken}, authHeader: "Basic nope", wantStatus: http.StatusUnauthorized, wantWWW: true},
		{name: "invalid bearer", tokens: []string{testToken}, authHeader: "Bearer wrong", wantStatus: http.StatusUnauthorized, wantWWW: true},
		{name: "valid bearer", tokens: []string{testToken}, authHeader: "Bearer " + testToken, wantStatus: http.StatusOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			handler := testHandler(t, Options{AuthTokens: tc.tokens})
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/v1/rpc", strings.NewReader(`{"jsonrpc":"2.0","id":"auth","method":"rpc.unsubscribe","params":{"subscription_id":"sub-1"}}`))
			if tc.authHeader != "" {
				req.Header.Set("Authorization", tc.authHeader)
			}
			handler.ServeHTTP(rec, testAuthorActivityRequest(req))

			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d body=%s", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if tc.wantWWW && !strings.Contains(rec.Header().Get("WWW-Authenticate"), "Bearer") {
				t.Fatalf("WWW-Authenticate = %q, want bearer challenge", rec.Header().Get("WWW-Authenticate"))
			}
		})
	}
}

func TestHandlerHTTPJSONRPCEnvelopeAndErrorSemantics(t *testing.T) {
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: map[string]MethodHandler{
			"health.ping": func(context.Context, Request) (any, error) {
				return nil, errors.New("boom")
			},
		},
	})

	tests := []struct {
		name      string
		body      string
		headerCID string
		wantCode  int
		wantApp   string
		wantOK    bool
	}{
		{name: "parse error", body: `{`, wantCode: codeParseError},
		{name: "invalid request", body: `{"jsonrpc":"2.0","method":"rpc.unsubscribe"}`, wantCode: codeInvalidRequest},
		{name: "invalid request object id", body: `{"jsonrpc":"2.0","id":{},"method":"rpc.unsubscribe","params":{"subscription_id":"sub-1"}}`, wantCode: codeInvalidRequest},
		{name: "invalid request array id", body: `{"jsonrpc":"2.0","id":[],"method":"rpc.unsubscribe","params":{"subscription_id":"sub-1"}}`, wantCode: codeInvalidRequest},
		{name: "invalid request boolean id", body: `{"jsonrpc":"2.0","id":true,"method":"rpc.unsubscribe","params":{"subscription_id":"sub-1"}}`, wantCode: codeInvalidRequest},
		{name: "duplicate semantic key", body: `{"jsonrpc":"2.0","id":"duplicate-key","method":"run.list","params":{"limit":1,"limit":2}}`, wantCode: codeInvalidRequest},
		{name: "unsafe integer param", body: `{"jsonrpc":"2.0","id":"unsafe-integer","method":"run.list","params":{"limit":9007199254740992}}`, wantCode: codeInvalidRequest},
		{name: "negative zero param", body: `{"jsonrpc":"2.0","id":"negative-zero","method":"run.list","params":{"limit":-0}}`, wantCode: codeInvalidRequest},
		{name: "positive underflow param", body: `{"jsonrpc":"2.0","id":"positive-underflow","method":"run.list","params":{"limit":1e-4000}}`, wantCode: codeInvalidRequest},
		{name: "negative underflow param", body: `{"jsonrpc":"2.0","id":"negative-underflow","method":"run.list","params":{"limit":-1e-4000}}`, wantCode: codeInvalidRequest},
		{name: "representable subnormal reaches schema validation", body: `{"jsonrpc":"2.0","id":"subnormal","method":"run.list","params":{"limit":5e-324}}`, wantCode: codeInvalidParams},
		{name: "method not found", body: `{"jsonrpc":"2.0","id":"missing","method":"missing.method","params":{}}`, wantCode: codeMethodNotFound},
		{name: "invalid params object", body: `{"jsonrpc":"2.0","id":"bad-params-object","method":"run.get","params":["run-1"]}`, wantCode: codeInvalidParams},
		{name: "invalid params required", body: `{"jsonrpc":"2.0","id":"bad-params-required","method":"run.get","params":{}}`, wantCode: codeInvalidParams},
		{name: "invalid integer param", body: `{"jsonrpc":"2.0","id":"bad-integer","method":"run.list","params":{"limit":1.5}}`, wantCode: codeInvalidParams},
		{name: "known business method unavailable", body: `{"jsonrpc":"2.0","id":"known","method":"run.list","params":{}}`, wantApp: MethodUnavailableCode},
		{name: "internal error", body: `{"jsonrpc":"2.0","id":"internal","method":"health.ping","params":{}}`, wantCode: codeInternalError},
		{name: "unsubscribe wrong transport", body: `{"jsonrpc":"2.0","id":"wrong-transport","method":"rpc.unsubscribe","params":{"subscription_id":"sub-1"}}`, headerCID: "trace-123", wantCode: codeMethodNotFound},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/v1/rpc", strings.NewReader(tc.body))
			req.Header.Set("Authorization", "Bearer "+testToken)
			if tc.headerCID != "" {
				req.Header.Set("X-Correlation-ID", tc.headerCID)
			}
			handler.ServeHTTP(rec, testAuthorActivityRequest(req))

			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200 body=%s", rec.Code, rec.Body.String())
			}
			var resp rpcResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
				t.Fatalf("decode rpc response: %v body=%s", err, rec.Body.String())
			}
			if tc.wantOK {
				if resp.Error != nil {
					t.Fatalf("error = %#v, want success", resp.Error)
				}
				result, ok := resp.Result.(map[string]any)
				if !ok || result["ok"] != true {
					t.Fatalf("result = %#v, want ok true", resp.Result)
				}
				if got := rec.Header().Get("X-Correlation-ID"); got != tc.headerCID {
					t.Fatalf("X-Correlation-ID = %q, want %q", got, tc.headerCID)
				}
				return
			}
			if resp.Error == nil {
				t.Fatalf("error = nil, want code/app error")
			}
			if tc.wantApp != "" {
				data := asMap(t, resp.Error.Data)
				if got := data["code"]; got != tc.wantApp {
					t.Fatalf("application data.code = %v, want %s", got, tc.wantApp)
				}
				if _, ok := data["correlation_id"].(string); !ok {
					t.Fatalf("application data missing correlation_id: %#v", data)
				}
				return
			}
			if resp.Error.Code != tc.wantCode {
				t.Fatalf("error code = %d, want %d body=%s", resp.Error.Code, tc.wantCode, rec.Body.String())
			}
			data := asMap(t, resp.Error.Data)
			if _, ok := data["correlation_id"].(string); !ok {
				t.Fatalf("standard error data missing correlation_id: %#v", data)
			}
		})
	}
}

func TestRequestBodyHashUsesCanonicalSemanticNumbers(t *testing.T) {
	lower := requestBodyHash("mailbox.decide", mustTestSemanticObject(map[string]any{"fields": map[string]any{"score": float64(9007199254740990)}}))
	upper := requestBodyHash("mailbox.decide", mustTestSemanticObject(map[string]any{"fields": map[string]any{"score": float64(9007199254740991)}}))
	if lower == upper {
		t.Fatalf("distinct safe integers share request hash %q", lower)
	}
	integer := requestBodyHash("mailbox.decide", mustTestSemanticObject(map[string]any{"fields": map[string]any{"score": 1}}))
	decimal := requestBodyHash("mailbox.decide", mustTestSemanticObject(map[string]any{"fields": map[string]any{"score": 1.0}}))
	if integer != decimal {
		t.Fatalf("equivalent semantic numbers have different hashes: %q != %q", integer, decimal)
	}
}

func TestChannelOnboardingRequestHashDoesNotDigestProviderCredential(t *testing.T) {
	credentialA := mustTestSemanticObject(map[string]any{"provider": "telegram", "provider_credential": "secret-a"})
	credentialB := mustTestSemanticObject(map[string]any{"provider": "telegram", "provider_credential": "secret-b"})
	withoutCredential := mustTestSemanticObject(map[string]any{"provider": "telegram"})

	startA := requestBodyHash("channel.onboarding_start", requestHashParams("channel.onboarding_start", credentialA))
	startB := requestBodyHash("channel.onboarding_start", requestHashParams("channel.onboarding_start", credentialB))
	if startA != startB {
		t.Fatalf("credential values produced distinct reusable request digests: %q != %q", startA, startB)
	}
	if startA == requestBodyHash("channel.onboarding_start", requestHashParams("channel.onboarding_start", withoutCredential)) {
		t.Fatal("credential presence was erased from onboarding request identity")
	}
	if requestBodyHash("unrelated", requestHashParams("unrelated", credentialA)) == requestBodyHash("unrelated", requestHashParams("unrelated", credentialB)) {
		t.Fatal("credential redaction leaked into unrelated API methods")
	}
}

func TestJSONRPCOutputAdmissionFailsClosedBeforeHTTPAndWebSocketWrites(t *testing.T) {
	const unsafeInteger = int64(9007199254740992)
	const safeInteger = int64(9007199254740991)

	t.Run("http response", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		writeRPC(recorder, rpcResponse{JSONRPC: jsonRPCVersion, ID: "unsafe-http", Result: map[string]any{"value": unsafeInteger}})
		if _, err := canonicaljson.Decode(recorder.Body.Bytes()); err != nil {
			t.Fatalf("HTTP fallback is not admitted semantic JSON: %v body=%s", err, recorder.Body.String())
		}
		if strings.Contains(recorder.Body.String(), "9007199254740992") {
			t.Fatalf("HTTP response leaked unsafe result: %s", recorder.Body.String())
		}
		var failure rpcResponse
		if err := json.Unmarshal(recorder.Body.Bytes(), &failure); err != nil {
			t.Fatal(err)
		}
		if failure.Error == nil || failure.Error.Code != codeInternalError || failure.ID != "unsafe-http" {
			t.Fatalf("HTTP response = %#v, want correlated internal failure", failure)
		}
		requireRPCFailure(t, failure.Error, runtimefailures.ClassInternalFailure, "typed_read_result_marshal_failed")

		safe := httptest.NewRecorder()
		writeRPC(safe, rpcResponse{JSONRPC: jsonRPCVersion, ID: "safe-http", Result: map[string]any{"value": safeInteger}})
		encoded, err := canonicaljson.Decode(safe.Body.Bytes())
		if err != nil {
			t.Fatalf("safe HTTP response rejected: %v body=%s", err, safe.Body.String())
		}
		result, _ := encoded.Lookup("result")
		value, _ := result.Lookup("value")
		number, ok := value.Number()
		if !ok || number != float64(safeInteger) {
			t.Fatalf("safe HTTP response value = %#v, want %d", value.Interface(), safeInteger)
		}
	})

	t.Run("websocket response remains usable", func(t *testing.T) {
		handler := testHandler(t, Options{
			AuthTokens: []string{testToken},
			Handlers: map[string]MethodHandler{
				"health.subscribe": func(context.Context, Request) (any, error) {
					return map[string]any{"value": unsafeInteger}, nil
				},
			},
		})
		server := httptest.NewServer(handler)
		defer server.Close()
		conn := dialTestWS(t, server.URL)
		defer conn.Close()

		writeWSRequest(t, conn, map[string]any{"jsonrpc": jsonRPCVersion, "id": "unsafe-ws", "method": "health.subscribe", "params": map[string]any{}})
		failure := readWSResponse(t, conn)
		if failure.Error == nil || failure.Error.Code != codeInternalError || failure.ID != "unsafe-ws" {
			t.Fatalf("WebSocket response = %#v, want correlated internal failure", failure)
		}
		requireRPCFailure(t, failure.Error, runtimefailures.ClassInternalFailure, "typed_read_result_marshal_failed")

		writeWSRequest(t, conn, map[string]any{"jsonrpc": jsonRPCVersion, "id": "after-failure", "method": "rpc.unsubscribe", "params": map[string]any{"subscription_id": "sub-1"}})
		after := readWSResponse(t, conn)
		if after.Error != nil || asMap(t, after.Result)["ok"] != true {
			t.Fatalf("WebSocket did not remain usable after response fallback: %#v", after)
		}
	})

	t.Run("websocket notification closes after safe failure", func(t *testing.T) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				return
			}
			ctx, cancel := context.WithCancel(r.Context())
			session := &webSocketSession{conn: conn, ctx: ctx, cancel: cancel, out: make(chan outboundMessage, 1), subs: map[string]context.CancelFunc{}}
			go session.writeLoop()
			_ = session.notify("unsafe-subscription", map[string]any{"value": unsafeInteger})
			<-ctx.Done()
		}))
		defer server.Close()
		wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
		conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()

		raw := requireWSMessage(t, conn, "safe WebSocket notification failure")
		if _, err := canonicaljson.Decode(raw); err != nil {
			t.Fatalf("WebSocket notification fallback is not admitted semantic JSON: %v raw=%s", err, raw)
		}
		if strings.Contains(string(raw), "9007199254740992") {
			t.Fatalf("WebSocket notification leaked unsafe result: %s", raw)
		}
		var failure rpcResponse
		if err := json.Unmarshal(raw, &failure); err != nil {
			t.Fatal(err)
		}
		if failure.Error == nil || failure.Error.Code != codeInternalError || failure.ID != nil {
			t.Fatalf("WebSocket notification fallback = %#v", failure)
		}
		requireRPCFailure(t, failure.Error, runtimefailures.ClassInternalFailure, "typed_read_result_marshal_failed")
		if _, _, err := conn.ReadMessage(); err == nil {
			t.Fatal("WebSocket notification admission failure left the connection open")
		}
	})
}

func TestHandlerLogsInternalFallbackErrors(t *testing.T) {
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: map[string]MethodHandler{
			"event.publish": func(context.Context, Request) (any, error) {
				return nil, errors.New("boom-event-publish-internal")
			},
		},
	})

	var resp rpcResponse
	logOutput := captureProcessLog(t, func() {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/v1/rpc", strings.NewReader(`{"jsonrpc":"2.0","id":null,"method":"event.publish","params":{"event_name":"scan.requested","run_id":"run-log","payload":{"entity_id":"entity-log","secret":"do-not-log"}}}`))
		req.Header.Set("Authorization", "Bearer "+testToken)
		req.Header.Set("X-Correlation-ID", "trace-log")
		handler.ServeHTTP(rec, testAuthorActivityRequest(req))

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200 body=%s", rec.Code, rec.Body.String())
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode rpc response: %v body=%s", err, rec.Body.String())
		}
	})

	if resp.Error == nil || resp.Error.Code != codeInternalError {
		t.Fatalf("error = %#v, want internal fallback", resp.Error)
	}
	for _, want := range []string{
		"runtime.error component=api",
		"json-rpc internal error",
		`"method":"event.publish"`,
		`"correlation_id":"trace-log"`,
		`"run_id":"run-log"`,
		`"event_name":"scan.requested"`,
		`"entity_id":"entity-log"`,
		"platform.internal_failure",
		"unclassified_runtime_error",
	} {
		if !strings.Contains(logOutput, want) {
			t.Fatalf("log output = %q, want substring %q", logOutput, want)
		}
	}
	if strings.Contains(logOutput, "boom-event-publish-internal") {
		t.Fatalf("log output leaked raw error prose: %q", logOutput)
	}
	if strings.Contains(logOutput, "do-not-log") || strings.Contains(logOutput, "secret") {
		t.Fatalf("log output leaked payload data: %q", logOutput)
	}
}

func TestHandlerCanonicalizesMalformedTypedFallbackFailure(t *testing.T) {
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: map[string]MethodHandler{
			"health.ping": func(context.Context, Request) (any, error) {
				return nil, &runtimefailures.Error{Failure: runtimefailures.Envelope{
					SchemaVersion: runtimefailures.EnvelopeSchemaVersion,
					Class:         runtimefailures.ClassConnectorFailure,
					Detail:        runtimefailures.Detail{Code: "provider_rate_limited"},
					Retryable:     true,
					Message:       "forged presentation",
					Remediation:   "forged remediation",
				}}
			},
		},
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/rpc", strings.NewReader(`{"jsonrpc":"2.0","id":"malformed","method":"health.ping","params":{}}`))
	req.Header.Set("Authorization", "Bearer "+testToken)
	handler.ServeHTTP(rec, testAuthorActivityRequest(req))

	var resp rpcResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode RPC response: %v body=%s", err, rec.Body.String())
	}
	if resp.Error == nil || resp.Error.Code != codeInternalError {
		t.Fatalf("RPC error = %#v, want internal fallback", resp.Error)
	}
	requireRPCFailure(t, resp.Error, runtimefailures.ClassInternalFailure, "invalid_failure_construction")
}

func TestHandlerWebSocketAuthAndFrameValidation(t *testing.T) {
	server := httptest.NewServer(testHandler(t, Options{AuthTokens: []string{testToken}}))
	defer server.Close()
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/ws"

	_, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err == nil {
		t.Fatal("missing auth websocket dial unexpectedly succeeded")
	}
	if resp == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("missing auth upgrade response = %#v, want 401", resp)
	}

	_, resp, err = websocket.DefaultDialer.Dial(wsURL, http.Header{"Authorization": []string{"Bearer wrong"}})
	if err == nil {
		t.Fatal("invalid auth websocket dial unexpectedly succeeded")
	}
	if resp == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("invalid auth upgrade response = %#v, want 401", resp)
	}

	conn, _, err := websocket.DefaultDialer.Dial(wsURL, http.Header{"Authorization": []string{"Bearer " + testToken}})
	if err != nil {
		t.Fatalf("valid websocket dial: %v", err)
	}
	defer conn.Close()

	if err := conn.WriteMessage(websocket.TextMessage, []byte(`{`)); err != nil {
		t.Fatalf("write invalid frame: %v", err)
	}
	var invalid rpcResponse
	if err := conn.ReadJSON(&invalid); err != nil {
		t.Fatalf("read invalid-frame response: %v", err)
	}
	if invalid.Error == nil || invalid.Error.Code != codeParseError {
		t.Fatalf("invalid-frame error = %#v, want parse error", invalid.Error)
	}

	if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"jsonrpc":"2.0","id":"ws-unsafe","method":"rpc.unsubscribe","params":{"subscription_id":"sub-1","unsafe":9007199254740992}}`)); err != nil {
		t.Fatalf("write unsafe semantic frame: %v", err)
	}
	var unsafe rpcResponse
	if err := conn.ReadJSON(&unsafe); err != nil {
		t.Fatalf("read unsafe-frame response: %v", err)
	}
	if unsafe.Error == nil || unsafe.Error.Code != codeInvalidRequest {
		t.Fatalf("unsafe-frame error = %#v, want invalid request", unsafe.Error)
	}

	if err := conn.WriteJSON(map[string]any{
		"jsonrpc": "2.0",
		"id":      "ws-ok",
		"method":  "rpc.unsubscribe",
		"params": map[string]any{
			"subscription_id": "sub-1",
		},
	}); err != nil {
		t.Fatalf("write unsubscribe: %v", err)
	}
	var ok rpcResponse
	if err := conn.ReadJSON(&ok); err != nil {
		t.Fatalf("read unsubscribe response: %v", err)
	}
	if ok.Error != nil {
		t.Fatalf("unsubscribe error = %#v, want success", ok.Error)
	}
	if result := asMap(t, ok.Result); result["ok"] != true {
		t.Fatalf("unsubscribe result = %#v, want ok true", ok.Result)
	}
}

func TestOperatorReadHandlersExposeHealthAndRunReadMethods(t *testing.T) {
	now := time.Unix(1700000000, 123456789).UTC()
	runID := "run-1"
	eventID := "event-1"
	fakeRuns := &fakeRunReadStore{
		headers: map[string]operatorread.RunHeader{
			runID: {
				RunID:       runID,
				Status:      "running",
				Origin:      mustEventRunOrigin(t, eventID, "scan.requested"),
				EntityCount: 2,
				EventCount:  1,
				StartedAt:   now.Add(-time.Hour),
			},
		},
		reports: map[string]operatorread.RunDebugReport{
			runID: {
				RunID:          runID,
				RunTableStatus: "running",
				RootEventID:    eventID,
				RootEventType:  "scan.requested",
				StartedAt:      now.Add(-time.Hour),
				LastEventAt:    now.Add(-time.Minute),
				EventCount:     1,
				EntityCount:    2,
				Deliveries:     []operatorread.RunDebugDeliveryCount{{SubscriberID: "worker", Status: "pending", Count: 1}},
			},
		},
	}
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: testOperatorHandlers(testOperatorCapabilities{
			ExecutionPosture: executionposture.MockOnly,
			Now:              func() time.Time { return now },
			Ready:            func() bool { return true },
			Database:         fakePinger{err: nil},
			Runs:             fakeRuns,
			Bundle: runtimecontracts.BundleIdentity{
				WorkflowName:    "review",
				WorkflowVersion: "1.2.3",
				BundleHash:      "bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			},
			RuntimeIdentity: RuntimeIdentityResult{
				RuntimeInstanceID:   "runtime-instance-1",
				StartedAt:           now.Format(time.RFC3339Nano),
				APIVersion:          "v1",
				SupportedTransports: []string{"tcp"},
				SourceArtifacts: []RuntimeSourceArtifactIdentity{{
					BundleHash: "bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				}},
			},
		}),
	})

	ping := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"ping","method":"health.ping","params":{}}`)
	if ping.Error != nil {
		t.Fatalf("health.ping error = %#v", ping.Error)
	}
	if got := asMap(t, ping.Result)["ts"]; got != now.Format(time.RFC3339Nano) {
		t.Fatalf("health.ping ts = %v, want %s", got, now.Format(time.RFC3339Nano))
	}

	health := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"health","method":"health.check","params":{}}`)
	if health.Error != nil {
		t.Fatalf("health.check error = %#v", health.Error)
	}
	healthResult := asMap(t, health.Result)
	if healthResult["ready"] != true || healthResult["db_ok"] != true || healthResult["runtime_ok"] != true {
		t.Fatalf("health.check result = %#v", healthResult)
	}
	if healthResult["execution_posture"] != "mock_only" {
		t.Fatalf("health.check execution_posture = %#v, want mock_only", healthResult["execution_posture"])
	}
	bundle := asMap(t, healthResult["bundle"])
	if bundle["bundle_hash"] != "bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
		t.Fatalf("bundle identity = %#v", bundle)
	}
	if raw, _ := json.Marshal(healthResult); strings.Contains(string(raw), "/") {
		t.Fatalf("health.check leaked path-like content: %s", raw)
	}

	identity := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"identity","method":"runtime.identity","params":{}}`)
	if identity.Error != nil {
		t.Fatalf("runtime.identity error = %#v", identity.Error)
	}
	identityResult := asMap(t, identity.Result)
	if identityResult["runtime_instance_id"] != "runtime-instance-1" || identityResult["api_version"] != "v1" {
		t.Fatalf("runtime.identity result = %#v", identityResult)
	}
	if identityResult["runtime_instance_id"] == bundle["bundle_hash"] {
		t.Fatalf("runtime.identity reused bundle hash: %#v", identityResult)
	}
	if sources, ok := identityResult["source_artifacts"].([]any); !ok || len(sources) != 1 {
		t.Fatalf("runtime.identity source_artifacts = %#v, want one exact source fact", identityResult["source_artifacts"])
	}

	get := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"get","method":"run.get","params":{"run_id":"run-1"}}`)
	if get.Error != nil {
		t.Fatalf("run.get error = %#v", get.Error)
	}
	origin := asMap(t, asMap(t, asMap(t, get.Result)["run"])["origin"])
	if origin["kind"] != string(runtimerunlifecycle.OriginEvent) || origin["event_id"] != eventID {
		t.Fatalf("run.get origin = %#v, want event %s", origin, eventID)
	}

	bundleHash := "bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	list := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"list","method":"run.list","params":{"bundle_hash":"`+bundleHash+`","limit":1}}`)
	if list.Error != nil {
		t.Fatalf("run.list error = %#v", list.Error)
	}
	runs, ok := asMap(t, list.Result)["runs"].([]any)
	if !ok || len(runs) != 1 {
		t.Fatalf("run.list runs = %#v, want one run", asMap(t, list.Result)["runs"])
	}
	if fakeRuns.lastListOpts.Limit != 1 {
		t.Fatalf("run.list limit = %d, want 1", fakeRuns.lastListOpts.Limit)
	}
	if fakeRuns.lastListOpts.BundleHash != bundleHash {
		t.Fatalf("run.list bundle_hash = %q, want %q", fakeRuns.lastListOpts.BundleHash, bundleHash)
	}

	diagnose := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"diagnose","method":"run.diagnose","params":{"run_id":"run-1"}}`)
	if diagnose.Error != nil {
		t.Fatalf("run.diagnose error = %#v", diagnose.Error)
	}
	if got := asMap(t, diagnose.Result)["operational_state"]; got != "running" {
		t.Fatalf("run.diagnose operational_state = %v, want running", got)
	}
	fanOut := asMap(t, asMap(t, diagnose.Result)["fan_out"])
	if blocked, ok := fanOut["blocked_intents"].([]any); !ok || len(blocked) != 0 {
		t.Fatalf("run.diagnose fan_out.blocked_intents = %#v, want empty array", fanOut["blocked_intents"])
	}
	quiescence := asMap(t, asMap(t, diagnose.Result)["test_quiescence"])
	if quiescence["ready"] != true || quiescence["active_deliveries"] != float64(0) {
		t.Fatalf("run.diagnose test_quiescence = %#v, want ready zero-count projection", quiescence)
	}
}

func TestOperatorReadHandlersRunNotFoundAndRunStartStaysUnavailable(t *testing.T) {
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: testOperatorHandlers(testOperatorCapabilities{
			Ready:    func() bool { return true },
			Database: fakePinger{err: nil},
			Runs: &fakeRunReadStore{
				notFound: map[string]bool{"missing": true},
			},
			Bundle: runtimecontracts.BundleIdentity{
				WorkflowName:    "review",
				WorkflowVersion: "1.2.3",
				BundleHash:      "bundle-v2:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			},
		}),
	})

	missing := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"missing","method":"run.get","params":{"run_id":"missing"}}`)
	if missing.Error == nil {
		t.Fatal("run.get missing error = nil, want RUN_NOT_FOUND")
	}
	data := asMap(t, missing.Error.Data)
	if data["code"] != RunNotFoundCode || asMap(t, data["details"])["run_id"] != "missing" {
		t.Fatalf("run.get missing data = %#v", data)
	}

	start := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"start","method":"run.start","params":{"event_name":"system.started","payload":{}}}`)
	if start.Error == nil {
		t.Fatal("run.start error = nil, want METHOD_UNAVAILABLE")
	}
	if data := asMap(t, start.Error.Data); data["code"] != MethodUnavailableCode {
		t.Fatalf("run.start error data = %#v, want METHOD_UNAVAILABLE", data)
	}
}

func TestOperatorReadHandlersRunListRejectsInvalidFilters(t *testing.T) {
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: testOperatorHandlers(testOperatorCapabilities{
			Ready:    func() bool { return true },
			Database: fakePinger{err: nil},
			Runs: &fakeRunReadStore{
				headers: map[string]operatorread.RunHeader{},
			},
			Bundle: runtimecontracts.BundleIdentity{
				WorkflowName:    "review",
				WorkflowVersion: "1.2.3",
				BundleHash:      "bundle-v2:sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
			},
		}),
	})

	tests := []struct {
		name string
		body string
	}{
		{
			name: "unknown status",
			body: `{"jsonrpc":"2.0","id":"bad-status","method":"run.list","params":{"status":"runnning"}}`,
		},
		{
			name: "numeric since",
			body: `{"jsonrpc":"2.0","id":"bad-since","method":"run.list","params":{"since":123}}`,
		},
		{
			name: "numeric until",
			body: `{"jsonrpc":"2.0","id":"bad-until","method":"run.list","params":{"until":123}}`,
		},
		{
			name: "invalid bundle hash",
			body: `{"jsonrpc":"2.0","id":"bad-bundle","method":"run.list","params":{"bundle_hash":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp := rpcCall(t, handler, tc.body)
			if resp.Error == nil || resp.Error.Code != codeInvalidParams {
				t.Fatalf("run.list error = %#v, want invalid params", resp.Error)
			}
		})
	}
}

func rpcCall(t *testing.T, handler *Handler, body string) rpcResponse {
	t.Helper()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/rpc", strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+testToken)
	handler.ServeHTTP(recorder, testAuthorActivityRequest(request))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", recorder.Code, recorder.Body.String())
	}
	var response rpcResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode rpc response: %v body=%s", err, recorder.Body.String())
	}
	return response
}

func captureProcessLog(t *testing.T, fn func()) string {
	t.Helper()
	var buffer bytes.Buffer
	previousWriter := log.Writer()
	previousFlags := log.Flags()
	log.SetOutput(&buffer)
	log.SetFlags(0)
	defer log.SetOutput(previousWriter)
	defer log.SetFlags(previousFlags)
	fn()
	return buffer.String()
}

type fakePinger struct {
	err error
}

func (p fakePinger) Ping(context.Context) error {
	return p.err
}

type fakeRunReadStore struct {
	headers      map[string]operatorread.RunHeader
	reports      map[string]operatorread.RunDebugReport
	notFound     map[string]bool
	lastListOpts operatorread.RunHeaderListOptions
}

func (s *fakeRunReadStore) LoadRunHeader(_ context.Context, runID string) (operatorread.RunHeader, error) {
	if s.notFound[runID] {
		return operatorread.RunHeader{}, operatorread.ErrRunNotFound
	}
	header, ok := s.headers[runID]
	if !ok {
		return operatorread.RunHeader{}, operatorread.ErrRunNotFound
	}
	return header, nil
}

func (s *fakeRunReadStore) ListRunHeaders(_ context.Context, opts operatorread.RunHeaderListOptions) ([]operatorread.RunHeader, string, error) {
	s.lastListOpts = opts
	out := make([]operatorread.RunHeader, 0, len(s.headers))
	for _, header := range s.headers {
		out = append(out, header)
	}
	if opts.Limit > 0 && len(out) > opts.Limit {
		return out[:opts.Limit], "next", nil
	}
	return out, "", nil
}

func (s *fakeRunReadStore) LoadRunDebugReport(_ context.Context, runID string, _ operatorread.RunDebugQueryOptions) (operatorread.RunDebugReport, error) {
	if s.notFound[runID] {
		return operatorread.RunDebugReport{}, operatorread.ErrRunNotFound
	}
	report, ok := s.reports[runID]
	if !ok {
		return operatorread.RunDebugReport{}, operatorread.ErrRunNotFound
	}
	return report, nil
}

func testHandler(t *testing.T, opts Options) *Handler {
	t.Helper()
	opts.Registry = testRegistry(t)
	if opts.ProcessWorkOwner == nil {
		opts.ProcessWorkOwner = worklifetime.NewProcess()
		process := opts.ProcessWorkOwner
		t.Cleanup(func() {
			process.Retire()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, err := process.Join(ctx); err != nil {
				t.Errorf("join API test process work: %v", err)
			}
		})
	}
	handler, err := NewHandler(opts)
	if err != nil {
		t.Fatalf("NewHandler() error = %v", err)
	}
	return handler
}

func testRegistry(t *testing.T) *Registry {
	t.Helper()
	registry, err := LoadRegistry(compliancePlatformSpecPath(repoRoot(t)))
	if err != nil {
		t.Fatalf("LoadRegistry() error = %v", err)
	}
	return registry
}

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("repo root with go.mod not found")
		}
		dir = parent
	}
}

func asMap(t *testing.T, value any) map[string]any {
	t.Helper()
	out, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("value = %#v, want map[string]any", value)
	}
	return out
}
