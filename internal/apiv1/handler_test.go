package apiv1

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
)

const testToken = "test-v1-token"

func TestRegistryMethodNamesMatchGeneratedOpenRPC(t *testing.T) {
	registry := testRegistry(t)
	openRPCNames, err := OpenRPCMethodNames(filepath.Join(repoRoot(t), "docs", "specs", "swarm-platform", "platform", "contracts", "openrpc.json"))
	if err != nil {
		t.Fatalf("OpenRPCMethodNames() error = %v", err)
	}
	if got := registry.MethodNames(); !reflect.DeepEqual(got, openRPCNames) {
		t.Fatalf("registry method names drifted from generated OpenRPC:\nregistry=%v\nopenrpc=%v", got, openRPCNames)
	}
	if len(openRPCNames) != 36 {
		t.Fatalf("method count = %d, want 36", len(openRPCNames))
	}
	if _, ok := registry.Method("rpc.unsubscribe"); !ok {
		t.Fatal("rpc.unsubscribe missing from generated registry")
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

func TestAuthTokensFromEnvironmentPrefersUnifiedTokenAndDeduplicatesLegacyFallbacks(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "api")
	t.Setenv("SWARM_BUILDER_AUTH_TOKEN", "legacy")
	t.Setenv("SWARM_OPERATOR_AUTH_TOKEN", "legacy")

	got := AuthTokensFromEnvironment()
	want := []string{"api", "legacy"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("AuthTokensFromEnvironment() = %v, want %v", got, want)
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
			handler.ServeHTTP(rec, req)

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
		{name: "method not found", body: `{"jsonrpc":"2.0","id":"missing","method":"missing.method","params":{}}`, wantCode: codeMethodNotFound},
		{name: "invalid params object", body: `{"jsonrpc":"2.0","id":"bad-params-object","method":"rpc.unsubscribe","params":["sub-1"]}`, wantCode: codeInvalidParams},
		{name: "invalid params required", body: `{"jsonrpc":"2.0","id":"bad-params-required","method":"rpc.unsubscribe","params":{}}`, wantCode: codeInvalidParams},
		{name: "invalid integer param", body: `{"jsonrpc":"2.0","id":"bad-integer","method":"run.list","params":{"limit":1.5}}`, wantCode: codeInvalidParams},
		{name: "known business method unavailable", body: `{"jsonrpc":"2.0","id":"known","method":"run.list","params":{}}`, wantApp: MethodUnavailableCode},
		{name: "internal error", body: `{"jsonrpc":"2.0","id":"internal","method":"health.ping","params":{}}`, wantCode: codeInternalError},
		{name: "unsubscribe success", body: `{"jsonrpc":"2.0","id":"ok","method":"rpc.unsubscribe","params":{"subscription_id":"sub-1"}}`, headerCID: "trace-123", wantOK: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/v1/rpc", strings.NewReader(tc.body))
			req.Header.Set("Authorization", "Bearer "+testToken)
			if tc.headerCID != "" {
				req.Header.Set("X-Correlation-ID", tc.headerCID)
			}
			handler.ServeHTTP(rec, req)

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

func testHandler(t *testing.T, opts Options) *Handler {
	t.Helper()
	opts.Registry = testRegistry(t)
	handler, err := NewHandler(opts)
	if err != nil {
		t.Fatalf("NewHandler() error = %v", err)
	}
	return handler
}

func testRegistry(t *testing.T) *Registry {
	t.Helper()
	registry, err := LoadRegistry(filepath.Join(repoRoot(t), "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml"))
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
