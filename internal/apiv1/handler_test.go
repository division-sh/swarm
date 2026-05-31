package apiv1

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/store"
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
	if len(openRPCNames) != 55 {
		t.Fatalf("method count = %d, want 55", len(openRPCNames))
	}
	if _, ok := registry.Method("run.fork"); !ok {
		t.Fatal("run.fork missing from generated registry")
	}
	if _, ok := registry.Method("bundle.register"); !ok {
		t.Fatal("bundle.register missing from generated registry")
	}
	if _, ok := registry.Method("rpc.unsubscribe"); !ok {
		t.Fatal("rpc.unsubscribe missing from generated registry")
	}
	if _, ok := registry.Method("runtime.nuke"); !ok {
		t.Fatal("runtime.nuke missing from generated registry")
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

func TestAuthTokensFromEnvironmentUsesOnlyUnifiedToken(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "api")
	t.Setenv("SWARM_BUILDER_AUTH_TOKEN", "legacy")
	t.Setenv("SWARM_OPERATOR_AUTH_TOKEN", "operator")

	got := AuthTokensFromEnvironment()
	want := []string{"api"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("AuthTokensFromEnvironment() = %v, want %v", got, want)
	}
}

func TestResolveAuthTokensFromEnvironmentDefaultsToLoopbackToken(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "")
	resolution := ResolveAuthTokensFromEnvironment()
	if !resolution.UsesDefaultLoopbackToken() {
		t.Fatalf("resolution = %#v, want default loopback token", resolution)
	}
	want := []string{DefaultLoopbackAPIToken}
	if !reflect.DeepEqual(resolution.Tokens, want) {
		t.Fatalf("tokens = %v, want %v", resolution.Tokens, want)
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

	handler := testHandler(t, Options{AuthTokens: AuthTokensFromEnvironment()})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/rpc", strings.NewReader(`{"jsonrpc":"2.0","id":"auth","method":"rpc.unsubscribe","params":{"subscription_id":"sub-1"}}`))
	req.Header.Set("Authorization", "Bearer legacy-builder")
	handler.ServeHTTP(rec, req)
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

func TestOperatorReadHandlersExposeHealthAndRunReadMethods(t *testing.T) {
	now := time.Unix(1700000000, 123456789).UTC()
	runID := "run-1"
	eventID := "event-1"
	fakeRuns := &fakeRunReadStore{
		headers: map[string]store.RunHeader{
			runID: {
				RunID:            runID,
				Status:           "running",
				TriggerEventType: "scan.requested",
				TriggerEventID:   eventID,
				EntityCount:      2,
				EventCount:       1,
				StartedAt:        now.Add(-time.Hour),
			},
		},
		reports: map[string]store.RunDebugReport{
			runID: {
				RunID:          runID,
				RunTableStatus: "running",
				RootEventID:    eventID,
				RootEventType:  "scan.requested",
				StartedAt:      now.Add(-time.Hour),
				LastEventAt:    now.Add(-time.Minute),
				EventCount:     1,
				EntityCount:    2,
				Deliveries:     []store.RunDebugDeliveryCount{{SubscriberID: "worker", Status: "pending", Count: 1}},
			},
		},
	}
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Now:      func() time.Time { return now },
			Ready:    func() bool { return true },
			Database: fakePinger{err: nil},
			Runs:     fakeRuns,
			Bundle: runtimecontracts.BundleIdentity{
				WorkflowName:    "review",
				WorkflowVersion: "1.2.3",
				Fingerprint:     "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
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
	bundle := asMap(t, healthResult["bundle"])
	if bundle["fingerprint"] != "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
		t.Fatalf("bundle identity = %#v", bundle)
	}
	if raw, _ := json.Marshal(healthResult); strings.Contains(string(raw), "/") {
		t.Fatalf("health.check leaked path-like content: %s", raw)
	}

	get := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"get","method":"run.get","params":{"run_id":"run-1"}}`)
	if get.Error != nil {
		t.Fatalf("run.get error = %#v", get.Error)
	}
	if got := asMap(t, asMap(t, get.Result)["run"])["trigger_event_id"]; got != eventID {
		t.Fatalf("run.get trigger_event_id = %v, want %s", got, eventID)
	}

	bundleHash := "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
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
}

func TestOperatorReadHandlersRunNotFoundAndRunStartStaysUnavailable(t *testing.T) {
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Ready:    func() bool { return true },
			Database: fakePinger{err: nil},
			Runs: &fakeRunReadStore{
				notFound: map[string]bool{"missing": true},
			},
			Bundle: runtimecontracts.BundleIdentity{
				WorkflowName:    "review",
				WorkflowVersion: "1.2.3",
				Fingerprint:     "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
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
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Ready:    func() bool { return true },
			Database: fakePinger{err: nil},
			Runs: &fakeRunReadStore{
				headers: map[string]store.RunHeader{},
			},
			Bundle: runtimecontracts.BundleIdentity{
				WorkflowName:    "review",
				WorkflowVersion: "1.2.3",
				Fingerprint:     "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
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

func TestOperatorBundleCatalogHandlersExposeStoreOwner(t *testing.T) {
	now := time.Unix(1700000100, 0).UTC()
	bundleHash := "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	catalog := &fakeBundleCatalogReadStore{
		listResult: store.BundleCatalogListResult{
			Bundles: []store.BundleCatalogSummary{{
				BundleHash:    bundleHash,
				AgentCount:    1,
				HasData:       true,
				DataSizeBytes: 4,
				Metadata:      map[string]any{"source": "test"},
				IngestedAt:    now,
			}},
			NextCursor: "cursor-2",
		},
		details: map[string]store.BundleCatalogDetail{
			bundleHash: {
				BundleHash:    bundleHash,
				ContentYAML:   "name: test",
				ParsedJSON:    map[string]any{"agents": map[string]any{}},
				Metadata:      map[string]any{"source": "test"},
				AgentCount:    1,
				HasData:       true,
				DataSizeBytes: 4,
				IngestedAt:    now,
			},
		},
		agents: map[string]store.BundleCatalogAgentsResult{
			bundleHash: {
				Agents: []store.BundleCatalogAgentDefinition{{
					AgentID:          "researcher",
					Role:             "research",
					Type:             "managed",
					ModelTier:        "haiku",
					LLMBackend:       "claude",
					ConversationMode: "session",
					SessionScope:     "flow",
					PromptPath:       "prompts/researcher.md",
					Subscriptions:    []string{"scan.requested"},
					Tools:            []string{"web_search"},
				}},
			},
		},
	}
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Ready:         func() bool { return true },
			Database:      fakePinger{err: nil},
			BundleCatalog: catalog,
		}),
	})

	list := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"list","method":"bundle.list","params":{"limit":1,"cursor":"cursor-1"}}`)
	if list.Error != nil {
		t.Fatalf("bundle.list error = %#v", list.Error)
	}
	if catalog.lastList.Limit != 1 || catalog.lastList.Cursor != "cursor-1" {
		t.Fatalf("bundle.list opts = %#v", catalog.lastList)
	}
	listResult := asMap(t, list.Result)
	if listResult["next_cursor"] != "cursor-2" {
		t.Fatalf("bundle.list next_cursor = %#v", listResult["next_cursor"])
	}
	bundles, ok := listResult["bundles"].([]any)
	if !ok || len(bundles) != 1 {
		t.Fatalf("bundle.list bundles = %#v", listResult["bundles"])
	}
	if asMap(t, bundles[0])["bundle_hash"] != bundleHash {
		t.Fatalf("bundle.list bundle row = %#v", bundles[0])
	}

	get := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"get","method":"bundle.get","params":{"bundle_hash":"`+bundleHash+`"}}`)
	if get.Error != nil {
		t.Fatalf("bundle.get error = %#v", get.Error)
	}
	if got := asMap(t, get.Result)["bundle_hash"]; got != bundleHash {
		t.Fatalf("bundle.get bundle_hash = %#v", got)
	}

	agents := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"agents","method":"bundle.agents","params":{"bundle_hash":"`+bundleHash+`"}}`)
	if agents.Error != nil {
		t.Fatalf("bundle.agents error = %#v", agents.Error)
	}
	agentRows := asMap(t, agents.Result)["agents"].([]any)
	agent := asMap(t, agentRows[0])
	if agent["agent_id"] != "researcher" || agent["model_tier"] != "haiku" {
		t.Fatalf("bundle.agents row = %#v", agent)
	}
	for _, runtimeKey := range []string{"status", "runtime_state", "queue", "active", "session_id"} {
		if _, ok := agent[runtimeKey]; ok {
			t.Fatalf("bundle.agents leaked runtime key %q: %#v", runtimeKey, agent)
		}
	}
}

func TestOperatorBundleCatalogHandlersErrors(t *testing.T) {
	bundleHash := "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			BundleCatalog: &fakeBundleCatalogReadStore{
				missing: map[string]bool{bundleHash: true},
				listErr: store.ErrInvalidBundleCatalogCursor,
			},
		}),
	})

	badHash := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"bad","method":"bundle.get","params":{"bundle_hash":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}`)
	if badHash.Error == nil || badHash.Error.Code != codeInvalidParams {
		t.Fatalf("bundle.get invalid hash error = %#v, want invalid params", badHash.Error)
	}

	missing := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"missing","method":"bundle.get","params":{"bundle_hash":"`+bundleHash+`"}}`)
	if missing.Error == nil {
		t.Fatal("bundle.get missing error = nil, want BUNDLE_NOT_FOUND")
	}
	if data := asMap(t, missing.Error.Data); data["code"] != BundleNotFoundCode {
		t.Fatalf("bundle.get missing error data = %#v", data)
	}

	badCursor := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"cursor","method":"bundle.list","params":{"cursor":"bad"}}`)
	if badCursor.Error == nil || badCursor.Error.Code != codeInvalidParams {
		t.Fatalf("bundle.list invalid cursor error = %#v, want invalid params", badCursor.Error)
	}
}

func TestOperatorBundleRegisterHandlersMaterializeCanonicalProjectionAndIdempotency(t *testing.T) {
	catalog := &fakeBundleCatalogReadStore{
		details: map[string]store.BundleCatalogDetail{},
		agents:  map[string]store.BundleCatalogAgentsResult{},
	}
	platformSpec := testBundleRegistrationPlatformSpec(t)
	platformHash, err := fileSHA256Hex(platformSpec)
	if err != nil {
		t.Fatalf("hash platform spec: %v", err)
	}
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Now:              func() time.Time { return time.Unix(1700000200, 0).UTC() },
			RepoRoot:         t.TempDir(),
			PlatformSpecPath: platformSpec,
			BundleCatalog:    catalog,
			Idempotency:      newRecordingAPIIdempotencyStore(),
		}),
	})
	envelope := testBundleRegistrationEnvelope()

	first := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"register","method":"bundle.register","params":{"content_yaml":%q,"idempotency_key":"idem-register"}}`, envelope))
	if first.Error != nil {
		t.Fatalf("bundle.register error = %#v", first.Error)
	}
	result := asMap(t, first.Result)
	bundleHash, ok := result["bundle_hash"].(string)
	if !ok || !bundleHashPattern.MatchString(bundleHash) {
		t.Fatalf("bundle.register bundle_hash = %#v", result["bundle_hash"])
	}
	if result["registered"] != true || result["has_data"] != false || result["data_size_bytes"] != float64(0) {
		t.Fatalf("bundle.register result = %#v", result)
	}
	if _, ok := result["idempotency_replayed"]; ok {
		t.Fatalf("bundle.register result must not expose idempotency_replayed: %#v", result)
	}
	if len(catalog.upserts) != 1 {
		t.Fatalf("upserts = %d, want 1", len(catalog.upserts))
	}
	upsert := catalog.upserts[0]
	if upsert.BundleHash != bundleHash || !strings.Contains(upsert.ContentYAML, "bundle/package.yaml") || !strings.Contains(upsert.ContentYAML, "platform/platform-spec.yaml") {
		t.Fatalf("bundle.register upsert = %#v", upsert)
	}
	if upsert.Metadata["registered_by"] != "bundle.register" || upsert.Metadata["platform_spec_hash"] != "sha256:"+platformHash || upsert.Metadata["platform_spec_source"] != "server_effective" {
		t.Fatalf("bundle.register metadata = %#v", upsert.Metadata)
	}
	agents := asMap(t, upsert.ParsedJSON["agents"])
	researcher := asMap(t, agents["researcher"])
	if researcher["model_tier"] != "tier2" || researcher["conversation_mode"] != "stateless" {
		t.Fatalf("projected researcher = %#v", researcher)
	}

	replay := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"replay","method":"bundle.register","params":{"content_yaml":%q,"idempotency_key":"idem-register"}}`, envelope))
	if replay.Error != nil {
		t.Fatalf("bundle.register replay error = %#v", replay.Error)
	}
	replayResult := asMap(t, replay.Result)
	if replayResult["bundle_hash"] != bundleHash || replayResult["registered"] != true || replayResult["has_data"] != false {
		t.Fatalf("bundle.register replay result = %#v", replayResult)
	}
	if len(catalog.upserts) != 1 {
		t.Fatalf("upserts after replay = %d, want 1", len(catalog.upserts))
	}

	duplicate := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"duplicate","method":"bundle.register","params":{"content_yaml":%q}}`, envelope))
	if duplicate.Error != nil {
		t.Fatalf("bundle.register duplicate error = %#v", duplicate.Error)
	}
	if duplicateResult := asMap(t, duplicate.Result); duplicateResult["registered"] != false || duplicateResult["has_data"] != false {
		t.Fatalf("bundle.register duplicate result = %#v", duplicateResult)
	}

	dataBlob := map[string]any{
		"api_version": "swarm.bundle.data.v1",
		"entries": []any{
			map[string]any{"path": "flows/alpha/data/payload.bin", "data_base64": base64.StdEncoding.EncodeToString([]byte{0x01, 0x02, 0x03})},
		},
	}
	dataBlobRaw, err := json.Marshal(dataBlob)
	if err != nil {
		t.Fatalf("marshal data_blob: %v", err)
	}
	withData := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"data","method":"bundle.register","params":{"content_yaml":%q,"data_blob":%s}}`, testBundleRegistrationEnvelopeWithFlowData(), dataBlobRaw))
	if withData.Error != nil {
		t.Fatalf("bundle.register with data error = %#v", withData.Error)
	}
	withDataResult := asMap(t, withData.Result)
	if withDataResult["registered"] != true || withDataResult["has_data"] != true {
		t.Fatalf("bundle.register with data result = %#v", withDataResult)
	}
	if got := withDataResult["data_size_bytes"].(float64); got <= 0 {
		t.Fatalf("bundle.register data_size_bytes = %v, want >0", got)
	}
}

func TestOperatorBundleRegisterHandlersFailClosed(t *testing.T) {
	catalog := &fakeBundleCatalogReadStore{
		details: map[string]store.BundleCatalogDetail{},
		agents:  map[string]store.BundleCatalogAgentsResult{},
	}
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			RepoRoot:         t.TempDir(),
			PlatformSpecPath: testBundleRegistrationPlatformSpec(t),
			BundleCatalog:    catalog,
			Idempotency:      newRecordingAPIIdempotencyStore(),
		}),
	})

	unconsumed := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"unconsumed","method":"bundle.register","params":{"content_yaml":%q,"data_blob":{"api_version":"swarm.bundle.data.v1","entries":[{"path":"flows/missing/data/unreferenced.bin","data_base64":"AQI="}]}}}`, testBundleRegistrationEnvelope()))
	if unconsumed.Error == nil || unconsumed.Error.Code != codeInvalidParams {
		t.Fatalf("bundle.register unconsumed error = %#v, want invalid params", unconsumed.Error)
	}
	if len(catalog.upserts) != 0 {
		t.Fatalf("upserts after invalid registration = %d, want 0", len(catalog.upserts))
	}

	catalog.conflict = true
	conflict := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"conflict","method":"bundle.register","params":{"content_yaml":%q}}`, testBundleRegistrationEnvelope()))
	if conflict.Error == nil {
		t.Fatal("bundle.register conflict error = nil")
	}
	if data := asMap(t, conflict.Error.Data); data["code"] != BundleRegisterConflictCode {
		t.Fatalf("bundle.register conflict data = %#v", data)
	}

	malformed := map[string]string{
		"legacy envelope version": `version: swarm.bundle.registration.v1
files:
  - path: package.yaml
    content: "name: legacy\nversion: \"1.0.0\"\nflows: []\n"
`,
		"dot segment": `api_version: swarm.bundle.register.v1
files:
  - path: ./package.yaml
    text: "name: bad\nversion: \"1.0.0\"\nflows: []\n"
`,
		"case collision": `api_version: swarm.bundle.register.v1
files:
  - path: package.yaml
    text: "name: bad\nversion: \"1.0.0\"\nflows: []\n"
  - path: Package.yaml
    text: "name: bad\n"
`,
		"non nfc path": "api_version: swarm.bundle.register.v1\nfiles:\n  - path: cafe\u0301.yaml\n    text: \"name: bad\\n\"\n",
	}
	for name, content := range malformed {
		t.Run(name, func(t *testing.T) {
			got := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"bad","method":"bundle.register","params":{"content_yaml":%q}}`, content))
			if got.Error == nil || got.Error.Code != codeInvalidParams {
				t.Fatalf("bundle.register malformed error = %#v, want invalid params", got.Error)
			}
		})
	}

	badData := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"bad-data","method":"bundle.register","params":{"content_yaml":%q,"data_blob":{"flows/alpha/data/payload.bin":"AQI="}}}`, testBundleRegistrationEnvelope()))
	if badData.Error == nil || badData.Error.Code != codeInvalidParams {
		t.Fatalf("bundle.register bad data_blob error = %#v, want invalid params", badData.Error)
	}

	unsortedData := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"unsorted-data","method":"bundle.register","params":{"content_yaml":%q,"data_blob":{"api_version":"swarm.bundle.data.v1","entries":[{"path":"flows/beta/data/payload.bin","data_base64":"AQI="},{"path":"flows/alpha/data/payload.bin","data_base64":"AQI="}]}}}`, testBundleRegistrationEnvelope()))
	if unsortedData.Error == nil || unsortedData.Error.Code != codeInvalidParams {
		t.Fatalf("bundle.register unsorted data_blob error = %#v, want invalid params", unsortedData.Error)
	}
}

func testBundleRegistrationEnvelope() string {
	return `api_version: swarm.bundle.register.v1
files:
  - path: package.yaml
    text: |
      name: registered
      version: "1.0.0"
      flows: []
  - path: agents.yaml
    text: |
      researcher:
        id: researcher
        role: research
        model_tier: tier2
        conversation_mode: stateless
        subscriptions:
          - scan.requested
`
}

func testBundleRegistrationEnvelopeWithFlowData() string {
	return `api_version: swarm.bundle.register.v1
files:
  - path: package.yaml
    text: |
      name: registered-data
      version: "1.0.0"
      flows:
        - id: alpha
          flow: alpha
  - path: flows/alpha/schema.yaml
    text: |
      initial_state: start
      states:
        - start
        - done
`
}

func testBundleRegistrationPlatformSpec(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(repoRoot(t), "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml"))
	if err != nil {
		t.Fatalf("read platform spec: %v", err)
	}
	path := filepath.Join(t.TempDir(), "platform-spec.yaml")
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatalf("write temp platform spec: %v", err)
	}
	return path
}

func rpcCall(t *testing.T, handler *Handler, body string) rpcResponse {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/rpc", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+testToken)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", rec.Code, rec.Body.String())
	}
	var resp rpcResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode rpc response: %v body=%s", err, rec.Body.String())
	}
	return resp
}

type fakePinger struct {
	err error
}

func (p fakePinger) Ping(context.Context) error {
	return p.err
}

type fakeRunReadStore struct {
	headers      map[string]store.RunHeader
	reports      map[string]store.RunDebugReport
	notFound     map[string]bool
	lastListOpts store.RunHeaderListOptions
}

func (s *fakeRunReadStore) LoadRunHeader(_ context.Context, runID string) (store.RunHeader, error) {
	if s.notFound[runID] {
		return store.RunHeader{}, store.ErrRunNotFound
	}
	header, ok := s.headers[runID]
	if !ok {
		return store.RunHeader{}, store.ErrRunNotFound
	}
	return header, nil
}

func (s *fakeRunReadStore) ListRunHeaders(_ context.Context, opts store.RunHeaderListOptions) ([]store.RunHeader, string, error) {
	s.lastListOpts = opts
	out := make([]store.RunHeader, 0, len(s.headers))
	for _, header := range s.headers {
		out = append(out, header)
	}
	if opts.Limit > 0 && len(out) > opts.Limit {
		return out[:opts.Limit], "next", nil
	}
	return out, "", nil
}

func (s *fakeRunReadStore) LoadRunDebugReport(_ context.Context, runID string, _ store.RunDebugQueryOptions) (store.RunDebugReport, error) {
	if s.notFound[runID] {
		return store.RunDebugReport{}, store.ErrRunNotFound
	}
	report, ok := s.reports[runID]
	if !ok {
		return store.RunDebugReport{}, store.ErrRunNotFound
	}
	return report, nil
}

type fakeBundleCatalogReadStore struct {
	listResult store.BundleCatalogListResult
	listErr    error
	lastList   store.BundleCatalogListOptions
	details    map[string]store.BundleCatalogDetail
	agents     map[string]store.BundleCatalogAgentsResult
	missing    map[string]bool
	upserts    []store.BundleCatalogUpsert
	conflict   bool
}

func (s *fakeBundleCatalogReadStore) ListBundleCatalog(_ context.Context, opts store.BundleCatalogListOptions) (store.BundleCatalogListResult, error) {
	s.lastList = opts
	if s.listErr != nil {
		return store.BundleCatalogListResult{}, s.listErr
	}
	return s.listResult, nil
}

func (s *fakeBundleCatalogReadStore) LoadBundleCatalog(_ context.Context, bundleHash string) (store.BundleCatalogDetail, error) {
	if s.missing[bundleHash] {
		return store.BundleCatalogDetail{}, store.ErrBundleNotFound
	}
	detail, ok := s.details[bundleHash]
	if !ok {
		return store.BundleCatalogDetail{}, store.ErrBundleNotFound
	}
	return detail, nil
}

func (s *fakeBundleCatalogReadStore) ListBundleCatalogAgents(_ context.Context, bundleHash string) (store.BundleCatalogAgentsResult, error) {
	if s.missing[bundleHash] {
		return store.BundleCatalogAgentsResult{}, store.ErrBundleNotFound
	}
	result, ok := s.agents[bundleHash]
	if !ok {
		return store.BundleCatalogAgentsResult{}, store.ErrBundleNotFound
	}
	return result, nil
}

func (s *fakeBundleCatalogReadStore) UpsertBundleCatalog(_ context.Context, req store.BundleCatalogUpsert) (store.BundleCatalogUpsertResult, error) {
	if s.conflict {
		return store.BundleCatalogUpsertResult{}, &store.BundleCatalogConflictError{BundleHash: req.BundleHash}
	}
	if s.details == nil {
		s.details = map[string]store.BundleCatalogDetail{}
	}
	if s.agents == nil {
		s.agents = map[string]store.BundleCatalogAgentsResult{}
	}
	_, exists := s.details[req.BundleHash]
	s.upserts = append(s.upserts, req)
	detail := store.BundleCatalogDetail{
		BundleHash:    req.BundleHash,
		ContentYAML:   req.ContentYAML,
		ParsedJSON:    req.ParsedJSON,
		Metadata:      req.Metadata,
		AgentCount:    1,
		HasData:       len(req.DataBlob) > 0,
		DataSizeBytes: int64(len(req.DataBlob)),
		IngestedAt:    time.Unix(1700000200, 0).UTC(),
	}
	if exists {
		detail = s.details[req.BundleHash]
	}
	s.details[req.BundleHash] = detail
	return store.BundleCatalogUpsertResult{Detail: detail, Registered: !exists}, nil
}

var _ BundleCatalogReadStore = (*fakeBundleCatalogReadStore)(nil)
var _ BundleCatalogRegisterStore = (*fakeBundleCatalogReadStore)(nil)

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
