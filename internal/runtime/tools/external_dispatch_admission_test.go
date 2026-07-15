package tools

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/failures"
	runtimemcp "github.com/division-sh/swarm/internal/runtime/mcp"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestParseExternalDispatchRateLimitGrammar(t *testing.T) {
	tests := []struct {
		name    string
		limit   string
		wait    string
		wantErr string
	}{
		{name: "per second", limit: "5/s", wait: "2s"},
		{name: "explicit seconds", limit: "5/1s", wait: "100ms"},
		{name: "minutes", limit: "300/15m", wait: "1m"},
		{name: "days", limit: "1000/1d", wait: "0s"},
		{name: "missing wait", limit: "1/s", wantErr: "rate_limit requires rate_limit_max_wait"},
		{name: "missing limit", wait: "1s", wantErr: "rate_limit_max_wait requires rate_limit"},
		{name: "zero count", limit: "0/s", wait: "1s", wantErr: "count must be a positive integer"},
		{name: "unknown unit", limit: "1/week", wait: "1s", wantErr: "duration unit"},
		{name: "whitespace rejected", limit: "1 /s", wait: "1s", wantErr: "must not contain whitespace"},
		{name: "negative wait rejected", limit: "1/s", wait: "-1s", wantErr: "duration must be positive"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := parseExternalDispatchRateLimit(tc.limit, tc.wait)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("parseExternalDispatchRateLimit(%q,%q): %v", tc.limit, tc.wait, err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("parseExternalDispatchRateLimit(%q,%q) err = %v, want %q", tc.limit, tc.wait, err, tc.wantErr)
			}
		})
	}
}

func TestValidateExternalDispatchRateLimitDeclarations(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"bad_builtin": {
				HandlerType:      "platform_builtin",
				RateLimit:        "1/s",
				RateLimitMaxWait: "1s",
			},
			"bad_http": {
				HandlerType: "http",
				HTTP:        &runtimecontracts.HTTPToolSpec{Method: "GET", URL: "https://example.test"},
				RateLimit:   "1/s",
			},
		},
		Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
			"mcp_servers": {Value: map[string]any{
				"infra": map[string]any{
					"transport":           "stdio",
					"command":             "infra",
					"rate_limit":          "1 /s",
					"rate_limit_max_wait": "1s",
				},
			}},
			"web_search_provider": {Value: map[string]any{
				"provider":            "custom",
				"rate_limit":          "1/s",
				"rate_limit_max_wait": 250,
			}},
		}},
	})

	errs := ValidateExternalDispatchRateLimitDeclarations(source)
	joined := externalDispatchJoinedErrors(errs)
	for _, want := range []string{
		"tool bad_builtin: rate_limit is only supported for handler_type http",
		"tool bad_http: rate_limit requires rate_limit_max_wait",
		"root policy.mcp_servers.infra: rate_limit: must not contain whitespace",
		"root policy.web_search_provider: rate_limit_max_wait must be a string",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("validation errors:\n%s\nmissing %q", joined, want)
		}
	}
}

func TestExecutor_HTTPToolRateLimitAdmitsDirectCallsAndLogsWait(t *testing.T) {
	var recorder dispatchTimeRecorder
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		recorder.record()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
	}))
	defer server.Close()

	bus := &telemetryBusStub{}
	exec := NewExecutorWithOptions(bus, nil, ExecutorOptions{
		WorkflowSource: rateLimitedHTTPToolSource(server.URL, "1/40ms", "500ms"),
	})
	ctx := models.WithActor(unmanagedToolTestContext(), models.AgentConfig{ExecutionMode: "live", ID: "agent-1", Tools: []string{"check_domain"}})
	for i := 0; i < 2; i++ {
		if _, err := exec.Execute(ctx, "check_domain", map[string]any{}); err != nil {
			t.Fatalf("Execute #%d: %v", i+1, err)
		}
	}
	recorder.requireGapAtLeast(t, 25*time.Millisecond)
	if len(bus.logs) != 2 {
		t.Fatalf("runtime logs = %d, want 2", len(bus.logs))
	}
	detail, _ := bus.logs[1].Detail.(map[string]any)
	if got := detail["rate_limit_scope"]; got != externalDispatchScopeHTTPTool {
		t.Fatalf("rate_limit_scope = %#v, want %q", got, externalDispatchScopeHTTPTool)
	}
	if got := detail["rate_limit_outcome"]; got != externalDispatchOutcomeWaited {
		t.Fatalf("rate_limit_outcome = %#v, want %q", got, externalDispatchOutcomeWaited)
	}
	if wait := int64FromAny(detail["rate_limit_wait_ms"]); wait <= 0 {
		t.Fatalf("rate_limit_wait_ms = %#v, want positive", detail["rate_limit_wait_ms"])
	}
}

func TestExecutor_HTTPTimeoutStartsAfterAdmissionWait(t *testing.T) {
	var recorder dispatchTimeRecorder
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		recorder.record()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
	}))
	defer server.Close()

	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{})
	tool := RegisteredTool{
		Name: "check_domain",
		HTTP: &runtimecontracts.HTTPToolSpec{Method: "GET", URL: server.URL},
		RateLimit: externalDispatchRateLimitConfig{
			Enabled: true,
			Limit:   1,
			Period:  60 * time.Millisecond,
			MaxWait: 500 * time.Millisecond,
		},
	}
	if _, err := exec.execHTTPRequestOnce(unmanagedToolTestContext(), http.MethodGet, server.URL, http.Header{}, nil, 20*time.Millisecond, tool, nil); err != nil {
		t.Fatalf("initial execHTTPRequestOnce: %v", err)
	}
	if _, err := exec.execHTTPRequestOnce(unmanagedToolTestContext(), http.MethodGet, server.URL, http.Header{}, nil, 20*time.Millisecond, tool, nil); err != nil {
		t.Fatalf("second execHTTPRequestOnce after admission wait: %v", err)
	}
	recorder.requireGapAtLeast(t, 45*time.Millisecond)
}

func TestExecutor_HTTPRateLimitTimeoutDoesNotRetry(t *testing.T) {
	var recorder dispatchTimeRecorder
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		recorder.record()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
	}))
	defer server.Close()

	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{
		WorkflowSource: rateLimitedHTTPToolSource(server.URL, "1/s", "0s"),
	})
	ctx := models.WithActor(unmanagedToolTestContext(), models.AgentConfig{ExecutionMode: "live", ID: "agent-1", Tools: []string{"check_domain"}})
	if _, err := exec.Execute(ctx, "check_domain", map[string]any{}); err != nil {
		t.Fatalf("initial Execute: %v", err)
	}
	_, err := exec.Execute(ctx, "check_domain", map[string]any{})
	runtimeErr, ok := failures.As(err)
	if !ok || runtimeErr == nil || runtimeErr.Failure.Detail.Code != externalDispatchRateLimitedCode {
		t.Fatalf("second Execute err = %v, want rate_limited runtime error", err)
	}
	if got := len(recorder.timesSnapshot()); got != 1 {
		t.Fatalf("outbound requests = %d, want no retry dispatch after admission timeout", got)
	}
}

func TestExecutor_MCPServerRateLimitIsSharedAcrossTools(t *testing.T) {
	var recorder dispatchTimeRecorder
	server := newRateLimitedMCPTestServer(t, &recorder, []string{"ping", "pong"})
	defer server.Close()

	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{
		WorkflowSource: rateLimitedMCPSource(server.URL, "1/40ms", "500ms"),
	})
	ctx := models.WithActor(unmanagedToolTestContext(), models.AgentConfig{ExecutionMode: "live", ID: "agent-1", Tools: []string{"infra.ping", "infra.pong"}})
	if _, err := exec.Execute(ctx, "infra.ping", map[string]any{}); err != nil {
		t.Fatalf("Execute(infra.ping): %v", err)
	}
	if _, err := exec.Execute(ctx, "infra.pong", map[string]any{}); err != nil {
		t.Fatalf("Execute(infra.pong): %v", err)
	}
	recorder.requireGapAtLeast(t, 25*time.Millisecond)
}

func TestExecutor_MCPStdioRuntimeCallUsesRateLimit(t *testing.T) {
	t.Setenv("SWARM_TEST_MCP_STDIO_HELPER", "1")
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
			"mcp_servers": {Value: map[string]any{
				"infra": map[string]any{
					"transport":           "stdio",
					"command":             os.Args[0],
					"args":                []string{"-test.run=TestExternalDispatchMCPStdioHelperProcess", "--"},
					"prefix":              "infra",
					"rate_limit":          "1/s",
					"rate_limit_max_wait": "0s",
				},
			}},
		}},
	})

	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{WorkflowSource: source})
	ctx := models.WithActor(unmanagedToolTestContext(), models.AgentConfig{ExecutionMode: "live", ID: "agent-1", Tools: []string{"infra.ping"}})
	if _, err := exec.Execute(ctx, "infra.ping", map[string]any{}); err != nil {
		t.Fatalf("initial Execute(infra.ping): %v", err)
	}
	_, err := exec.Execute(ctx, "infra.ping", map[string]any{})
	runtimeErr, ok := failures.As(err)
	if !ok || runtimeErr == nil || runtimeErr.Failure.Detail.Code != externalDispatchRateLimitedCode {
		t.Fatalf("second Execute err = %v, want runtime rate_limited", err)
	}
}

func TestExternalDispatchMCPStdioHelperProcess(t *testing.T) {
	if os.Getenv("SWARM_TEST_MCP_STDIO_HELPER") != "1" {
		return
	}
	scanner := bufio.NewScanner(os.Stdin)
	encoder := json.NewEncoder(os.Stdout)
	for scanner.Scan() {
		var req map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &req); err != nil {
			continue
		}
		id := req["id"]
		method, _ := req["method"].(string)
		if id == nil {
			continue
		}
		switch method {
		case "initialize":
			_ = encoder.Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      id,
				"result": map[string]any{
					"protocolVersion": "2025-03-26",
					"capabilities":    map[string]any{"tools": map[string]any{}},
					"serverInfo":      map[string]any{"name": "infra", "version": "1.0.0"},
				},
			})
		case "tools/list":
			_ = encoder.Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      id,
				"result": map[string]any{
					"tools": []map[string]any{{
						"name":        "ping",
						"description": "ping",
						"inputSchema": map[string]any{"type": "object", "additionalProperties": false},
					}},
				},
			})
		case "tools/call":
			_ = encoder.Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      id,
				"result":  map[string]any{"content": []any{}, "structuredContent": map[string]any{"ok": true}},
			})
		default:
			_ = encoder.Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      id,
				"error":   map[string]any{"code": -32601, "message": "method not found"},
			})
		}
	}
	os.Exit(0)
}

func TestGatewayToolPathProjectsHTTPRateLimitTimeout(t *testing.T) {
	var recorder dispatchTimeRecorder
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		recorder.record()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
	}))
	defer server.Close()

	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{
		WorkflowSource: rateLimitedHTTPToolSource(server.URL, "1/s", "0s"),
	})
	actor := models.AgentConfig{ExecutionMode: "live", ID: "agent-1", Tools: []string{"check_domain"}}
	ctx := models.WithActor(unmanagedToolTestContext(), actor)
	if _, err := exec.Execute(ctx, "check_domain", map[string]any{}); err != nil {
		t.Fatalf("initial Execute: %v", err)
	}

	gateway := runtimemcp.NewGateway(exec, "gateway-token", runtimemcp.GatewayHooks{
		WithActor:          models.WithActor,
		ResolveTurnContext: fixedTurnContextResolver(actor),
	})
	body, _ := json.Marshal(runtimemcp.ToolGatewayRequest{Input: map[string]any{}})
	req := httptest.NewRequest(http.MethodPost, "/tools/check_domain", bytes.NewReader(body))
	authorizeRateLimitGatewayRequest(req, "ctx-rate-limit")
	rec := httptest.NewRecorder()
	gateway.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("gateway status = %d body=%s, want 400", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), externalDispatchRateLimitedCode) {
		t.Fatalf("gateway body = %s, want rate_limited", rec.Body.String())
	}
	if got := len(recorder.timesSnapshot()); got != 1 {
		t.Fatalf("outbound requests = %d, want second call blocked before dispatch", got)
	}
}

func TestGatewayMCPToolsCallProjectsRateLimitedRuntimeError(t *testing.T) {
	var recorder dispatchTimeRecorder
	server := newRateLimitedMCPTestServer(t, &recorder, []string{"ping"})
	defer server.Close()

	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{
		WorkflowSource: rateLimitedMCPSource(server.URL, "1/s", "0s"),
	})
	actor := models.AgentConfig{ExecutionMode: "live", ID: "agent-1", Tools: []string{"infra.ping"}}
	ctx := models.WithActor(unmanagedToolTestContext(), actor)
	if _, err := exec.Execute(ctx, "infra.ping", map[string]any{}); err != nil {
		t.Fatalf("initial Execute: %v", err)
	}

	gateway := runtimemcp.NewGateway(exec, "gateway-token", runtimemcp.GatewayHooks{
		WithActor:          models.WithActor,
		ResolveTurnContext: fixedTurnContextResolver(actor),
	})
	body, _ := json.Marshal(map[string]any{
		"id":     "req-1",
		"method": "tools/call",
		"params": map[string]any{
			"name":      "infra.ping",
			"arguments": map[string]any{},
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader(body))
	authorizeRateLimitGatewayRequest(req, "ctx-rate-limit")
	rec := httptest.NewRecorder()
	gateway.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("gateway status = %d body=%s, want 200", rec.Code, rec.Body.String())
	}
	var resp runtimemcp.RPCResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	result, ok := resp.Result.(map[string]any)
	if !ok {
		t.Fatalf("result = %#v, want map", resp.Result)
	}
	runtimeErr, err := runtimemcp.DecodeRuntimeErrorPayload(result["runtimeError"])
	if err != nil {
		t.Fatalf("DecodeRuntimeErrorPayload: %v", err)
	}
	if runtimeErr.Failure == nil || runtimeErr.Failure.Detail.Code != externalDispatchRateLimitedCode {
		t.Fatalf("runtimeError.failure = %#v, want detail %q", runtimeErr.Failure, externalDispatchRateLimitedCode)
	}
	if !runtimeErr.Failure.Retryable {
		t.Fatalf("runtimeError.retryable = false, want true")
	}
	if got := len(recorder.timesSnapshot()); got != 1 {
		t.Fatalf("outbound mcp tools/call count = %d, want second call blocked before dispatch", got)
	}
}

func TestExecutor_NativeWebSearchHardcodedProviderUsesRateLimit(t *testing.T) {
	var recorder dispatchTimeRecorder
	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{
		WorkflowSource: rateLimitedNativeWebSearchSource("brave", "1/40ms", "500ms", nil),
		Credentials:    nativeWebSearchCredentialStore{"brave_search_api_key": "secret"},
		ModelRuntime:   nativeCapabilityRuntimeStub{},
	})
	exec.httpClient.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		recorder.record()
		if !strings.Contains(req.URL.Host, "brave.com") {
			t.Fatalf("host = %s, want brave provider", req.URL.Host)
		}
		return jsonHTTPResponse(http.StatusOK, `{"web":{"results":[{"title":"T","url":"https://example.test","description":"S"}]}}`), nil
	})
	ctx := models.WithActor(unmanagedToolTestContext(), models.AgentConfig{
		ExecutionMode: "live",
		ID:            "agent-1",
		NativeTools:   models.NativeToolConfig{WebSearch: true},
	})
	for i := 0; i < 2; i++ {
		if _, err := exec.Execute(ctx, "web_search", map[string]any{"query": "swarm"}); err != nil {
			t.Fatalf("Execute web_search #%d: %v", i+1, err)
		}
	}
	recorder.requireGapAtLeast(t, 25*time.Millisecond)
}

func TestExecutor_NativeWebSearchCustomProviderUsesRateLimit(t *testing.T) {
	var recorder dispatchTimeRecorder
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		recorder.record()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"items": []map[string]any{{"title": "T", "link": "https://example.test", "summary": "S"}},
		})
	}))
	defer server.Close()

	customHTTP := &runtimecontracts.HTTPToolSpec{
		Method: "GET",
		URL:    server.URL + "?q={{input.query}}&count={{input.max_results}}",
	}
	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{
		WorkflowSource: rateLimitedNativeWebSearchSource("custom", "1/40ms", "500ms", customHTTP),
		ModelRuntime:   nativeCapabilityRuntimeStub{},
	})
	ctx := models.WithActor(unmanagedToolTestContext(), models.AgentConfig{
		ExecutionMode: "live",
		ID:            "agent-1",
		NativeTools:   models.NativeToolConfig{WebSearch: true},
	})
	for i := 0; i < 2; i++ {
		if _, err := exec.Execute(ctx, "web_search", map[string]any{"query": "swarm"}); err != nil {
			t.Fatalf("Execute custom web_search #%d: %v", i+1, err)
		}
	}
	recorder.requireGapAtLeast(t, 25*time.Millisecond)
}

func TestExecutor_NativeWebSearchInheritedProviderPolicySharesBucketAcrossFlows(t *testing.T) {
	var recorder dispatchTimeRecorder
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		recorder.record()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"items": []map[string]any{{"title": "T", "link": "https://example.test", "summary": "S"}},
		})
	}))
	defer server.Close()

	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{
		WorkflowSource: rateLimitedNativeWebSearchSiblingFlowSource(server.URL),
		ModelRuntime:   nativeCapabilityRuntimeStub{},
	})
	first := models.WithActor(unmanagedToolTestContext(), models.AgentConfig{
		ExecutionMode: "live",
		ID:            "alpha-agent",
		FlowPath:      "alpha/instance-1",
		NativeTools:   models.NativeToolConfig{WebSearch: true},
	})
	if _, err := exec.Execute(first, "web_search", map[string]any{"query": "alpha"}); err != nil {
		t.Fatalf("first sibling Execute(web_search): %v", err)
	}
	second := models.WithActor(unmanagedToolTestContext(), models.AgentConfig{
		ExecutionMode: "live",
		ID:            "beta-agent",
		FlowPath:      "beta/instance-1",
		NativeTools:   models.NativeToolConfig{WebSearch: true},
	})
	_, err := exec.Execute(second, "web_search", map[string]any{"query": "beta"})
	runtimeErr, ok := failures.As(err)
	if !ok || runtimeErr == nil || runtimeErr.Failure.Detail.Code != externalDispatchRateLimitedCode {
		t.Fatalf("second sibling Execute(web_search) err = %v, want rate_limited runtime error", err)
	}
	if got := len(recorder.timesSnapshot()); got != 1 {
		t.Fatalf("outbound web_search requests = %d, want shared inherited-policy bucket to block second dispatch", got)
	}
}

type dispatchTimeRecorder struct {
	mu    sync.Mutex
	times []time.Time
}

type nativeWebSearchCredentialStore map[string]string

func (s nativeWebSearchCredentialStore) Get(_ context.Context, key string) (string, bool, error) {
	value, ok := s[strings.TrimSpace(key)]
	return value, ok, nil
}

func (s nativeWebSearchCredentialStore) Set(_ context.Context, key, value string) error {
	s[strings.TrimSpace(key)] = value
	return nil
}

func (s nativeWebSearchCredentialStore) List(context.Context) ([]string, error) {
	out := make([]string, 0, len(s))
	for key := range s {
		out = append(out, key)
	}
	return out, nil
}

func (s nativeWebSearchCredentialStore) Delete(_ context.Context, key string) error {
	delete(s, strings.TrimSpace(key))
	return nil
}

func (r *dispatchTimeRecorder) record() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.times = append(r.times, time.Now())
}

func (r *dispatchTimeRecorder) timesSnapshot() []time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]time.Time(nil), r.times...)
}

func (r *dispatchTimeRecorder) requireGapAtLeast(t *testing.T, min time.Duration) {
	t.Helper()
	times := r.timesSnapshot()
	if len(times) < 2 {
		t.Fatalf("dispatch timestamps = %d, want at least 2", len(times))
	}
	gap := times[1].Sub(times[0])
	if gap < min {
		t.Fatalf("dispatch gap = %s, want at least %s", gap, min)
	}
}

func rateLimitedHTTPToolSource(serverURL, rateLimit, maxWait string) semanticview.Source {
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"check_domain": {
				Description:      "Check domain availability",
				HandlerType:      "http",
				RateLimit:        rateLimit,
				RateLimitMaxWait: maxWait,
				InputSchema: runtimecontracts.ToolInputSchema{
					Type:                 "object",
					AdditionalProperties: runtimecontracts.ToolAdditionalProperties{Allowed: boolPtr(false)},
				},
				HTTP: &runtimecontracts.HTTPToolSpec{
					Method: "GET",
					URL:    serverURL,
				},
			},
		},
	})
}

func rateLimitedMCPSource(serverURL, rateLimit, maxWait string) semanticview.Source {
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
			"mcp_servers": {Value: map[string]any{
				"infra": map[string]any{
					"transport":           "http",
					"url":                 serverURL,
					"prefix":              "infra",
					"rate_limit":          rateLimit,
					"rate_limit_max_wait": maxWait,
				},
			}},
		}},
	})
}

func rateLimitedNativeWebSearchSource(provider, rateLimit, maxWait string, customHTTP *runtimecontracts.HTTPToolSpec) semanticview.Source {
	root := map[string]any{
		"provider":            provider,
		"max_results_default": 2,
		"rate_limit":          rateLimit,
		"rate_limit_max_wait": maxWait,
		"response_path":       "items",
		"field_mapping":       map[string]any{"title": "title", "url": "link", "snippet": "summary"},
	}
	if provider != "custom" {
		root["credentials_key"] = "brave_search_api_key"
	}
	if customHTTP != nil {
		rawHTTP, _ := json.Marshal(customHTTP)
		var httpMap map[string]any
		_ = json.Unmarshal(rawHTTP, &httpMap)
		root["http"] = httpMap
	}
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
			"web_search_provider": {Value: root},
		}},
	})
}

func rateLimitedNativeWebSearchSiblingFlowSource(serverURL string) semanticview.Source {
	rootPolicy := runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
		"web_search_provider": {Value: map[string]any{
			"provider":            "custom",
			"max_results_default": 2,
			"rate_limit":          "1/s",
			"rate_limit_max_wait": "0s",
			"response_path":       "items",
			"field_mapping":       map[string]any{"title": "title", "url": "link", "snippet": "summary"},
			"http": map[string]any{
				"method": "GET",
				"url":    serverURL + "?q={{input.query}}&count={{input.max_results}}",
			},
		}},
	}}
	root := runtimecontracts.FlowContractView{
		Policy: rootPolicy,
		Children: []runtimecontracts.FlowContractView{
			{Paths: runtimecontracts.FlowContractPaths{ID: "alpha", Flow: "alpha"}, Path: "alpha"},
			{Paths: runtimecontracts.FlowContractPaths{ID: "beta", Flow: "beta"}, Path: "beta"},
		},
	}
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Policy: rootPolicy,
		FlowTree: runtimecontracts.FlowTree{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"alpha": &root.Children[0],
				"beta":  &root.Children[1],
			},
			ByPath: map[string]*runtimecontracts.FlowContractView{
				"alpha": &root.Children[0],
				"beta":  &root.Children[1],
			},
		},
	})
}

func newRateLimitedMCPTestServer(t *testing.T, recorder *dispatchTimeRecorder, toolNames []string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("Decode request: %v", err)
		}
		method, _ := req["method"].(string)
		switch method {
		case "initialize":
			writeMCPResult(t, w, req["id"], map[string]any{
				"protocolVersion": "2025-03-26",
				"capabilities":    map[string]any{"tools": map[string]any{}},
				"serverInfo":      map[string]any{"name": "infra", "version": "1.0.0"},
			})
		case "notifications/initialized":
			writeMCPResult(t, w, nil, map[string]any{})
		case "tools/list":
			tools := make([]map[string]any, 0, len(toolNames))
			for _, name := range toolNames {
				tools = append(tools, map[string]any{
					"name":        name,
					"description": name,
					"inputSchema": map[string]any{"type": "object", "additionalProperties": false},
				})
			}
			writeMCPResult(t, w, req["id"], map[string]any{"tools": tools})
		case "tools/call":
			recorder.record()
			writeMCPResult(t, w, req["id"], map[string]any{
				"content":           []any{},
				"structuredContent": map[string]any{"ok": true},
			})
		default:
			t.Fatalf("unexpected mcp method %q", method)
		}
	}))
}

func fixedTurnContextResolver(actor models.AgentConfig) func(string) (runtimemcp.TurnContext, bool) {
	return func(token string) (runtimemcp.TurnContext, bool) {
		if strings.TrimSpace(token) != "ctx-rate-limit" {
			return runtimemcp.TurnContext{}, false
		}
		return runtimemcp.TurnContext{
			Actor:          actor,
			DifferentOwner: runtimeeffects.OwnerBuildTestInfrastructure,
			CreatedAt:      time.Now().UTC(),
			ExpiresAt:      time.Now().UTC().Add(time.Hour),
		}, true
	}
}

func authorizeRateLimitGatewayRequest(req *http.Request, contextToken string) {
	req.Header.Set("Authorization", "Bearer gateway-token")
	req.Header.Set("X-SWARM-Context-Token", contextToken)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func jsonHTTPResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func externalDispatchJoinedErrors(errs []error) string {
	parts := make([]string, 0, len(errs))
	for _, err := range errs {
		if err != nil {
			parts = append(parts, err.Error())
		}
	}
	return strings.Join(parts, "\n")
}

func int64FromAny(value any) int64 {
	switch typed := value.(type) {
	case int64:
		return typed
	case int:
		return int64(typed)
	case float64:
		return int64(typed)
	default:
		return 0
	}
}

func boolPtr(v bool) *bool {
	return &v
}
