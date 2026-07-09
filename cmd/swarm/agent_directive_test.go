package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
)

func TestAgentDirectiveUsesV1RPCWithRunIDAndIdempotencyKey(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var captured jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/rpc" {
			t.Errorf("path = %q, want /v1/rpc", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Errorf("Authorization = %q, want bearer token", got)
		}
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Errorf("decode request: %v", err)
		}
		writeJSONRPCResult(t, w, captured.ID, agentDirectiveTestResult("specified"))
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{
		"agent", "directive", "agent-1", "rerun corpus",
		"--run-id", "run-1",
		"--idempotency-key", "idem-1",
	}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if captured.JSONRPC != "2.0" || captured.Method != agentDirectiveMethod {
		t.Fatalf("request jsonrpc/method = %s/%s, want 2.0/%s", captured.JSONRPC, captured.Method, agentDirectiveMethod)
	}
	wantParams := map[string]any{
		"agent_id":        "agent-1",
		"directive":       "rerun corpus",
		"run_id":          "run-1",
		"idempotency_key": "idem-1",
	}
	if !reflect.DeepEqual(captured.Params, wantParams) {
		t.Fatalf("params = %#v, want %#v", captured.Params, wantParams)
	}
	for _, want := range []string{
		"agent directive ok:",
		"agent_id=agent-1",
		"run_id=run-1",
		"run_id_resolution=specified",
		"directive_event_id=event-directive-1",
		"directive_event_type=platform.agent_directive",
		"response=accepted",
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestAgentDirectiveOmitsOptionalParamsWhenNotProvided(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var captured jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Errorf("decode request: %v", err)
		}
		writeJSONRPCResult(t, w, captured.ID, agentDirectiveTestResult("new_run_allocated"))
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"agent", "directive", "agent-1", "start work"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	wantParams := map[string]any{"agent_id": "agent-1", "directive": "start work"}
	if !reflect.DeepEqual(captured.Params, wantParams) {
		t.Fatalf("params = %#v, want %#v", captured.Params, wantParams)
	}
	if !strings.Contains(stdout.String(), "run_id_resolution=new_run_allocated") {
		t.Fatalf("stdout = %q, want new run resolution", stdout.String())
	}
}

func TestAgentDirectivePreservesDirectiveWhitespace(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var captured jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Errorf("decode request: %v", err)
		}
		writeJSONRPCResult(t, w, captured.ID, agentDirectiveTestResult("specified"))
	}))
	defer server.Close()

	directive := "  keep this spacing\n  "
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"agent", "directive", "agent-1", directive}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if got := captured.Params["directive"]; got != directive {
		t.Fatalf("directive param = %#v, want exact original %#v", got, directive)
	}
}

func TestAgentDirectiveRejectsInvalidInputBeforeRequest(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeJSONRPCResult(t, w, "unexpected", agentDirectiveTestResult("specified"))
	}))
	defer server.Close()

	for _, tc := range []struct {
		name       string
		args       []string
		wantStderr string
	}{
		{name: "missing args", args: []string{"agent", "directive"}, wantStderr: "requires <agent-id>"},
		{name: "blank agent", args: []string{"agent", "directive", "  ", "run it"}, wantStderr: "agent id is required"},
		{name: "blank directive", args: []string{"agent", "directive", "agent-1", "  "}, wantStderr: "directive is required"},
		{name: "extra arg", args: []string{"agent", "directive", "agent-1", "run it", "extra"}, wantStderr: "accepts two arguments"},
		{name: "unsupported flag", args: []string{"agent", "directive", "agent-1", "run it", "--unknown"}, wantStderr: "unknown flag"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls.Store(0)
			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), tc.args, &stdout, &stderr, testRootCommandOptions(server))
			if code != 2 {
				t.Fatalf("code = %d, want 2 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.wantStderr) {
				t.Fatalf("stderr = %q, want substring %q", stderr.String(), tc.wantStderr)
			}
			if calls.Load() != 0 {
				t.Fatalf("RPC calls = %d, want 0", calls.Load())
			}
		})
	}
}

func TestAgentDirectiveFailClosedWithoutToken(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeJSONRPCResult(t, w, "unexpected", agentDirectiveTestResult("specified"))
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"agent", "directive", "agent-1", "run it"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 4 {
		t.Fatalf("code = %d, want 4 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), "API token source is required") {
		t.Fatalf("stderr = %q, want token failure", stderr.String())
	}
	if calls.Load() != 0 {
		t.Fatalf("RPC calls = %d, want 0", calls.Load())
	}
}

func TestAgentDirectiveMapsFailureExitCodes(t *testing.T) {
	for _, tc := range []struct {
		name       string
		handler    http.HandlerFunc
		wantCode   int
		wantStderr string
	}{
		{
			name: "http auth failure exits four",
			handler: func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
			},
			wantCode:   4,
			wantStderr: "rejected the request with status 401",
		},
		{
			name: "http runtime failure exits three",
			handler: func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "unavailable", http.StatusServiceUnavailable)
			},
			wantCode:   3,
			wantStderr: "returned status 503",
		},
		{
			name: "agent not found exits five",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeAgentDirectiveJSONRPCError(t, w, req.ID, "AGENT_NOT_FOUND")
			},
			wantCode:   5,
			wantStderr: "AGENT_NOT_FOUND",
		},
		{
			name: "run not found exits five",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeAgentDirectiveJSONRPCError(t, w, req.ID, "RUN_NOT_FOUND")
			},
			wantCode:   5,
			wantStderr: "RUN_NOT_FOUND",
		},
		{
			name: "agent not running exits six",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeAgentDirectiveJSONRPCError(t, w, req.ID, "AGENT_NOT_RUNNING")
			},
			wantCode:   6,
			wantStderr: "AGENT_NOT_RUNNING",
		},
		{
			name: "terminal run exits six",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeAgentDirectiveJSONRPCError(t, w, req.ID, "RUN_ALREADY_TERMINAL")
			},
			wantCode:   6,
			wantStderr: "RUN_ALREADY_TERMINAL",
		},
		{
			name: "ambiguous target exits six",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeAgentDirectiveJSONRPCError(t, w, req.ID, "AMBIGUOUS_RUN_TARGET")
			},
			wantCode:   6,
			wantStderr: "AMBIGUOUS_RUN_TARGET",
		},
		{
			name: "idempotency conflict exits six",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeAgentDirectiveJSONRPCError(t, w, req.ID, "IDEMPOTENCY_CONFLICT")
			},
			wantCode:   6,
			wantStderr: "IDEMPOTENCY_CONFLICT",
		},
		{
			name: "malformed result exits three",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				result := agentDirectiveTestResult("specified")
				delete(result, "directive_event_id")
				writeJSONRPCResult(t, w, req.ID, result)
			},
			wantCode:   3,
			wantStderr: "directive_event_id is required",
		},
		{
			name: "invalid resolution exits three",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeJSONRPCResult(t, w, req.ID, agentDirectiveTestResult("local_guess"))
			},
			wantCode:   3,
			wantStderr: "run_id_resolution",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setCLIAPITestToken(t, "test-token")
			server := httptest.NewServer(tc.handler)
			defer server.Close()

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"agent", "directive", "agent-1", "run it"}, &stdout, &stderr, testRootCommandOptions(server))
			if code != tc.wantCode {
				t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, tc.wantCode, stdout.String(), stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.wantStderr) {
				t.Fatalf("stderr = %q, want substring %q", stderr.String(), tc.wantStderr)
			}
		})
	}
}

func agentDirectiveTestResult(resolution string) map[string]any {
	return map[string]any{
		"ok":                   true,
		"response":             "accepted",
		"run_id":               "run-1",
		"run_id_resolution":    resolution,
		"directive_event_id":   "event-directive-1",
		"directive_event_type": "platform.agent_directive",
	}
}

func writeAgentDirectiveJSONRPCError(t *testing.T, w http.ResponseWriter, id string, code string) {
	t.Helper()
	w.Header().Set("content-type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"error": map[string]any{
			"code":    -32010,
			"message": "Application error: " + code,
			"data": map[string]any{
				"code":           code,
				"details":        map[string]any{"agent_id": "agent-1"},
				"retryable":      false,
				"correlation_id": "corr-agent-directive",
			},
		},
	}); err != nil {
		t.Fatalf("encode response: %v", err)
	}
}
