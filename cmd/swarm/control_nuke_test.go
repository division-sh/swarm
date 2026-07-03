package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
)

func TestControlNukeDryRunSendsV1RPCRequest(t *testing.T) {
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
		writeJSONRPCResult(t, w, captured.ID, runtimeNukeTestResult(runtimeNukeStatusDryRun))
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"control", "nuke", "--dry-run"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if captured.JSONRPC != "2.0" || captured.Method != "runtime.nuke" {
		t.Fatalf("request jsonrpc/method = %s/%s, want 2.0/runtime.nuke", captured.JSONRPC, captured.Method)
	}
	if !reflect.DeepEqual(captured.Params, map[string]any{"dry_run": true}) {
		t.Fatalf("params = %#v, want dry_run=true only", captured.Params)
	}
	for _, want := range []string{"runtime nuke dry-run", "status=dry_run", "active_runs=2", "selected_containers=1", "preserved_containers=1"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestControlNukeYesAppliesThroughV1RPC(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var captured jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Errorf("decode request: %v", err)
		}
		writeJSONRPCResult(t, w, captured.ID, runtimeNukeTestResult(runtimeNukeStatusDone))
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"control", "nuke", "--yes"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if captured.Method != "runtime.nuke" {
		t.Fatalf("method = %q, want runtime.nuke", captured.Method)
	}
	if !reflect.DeepEqual(captured.Params, map[string]any{"dry_run": false}) {
		t.Fatalf("params = %#v, want dry_run=false only", captured.Params)
	}
	for _, want := range []string{"runtime nuke complete", "status=completed", "cleanup_deleted_rows=17", "stopped_containers=1"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestControlNukeSendsIdempotencyKey(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	for _, tc := range []struct {
		name          string
		args          []string
		input         string
		stdinTerminal bool
		wantParams    map[string]any
		wantPrompt    bool
	}{
		{
			name:       "dry run",
			args:       []string{"control", "nuke", "--dry-run", "--idempotency-key", "idem-dry"},
			wantParams: map[string]any{"dry_run": true, "idempotency_key": "idem-dry"},
		},
		{
			name:       "yes apply",
			args:       []string{"control", "nuke", "--yes", "--idempotency-key", "idem-apply"},
			wantParams: map[string]any{"dry_run": false, "idempotency_key": "idem-apply"},
		},
		{
			name:       "yes apply preserves caller key",
			args:       []string{"control", "nuke", "--yes", "--idempotency-key", "  idem-spaced  "},
			wantParams: map[string]any{"dry_run": false, "idempotency_key": "  idem-spaced  "},
		},
		{
			name:          "tty confirmed apply",
			args:          []string{"control", "nuke", "--idempotency-key", "idem-tty"},
			input:         "y\n",
			stdinTerminal: true,
			wantParams:    map[string]any{"dry_run": false, "idempotency_key": "idem-tty"},
			wantPrompt:    true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var captured jsonRPCRequest
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
					t.Errorf("decode request: %v", err)
				}
				status := runtimeNukeStatusDone
				if tc.wantParams["dry_run"] == true {
					status = runtimeNukeStatusDryRun
				}
				writeJSONRPCResult(t, w, captured.ID, runtimeNukeTestResult(status))
			}))
			defer server.Close()

			var stdout, stderr bytes.Buffer
			opts := testRootCommandOptions(server)
			opts.input = strings.NewReader(tc.input)
			opts.stdinIsTerminal = func() bool { return tc.stdinTerminal }
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), tc.args, &stdout, &stderr, opts)
			if code != 0 {
				t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
			}
			if captured.Method != "runtime.nuke" {
				t.Fatalf("method = %q, want runtime.nuke", captured.Method)
			}
			if !reflect.DeepEqual(captured.Params, tc.wantParams) {
				t.Fatalf("params = %#v, want %#v", captured.Params, tc.wantParams)
			}
			if gotPrompt := strings.Contains(stderr.String(), "Continue? [y/N]"); gotPrompt != tc.wantPrompt {
				t.Fatalf("prompt present = %v, want %v stderr=%q", gotPrompt, tc.wantPrompt, stderr.String())
			}
		})
	}
}

func TestControlNukeNoOpResultExitsZero(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req jsonRPCRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		writeJSONRPCResult(t, w, req.ID, runtimeNukeEmptyResult())
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"control", "nuke", "--yes"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if !strings.Contains(stdout.String(), "nuke complete (no-op)") {
		t.Fatalf("stdout = %q, want no-op output", stdout.String())
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestControlNukeConfirmationAndNoCallPaths(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var req jsonRPCRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		writeJSONRPCResult(t, w, req.ID, runtimeNukeTestResult(runtimeNukeStatusDone))
	}))
	defer server.Close()

	for _, tc := range []struct {
		name          string
		args          []string
		input         string
		stdinTerminal bool
		wantCode      int
		wantCalls     int32
		wantStderr    string
	}{
		{name: "tty confirmation proceeds", args: []string{"control", "nuke"}, input: "y\n", stdinTerminal: true, wantCode: 0, wantCalls: 1, wantStderr: "Continue? [y/N]"},
		{name: "tty abort makes no request", args: []string{"control", "nuke"}, input: "n\n", stdinTerminal: true, wantCode: 2, wantCalls: 0, wantStderr: "Aborted; no destruction performed."},
		{name: "non tty apply without yes makes no request", args: []string{"control", "nuke"}, stdinTerminal: false, wantCode: 2, wantCalls: 0, wantStderr: "pass --yes for non-TTY"},
		{name: "tty abort with key makes no request", args: []string{"control", "nuke", "--idempotency-key", "idem-abort"}, input: "n\n", stdinTerminal: true, wantCode: 2, wantCalls: 0, wantStderr: "Aborted; no destruction performed."},
		{name: "non tty apply with key without yes makes no request", args: []string{"control", "nuke", "--idempotency-key", "idem-ntty"}, stdinTerminal: false, wantCode: 2, wantCalls: 0, wantStderr: "pass --yes for non-TTY"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls.Store(0)
			var stdout, stderr bytes.Buffer
			opts := testRootCommandOptions(server)
			opts.input = strings.NewReader(tc.input)
			opts.stdinIsTerminal = func() bool { return tc.stdinTerminal }
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), tc.args, &stdout, &stderr, opts)
			if code != tc.wantCode {
				t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, tc.wantCode, stdout.String(), stderr.String())
			}
			if calls.Load() != tc.wantCalls {
				t.Fatalf("server calls = %d, want %d", calls.Load(), tc.wantCalls)
			}
			if !strings.Contains(stderr.String(), tc.wantStderr) {
				t.Fatalf("stderr = %q, want substring %q", stderr.String(), tc.wantStderr)
			}
		})
	}
}

func TestControlNukeRejectsBlankIdempotencyKeyBeforePromptOrRequest(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeJSONRPCResult(t, w, "unexpected", runtimeNukeTestResult(runtimeNukeStatusDone))
	}))
	defer server.Close()

	for _, tc := range []struct {
		name          string
		args          []string
		stdinTerminal bool
	}{
		{name: "dry run", args: []string{"control", "nuke", "--dry-run", "--idempotency-key", "   "}},
		{name: "yes apply", args: []string{"control", "nuke", "--yes", "--idempotency-key", "   "}},
		{name: "prompt path", args: []string{"control", "nuke", "--idempotency-key", "   "}, stdinTerminal: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls.Store(0)
			var stdout, stderr bytes.Buffer
			opts := testRootCommandOptions(server)
			opts.input = strings.NewReader("y\n")
			opts.stdinIsTerminal = func() bool { return tc.stdinTerminal }
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), tc.args, &stdout, &stderr, opts)
			if code != 2 {
				t.Fatalf("code = %d, want 2 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
			}
			if calls.Load() != 0 {
				t.Fatalf("server calls = %d, want 0", calls.Load())
			}
			if !strings.Contains(stderr.String(), "--idempotency-key must be non-empty") {
				t.Fatalf("stderr = %q, want blank key validation", stderr.String())
			}
			if strings.Contains(stderr.String(), "Continue? [y/N]") {
				t.Fatalf("stderr = %q, want no confirmation prompt", stderr.String())
			}
		})
	}
}

func TestControlNukeMapsFailureExitCodes(t *testing.T) {
	for _, tc := range []struct {
		name       string
		token      string
		handler    http.HandlerFunc
		wantCode   int
		wantStderr string
	}{
		{
			name:     "partial failure exits three",
			token:    "test-token",
			wantCode: 3,
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeJSONRPCResult(t, w, req.ID, runtimeNukeTestResult(runtimeNukeStatusPartial))
			},
			wantStderr: "runtime.nuke failure",
		},
		{
			name:  "idempotency conflict exits six",
			token: "test-token",
			handler: func(w http.ResponseWriter, r *http.Request) {
				writeRuntimeNukeJSONRPCError(t, w, r, "IDEMPOTENCY_CONFLICT")
			},
			wantCode:   6,
			wantStderr: "IDEMPOTENCY_CONFLICT",
		},
		{
			name:  "operation in progress exits six",
			token: "test-token",
			handler: func(w http.ResponseWriter, r *http.Request) {
				writeRuntimeNukeJSONRPCError(t, w, r, "RUNTIME_NUKE_IN_PROGRESS")
			},
			wantCode:   6,
			wantStderr: "RUNTIME_NUKE_IN_PROGRESS",
		},
		{
			name:  "json rpc unauthorized exits four",
			token: "test-token",
			handler: func(w http.ResponseWriter, r *http.Request) {
				writeRuntimeNukeJSONRPCError(t, w, r, "UNAUTHORIZED")
			},
			wantCode:   4,
			wantStderr: "UNAUTHORIZED",
		},
		{
			name:  "http unauthorized exits four",
			token: "test-token",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"error":"invalid bearer token"}`))
			},
			wantCode:   4,
			wantStderr: "v1 RPC HTTP 401",
		},
		{
			name:     "missing token exits four before request",
			token:    "",
			wantCode: 4,
			handler: func(w http.ResponseWriter, r *http.Request) {
				t.Fatal("unexpected request without SWARM_API_TOKEN")
			},
			wantStderr: "API token source is required",
		},
		{
			name:  "malformed response exits three",
			token: "test-token",
			handler: func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(`{`))
			},
			wantCode:   3,
			wantStderr: "decode JSON-RPC response",
		},
		{
			name:  "malformed result exits three",
			token: "test-token",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				result := runtimeNukeTestResult(runtimeNukeStatusDone)
				result["operation_name"] = ""
				writeJSONRPCResult(t, w, req.ID, result)
			},
			wantCode:   3,
			wantStderr: "operation_name is required",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setCLIAPITestToken(t, tc.token)
			server := httptest.NewServer(tc.handler)
			defer server.Close()

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"control", "nuke", "--yes"}, &stdout, &stderr, testRootCommandOptions(server))
			if code != tc.wantCode {
				t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, tc.wantCode, stdout.String(), stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.wantStderr) {
				t.Fatalf("stderr = %q, want substring %q", stderr.String(), tc.wantStderr)
			}
		})
	}
}

func TestControlNukeTransportFailureExitsThree(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var stdout, stderr bytes.Buffer
	opts := defaultRootCommandOptions()
	opts.apiRPCEndpointOverride = "http://127.0.0.1:1/v1/rpc"
	opts.httpClient = &http.Client{Transport: errRoundTripper{}}
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"control", "nuke", "--yes"}, &stdout, &stderr, opts)
	if code != 3 {
		t.Fatalf("code = %d, want 3 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), "v1 RPC request failed") {
		t.Fatalf("stderr = %q, want transport failure", stderr.String())
	}
}

type errRoundTripper struct{}

func (errRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("transport unavailable")
}

func runtimeNukeTestResult(status string) map[string]any {
	result := map[string]any{
		"ok":             true,
		"status":         status,
		"dry_run":        status == runtimeNukeStatusDryRun,
		"operation_name": "runtime.destructive_reset",
		"plan": map[string]any{
			"plan": map[string]any{
				"active_runs": []any{
					map[string]any{"run_id": "run-1", "status": "running"},
					map[string]any{"run_id": "run-2", "status": "paused"},
				},
				"active_deliveries": []any{
					map[string]any{"delivery_id": "delivery-1", "run_id": "run-1", "status": "pending"},
				},
				"run_scoped_tables": []any{
					map[string]any{"name": "events", "action": "delete"},
					map[string]any{"name": "event_deliveries", "action": "delete"},
				},
				"entity_containers": []any{
					map[string]any{"name": "swarm-agent-1", "kind": "entity"},
				},
			},
		},
		"quiescence": map[string]any{
			"runs": []any{
				map[string]any{"run_id": "run-1", "status": "stopped", "changed": true},
			},
			"deliveries": []any{
				map[string]any{"delivery_id": "delivery-1", "status": "exhausted", "changed": true},
			},
		},
		"cleanup": map[string]any{
			"tables": []any{
				map[string]any{"table": "events", "matched_rows": 10, "deleted_rows": 10},
				map[string]any{"table": "event_deliveries", "matched_rows": 7, "deleted_rows": 7},
			},
		},
		"containers": map[string]any{
			"selected": []any{
				map[string]any{"name": "swarm-agent-1", "kind": "entity"},
			},
			"preserved": []any{
				map[string]any{"name": "swarm-system", "kind": "system"},
			},
			"stopped": []any{
				map[string]any{"name": "swarm-agent-1", "kind": "entity"},
			},
		},
		"partial_failure": false,
	}
	switch status {
	case runtimeNukeStatusPartial:
		result["ok"] = false
		result["dry_run"] = false
		result["partial_failure"] = true
		result["errors"] = []any{
			map[string]any{"scope": "managed_containers", "message": "docker stop denied"},
		}
		result["containers"].(map[string]any)["failed"] = []any{
			map[string]any{
				"container": map[string]any{"name": "swarm-agent-2", "kind": "entity"},
				"error":     "docker stop denied",
			},
		}
	case runtimeNukeStatusDone:
		result["dry_run"] = false
	}
	return result
}

func runtimeNukeEmptyResult() map[string]any {
	return map[string]any{
		"ok":             true,
		"status":         runtimeNukeStatusDone,
		"dry_run":        false,
		"operation_name": "runtime.destructive_reset",
		"plan": map[string]any{
			"plan": map[string]any{
				"active_runs":       []any{},
				"active_deliveries": []any{},
				"run_scoped_tables": []any{},
				"entity_containers": []any{},
			},
		},
		"quiescence":      map[string]any{"runs": []any{}, "deliveries": []any{}},
		"cleanup":         map[string]any{"tables": []any{}},
		"containers":      map[string]any{"selected": []any{}, "preserved": []any{}, "stopped": []any{}},
		"partial_failure": false,
	}
}

func writeRuntimeNukeJSONRPCError(t *testing.T, w http.ResponseWriter, r *http.Request, code string) {
	t.Helper()
	var req jsonRPCRequest
	_ = json.NewDecoder(r.Body).Decode(&req)
	w.Header().Set("content-type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{
		"jsonrpc": "2.0",
		"id":      req.ID,
		"error": map[string]any{
			"code":    -32000,
			"message": "Application error: " + code,
			"data": map[string]any{
				"code":           code,
				"details":        map[string]any{},
				"retryable":      false,
				"correlation_id": "corr-1",
			},
		},
	}); err != nil {
		t.Fatalf("encode response: %v", err)
	}
}
