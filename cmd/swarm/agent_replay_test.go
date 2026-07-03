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

func TestAgentReplayUsesV1RPCWithEventIDAndIdempotencyKey(t *testing.T) {
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
		writeJSONRPCResult(t, w, captured.ID, agentReplayTestResult())
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{
		"agent", "replay", "agent-1",
		"--event-id", "event-1",
		"--idempotency-key", "idem-1",
	}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if captured.JSONRPC != "2.0" || captured.Method != agentReplayMethod {
		t.Fatalf("request jsonrpc/method = %s/%s, want 2.0/%s", captured.JSONRPC, captured.Method, agentReplayMethod)
	}
	wantParams := map[string]any{
		"agent_id":        "agent-1",
		"event_id":        "event-1",
		"idempotency_key": "idem-1",
	}
	if !reflect.DeepEqual(captured.Params, wantParams) {
		t.Fatalf("params = %#v, want %#v", captured.Params, wantParams)
	}
	for _, want := range []string{
		"agent replay ok:",
		"agent_id=agent-1",
		"event_id=event-1",
		"replay_event_id=event-replay-1",
		"audit_event_id=event-audit-1",
		"original_delivery.delivery_id=delivery-original-1",
		"new_delivery.delivery_id=delivery-new-1",
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestAgentReplayOmitsIdempotencyKeyWhenNotProvided(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var captured jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Errorf("decode request: %v", err)
		}
		writeJSONRPCResult(t, w, captured.ID, agentReplayTestResult())
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"agent", "replay", "agent-1", "--event-id", "event-1"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	wantParams := map[string]any{"agent_id": "agent-1", "event_id": "event-1"}
	if !reflect.DeepEqual(captured.Params, wantParams) {
		t.Fatalf("params = %#v, want %#v", captured.Params, wantParams)
	}
}

func TestAgentReplayRejectsInvalidInputBeforeRequest(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeJSONRPCResult(t, w, "unexpected", agentReplayTestResult())
	}))
	defer server.Close()

	for _, tc := range []struct {
		name       string
		args       []string
		wantStderr string
	}{
		{name: "missing id", args: []string{"agent", "replay", "--event-id", "event-1"}, wantStderr: "accepts 1 arg(s)"},
		{name: "blank id", args: []string{"agent", "replay", "  ", "--event-id", "event-1"}, wantStderr: "agent id is required"},
		{name: "missing event id", args: []string{"agent", "replay", "agent-1"}, wantStderr: "--event-id is required"},
		{name: "blank event id", args: []string{"agent", "replay", "agent-1", "--event-id", "  "}, wantStderr: "--event-id is required"},
		{name: "extra arg", args: []string{"agent", "replay", "agent-1", "extra", "--event-id", "event-1"}, wantStderr: "accepts 1 arg(s)"},
		{name: "unsupported flag", args: []string{"agent", "replay", "agent-1", "--event-id", "event-1", "--unknown"}, wantStderr: "unknown flag"},
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

func TestAgentReplayFailClosedWithoutToken(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeJSONRPCResult(t, w, "unexpected", agentReplayTestResult())
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"agent", "replay", "agent-1", "--event-id", "event-1"}, &stdout, &stderr, testRootCommandOptions(server))
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

func TestAgentReplayMapsFailureExitCodes(t *testing.T) {
	resourceErrors := []string{
		"EVENT_REPLAY_NO_DELIVERY_HISTORY",
		"EVENT_REPLAY_SUBSCRIBER_NOT_ORIGINAL",
		"EVENT_REPLAY_SUBSCRIBER_UNAVAILABLE",
		"EVENT_REPLAY_NOT_ELIGIBLE",
		"PAYLOAD_VALIDATION_FAILED",
		"IDEMPOTENCY_CONFLICT",
	}
	for _, code := range resourceErrors {
		code := code
		t.Run(code+" exits six", func(t *testing.T) {
			setCLIAPITestToken(t, "test-token")
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeAgentReplayJSONRPCError(t, w, req.ID, code)
			}))
			defer server.Close()

			var stdout, stderr bytes.Buffer
			exit := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"agent", "replay", "agent-1", "--event-id", "event-1"}, &stdout, &stderr, testRootCommandOptions(server))
			if exit != 6 {
				t.Fatalf("code = %d, want 6 stdout=%s stderr=%s", exit, stdout.String(), stderr.String())
			}
			if !strings.Contains(stderr.String(), code) {
				t.Fatalf("stderr = %q, want substring %q", stderr.String(), code)
			}
		})
	}

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
			wantStderr: "v1 RPC HTTP 401",
		},
		{
			name: "http runtime failure exits three",
			handler: func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "unavailable", http.StatusServiceUnavailable)
			},
			wantCode:   3,
			wantStderr: "v1 RPC HTTP 503",
		},
		{
			name: "event not found exits five",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeAgentReplayJSONRPCError(t, w, req.ID, "EVENT_NOT_FOUND")
			},
			wantCode:   5,
			wantStderr: "EVENT_NOT_FOUND",
		},
		{
			name: "unauthorized rpc exits four",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeAgentReplayJSONRPCError(t, w, req.ID, "UNAUTHORIZED")
			},
			wantCode:   4,
			wantStderr: "UNAUTHORIZED",
		},
		{
			name: "unknown rpc error exits three",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeAgentReplayJSONRPCError(t, w, req.ID, "METHOD_UNAVAILABLE")
			},
			wantCode:   3,
			wantStderr: "METHOD_UNAVAILABLE",
		},
		{
			name: "malformed result exits three",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				result := agentReplayTestResult()
				delete(result, "replay_event_id")
				writeJSONRPCResult(t, w, req.ID, result)
			},
			wantCode:   3,
			wantStderr: "replay_event_id is required",
		},
		{
			name: "missing delivery id exits three",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				result := agentReplayTestResult()
				original := result["original_delivery"].(map[string]any)
				delete(original, "delivery_id")
				writeJSONRPCResult(t, w, req.ID, result)
			},
			wantCode:   3,
			wantStderr: "original_delivery.delivery_id is required",
		},
		{
			name: "invalid delivery status exits three",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				result := agentReplayTestResult()
				newDelivery := result["new_delivery"].(map[string]any)
				newDelivery["status"] = "locally_replayed"
				writeJSONRPCResult(t, w, req.ID, result)
			},
			wantCode:   3,
			wantStderr: "new_delivery.status",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setCLIAPITestToken(t, "test-token")
			server := httptest.NewServer(tc.handler)
			defer server.Close()

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"agent", "replay", "agent-1", "--event-id", "event-1"}, &stdout, &stderr, testRootCommandOptions(server))
			if code != tc.wantCode {
				t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, tc.wantCode, stdout.String(), stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.wantStderr) {
				t.Fatalf("stderr = %q, want substring %q", stderr.String(), tc.wantStderr)
			}
		})
	}
}

func agentReplayTestResult() map[string]any {
	return map[string]any{
		"event_id":        "event-1",
		"agent_id":        "agent-1",
		"replay_event_id": "event-replay-1",
		"audit_event_id":  "event-audit-1",
		"original_delivery": map[string]any{
			"delivery_id":   "delivery-original-1",
			"subscriber_id": "agent-1",
			"session_id":    "session-original-1",
			"status":        "delivered",
			"attempt":       1,
		},
		"new_delivery": map[string]any{
			"delivery_id":        "delivery-new-1",
			"subscriber_id":      "agent-1",
			"session_id":         "session-new-1",
			"status":             "pending",
			"attempt":            1,
			"source_delivery_id": "delivery-original-1",
		},
	}
}

func writeAgentReplayJSONRPCError(t *testing.T, w http.ResponseWriter, id string, code string) {
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
				"details":        map[string]any{"agent_id": "agent-1", "event_id": "event-1"},
				"retryable":      false,
				"correlation_id": "corr-agent-replay",
			},
		},
	}); err != nil {
		t.Fatalf("encode response: %v", err)
	}
}
