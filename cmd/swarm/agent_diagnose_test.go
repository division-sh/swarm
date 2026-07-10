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

func TestAgentDiagnoseUsesAgentDiagnoseAndRendersOwnedFields(t *testing.T) {
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
		writeJSONRPCResult(t, w, captured.ID, validAgentDiagnosisResult())
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"agent", "diagnose", " agent-1 ", "--queue-limit", "2", "--queue-cursor", "cursor-1"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if captured.JSONRPC != "2.0" || captured.Method != "agent.diagnose" {
		t.Fatalf("request jsonrpc/method = %s/%s, want 2.0/agent.diagnose", captured.JSONRPC, captured.Method)
	}
	wantParams := map[string]any{"agent_id": "agent-1", "queue_limit": float64(2), "queue_cursor": "cursor-1"}
	if !reflect.DeepEqual(captured.Params, wantParams) {
		t.Fatalf("params = %#v, want %#v", captured.Params, wantParams)
	}
	for _, want := range []string{
		"Agent agent-1  running",
		"session      session-1, started 2026-05-18T03:00:00Z",
		"last turn    turn-1, completed 2026-05-18T03:05:00Z, parsed true",
		"queue        2 pending deliveries, oldest 30s, next cursor cursor-2",
		"lifecycle    retrying, delivery retry",
		"watchdog     no output, session execution, session produced no output, warning emitted",
		"observed     2026-05-18T03:02:00Z",
		"active       turn turn-1, task task-1, entity entity-1",
		`latest tool  read_file on turn turn-1, ok true, use toolu-1, result {"summary":"ok"}`,
		"pending deliveries",
		"platform.agent_directive (event-1), queued 2026-05-18T03:01:00Z, 1 attempt",
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
	for _, notWant := range []string{"token_usage", "recent_failures", "dead_letters", "runtime_state", "current_session_ref", "last_turn_ref", "last_tool_outcome", "agent.get", "agent.list", "conversation.", "run.diagnose", "trace"} {
		if strings.Contains(stdout.String(), notWant) {
			t.Fatalf("stdout contains split concept %q:\n%s", notWant, stdout.String())
		}
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestAgentDiagnoseJSONPreservesAPIResultShape(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var captured jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Errorf("decode request: %v", err)
		}
		writeJSONRPCResult(t, w, captured.ID, validAgentDiagnosisResult())
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"agent", "diagnose", "agent-1", "--json"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if captured.Method != "agent.diagnose" {
		t.Fatalf("method = %q, want agent.diagnose", captured.Method)
	}
	if !reflect.DeepEqual(captured.Params, map[string]any{"agent_id": "agent-1"}) {
		t.Fatalf("params = %#v, want agent_id only", captured.Params)
	}
	var decoded map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &decoded); err != nil {
		t.Fatalf("decode stdout JSON: %v\n%s", err, stdout.String())
	}
	if decoded["agent_id"] != "agent-1" || decoded["status"] != "running" {
		t.Fatalf("json identity/status = %#v", decoded)
	}
	if _, ok := decoded["queue"].(map[string]any); !ok {
		t.Fatalf("json queue = %#v, want object", decoded["queue"])
	}
	if _, ok := decoded["last_tool_outcome"].(map[string]any); !ok {
		t.Fatalf("json last_tool_outcome = %#v, want object", decoded["last_tool_outcome"])
	}
	for _, wrapper := range []string{"agent", "diagnosis"} {
		if _, ok := decoded[wrapper]; ok {
			t.Fatalf("json output contains CLI wrapper %q: %#v", wrapper, decoded)
		}
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestAgentDiagnoseProjectsEveryCurrentLifecyclePairAndPreservesMachineValues(t *testing.T) {
	tests := []struct {
		state       string
		layer       string
		humanPhrase string
	}{
		{state: "queued", layer: "delivery_queue", humanPhrase: "queued, delivery queue"},
		{state: "launching", layer: "session_launch", humanPhrase: "launching, session launch"},
		{state: "active", layer: "session_execution", humanPhrase: "active, session execution"},
		{state: "retrying", layer: "delivery_retry", humanPhrase: "retrying, delivery retry"},
		{state: "exhausted", layer: "delivery_terminal", humanPhrase: "exhausted, delivery terminal"},
	}
	for _, tc := range tests {
		t.Run(tc.state, func(t *testing.T) {
			setCLIAPITestToken(t, "test-token")
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&request)
				result := validAgentDiagnosisResult()
				result["delivery_lifecycle"] = map[string]any{"state": tc.state, "blocking_layer": tc.layer}
				writeJSONRPCResult(t, w, request.ID, result)
			}))
			defer server.Close()

			var humanOut, humanErr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"agent", "diagnose", "agent-1"}, &humanOut, &humanErr, testRootCommandOptions(server))
			if code != 0 || !strings.Contains(humanOut.String(), "lifecycle    "+tc.humanPhrase) {
				t.Fatalf("human lifecycle code=%d stdout=%s stderr=%s", code, humanOut.String(), humanErr.String())
			}

			var jsonOut, jsonErr bytes.Buffer
			code = executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"agent", "diagnose", "agent-1", "--json"}, &jsonOut, &jsonErr, testRootCommandOptions(server))
			if code != 0 {
				t.Fatalf("json lifecycle code=%d stdout=%s stderr=%s", code, jsonOut.String(), jsonErr.String())
			}
			var decoded map[string]any
			if err := json.Unmarshal(jsonOut.Bytes(), &decoded); err != nil {
				t.Fatalf("decode JSON: %v", err)
			}
			lifecycle := decoded["delivery_lifecycle"].(map[string]any)
			if lifecycle["state"] != tc.state || lifecycle["blocking_layer"] != tc.layer {
				t.Fatalf("machine lifecycle = %#v, want %s/%s", lifecycle, tc.state, tc.layer)
			}
		})
	}
}

func TestAgentDiagnoseProjectsEveryCurrentWatchdogTuple(t *testing.T) {
	tests := []struct {
		name        string
		watchdog    map[string]any
		humanPhrase string
	}{
		{
			name: "healthy long running",
			watchdog: map[string]any{
				"state": "healthy_long_running", "blocking_layer": "session_execution",
				"action": "turn_long_running", "outcome": "observed",
				"last_output_at": "2026-05-18T03:01:00Z", "recorded_at": "2026-05-18T03:02:00Z",
			},
			humanPhrase: "healthy, long-running, session execution, turn running for a long time, observed",
		},
		{
			name: "no output",
			watchdog: map[string]any{
				"state": "no_output", "blocking_layer": "session_execution",
				"action": "session_no_output", "outcome": "warning_emitted", "recorded_at": "2026-05-18T03:02:00Z",
			},
			humanPhrase: "no output, session execution, session produced no output, warning emitted",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			setCLIAPITestToken(t, "test-token")
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&request)
				result := validAgentDiagnosisResult()
				result["runtime_state"] = map[string]any{"watchdog": tc.watchdog}
				writeJSONRPCResult(t, w, request.ID, result)
			}))
			defer server.Close()

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"agent", "diagnose", "agent-1"}, &stdout, &stderr, testRootCommandOptions(server))
			if code != 0 || !strings.Contains(stdout.String(), "watchdog     "+tc.humanPhrase) {
				t.Fatalf("watchdog code=%d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
			}
		})
	}
}

func TestAgentDiagnoseRejectsInvalidInputBeforeRequest(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeJSONRPCResult(t, w, "unexpected", validAgentDiagnosisResult())
	}))
	defer server.Close()

	for _, tc := range []struct {
		name       string
		args       []string
		wantStderr string
	}{
		{name: "missing id", args: []string{"agent", "diagnose"}, wantStderr: "requires <agent-id>"},
		{name: "blank id", args: []string{"agent", "diagnose", "  "}, wantStderr: "agent id is required"},
		{name: "extra arg", args: []string{"agent", "diagnose", "agent-1", "extra"}, wantStderr: "accepts one argument"},
		{name: "unsupported flag", args: []string{"agent", "diagnose", "agent-1", "--unknown"}, wantStderr: "unknown flag"},
		{name: "queue limit too small", args: []string{"agent", "diagnose", "agent-1", "--queue-limit", "0"}, wantStderr: "--queue-limit must be between 1 and 200"},
		{name: "queue limit too large", args: []string{"agent", "diagnose", "agent-1", "--queue-limit", "201"}, wantStderr: "--queue-limit must be between 1 and 200"},
		{name: "blank queue cursor", args: []string{"agent", "diagnose", "agent-1", "--queue-cursor", ""}, wantStderr: "--queue-cursor is required when provided"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls.Store(0)
			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), tc.args, &stdout, &stderr, testRootCommandOptions(server))
			if code != cliExitValidation {
				t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, cliExitValidation, stdout.String(), stderr.String())
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

func TestAgentDiagnoseFailClosedOnRPCAndMalformedResponses(t *testing.T) {
	for _, tc := range []struct {
		name       string
		handler    http.HandlerFunc
		wantCode   int
		wantStderr string
	}{
		{
			name: "http auth failure",
			handler: func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
			},
			wantCode:   cliExitAuth,
			wantStderr: "rejected the request with status 401",
		},
		{
			name: "agent not found",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeAgentJSONRPCError(t, w, req.ID, "AGENT_NOT_FOUND")
			},
			wantCode:   cliExitNotFound,
			wantStderr: "AGENT_NOT_FOUND: Application error: AGENT_NOT_FOUND",
		},
		{
			name: "api invalid params",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeAgentInvalidParamsJSONRPCError(t, w, req.ID, "Invalid params: queue_limit")
			},
			wantCode:   cliExitRuntime,
			wantStderr: "Invalid params: queue_limit",
		},
		{
			name: "missing queue deliveries",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				result := validAgentDiagnosisResult()
				result["queue"] = map[string]any{"pending_count": 1, "oldest_pending_age_seconds": 30}
				writeJSONRPCResult(t, w, req.ID, result)
			},
			wantCode:   cliExitRuntime,
			wantStderr: "malformed agent.diagnose result: queue.pending_deliveries is required",
		},
		{
			name: "missing last tool ok",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				result := validAgentDiagnosisResult()
				result["last_tool_outcome"] = map[string]any{"turn_id": "turn-1", "tool_name": "read_file"}
				writeJSONRPCResult(t, w, req.ID, result)
			},
			wantCode:   cliExitRuntime,
			wantStderr: "malformed agent.diagnose result: last_tool_outcome.ok is required",
		},
		{
			name: "healthy watchdog missing last output",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				result := validAgentDiagnosisResult()
				result["runtime_state"] = map[string]any{
					"watchdog": map[string]any{
						"state":          "healthy_long_running",
						"blocking_layer": "session_execution",
						"action":         "turn_long_running",
						"outcome":        "observed",
						"recorded_at":    "2026-05-18T03:02:00Z",
					},
				}
				writeJSONRPCResult(t, w, req.ID, result)
			},
			wantCode:   cliExitRuntime,
			wantStderr: "malformed agent.diagnose result: runtime_state.watchdog.last_output_at is required for healthy_long_running state",
		},
		{
			name: "malformed last tool result",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				result := validAgentDiagnosisResult()
				result["last_tool_outcome"] = map[string]any{"turn_id": "turn-1", "tool_name": "read_file", "ok": true, "result": "not-object"}
				writeJSONRPCResult(t, w, req.ID, result)
			},
			wantCode:   cliExitRuntime,
			wantStderr: "malformed agent.diagnose result: last_tool_outcome.result must be a JSON object",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setCLIAPITestToken(t, "test-token")
			server := httptest.NewServer(tc.handler)
			defer server.Close()

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"agent", "diagnose", "agent-1"}, &stdout, &stderr, testRootCommandOptions(server))
			if code != tc.wantCode {
				t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, tc.wantCode, stdout.String(), stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.wantStderr) {
				t.Fatalf("stderr = %q, want substring %q", stderr.String(), tc.wantStderr)
			}
		})
	}
}

func validAgentDiagnosisResult() map[string]any {
	return map[string]any{
		"agent_id":            "agent-1",
		"status":              "running",
		"current_session_ref": map[string]any{"session_id": "session-1", "started_at": "2026-05-18T03:00:00Z"},
		"last_turn_ref":       map[string]any{"turn_id": "turn-1", "completed_at": "2026-05-18T03:05:00Z", "parse_ok": true},
		"queue": map[string]any{
			"pending_count":              2,
			"oldest_pending_age_seconds": 30,
			"pending_deliveries": []map[string]any{
				{"event_id": "event-1", "event_name": "platform.agent_directive", "enqueued_at": "2026-05-18T03:01:00Z", "attempts": 1},
			},
			"next_cursor": "cursor-2",
		},
		"delivery_lifecycle": map[string]any{"state": "retrying", "blocking_layer": "delivery_retry"},
		"runtime_state": map[string]any{
			"watchdog": map[string]any{
				"state":          "no_output",
				"blocking_layer": "session_execution",
				"action":         "session_no_output",
				"outcome":        "warning_emitted",
				"recorded_at":    "2026-05-18T03:02:00Z",
			},
		},
		"active":            map[string]any{"turn_id": "turn-1", "task_id": "task-1", "entity_id": "entity-1"},
		"last_tool_outcome": map[string]any{"turn_id": "turn-1", "tool_name": "read_file", "tool_use_id": "toolu-1", "ok": true, "result": map[string]any{"summary": "ok"}},
	}
}

func writeAgentInvalidParamsJSONRPCError(t *testing.T, w http.ResponseWriter, id string, message string) {
	t.Helper()
	w.Header().Set("content-type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"error": map[string]any{
			"code":    -32602,
			"message": message,
		},
	}); err != nil {
		t.Fatalf("encode response: %v", err)
	}
}
