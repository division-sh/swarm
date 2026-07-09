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

func TestAgentsListUsesV1RPCWithFilters(t *testing.T) {
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
		writeJSONRPCResult(t, w, captured.ID, map[string]any{
			"agents": []map[string]any{
				agentSummaryResult("agent-1", "researcher", "running"),
				agentSummaryResult("agent-2", "researcher", "idle"),
			},
		})
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"agent", "list", "--flow", "flows/research", "--role", "researcher"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if captured.JSONRPC != "2.0" || captured.Method != "agent.list" {
		t.Fatalf("request jsonrpc/method = %s/%s, want 2.0/agent.list", captured.JSONRPC, captured.Method)
	}
	wantParams := map[string]any{"flow": "flows/research", "role": "researcher"}
	if !reflect.DeepEqual(captured.Params, wantParams) {
		t.Fatalf("params = %#v, want %#v", captured.Params, wantParams)
	}
	for _, want := range []string{"AGENT_ID", "agent-1", "researcher", "worker", "running", "default", "task", "agent-2", "idle"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestAgentsListEmptyResult(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var captured jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Errorf("decode request: %v", err)
		}
		writeJSONRPCResult(t, w, captured.ID, map[string]any{"agents": []any{}})
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"agent", "list"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if captured.Method != "agent.list" {
		t.Fatalf("method = %q, want agent.list", captured.Method)
	}
	if len(captured.Params) != 0 {
		t.Fatalf("params = %#v, want empty", captured.Params)
	}
	if !strings.Contains(stdout.String(), "No agents match the current filters.") {
		t.Fatalf("stdout = %q, want empty message", stdout.String())
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestAgentViewUsesAgentGetAndRendersRefsOnly(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var captured jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Errorf("Authorization = %q, want bearer token", got)
		}
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Errorf("decode request: %v", err)
		}
		parseOK := true
		writeJSONRPCResult(t, w, captured.ID, map[string]any{
			"agent":               agentSummaryResult("agent-1", "reviewer", "running"),
			"current_session_ref": map[string]any{"session_id": "session-1", "started_at": "2026-05-18T03:00:00Z"},
			"last_turn_ref":       map[string]any{"turn_id": "turn-1", "completed_at": "2026-05-18T03:05:00Z", "parse_ok": parseOK},
		})
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"agent", "view", "agent-1"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if captured.JSONRPC != "2.0" || captured.Method != "agent.get" {
		t.Fatalf("request jsonrpc/method = %s/%s, want 2.0/agent.get", captured.JSONRPC, captured.Method)
	}
	if !reflect.DeepEqual(captured.Params, map[string]any{"agent_id": "agent-1"}) {
		t.Fatalf("params = %#v, want agent id", captured.Params)
	}
	for _, want := range []string{
		"Agent agent-1",
		"role=reviewer type=worker status=running model=default mode=task session_scope=",
		"current_session_ref: session_id=session-1 started_at=2026-05-18T03:00:00Z",
		"last_turn_ref: turn_id=turn-1 completed_at=2026-05-18T03:05:00Z parse_ok=true error=-",
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
	for _, notWant := range []string{"transcript", "conversation.current_for_agent", "conversation.get"} {
		if strings.Contains(stdout.String(), notWant) {
			t.Fatalf("stdout contains split concept %q:\n%s", notWant, stdout.String())
		}
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestAgentReadCommandsRejectInvalidInputBeforeRequest(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeJSONRPCResult(t, w, "unexpected", map[string]any{})
	}))
	defer server.Close()

	for _, tc := range []struct {
		name       string
		args       []string
		wantStderr string
	}{
		{name: "agents list extra arg", args: []string{"agent", "list", "extra"}, wantStderr: "unknown command"},
		{name: "agents list unsupported flag", args: []string{"agent", "list", "--unknown"}, wantStderr: "unknown flag"},
		{name: "agent view missing id", args: []string{"agent", "view"}, wantStderr: "requires <agent-id>"},
		{name: "agent view blank id", args: []string{"agent", "view", "  "}, wantStderr: "agent id is required"},
		{name: "agent view extra arg", args: []string{"agent", "view", "agent-1", "extra"}, wantStderr: "accepts one argument"},
		{name: "agent view unsupported flag", args: []string{"agent", "view", "agent-1", "--unknown"}, wantStderr: "unknown flag"},
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

func TestAgentReadCommandsFailClosedWithoutToken(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeJSONRPCResult(t, w, "unexpected", map[string]any{})
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"agent", "list"}, &stdout, &stderr, testRootCommandOptions(server))
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

func TestAgentReadCommandsFailClosedOnRPCAndMalformedResponses(t *testing.T) {
	for _, tc := range []struct {
		name       string
		args       []string
		handler    http.HandlerFunc
		wantCode   int
		wantStderr string
	}{
		{
			name: "list http auth failure",
			args: []string{"agent", "list"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
			},
			wantCode:   4,
			wantStderr: "rejected the request with status 401",
		},
		{
			name: "list missing agents",
			args: []string{"agent", "list"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeJSONRPCResult(t, w, req.ID, map[string]any{})
			},
			wantCode:   3,
			wantStderr: "malformed agent.list result: agents is required",
		},
		{
			name: "list malformed agent",
			args: []string{"agent", "list"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				agent := agentSummaryResult("agent-1", "researcher", "running")
				delete(agent, "status")
				writeJSONRPCResult(t, w, req.ID, map[string]any{"agents": []map[string]any{agent}})
			},
			wantCode:   3,
			wantStderr: "malformed agent.list result: agents[0]: status is required",
		},
		{
			name: "view missing agent",
			args: []string{"agent", "view", "agent-1"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeJSONRPCResult(t, w, req.ID, map[string]any{})
			},
			wantCode:   3,
			wantStderr: "malformed agent.get result: agent: agent_id is required",
		},
		{
			name: "view agent not found",
			args: []string{"agent", "view", "missing"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeAgentJSONRPCError(t, w, req.ID, "AGENT_NOT_FOUND")
			},
			wantCode:   5,
			wantStderr: "AGENT_NOT_FOUND: Application error: AGENT_NOT_FOUND",
		},
		{
			name: "view malformed ref",
			args: []string{"agent", "view", "agent-1"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeJSONRPCResult(t, w, req.ID, map[string]any{
					"agent":               agentSummaryResult("agent-1", "researcher", "running"),
					"current_session_ref": map[string]any{"session_id": "session-1"},
				})
			},
			wantCode:   3,
			wantStderr: "malformed agent.get result: current_session_ref.started_at is required",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setCLIAPITestToken(t, "test-token")
			server := httptest.NewServer(tc.handler)
			defer server.Close()

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), tc.args, &stdout, &stderr, testRootCommandOptions(server))
			if code != tc.wantCode {
				t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, tc.wantCode, stdout.String(), stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.wantStderr) {
				t.Fatalf("stderr = %q, want substring %q", stderr.String(), tc.wantStderr)
			}
		})
	}
}

func agentSummaryResult(agentID, role, status string) map[string]any {
	return map[string]any{
		"agent_id": agentID,
		"role":     role,
		"type":     "worker",
		"model":    "default",
		"mode":     "task",
		"status":   status,
	}
}

func writeAgentJSONRPCError(t *testing.T, w http.ResponseWriter, id string, code string) {
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
				"details":        map[string]any{"agent_id": "missing"},
				"retryable":      false,
				"correlation_id": "corr-agent",
			},
		},
	}); err != nil {
		t.Fatalf("encode response: %v", err)
	}
}
