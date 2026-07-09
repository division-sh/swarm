package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestAgentDeliveriesUsesAgentDeliveryLifecycleAndRendersRows(t *testing.T) {
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
		writeJSONRPCResult(t, w, captured.ID, validAgentDeliveryLifecycleResult())
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{
		"agent", "deliveries", " agent-1 ",
		"--run-id", "run-1",
		"--delivery-status", "pending",
		"--delivery-status", "delivered",
		"--limit", "2",
		"--cursor", "cursor-1",
	}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if captured.JSONRPC != "2.0" || captured.Method != "agent.delivery_lifecycle" {
		t.Fatalf("request jsonrpc/method = %s/%s, want 2.0/agent.delivery_lifecycle", captured.JSONRPC, captured.Method)
	}
	assertAgentDeliveriesParams(t, captured.Params, map[string]any{
		"agent_id":        "agent-1",
		"run_id":          "run-1",
		"delivery_status": []string{"pending", "delivered"},
		"limit":           float64(2),
		"cursor":          "cursor-1",
	})
	for _, want := range []string{
		"Agent agent-1 deliveries",
		"DELIVERY_ID",
		"delivery-1   platform.agent_directive",
		"delivery-2   platform.agent_followup",
		"queued       waiting",
		"next_cursor=cursor-2",
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
	for _, notWant := range []string{"run.trace", "event.get", "agent.diagnose", "agent.delivery_diagnostics", "delivery_target_route", "dead_letters", "retry_policy"} {
		if strings.Contains(stdout.String(), notWant) {
			t.Fatalf("stdout contains split or forbidden concept %q:\n%s", notWant, stdout.String())
		}
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestAgentDeliveriesEmptyResult(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var captured jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Errorf("decode request: %v", err)
		}
		writeJSONRPCResult(t, w, captured.ID, map[string]any{"agent_id": "agent-1", "deliveries": []any{}})
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"agent", "deliveries", "agent-1"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if captured.Method != "agent.delivery_lifecycle" {
		t.Fatalf("method = %q, want agent.delivery_lifecycle", captured.Method)
	}
	assertAgentDeliveriesParams(t, captured.Params, map[string]any{"agent_id": "agent-1"})
	if !strings.Contains(stdout.String(), "No deliveries match the current filters.") {
		t.Fatalf("stdout = %q, want empty result message", stdout.String())
	}
	if strings.Contains(stdout.String(), "next_cursor=") {
		t.Fatalf("stdout rendered absent cursor:\n%s", stdout.String())
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestAgentDeliveriesJSONPreservesAPIResultShape(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var captured jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Errorf("decode request: %v", err)
		}
		result := validAgentDeliveryLifecycleResult()
		result["forbidden_local_field"] = "dropped"
		writeJSONRPCResult(t, w, captured.ID, result)
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"agent", "deliveries", "agent-1", "--json"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if captured.Method != "agent.delivery_lifecycle" {
		t.Fatalf("method = %q, want agent.delivery_lifecycle", captured.Method)
	}
	var decoded map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &decoded); err != nil {
		t.Fatalf("decode stdout JSON: %v\n%s", err, stdout.String())
	}
	if decoded["agent_id"] != "agent-1" {
		t.Fatalf("agent_id = %#v, want agent-1", decoded["agent_id"])
	}
	deliveries, ok := decoded["deliveries"].([]any)
	if !ok || len(deliveries) != 2 {
		t.Fatalf("deliveries = %#v, want two rows", decoded["deliveries"])
	}
	if decoded["next_cursor"] != "cursor-2" {
		t.Fatalf("next_cursor = %#v, want cursor-2", decoded["next_cursor"])
	}
	for _, wrapper := range []string{"agent", "delivery_lifecycle", "forbidden_local_field"} {
		if _, ok := decoded[wrapper]; ok {
			t.Fatalf("json output contains wrapper or non-owner field %q: %#v", wrapper, decoded)
		}
	}
	if row, ok := deliveries[0].(map[string]any); !ok || row["delivery_id"] != "delivery-1" || row["retry_count"] != float64(1) {
		t.Fatalf("first row = %#v", deliveries[0])
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestAgentDeliveriesRejectsInvalidInputBeforeRequest(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeJSONRPCResult(t, w, "unexpected", validAgentDeliveryLifecycleResult())
	}))
	defer server.Close()

	for _, tc := range []struct {
		name       string
		args       []string
		wantStderr string
	}{
		{name: "missing id", args: []string{"agent", "deliveries"}, wantStderr: "requires <agent-id>"},
		{name: "blank id", args: []string{"agent", "deliveries", "  "}, wantStderr: "agent id is required"},
		{name: "invalid id", args: []string{"agent", "deliveries", "bad id!"}, wantStderr: "agent id must match OpaqueId pattern"},
		{name: "extra arg", args: []string{"agent", "deliveries", "agent-1", "extra"}, wantStderr: "accepts one argument"},
		{name: "unsupported flag", args: []string{"agent", "deliveries", "agent-1", "--unknown"}, wantStderr: "unknown flag"},
		{name: "limit too small", args: []string{"agent", "deliveries", "agent-1", "--limit", "0"}, wantStderr: "--limit must be between 1 and 200"},
		{name: "limit too large", args: []string{"agent", "deliveries", "agent-1", "--limit", "201"}, wantStderr: "--limit must be between 1 and 200"},
		{name: "blank cursor", args: []string{"agent", "deliveries", "agent-1", "--cursor", ""}, wantStderr: "--cursor is required when provided"},
		{name: "blank run id", args: []string{"agent", "deliveries", "agent-1", "--run-id", ""}, wantStderr: "--run-id is required when provided"},
		{name: "invalid run id", args: []string{"agent", "deliveries", "agent-1", "--run-id", "bad id!"}, wantStderr: "--run-id must match OpaqueId pattern"},
		{name: "blank status", args: []string{"agent", "deliveries", "agent-1", "--delivery-status", ""}, wantStderr: "--delivery-status must not be empty"},
		{name: "invalid status", args: []string{"agent", "deliveries", "agent-1", "--delivery-status", "done"}, wantStderr: "--delivery-status must be one of"},
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

func TestAgentDeliveriesFailClosedWithoutToken(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeJSONRPCResult(t, w, "unexpected", validAgentDeliveryLifecycleResult())
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"agent", "deliveries", "agent-1"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != cliExitAuth {
		t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, cliExitAuth, stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), "API token source is required") {
		t.Fatalf("stderr = %q, want token failure", stderr.String())
	}
	if calls.Load() != 0 {
		t.Fatalf("RPC calls = %d, want 0", calls.Load())
	}
}

func TestAgentDeliveriesFailClosedOnRPCAndMalformedResponses(t *testing.T) {
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
				writeAgentInvalidParamsJSONRPCError(t, w, req.ID, "Invalid params: delivery_status")
			},
			wantCode:   cliExitRuntime,
			wantStderr: "Invalid params: delivery_status",
		},
		{
			name: "missing deliveries",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeJSONRPCResult(t, w, req.ID, map[string]any{"agent_id": "agent-1"})
			},
			wantCode:   cliExitRuntime,
			wantStderr: "malformed agent.delivery_lifecycle result: deliveries is required",
		},
		{
			name: "missing delivery id",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				result := validAgentDeliveryLifecycleResult()
				row := result["deliveries"].([]map[string]any)[0]
				delete(row, "delivery_id")
				writeJSONRPCResult(t, w, req.ID, result)
			},
			wantCode:   cliExitRuntime,
			wantStderr: "malformed agent.delivery_lifecycle result: deliveries[0].delivery_id is required",
		},
		{
			name: "missing retry count",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				result := validAgentDeliveryLifecycleResult()
				row := result["deliveries"].([]map[string]any)[0]
				delete(row, "retry_count")
				writeJSONRPCResult(t, w, req.ID, result)
			},
			wantCode:   cliExitRuntime,
			wantStderr: "malformed agent.delivery_lifecycle result: deliveries[0].retry_count is required",
		},
		{
			name: "negative retry count",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				result := validAgentDeliveryLifecycleResult()
				row := result["deliveries"].([]map[string]any)[0]
				row["retry_count"] = -1
				writeJSONRPCResult(t, w, req.ID, result)
			},
			wantCode:   cliExitRuntime,
			wantStderr: "malformed agent.delivery_lifecycle result: deliveries[0].retry_count must be non-negative",
		},
		{
			name: "invalid timestamp",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				result := validAgentDeliveryLifecycleResult()
				row := result["deliveries"].([]map[string]any)[0]
				row["delivery_created_at"] = "not-time"
				writeJSONRPCResult(t, w, req.ID, result)
			},
			wantCode:   cliExitRuntime,
			wantStderr: "malformed agent.delivery_lifecycle result: deliveries[0].delivery_created_at must be an RFC3339 timestamp",
		},
		{
			name: "empty next cursor",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				result := validAgentDeliveryLifecycleResult()
				result["next_cursor"] = ""
				writeJSONRPCResult(t, w, req.ID, result)
			},
			wantCode:   cliExitRuntime,
			wantStderr: "malformed agent.delivery_lifecycle result: next_cursor is empty",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setCLIAPITestToken(t, "test-token")
			server := httptest.NewServer(tc.handler)
			defer server.Close()

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"agent", "deliveries", "agent-1"}, &stdout, &stderr, testRootCommandOptions(server))
			if code != tc.wantCode {
				t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, tc.wantCode, stdout.String(), stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.wantStderr) {
				t.Fatalf("stderr = %q, want substring %q", stderr.String(), tc.wantStderr)
			}
		})
	}
}

func validAgentDeliveryLifecycleResult() map[string]any {
	return map[string]any{
		"agent_id": "agent-1",
		"deliveries": []map[string]any{
			{
				"delivery_id":           "delivery-1",
				"event_id":              "event-1",
				"event_name":            "platform.agent_directive",
				"run_id":                "run-1",
				"entity_id":             "entity-1",
				"status":                "delivered",
				"retry_count":           1,
				"delivery_created_at":   "2026-05-18T03:01:00Z",
				"delivery_started_at":   "2026-05-18T03:02:00Z",
				"delivery_delivered_at": "2026-05-18T03:03:00Z",
			},
			{
				"delivery_id":         "delivery-2",
				"event_id":            "event-2",
				"event_name":          "platform.agent_followup",
				"status":              "pending",
				"retry_count":         0,
				"reason_code":         "queued",
				"last_error":          "waiting",
				"delivery_created_at": "2026-05-18T03:04:00Z",
			},
		},
		"next_cursor": "cursor-2",
	}
}

func assertAgentDeliveriesParams(t *testing.T, got map[string]any, want map[string]any) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("params = %#v, want %#v", got, want)
	}
	for key, wantValue := range want {
		gotValue, ok := got[key]
		if !ok {
			t.Fatalf("params missing %q: %#v", key, got)
		}
		switch wantTyped := wantValue.(type) {
		case []string:
			gotList, ok := gotValue.([]any)
			if !ok || len(gotList) != len(wantTyped) {
				t.Fatalf("params[%s] = %#v, want %#v", key, gotValue, wantTyped)
			}
			for i, item := range wantTyped {
				if gotList[i] != item {
					t.Fatalf("params[%s][%d] = %#v, want %q", key, i, gotList[i], item)
				}
			}
		default:
			if gotValue != wantValue {
				t.Fatalf("params[%s] = %#v, want %#v", key, gotValue, wantValue)
			}
		}
	}
}
