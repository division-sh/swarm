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

func TestIncidentsUsesRuntimeIncidentsV1RPCWithFilters(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
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
			"incidents":   []any{validRuntimeIncident("incident-1")},
			"next_cursor": "incident-cursor-2",
		})
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{
		"incidents",
		"--since-hours", "48",
		"--component", "mcp.tools",
		"--level", "WARN",
		"--mcp-only",
		"--limit", "25",
		"--cursor", "cursor-1",
	}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if captured.Method != runtimeIncidentsMethod {
		t.Fatalf("method = %q, want %s", captured.Method, runtimeIncidentsMethod)
	}
	wantParams := map[string]any{
		"since_hours": float64(48),
		"component":   "mcp.tools",
		"level":       "warn",
		"mcp_only":    true,
		"limit":       float64(25),
		"cursor":      "cursor-1",
	}
	if !reflect.DeepEqual(captured.Params, wantParams) {
		t.Fatalf("params = %#v, want %#v", captured.Params, wantParams)
	}
	for _, want := range []string{"LAST SEEN", "incident-1", "mcp.tools", "WARN_DELIVERY_FAILED", "sample incident", "next_cursor=incident-cursor-2"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestIncidentsOmitOptionalParamsWhenUnset(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
	var captured jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Errorf("decode request: %v", err)
		}
		writeJSONRPCResult(t, w, captured.ID, map[string]any{"incidents": []any{}})
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"incidents"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if captured.Method != runtimeIncidentsMethod {
		t.Fatalf("method = %q, want %s", captured.Method, runtimeIncidentsMethod)
	}
	if len(captured.Params) != 0 {
		t.Fatalf("params = %#v, want empty map", captured.Params)
	}
	if !strings.Contains(stdout.String(), "No runtime incidents match the filter.") {
		t.Fatalf("stdout = %q, want empty-state text", stdout.String())
	}
}

func TestIncidentsRejectInvalidInputBeforeRequest(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeJSONRPCResult(t, w, "unexpected", map[string]any{"incidents": []any{}})
	}))
	defer server.Close()

	for _, tc := range []struct {
		name       string
		args       []string
		wantStderr string
	}{
		{name: "invalid since hours low", args: []string{"incidents", "--since-hours", "0"}, wantStderr: "--since-hours must be between 1 and 720"},
		{name: "invalid since hours high", args: []string{"incidents", "--since-hours", "721"}, wantStderr: "--since-hours must be between 1 and 720"},
		{name: "invalid level", args: []string{"incidents", "--level", "fatal"}, wantStderr: "--level must be one of"},
		{name: "invalid limit low", args: []string{"incidents", "--limit", "0"}, wantStderr: "--limit must be between 1 and 500"},
		{name: "invalid limit high", args: []string{"incidents", "--limit", "501"}, wantStderr: "--limit must be between 1 and 500"},
		{name: "unexpected arg", args: []string{"incidents", "incident-1"}, wantStderr: "unknown command"},
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
				t.Fatalf("calls = %d, want 0", calls.Load())
			}
		})
	}
}

func TestIncidentsFailClosedWithoutTokenBeforeRequest(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeJSONRPCResult(t, w, "unexpected", map[string]any{"incidents": []any{}})
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"incidents"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 4 {
		t.Fatalf("code = %d, want 4 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), "SWARM_API_TOKEN is required") {
		t.Fatalf("stderr = %q, want token failure", stderr.String())
	}
	if calls.Load() != 0 {
		t.Fatalf("calls = %d, want 0", calls.Load())
	}
}

func TestIncidentsMapRuntimeFailuresAndMalformedResults(t *testing.T) {
	for _, tc := range []struct {
		name       string
		handler    http.HandlerFunc
		wantCode   int
		wantStderr string
	}{
		{
			name: "http auth exits four",
			handler: func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
			},
			wantCode:   4,
			wantStderr: "v1 RPC HTTP 401",
		},
		{
			name: "http runtime exits three",
			handler: func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "unavailable", http.StatusServiceUnavailable)
			},
			wantCode:   3,
			wantStderr: "v1 RPC HTTP 503",
		},
		{
			name: "unknown rpc error exits three",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeRuntimeIncidentJSONRPCError(t, w, req.ID, "METHOD_UNAVAILABLE")
			},
			wantCode:   3,
			wantStderr: "METHOD_UNAVAILABLE",
		},
		{
			name: "unauthorized rpc exits four",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeRuntimeIncidentJSONRPCError(t, w, req.ID, "UNAUTHORIZED")
			},
			wantCode:   4,
			wantStderr: "UNAUTHORIZED",
		},
		{
			name: "malformed list missing incidents exits three",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeJSONRPCResult(t, w, req.ID, map[string]any{})
			},
			wantCode:   3,
			wantStderr: "incidents is required",
		},
		{
			name: "malformed incident missing id exits three",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				incident := validRuntimeIncident("incident-1")
				delete(incident, "incident_id")
				writeJSONRPCResult(t, w, req.ID, map[string]any{"incidents": []any{incident}})
			},
			wantCode:   3,
			wantStderr: "incident_id is required",
		},
		{
			name: "malformed incident invalid timestamp exits three",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				incident := validRuntimeIncident("incident-1")
				incident["last_seen"] = "not-time"
				writeJSONRPCResult(t, w, req.ID, map[string]any{"incidents": []any{incident}})
			},
			wantCode:   3,
			wantStderr: "last_seen must be an RFC3339 timestamp",
		},
		{
			name: "malformed incident invalid opaque incident id exits three",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				incident := validRuntimeIncident("bad id!")
				writeJSONRPCResult(t, w, req.ID, map[string]any{"incidents": []any{incident}})
			},
			wantCode:   3,
			wantStderr: "incident_id must match OpaqueId pattern",
		},
		{
			name: "malformed incident too long incident id exits three",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				incident := validRuntimeIncident(strings.Repeat("a", 257))
				writeJSONRPCResult(t, w, req.ID, map[string]any{"incidents": []any{incident}})
			},
			wantCode:   3,
			wantStderr: "incident_id must be at most 256 characters",
		},
		{
			name: "malformed incident invalid count exits three",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				incident := validRuntimeIncident("incident-1")
				incident["count"] = 0
				writeJSONRPCResult(t, w, req.ID, map[string]any{"incidents": []any{incident}})
			},
			wantCode:   3,
			wantStderr: "count must be at least 1",
		},
		{
			name: "malformed incident invalid level exits three",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				incident := validRuntimeIncident("incident-1")
				incident["level"] = "fatal"
				writeJSONRPCResult(t, w, req.ID, map[string]any{"incidents": []any{incident}})
			},
			wantCode:   3,
			wantStderr: "not a valid LogLevel",
		},
		{
			name: "malformed incident missing sample message exits three",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				incident := validRuntimeIncident("incident-1")
				delete(incident, "sample_message")
				writeJSONRPCResult(t, w, req.ID, map[string]any{"incidents": []any{incident}})
			},
			wantCode:   3,
			wantStderr: "sample_message is required",
		},
		{
			name: "malformed incident missing sample log ids exits three",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				incident := validRuntimeIncident("incident-1")
				delete(incident, "sample_log_ids")
				writeJSONRPCResult(t, w, req.ID, map[string]any{"incidents": []any{incident}})
			},
			wantCode:   3,
			wantStderr: "sample_log_ids is required",
		},
		{
			name: "malformed incident invalid sample log id exits three",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				incident := validRuntimeIncident("incident-1")
				incident["sample_log_ids"] = []string{"log-1", "bad log id!"}
				writeJSONRPCResult(t, w, req.ID, map[string]any{"incidents": []any{incident}})
			},
			wantCode:   3,
			wantStderr: "sample_log_ids[1] must match OpaqueId pattern",
		},
		{
			name: "malformed incident empty sample log id exits three",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				incident := validRuntimeIncident("incident-1")
				incident["sample_log_ids"] = []string{"log-1", ""}
				writeJSONRPCResult(t, w, req.ID, map[string]any{"incidents": []any{incident}})
			},
			wantCode:   3,
			wantStderr: "sample_log_ids[1] must not be empty",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("SWARM_API_TOKEN", "test-token")
			server := httptest.NewServer(tc.handler)
			defer server.Close()

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"incidents"}, &stdout, &stderr, testRootCommandOptions(server))
			if code != tc.wantCode {
				t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, tc.wantCode, stdout.String(), stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.wantStderr) {
				t.Fatalf("stderr = %q, want substring %q", stderr.String(), tc.wantStderr)
			}
		})
	}
}

func validRuntimeIncident(incidentID string) map[string]any {
	return map[string]any{
		"incident_id":    incidentID,
		"first_seen":     "2026-05-19T09:00:00Z",
		"last_seen":      "2026-05-19T10:00:00Z",
		"count":          3,
		"level":          "warn",
		"component":      "mcp.tools",
		"error_code":     "WARN_DELIVERY_FAILED",
		"sample_message": "sample incident",
		"sample_log_ids": []string{"log-1", "log-2"},
	}
}

func writeRuntimeIncidentJSONRPCError(t *testing.T, w http.ResponseWriter, id string, code string) {
	t.Helper()
	w.Header().Set("content-type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"error": map[string]any{
			"code":    -32000,
			"message": code,
			"data": map[string]any{
				"code": code,
			},
		},
	}); err != nil {
		t.Fatalf("encode error response: %v", err)
	}
}
