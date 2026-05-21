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

func TestEntitiesListUsesEntityListV1RPCWithFilters(t *testing.T) {
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
			"entities":    []map[string]any{validEntitySummary("entity-1")},
			"next_cursor": "entity-cursor-2",
		})
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{
		"entities", "list",
		"--run-id", "run-1",
		"--flow", "flows/review",
		"--type", "vertical",
		"--current-state", "collecting",
		"--limit", "25",
		"--cursor", "cursor-1",
	}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if captured.JSONRPC != "2.0" || captured.Method != entityListMethod {
		t.Fatalf("request jsonrpc/method = %s/%s, want 2.0/%s", captured.JSONRPC, captured.Method, entityListMethod)
	}
	wantParams := map[string]any{
		"run_id":        "run-1",
		"flow":          "flows/review",
		"type":          "vertical",
		"current_state": "collecting",
		"limit":         float64(25),
		"cursor":        "cursor-1",
	}
	if !reflect.DeepEqual(captured.Params, wantParams) {
		t.Fatalf("params = %#v, want %#v", captured.Params, wantParams)
	}
	for _, want := range []string{"ENTITY_ID", "RUN_ID", "entity-1", "run-1", "flows/review", "vertical", "collecting", "next_cursor=entity-cursor-2"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestEntitiesListEmptyResultOmitsUnsetParams(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
	var captured jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Errorf("decode request: %v", err)
		}
		writeJSONRPCResult(t, w, captured.ID, map[string]any{"entities": []any{}})
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"entities", "list"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if captured.Method != entityListMethod {
		t.Fatalf("method = %q, want %s", captured.Method, entityListMethod)
	}
	if len(captured.Params) != 0 {
		t.Fatalf("params = %#v, want empty", captured.Params)
	}
	if !strings.Contains(stdout.String(), "No entities match the filter.") {
		t.Fatalf("stdout = %q, want empty-state text", stdout.String())
	}
}

func TestEntityViewUsesEntityGetAndRendersEntityNativeDetail(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
	var captured jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Errorf("Authorization = %q, want bearer token", got)
		}
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Errorf("decode request: %v", err)
		}
		writeJSONRPCResult(t, w, captured.ID, validEntityFullResult("entity-1"))
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"entity", "view", "entity-1", "--run-id", "run-1"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if captured.JSONRPC != "2.0" || captured.Method != entityGetMethod {
		t.Fatalf("request jsonrpc/method = %s/%s, want 2.0/%s", captured.JSONRPC, captured.Method, entityGetMethod)
	}
	if !reflect.DeepEqual(captured.Params, map[string]any{"entity_id": "entity-1", "run_id": "run-1"}) {
		t.Fatalf("params = %#v, want entity_id/run_id", captured.Params)
	}
	for _, want := range []string{
		"Entity entity-1",
		"run_id=run-1 flow=flows/review type=vertical state=collecting revision=3",
		"slug=vertical-1 name=Vertical One",
		`fields={"score":7}`,
		`gates={"ready":true}`,
		`accumulated={"notes":["a"]}`,
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestEntityAggregateUsesEntityAggregateDefaultsAndFilters(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
	var captured []jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req jsonRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
		}
		captured = append(captured, req)
		writeJSONRPCResult(t, w, req.ID, map[string]any{"counts": map[string]any{"collecting": 2, "reviewing": 1}})
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"entity", "aggregate"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("default code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if captured[0].Method != entityAggregateMethod {
		t.Fatalf("method = %q, want %s", captured[0].Method, entityAggregateMethod)
	}
	if len(captured[0].Params) != 0 {
		t.Fatalf("default params = %#v, want empty for server-owned default group", captured[0].Params)
	}
	if !strings.Contains(stdout.String(), "GROUP\tCOUNT") || !strings.Contains(stdout.String(), "collecting\t2") {
		t.Fatalf("stdout = %q, want aggregate table", stdout.String())
	}

	stdout.Reset()
	stderr.Reset()
	code = executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{
		"entity", "aggregate",
		"--run-id", "run-1",
		"--group-by", "fields.priority",
		"--type", "vertical",
	}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("filtered code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	wantParams := map[string]any{"run_id": "run-1", "group_by": "fields.priority", "type": "vertical"}
	if !reflect.DeepEqual(captured[1].Params, wantParams) {
		t.Fatalf("filtered params = %#v, want %#v", captured[1].Params, wantParams)
	}
}

func TestEntityCommandsRejectInvalidInputBeforeRequest(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeJSONRPCResult(t, w, "unexpected", map[string]any{"entities": []any{}})
	}))
	defer server.Close()

	for _, tc := range []struct {
		name       string
		args       []string
		wantStderr string
	}{
		{name: "list invalid limit low", args: []string{"entities", "list", "--limit", "0"}, wantStderr: "--limit must be between 1 and 500"},
		{name: "list invalid run id", args: []string{"entities", "list", "--run-id", "bad id!"}, wantStderr: "--run-id must match OpaqueId pattern"},
		{name: "list blank flow", args: []string{"entities", "list", "--flow", " "}, wantStderr: "--flow must not be empty"},
		{name: "view missing id", args: []string{"entity", "view"}, wantStderr: "accepts 1 arg(s)"},
		{name: "view blank id", args: []string{"entity", "view", " "}, wantStderr: "entity id is required"},
		{name: "view invalid id", args: []string{"entity", "view", "bad id!"}, wantStderr: "entity id must match OpaqueId pattern"},
		{name: "view invalid run id", args: []string{"entity", "view", "entity-1", "--run-id", "bad id!"}, wantStderr: "--run-id must match OpaqueId pattern"},
		{name: "aggregate invalid group", args: []string{"entity", "aggregate", "--group-by", "bad field"}, wantStderr: "--group-by must be one of"},
		{name: "aggregate blank type", args: []string{"entity", "aggregate", "--type", " "}, wantStderr: "--type must not be empty"},
		{name: "aggregate extra arg", args: []string{"entity", "aggregate", "extra"}, wantStderr: "unknown command"},
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

func TestEntityCommandsFailClosedWithoutTokenBeforeRequest(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeJSONRPCResult(t, w, "unexpected", map[string]any{"entities": []any{}})
	}))
	defer server.Close()

	for _, args := range [][]string{
		{"entities", "list"},
		{"entity", "view", "entity-1"},
		{"entity", "aggregate"},
	} {
		var stdout, stderr bytes.Buffer
		code := executeRootCommandWithOptions(context.Background(), t.TempDir(), args, &stdout, &stderr, testRootCommandOptions(server))
		if code != 4 {
			t.Fatalf("%v code = %d, want 4 stdout=%s stderr=%s", args, code, stdout.String(), stderr.String())
		}
		if !strings.Contains(stderr.String(), "SWARM_API_TOKEN is required") {
			t.Fatalf("%v stderr = %q, want token failure", args, stderr.String())
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("RPC calls = %d, want 0", calls.Load())
	}
}

func TestEntityCommandsMapRuntimeFailuresAndMalformedResults(t *testing.T) {
	for _, tc := range []struct {
		name       string
		args       []string
		handler    http.HandlerFunc
		wantCode   int
		wantStderr string
	}{
		{
			name: "list http auth exits four",
			args: []string{"entities", "list"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
			},
			wantCode:   4,
			wantStderr: "v1 RPC HTTP 401",
		},
		{
			name: "list missing entities exits three",
			args: []string{"entities", "list"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeJSONRPCResult(t, w, req.ID, map[string]any{})
			},
			wantCode:   3,
			wantStderr: "entities is required",
		},
		{
			name: "list malformed entity exits three",
			args: []string{"entities", "list"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				entity := validEntitySummary("entity-1")
				delete(entity, "current_state")
				writeJSONRPCResult(t, w, req.ID, map[string]any{"entities": []map[string]any{entity}})
			},
			wantCode:   3,
			wantStderr: "current_state is required",
		},
		{
			name: "view entity not found exits five",
			args: []string{"entity", "view", "missing"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeEntityJSONRPCError(t, w, req.ID, "ENTITY_NOT_FOUND")
			},
			wantCode:   5,
			wantStderr: "ENTITY_NOT_FOUND",
		},
		{
			name: "view missing fields exits three",
			args: []string{"entity", "view", "entity-1"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				result := validEntityFullResult("entity-1")
				delete(result, "fields")
				writeJSONRPCResult(t, w, req.ID, result)
			},
			wantCode:   3,
			wantStderr: "fields is required",
		},
		{
			name: "aggregate unknown rpc exits three",
			args: []string{"entity", "aggregate"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeEntityJSONRPCError(t, w, req.ID, "METHOD_UNAVAILABLE")
			},
			wantCode:   3,
			wantStderr: "METHOD_UNAVAILABLE",
		},
		{
			name: "aggregate missing counts exits three",
			args: []string{"entity", "aggregate"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeJSONRPCResult(t, w, req.ID, map[string]any{})
			},
			wantCode:   3,
			wantStderr: "counts is required",
		},
		{
			name: "aggregate negative count exits three",
			args: []string{"entity", "aggregate"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeJSONRPCResult(t, w, req.ID, map[string]any{"counts": map[string]any{"collecting": -1}})
			},
			wantCode:   3,
			wantStderr: "counts[\"collecting\"] must be non-negative",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("SWARM_API_TOKEN", "test-token")
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

func validEntitySummary(entityID string) map[string]any {
	return map[string]any{
		"entity_id":     entityID,
		"run_id":        "run-1",
		"flow_instance": "flows/review",
		"entity_type":   "vertical",
		"current_state": "collecting",
		"slug":          "vertical-1",
		"name":          "Vertical One",
		"revision":      3,
		"created_at":    "2026-05-20T01:00:00Z",
		"updated_at":    "2026-05-20T01:05:00Z",
	}
}

func validEntityFullResult(entityID string) map[string]any {
	return map[string]any{
		"entity":      validEntitySummary(entityID),
		"fields":      map[string]any{"score": 7},
		"gates":       map[string]any{"ready": true},
		"accumulated": map[string]any{"notes": []any{"a"}},
	}
}

func writeEntityJSONRPCError(t *testing.T, w http.ResponseWriter, id string, code string) {
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
