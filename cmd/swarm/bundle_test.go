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

func TestBundleCommandsUseCanonicalRPCAndRender(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
	bundleHash := validBundleHash("a")
	var requests []jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/rpc" {
			t.Errorf("path = %q, want /v1/rpc", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Errorf("Authorization = %q, want bearer token", got)
		}
		var req jsonRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
		}
		requests = append(requests, req)
		switch req.Method {
		case bundleListMethod:
			writeJSONRPCResult(t, w, req.ID, map[string]any{
				"bundles":     []map[string]any{validBundleSummary(bundleHash)},
				"next_cursor": "bundle-cursor-2",
			})
		case bundleGetMethod:
			writeJSONRPCResult(t, w, req.ID, validBundleDetail(bundleHash))
		case bundleAgentsMethod:
			writeJSONRPCResult(t, w, req.ID, map[string]any{
				"agents": []map[string]any{validBundleAgent("agent-alpha")},
			})
		default:
			t.Errorf("unexpected method %q", req.Method)
			writeJSONRPCResult(t, w, req.ID, map[string]any{})
		}
	}))
	defer server.Close()

	commands := []struct {
		args       []string
		wantStdout []string
	}{
		{args: []string{"bundle", "list", "--limit", "2", "--cursor", "bundle-cursor-1"}, wantStdout: []string{"bundle " + bundleHash, "agents=2", "next_cursor=bundle-cursor-2"}},
		{args: []string{"bundle", "show", bundleHash}, wantStdout: []string{"Bundle " + bundleHash, "content_yaml:", "agents:"}},
		{args: []string{"bundle", "agents", bundleHash}, wantStdout: []string{"agent agent-alpha", "role=researcher", "subscriptions=scan.requested"}},
	}
	for _, command := range commands {
		var stdout, stderr bytes.Buffer
		code := executeRootCommandWithOptions(context.Background(), t.TempDir(), command.args, &stdout, &stderr, testRootCommandOptions(server))
		if code != 0 {
			t.Fatalf("%v code = %d stderr=%s stdout=%s", command.args, code, stderr.String(), stdout.String())
		}
		for _, want := range command.wantStdout {
			if !strings.Contains(stdout.String(), want) {
				t.Fatalf("%v stdout missing %q:\n%s", command.args, want, stdout.String())
			}
		}
		if strings.TrimSpace(stderr.String()) != "" {
			t.Fatalf("%v stderr = %q, want empty", command.args, stderr.String())
		}
	}

	if len(requests) != 3 {
		t.Fatalf("requests = %d, want 3", len(requests))
	}
	assertBundleRequest(t, requests[0], bundleListMethod, map[string]any{"limit": float64(2), "cursor": "bundle-cursor-1"})
	assertBundleRequest(t, requests[1], bundleGetMethod, map[string]any{"bundle_hash": bundleHash})
	assertBundleRequest(t, requests[2], bundleAgentsMethod, map[string]any{"bundle_hash": bundleHash})
}

func TestBundleCommandsJSONPreserveAPIShape(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
	bundleHash := validBundleHash("b")
	var captured jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Errorf("decode request: %v", err)
		}
		writeJSONRPCResult(t, w, captured.ID, validBundleDetail(bundleHash))
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"bundle", "show", bundleHash, "--json"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	assertBundleRequest(t, captured, bundleGetMethod, map[string]any{"bundle_hash": bundleHash})
	var decoded map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &decoded); err != nil {
		t.Fatalf("decode stdout json: %v\n%s", err, stdout.String())
	}
	if decoded["bundle_hash"] != bundleHash || decoded["content_yaml"] == "" {
		t.Fatalf("json bundle detail = %#v", decoded)
	}
	if _, ok := decoded["parsed_json"].(map[string]any); !ok {
		t.Fatalf("json parsed_json = %#v, want object", decoded["parsed_json"])
	}
	for _, wrapper := range []string{"bundle", "detail"} {
		if _, ok := decoded[wrapper]; ok {
			t.Fatalf("json output contains CLI wrapper %q: %#v", wrapper, decoded)
		}
	}
}

func TestBundleCommandsRejectInvalidInputBeforeRequest(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
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
		{name: "list limit low", args: []string{"bundle", "list", "--limit", "0"}, wantStderr: "--limit must be between 1 and 500"},
		{name: "list limit high", args: []string{"bundle", "list", "--limit", "501"}, wantStderr: "--limit must be between 1 and 500"},
		{name: "blank cursor", args: []string{"bundle", "list", "--cursor", " "}, wantStderr: "--cursor must be non-empty"},
		{name: "show missing hash", args: []string{"bundle", "show"}, wantStderr: "accepts 1 arg(s)"},
		{name: "show invalid hash", args: []string{"bundle", "show", "sha256:abc"}, wantStderr: "bundle hash must match bundle-v1:sha256:<64 lowercase hex>"},
		{name: "agents invalid hash", args: []string{"bundle", "agents", "bad"}, wantStderr: "bundle hash must match bundle-v1:sha256:<64 lowercase hex>"},
		{name: "get alias not promoted", args: []string{"bundle", "get", validBundleHash("a")}, wantStderr: "unknown command"},
		{name: "register not promoted", args: []string{"bundle", "register"}, wantStderr: "unknown command"},
		{name: "delete not promoted", args: []string{"bundle", "delete", validBundleHash("a")}, wantStderr: "unknown command"},
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

func TestBundleCommandsFailClosedOnRPCAndMalformedResponses(t *testing.T) {
	bundleHash := validBundleHash("c")
	for _, tc := range []struct {
		name       string
		args       []string
		handler    http.HandlerFunc
		wantCode   int
		wantStderr string
	}{
		{
			name: "bundle not found",
			args: []string{"bundle", "show", bundleHash},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeBundleJSONRPCError(t, w, req.ID, "BUNDLE_NOT_FOUND")
			},
			wantCode:   cliExitNotFound,
			wantStderr: "BUNDLE_NOT_FOUND: Application error: BUNDLE_NOT_FOUND",
		},
		{
			name: "malformed list missing bundles",
			args: []string{"bundle", "list"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeJSONRPCResult(t, w, req.ID, map[string]any{"next_cursor": "cursor-2"})
			},
			wantCode:   cliExitRuntime,
			wantStderr: "malformed bundle.list result: bundles is required",
		},
		{
			name: "malformed detail missing parsed json",
			args: []string{"bundle", "show", bundleHash},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				result := validBundleDetail(bundleHash)
				delete(result, "parsed_json")
				writeJSONRPCResult(t, w, req.ID, result)
			},
			wantCode:   cliExitRuntime,
			wantStderr: "malformed bundle.get result: parsed_json is required",
		},
		{
			name: "malformed agents missing agents",
			args: []string{"bundle", "agents", bundleHash},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeJSONRPCResult(t, w, req.ID, map[string]any{})
			},
			wantCode:   cliExitRuntime,
			wantStderr: "malformed bundle.agents result: agents is required",
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

func assertBundleRequest(t *testing.T, req jsonRPCRequest, wantMethod string, wantParams map[string]any) {
	t.Helper()
	if req.JSONRPC != "2.0" || req.Method != wantMethod {
		t.Fatalf("request jsonrpc/method = %s/%s, want 2.0/%s", req.JSONRPC, req.Method, wantMethod)
	}
	if !reflect.DeepEqual(req.Params, wantParams) {
		t.Fatalf("%s params = %#v, want %#v", wantMethod, req.Params, wantParams)
	}
}

func validBundleHash(hexDigit string) string {
	return "bundle-v1:sha256:" + strings.Repeat(hexDigit, 64)
}

func validBundleSummary(bundleHash string) map[string]any {
	return map[string]any{
		"bundle_hash":     bundleHash,
		"agent_count":     2,
		"has_data":        true,
		"data_size_bytes": 512,
		"metadata":        map[string]any{"source": "test"},
		"ingested_at":     "2026-05-31T10:00:00Z",
	}
}

func validBundleDetail(bundleHash string) map[string]any {
	result := validBundleSummary(bundleHash)
	result["content_yaml"] = "agents:\n  - id: agent-alpha\n"
	result["parsed_json"] = map[string]any{"agents": []map[string]any{{"id": "agent-alpha"}}}
	return result
}

func validBundleAgent(agentID string) map[string]any {
	return map[string]any{
		"agent_id":          agentID,
		"flow_instance":     "default",
		"role":              "researcher",
		"type":              "business",
		"model_tier":        "standard",
		"llm_backend":       "openai_compatible",
		"conversation_mode": "task",
		"session_scope":     "run",
		"prompt_path":       "agents/researcher.md",
		"subscriptions":     []string{"scan.requested"},
		"tools":             []string{"read_file"},
	}
}

func writeBundleJSONRPCError(t *testing.T, w http.ResponseWriter, id string, code string) {
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
				"details":        map[string]any{"bundle_hash": "missing"},
				"retryable":      false,
				"correlation_id": "corr-bundle",
			},
		},
	}); err != nil {
		t.Fatalf("encode response: %v", err)
	}
}
