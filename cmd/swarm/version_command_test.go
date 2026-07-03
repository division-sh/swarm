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

func TestVersionLocalDoesNotRequireTokenOrRequest(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeJSONRPCResult(t, w, "unexpected", map[string]any{})
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"version"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if calls.Load() != 0 {
		t.Fatalf("server calls = %d, want 0", calls.Load())
	}
	metadata := currentTestVersionMetadata(t)
	for _, want := range []string{"Swarm " + metadata.BinaryVersion, "Commit: " + metadata.Commit, "Built: " + metadata.Built, "Go:"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
	if strings.Contains(stdout.String(), "Server:") {
		t.Fatalf("local version output unexpectedly included server section:\n%s", stdout.String())
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestVersionServerUsesHealthCheck(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var captured jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %q, want POST", r.Method)
		}
		if r.URL.Path != "/v1/rpc" {
			t.Errorf("path = %q, want /v1/rpc", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Errorf("Authorization = %q, want bearer token", got)
		}
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Errorf("decode request: %v", err)
		}
		writeJSONRPCResult(t, w, captured.ID, validVersionHealthResult())
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"version", "--server"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if captured.JSONRPC != "2.0" || captured.Method != "health.check" {
		t.Fatalf("request jsonrpc/method = %s/%s, want 2.0/health.check", captured.JSONRPC, captured.Method)
	}
	if len(captured.Params) != 0 {
		t.Fatalf("params = %#v, want empty", captured.Params)
	}
	metadata := currentTestVersionMetadata(t)
	for _, want := range []string{
		"Swarm " + metadata.BinaryVersion,
		"Commit: " + metadata.Commit,
		"Built: " + metadata.Built,
		"Go:",
		"Server:",
		"alive=true ready=false db_ok=true runtime_ok=false",
		"bundle fingerprint=sha256:server workflow_name=workflow workflow_version=v1",
		"bundle_hash=bundle-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestVersionServerRejectsInvalidInputBeforeRequest(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeJSONRPCResult(t, w, "unexpected", validVersionHealthResult())
	}))
	defer server.Close()

	for _, tc := range []struct {
		name       string
		args       []string
		wantStderr string
	}{
		{name: "extra arg", args: []string{"version", "extra"}, wantStderr: "unknown command"},
		{name: "server extra arg", args: []string{"version", "--server", "extra"}, wantStderr: "unknown command"},
		{name: "unknown flag", args: []string{"version", "--bogus"}, wantStderr: "unknown flag"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := calls.Load()
			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), tc.args, &stdout, &stderr, testRootCommandOptions(server))
			if code != 2 {
				t.Fatalf("code = %d, want 2 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.wantStderr) {
				t.Fatalf("stderr = %q, want %q", stderr.String(), tc.wantStderr)
			}
			if strings.TrimSpace(stdout.String()) != "" {
				t.Fatalf("stdout = %q, want empty", stdout.String())
			}
			if calls.Load() != before {
				t.Fatalf("server calls changed from %d to %d, want no request", before, calls.Load())
			}
		})
	}
}

func TestVersionServerRequiresTokenBeforeRequest(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeJSONRPCResult(t, w, "unexpected", validVersionHealthResult())
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"version", "--server"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 4 {
		t.Fatalf("code = %d, want 4 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), "API token source is required") {
		t.Fatalf("stderr = %q, want missing-token message", stderr.String())
	}
	if strings.TrimSpace(stdout.String()) != "" {
		t.Fatalf("stdout = %q, want empty", stdout.String())
	}
	if calls.Load() != 0 {
		t.Fatalf("server calls = %d, want 0", calls.Load())
	}
}

func TestVersionServerFailsClosedOnAPIAndMalformedResults(t *testing.T) {
	for _, tc := range []struct {
		name       string
		handler    http.HandlerFunc
		wantCode   int
		wantStderr string
	}{
		{
			name: "http auth failure",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"error":"invalid bearer token"}`))
			},
			wantCode:   4,
			wantStderr: "v1 RPC HTTP 401",
		},
		{
			name: "json rpc error",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				w.Header().Set("content-type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{
					"jsonrpc": "2.0",
					"id":      req.ID,
					"error": map[string]any{
						"code":    -32000,
						"message": "unauthorized",
						"data":    map[string]any{"code": "UNAUTHORIZED"},
					},
				})
			},
			wantCode:   4,
			wantStderr: "UNAUTHORIZED: unauthorized",
		},
		{
			name: "malformed envelope",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				w.Header().Set("content-type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "1.0", "id": req.ID, "result": validVersionHealthResult()})
			},
			wantCode:   3,
			wantStderr: "malformed JSON-RPC response",
		},
		{
			name: "malformed health result",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				result := validVersionHealthResult()
				result["bundle"] = map[string]any{
					"fingerprint":    "sha256:server",
					"workflow_name":  "workflow",
					"workflow_extra": "ignored",
				}
				writeJSONRPCResult(t, w, req.ID, result)
			},
			wantCode:   3,
			wantStderr: "bundle.workflow_version is required",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setCLIAPITestToken(t, "test-token")
			server := httptest.NewServer(tc.handler)
			defer server.Close()

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"version", "--server"}, &stdout, &stderr, testRootCommandOptions(server))
			if code != tc.wantCode {
				t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, tc.wantCode, stdout.String(), stderr.String())
			}
			if strings.TrimSpace(stdout.String()) != "" {
				t.Fatalf("stdout = %q, want empty", stdout.String())
			}
			if !strings.Contains(stderr.String(), tc.wantStderr) {
				t.Fatalf("stderr = %q, want substring %q", stderr.String(), tc.wantStderr)
			}
		})
	}
}

func TestVersionServerFailsClosedOnTransportError(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unexpected request to closed server")
	}))
	opts := testRootCommandOptions(server)
	server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"version", "--server"}, &stdout, &stderr, opts)
	if code != 3 {
		t.Fatalf("code = %d, want 3 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if strings.TrimSpace(stdout.String()) != "" {
		t.Fatalf("stdout = %q, want empty", stdout.String())
	}
	if !strings.Contains(stderr.String(), "v1 RPC request failed") {
		t.Fatalf("stderr = %q, want transport failure", stderr.String())
	}
}

func validVersionHealthResult() map[string]any {
	return map[string]any{
		"alive":      true,
		"ready":      false,
		"db_ok":      true,
		"runtime_ok": false,
		"bundle": map[string]any{
			"fingerprint":      "sha256:server",
			"bundle_hash":      "bundle-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			"workflow_name":    "workflow",
			"workflow_version": "v1",
		},
	}
}
