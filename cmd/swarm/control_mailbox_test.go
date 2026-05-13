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

func TestControlMailboxCommandsSendV1RPCRequests(t *testing.T) {
	for _, tc := range []struct {
		name       string
		args       []string
		wantMethod string
		wantParams map[string]any
		wantOutput []string
	}{
		{
			name:       "approve",
			args:       []string{"control", "mailbox", "approve", "mailbox-1", "--idempotency-key", "idem-approve", "--decision-payload-json", `{"approved":true}`},
			wantMethod: "mailbox.approve",
			wantParams: map[string]any{
				"mailbox_id":       "mailbox-1",
				"idempotency_key":  "idem-approve",
				"decision_payload": map[string]any{"approved": true},
			},
			wantOutput: []string{"mailbox approve ok:", "mailbox_id=mailbox-1", "status=decided", "decision_id=decision-1", "downstream_event_id=event-1"},
		},
		{
			name:       "reject",
			args:       []string{"control", "mailbox", "reject", "mailbox-2", "--reason", "not enough evidence", "--idempotency-key", "idem-reject"},
			wantMethod: "mailbox.reject",
			wantParams: map[string]any{
				"mailbox_id":      "mailbox-2",
				"reason":          "not enough evidence",
				"idempotency_key": "idem-reject",
			},
			wantOutput: []string{"mailbox reject ok:", "mailbox_id=mailbox-2", "status=decided", "decision_id=decision-1"},
		},
		{
			name:       "defer",
			args:       []string{"control", "mailbox", "defer", "mailbox-3", "--until", "2026-05-13T12:30:00Z", "--idempotency-key", "idem-defer"},
			wantMethod: "mailbox.defer",
			wantParams: map[string]any{
				"mailbox_id":      "mailbox-3",
				"until":           "2026-05-13T12:30:00Z",
				"idempotency_key": "idem-defer",
			},
			wantOutput: []string{"mailbox defer ok:", "mailbox_id=mailbox-3", "status=deferred", "decision_id=decision-1"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
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
				status := "decided"
				downstream := ""
				if tc.name == "defer" {
					status = "deferred"
				}
				if tc.name == "approve" {
					downstream = "event-1"
				}
				writeJSONRPCResult(t, w, captured.ID, map[string]any{
					"ok":                  true,
					"mailbox_decision_id": "decision-1",
					"downstream_event_id": downstream,
					"status":              status,
				})
			}))
			defer server.Close()

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), tc.args, &stdout, &stderr, testRootCommandOptions(server))
			if code != 0 {
				t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
			}
			if captured.JSONRPC != "2.0" || captured.Method != tc.wantMethod {
				t.Fatalf("request jsonrpc/method = %s/%s, want 2.0/%s", captured.JSONRPC, captured.Method, tc.wantMethod)
			}
			if !reflect.DeepEqual(captured.Params, tc.wantParams) {
				t.Fatalf("params = %#v, want %#v", captured.Params, tc.wantParams)
			}
			for _, want := range tc.wantOutput {
				if !strings.Contains(stdout.String(), want) {
					t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
				}
			}
			if strings.TrimSpace(stderr.String()) != "" {
				t.Fatalf("stderr = %q, want empty", stderr.String())
			}
		})
	}
}

func TestControlMailboxRejectsInvalidInputBeforeRequest(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeJSONRPCResult(t, w, "unexpected", map[string]any{"ok": true})
	}))
	defer server.Close()

	for _, tc := range []struct {
		name        string
		args        []string
		wantStderr  string
		wantNoCalls bool
	}{
		{name: "approve missing id", args: []string{"control", "mailbox", "approve"}, wantStderr: "accepts 1 arg(s)", wantNoCalls: true},
		{name: "approve extra arg", args: []string{"control", "mailbox", "approve", "mailbox-1", "extra"}, wantStderr: "accepts 1 arg(s)", wantNoCalls: true},
		{name: "approve unsupported flag", args: []string{"control", "mailbox", "approve", "mailbox-1", "--unknown"}, wantStderr: "unknown flag", wantNoCalls: true},
		{name: "approve malformed payload", args: []string{"control", "mailbox", "approve", "mailbox-1", "--decision-payload-json", "{"}, wantStderr: "--decision-payload-json must be a JSON object", wantNoCalls: true},
		{name: "approve non-object payload", args: []string{"control", "mailbox", "approve", "mailbox-1", "--decision-payload-json", "[]"}, wantStderr: "--decision-payload-json must be a JSON object", wantNoCalls: true},
		{name: "reject missing reason", args: []string{"control", "mailbox", "reject", "mailbox-1"}, wantStderr: "--reason is required", wantNoCalls: true},
		{name: "reject blank reason", args: []string{"control", "mailbox", "reject", "mailbox-1", "--reason", "  "}, wantStderr: "--reason is required", wantNoCalls: true},
		{name: "defer missing until", args: []string{"control", "mailbox", "defer", "mailbox-1"}, wantStderr: "--until is required", wantNoCalls: true},
		{name: "defer invalid until", args: []string{"control", "mailbox", "defer", "mailbox-1", "--until", "tomorrow"}, wantStderr: "--until must be an RFC3339 timestamp", wantNoCalls: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := calls.Load()
			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), tc.args, &stdout, &stderr, testRootCommandOptions(server))
			if code != 2 {
				t.Fatalf("code = %d, want 2 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
			}
			if strings.TrimSpace(stdout.String()) != "" {
				t.Fatalf("stdout = %q, want empty", stdout.String())
			}
			if !strings.Contains(stderr.String(), tc.wantStderr) {
				t.Fatalf("stderr = %q, want substring %q", stderr.String(), tc.wantStderr)
			}
			if tc.wantNoCalls && calls.Load() != before {
				t.Fatalf("server calls changed from %d to %d, want no request", before, calls.Load())
			}
		})
	}
}

func TestControlMailboxRequiresAPITokenBeforeRequest(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeJSONRPCResult(t, w, "unexpected", map[string]any{"ok": true})
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"control", "mailbox", "approve", "mailbox-1"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 2 {
		t.Fatalf("code = %d, want 2 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), "SWARM_API_TOKEN is required") {
		t.Fatalf("stderr = %q, want missing-token message", stderr.String())
	}
	if calls.Load() != 0 {
		t.Fatalf("server calls = %d, want 0", calls.Load())
	}
}

func TestControlMailboxSurfacesTransportAndRPCFailures(t *testing.T) {
	for _, tc := range []struct {
		name       string
		handler    http.HandlerFunc
		wantStderr string
	}{
		{
			name: "http auth failure",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"error":"invalid bearer token"}`))
			},
			wantStderr: "v1 RPC HTTP 401",
		},
		{
			name: "malformed response",
			handler: func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(`{`))
			},
			wantStderr: "decode JSON-RPC response",
		},
		{
			name: "malformed result",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeJSONRPCResult(t, w, req.ID, map[string]any{"ok": true})
			},
			wantStderr: "mailbox_decision_id is required",
		},
		{
			name: "json rpc application error",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"jsonrpc": "2.0",
					"id":      req.ID,
					"error": map[string]any{
						"code":    -32010,
						"message": "Application error: MAILBOX_NOT_FOUND",
						"data": map[string]any{
							"code":           "MAILBOX_NOT_FOUND",
							"details":        map[string]any{"mailbox_id": "missing"},
							"retryable":      false,
							"correlation_id": "corr-1",
						},
					},
				})
			},
			wantStderr: "MAILBOX_NOT_FOUND",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("SWARM_API_TOKEN", "test-token")
			server := httptest.NewServer(tc.handler)
			defer server.Close()

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"control", "mailbox", "approve", "mailbox-1"}, &stdout, &stderr, testRootCommandOptions(server))
			if code != 2 {
				t.Fatalf("code = %d, want 2 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
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

func testRootCommandOptions(server *httptest.Server) rootCommandOptions {
	opts := defaultRootCommandOptions()
	opts.apiEndpoint = server.URL + "/v1/rpc"
	opts.httpClient = server.Client()
	return opts
}

func writeJSONRPCResult(t *testing.T, w http.ResponseWriter, id string, result map[string]any) {
	t.Helper()
	w.Header().Set("content-type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"result":  result,
	}); err != nil {
		t.Fatalf("encode response: %v", err)
	}
}
