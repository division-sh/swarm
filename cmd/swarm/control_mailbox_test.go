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

func TestMailboxCommandsSendV1RPCRequests(t *testing.T) {
	for _, tc := range []struct {
		name       string
		args       []string
		wantMethod string
		wantParams map[string]any
		wantOutput []string
	}{
		{
			name:       "approve",
			args:       []string{"mailbox", "approve", "mailbox-1", "--idempotency-key", "idem-approve", "--decision-payload-json", `{"approved":true}`},
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
			args:       []string{"mailbox", "reject", "mailbox-2", "--reason", "not enough evidence", "--idempotency-key", "idem-reject"},
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
			args:       []string{"mailbox", "defer", "mailbox-3", "--until", "2026-05-13T12:30:00Z", "--idempotency-key", "idem-defer"},
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

func TestMailboxListSendsV1RPCRequestAndRendersResult(t *testing.T) {
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
		first := mailboxItemResult("mailbox-1", "pending", "high")
		delete(first, "source_event_id")
		writeJSONRPCResult(t, w, captured.ID, map[string]any{
			"items": []map[string]any{
				first,
				mailboxItemResult("mailbox-2", "decided", "normal"),
			},
			"next_cursor": "cursor-2",
		})
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{
		"mailbox", "list",
		"--all",
		"--run-id", "run-1",
		"--entity-id", "entity-1",
		"--type", "review_request",
		"--priority", "high",
		"--limit", "25",
		"--cursor", "cursor-1",
	}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if captured.JSONRPC != "2.0" || captured.Method != "mailbox.list" {
		t.Fatalf("request jsonrpc/method = %s/%s, want 2.0/mailbox.list", captured.JSONRPC, captured.Method)
	}
	wantParams := map[string]any{
		"run_id":    "run-1",
		"entity_id": "entity-1",
		"type":      "review_request",
		"priority":  "high",
		"limit":     float64(25),
		"cursor":    "cursor-1",
	}
	if !reflect.DeepEqual(captured.Params, wantParams) {
		t.Fatalf("params = %#v, want %#v", captured.Params, wantParams)
	}
	for _, want := range []string{
		"MAILBOX_ID\tSTATUS\tPRIORITY\tTYPE",
		"mailbox-1\tpending\thigh\treview_request\t-\tentity-1",
		"mailbox-2\tdecided\tnormal\treview_request",
		"next_cursor=cursor-2",
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestMailboxListDefaultsToPendingAndRendersEmptyResult(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
	var captured jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Errorf("decode request: %v", err)
		}
		writeJSONRPCResult(t, w, captured.ID, map[string]any{"items": []any{}})
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"mailbox", "list"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if captured.Method != "mailbox.list" {
		t.Fatalf("method = %q, want mailbox.list", captured.Method)
	}
	if !reflect.DeepEqual(captured.Params, map[string]any{"status": "pending"}) {
		t.Fatalf("params = %#v, want pending default", captured.Params)
	}
	if !strings.Contains(stdout.String(), "No mailbox items match the filter.") {
		t.Fatalf("stdout = %q, want empty message", stdout.String())
	}
}

func TestMailboxViewSendsV1RPCRequestAndRendersDetail(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
	var captured jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Errorf("Authorization = %q, want bearer token", got)
		}
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Errorf("decode request: %v", err)
		}
		item := mailboxItemResult("mailbox-1", "pending", "high")
		delete(item, "source_event_id")
		writeJSONRPCResult(t, w, captured.ID, map[string]any{
			"item":    item,
			"payload": map[string]any{"summary": "needs review"},
			"history": []map[string]any{
				{"action": "created", "actor_token_id": "system", "ts": "2026-05-13T12:00:00Z"},
			},
		})
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"mailbox", "view", "mailbox-1"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if captured.JSONRPC != "2.0" || captured.Method != "mailbox.get" {
		t.Fatalf("request jsonrpc/method = %s/%s, want 2.0/mailbox.get", captured.JSONRPC, captured.Method)
	}
	if !reflect.DeepEqual(captured.Params, map[string]any{"mailbox_id": "mailbox-1"}) {
		t.Fatalf("params = %#v, want mailbox id", captured.Params)
	}
	for _, want := range []string{
		"Mailbox mailbox-1",
		"status=pending priority=high type=review_request",
		"source_event_id=- source_flow=validation",
		`payload={"summary":"needs review"}`,
		"history:",
		"- action=created actor=system ts=2026-05-13T12:00:00Z",
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestMailboxRejectsInvalidInputBeforeRequest(t *testing.T) {
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
		{name: "list extra arg", args: []string{"mailbox", "list", "extra"}, wantStderr: "unknown command", wantNoCalls: true},
		{name: "list invalid status", args: []string{"mailbox", "list", "--status", "open"}, wantStderr: "--status must be one of", wantNoCalls: true},
		{name: "list invalid priority", args: []string{"mailbox", "list", "--priority", "low"}, wantStderr: "--priority must be one of", wantNoCalls: true},
		{name: "list invalid limit", args: []string{"mailbox", "list", "--limit", "201"}, wantStderr: "--limit must be an integer from 1 to 200", wantNoCalls: true},
		{name: "list unsupported flag", args: []string{"mailbox", "list", "--unknown"}, wantStderr: "unknown flag", wantNoCalls: true},
		{name: "view missing id", args: []string{"mailbox", "view"}, wantStderr: "accepts 1 arg(s)", wantNoCalls: true},
		{name: "view extra arg", args: []string{"mailbox", "view", "mailbox-1", "extra"}, wantStderr: "accepts 1 arg(s)", wantNoCalls: true},
		{name: "view blank id", args: []string{"mailbox", "view", "  "}, wantStderr: "mailbox item id is required", wantNoCalls: true},
		{name: "view unsupported flag", args: []string{"mailbox", "view", "mailbox-1", "--unknown"}, wantStderr: "unknown flag", wantNoCalls: true},
		{name: "approve missing id", args: []string{"mailbox", "approve"}, wantStderr: "accepts 1 arg(s)", wantNoCalls: true},
		{name: "approve extra arg", args: []string{"mailbox", "approve", "mailbox-1", "extra"}, wantStderr: "accepts 1 arg(s)", wantNoCalls: true},
		{name: "approve unsupported flag", args: []string{"mailbox", "approve", "mailbox-1", "--unknown"}, wantStderr: "unknown flag", wantNoCalls: true},
		{name: "approve malformed payload", args: []string{"mailbox", "approve", "mailbox-1", "--decision-payload-json", "{"}, wantStderr: "--decision-payload-json must be a JSON object", wantNoCalls: true},
		{name: "approve non-object payload", args: []string{"mailbox", "approve", "mailbox-1", "--decision-payload-json", "[]"}, wantStderr: "--decision-payload-json must be a JSON object", wantNoCalls: true},
		{name: "reject missing reason", args: []string{"mailbox", "reject", "mailbox-1"}, wantStderr: "--reason is required", wantNoCalls: true},
		{name: "reject blank reason", args: []string{"mailbox", "reject", "mailbox-1", "--reason", "  "}, wantStderr: "--reason is required", wantNoCalls: true},
		{name: "defer missing until", args: []string{"mailbox", "defer", "mailbox-1"}, wantStderr: "--until is required", wantNoCalls: true},
		{name: "defer invalid until", args: []string{"mailbox", "defer", "mailbox-1", "--until", "tomorrow"}, wantStderr: "--until must be an RFC3339 timestamp", wantNoCalls: true},
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

func TestMailboxRequiresAPITokenBeforeRequest(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeJSONRPCResult(t, w, "unexpected", map[string]any{"ok": true})
	}))
	defer server.Close()

	for _, args := range [][]string{
		{"mailbox", "list"},
		{"mailbox", "view", "mailbox-1"},
		{"mailbox", "approve", "mailbox-1"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			t.Setenv("SWARM_API_TOKEN", "")
			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), args, &stdout, &stderr, testRootCommandOptions(server))
			if code != 4 {
				t.Fatalf("code = %d, want 4 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
			}
			if !strings.Contains(stderr.String(), "SWARM_API_TOKEN is required") {
				t.Fatalf("stderr = %q, want missing-token message", stderr.String())
			}
			if calls.Load() != 0 {
				t.Fatalf("server calls = %d, want 0", calls.Load())
			}
		})
	}
}

func TestControlMailboxRetiredAliasesFailClosedBeforeRequest(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeJSONRPCResult(t, w, "unexpected", map[string]any{"ok": true})
	}))
	defer server.Close()

	for _, args := range [][]string{
		{"control", "mailbox", "list"},
		{"control", "mailbox", "view", "mailbox-1"},
		{"control", "mailbox", "approve", "mailbox-1"},
		{"control", "mailbox", "reject", "mailbox-1", "--reason", "not enough evidence"},
		{"control", "mailbox", "defer", "mailbox-1", "--until", "2026-05-13T12:30:00Z"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			before := calls.Load()
			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), args, &stdout, &stderr, testRootCommandOptions(server))
			if code != 2 {
				t.Fatalf("code = %d, want 2 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
			}
			if strings.TrimSpace(stdout.String()) != "" {
				t.Fatalf("stdout = %q, want empty", stdout.String())
			}
			for _, want := range []string{
				"`swarm control mailbox` was removed in CLI v2",
				"`swarm mailbox approve <mailbox-item-id>`",
				"`swarm mailbox reject <mailbox-item-id> --reason <text>`",
				"`swarm mailbox defer <mailbox-item-id> --until <RFC3339>`",
			} {
				if !strings.Contains(stderr.String(), want) {
					t.Fatalf("stderr missing %q:\n%s", want, stderr.String())
				}
			}
			if calls.Load() != before {
				t.Fatalf("server calls changed from %d to %d, want no request", before, calls.Load())
			}
		})
	}
}

func TestMailboxSurfacesTransportAndRPCFailures(t *testing.T) {
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
			name: "malformed response",
			handler: func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(`{`))
			},
			wantCode:   3,
			wantStderr: "decode JSON-RPC response",
		},
		{
			name: "malformed result",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeJSONRPCResult(t, w, req.ID, map[string]any{"ok": true})
			},
			wantCode:   3,
			wantStderr: "mailbox_decision_id is required",
		},
		{
			name: "mismatched json rpc id",
			handler: func(w http.ResponseWriter, r *http.Request) {
				writeJSONRPCResult(t, w, "swarm-cli:mailbox.reject", map[string]any{
					"ok":                  true,
					"mailbox_decision_id": "decision-1",
					"status":              "decided",
				})
			},
			wantCode:   3,
			wantStderr: "malformed JSON-RPC response: id",
		},
		{
			name: "missing json rpc id",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("content-type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{
					"jsonrpc": "2.0",
					"result": map[string]any{
						"ok":                  true,
						"mailbox_decision_id": "decision-1",
						"status":              "decided",
					},
				})
			},
			wantCode:   3,
			wantStderr: "malformed JSON-RPC response: id=<missing>",
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
			wantCode:   5,
			wantStderr: "MAILBOX_NOT_FOUND",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("SWARM_API_TOKEN", "test-token")
			server := httptest.NewServer(tc.handler)
			defer server.Close()

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"mailbox", "approve", "mailbox-1"}, &stdout, &stderr, testRootCommandOptions(server))
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

func TestMailboxReadCommandsFailClosedOnRPCAndMalformedResponses(t *testing.T) {
	for _, tc := range []struct {
		name       string
		args       []string
		handler    http.HandlerFunc
		wantCode   int
		wantStderr string
	}{
		{
			name: "list http auth failure",
			args: []string{"mailbox", "list"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"error":"invalid bearer token"}`))
			},
			wantCode:   4,
			wantStderr: "v1 RPC HTTP 401",
		},
		{
			name: "list malformed missing items",
			args: []string{"mailbox", "list"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeJSONRPCResult(t, w, req.ID, map[string]any{"next_cursor": "cursor-1"})
			},
			wantCode:   3,
			wantStderr: "malformed mailbox.list result: items is required",
		},
		{
			name: "list malformed item",
			args: []string{"mailbox", "list"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				item := mailboxItemResult("mailbox-1", "pending", "normal")
				delete(item, "source_flow")
				writeJSONRPCResult(t, w, req.ID, map[string]any{"items": []map[string]any{item}})
			},
			wantCode:   3,
			wantStderr: "items[0]: source_flow is required",
		},
		{
			name: "view malformed missing payload",
			args: []string{"mailbox", "view", "mailbox-1"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeJSONRPCResult(t, w, req.ID, map[string]any{
					"item":    mailboxItemResult("mailbox-1", "pending", "normal"),
					"history": []map[string]any{{"action": "created", "actor_token_id": "system", "ts": "2026-05-13T12:00:00Z"}},
				})
			},
			wantCode:   3,
			wantStderr: "malformed mailbox.get result: payload is required",
		},
		{
			name: "view mailbox not found",
			args: []string{"mailbox", "view", "missing"},
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
							"code":    "MAILBOX_NOT_FOUND",
							"details": map[string]any{"mailbox_id": "missing"},
						},
					},
				})
			},
			wantCode:   5,
			wantStderr: "MAILBOX_NOT_FOUND",
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
			if strings.TrimSpace(stdout.String()) != "" {
				t.Fatalf("stdout = %q, want empty", stdout.String())
			}
			if !strings.Contains(stderr.String(), tc.wantStderr) {
				t.Fatalf("stderr = %q, want substring %q", stderr.String(), tc.wantStderr)
			}
		})
	}
}

func TestMailboxRejectsMalformedStatusByAction(t *testing.T) {
	for _, tc := range []struct {
		name   string
		args   []string
		status string
		want   string
	}{
		{
			name:   "approve invalid enum status",
			args:   []string{"mailbox", "approve", "mailbox-1"},
			status: "garbage",
			want:   `status="garbage", want "decided" for mailbox approve`,
		},
		{
			name:   "reject wrong action status",
			args:   []string{"mailbox", "reject", "mailbox-1", "--reason", "not enough evidence"},
			status: "deferred",
			want:   `status="deferred", want "decided" for mailbox reject`,
		},
		{
			name:   "defer wrong action status",
			args:   []string{"mailbox", "defer", "mailbox-1", "--until", "2026-05-13T12:30:00Z"},
			status: "decided",
			want:   `status="decided", want "deferred" for mailbox defer`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("SWARM_API_TOKEN", "test-token")
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeJSONRPCResult(t, w, req.ID, map[string]any{
					"ok":                  true,
					"mailbox_decision_id": "decision-1",
					"status":              tc.status,
				})
			}))
			defer server.Close()

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), tc.args, &stdout, &stderr, testRootCommandOptions(server))
			if code != 3 {
				t.Fatalf("code = %d, want 3 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
			}
			if strings.TrimSpace(stdout.String()) != "" {
				t.Fatalf("stdout = %q, want empty", stdout.String())
			}
			if !strings.Contains(stderr.String(), tc.want) {
				t.Fatalf("stderr = %q, want substring %q", stderr.String(), tc.want)
			}
		})
	}
}

func mailboxItemResult(mailboxID, status, priority string) map[string]any {
	return map[string]any{
		"mailbox_id":       mailboxID,
		"type":             "review_request",
		"status":           status,
		"priority":         priority,
		"source_event_id":  "event-" + mailboxID,
		"source_flow":      "validation",
		"source_entity_id": "entity-1",
		"payload":          map[string]any{"summary": "needs review"},
		"created_at":       "2026-05-13T12:00:00Z",
		"decision":         decisionForMailboxStatus(status),
	}
}

func decisionForMailboxStatus(status string) string {
	switch status {
	case "decided":
		return "approved"
	case "deferred":
		return "deferred"
	case "expired":
		return "expired"
	default:
		return ""
	}
}

func testRootCommandOptions(server *httptest.Server) rootCommandOptions {
	opts := defaultRootCommandOptions()
	opts.apiRPCEndpointOverride = server.URL + "/v1/rpc"
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
