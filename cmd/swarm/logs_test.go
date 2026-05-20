package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/gorilla/websocket"
)

func TestLogsUsesRuntimeLogsV1RPCWithSnapshotFilters(t *testing.T) {
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
			"logs":        []any{validRuntimeLogEntry("log-1")},
			"next_cursor": "log-cursor-2",
		})
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{
		"logs",
		"--run-id", "run-1",
		"--entity-id", "entity-1",
		"--session-id", "session-1",
		"--component", "scheduler",
		"--level", "WARN",
		"--error-code", "DELIVERY_FAILED",
		"--source", "runtime",
		"--since", "2026-05-19T10:00:00Z",
		"--until", "2026-05-19T11:00:00Z",
		"--limit", "25",
		"--cursor", "cursor-1",
		"--order", "ASC",
	}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if captured.Method != runtimeLogsMethodList {
		t.Fatalf("method = %q, want %s", captured.Method, runtimeLogsMethodList)
	}
	wantParams := map[string]any{
		"run_id":     "run-1",
		"entity_id":  "entity-1",
		"session_id": "session-1",
		"component":  "scheduler",
		"level":      "warn",
		"error_code": "DELIVERY_FAILED",
		"source":     "runtime",
		"since":      "2026-05-19T10:00:00Z",
		"until":      "2026-05-19T11:00:00Z",
		"limit":      float64(25),
		"cursor":     "cursor-1",
		"order":      "asc",
	}
	if !reflect.DeepEqual(captured.Params, wantParams) {
		t.Fatalf("params = %#v, want %#v", captured.Params, wantParams)
	}
	for _, want := range []string{"TIME", "log message", "run-1", "entity-1", "session-1", "DELIVERY_FAILED", "next_cursor=log-cursor-2"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestLogsFollowUsesRuntimeSubscribeLogsV1WS(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
	server, wsRequests := newRuntimeLogWSServer(t, runtimeLogWSServerOptions{
		logs:           []map[string]any{validRuntimeLogEntry("log-live-1")},
		closeAfterRows: true,
	})
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{
		"logs",
		"--follow",
		"--run-id", "run-1",
		"--entity-id", "entity-1",
		"--session-id", "session-1",
		"--component", "scheduler",
		"--level", "info",
		"--error-code", "DELIVERY_FAILED",
		"--source", "runtime",
		"--replay-since", "2026-05-19T10:00:00Z",
	}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if len(*wsRequests) != 1 {
		t.Fatalf("ws requests = %d, want 1", len(*wsRequests))
	}
	req := (*wsRequests)[0]
	if req.Method != runtimeLogsMethodSubscribe {
		t.Fatalf("method = %q, want %s", req.Method, runtimeLogsMethodSubscribe)
	}
	wantParams := map[string]any{
		"run_id":       "run-1",
		"entity_id":    "entity-1",
		"session_id":   "session-1",
		"component":    "scheduler",
		"level":        "info",
		"error_code":   "DELIVERY_FAILED",
		"source":       "runtime",
		"replay_since": "2026-05-19T10:00:00Z",
	}
	if !reflect.DeepEqual(req.Params, wantParams) {
		t.Fatalf("params = %#v, want %#v", req.Params, wantParams)
	}
	for _, want := range []string{"log log_id=log-live-1", "run_id=run-1", "message=log message", "details={\"attempt\":2}"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestLogsRejectInvalidInputBeforeRequest(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeJSONRPCResult(t, w, "unexpected", map[string]any{"logs": []any{}})
	}))
	defer server.Close()

	for _, tc := range []struct {
		name       string
		args       []string
		wantStderr string
	}{
		{name: "invalid level", args: []string{"logs", "--level", "fatal"}, wantStderr: "--level must be one of"},
		{name: "invalid order", args: []string{"logs", "--order", "sideways"}, wantStderr: "--order must be one of"},
		{name: "invalid limit", args: []string{"logs", "--limit", "0"}, wantStderr: "--limit must be between 1 and 1000"},
		{name: "invalid since", args: []string{"logs", "--since", "not-time"}, wantStderr: "--since must be an RFC3339 timestamp"},
		{name: "invalid window", args: []string{"logs", "--since", "2026-05-19T11:00:00Z", "--until", "2026-05-19T10:00:00Z"}, wantStderr: "--until must be greater than or equal to --since"},
		{name: "replay without follow", args: []string{"logs", "--replay-since", "2026-05-19T10:00:00Z"}, wantStderr: "--replay-since requires --follow"},
		{name: "follow rejects limit", args: []string{"logs", "--follow", "--limit", "10"}, wantStderr: "--limit is not supported with --follow"},
		{name: "follow rejects since", args: []string{"logs", "--follow", "--since", "2026-05-19T10:00:00Z"}, wantStderr: "--since is not supported with --follow; use --replay-since"},
		{name: "follow rejects cursor", args: []string{"logs", "--follow", "--cursor", "cursor-1"}, wantStderr: "--cursor is not supported with --follow"},
		{name: "follow invalid replay since", args: []string{"logs", "--follow", "--replay-since", "not-time"}, wantStderr: "--replay-since must be an RFC3339 timestamp"},
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

func TestLogsFailClosedWithoutTokenBeforeRequest(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeJSONRPCResult(t, w, "unexpected", map[string]any{"logs": []any{}})
	}))
	defer server.Close()

	for _, args := range [][]string{
		{"logs"},
		{"logs", "--follow"},
	} {
		calls.Store(0)
		var stdout, stderr bytes.Buffer
		code := executeRootCommandWithOptions(context.Background(), t.TempDir(), args, &stdout, &stderr, testRootCommandOptions(server))
		if code != 4 {
			t.Fatalf("args=%v code = %d, want 4 stdout=%s stderr=%s", args, code, stdout.String(), stderr.String())
		}
		if !strings.Contains(stderr.String(), "SWARM_API_TOKEN is required") {
			t.Fatalf("stderr = %q, want token failure", stderr.String())
		}
		if calls.Load() != 0 {
			t.Fatalf("args=%v calls = %d, want 0", args, calls.Load())
		}
	}
}

func TestLogsMapRuntimeFailuresAndMalformedResults(t *testing.T) {
	for _, tc := range []struct {
		name       string
		args       []string
		handler    http.HandlerFunc
		wantCode   int
		wantStderr string
	}{
		{
			name: "http auth exits four",
			args: []string{"logs"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
			},
			wantCode:   4,
			wantStderr: "v1 RPC HTTP 401",
		},
		{
			name: "http runtime exits three",
			args: []string{"logs"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "unavailable", http.StatusServiceUnavailable)
			},
			wantCode:   3,
			wantStderr: "v1 RPC HTTP 503",
		},
		{
			name: "unknown rpc error exits three",
			args: []string{"logs"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeRuntimeLogJSONRPCError(t, w, req.ID, "METHOD_UNAVAILABLE")
			},
			wantCode:   3,
			wantStderr: "METHOD_UNAVAILABLE",
		},
		{
			name: "unauthorized rpc exits four",
			args: []string{"logs"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeRuntimeLogJSONRPCError(t, w, req.ID, "UNAUTHORIZED")
			},
			wantCode:   4,
			wantStderr: "UNAUTHORIZED",
		},
		{
			name: "malformed list missing logs exits three",
			args: []string{"logs"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeJSONRPCResult(t, w, req.ID, map[string]any{})
			},
			wantCode:   3,
			wantStderr: "logs is required",
		},
		{
			name: "malformed log row exits three",
			args: []string{"logs"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				log := validRuntimeLogEntry("log-1")
				delete(log, "log_id")
				writeJSONRPCResult(t, w, req.ID, map[string]any{"logs": []any{log}})
			},
			wantCode:   3,
			wantStderr: "log_id is required",
		},
		{
			name: "malformed log row missing message exits three",
			args: []string{"logs"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				log := validRuntimeLogEntry("log-1")
				delete(log, "message")
				writeJSONRPCResult(t, w, req.ID, map[string]any{"logs": []any{log}})
			},
			wantCode:   3,
			wantStderr: "message is required",
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

func TestLogsFollowMalformedWSFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name               string
		subscriptionResult map[string]any
		logs               []map[string]any
		wantStderr         string
	}{
		{
			name:               "missing subscription id",
			subscriptionResult: map[string]any{},
			wantStderr:         "subscription_id is required",
		},
		{
			name: "malformed notification",
			logs: []map[string]any{
				func() map[string]any {
					log := validRuntimeLogEntry("log-1")
					delete(log, "ts")
					return log
				}(),
			},
			wantStderr: "ts is required",
		},
		{
			name: "malformed notification missing message",
			logs: []map[string]any{
				func() map[string]any {
					log := validRuntimeLogEntry("log-1")
					delete(log, "message")
					return log
				}(),
			},
			wantStderr: "message is required",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("SWARM_API_TOKEN", "test-token")
			server, _ := newRuntimeLogWSServer(t, runtimeLogWSServerOptions{
				subscriptionResult: tc.subscriptionResult,
				logs:               tc.logs,
				closeAfterRows:     true,
			})
			defer server.Close()

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"logs", "--follow"}, &stdout, &stderr, testRootCommandOptions(server))
			if code != 3 {
				t.Fatalf("code = %d, want 3 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.wantStderr) {
				t.Fatalf("stderr = %q, want substring %q", stderr.String(), tc.wantStderr)
			}
		})
	}
}

func TestLogsFollowMapsHandshakeAuthToAuthExit(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
	var rpcCalls atomic.Int32
	var wsCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/rpc":
			rpcCalls.Add(1)
			t.Errorf("unexpected RPC request for log follow")
			http.Error(w, "unexpected rpc", http.StatusInternalServerError)
		case "/v1/ws":
			wsCalls.Add(1)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"logs", "--follow"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 4 {
		t.Fatalf("code = %d, want 4 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if rpcCalls.Load() != 0 {
		t.Fatalf("rpc calls = %d, want 0", rpcCalls.Load())
	}
	if wsCalls.Load() != 1 {
		t.Fatalf("ws calls = %d, want 1", wsCalls.Load())
	}
	if !strings.Contains(stderr.String(), "v1 WS HTTP 401") {
		t.Fatalf("stderr = %q, want WS auth status", stderr.String())
	}
}

func TestLogsFollowCancellationReturnsInterrupted(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
	ctx, cancel := context.WithCancel(context.Background())
	server, _ := newRuntimeLogWSServer(t, runtimeLogWSServerOptions{
		afterSubscription: cancel,
	})
	defer server.Close()

	defer cancel()
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(ctx, t.TempDir(), []string{"logs", "--follow"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 130 {
		t.Fatalf("code = %d, want 130 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), "detached from runtime log stream") {
		t.Fatalf("stderr = %q, want detach message", stderr.String())
	}
}

type runtimeLogWSServerOptions struct {
	subscriptionResult map[string]any
	logs               []map[string]any
	closeAfterRows     bool
	afterSubscription  func()
}

func newRuntimeLogWSServer(t *testing.T, opts runtimeLogWSServerOptions) (*httptest.Server, *[]jsonRPCRequest) {
	t.Helper()
	var mu sync.Mutex
	wsRequests := []jsonRPCRequest{}
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/rpc":
			t.Fatalf("unexpected RPC request for log follow")
		case "/v1/ws":
			if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
				t.Errorf("WS Authorization = %q, want bearer token", got)
			}
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				t.Errorf("upgrade websocket: %v", err)
				return
			}
			defer conn.Close()
			var req jsonRPCRequest
			if err := conn.ReadJSON(&req); err != nil {
				t.Errorf("read ws request: %v", err)
				return
			}
			mu.Lock()
			wsRequests = append(wsRequests, req)
			mu.Unlock()
			result := opts.subscriptionResult
			if result == nil {
				result = map[string]any{"subscription_id": "sub-logs"}
			}
			if err := conn.WriteJSON(map[string]any{
				"jsonrpc": "2.0",
				"id":      req.ID,
				"result":  result,
			}); err != nil {
				t.Errorf("write ws subscription response: %v", err)
				return
			}
			if opts.afterSubscription != nil {
				opts.afterSubscription()
			}
			for _, log := range opts.logs {
				if err := conn.WriteJSON(map[string]any{
					"jsonrpc": "2.0",
					"method":  "rpc.subscription",
					"params": map[string]any{
						"subscription": "sub-logs",
						"result":       log,
					},
				}); err != nil {
					return
				}
			}
			if opts.closeAfterRows {
				_ = conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
				return
			}
			<-r.Context().Done()
		default:
			t.Errorf("path = %q, want /v1/ws", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	return server, &wsRequests
}

func validRuntimeLogEntry(logID string) map[string]any {
	return map[string]any{
		"log_id":     logID,
		"ts":         "2026-05-19T10:00:01Z",
		"level":      "info",
		"component":  "scheduler",
		"source":     "runtime",
		"run_id":     "run-1",
		"entity_id":  "entity-1",
		"session_id": "session-1",
		"error_code": "DELIVERY_FAILED",
		"message":    "log message",
		"details": map[string]any{
			"attempt": 2,
		},
	}
}

func writeRuntimeLogJSONRPCError(t *testing.T, w http.ResponseWriter, id string, code string) {
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
