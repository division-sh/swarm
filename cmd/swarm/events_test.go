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

func TestEventsListUsesEventListV1RPC(t *testing.T) {
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
			"events":      []any{validEventObservationEvent("event-1")},
			"next_cursor": "event-cursor-2",
		})
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{
		"events", "list",
		"--run-id", "run-1",
		"--entity-id", "entity-1",
		"--event-name", "scan.requested",
		"--delivery-status", "dead_letter",
		"--subscriber-id", "agent-1",
		"--subscriber-type", "agent",
		"--reason-code", "handler_failed",
		"--has-dead-letter=false",
		"--limit", "2",
		"--cursor", "cursor-1",
		"--since", "2026-05-13T10:00:00Z",
		"--until", "2026-05-13T11:00:00Z",
	}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if captured.Method != eventObservationMethodList {
		t.Fatalf("method = %q, want %s", captured.Method, eventObservationMethodList)
	}
	wantParams := map[string]any{
		"filter": map[string]any{
			"run_id":          "run-1",
			"entity_id":       "entity-1",
			"event_name":      "scan.requested",
			"delivery_status": "dead_letter",
			"subscriber_id":   "agent-1",
			"subscriber_type": "agent",
			"reason_code":     "handler_failed",
			"has_dead_letter": false,
		},
		"limit":  float64(2),
		"cursor": "cursor-1",
		"since":  "2026-05-13T10:00:00Z",
		"until":  "2026-05-13T11:00:00Z",
	}
	if !reflect.DeepEqual(captured.Params, wantParams) {
		t.Fatalf("params = %#v, want %#v", captured.Params, wantParams)
	}
	for _, want := range []string{"EVENT AT", "scan.requested", "event-1", "run-1", "entity-1", "next_cursor=event-cursor-2"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestEventViewUsesEventGetV1RPC(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
	var captured jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Errorf("decode request: %v", err)
		}
		writeJSONRPCResult(t, w, captured.ID, validEventObservationEvent("event-1"))
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"event", "view", "event-1"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if captured.Method != eventObservationMethodGet {
		t.Fatalf("method = %q, want %s", captured.Method, eventObservationMethodGet)
	}
	if !reflect.DeepEqual(captured.Params, map[string]any{"event_id": "event-1"}) {
		t.Fatalf("params = %#v", captured.Params)
	}
	for _, want := range []string{
		"Event event-1",
		"event_name=scan.requested",
		"payload={\"priority\":\"high\"}",
		"delivery_id=delivery-1",
		"dead_letter_id=dead-1",
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestEventsFollowUsesEventSubscribeV1WS(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
	server, wsRequests := newEventObservationWSServer(t, eventObservationWSServerOptions{
		events:         []map[string]any{validEventObservationEvent("event-live-1")},
		closeAfterRows: true,
	})
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{
		"events", "follow",
		"--run-id", "run-1",
		"--event-name", "scan.requested",
		"--delivery-status", "delivered",
		"--subscriber-type", "agent",
		"--has-dead-letter",
		"--replay-since", "2026-05-13T10:00:00Z",
	}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if len(*wsRequests) != 1 {
		t.Fatalf("ws requests = %d, want 1", len(*wsRequests))
	}
	req := (*wsRequests)[0]
	if req.Method != eventObservationMethodSubscribe {
		t.Fatalf("method = %q, want %s", req.Method, eventObservationMethodSubscribe)
	}
	wantParams := map[string]any{
		"filter": map[string]any{
			"run_id":          "run-1",
			"event_name":      "scan.requested",
			"delivery_status": "delivered",
			"subscriber_type": "agent",
			"has_dead_letter": true,
		},
		"replay_since": "2026-05-13T10:00:00Z",
	}
	if !reflect.DeepEqual(req.Params, wantParams) {
		t.Fatalf("params = %#v, want %#v", req.Params, wantParams)
	}
	for _, want := range []string{"event event_id=event-live-1", "event_name=scan.requested", "run_id=run-1", "deliveries=1", "dead_letters=1"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestEventsRejectInvalidInputBeforeRequest(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeJSONRPCResult(t, w, "unexpected", map[string]any{"events": []any{}})
	}))
	defer server.Close()

	for _, tc := range []struct {
		name       string
		args       []string
		wantStderr string
	}{
		{name: "list invalid limit", args: []string{"events", "list", "--limit", "0"}, wantStderr: "--limit must be between 1 and 1000"},
		{name: "list invalid since", args: []string{"events", "list", "--since", "not-time"}, wantStderr: "--since must be an RFC3339 timestamp"},
		{name: "list invalid delivery status", args: []string{"events", "list", "--delivery-status", "done"}, wantStderr: "--delivery-status must be one of"},
		{name: "view blank event id", args: []string{"event", "view", "  "}, wantStderr: "event id is required"},
		{name: "follow rejects list limit", args: []string{"events", "follow", "--limit", "1"}, wantStderr: "unknown flag"},
		{name: "follow invalid replay since", args: []string{"events", "follow", "--replay-since", "not-time"}, wantStderr: "--replay-since must be an RFC3339 timestamp"},
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

func TestEventsFailClosedWithoutTokenBeforeRequest(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeJSONRPCResult(t, w, "unexpected", map[string]any{"events": []any{}})
	}))
	defer server.Close()

	for _, args := range [][]string{
		{"events", "list"},
		{"event", "view", "event-1"},
		{"events", "follow"},
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

func TestEventsMapFailureExitCodes(t *testing.T) {
	for _, tc := range []struct {
		name       string
		args       []string
		handler    http.HandlerFunc
		wantCode   int
		wantStderr string
	}{
		{
			name: "event not found exits five",
			args: []string{"event", "view", "missing-event"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeEventObservationJSONRPCError(t, w, req.ID, "EVENT_NOT_FOUND")
			},
			wantCode:   5,
			wantStderr: "EVENT_NOT_FOUND",
		},
		{
			name: "http auth exits four",
			args: []string{"events", "list"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
			},
			wantCode:   4,
			wantStderr: "v1 RPC HTTP 401",
		},
		{
			name: "unknown rpc error exits three",
			args: []string{"events", "list"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeEventObservationJSONRPCError(t, w, req.ID, "METHOD_UNAVAILABLE")
			},
			wantCode:   3,
			wantStderr: "METHOD_UNAVAILABLE",
		},
		{
			name: "malformed list exits three",
			args: []string{"events", "list"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeJSONRPCResult(t, w, req.ID, map[string]any{})
			},
			wantCode:   3,
			wantStderr: "events is required",
		},
		{
			name: "malformed view exits three",
			args: []string{"event", "view", "event-1"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				event := validEventObservationEvent("event-1")
				delete(event, "event_id")
				writeJSONRPCResult(t, w, req.ID, event)
			},
			wantCode:   3,
			wantStderr: "event_id is required",
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

func TestEventsFollowMalformedWSFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name               string
		subscriptionResult map[string]any
		events             []map[string]any
		wantStderr         string
	}{
		{
			name:               "missing subscription id",
			subscriptionResult: map[string]any{},
			wantStderr:         "subscription_id is required",
		},
		{
			name: "malformed notification",
			events: []map[string]any{
				func() map[string]any {
					event := validEventObservationEvent("event-1")
					delete(event, "event_id")
					return event
				}(),
			},
			wantStderr: "event_id is required",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("SWARM_API_TOKEN", "test-token")
			server, _ := newEventObservationWSServer(t, eventObservationWSServerOptions{
				subscriptionResult: tc.subscriptionResult,
				events:             tc.events,
				closeAfterRows:     true,
			})
			defer server.Close()

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"events", "follow"}, &stdout, &stderr, testRootCommandOptions(server))
			if code != 3 {
				t.Fatalf("code = %d, want 3 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.wantStderr) {
				t.Fatalf("stderr = %q, want substring %q", stderr.String(), tc.wantStderr)
			}
		})
	}
}

func TestEventsFollowMapsHandshakeAuthToAuthExit(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
	var rpcCalls atomic.Int32
	var wsCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/rpc":
			rpcCalls.Add(1)
			t.Errorf("unexpected RPC request for event follow")
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
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"events", "follow"}, &stdout, &stderr, testRootCommandOptions(server))
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

type eventObservationWSServerOptions struct {
	subscriptionResult map[string]any
	events             []map[string]any
	closeAfterRows     bool
}

func newEventObservationWSServer(t *testing.T, opts eventObservationWSServerOptions) (*httptest.Server, *[]jsonRPCRequest) {
	t.Helper()
	var mu sync.Mutex
	wsRequests := []jsonRPCRequest{}
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/rpc":
			t.Fatalf("unexpected RPC request for event follow")
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
				result = map[string]any{"subscription_id": "sub-events"}
			}
			if err := conn.WriteJSON(map[string]any{
				"jsonrpc": "2.0",
				"id":      req.ID,
				"result":  result,
			}); err != nil {
				t.Errorf("write ws subscription response: %v", err)
				return
			}
			for _, event := range opts.events {
				if err := conn.WriteJSON(map[string]any{
					"jsonrpc": "2.0",
					"method":  "rpc.subscription",
					"params": map[string]any{
						"subscription": "sub-events",
						"result":       event,
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

func validEventObservationEvent(eventID string) map[string]any {
	return map[string]any{
		"event_id":   eventID,
		"event_name": "scan.requested",
		"created_at": "2026-05-13T10:00:01Z",
		"run_id":     "run-1",
		"entity_id":  "entity-1",
		"source":     "external",
		"payload": map[string]any{
			"priority": "high",
		},
		"deliveries": []any{
			map[string]any{
				"delivery_id":     "delivery-1",
				"subscriber_type": "agent",
				"subscriber_id":   "agent-1",
				"status":          "delivered",
				"session_id":      "session-1",
			},
		},
		"dead_letters": []any{
			map[string]any{
				"dead_letter_id": "dead-1",
				"failure_type":   "handler_error",
				"retry_count":    1,
				"chain_depth":    2,
				"created_at":     "2026-05-13T10:00:02Z",
				"handler_node":   "agent-1",
				"error_message":  "boom",
			},
		},
	}
}

func writeEventObservationJSONRPCError(t *testing.T, w http.ResponseWriter, id string, code string) {
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
