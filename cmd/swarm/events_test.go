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
	setCLIAPITestToken(t, "test-token")
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
		"source_event_id=source-event-1",
		"payload={\"priority\":\"high\"}",
		"delivery_id=delivery-1",
		"created_at=2026-05-13T10:00:02Z",
		"started_at=2026-05-13T10:00:03Z",
		"finished_at=2026-05-13T10:00:05Z",
		"reason_code=retry_exhausted",
		"last_error=boom",
		"retry_count=2",
		"retry_eligible=false",
		"terminal=true",
		"delivery_dead_letter dead_letter_id=delivery-dead-1",
		"dead_letter_id=dead-1",
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
	if strings.Contains(stdout.String(), "delivery_target_route") {
		t.Fatalf("stdout exposed internal route target:\n%s", stdout.String())
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestEventReplayUsesEventReplayV1RPCWithSubscribersAndIdempotencyKey(t *testing.T) {
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
		writeJSONRPCResult(t, w, captured.ID, eventReplayTestResult())
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{
		"event", "replay", "event-1",
		"--subscriber", "agent-1",
		"--subscriber", "agent-2",
		"--idempotency-key", "idem-1",
	}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if captured.JSONRPC != "2.0" || captured.Method != eventReplayMethod {
		t.Fatalf("request jsonrpc/method = %s/%s, want 2.0/%s", captured.JSONRPC, captured.Method, eventReplayMethod)
	}
	wantParams := map[string]any{
		"event_id":        "event-1",
		"subscribers":     []any{"agent-1", "agent-2"},
		"idempotency_key": "idem-1",
	}
	if !reflect.DeepEqual(captured.Params, wantParams) {
		t.Fatalf("params = %#v, want %#v", captured.Params, wantParams)
	}
	for _, want := range []string{
		"event replay ok:",
		"event_id=event-1",
		"replay_event_id=event-replay-1",
		"audit_event_id=event-audit-1",
		"subscribers_replayed=agent-1,agent-2",
		"original_delivery delivery_id=delivery-original-1",
		"new_delivery delivery_id=delivery-new-1",
		"source_delivery_id=delivery-original-1",
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestEventReplayOmitsOptionalParamsWhenNotProvided(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var captured jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Errorf("decode request: %v", err)
		}
		result := eventReplayTestResult()
		result["subscribers_replayed"] = []any{"agent-1"}
		result["original_deliveries"] = []any{
			map[string]any{
				"delivery_id":   "delivery-original-1",
				"subscriber_id": "agent-1",
				"session_id":    "session-original-1",
				"status":        "delivered",
				"attempt":       1,
			},
		}
		result["new_deliveries"] = []any{
			map[string]any{
				"delivery_id":        "delivery-new-1",
				"subscriber_id":      "agent-1",
				"session_id":         "session-new-1",
				"status":             "pending",
				"attempt":            1,
				"source_delivery_id": "delivery-original-1",
			},
		}
		writeJSONRPCResult(t, w, captured.ID, result)
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"event", "replay", "event-1"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	wantParams := map[string]any{"event_id": "event-1"}
	if !reflect.DeepEqual(captured.Params, wantParams) {
		t.Fatalf("params = %#v, want %#v", captured.Params, wantParams)
	}
}

func TestEventsFollowUsesEventSubscribeV1WS(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
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
	setCLIAPITestToken(t, "test-token")
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
		{name: "replay missing event id", args: []string{"event", "replay"}, wantStderr: "accepts 1 arg(s)"},
		{name: "replay blank event id", args: []string{"event", "replay", "  "}, wantStderr: "event id is required"},
		{name: "replay blank subscriber", args: []string{"event", "replay", "event-1", "--subscriber", "  "}, wantStderr: "--subscriber must be a non-empty agent id"},
		{name: "replay extra arg", args: []string{"event", "replay", "event-1", "extra"}, wantStderr: "accepts 1 arg(s)"},
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
		{"event", "replay", "event-1"},
		{"events", "follow"},
	} {
		calls.Store(0)
		var stdout, stderr bytes.Buffer
		code := executeRootCommandWithOptions(context.Background(), t.TempDir(), args, &stdout, &stderr, testRootCommandOptions(server))
		if code != 4 {
			t.Fatalf("args=%v code = %d, want 4 stdout=%s stderr=%s", args, code, stdout.String(), stderr.String())
		}
		if !strings.Contains(stderr.String(), "API token source is required") {
			t.Fatalf("stderr = %q, want token failure", stderr.String())
		}
		if calls.Load() != 0 {
			t.Fatalf("args=%v calls = %d, want 0", args, calls.Load())
		}
	}
}

func TestEventsMapFailureExitCodes(t *testing.T) {
	replayConflictErrors := []string{
		"EVENT_REPLAY_NO_DELIVERY_HISTORY",
		"EVENT_REPLAY_SUBSCRIBER_NOT_ORIGINAL",
		"EVENT_REPLAY_SUBSCRIBER_UNAVAILABLE",
		"EVENT_REPLAY_NOT_ELIGIBLE",
		"PAYLOAD_VALIDATION_FAILED",
		"IDEMPOTENCY_CONFLICT",
	}
	for _, code := range replayConflictErrors {
		code := code
		t.Run("event replay "+code+" exits six", func(t *testing.T) {
			setCLIAPITestToken(t, "test-token")
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeEventReplayJSONRPCError(t, w, req.ID, code)
			}))
			defer server.Close()

			var stdout, stderr bytes.Buffer
			exit := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"event", "replay", "event-1"}, &stdout, &stderr, testRootCommandOptions(server))
			if exit != 6 {
				t.Fatalf("code = %d, want 6 stdout=%s stderr=%s", exit, stdout.String(), stderr.String())
			}
			if !strings.Contains(stderr.String(), code) {
				t.Fatalf("stderr = %q, want substring %q", stderr.String(), code)
			}
		})
	}

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
			name: "event replay not found exits five",
			args: []string{"event", "replay", "missing-event"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeEventReplayJSONRPCError(t, w, req.ID, "EVENT_NOT_FOUND")
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
			name: "event replay http auth exits four",
			args: []string{"event", "replay", "event-1"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
			},
			wantCode:   4,
			wantStderr: "v1 RPC HTTP 401",
		},
		{
			name: "event replay http runtime exits three",
			args: []string{"event", "replay", "event-1"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "unavailable", http.StatusServiceUnavailable)
			},
			wantCode:   3,
			wantStderr: "v1 RPC HTTP 503",
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
			name: "event replay unauthorized rpc exits four",
			args: []string{"event", "replay", "event-1"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeEventReplayJSONRPCError(t, w, req.ID, "UNAUTHORIZED")
			},
			wantCode:   4,
			wantStderr: "UNAUTHORIZED",
		},
		{
			name: "event replay unknown rpc exits three",
			args: []string{"event", "replay", "event-1"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeEventReplayJSONRPCError(t, w, req.ID, "METHOD_UNAVAILABLE")
			},
			wantCode:   3,
			wantStderr: "METHOD_UNAVAILABLE",
		},
		{
			name: "malformed event replay exits three",
			args: []string{"event", "replay", "event-1"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				result := eventReplayTestResult()
				delete(result, "replay_event_id")
				writeJSONRPCResult(t, w, req.ID, result)
			},
			wantCode:   3,
			wantStderr: "replay_event_id is required",
		},
		{
			name: "malformed event replay subscribers exits three",
			args: []string{"event", "replay", "event-1"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				result := eventReplayTestResult()
				delete(result, "subscribers_replayed")
				writeJSONRPCResult(t, w, req.ID, result)
			},
			wantCode:   3,
			wantStderr: "subscribers_replayed is required",
		},
		{
			name: "malformed event replay source lineage exits three",
			args: []string{"event", "replay", "event-1"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				result := eventReplayTestResult()
				newDeliveries := result["new_deliveries"].([]any)
				newDelivery := newDeliveries[0].(map[string]any)
				delete(newDelivery, "source_delivery_id")
				writeJSONRPCResult(t, w, req.ID, result)
			},
			wantCode:   3,
			wantStderr: "new_deliveries[0].source_delivery_id is required",
		},
		{
			name: "malformed event replay status exits three",
			args: []string{"event", "replay", "event-1"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				result := eventReplayTestResult()
				originalDeliveries := result["original_deliveries"].([]any)
				originalDelivery := originalDeliveries[0].(map[string]any)
				originalDelivery["status"] = "locally_replayed"
				writeJSONRPCResult(t, w, req.ID, result)
			},
			wantCode:   3,
			wantStderr: "original_deliveries[0].status",
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
		{
			name: "malformed view delivery retry count exits three",
			args: []string{"event", "view", "event-1"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				event := validEventObservationEvent("event-1")
				delivery := event["deliveries"].([]any)[0].(map[string]any)
				delete(delivery, "retry_count")
				writeJSONRPCResult(t, w, req.ID, event)
			},
			wantCode:   3,
			wantStderr: "retry_count is required",
		},
		{
			name: "malformed view delivery retry eligible exits three",
			args: []string{"event", "view", "event-1"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				event := validEventObservationEvent("event-1")
				delivery := event["deliveries"].([]any)[0].(map[string]any)
				delete(delivery, "retry_eligible")
				writeJSONRPCResult(t, w, req.ID, event)
			},
			wantCode:   3,
			wantStderr: "retry_eligible is required",
		},
		{
			name: "malformed view delivery terminal exits three",
			args: []string{"event", "view", "event-1"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				event := validEventObservationEvent("event-1")
				delivery := event["deliveries"].([]any)[0].(map[string]any)
				delete(delivery, "terminal")
				writeJSONRPCResult(t, w, req.ID, event)
			},
			wantCode:   3,
			wantStderr: "terminal is required",
		},
		{
			name: "malformed view delivery created timestamp exits three",
			args: []string{"event", "view", "event-1"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				event := validEventObservationEvent("event-1")
				delivery := event["deliveries"].([]any)[0].(map[string]any)
				delivery["created_at"] = "not-a-timestamp"
				writeJSONRPCResult(t, w, req.ID, event)
			},
			wantCode:   3,
			wantStderr: "created_at must be an RFC3339 timestamp",
		},
		{
			name: "malformed view delivery started timestamp exits three",
			args: []string{"event", "view", "event-1"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				event := validEventObservationEvent("event-1")
				delivery := event["deliveries"].([]any)[0].(map[string]any)
				delivery["started_at"] = "not-a-timestamp"
				writeJSONRPCResult(t, w, req.ID, event)
			},
			wantCode:   3,
			wantStderr: "started_at must be an RFC3339 timestamp",
		},
		{
			name: "malformed view delivery finished timestamp exits three",
			args: []string{"event", "view", "event-1"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				event := validEventObservationEvent("event-1")
				delivery := event["deliveries"].([]any)[0].(map[string]any)
				delivery["finished_at"] = "not-a-timestamp"
				writeJSONRPCResult(t, w, req.ID, event)
			},
			wantCode:   3,
			wantStderr: "finished_at must be an RFC3339 timestamp",
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
			setCLIAPITestToken(t, "test-token")
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
	setCLIAPITestToken(t, "test-token")
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
		"event_id":        eventID,
		"event_name":      "scan.requested",
		"created_at":      "2026-05-13T10:00:01Z",
		"run_id":          "run-1",
		"entity_id":       "entity-1",
		"source_event_id": "source-event-1",
		"source":          "external",
		"payload": map[string]any{
			"priority": "high",
		},
		"deliveries": []any{
			map[string]any{
				"delivery_id":     "delivery-1",
				"subscriber_type": "agent",
				"subscriber_id":   "agent-1",
				"status":          "dead_letter",
				"session_id":      "session-1",
				"reason_code":     "retry_exhausted",
				"last_error":      "boom",
				"retry_count":     2,
				"retry_eligible":  false,
				"terminal":        true,
				"created_at":      "2026-05-13T10:00:02Z",
				"started_at":      "2026-05-13T10:00:03Z",
				"finished_at":     "2026-05-13T10:00:05Z",
				"dead_letters": []any{
					map[string]any{
						"dead_letter_id": "delivery-dead-1",
						"failure_type":   "retry_exhausted",
						"retry_count":    2,
						"chain_depth":    3,
						"created_at":     "2026-05-13T10:00:05Z",
						"handler_node":   "agent-1",
						"error_message":  "boom",
					},
				},
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

func eventReplayTestResult() map[string]any {
	return map[string]any{
		"event_id":             "event-1",
		"replay_event_id":      "event-replay-1",
		"audit_event_id":       "event-audit-1",
		"subscribers_replayed": []any{"agent-1", "agent-2"},
		"original_deliveries":  []any{eventReplayTestDelivery("delivery-original-1", "agent-1", "session-original-1", "delivered", 1, ""), eventReplayTestDelivery("delivery-original-2", "agent-2", "session-original-2", "failed", 2, "")},
		"new_deliveries":       []any{eventReplayTestDelivery("delivery-new-1", "agent-1", "session-new-1", "pending", 1, "delivery-original-1"), eventReplayTestDelivery("delivery-new-2", "agent-2", "session-new-2", "pending", 1, "delivery-original-2")},
	}
}

func eventReplayTestDelivery(deliveryID, subscriberID, sessionID, status string, attempt int, sourceDeliveryID string) map[string]any {
	delivery := map[string]any{
		"delivery_id":   deliveryID,
		"subscriber_id": subscriberID,
		"session_id":    sessionID,
		"status":        status,
		"attempt":       attempt,
	}
	if sourceDeliveryID != "" {
		delivery["source_delivery_id"] = sourceDeliveryID
	}
	return delivery
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

func writeEventReplayJSONRPCError(t *testing.T, w http.ResponseWriter, id string, code string) {
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
				"details":        map[string]any{"event_id": "event-1"},
				"retryable":      false,
				"correlation_id": "corr-event-replay",
			},
		},
	}); err != nil {
		t.Fatalf("encode response: %v", err)
	}
}
