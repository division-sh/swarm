package apiv1

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/store"
	"github.com/gorilla/websocket"
)

func TestHandlerWebSocketHealthSubscribeAndUnsubscribe(t *testing.T) {
	now := time.Unix(1700001000, 0).UTC()
	readOpts := OperatorReadOptions{
		Now:      func() time.Time { return now },
		Ready:    func() bool { return true },
		Database: fakePinger{err: nil},
		Bundle: runtimecontracts.BundleIdentity{
			WorkflowName:    "review",
			WorkflowVersion: "1.2.3",
			Fingerprint:     "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		},
	}
	handler := testHandler(t, Options{
		AuthTokens:    []string{testToken},
		Handlers:      OperatorReadHandlers(readOpts),
		Subscriptions: OperatorSubscriptions(readOpts, SubscriptionRuntimeOptions{HealthInterval: time.Hour, QueueSize: 4}),
	})

	health := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"health","method":"health.check","params":{}}`)
	if health.Error != nil {
		t.Fatalf("health.check error = %#v", health.Error)
	}

	server := httptest.NewServer(handler)
	defer server.Close()
	conn := dialTestWS(t, server.URL)
	defer conn.Close()

	writeWSRequest(t, conn, map[string]any{
		"jsonrpc": "2.0",
		"id":      "sub-health",
		"method":  "health.subscribe",
		"params":  map[string]any{},
	})
	subscribe := readWSResponse(t, conn)
	if subscribe.Error != nil {
		t.Fatalf("health.subscribe error = %#v", subscribe.Error)
	}
	subscriptionID, ok := asMap(t, subscribe.Result)["subscription_id"].(string)
	if !ok || subscriptionID == "" {
		t.Fatalf("health.subscribe result = %#v, want subscription_id", subscribe.Result)
	}

	notification := readWSNotification(t, conn)
	if notification.Method != "rpc.subscription" || notification.Params.Subscription != subscriptionID {
		t.Fatalf("notification = %#v, want rpc.subscription for %s", notification, subscriptionID)
	}
	if got, want := asMap(t, notification.Params.Result), asMap(t, health.Result); !sameJSON(got, want) {
		t.Fatalf("health notification = %#v, want health.check result %#v", got, want)
	}

	writeWSRequest(t, conn, map[string]any{
		"jsonrpc": "2.0",
		"id":      "unsub-health",
		"method":  "rpc.unsubscribe",
		"params": map[string]any{
			"subscription_id": subscriptionID,
		},
	})
	unsubscribe := readWSResponse(t, conn)
	if unsubscribe.Error != nil || asMap(t, unsubscribe.Result)["ok"] != true {
		t.Fatalf("rpc.unsubscribe response = %#v", unsubscribe)
	}
}

func TestHandlerWebSocketEventSubscribeUsesOwnerFilterAndReplay(t *testing.T) {
	base := time.Unix(1700001100, 0).UTC()
	hasDeadLetter := false
	observability := &fakeObservabilityReadStore{
		events: map[string]store.OperatorEventFull{
			"evt-1": {
				EventID:       "evt-1",
				EventName:     "scan.requested",
				ExecutionMode: "live",
				RunID:         "run-1",
				EntityID:      "entity-1",
				CreatedAt:     base.Add(time.Second),
				Source:        "runtime",
				ProducerType:  events.EventProducerPlatform,
				Payload:       map[string]any{"ok": true},
				Deliveries:    []store.OperatorEventDelivery{},
				DeadLetters:   []store.OperatorDeadLetterRecord{},
			},
			"evt-payload-only": {
				EventID:       "evt-payload-only",
				EventName:     "scan.requested",
				ExecutionMode: "live",
				RunID:         "run-1",
				CreatedAt:     base.Add(2 * time.Second),
				Source:        "runtime",
				ProducerType:  events.EventProducerPlatform,
				Payload:       map[string]any{"entity_id": "entity-1", "marker": "payload-only"},
				Deliveries:    []store.OperatorEventDelivery{},
				DeadLetters:   []store.OperatorDeadLetterRecord{},
			},
		},
	}
	readOpts := OperatorReadOptions{
		Now:           func() time.Time { return base.Add(10 * time.Second) },
		Observability: observability,
	}
	handler := testHandler(t, Options{
		AuthTokens:    []string{testToken},
		Subscriptions: OperatorSubscriptions(readOpts, SubscriptionRuntimeOptions{PollInterval: time.Hour, QueueSize: 4}),
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	conn := dialTestWS(t, server.URL)
	defer conn.Close()

	writeWSRequest(t, conn, map[string]any{
		"jsonrpc": "2.0",
		"id":      "sub-events",
		"method":  "event.subscribe",
		"params": map[string]any{
			"filter": map[string]any{
				"run_id":          "run-1",
				"entity_id":       "entity-1",
				"event_name":      "scan.requested",
				"has_dead_letter": hasDeadLetter,
			},
			"replay_since": base.Format(time.RFC3339Nano),
		},
	})
	subscribe := readWSResponse(t, conn)
	if subscribe.Error != nil {
		t.Fatalf("event.subscribe error = %#v", subscribe.Error)
	}
	subscriptionID := asMap(t, subscribe.Result)["subscription_id"].(string)
	notification := readWSNotification(t, conn)
	if notification.Params.Subscription != subscriptionID {
		t.Fatalf("event notification subscription = %q, want %q", notification.Params.Subscription, subscriptionID)
	}
	if got := asMap(t, notification.Params.Result)["event_id"]; got != "evt-1" {
		t.Fatalf("event notification result = %#v, want evt-1", notification.Params.Result)
	}
	requireNoWSMessage(t, conn, apiv1WebSocketNoMessageTimeout, "payload-only event notification")
	if observability.lastEventList.Filter.RunID != "run-1" ||
		observability.lastEventList.Filter.EntityID != "entity-1" ||
		observability.lastEventList.Filter.EventName != "scan.requested" ||
		observability.lastEventList.Filter.HasDeadLetter == nil ||
		*observability.lastEventList.Filter.HasDeadLetter != hasDeadLetter {
		t.Fatalf("event.subscribe filter = %#v", observability.lastEventList.Filter)
	}
	if observability.lastEventList.Since == nil || !observability.lastEventList.Since.Equal(base) {
		t.Fatalf("event.subscribe since = %#v, want %s", observability.lastEventList.Since, base)
	}
	if observability.lastEventList.Order != "asc" {
		t.Fatalf("event.subscribe order = %q, want asc", observability.lastEventList.Order)
	}
}

func TestEventListFilterValidationCoversListAndSubscribe(t *testing.T) {
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Observability: &fakeObservabilityReadStore{},
		}),
		Subscriptions: OperatorSubscriptions(OperatorReadOptions{
			Observability: &fakeObservabilityReadStore{},
		}, SubscriptionRuntimeOptions{PollInterval: time.Hour, QueueSize: 4}),
	})

	listUnknown := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"list-unknown","method":"event.list","params":{"filter":{"event_nmae":"typo"}}}`)
	if listUnknown.Error == nil || listUnknown.Error.Code != codeInvalidParams {
		t.Fatalf("event.list unknown filter error = %#v, want invalid params", listUnknown.Error)
	}
	if details := asMap(t, asMap(t, listUnknown.Error.Data)["details"]); details["field"] != "filter.event_nmae" {
		t.Fatalf("event.list unknown filter details = %#v", details)
	}

	listEnum := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"list-enum","method":"event.list","params":{"filter":{"delivery_status":"done"}}}`)
	if listEnum.Error == nil || listEnum.Error.Code != codeInvalidParams {
		t.Fatalf("event.list enum error = %#v, want invalid params", listEnum.Error)
	}
	if details := asMap(t, asMap(t, listEnum.Error.Data)["details"]); details["field"] != "filter.delivery_status" {
		t.Fatalf("event.list enum details = %#v", details)
	}

	listMissingRun := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"list-missing-run","method":"event.list","params":{"filter":{"event_name":"scan.requested"}}}`)
	if listMissingRun.Error == nil || listMissingRun.Error.Code != codeInvalidParams {
		t.Fatalf("event.list missing run scope error = %#v, want invalid params", listMissingRun.Error)
	}
	if details := asMap(t, asMap(t, listMissingRun.Error.Data)["details"]); details["field"] != "filter.run_id" {
		t.Fatalf("event.list missing run scope details = %#v", details)
	}

	server := httptest.NewServer(handler)
	defer server.Close()
	conn := dialTestWS(t, server.URL)
	defer conn.Close()
	writeWSRequest(t, conn, map[string]any{
		"jsonrpc": "2.0",
		"id":      "sub-unknown",
		"method":  "event.subscribe",
		"params": map[string]any{
			"filter": map[string]any{"event_nmae": "typo"},
		},
	})
	subUnknown := readWSResponse(t, conn)
	if subUnknown.Error == nil || subUnknown.Error.Code != codeInvalidParams {
		t.Fatalf("event.subscribe unknown filter error = %#v, want invalid params", subUnknown.Error)
	}
	if details := asMap(t, asMap(t, subUnknown.Error.Data)["details"]); details["field"] != "filter.event_nmae" {
		t.Fatalf("event.subscribe unknown filter details = %#v", details)
	}

	writeWSRequest(t, conn, map[string]any{
		"jsonrpc": "2.0",
		"id":      "sub-enum",
		"method":  "event.subscribe",
		"params": map[string]any{
			"filter": map[string]any{"subscriber_type": "system"},
		},
	})
	subEnum := readWSResponse(t, conn)
	if subEnum.Error == nil || subEnum.Error.Code != codeInvalidParams {
		t.Fatalf("event.subscribe enum error = %#v, want invalid params", subEnum.Error)
	}
	if details := asMap(t, asMap(t, subEnum.Error.Data)["details"]); details["field"] != "filter.subscriber_type" {
		t.Fatalf("event.subscribe enum details = %#v", details)
	}

	writeWSRequest(t, conn, map[string]any{
		"jsonrpc": "2.0",
		"id":      "sub-missing-run",
		"method":  "event.subscribe",
		"params": map[string]any{
			"filter": map[string]any{"event_name": "scan.requested"},
		},
	})
	subMissingRun := readWSResponse(t, conn)
	if subMissingRun.Error == nil || subMissingRun.Error.Code != codeInvalidParams {
		t.Fatalf("event.subscribe missing run scope error = %#v, want invalid params", subMissingRun.Error)
	}
	if details := asMap(t, asMap(t, subMissingRun.Error.Data)["details"]); details["field"] != "filter.run_id" {
		t.Fatalf("event.subscribe missing run scope details = %#v", details)
	}
}

func TestHandlerWebSocketSubscriptionOwnerErrorClosesConnection(t *testing.T) {
	observability := &fakeObservabilityReadStore{listErr: errors.New("store unavailable")}
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Subscriptions: OperatorSubscriptions(OperatorReadOptions{
			Observability: observability,
		}, SubscriptionRuntimeOptions{PollInterval: time.Hour, QueueSize: 4}),
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	conn := dialTestWS(t, server.URL)
	defer conn.Close()

	writeWSRequest(t, conn, map[string]any{
		"jsonrpc": "2.0",
		"id":      "sub-events",
		"method":  "event.subscribe",
		"params": map[string]any{
			"filter": map[string]any{"run_id": "run-1"},
		},
	})
	raw, err := readWSMessageWithTimeout(t, conn, apiv1WebSocketReadTimeout)
	if err != nil {
		return
	}
	var subscribe rpcResponse
	if err := json.Unmarshal(raw, &subscribe); err != nil {
		t.Fatalf("decode event.subscribe response before close: %v raw=%s", err, raw)
	}
	if subscribe.Error != nil {
		t.Fatalf("event.subscribe response error = %#v", subscribe.Error)
	}
	if _, err := readWSMessageWithTimeout(t, conn, apiv1WebSocketReadTimeout); err == nil {
		t.Fatal("websocket stayed open after owner read error, want fail-closed disconnect")
	}
}

func TestHandlerWebSocketRuntimeSubscribeLogsOwnerErrorClosesConnection(t *testing.T) {
	observability := &fakeObservabilityReadStore{runtimeLogsErr: errors.New("runtime logs unavailable")}
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Subscriptions: OperatorSubscriptions(OperatorReadOptions{
			Observability: observability,
		}, SubscriptionRuntimeOptions{PollInterval: time.Hour, QueueSize: 4}),
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	conn := dialTestWS(t, server.URL)
	defer conn.Close()

	writeWSRequest(t, conn, map[string]any{
		"jsonrpc": "2.0",
		"id":      "sub-runtime-logs",
		"method":  "runtime.subscribe_logs",
		"params":  map[string]any{},
	})
	raw, err := readWSMessageWithTimeout(t, conn, apiv1WebSocketReadTimeout)
	if err != nil {
		return
	}
	var subscribe rpcResponse
	if err := json.Unmarshal(raw, &subscribe); err != nil {
		t.Fatalf("decode runtime.subscribe_logs response before close: %v raw=%s", err, raw)
	}
	if subscribe.Error != nil {
		t.Fatalf("runtime.subscribe_logs response error = %#v", subscribe.Error)
	}
	if _, err := readWSMessageWithTimeout(t, conn, apiv1WebSocketReadTimeout); err == nil {
		t.Fatal("websocket stayed open after runtime log owner read error, want fail-closed disconnect")
	}
}

func TestHandlerWebSocketRunSubscribeTraceUsesOwnerReplayAndRunNotFound(t *testing.T) {
	base := time.Unix(1700001200, 0).UTC()
	missing := &fakeObservabilityReadStore{traceErr: store.ErrRunNotFound}
	missingHandler := testHandler(t, Options{
		AuthTokens:    []string{testToken},
		Subscriptions: OperatorSubscriptions(OperatorReadOptions{Observability: missing}, SubscriptionRuntimeOptions{PollInterval: time.Hour, QueueSize: 4}),
	})
	missingServer := httptest.NewServer(missingHandler)
	defer missingServer.Close()
	missingConn := dialTestWS(t, missingServer.URL)
	defer missingConn.Close()
	writeWSRequest(t, missingConn, map[string]any{
		"jsonrpc": "2.0",
		"id":      "missing-trace",
		"method":  "run.subscribe_trace",
		"params": map[string]any{
			"run_id":       "missing-run",
			"replay_since": base.Format(time.RFC3339Nano),
		},
	})
	missingResp := readWSResponse(t, missingConn)
	if missingResp.Error == nil || asMap(t, missingResp.Error.Data)["code"] != RunNotFoundCode {
		t.Fatalf("missing run response = %#v, want RUN_NOT_FOUND", missingResp)
	}

	observability := &fakeObservabilityReadStore{
		traceRows: map[string][]store.RunDebugTraceRow{
			"run-1": {{
				EventID:        "evt-1",
				EventName:      "scan.requested",
				EventCreatedAt: base.Add(time.Second),
			}},
		},
	}
	handler := testHandler(t, Options{
		AuthTokens:    []string{testToken},
		Subscriptions: OperatorSubscriptions(OperatorReadOptions{Observability: observability}, SubscriptionRuntimeOptions{PollInterval: time.Hour, QueueSize: 4}),
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	conn := dialTestWS(t, server.URL)
	defer conn.Close()

	writeWSRequest(t, conn, map[string]any{
		"jsonrpc": "2.0",
		"id":      "sub-trace",
		"method":  "run.subscribe_trace",
		"params": map[string]any{
			"run_id":       "run-1",
			"replay_since": base.Format(time.RFC3339Nano),
			"filter": map[string]any{
				"event_name":      []any{"scan.requested"},
				"entity_id":       []any{"entity-1"},
				"delivery_status": []any{"delivered"},
				"subscriber_id":   []any{"agent-1"},
				"subscriber_type": []any{"agent"},
			},
		},
	})
	subscribe := readWSResponse(t, conn)
	if subscribe.Error != nil {
		t.Fatalf("run.subscribe_trace error = %#v", subscribe.Error)
	}
	notification := readWSNotification(t, conn)
	if got := asMap(t, notification.Params.Result)["event_id"]; got != "evt-1" {
		t.Fatalf("trace notification result = %#v, want evt-1", notification.Params.Result)
	}
	if observability.lastTrace.Since == nil || !observability.lastTrace.Since.Equal(base) {
		t.Fatalf("run.subscribe_trace since = %#v, want %s", observability.lastTrace.Since, base)
	}
	if !observability.lastTrace.ExcludeRuntimeLogs {
		t.Fatalf("run.subscribe_trace ExcludeRuntimeLogs = false, want default true")
	}
	if got := observability.lastTrace.Filter; len(got.EventNames) != 1 || got.EventNames[0] != "scan.requested" || len(got.EntityIDs) != 1 || got.EntityIDs[0] != "entity-1" || len(got.DeliveryStatuses) != 1 || got.DeliveryStatuses[0] != "delivered" || len(got.SubscriberIDs) != 1 || got.SubscriberIDs[0] != "agent-1" || len(got.SubscriberTypes) != 1 || got.SubscriberTypes[0] != "agent" {
		t.Fatalf("run.subscribe_trace filter = %#v", got)
	}

	verboseConn := dialTestWS(t, server.URL)
	defer verboseConn.Close()
	writeWSRequest(t, verboseConn, map[string]any{
		"jsonrpc": "2.0",
		"id":      "sub-trace-verbose",
		"method":  "run.subscribe_trace",
		"params": map[string]any{
			"run_id":           "run-1",
			"include_internal": true,
			"replay_since":     base.Format(time.RFC3339Nano),
		},
	})
	verboseSubscribe := readWSResponse(t, verboseConn)
	if verboseSubscribe.Error != nil {
		t.Fatalf("run.subscribe_trace include_internal error = %#v", verboseSubscribe.Error)
	}
	_ = readWSNotification(t, verboseConn)
	if observability.lastTrace.ExcludeRuntimeLogs {
		t.Fatalf("run.subscribe_trace include_internal ExcludeRuntimeLogs = true, want false")
	}
}

func TestRunSubscribeTraceFilterValidation(t *testing.T) {
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Subscriptions: OperatorSubscriptions(OperatorReadOptions{
			Observability: &fakeObservabilityReadStore{},
		}, SubscriptionRuntimeOptions{PollInterval: time.Hour, QueueSize: 4}),
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	for _, tc := range []struct {
		name      string
		params    map[string]any
		wantField string
	}{
		{name: "unknown top-level limit", params: map[string]any{"run_id": "run-1", "limit": 1}, wantField: "limit"},
		{name: "unknown top-level until", params: map[string]any{"run_id": "run-1", "until": "2026-05-13T10:05:00Z"}, wantField: "until"},
		{name: "invalid include internal", params: map[string]any{"run_id": "run-1", "include_internal": "yes"}, wantField: "include_internal"},
		{name: "unknown filter field", params: map[string]any{"run_id": "run-1", "filter": map[string]any{"event_nmae": []any{"scan.requested"}}}, wantField: "filter.event_nmae"},
		{name: "invalid delivery status", params: map[string]any{"run_id": "run-1", "filter": map[string]any{"delivery_status": []any{"done"}}}, wantField: "filter.delivery_status"},
		{name: "invalid subscriber type", params: map[string]any{"run_id": "run-1", "filter": map[string]any{"subscriber_type": []any{"system"}}}, wantField: "filter.subscriber_type"},
		{name: "invalid entity id", params: map[string]any{"run_id": "run-1", "filter": map[string]any{"entity_id": []any{"bad id!"}}}, wantField: "filter.entity_id"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := dialTestWS(t, server.URL)
			defer conn.Close()
			writeWSRequest(t, conn, map[string]any{
				"jsonrpc": "2.0",
				"id":      "bad-run-trace",
				"method":  "run.subscribe_trace",
				"params":  tc.params,
			})
			resp := readWSResponse(t, conn)
			if resp.Error == nil || resp.Error.Code != codeInvalidParams {
				t.Fatalf("run.subscribe_trace response = %#v, want invalid params", resp)
			}
			if details := asMap(t, asMap(t, resp.Error.Data)["details"]); details["field"] != tc.wantField {
				t.Fatalf("run.subscribe_trace details = %#v, want field %q", details, tc.wantField)
			}
		})
	}
}

func TestHandlerWebSocketRuntimeSubscribeLogsUsesOwnerFiltersAndReplay(t *testing.T) {
	base := time.Unix(1700001300, 0).UTC()
	observability := &fakeObservabilityReadStore{
		logs: []store.OperatorRuntimeLogEntry{
			{
				LogID:     "old-log",
				TS:        base.Add(-time.Second),
				Level:     "error",
				Component: "scheduler",
				Source:    "agent-1",
				RunID:     "run-1",
				EntityID:  "entity-1",
				SessionID: "sess-1",
				ErrorCode: "E_OLD",
				Message:   "old log",
			},
			{
				LogID:     "log-1",
				TS:        base.Add(time.Second),
				Level:     "error",
				Component: "scheduler",
				Source:    "agent-1",
				RunID:     "run-1",
				EntityID:  "entity-1",
				SessionID: "sess-1",
				ErrorCode: "E_RUNTIME",
				Message:   "runtime failed",
				Details:   map[string]any{"action": "dispatch"},
			},
		},
	}
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Subscriptions: OperatorSubscriptions(OperatorReadOptions{
			Now:           func() time.Time { return base.Add(10 * time.Second) },
			Observability: observability,
		}, SubscriptionRuntimeOptions{PollInterval: time.Hour, QueueSize: 4}),
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	conn := dialTestWS(t, server.URL)
	defer conn.Close()

	writeWSRequest(t, conn, map[string]any{
		"jsonrpc": "2.0",
		"id":      "sub-runtime-logs",
		"method":  "runtime.subscribe_logs",
		"params": map[string]any{
			"bundle_hash":  "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			"run_id":       "run-1",
			"entity_id":    "entity-1",
			"session_id":   "sess-1",
			"component":    "scheduler",
			"level":        "error",
			"error_code":   "E_RUNTIME",
			"source":       "agent-1",
			"replay_since": base.Format(time.RFC3339Nano),
		},
	})
	subscribe := readWSResponse(t, conn)
	if subscribe.Error != nil {
		t.Fatalf("runtime.subscribe_logs error = %#v", subscribe.Error)
	}
	subscriptionID := asMap(t, subscribe.Result)["subscription_id"].(string)
	notification := readWSNotification(t, conn)
	if notification.Params.Subscription != subscriptionID {
		t.Fatalf("runtime log notification subscription = %q, want %q", notification.Params.Subscription, subscriptionID)
	}
	got := asMap(t, notification.Params.Result)
	if got["log_id"] != "log-1" || got["message"] != "runtime failed" {
		t.Fatalf("runtime log notification result = %#v", got)
	}
	if observability.lastRuntimeLogs.RunID != "run-1" ||
		observability.lastRuntimeLogs.BundleHash != "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" ||
		observability.lastRuntimeLogs.EntityID != "entity-1" ||
		observability.lastRuntimeLogs.SessionID != "sess-1" ||
		observability.lastRuntimeLogs.Component != "scheduler" ||
		observability.lastRuntimeLogs.Level != "error" ||
		observability.lastRuntimeLogs.ErrorCode != "E_RUNTIME" ||
		observability.lastRuntimeLogs.Source != "agent-1" {
		t.Fatalf("runtime.subscribe_logs filters = %#v", observability.lastRuntimeLogs)
	}
	if observability.lastRuntimeLogs.Since == nil || !observability.lastRuntimeLogs.Since.Equal(base) {
		t.Fatalf("runtime.subscribe_logs since = %#v, want %s", observability.lastRuntimeLogs.Since, base)
	}
	if observability.lastRuntimeLogs.Order != "asc" || observability.lastRuntimeLogs.Limit != subscriptionBatchLimit || observability.lastRuntimeLogs.Cursor != "" {
		t.Fatalf("runtime.subscribe_logs paging = %#v, want asc limit %d without cursor", observability.lastRuntimeLogs, subscriptionBatchLimit)
	}

	writeWSRequest(t, conn, map[string]any{
		"jsonrpc": "2.0",
		"id":      "unsub-runtime-logs",
		"method":  "rpc.unsubscribe",
		"params":  map[string]any{"subscription_id": subscriptionID},
	})
	unsubscribe := readWSResponse(t, conn)
	if unsubscribe.Error != nil || asMap(t, unsubscribe.Result)["ok"] != true {
		t.Fatalf("rpc.unsubscribe runtime logs response = %#v", unsubscribe)
	}
}

func TestRuntimeLogSubscriptionPreservesWatermarkAcrossPolls(t *testing.T) {
	base := time.Unix(1700001350, 0).UTC()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	session := &webSocketSession{
		ctx:    ctx,
		cancel: cancel,
		out:    make(chan outboundMessage, 4),
		subs:   map[string]context.CancelFunc{},
	}
	observability := &fakeObservabilityReadStore{
		logs: []store.OperatorRuntimeLogEntry{
			{
				LogID:   "log-1",
				TS:      base.Add(time.Second),
				Message: "first",
			},
		},
	}
	state := &runtimeLogSubscriptionState{since: &base}
	runtime := &SubscriptionRuntime{}
	opts := store.OperatorRuntimeLogListOptions{
		Limit: subscriptionBatchLimit,
		Order: "asc",
	}

	if !runtime.emitRuntimeLogNotifications(ctx, session, "sub-runtime-logs", observability, opts, state) {
		t.Fatal("first runtime log emit returned false")
	}
	if observability.lastRuntimeLogs.Since == nil || !observability.lastRuntimeLogs.Since.Equal(base) {
		t.Fatalf("first runtime log since = %#v, want %s", observability.lastRuntimeLogs.Since, base)
	}
	if state.since == nil || !state.since.After(base) {
		t.Fatalf("runtime log watermark after first poll = %#v, want after %s", state.since, base)
	}

	observability.logs = append(observability.logs, store.OperatorRuntimeLogEntry{
		LogID:   "log-2",
		TS:      base.Add(2 * time.Second),
		Message: "second",
	})
	if !runtime.emitRuntimeLogNotifications(ctx, session, "sub-runtime-logs", observability, opts, state) {
		t.Fatal("second runtime log emit returned false")
	}
	if observability.lastRuntimeLogs.Since == nil || !observability.lastRuntimeLogs.Since.After(base) {
		t.Fatalf("second runtime log since = %#v, want advanced watermark after %s", observability.lastRuntimeLogs.Since, base)
	}
	if got := len(session.out); got != 2 {
		t.Fatalf("runtime log notifications = %d, want 2 without replaying old log", got)
	}
	firstID := outboundNotificationResultString(t, <-session.out, "log_id")
	secondID := outboundNotificationResultString(t, <-session.out, "log_id")
	if firstID != "log-1" || secondID != "log-2" {
		t.Fatalf("runtime log notifications = %q then %q, want log-1 then log-2", firstID, secondID)
	}
}

func TestRuntimeSubscribeLogsRejectsSnapshotControls(t *testing.T) {
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Subscriptions: OperatorSubscriptions(OperatorReadOptions{
			Observability: &fakeObservabilityReadStore{},
		}, SubscriptionRuntimeOptions{PollInterval: time.Hour, QueueSize: 4}),
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	for _, field := range []string{"limit", "cursor", "order", "since", "until"} {
		t.Run(field, func(t *testing.T) {
			conn := dialTestWS(t, server.URL)
			defer conn.Close()
			writeWSRequest(t, conn, map[string]any{
				"jsonrpc": "2.0",
				"id":      "bad-runtime-logs",
				"method":  "runtime.subscribe_logs",
				"params":  map[string]any{field: "bad"},
			})
			resp := readWSResponse(t, conn)
			if resp.Error == nil || resp.Error.Code != codeInvalidParams {
				t.Fatalf("runtime.subscribe_logs %s response = %#v, want invalid params", field, resp)
			}
			if details := asMap(t, asMap(t, resp.Error.Data)["details"]); details["field"] != field {
				t.Fatalf("runtime.subscribe_logs %s details = %#v", field, details)
			}
		})
	}

	t.Run("invalid bundle_hash", func(t *testing.T) {
		conn := dialTestWS(t, server.URL)
		defer conn.Close()
		writeWSRequest(t, conn, map[string]any{
			"jsonrpc": "2.0",
			"id":      "bad-runtime-logs-bundle",
			"method":  "runtime.subscribe_logs",
			"params":  map[string]any{"bundle_hash": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
		})
		resp := readWSResponse(t, conn)
		if resp.Error == nil || resp.Error.Code != codeInvalidParams {
			t.Fatalf("runtime.subscribe_logs invalid bundle_hash response = %#v, want invalid params", resp)
		}
		if details := asMap(t, asMap(t, resp.Error.Data)["details"]); details["field"] != "bundle_hash" {
			t.Fatalf("runtime.subscribe_logs invalid bundle_hash details = %#v", details)
		}
	})
}

func TestWebSocketSessionBackpressureAndUnsubscribeCancelState(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	session := &webSocketSession{
		ctx:    ctx,
		cancel: cancel,
		out:    make(chan outboundMessage, 1),
		subs:   map[string]context.CancelFunc{},
	}
	if !session.enqueue(rpcResponse{JSONRPC: jsonRPCVersion, ID: "first", Result: map[string]any{"ok": true}}) {
		t.Fatal("first enqueue unexpectedly failed")
	}
	if session.enqueue(rpcResponse{JSONRPC: jsonRPCVersion, ID: "second", Result: map[string]any{"ok": true}}) {
		t.Fatal("second enqueue succeeded, want bounded backpressure cancellation")
	}
	requireContextCanceled(t, ctx, "session context was not canceled on backpressure")

	subCtx, subCancel := context.WithCancel(context.Background())
	session.registerSubscription("sub-1", subCancel)
	session.cancelSubscription("sub-1")
	requireContextCanceled(t, subCtx, "subscription context was not canceled by unsubscribe")
}

func outboundNotificationResultString(t *testing.T, message outboundMessage, field string) string {
	t.Helper()
	params, ok := message.value.Lookup("params")
	if !ok {
		t.Fatalf("outbound notification has no params: %#v", message.value.Interface())
	}
	result, ok := params.Lookup("result")
	if !ok {
		t.Fatalf("outbound notification has no result: %#v", message.value.Interface())
	}
	value, ok := result.Lookup(field)
	if !ok {
		t.Fatalf("outbound notification result has no %s: %#v", field, result.Interface())
	}
	text, ok := value.String()
	if !ok {
		t.Fatalf("outbound notification result %s is not a string: %#v", field, value.Interface())
	}
	return text
}

func dialTestWS(t *testing.T, serverURL string) *websocket.Conn {
	t.Helper()
	wsURL := "ws" + strings.TrimPrefix(serverURL, "http") + "/v1/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, http.Header{"Authorization": []string{"Bearer " + testToken}})
	if err != nil {
		t.Fatalf("websocket dial: %v", err)
	}
	return conn
}

func writeWSRequest(t *testing.T, conn *websocket.Conn, request map[string]any) {
	t.Helper()
	if err := conn.WriteJSON(request); err != nil {
		t.Fatalf("write websocket request: %v", err)
	}
}

const (
	apiv1WebSocketReadTimeout      = 2 * time.Second
	apiv1WebSocketNoMessageTimeout = 100 * time.Millisecond
)

func readWSResponse(t *testing.T, conn *websocket.Conn) rpcResponse {
	t.Helper()
	raw := requireWSMessage(t, conn, "websocket response")
	var envelope map[string]any
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatalf("decode websocket response envelope: %v raw=%s", err, raw)
	}
	if envelope["method"] == "rpc.subscription" {
		t.Fatalf("got notification before response: %s", raw)
	}
	var resp rpcResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatalf("decode websocket response: %v raw=%s", err, raw)
	}
	return resp
}

func readWSNotification(t *testing.T, conn *websocket.Conn) rpcSubscriptionNotification {
	t.Helper()
	raw := requireWSMessage(t, conn, "websocket notification")
	var notification rpcSubscriptionNotification
	if err := json.Unmarshal(raw, &notification); err != nil {
		t.Fatalf("decode websocket notification: %v raw=%s", err, raw)
	}
	if notification.Method != "rpc.subscription" {
		t.Fatalf("websocket notification = %s, want rpc.subscription", raw)
	}
	return notification
}

func requireWSMessage(t *testing.T, conn *websocket.Conn, description string) []byte {
	t.Helper()
	raw, err := readWSMessageWithTimeout(t, conn, apiv1WebSocketReadTimeout)
	if err != nil {
		t.Fatalf("read %s: %v", description, err)
	}
	return raw
}

func requireNoWSMessage(t *testing.T, conn *websocket.Conn, timeout time.Duration, description string) {
	t.Helper()
	raw, err := readWSMessageWithTimeout(t, conn, timeout)
	if err == nil {
		t.Fatalf("unexpected %s: %s", description, raw)
	}
}

func readWSMessageWithTimeout(t *testing.T, conn *websocket.Conn, timeout time.Duration) ([]byte, error) {
	t.Helper()
	clearDeadline := setWSReadDeadline(t, conn, timeout)
	defer clearDeadline()
	_, raw, err := conn.ReadMessage()
	return raw, err
}

func setWSReadDeadline(t *testing.T, conn *websocket.Conn, timeout time.Duration) func() {
	t.Helper()
	deadline := time.Now().Add(timeout)
	if err := conn.SetReadDeadline(deadline); err != nil {
		t.Fatalf("set websocket read deadline: %v", err)
	}
	return func() {
		if err := conn.SetReadDeadline(time.Time{}); err != nil {
			t.Fatalf("clear websocket read deadline: %v", err)
		}
	}
}

func requireContextCanceled(t *testing.T, ctx context.Context, description string) {
	t.Helper()
	select {
	case <-ctx.Done():
	default:
		t.Fatal(description)
	}
}

func sameJSON(a, b any) bool {
	rawA, errA := json.Marshal(a)
	rawB, errB := json.Marshal(b)
	return errA == nil && errB == nil && string(rawA) == string(rawB)
}
