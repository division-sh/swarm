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

	"github.com/gorilla/websocket"
	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/store"
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
				EventID:     "evt-1",
				EventName:   "scan.requested",
				RunID:       "run-1",
				EntityID:    "entity-1",
				CreatedAt:   base.Add(time.Second),
				Source:      "runtime",
				Payload:     map[string]any{"ok": true},
				Deliveries:  []store.OperatorEventDelivery{},
				DeadLetters: []store.OperatorDeadLetterRecord{},
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
		"params":  map[string]any{},
	})
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	defer conn.SetReadDeadline(time.Time{})
	_, raw, err := conn.ReadMessage()
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
	if _, _, err := conn.ReadMessage(); err == nil {
		t.Fatal("websocket stayed open after owner read error, want fail-closed disconnect")
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
}

func TestWebSocketSessionBackpressureAndUnsubscribeCancelState(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	session := &webSocketSession{
		ctx:    ctx,
		cancel: cancel,
		out:    make(chan any, 1),
		subs:   map[string]context.CancelFunc{},
	}
	if !session.enqueue(rpcResponse{JSONRPC: jsonRPCVersion, ID: "first", Result: map[string]any{"ok": true}}) {
		t.Fatal("first enqueue unexpectedly failed")
	}
	if session.enqueue(rpcResponse{JSONRPC: jsonRPCVersion, ID: "second", Result: map[string]any{"ok": true}}) {
		t.Fatal("second enqueue succeeded, want bounded backpressure cancellation")
	}
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("session context was not canceled on backpressure")
	}

	subCtx, subCancel := context.WithCancel(context.Background())
	session.registerSubscription("sub-1", subCancel)
	session.cancelSubscription("sub-1")
	select {
	case <-subCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("subscription context was not canceled by unsubscribe")
	}
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

func readWSResponse(t *testing.T, conn *websocket.Conn) rpcResponse {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	defer conn.SetReadDeadline(time.Time{})
	_, raw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read websocket response: %v", err)
	}
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
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	defer conn.SetReadDeadline(time.Time{})
	_, raw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read websocket notification: %v", err)
	}
	var notification rpcSubscriptionNotification
	if err := json.Unmarshal(raw, &notification); err != nil {
		t.Fatalf("decode websocket notification: %v raw=%s", err, raw)
	}
	if notification.Method != "rpc.subscription" {
		t.Fatalf("websocket notification = %s, want rpc.subscription", raw)
	}
	return notification
}

func sameJSON(a, b any) bool {
	rawA, errA := json.Marshal(a)
	rawB, errB := json.Marshal(b)
	return errA == nil && errB == nil && string(rawA) == string(rawB)
}
