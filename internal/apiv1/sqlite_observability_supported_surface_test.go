package apiv1

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	storepkg "github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestSQLiteObservabilityOwnerBacksSupportedAPISurfaces(t *testing.T) {
	ctx := context.Background()
	fixture := newSQLiteObservabilitySurfaceFixture(t, ctx)
	readOpts := OperatorReadOptions{
		Now:           func() time.Time { return fixture.now.Add(time.Minute) },
		Observability: fixture.store,
	}
	handler := testHandler(t, Options{
		AuthTokens:    []string{testToken},
		Handlers:      OperatorReadHandlers(readOpts),
		Subscriptions: OperatorSubscriptions(readOpts, SubscriptionRuntimeOptions{PollInterval: time.Hour, QueueSize: 4}),
	})

	trace := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"trace","method":"run.trace","params":{"run_id":%q,"filter":{"event_name":["trace.visible"]},"limit":10}}`, fixture.runID))
	if trace.Error != nil {
		t.Fatalf("run.trace error = %#v", trace.Error)
	}
	if rows, _ := asMap(t, trace.Result)["trace"].([]any); len(rows) != 1 || asMap(t, rows[0])["event_id"] != fixture.eventID {
		t.Fatalf("run.trace rows = %#v, want event %s", asMap(t, trace.Result)["trace"], fixture.eventID)
	}

	logs := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"logs","method":"runtime.logs","params":{"run_id":%q,"component":"scheduler","level":"warn","limit":10}}`, fixture.runID))
	if logs.Error != nil {
		t.Fatalf("runtime.logs error = %#v", logs.Error)
	}
	if rows, _ := asMap(t, logs.Result)["logs"].([]any); len(rows) != 1 || asMap(t, rows[0])["log_id"] != fixture.logID {
		t.Fatalf("runtime.logs rows = %#v, want log %s", asMap(t, logs.Result)["logs"], fixture.logID)
	} else if got := asMap(t, rows[0])["source"]; got != "runtime" {
		t.Fatalf("runtime.logs source = %#v, want runtime", got)
	}
	matchingSourceLogs := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"logs-source","method":"runtime.logs","params":{"run_id":%q,"component":"scheduler","level":"warn","source":"runtime","limit":10}}`, fixture.runID))
	if matchingSourceLogs.Error != nil {
		t.Fatalf("runtime.logs source match error = %#v", matchingSourceLogs.Error)
	}
	if rows, _ := asMap(t, matchingSourceLogs.Result)["logs"].([]any); len(rows) != 1 || asMap(t, rows[0])["log_id"] != fixture.logID {
		t.Fatalf("runtime.logs source match rows = %#v, want log %s", asMap(t, matchingSourceLogs.Result)["logs"], fixture.logID)
	}
	nonmatchingSourceLogs := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"logs-source-miss","method":"runtime.logs","params":{"run_id":%q,"component":"scheduler","level":"warn","source":"agent-1","limit":10}}`, fixture.runID))
	if nonmatchingSourceLogs.Error != nil {
		t.Fatalf("runtime.logs source miss error = %#v", nonmatchingSourceLogs.Error)
	}
	if rows, _ := asMap(t, nonmatchingSourceLogs.Result)["logs"].([]any); len(rows) != 0 {
		t.Fatalf("runtime.logs source miss rows = %#v, want none", rows)
	}

	server := httptest.NewServer(handler)
	defer server.Close()
	assertSQLiteSubscriptionNotification(t, server.URL, "event.subscribe", map[string]any{
		"filter": map[string]any{
			"run_id":     fixture.runID,
			"event_name": "trace.visible",
		},
		"replay_since": fixture.now.Add(-time.Minute).Format(time.RFC3339Nano),
	}, "event_id", fixture.eventID)
	assertSQLiteSubscriptionNotification(t, server.URL, "run.subscribe_trace", map[string]any{
		"run_id":       fixture.runID,
		"filter":       map[string]any{"event_name": []any{"trace.visible"}},
		"replay_since": fixture.now.Add(-time.Minute).Format(time.RFC3339Nano),
	}, "event_id", fixture.eventID)
	logNotification := assertSQLiteSubscriptionNotification(t, server.URL, "runtime.subscribe_logs", map[string]any{
		"run_id":       fixture.runID,
		"component":    "scheduler",
		"level":        "warn",
		"source":       "runtime",
		"replay_since": fixture.now.Add(-time.Minute).Format(time.RFC3339Nano),
	}, "log_id", fixture.logID)
	if got := logNotification["source"]; got != "runtime" {
		t.Fatalf("runtime.subscribe_logs source = %#v, want runtime", got)
	}
	assertSQLiteSubscriptionNoNotification(t, server.URL, "runtime.subscribe_logs", map[string]any{
		"run_id":       fixture.runID,
		"component":    "scheduler",
		"level":        "warn",
		"source":       "agent-1",
		"replay_since": fixture.now.Add(-time.Minute).Format(time.RFC3339Nano),
	})
}

type sqliteObservabilitySurfaceFixture struct {
	store   *storepkg.SQLiteRuntimeStore
	runID   string
	eventID string
	logID   string
	now     time.Time
}

func newSQLiteObservabilitySurfaceFixture(t *testing.T, ctx context.Context) sqliteObservabilitySurfaceFixture {
	t.Helper()
	sqliteStore := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)

	now := time.Unix(1700002000, 0).UTC()
	runID := uuid.NewString()
	eventID := uuid.NewString()
	if err := sqliteStore.PersistEventWithDeliveries(ctx, events.Event{
		ID:          eventID,
		RunID:       runID,
		Type:        events.EventType("trace.visible"),
		SourceAgent: "agent-1",
		Payload:     json.RawMessage(`{"trace":true}`),
		CreatedAt:   now,
	}, []string{"agent-1"}); err != nil {
		t.Fatalf("PersistEventWithDeliveries: %v", err)
	}
	if err := sqliteStore.MarkEventDeliveryInProgress(ctx, eventID, "agent-1", "session-1"); err != nil {
		t.Fatalf("MarkEventDeliveryInProgress: %v", err)
	}

	logger := runtimepkg.NewRuntimeLogger(sqliteStore)
	logCtx := runtimecorrelation.WithRunID(ctx, runID)
	if err := logger.Log(logCtx, runtimepkg.RuntimeLogEntry{
		Level:     "warn",
		Message:   "runtime warning",
		Component: "scheduler",
		Action:    "supported_surface",
		SessionID: "session-1",
	}); err != nil {
		t.Fatalf("RuntimeLogger.Log sqlite runtime log: %v", err)
	}
	logs, err := sqliteStore.ListOperatorRuntimeLogs(ctx, storepkg.OperatorRuntimeLogListOptions{
		RunID:     runID,
		Component: "scheduler",
		Level:     "warn",
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("ListOperatorRuntimeLogs after logger write: %v", err)
	}
	if len(logs.Logs) != 1 {
		t.Fatalf("logger-written runtime logs = %#v, want one", logs.Logs)
	}

	return sqliteObservabilitySurfaceFixture{
		store:   sqliteStore,
		runID:   runID,
		eventID: eventID,
		logID:   logs.Logs[0].LogID,
		now:     now,
	}
}

func assertSQLiteSubscriptionNotification(t *testing.T, serverURL, method string, params map[string]any, key, want string) map[string]any {
	t.Helper()
	conn := dialTestWS(t, serverURL)
	defer conn.Close()
	writeWSRequest(t, conn, map[string]any{
		"jsonrpc": "2.0",
		"id":      method + "-sqlite",
		"method":  method,
		"params":  params,
	})
	subscribe := readWSResponse(t, conn)
	if subscribe.Error != nil {
		t.Fatalf("%s error = %#v", method, subscribe.Error)
	}
	notification := readWSNotification(t, conn)
	result := asMap(t, notification.Params.Result)
	if got := result[key]; got != want {
		t.Fatalf("%s notification %s = %#v, want %s", method, key, got, want)
	}
	return result
}

func assertSQLiteSubscriptionNoNotification(t *testing.T, serverURL, method string, params map[string]any) {
	t.Helper()
	conn := dialTestWS(t, serverURL)
	defer conn.Close()
	writeWSRequest(t, conn, map[string]any{
		"jsonrpc": "2.0",
		"id":      method + "-sqlite-miss",
		"method":  method,
		"params":  params,
	})
	subscribe := readWSResponse(t, conn)
	if subscribe.Error != nil {
		t.Fatalf("%s error = %#v", method, subscribe.Error)
	}
	requireNoWSMessage(t, conn, apiv1WebSocketNoMessageTimeout, method+" nonmatching source notification")
}
