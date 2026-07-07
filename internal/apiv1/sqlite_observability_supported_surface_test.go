package apiv1

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	storepkg "github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestSQLiteObservabilityOwnerBacksSupportedAPISurfaces(t *testing.T) {
	ctx := context.Background()
	fixture := newSQLiteObservabilitySurfaceFixture(t, ctx)
	assertObservabilityOwnerBacksSupportedAPISurfaces(t, fixture)
}

func TestPostgresObservabilityOwnerBacksSupportedAPISurfaces(t *testing.T) {
	ctx := context.Background()
	fixture := newPostgresObservabilitySurfaceFixture(t, ctx)
	assertObservabilityOwnerBacksSupportedAPISurfaces(t, fixture)
}

func assertObservabilityOwnerBacksSupportedAPISurfaces(t *testing.T, fixture observabilitySurfaceFixture) {
	t.Helper()
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

	incidents := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"incidents","method":"runtime.incidents","params":{"component":"scheduler","level":"error","limit":10}}`)
	if incidents.Error != nil {
		t.Fatalf("runtime.incidents error = %#v", incidents.Error)
	}
	if rows, _ := asMap(t, incidents.Result)["incidents"].([]any); len(rows) != 1 || asMap(t, rows[0])["error_code"] != fixture.incidentCode {
		t.Fatalf("runtime.incidents rows = %#v, want incident %s", asMap(t, incidents.Result)["incidents"], fixture.incidentCode)
	}

	server := httptest.NewServer(handler)
	defer server.Close()
	assertObservabilitySubscriptionNotification(t, server.URL, "event.subscribe", map[string]any{
		"filter": map[string]any{
			"run_id":     fixture.runID,
			"event_name": "trace.visible",
		},
		"replay_since": fixture.now.Add(-time.Minute).Format(time.RFC3339Nano),
	}, "event_id", fixture.eventID)
	assertObservabilitySubscriptionNotification(t, server.URL, "run.subscribe_trace", map[string]any{
		"run_id":       fixture.runID,
		"filter":       map[string]any{"event_name": []any{"trace.visible"}},
		"replay_since": fixture.now.Add(-time.Minute).Format(time.RFC3339Nano),
	}, "event_id", fixture.eventID)
	logNotification := assertObservabilitySubscriptionNotification(t, server.URL, "runtime.subscribe_logs", map[string]any{
		"run_id":       fixture.runID,
		"component":    "scheduler",
		"level":        "warn",
		"source":       "runtime",
		"replay_since": fixture.now.Add(-time.Minute).Format(time.RFC3339Nano),
	}, "log_id", fixture.logID)
	if got := logNotification["source"]; got != "runtime" {
		t.Fatalf("runtime.subscribe_logs source = %#v, want runtime", got)
	}
	assertObservabilitySubscriptionNoNotification(t, server.URL, "runtime.subscribe_logs", map[string]any{
		"run_id":       fixture.runID,
		"component":    "scheduler",
		"level":        "warn",
		"source":       "agent-1",
		"replay_since": fixture.now.Add(-time.Minute).Format(time.RFC3339Nano),
	})
}

func TestSQLiteRunTraceAPISurfacePaginatesAndUsesMaterializationWindow(t *testing.T) {
	ctx := context.Background()
	sqliteStore := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
	base := time.Unix(1700003100, 0).UTC()
	runID := "00000000-0000-0000-0000-000000001429"
	eventOnlyID := "00000000-0000-0000-0000-000000001401"
	lateDeliveryID := "00000000-0000-0000-0000-000000001402"
	secondDeliveryID := "00000000-0000-0000-0000-000000001403"
	if _, err := sqliteStore.DB.ExecContext(ctx, `INSERT INTO runs (run_id, status, started_at) VALUES (?, 'running', ?)`, runID, base.Add(-time.Minute)); err != nil {
		t.Fatalf("seed sqlite run: %v", err)
	}
	if _, err := sqliteStore.DB.ExecContext(ctx, `
		INSERT INTO events (run_id, event_id, event_name, entity_id, scope, payload, produced_by, produced_by_type, created_at)
		VALUES
			(?, ?, 'trace.event_only', NULL, 'global', '{}', 'runtime', 'platform', ?),
			(?, ?, 'trace.late_delivery', NULL, 'global', '{}', 'runtime', 'platform', ?),
			(?, ?, 'trace.second_delivery', NULL, 'global', '{}', 'runtime', 'platform', ?)
	`, runID, eventOnlyID, base, runID, lateDeliveryID, base, runID, secondDeliveryID, base.Add(time.Second)); err != nil {
		t.Fatalf("seed sqlite events: %v", err)
	}
	if _, err := sqliteStore.DB.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			delivery_id, run_id, event_id, subscriber_type, subscriber_id, status, reason_code, created_at, started_at, delivered_at
		) VALUES
			('00000000-0000-0000-0000-000000001411', ?, ?, 'agent', 'agent-late', 'delivered', 'ok', ?, ?, ?),
			('00000000-0000-0000-0000-000000001412', ?, ?, 'agent', 'agent-second', 'delivered', 'ok', ?, ?, ?)
	`, runID, lateDeliveryID, base.Add(time.Second), base.Add(2*time.Second), base.Add(3*time.Second),
		runID, secondDeliveryID, base.Add(2*time.Second), base.Add(3*time.Second), base.Add(4*time.Second)); err != nil {
		t.Fatalf("seed sqlite deliveries: %v", err)
	}

	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Now:           func() time.Time { return base.Add(time.Minute) },
			Observability: sqliteStore,
		}),
	})
	page1 := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"page1","method":"run.trace","params":{"run_id":%q,"limit":1,"filter":{"delivery_status":["delivered"]}}}`, runID))
	if page1.Error != nil {
		t.Fatalf("run.trace page1 error = %#v", page1.Error)
	}
	page1Result := asMap(t, page1.Result)
	page1Rows, _ := page1Result["trace"].([]any)
	nextCursor, _ := page1Result["next_cursor"].(string)
	if len(page1Rows) != 1 || asMap(t, page1Rows[0])["event_id"] != lateDeliveryID || nextCursor == "" {
		t.Fatalf("run.trace page1 result = %#v, want late delivery plus next_cursor", page1Result)
	}
	page2 := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"page2","method":"run.trace","params":{"run_id":%q,"limit":1,"cursor":%q,"filter":{"delivery_status":["delivered"]}}}`, runID, nextCursor))
	if page2.Error != nil {
		t.Fatalf("run.trace page2 error = %#v", page2.Error)
	}
	page2Result := asMap(t, page2.Result)
	page2Rows, _ := page2Result["trace"].([]any)
	if len(page2Rows) != 1 || asMap(t, page2Rows[0])["event_id"] != secondDeliveryID || page2Result["next_cursor"] != nil {
		t.Fatalf("run.trace page2 result = %#v, want second delivery and no next_cursor", page2Result)
	}

	since := base.Format(time.RFC3339Nano)
	sinceResp := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"since","method":"run.trace","params":{"run_id":%q,"limit":10,"since":%q}}`, runID, since))
	if sinceResp.Error != nil {
		t.Fatalf("run.trace since error = %#v", sinceResp.Error)
	}
	sinceRows, _ := asMap(t, sinceResp.Result)["trace"].([]any)
	if len(sinceRows) != 2 || asMap(t, sinceRows[0])["event_id"] != lateDeliveryID || asMap(t, sinceRows[1])["event_id"] != secondDeliveryID {
		t.Fatalf("run.trace since rows = %#v, want delivery-materialized rows only", sinceRows)
	}
	until := base.Add(2500 * time.Millisecond).Format(time.RFC3339Nano)
	untilResp := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"until","method":"run.trace","params":{"run_id":%q,"limit":10,"until":%q}}`, runID, until))
	if untilResp.Error != nil {
		t.Fatalf("run.trace until error = %#v", untilResp.Error)
	}
	untilRows, _ := asMap(t, untilResp.Result)["trace"].([]any)
	if len(untilRows) != 1 || asMap(t, untilRows[0])["event_id"] != eventOnlyID {
		t.Fatalf("run.trace until rows = %#v, want only event-only row before delivery materialization", untilRows)
	}

	invalid := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"bad-cursor","method":"run.trace","params":{"run_id":%q,"cursor":"not-a-cursor"}}`, runID))
	if invalid.Error == nil {
		t.Fatal("run.trace invalid cursor error = nil")
	}
	details := asMap(t, invalid.Error.Data)
	if nested := asMap(t, details["details"]); nested["field"] != "cursor" {
		t.Fatalf("run.trace invalid cursor details = %#v, want cursor field", details)
	}
}

type observabilitySurfaceFixture struct {
	store        ObservabilityReadStore
	runID        string
	eventID      string
	logID        string
	incidentCode string
	now          time.Time
}

func newSQLiteObservabilitySurfaceFixture(t *testing.T, ctx context.Context) observabilitySurfaceFixture {
	t.Helper()
	sqliteStore := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
	return newObservabilitySurfaceFixture(t, ctx, sqliteStore)
}

func newPostgresObservabilitySurfaceFixture(t *testing.T, ctx context.Context) observabilitySurfaceFixture {
	t.Helper()
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg := &storepkg.PostgresStore{DB: db}
	if _, err := pg.BindSchemaCapabilities(ctx); err != nil {
		t.Fatalf("BindSchemaCapabilities postgres: %v", err)
	}
	return newObservabilitySurfaceFixture(t, ctx, pg)
}

type observabilityFixtureStore interface {
	ObservabilityReadStore
	PersistEventWithDeliveries(context.Context, events.Event, []string) error
	MarkEventDeliveryInProgress(context.Context, string, string, string) error
	runtimepkg.RuntimeLogPersistence
}

func newObservabilitySurfaceFixture(t *testing.T, ctx context.Context, store observabilityFixtureStore) observabilitySurfaceFixture {
	t.Helper()

	now := time.Now().UTC().Add(-2 * time.Minute)
	runID := uuid.NewString()
	eventID := uuid.NewString()
	if err := store.PersistEventWithDeliveries(ctx, eventtest.PersistedProjection(eventID,

		events.EventType("trace.visible"),
		"agent-1", "", json.RawMessage(`{"trace":true}`), 0, runID, "", events.EventEnvelope{}, now),

		[]string{"agent-1"}); err != nil {
		t.Fatalf("PersistEventWithDeliveries: %v", err)
	}
	if err := store.MarkEventDeliveryInProgress(ctx, eventID, "agent-1", "session-1"); err != nil {
		t.Fatalf("MarkEventDeliveryInProgress: %v", err)
	}

	logger := runtimepkg.NewRuntimeLogger(store)
	logCtx := runtimecorrelation.WithRunID(ctx, runID)
	if err := logger.Log(logCtx, runtimepkg.RuntimeLogEntry{
		Level:     "warn",
		Message:   "runtime warning",
		Component: "scheduler",
		Action:    "supported_surface",
		SessionID: "session-1",
	}); err != nil {
		t.Fatalf("RuntimeLogger.Log warning runtime log: %v", err)
	}
	incidentCode := "supported_surface_failed"
	if err := logger.Log(logCtx, runtimepkg.RuntimeLogEntry{
		Level:     "error",
		Message:   "runtime failed",
		Component: "scheduler",
		Action:    "supported_surface_failed",
		SessionID: "session-1",
		Error:     "boom",
		Detail: map[string]any{
			"error":      "boom",
			"error_code": incidentCode,
		},
	}); err != nil {
		t.Fatalf("RuntimeLogger.Log error runtime log: %v", err)
	}
	logs, err := store.ListOperatorRuntimeLogs(ctx, storepkg.OperatorRuntimeLogListOptions{
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
	incidents, err := store.ListOperatorRuntimeIncidents(ctx, storepkg.OperatorRuntimeIncidentListOptions{
		Component: "scheduler",
		Level:     "error",
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("ListOperatorRuntimeIncidents after logger write: %v", err)
	}
	if len(incidents.Incidents) != 1 || incidents.Incidents[0].ErrorCode != incidentCode {
		t.Fatalf("logger-written runtime incidents = %#v, want %s", incidents.Incidents, incidentCode)
	}

	return observabilitySurfaceFixture{
		store:        store,
		runID:        runID,
		eventID:      eventID,
		logID:        logs.Logs[0].LogID,
		incidentCode: incidentCode,
		now:          now,
	}
}

func assertObservabilitySubscriptionNotification(t *testing.T, serverURL, method string, params map[string]any, key, want string) map[string]any {
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

func assertObservabilitySubscriptionNoNotification(t *testing.T, serverURL, method string, params map[string]any) {
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
