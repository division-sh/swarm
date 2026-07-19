package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func canonicalRuntimeLogTestPayload(t *testing.T, component, action, code, message, agentID string) string {
	t.Helper()
	class := runtimefailures.ClassInternalFailure
	if code == "retry_exhausted" {
		class = runtimefailures.ClassRetryExhausted
	}
	failure := runtimefailures.Normalize(runtimefailures.New(class, code, "test-runtime", action, nil), "test-runtime", action)
	details := map[string]any{
		"action":  action,
		"failure": failure,
	}
	if component != "" {
		details["component"] = component
	}
	if agentID != "" {
		details["agent_id"] = agentID
	}
	payload, err := json.Marshal(map[string]any{
		"log_level": "error",
		"message":   message,
		"details":   details,
	})
	if err != nil {
		t.Fatalf("marshal canonical runtime log payload: %v", err)
	}
	return string(payload)
}

func TestSQLObservabilityReader_ListEvents_UsesCanonicalDeliveryLifecycle(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	reader := NewSQLObservabilityReader(db, storetest.AdmitPostgresRuntimeStore(t, db))
	ctx := context.Background()

	runID := uuid.NewString()
	eventID := uuid.NewString()
	entityID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	storetest.InsertRootEventRecord(t, ctx, db, runtimeauthoractivity.DialectPostgres,
		eventID, runID, "task.completed", eventtest.Producer(events.EventProducerAgent, "runtime"),
		[]byte(`{"entity_id":"`+entityID+`"}`), events.EventEnvelope{EntityID: entityID, Scope: events.EventScopeEntity}, time.Unix(1700000000, 0).UTC())

	seedDelivery := func(subscriberID, status string, retryCount int, failureCode string, createdAt time.Time) {
		t.Helper()
		failureJSON := ""
		if failureCode != "" {
			failureJSON = mustMarshalFailure(t, testFailure(failureCode))
		}
		if _, err := db.ExecContext(ctx, `
			INSERT INTO event_deliveries (
				run_id, event_id, subscriber_type, subscriber_id, status, retry_count, failure, created_at
			) VALUES (
				$1::uuid, $2::uuid, 'agent', $3, $4, $5, NULLIF($6, '')::jsonb, $7
			)
		`, runID, eventID, subscriberID, status, retryCount, failureJSON, createdAt); err != nil {
			t.Fatalf("seed delivery %s: %v", subscriberID, err)
		}
	}

	now := time.Unix(1700000000, 0).UTC()
	seedDelivery("agent-pending", "pending", 0, "", now)
	seedDelivery("agent-progress", "in_progress", 0, "", now.Add(time.Second))
	seedDelivery("agent-delivered", "delivered", 0, "", now.Add(2*time.Second))
	seedDelivery("agent-failed", "failed", 1, "delivery-failed", now.Add(3*time.Second))
	seedDelivery("agent-dead", "dead_letter", 2, "delivery-dead", now.Add(4*time.Second))

	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_receipts (
			event_id, subscriber_type, subscriber_id, outcome, side_effects, failure, processed_at
		) VALUES
			($1::uuid, 'agent', 'agent-pending', 'dead_letter', '{"retry_count":9}'::jsonb, $2::jsonb, now()),
			($1::uuid, 'agent', 'agent-failed', 'success', '{"retry_count":0}'::jsonb, NULL, now())
	`, eventID, mustMarshalFailure(t, testFailure("receipt_should_not_win"))); err != nil {
		t.Fatalf("seed conflicting receipts: %v", err)
	}

	rows, err := reader.ListEvents(ctx, EventFilter{Type: "task.completed"}, 10)
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows len = %d, want 1", len(rows))
	}
	got := rows[0]
	if got.DeliveryLifecycle.Pending != 1 || got.DeliveryLifecycle.InProgress != 1 || got.DeliveryLifecycle.Delivered != 1 || got.DeliveryLifecycle.Failed != 1 || got.DeliveryLifecycle.DeadLetter != 1 {
		t.Fatalf("delivery lifecycle = %#v", got.DeliveryLifecycle)
	}
	if got.PendingCount != 1 || got.ErrorCount != 1 || got.DeadCount != 1 {
		t.Fatalf("legacy counts = pending=%d error=%d dead=%d", got.PendingCount, got.ErrorCount, got.DeadCount)
	}
	if len(got.Deliveries) != 5 {
		t.Fatalf("deliveries len = %d, want 5", len(got.Deliveries))
	}
	for _, item := range got.Deliveries {
		if strings.TrimSpace(item.DeliveryID) == "" || item.SubscriberType != "agent" || strings.TrimSpace(item.SubscriberID) == "" {
			t.Fatalf("delivery identity = %#v, want delivery_id and agent subscriber identity", item)
		}
	}
}

func TestSQLObservabilityReader_ListEvents_FiltersTypedSubscriberIdentity(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	reader := NewSQLObservabilityReader(db, storetest.AdmitPostgresRuntimeStore(t, db))
	ctx := context.Background()

	runID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}

	type deliverySeed struct {
		subscriberType string
		subscriberID   string
	}
	seedEvent := func(eventName string, at time.Time, deliveries ...deliverySeed) string {
		t.Helper()
		eventID := uuid.NewString()
		storetest.InsertRootEventRecord(t, ctx, db, runtimeauthoractivity.DialectPostgres,
			eventID, runID, events.EventType(eventName), eventtest.Producer(events.EventProducerAgent, "runtime"),
			[]byte(`{}`), events.EventEnvelope{Scope: events.EventScopeGlobal}, at.UTC())
		for _, delivery := range deliveries {
			if _, err := db.ExecContext(ctx, `
				INSERT INTO event_deliveries (
					run_id, event_id, subscriber_type, subscriber_id, status, created_at
				) VALUES (
					$1::uuid, $2::uuid, $3, $4, 'pending', $5
				)
			`, runID, eventID, delivery.subscriberType, delivery.subscriberID, at.UTC()); err != nil {
				t.Fatalf("seed delivery %s/%s: %v", delivery.subscriberType, delivery.subscriberID, err)
			}
		}
		return eventID
	}

	base := time.Unix(1700000600, 0).UTC()
	wantAgentEvent := seedEvent("typed.agent", base,
		deliverySeed{subscriberType: "agent", subscriberID: "colliding-subscriber"},
	)
	seedEvent("typed.node", base.Add(time.Second),
		deliverySeed{subscriberType: "node", subscriberID: "colliding-subscriber"},
	)
	seedEvent("typed.cross-product", base.Add(2*time.Second),
		deliverySeed{subscriberType: "node", subscriberID: "colliding-subscriber"},
		deliverySeed{subscriberType: "agent", subscriberID: "different-subscriber"},
	)

	rows, err := reader.ListEvents(ctx, EventFilter{
		SubscriberID:   "colliding-subscriber",
		SubscriberType: "agent",
	}, 10)
	if err != nil {
		t.Fatalf("ListEvents typed subscriber filter: %v", err)
	}
	if len(rows) != 1 || rows[0].EventID != wantAgentEvent {
		t.Fatalf("typed subscriber filtered rows = %#v, want only %s", rows, wantAgentEvent)
	}
}

func TestSQLObservabilityReader_GetEvent_UsesCanonicalDeliveryRows(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	reader := NewSQLObservabilityReader(db, storetest.AdmitPostgresRuntimeStore(t, db))
	ctx := context.Background()

	runID := uuid.NewString()
	eventID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	storetest.InsertRootEventRecord(t, ctx, db, runtimeauthoractivity.DialectPostgres,
		eventID, runID, "task.completed", eventtest.Producer(events.EventProducerAgent, "runtime"),
		[]byte(`{}`), events.EventEnvelope{Scope: events.EventScopeGlobal}, time.Now().UTC())
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, retry_count, failure, created_at
		) VALUES (
			$1::uuid, $2::uuid, 'agent', 'agent-a', 'pending', 1, $3::jsonb, now()
		)
	`, runID, eventID, mustMarshalFailure(t, testFailure("delivery_wins"))); err != nil {
		t.Fatalf("seed delivery: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_receipts (
			event_id, subscriber_type, subscriber_id, outcome, side_effects, processed_at
		) VALUES (
			$1::uuid, 'agent', 'agent-a', 'dead_letter', '{"retry_count":7,"error":"receipt-loses"}'::jsonb, now()
		)
	`, eventID); err != nil {
		t.Fatalf("seed conflicting receipt: %v", err)
	}

	got, ok, err := reader.GetEvent(ctx, eventID)
	if err != nil {
		t.Fatalf("GetEvent: %v", err)
	}
	if !ok {
		t.Fatal("expected event to exist")
	}
	if got.DeliveryLifecycle.Pending != 1 || got.DeliveryLifecycle.DeadLetter != 0 {
		t.Fatalf("delivery lifecycle = %#v", got.DeliveryLifecycle)
	}
	if len(got.Deliveries) != 1 {
		t.Fatalf("deliveries len = %d, want 1", len(got.Deliveries))
	}
	if item := got.Deliveries[0]; strings.TrimSpace(item.DeliveryID) == "" || item.SubscriberType != "agent" || item.SubscriberID != "agent-a" || item.Status != "pending" || item.RetryCount != 1 || item.Failure == nil || item.Failure.Detail.Code != "delivery_wins" {
		t.Fatalf("delivery = %#v", item)
	}
}

func TestSQLObservabilityReader_EventIdentityDoesNotPromotePayloadEntity(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	reader := NewSQLObservabilityReader(db, storetest.AdmitPostgresRuntimeStore(t, db))
	ctx := context.Background()

	runID := uuid.NewString()
	targetEntityID := uuid.NewString()
	payloadOnlyEventID := uuid.NewString()
	canonicalEventID := uuid.NewString()
	base := time.Unix(1700001200, 0).UTC()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status, started_at) VALUES ($1::uuid, 'running', $2)`, runID, base); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	storetest.InsertRootEventRecord(t, ctx, db, runtimeauthoractivity.DialectPostgres,
		payloadOnlyEventID, runID, "task.payload_only", eventtest.Producer(events.EventProducerAgent, "agent-a"),
		[]byte(`{"entity_id":"`+targetEntityID+`","marker":"payload-only"}`), events.EventEnvelope{Scope: events.EventScopeGlobal}, base)
	storetest.InsertRootEventRecord(t, ctx, db, runtimeauthoractivity.DialectPostgres,
		canonicalEventID, runID, "task.canonical_entity", eventtest.Producer(events.EventProducerAgent, "agent-b"),
		[]byte(`{"entity_id":"payload-business-value","marker":"canonical"}`),
		events.EventEnvelope{EntityID: targetEntityID, Scope: events.EventScopeEntity}, base.Add(time.Second))

	filtered, err := reader.ListEvents(ctx, EventFilter{EntityID: targetEntityID}, 10)
	if err != nil {
		t.Fatalf("ListEvents entity filter: %v", err)
	}
	if len(filtered) != 1 || filtered[0].EventID != canonicalEventID {
		t.Fatalf("filtered events = %#v, want only canonical event %s", filtered, canonicalEventID)
	}
	if filtered[0].EntityID != targetEntityID {
		t.Fatalf("canonical dashboard entity_id = %q, want %s", filtered[0].EntityID, targetEntityID)
	}

	payloadOnly, ok, err := reader.GetEvent(ctx, payloadOnlyEventID)
	if err != nil {
		t.Fatalf("GetEvent payload-only: %v", err)
	}
	if !ok {
		t.Fatal("expected payload-only event to exist")
	}
	if payloadOnly.EntityID != "" {
		t.Fatalf("payload-only dashboard entity_id = %q, want empty", payloadOnly.EntityID)
	}
	payload, _ := payloadOnly.Payload.(map[string]any)
	if payload["entity_id"] != targetEntityID {
		t.Fatalf("payload-only payload = %#v, want preserved payload entity_id", payload)
	}

	canonical, ok, err := reader.GetEvent(ctx, canonicalEventID)
	if err != nil {
		t.Fatalf("GetEvent canonical: %v", err)
	}
	if !ok {
		t.Fatal("expected canonical event to exist")
	}
	if canonical.EntityID != targetEntityID {
		t.Fatalf("canonical dashboard entity_id = %q, want %s", canonical.EntityID, targetEntityID)
	}
	canonicalPayload, _ := canonical.Payload.(map[string]any)
	if canonicalPayload["entity_id"] != "payload-business-value" {
		t.Fatalf("canonical payload = %#v, want payload business value preserved", canonicalPayload)
	}
}

func TestHandler_EventDetailIncludesDeliveryLifecycle(t *testing.T) {
	t.Skip("legacy dashboard/Builder operator endpoint retired under #731; canonical v1 owner tests cover this behavior")
	handler := NewHandler(Options{
		AuthToken: testOperatorAuthToken,
		Observability: stubObservability{
			eventDetail: map[string]eventRecord{
				"evt-1": {
					ID:      "evt-1",
					EventID: "evt-1",
					Type:    "task.completed",
					DeliveryLifecycle: deliveryLifecycleSummary{
						Pending:    1,
						InProgress: 2,
						Delivered:  3,
					},
				},
			},
		},
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/events/evt-1", nil)
	setOperatorAuth(req)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("event detail status=%d body=%s", rec.Code, rec.Body.String())
	}

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal event detail: %v", err)
	}
	lifecycle, ok := payload["delivery_lifecycle"].(map[string]any)
	if !ok {
		t.Fatalf("delivery_lifecycle = %#v", payload["delivery_lifecycle"])
	}
	if lifecycle["pending"] != float64(1) || lifecycle["in_progress"] != float64(2) || lifecycle["delivered"] != float64(3) {
		t.Fatalf("delivery_lifecycle = %#v", lifecycle)
	}
}

func TestSQLObservabilityReader_ListRuntimeLogs_ProjectsDeliveryLifecycleFields(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	reader := NewSQLObservabilityReader(db, storetest.AdmitPostgresRuntimeStore(t, db))
	ctx := context.Background()

	insertRuntimeLog := func(eventID, state, prev, reason, terminal string, retryCount int, createdAt time.Time) {
		t.Helper()
		payload := `{
			"log_level":"debug",
			"message":"delivery lifecycle",
			"details":{
				"component":"agent-manager",
				"action":"delivery_lifecycle_transition",
				"event_id":"` + eventID + `",
				"agent_id":"agent-1",
				"delivery_state":"` + state + `",
				"delivery_previous_state":"` + prev + `",
				"delivery_transition":"` + state + `",
				"delivery_reason":"` + reason + `",
				"delivery_terminal_outcome":"` + terminal + `",
				"retry_count":` + fmt.Sprintf("%d", retryCount) + `
			}
		}`
		storetest.InsertDiagnosticDirectEventRecord(t, ctx, db, runtimeauthoractivity.DialectPostgres,
			uuid.NewString(), "runtime", []byte(payload), createdAt)
	}

	insertRuntimeLog("evt-retry", "retrying", "active", "boom", "", 1, time.Unix(1700000100, 0).UTC())
	insertRuntimeLog("evt-dead", "exhausted", "retrying", "boom", "retry_exhausted", 2, time.Unix(1700000200, 0).UTC())
	insertRuntimeLog("evt-cancel", "exhausted", "active", "cancelled_by_kill_previous", "cancelled_by_kill_previous", 0, time.Unix(1700000300, 0).UTC())

	rows, err := reader.ListRuntimeLogs(ctx, RuntimeLogFilter{Component: "agent-manager"}, 10)
	if err != nil {
		t.Fatalf("ListRuntimeLogs: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("rows len = %d, want 3", len(rows))
	}
	if rows[0].DeliveryState != "exhausted" || rows[0].PreviousState != "active" || rows[0].Reason != "cancelled_by_kill_previous" || rows[0].Terminal != "cancelled_by_kill_previous" || rows[0].RetryCount != 0 {
		t.Fatalf("cancel runtime log = %#v", rows[0])
	}
	if rows[1].DeliveryState != "exhausted" || rows[1].PreviousState != "retrying" || rows[1].Terminal != "retry_exhausted" || rows[1].RetryCount != 2 {
		t.Fatalf("terminal runtime log = %#v", rows[1])
	}
	if rows[2].DeliveryState != "retrying" || rows[2].PreviousState != "active" || rows[2].Reason != "boom" || rows[2].RetryCount != 1 {
		t.Fatalf("retry runtime log = %#v", rows[2])
	}
}

func TestSQLObservabilityReader_ListRuntimeLogs_FailsClosedOnMalformedCanonicalPayload(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	reader := NewSQLObservabilityReader(db, storetest.AdmitPostgresRuntimeStore(t, db))
	ctx := context.Background()

	payload := `{
		"log_level":"error",
		"message":"malformed runtime log",
		"details":"not-an-object"
	}`
	storetest.InsertDiagnosticDirectEventRecord(t, ctx, db, runtimeauthoractivity.DialectPostgres,
		uuid.NewString(), "runtime", []byte(payload), time.Now().UTC())

	_, err := reader.ListRuntimeLogs(ctx, RuntimeLogFilter{}, 10)
	if err == nil || !strings.Contains(err.Error(), "runtime log details must be an object") {
		t.Fatalf("ListRuntimeLogs() error = %v, want malformed canonical payload failure", err)
	}
}

func TestSQLObservabilityReader_ListIncidents_UsesCanonicalRuntimeLogPayloads(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	reader := NewSQLObservabilityReader(db, storetest.AdmitPostgresRuntimeStore(t, db))
	ctx := context.Background()

	insertRuntimeLog := func(component, action, agentID string, createdAt time.Time) {
		t.Helper()
		payload := canonicalRuntimeLogTestPayload(t, component, action, "retry_exhausted", "runtime incident", agentID)
		storetest.InsertDiagnosticDirectEventRecord(t, ctx, db, runtimeauthoractivity.DialectPostgres,
			uuid.NewString(), "runtime", []byte(payload), createdAt)
	}

	now := time.Now().UTC()
	insertRuntimeLog("mcp-gateway", "request_failed", "agent-a", now.Add(-2*time.Minute))
	insertRuntimeLog("mcp-gateway", "request_failed", "agent-b", now.Add(-1*time.Minute))

	rows, err := reader.ListIncidents(ctx, IncidentFilter{SinceHours: 24, MCPOnly: true})
	if err != nil {
		t.Fatalf("ListIncidents: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("incident rows = %d, want 1: %#v", len(rows), rows)
	}
	if rows[0].Code != "retry_exhausted" || rows[0].Count != 2 || rows[0].Component != "mcp-gateway" || rows[0].Level != "error" {
		t.Fatalf("incident row = %#v", rows[0])
	}
	if rows[0].RootCause != "Retry policy was exhausted (retry_exhausted)." {
		t.Fatalf("incident root_cause = %#v, want canonical failure message", rows[0].RootCause)
	}
	if len(rows[0].Agents) != 2 || rows[0].Agents[0] != "agent-a" || rows[0].Agents[1] != "agent-b" {
		t.Fatalf("incident agents = %#v", rows[0].Agents)
	}
	if len(rows[0].Actions) != 1 || rows[0].Actions[0] != "request_failed" {
		t.Fatalf("incident actions = %#v", rows[0].Actions)
	}
}

func TestSQLObservabilityReader_ListIncidents_FailsClosedOnMissingCanonicalComponent(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	reader := NewSQLObservabilityReader(db, storetest.AdmitPostgresRuntimeStore(t, db))
	ctx := context.Background()

	payload := canonicalRuntimeLogTestPayload(t, "", "request_failed", "retry_exhausted", "incomplete runtime incident", "")
	storetest.InsertDiagnosticDirectEventRecord(t, ctx, db, runtimeauthoractivity.DialectPostgres,
		uuid.NewString(), "runtime", []byte(payload), time.Now().UTC())

	_, err := reader.ListIncidents(ctx, IncidentFilter{SinceHours: 24})
	if err == nil || !strings.Contains(err.Error(), "runtime log component is required") {
		t.Fatalf("ListIncidents() error = %v, want missing canonical component failure", err)
	}
}

func TestSQLObservabilityReader_ListIncidents_IgnoresErrorLogsWithoutCanonicalFailure(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	reader := NewSQLObservabilityReader(db, storetest.AdmitPostgresRuntimeStore(t, db))
	ctx := context.Background()

	payload := `{
		"log_level":"error",
		"message":"message fallback",
		"details":{
			"component":"diagnostics",
			"action":"fallback_case"
		}
	}`
	storetest.InsertDiagnosticDirectEventRecord(t, ctx, db, runtimeauthoractivity.DialectPostgres,
		uuid.NewString(), "runtime", []byte(payload), time.Now().UTC())

	rows, err := reader.ListIncidents(ctx, IncidentFilter{SinceHours: 24})
	if err != nil {
		t.Fatalf("ListIncidents: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("incident rows = %d, want none without canonical failure: %#v", len(rows), rows)
	}
}

func TestSQLObservabilityReader_ListIncidents_SortsByRawLastSeenBeforeLimit(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	reader := NewSQLObservabilityReader(db, storetest.AdmitPostgresRuntimeStore(t, db))
	ctx := context.Background()

	base := time.Now().UTC().Truncate(time.Second)
	insertRuntimeLog := func(code, message string, createdAt time.Time) {
		t.Helper()
		payload := canonicalRuntimeLogTestPayload(t, "diagnostics", "same_second_order", code, message, "")
		storetest.InsertDiagnosticDirectEventRecord(t, ctx, db, runtimeauthoractivity.DialectPostgres,
			uuid.NewString(), "runtime", []byte(payload), createdAt)
	}

	insertRuntimeLog("older_code", "older", base.Add(100*time.Millisecond))
	insertRuntimeLog("newer_code", "newer", base.Add(900*time.Millisecond))

	rows, err := reader.ListIncidents(ctx, IncidentFilter{SinceHours: 24, Limit: 1})
	if err != nil {
		t.Fatalf("ListIncidents: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("incident rows = %d, want 1: %#v", len(rows), rows)
	}
	if rows[0].Code != "newer_code" {
		t.Fatalf("incident code = %#v, want newer_code", rows[0].Code)
	}
}
