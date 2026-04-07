package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"swarm/internal/testutil"
)

func TestSQLObservabilityReader_ListEvents_UsesCanonicalDeliveryLifecycle(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	reader := NewSQLObservabilityReader(db)
	ctx := context.Background()

	runID := uuid.NewString()
	eventID := uuid.NewString()
	entityID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			event_id, run_id, event_name, entity_id, scope, payload, produced_by, produced_by_type, created_at
		) VALUES (
			$1::uuid, $2::uuid, 'task.completed', $3::uuid, 'entity', '{"entity_id": "`+entityID+`"}'::jsonb, 'runtime', 'agent', $4
		)
	`, eventID, runID, entityID, time.Unix(1700000000, 0).UTC()); err != nil {
		t.Fatalf("seed event: %v", err)
	}

	seedDelivery := func(subscriberID, status string, retryCount int, errText string, createdAt time.Time) {
		t.Helper()
		if _, err := db.ExecContext(ctx, `
			INSERT INTO event_deliveries (
				run_id, event_id, subscriber_type, subscriber_id, status, retry_count, last_error, created_at
			) VALUES (
				$1::uuid, $2::uuid, 'agent', $3, $4, $5, NULLIF($6, ''), $7
			)
		`, runID, eventID, subscriberID, status, retryCount, errText, createdAt); err != nil {
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
			event_id, subscriber_type, subscriber_id, outcome, side_effects, processed_at
		) VALUES
			($1::uuid, 'agent', 'agent-pending', 'dead_letter', '{"retry_count":9,"error":"receipt-should-not-win"}'::jsonb, now()),
			($1::uuid, 'agent', 'agent-failed', 'success', '{"retry_count":0,"error":"receipt-success"}'::jsonb, now())
	`, eventID); err != nil {
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
}

func TestSQLObservabilityReader_GetEvent_UsesCanonicalDeliveryRows(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	reader := NewSQLObservabilityReader(db)
	ctx := context.Background()

	runID := uuid.NewString()
	eventID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			event_id, run_id, event_name, scope, payload, produced_by, produced_by_type, created_at
		) VALUES (
			$1::uuid, $2::uuid, 'task.completed', 'global', '{}'::jsonb, 'runtime', 'agent', now()
		)
	`, eventID, runID); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, retry_count, last_error, created_at
		) VALUES (
			$1::uuid, $2::uuid, 'agent', 'agent-a', 'pending', 1, 'delivery-wins', now()
		)
	`, runID, eventID); err != nil {
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
	if item := got.Deliveries[0]; item.AgentID != "agent-a" || item.Status != "pending" || item.RetryCount != 1 || item.Error != "delivery-wins" {
		t.Fatalf("delivery = %#v", item)
	}
}

func TestHandler_EventDetailIncludesDeliveryLifecycle(t *testing.T) {
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
	reader := NewSQLObservabilityReader(db)
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
		if _, err := db.ExecContext(ctx, `
			INSERT INTO events (
				event_id, event_name, scope, payload, produced_by, produced_by_type, created_at
			) VALUES (
				gen_random_uuid(), 'platform.runtime_log', 'global', $1::jsonb, 'runtime', 'platform', $2
			)
		`, payload, createdAt); err != nil {
			t.Fatalf("insert runtime_log: %v", err)
		}
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
