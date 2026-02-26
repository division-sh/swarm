package runtime

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"empireai/internal/config"
	"empireai/internal/events"
	"empireai/internal/models"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

type captureEventStore struct {
	events     []events.Event
	deliveries int
}

func (c *captureEventStore) AppendEvent(_ context.Context, evt events.Event) error {
	c.events = append(c.events, evt)
	return nil
}
func (c *captureEventStore) InsertEventDeliveries(_ context.Context, _ string, agentIDs []string) error {
	c.deliveries += len(agentIDs)
	return nil
}

func TestExecHumanTaskDecide_ValidationErrors(t *testing.T) {
	ctx := context.Background()

	// Missing DB.
	ex := NewRuntimeToolExecutor(NewEventBus(InMemoryEventStore{}), nil, nil)
	if _, err := ex.execHumanTaskDecide(ctx, models.AgentConfig{ID: "ec", Role: "empire-coordinator"}, map[string]any{
		"task_id": "t", "decision": "approve",
	}); err == nil || !strings.Contains(err.Error(), "sql db is not configured") {
		t.Fatalf("expected db error, got %v", err)
	}

	// Missing bus.
	_, db, _ := testutil.StartPostgres(t)
	ex2 := NewRuntimeToolExecutor(nil, nil, nil)
	ex2.SetSQLDB(db)
	if _, err := ex2.execHumanTaskDecide(ctx, models.AgentConfig{ID: "ec", Role: "empire-coordinator"}, map[string]any{
		"task_id": "t", "decision": "approve",
	}); err == nil || !strings.Contains(err.Error(), "event bus is not configured") {
		t.Fatalf("expected bus error, got %v", err)
	}

	// Unauthorized role.
	ex3 := NewRuntimeToolExecutor(NewEventBus(InMemoryEventStore{}), nil, nil)
	ex3.SetSQLDB(db)
	if _, err := ex3.execHumanTaskDecide(ctx, models.AgentConfig{ID: "a", Role: "opco-ceo"}, map[string]any{
		"task_id": "t", "decision": "approve",
	}); err == nil || !strings.Contains(err.Error(), "not authorized") {
		t.Fatalf("expected auth error, got %v", err)
	}

	// Input validation.
	if _, err := ex3.execHumanTaskDecide(ctx, models.AgentConfig{ID: "ec", Role: "empire-coordinator"}, map[string]any{
		"decision": "approve",
	}); err == nil || !strings.Contains(err.Error(), "task_id is required") {
		t.Fatalf("expected task_id required, got %v", err)
	}
	if _, err := ex3.execHumanTaskDecide(ctx, models.AgentConfig{ID: "ec", Role: "empire-coordinator"}, map[string]any{
		"task_id": "t",
	}); err == nil || !strings.Contains(err.Error(), "decision is required") {
		t.Fatalf("expected decision required, got %v", err)
	}
	if _, err := ex3.execHumanTaskDecide(ctx, models.AgentConfig{ID: "ec", Role: "empire-coordinator"}, map[string]any{
		"task_id": "t", "decision": "weird",
	}); err == nil || !strings.Contains(err.Error(), "unknown decision") {
		t.Fatalf("expected unknown decision, got %v", err)
	}
}

func TestExecHumanTaskDecide_ApproveRejectAndBudgetDeferral(t *testing.T) {
	ctx := context.Background()
	_, db, _ := testutil.StartPostgres(t)

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, credentials, created_at, updated_at)
		VALUES ($1::uuid,'V','v','us','operating','operating','{}'::jsonb, now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}

	store := &captureEventStore{}
	bus := NewEventBus(store)
	ex := NewRuntimeToolExecutor(bus, nil, nil)
	ex.SetSQLDB(db)
	ex.SetConfig(&config.Config{
		Budget: config.BudgetConfig{
			HumanTasks: config.HumanTasksConfig{
				MaxTasksPerWeek: 1,
				BudgetReset:     "monday",
			},
		},
	})

	task1 := uuid.NewString()
	task2 := uuid.NewString()
	task3 := uuid.NewString()
	for _, id := range []string{task1, task2, task3} {
		if _, err := db.ExecContext(ctx, `
			INSERT INTO human_tasks (id, requesting_agent, vertical_id, category, description, status, created_at)
			VALUES ($1::uuid, 'requester-1', $2::uuid, 'ops', 'do it', 'pending_review', now())
		`, id, verticalID); err != nil {
			t.Fatalf("seed task %s: %v", id, err)
		}
	}

	actor := models.AgentConfig{ID: "empire-coordinator", Role: "empire-coordinator"}

	// First approval should succeed (count=0).
	out, err := ex.execHumanTaskDecide(ctx, actor, map[string]any{
		"task_id":       task1,
		"decision":      "approve",
		"reason":        "ok",
		"priority_rank": 2,
	})
	if err != nil {
		t.Fatalf("approve 1: %v", err)
	}
	if out.(map[string]any)["status"] != "approved" {
		t.Fatalf("unexpected approve out: %#v", out)
	}

	// Second approval should be forced to deferred due to weekly cap (max=1).
	out2, err := ex.execHumanTaskDecide(ctx, actor, map[string]any{
		"task_id":  task2,
		"decision": "approved",
	})
	if err != nil {
		t.Fatalf("approve 2: %v", err)
	}
	if out2.(map[string]any)["status"] != "deferred" {
		t.Fatalf("expected deferred, got %#v", out2)
	}

	// Reject path.
	out3, err := ex.execHumanTaskDecide(ctx, actor, map[string]any{
		"task_id":   task3,
		"decision":  "reject",
		"reason":    "no",
		"requeue_date": time.Now().Add(24 * time.Hour).UTC().Format(time.RFC3339),
	})
	if err != nil {
		t.Fatalf("reject: %v", err)
	}
	if out3.(map[string]any)["status"] != "rejected" {
		t.Fatalf("expected rejected, got %#v", out3)
	}

	// Verify DB state updated.
	var s1, s2, s3 string
	_ = db.QueryRowContext(ctx, `SELECT status FROM human_tasks WHERE id = $1::uuid`, task1).Scan(&s1)
	_ = db.QueryRowContext(ctx, `SELECT status FROM human_tasks WHERE id = $1::uuid`, task2).Scan(&s2)
	_ = db.QueryRowContext(ctx, `SELECT status FROM human_tasks WHERE id = $1::uuid`, task3).Scan(&s3)
	if s1 != "approved" || s2 != "deferred" || s3 != "rejected" {
		t.Fatalf("unexpected statuses: %q %q %q", s1, s2, s3)
	}

	// Verify emitted event payloads cover per-decision key branches.
	types := map[string]events.Event{}
	for _, evt := range store.events {
		types[string(evt.Type)] = evt
	}
	if _, ok := types["human_task.approved"]; !ok {
		t.Fatalf("expected approved event, have=%v", store.events)
	}
	if evt, ok := types["human_task.deferred"]; !ok {
		t.Fatalf("expected deferred event, have=%v", store.events)
	} else {
		var p map[string]any
		_ = json.Unmarshal(evt.Payload, &p)
		if strings.TrimSpace(asString(p["requeue_date"])) == "" {
			t.Fatalf("expected requeue_date in deferred payload, got %#v", p)
		}
		if !strings.Contains(strings.ToLower(asString(p["defer_reason"])), "budget") {
			t.Fatalf("expected defer reason to mention budget, got %#v", p)
		}
	}
	if evt, ok := types["human_task.rejected"]; !ok {
		t.Fatalf("expected rejected event, have=%v", store.events)
	} else {
		var p map[string]any
		_ = json.Unmarshal(evt.Payload, &p)
		if asString(p["rejection_reason"]) != "no" {
			t.Fatalf("expected rejection_reason, got %#v", p)
		}
	}
}

