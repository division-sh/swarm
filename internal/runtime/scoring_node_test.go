package runtime

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"empireai/internal/events"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestScoringNodeParity(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	node := NewScoringNode(bus, pc, db)
	if node == nil {
		t.Fatal("expected scoring node")
	}

	ch := bus.Subscribe("analysis-agent", events.EventType("scoring.requested"))
	verticalID := uuid.NewString()
	evt := events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("vertical.discovered"),
		SourceAgent: "pipeline-coordinator",
		VerticalID:  verticalID,
		Payload: mustJSON(map[string]any{
			"vertical_id":   verticalID,
			"vertical_name": "Payroll Automation",
			"geography":     "argentina",
			"mode":          "saas_gap",
		}),
		CreatedAt: time.Now().UTC(),
	}

	insertEventForScoringNodeLedger(t, db, evt)
	node.processEvent(context.Background(), evt)
	out := waitForEventType(t, ch, "scoring.requested")
	if out.SourceAgent != scoringNodeID {
		t.Fatalf("expected source_agent=%s, got %q", scoringNodeID, out.SourceAgent)
	}
	payload := parsePayloadMap(out.Payload)
	if got := asString(payload["vertical_id"]); got != verticalID {
		t.Fatalf("expected vertical_id=%s, got %q", verticalID, got)
	}
	if got := asString(payload["rubric"]); got == "" {
		t.Fatalf("expected rubric in scoring.requested payload, got %+v", payload)
	}
	dims, _ := payload["dimensions_requested"].([]any)
	if len(dims) == 0 {
		t.Fatalf("expected non-empty dimensions_requested, got payload=%+v", payload)
	}
}

func TestScoringNodeIdempotency(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	node := NewScoringNode(bus, pc, db)
	if node == nil {
		t.Fatal("expected scoring node")
	}

	ch := bus.Subscribe("analysis-agent", events.EventType("scoring.requested"))
	verticalID := uuid.NewString()
	evt := events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("vertical.discovered"),
		SourceAgent: "pipeline-coordinator",
		VerticalID:  verticalID,
		Payload: mustJSON(map[string]any{
			"vertical_id":   verticalID,
			"vertical_name": "Dental Scheduling",
			"geography":     "paraguay",
			"mode":          "saas_gap",
		}),
		CreatedAt: time.Now().UTC(),
	}

	insertEventForScoringNodeLedger(t, db, evt)
	node.processEvent(context.Background(), evt)
	_ = waitForEventType(t, ch, "scoring.requested")

	node.processEvent(context.Background(), evt)
	assertNoEventType(t, ch, "scoring.requested", 250*time.Millisecond)

	var count int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM system_node_ledger
		WHERE event_id = $1::uuid
		  AND node_id = $2
	`, evt.ID, scoringNodeID).Scan(&count); err != nil {
		t.Fatalf("query ledger count: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected one ledger row for replayed event, got %d", count)
	}
}

func TestScoringNodeDeadLetter(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	node := NewScoringNode(bus, pc, db)
	if node == nil {
		t.Fatal("expected scoring node")
	}
	node.retryLimit = 2
	node.backoffFn = func(int) time.Duration { return time.Millisecond }
	node.overrideHandle = func(context.Context, events.Event) error {
		return errors.New("forced failure")
	}

	deadCh := bus.Subscribe("ops-watch", events.EventType("pipeline.dead_letter"))
	verticalID := uuid.NewString()
	evt := events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("vertical.discovered"),
		SourceAgent: "pipeline-coordinator",
		VerticalID:  verticalID,
		Payload: mustJSON(map[string]any{
			"vertical_id":   verticalID,
			"vertical_name": "Invoice Automation",
			"geography":     "argentina",
			"mode":          "saas_gap",
		}),
		CreatedAt: time.Now().UTC(),
	}

	insertEventForScoringNodeLedger(t, db, evt)
	node.processEvent(context.Background(), evt)
	out := waitForEventType(t, deadCh, "pipeline.dead_letter")
	if out.SourceAgent != scoringNodeID {
		t.Fatalf("expected dead-letter source=%s, got %q", scoringNodeID, out.SourceAgent)
	}
	payload := parsePayloadMap(out.Payload)
	if got := asString(payload["event_id"]); got != evt.ID {
		t.Fatalf("expected dead-letter event_id=%s, got %q", evt.ID, got)
	}
	if got := asString(payload["node_id"]); got != scoringNodeID {
		t.Fatalf("expected dead-letter node_id=%s, got %q", scoringNodeID, got)
	}

	var count int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM system_node_ledger
		WHERE event_id = $1::uuid
		  AND node_id = $2
	`, evt.ID, scoringNodeID).Scan(&count); err != nil {
		t.Fatalf("query ledger count: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected dead-letter path to mark event as processed once, got %d", count)
	}
}

func insertEventForScoringNodeLedger(t *testing.T, db *sql.DB, evt events.Event) {
	t.Helper()
	if db == nil {
		t.Fatal("db is required")
	}
	if _, err := db.Exec(`
		INSERT INTO events (id, type, source_agent, task_id, vertical_id, payload, created_at)
		VALUES ($1::uuid, $2, $3, NULLIF($4,'')::uuid, NULLIF($5,'')::uuid, $6::jsonb, $7)
		ON CONFLICT (id) DO NOTHING
	`,
		strings.TrimSpace(evt.ID),
		strings.TrimSpace(string(evt.Type)),
		strings.TrimSpace(evt.SourceAgent),
		strings.TrimSpace(evt.TaskID),
		"",
		string(evt.Payload),
		evt.CreatedAt,
	); err != nil {
		t.Fatalf("insert event row for scoring-node ledger: %v", err)
	}
}
