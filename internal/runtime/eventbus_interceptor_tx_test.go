package runtime_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"empireai/internal/events"
	rt "empireai/internal/runtime"
	"empireai/internal/store"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

type consumeWithDeferred struct {
	deferred events.Event
}

func (i consumeWithDeferred) Intercept(_ context.Context, _ events.Event) (bool, []events.Event, error) {
	return false, []events.Event{i.deferred}, nil
}

func TestEventBusTransactionalPublish_RollsBackWhenDeferredPersistFails(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	bus := rt.NewEventBus(pg)

	bus.SetInterceptors(consumeWithDeferred{
		deferred: events.Event{
			ID:          "not-a-uuid", // force AppendEventTx failure inside publish transaction
			Type:        events.EventType("portfolio.digest_compiled"),
			SourceAgent: "pipeline-coordinator",
			Payload:     []byte(`{"ok":true}`),
			CreatedAt:   time.Now(),
		},
	})

	watch := bus.Subscribe("watcher", events.EventType("portfolio.digest_compiled"))

	err := bus.Publish(context.Background(), events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("vertical.shortlisted"),
		SourceAgent: "scoring-coordinator",
		Payload:     []byte(`{"vertical_id":"v-temp"}`),
		CreatedAt:   time.Now(),
	})
	if err == nil {
		t.Fatal("expected transactional publish failure")
	}
	if !strings.Contains(err.Error(), "persist deferred event") {
		t.Fatalf("expected deferred persist error, got %v", err)
	}

	var eventCount int
	if qErr := db.QueryRowContext(context.Background(), `SELECT count(*) FROM events`).Scan(&eventCount); qErr != nil {
		t.Fatalf("query event count: %v", qErr)
	}
	if eventCount != 0 {
		t.Fatalf("expected full transaction rollback (0 events), got %d", eventCount)
	}

	select {
	case evt := <-watch:
		t.Fatalf("expected no delivery after rollback, got %s", evt.Type)
	case <-time.After(150 * time.Millisecond):
	}
}

func TestFactoryPipelineInterceptor_DeduplicatesRepeatedInboundEventID(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	if _, err := db.ExecContext(context.Background(), `
		CREATE TABLE IF NOT EXISTS pipeline_transitions (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			event_id UUID NOT NULL REFERENCES events(id),
			event_type TEXT NOT NULL,
			handler TEXT NOT NULL,
			pipeline_type TEXT NOT NULL,
			pipeline_id UUID NOT NULL,
			action TEXT NOT NULL,
			state_before JSONB,
			state_after JSONB,
			events_emitted TEXT[],
			drop_reason TEXT,
			error TEXT,
			duration_us INT,
			created_at TIMESTAMPTZ DEFAULT now()
		)
	`); err != nil {
		t.Fatalf("create pipeline_transitions: %v", err)
	}
	pg := &store.PostgresStore{DB: db}
	bus := rt.NewEventBus(pg)
	pc := rt.NewFactoryPipelineCoordinator(bus, db)
	bus.SetInterceptors(pc)

	watch := bus.Subscribe("watcher", events.EventType("market_research.scan_assigned"))
	scanID := uuid.NewString()
	inbound := events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("scan.requested"),
		SourceAgent: "empire-coordinator",
		Payload: mustJSON(t, map[string]any{
			"scan_id":   scanID,
			"mode":      "saas_gap",
			"geography": "Buenos Aires, Argentina",
		}),
		CreatedAt: time.Now(),
	}

	if err := bus.Publish(context.Background(), inbound); err != nil {
		t.Fatalf("publish first: %v", err)
	}
	if err := bus.Publish(context.Background(), inbound); err != nil {
		t.Fatalf("publish duplicate: %v", err)
	}

	count := 0
	deadline := time.After(400 * time.Millisecond)
	for {
		select {
		case evt := <-watch:
			if string(evt.Type) != "market_research.scan_assigned" {
				continue
			}
			var payload map[string]any
			if err := json.Unmarshal(evt.Payload, &payload); err != nil {
				t.Fatalf("decode payload: %v", err)
			}
			if strings.TrimSpace(asString(payload["scan_id"])) == scanID {
				count++
			}
		case <-deadline:
			if count != 1 {
				t.Fatalf("expected exactly one emitted scan assignment for duplicate inbound event id, got %d", count)
			}
			return
		}
	}
}

func asString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}
