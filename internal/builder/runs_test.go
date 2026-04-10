package builder

import (
	"context"
	"testing"
	"time"

	runtimepkg "swarm/internal/runtime"
	runtimebus "swarm/internal/runtime/bus"
)

type snapshotRunStore struct {
	runtimebus.InMemoryEventStore
	snapshot runtimebus.RunLifecycleSnapshot
}

func (s *snapshotRunStore) MarkRunTerminal(_ context.Context, runID, status, errorSummary string, endedAt time.Time) error {
	s.snapshot.RunID = runID
	s.snapshot.Status = status
	s.snapshot.ErrorSummary = errorSummary
	ended := endedAt
	s.snapshot.EndedAt = &ended
	return nil
}

func (s *snapshotRunStore) LoadRunLifecycleSnapshot(context.Context, string) (runtimebus.RunLifecycleSnapshot, error) {
	return s.snapshot, nil
}

func TestRunHubStartRunPublishesTypedEntityEnvelope(t *testing.T) {
	eb, err := runtimebus.NewEventBus(runtimebus.InMemoryEventStore{})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	rt := &runtimepkg.Runtime{Bus: eb}
	hub := newRunHub(func() *runtimepkg.Runtime { return rt }, nil, nil, nil)
	ch := eb.Subscribe("observer", "review.requested")

	if err := hub.startRun(context.Background(), "run-123", map[string]any{
		"review.requested": map[string]any{
			"entity_id": "ent-001",
			"name":      "Telemedicine",
		},
	}, nil); err != nil {
		t.Fatalf("startRun: %v", err)
	}

	select {
	case evt := <-ch:
		if got := evt.EntityID(); got != "ent-001" {
			t.Fatalf("event entity_id = %q, want ent-001", got)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("expected run input event to be published")
	}
}

func TestRunHubAwaitCompletion_MarksSessionTerminalWhenCompletionPersistenceFails(t *testing.T) {
	eb, err := runtimebus.NewEventBus(runtimebus.InMemoryEventStore{})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	rt := &runtimepkg.Runtime{Bus: eb}
	hub := &runHub{
		sessions: map[string]*runSession{
			"run-123": {
				runID:   "run-123",
				runtime: rt,
				subs:    map[string]func(RunEventEnvelope){},
				events:  []RunEventEnvelope{},
			},
		},
	}

	hub.awaitCompletion("run-123")

	if !hub.isTerminal("run-123") {
		t.Fatal("expected run session to be marked terminal when completion persistence fails")
	}
	session := hub.session("run-123")
	if session == nil {
		t.Fatal("expected run session to remain addressable")
	}
	if len(session.events) == 0 {
		t.Fatal("expected terminal failure event to be emitted")
	}
	last := session.events[len(session.events)-1]
	if got, _ := last["type"].(string); got != "run.failed" {
		t.Fatalf("last event type = %q, want run.failed", got)
	}
	payload, _ := last["payload"].(map[string]any)
	if _, ok := payload["persistence_error"]; !ok {
		t.Fatalf("last payload = %#v, want persistence_error", payload)
	}
}

func TestRunHubAwaitCompletion_EmitsAuthoritativeRunSummary(t *testing.T) {
	store := &snapshotRunStore{
		snapshot: runtimebus.RunLifecycleSnapshot{
			RunID:       "run-123",
			EventCount:  3,
			EntityCount: 2,
			StartedAt:   time.Now().UTC().Add(-2 * time.Second),
		},
	}
	eb, err := runtimebus.NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	rt := &runtimepkg.Runtime{Bus: eb}
	hub := &runHub{
		sessions: map[string]*runSession{
			"run-123": {
				runID:   "run-123",
				runtime: rt,
				subs:    map[string]func(RunEventEnvelope){},
				events:  []RunEventEnvelope{},
			},
		},
	}

	hub.awaitCompletion("run-123")

	session := hub.session("run-123")
	if session == nil || len(session.events) == 0 {
		t.Fatal("expected terminal event to be emitted")
	}
	last := session.events[len(session.events)-1]
	if got, _ := last["type"].(string); got != "run.completed" {
		t.Fatalf("last event type = %q, want run.completed", got)
	}
	payload, _ := last["payload"].(map[string]any)
	summary, _ := payload["summary"].(map[string]any)
	if got := int(summary["total_events"].(int)); got != 3 {
		t.Fatalf("summary.total_events = %d, want 3", got)
	}
	if got := int(summary["entity_count"].(int)); got != 2 {
		t.Fatalf("summary.entity_count = %d, want 2", got)
	}
	if got, ok := summary["duration_ms"].(int64); !ok || got <= 0 {
		t.Fatalf("summary.duration_ms = %#v, want positive int64", summary["duration_ms"])
	}
}
