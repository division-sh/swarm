package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	"empireai/internal/events"
	"empireai/internal/models"
	"empireai/internal/runtime"
	"empireai/internal/store"
	"empireai/internal/testutil"
)

type loadAgentsErrStore struct {
	runtime.ManagerPersistence
}

func (s loadAgentsErrStore) LoadAgents(context.Context) ([]runtime.PersistedAgent, error) {
	return nil, errors.New("load failed")
}

func TestEnsureTargetAgentRegistered_Branches(t *testing.T) {
	ctx := context.Background()

	// Nil manager store is a no-op.
	if err := ensureTargetAgentRegistered(ctx, storeBundle{}, targetAgent{ID: "a1"}); err != nil {
		t.Fatalf("nil manager store should be noop: %v", err)
	}

	// LoadAgents failure should be wrapped.
	if err := ensureTargetAgentRegistered(ctx, storeBundle{ManagerStore: loadAgentsErrStore{}}, targetAgent{ID: "a1"}); err == nil || !strings.Contains(err.Error(), "load agents") {
		t.Fatalf("expected load agents error, got %v", err)
	}

	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}

	// Missing system_prompt for a new target should fail.
	err := ensureTargetAgentRegistered(ctx, storeBundle{ManagerStore: pg}, targetAgent{
		ID: "new-target",
		Config: models.AgentConfig{
			ID:   "new-target",
			Role: "backend-agent",
			Mode: "operating",
			Type: "sonnet",
		},
	})
	if err == nil || !strings.Contains(err.Error(), "system_prompt") {
		t.Fatalf("expected missing system_prompt error, got %v", err)
	}

	// Existing target should short-circuit even if config has no system prompt.
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (id, type, role, mode, status, config, started_at, last_active_at)
		VALUES ('existing-target', 'sonnet', 'backend-agent', 'operating', 'active', '{}'::jsonb, now(), now())
	`); err != nil {
		t.Fatalf("seed existing target: %v", err)
	}
	if err := ensureTargetAgentRegistered(ctx, storeBundle{ManagerStore: pg}, targetAgent{
		ID: "existing-target",
		Config: models.AgentConfig{
			ID:   "existing-target",
			Role: "backend-agent",
		},
	}); err != nil {
		t.Fatalf("existing target should succeed: %v", err)
	}

	// New target with system prompt should be upserted.
	if err := ensureTargetAgentRegistered(ctx, storeBundle{ManagerStore: pg}, targetAgent{
		ID: "fresh-target",
		Config: models.AgentConfig{
			ID:   "fresh-target",
			Role: "qa-agent",
			Mode: "operating",
			Type: "sonnet",
			Config: mustJSON(map[string]any{
				"system_prompt": "You are QA.",
				"tools":         []string{"mailbox_send"},
				"subscriptions": []string{"bug_reported"},
			}),
		},
	}); err != nil {
		t.Fatalf("upsert fresh target: %v", err)
	}
	var found int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM agents WHERE id = 'fresh-target'`).Scan(&found); err != nil {
		t.Fatalf("query fresh-target: %v", err)
	}
	if found != 1 {
		t.Fatalf("expected fresh-target inserted, got %d", found)
	}
}

type captureEventStore struct {
	appendErr  error
	deliverErr error
	lastEvent  events.Event
	lastID     string
	lastAgents []string
}

func (s *captureEventStore) AppendEvent(_ context.Context, evt events.Event) error {
	s.lastEvent = evt
	return s.appendErr
}

func (s *captureEventStore) InsertEventDeliveries(_ context.Context, eventID string, agentIDs []string) error {
	s.lastID = eventID
	s.lastAgents = append([]string(nil), agentIDs...)
	return s.deliverErr
}

func TestDispatchBoardMessage_Branches(t *testing.T) {
	ctx := context.Background()
	target := targetAgent{ID: "empire-coordinator", Role: "empire-coordinator"}

	if _, err := dispatchBoardMessage(ctx, storeBundle{}, target, events.EventType("board.chat"), "hello"); err == nil {
		t.Fatal("expected error without persistent event store")
	}

	if _, err := dispatchBoardMessage(ctx, storeBundle{EventStore: &captureEventStore{}}, target, events.EventType("board.chat"), "   "); err == nil {
		t.Fatal("expected message required error")
	}

	appendFail := &captureEventStore{appendErr: errors.New("append failed")}
	if _, err := dispatchBoardMessage(ctx, storeBundle{EventStore: appendFail}, target, events.EventType("board.chat"), "hello"); err == nil || !strings.Contains(err.Error(), "append failed") {
		t.Fatalf("expected append error, got %v", err)
	}

	deliverFail := &captureEventStore{deliverErr: errors.New("delivery failed")}
	if _, err := dispatchBoardMessage(ctx, storeBundle{EventStore: deliverFail}, target, events.EventType("board.chat"), "hello"); err == nil || !strings.Contains(err.Error(), "delivery failed") {
		t.Fatalf("expected delivery error, got %v", err)
	}

	okStore := &captureEventStore{}
	eventID, err := dispatchBoardMessage(ctx, storeBundle{EventStore: okStore}, target, events.EventType("board.chat"), "hello")
	if err != nil {
		t.Fatalf("dispatchBoardMessage success: %v", err)
	}
	if strings.TrimSpace(eventID) == "" {
		t.Fatal("expected non-empty event id")
	}
	if okStore.lastEvent.Type != events.EventType("board.chat") || okStore.lastEvent.SourceAgent != "human-board" {
		t.Fatalf("unexpected appended event: %+v", okStore.lastEvent)
	}
	if okStore.lastID != eventID {
		t.Fatalf("expected delivery id %q got %q", eventID, okStore.lastID)
	}
	if len(okStore.lastAgents) != 1 || okStore.lastAgents[0] != "empire-coordinator" {
		t.Fatalf("unexpected delivery agents: %#v", okStore.lastAgents)
	}
}

func TestRequireSystemStarted_Branches(t *testing.T) {
	ctx := context.Background()
	if _, err := hasSystemStarted(ctx, nil); err == nil {
		t.Fatal("expected hasSystemStarted db=nil error")
	}
	if err := requireSystemStarted(ctx, nil); err == nil {
		t.Fatal("expected requireSystemStarted db=nil error")
	}

	_, db, _ := testutil.StartPostgres(t)
	ok, err := hasSystemStarted(ctx, db)
	if err != nil {
		t.Fatalf("hasSystemStarted fresh db: %v", err)
	}
	if ok {
		t.Fatal("expected hasSystemStarted false for fresh db")
	}
	if err := requireSystemStarted(ctx, db); err == nil {
		t.Fatal("expected requireSystemStarted error when event missing")
	}

	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (id, type, source_agent, payload, created_at)
		VALUES (gen_random_uuid(), 'system.started', 'runtime', '{}'::jsonb, now())
	`); err != nil {
		t.Fatalf("seed system.started event: %v", err)
	}
	ok, err = hasSystemStarted(ctx, db)
	if err != nil || !ok {
		t.Fatalf("hasSystemStarted after seed ok=%v err=%v", ok, err)
	}
	if err := requireSystemStarted(ctx, db); err != nil {
		t.Fatalf("requireSystemStarted after seed: %v", err)
	}
}

