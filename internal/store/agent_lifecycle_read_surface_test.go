package store

import (
	"testing"

	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestAgentLifecycleBlockingLayerCoversEveryCurrentProducerState(t *testing.T) {
	want := map[runtimedelivery.State]string{
		runtimedelivery.StateQueued:    "delivery_queue",
		runtimedelivery.StateLaunching: "session_launch",
		runtimedelivery.StateActive:    "session_execution",
		runtimedelivery.StateRetrying:  "delivery_retry",
		runtimedelivery.StateExhausted: "delivery_terminal",
	}
	for state, layer := range want {
		if got := agentLifecycleBlockingLayer(state); got != layer {
			t.Errorf("agentLifecycleBlockingLayer(%q) = %q, want %q", state, got, layer)
		}
	}
	if got := agentLifecycleBlockingLayer(runtimedelivery.StateDelivered); got != "" {
		t.Fatalf("delivered blocking layer = %q, want empty because delivered is not a current diagnosis state", got)
	}
}

func TestPostgresStore_ListAgentDeliveryLifecycleFacts_CoversEveryCurrentStateLayerPair(t *testing.T) {
	dsn, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pg, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	bootstrapTestPostgresStore(t, pg)
	t.Cleanup(func() { _ = pg.DB.Close() })

	ctx := testAuthorActivityContext()
	runID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}

	type lifecycleCase struct {
		agentID       string
		status        string
		activeSession string
		wantState     string
		wantLayer     string
	}
	cases := []lifecycleCase{
		{agentID: "agent-queued", status: "pending", wantState: "queued", wantLayer: "delivery_queue"},
		{agentID: "agent-launching", status: "in_progress", wantState: "launching", wantLayer: "session_launch"},
		{agentID: "agent-active", status: "in_progress", activeSession: uuid.NewString(), wantState: "active", wantLayer: "session_execution"},
		{agentID: "agent-retrying", status: "failed", wantState: "retrying", wantLayer: "delivery_retry"},
		{agentID: "agent-exhausted", status: "dead_letter", wantState: "exhausted", wantLayer: "delivery_terminal"},
	}
	agentIDs := make([]string, 0, len(cases))
	for index, tc := range cases {
		eventID := uuid.NewString()
		if _, err := db.ExecContext(ctx, `
			INSERT INTO events (execution_mode, event_id, run_id, event_name, scope, payload, produced_by, produced_by_type)
			VALUES ('live', $1::uuid, $2::uuid, 'task.completed', 'global', '{}'::jsonb, 'runtime', 'agent')
		`, eventID, runID); err != nil {
			t.Fatalf("seed event for %s: %v", tc.agentID, err)
		}
		var activeSession any
		if tc.activeSession != "" {
			activeSession = tc.activeSession
		}
		if _, err := db.ExecContext(ctx, `
			INSERT INTO event_deliveries (
				run_id, event_id, subscriber_type, subscriber_id, status, active_session_id, created_at, delivered_at
			) VALUES (
				$1::uuid, $2::uuid, 'agent', $3, $4, $5::uuid,
				now() - ($6 * interval '1 minute'),
				CASE WHEN $4 = 'dead_letter' THEN now() ELSE NULL END
			)
		`, runID, eventID, tc.agentID, tc.status, activeSession, index+1); err != nil {
			t.Fatalf("seed delivery for %s: %v", tc.agentID, err)
		}
		agentIDs = append(agentIDs, tc.agentID)
	}

	facts, err := pg.ListAgentDeliveryLifecycleFacts(ctx, agentIDs)
	if err != nil {
		t.Fatalf("ListAgentDeliveryLifecycleFacts: %v", err)
	}
	for _, tc := range cases {
		got := facts[tc.agentID]
		if got.CurrentState != tc.wantState || got.BlockingLayer != tc.wantLayer {
			t.Errorf("%s lifecycle facts = %#v, want state=%q layer=%q", tc.agentID, got, tc.wantState, tc.wantLayer)
		}
	}
}

func TestPostgresStore_ListAgentDeliveryLifecycleFacts_UsesCanonicalLiveLifecycle(t *testing.T) {
	dsn, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pg, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	bootstrapTestPostgresStore(t, pg)
	t.Cleanup(func() { _ = pg.DB.Close() })

	ctx := testAuthorActivityContext()
	runID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}

	activeEventID := uuid.NewString()
	oldDeadLetterEventID := uuid.NewString()
	for _, eventID := range []string{activeEventID, oldDeadLetterEventID} {
		if _, err := db.ExecContext(ctx, `
			INSERT INTO events (execution_mode,
				event_id, run_id, event_name, scope, payload, produced_by, produced_by_type
			) VALUES ('live',
				$1::uuid, $2::uuid, 'task.completed', 'global', '{}'::jsonb, 'runtime', 'agent'
			)
		`, eventID, runID); err != nil {
			t.Fatalf("seed event %s: %v", eventID, err)
		}
	}

	activeSessionID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, active_session_id, created_at
		) VALUES
			($1::uuid, $2::uuid, 'agent', 'agent-1', 'in_progress', $3::uuid, now() - interval '1 minute'),
			($1::uuid, $4::uuid, 'agent', 'agent-1', 'dead_letter', NULL, now() - interval '2 hours')
	`, runID, activeEventID, activeSessionID, oldDeadLetterEventID); err != nil {
		t.Fatalf("seed deliveries: %v", err)
	}

	facts, err := pg.ListAgentDeliveryLifecycleFacts(ctx, []string{"agent-1"})
	if err != nil {
		t.Fatalf("ListAgentDeliveryLifecycleFacts: %v", err)
	}
	if got := facts["agent-1"].CurrentState; got != "active" {
		t.Fatalf("current_state = %q, want active", got)
	}
	if got := facts["agent-1"].BlockingLayer; got != "session_execution" {
		t.Fatalf("blocking_layer = %q, want session_execution", got)
	}
}

func TestPostgresStore_ListAgentDeliveryLifecycleFacts_UsesCanonicalTerminalLifecycleWhenNoLiveWorkRemains(t *testing.T) {
	dsn, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pg, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	bootstrapTestPostgresStore(t, pg)
	t.Cleanup(func() { _ = pg.DB.Close() })

	ctx := testAuthorActivityContext()
	runID := uuid.NewString()
	eventID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (execution_mode,
			event_id, run_id, event_name, scope, payload, produced_by, produced_by_type
		) VALUES ('live',
			$1::uuid, $2::uuid, 'task.completed', 'global', '{}'::jsonb, 'runtime', 'agent'
		)
	`, eventID, runID); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, created_at, delivered_at
		) VALUES (
			$1::uuid, $2::uuid, 'agent', 'agent-1', 'dead_letter', now() - interval '5 minutes', now() - interval '1 minute'
		)
	`, runID, eventID); err != nil {
		t.Fatalf("seed delivery: %v", err)
	}

	facts, err := pg.ListAgentDeliveryLifecycleFacts(ctx, []string{"agent-1"})
	if err != nil {
		t.Fatalf("ListAgentDeliveryLifecycleFacts: %v", err)
	}
	if got := facts["agent-1"].CurrentState; got != "exhausted" {
		t.Fatalf("current_state = %q, want exhausted", got)
	}
	if got := facts["agent-1"].BlockingLayer; got != "delivery_terminal" {
		t.Fatalf("blocking_layer = %q, want delivery_terminal", got)
	}
}
