package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
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
		state         runtimedelivery.State
		activeSession string
		wantState     string
		wantLayer     string
	}
	cases := []lifecycleCase{
		{agentID: "agent-queued", state: runtimedelivery.StateQueued, wantState: "queued", wantLayer: "delivery_queue"},
		{agentID: "agent-launching", state: runtimedelivery.StateLaunching, wantState: "launching", wantLayer: "session_launch"},
		{agentID: "agent-active", state: runtimedelivery.StateActive, activeSession: uuid.NewString(), wantState: "active", wantLayer: "session_execution"},
		{agentID: "agent-retrying", state: runtimedelivery.StateRetrying, wantState: "retrying", wantLayer: "delivery_retry"},
		{agentID: "agent-exhausted", state: runtimedelivery.StateExhausted, wantState: "exhausted", wantLayer: "delivery_terminal"},
	}
	agentIDs := make([]string, 0, len(cases))
	for _, tc := range cases {
		eventID := uuid.NewString()
		route := events.DeliveryRoute{SubscriberType: "agent", SubscriberID: tc.agentID}
		event := seedAgentLifecycleEvent(t, ctx, pg, eventID, runID, route, time.Now().UTC())
		if tc.activeSession != "" {
			seedAgentLifecycleSession(t, ctx, db, runID, tc.agentID, tc.activeSession)
		}
		if tc.state != runtimedelivery.StateQueued {
			claimed, err := pg.ClaimAgentDelivery(ctx, event, route)
			if err != nil {
				t.Fatalf("claim delivery for %s: %v", tc.agentID, err)
			}
			switch tc.state {
			case runtimedelivery.StateLaunching:
			case runtimedelivery.StateActive:
				if _, err := pg.BindAgentSession(ctx, claimed.Claim, tc.activeSession); err != nil {
					t.Fatalf("bind delivery for %s: %v", tc.agentID, err)
				}
			case runtimedelivery.StateRetrying:
				if _, err := pg.SettleFailure(ctx, claimed.Claim, runtimedelivery.Settlement{
					Disposition: runtimedelivery.FailureRetry,
					Failure:     testRetryableFailure(),
					RetryBase:   time.Hour,
				}); err != nil {
					t.Fatalf("settle retry for %s: %v", tc.agentID, err)
				}
			case runtimedelivery.StateExhausted:
				failure := testFailureEnvelope(runtimefailures.ClassRetryExhausted, "lifecycle_exhausted", nil)
				if _, err := pg.SettleFailure(ctx, claimed.Claim, runtimedelivery.Settlement{
					Disposition: runtimedelivery.FailureDeadLetter,
					ReasonCode:  "lifecycle_exhausted",
					Failure:     &failure,
				}); err != nil {
					t.Fatalf("settle exhaustion for %s: %v", tc.agentID, err)
				}
			}
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
	activeRoute := events.DeliveryRoute{SubscriberType: "agent", SubscriberID: "agent-1"}
	activeEvent := seedAgentLifecycleEvent(t, ctx, pg, activeEventID, runID, activeRoute, time.Now().UTC())
	deadLetterEvent := seedAgentLifecycleEvent(t, ctx, pg, oldDeadLetterEventID, runID, activeRoute, time.Now().UTC().Add(-time.Hour))
	activeSessionID := uuid.NewString()
	seedAgentLifecycleSession(t, ctx, db, runID, "agent-1", activeSessionID)
	deadLetterClaim, err := pg.ClaimAgentDelivery(ctx, deadLetterEvent, activeRoute)
	if err != nil {
		t.Fatalf("claim old delivery: %v", err)
	}
	deadLetterFailure := testFailureEnvelope(runtimefailures.ClassRetryExhausted, "old_exhausted", nil)
	if _, err := pg.SettleFailure(ctx, deadLetterClaim.Claim, runtimedelivery.Settlement{Disposition: runtimedelivery.FailureDeadLetter, ReasonCode: "old_exhausted", Failure: &deadLetterFailure}); err != nil {
		t.Fatalf("settle old delivery: %v", err)
	}
	activeClaim, err := pg.ClaimAgentDelivery(ctx, activeEvent, activeRoute)
	if err != nil {
		t.Fatalf("claim active delivery: %v", err)
	}
	if _, err := pg.BindAgentSession(ctx, activeClaim.Claim, activeSessionID); err != nil {
		t.Fatalf("bind active delivery: %v", err)
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
	route := events.DeliveryRoute{SubscriberType: "agent", SubscriberID: "agent-1"}
	event := seedAgentLifecycleEvent(t, ctx, pg, eventID, runID, route, time.Now().UTC())
	claimed, err := pg.ClaimAgentDelivery(ctx, event, route)
	if err != nil {
		t.Fatalf("claim terminal delivery: %v", err)
	}
	failure := testFailureEnvelope(runtimefailures.ClassRetryExhausted, "terminal_exhausted", nil)
	if _, err := pg.SettleFailure(ctx, claimed.Claim, runtimedelivery.Settlement{Disposition: runtimedelivery.FailureDeadLetter, ReasonCode: "terminal_exhausted", Failure: &failure}); err != nil {
		t.Fatalf("settle terminal delivery: %v", err)
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

func seedAgentLifecycleEvent(t *testing.T, ctx context.Context, pg *PostgresStore, eventID, runID string, route events.DeliveryRoute, createdAt time.Time) events.Event {
	t.Helper()
	parentID := eventtest.UUID("agent-lifecycle-parent:" + eventID)
	if err := commitSemanticParentFixture(ctx, pg, runID, parentID, createdAt.Add(-time.Microsecond)); err != nil {
		t.Fatalf("seed lifecycle parent %s: %v", parentID, err)
	}
	event := eventtest.PersistedChildForProducer(
		eventID, "task.completed", eventtest.Producer(events.EventProducerAgent, "runtime"), "",
		json.RawMessage(`{}`), 0, runID, parentID, events.EventEnvelope{}, createdAt,
	)
	if err := commitSemanticEventFixtureWithRoutes(ctx, pg, event, []events.DeliveryRoute{route}); err != nil {
		t.Fatalf("seed lifecycle event %s: %v", eventID, err)
	}
	return event
}

func seedAgentLifecycleSession(t *testing.T, ctx context.Context, db *sql.DB, runID, agentID, sessionID string) {
	t.Helper()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (agent_id, role, model, memory_enabled, memory_source)
		VALUES ($1, 'worker', 'test', TRUE, 'authored')
		ON CONFLICT (agent_id) DO NOTHING
	`, agentID); err != nil {
		t.Fatalf("seed lifecycle agent %s: %v", agentID, err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (session_id, run_id, agent_id, flow_instance, memory_enabled, memory_source, conversation, runtime_state, status)
		VALUES ($1::uuid, $2::uuid, $3, 'lifecycle/test', TRUE, 'authored', '[]'::jsonb, '{}'::jsonb, 'active')
	`, sessionID, runID, agentID); err != nil {
		t.Fatalf("seed lifecycle session %s: %v", sessionID, err)
	}
}
