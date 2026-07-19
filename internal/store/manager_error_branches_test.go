package store

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestPostgresStore_Manager_ErrorBranches(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	const runID = "44444444-4444-4444-4444-444444444444"
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(), runID)

	// UpsertAgent: missing id.
	if err := pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{Config: runtimeactors.AgentConfig{ExecutionMode: "live"}}); err == nil {
		t.Fatal("expected missing agent id error")
	}

	// Canonical lifecycle transition: validation.
	if _, err := pg.CommitAgentLifecycleTransition(ctx, runtimemanager.AgentLifecycleTransition{}); err == nil {
		t.Fatal("expected lifecycle transition validation error")
	}

	// Routing rule required fields.
	if err := pg.UpsertRoutingRule(ctx, runtimemanager.PersistedRoutingRule{}); err == nil {
		t.Fatal("expected UpsertRoutingRule validation error")
	}

	// UpsertEventReceipt should accept empty errText; also exercise invalid status guardrails indirectly.
	aid := "a1"
	_ = pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{ExecutionMode: "live", ID: aid, Role: "r", FlowID: "global", Type: "stub", Model: "regular", Config: []byte(`{"subscriptions":["*"]}`)},
		Status: "active", HiredBy: "t", StartedAt: time.Now(),
	})
	evtID := uuid.NewString()
	if err := commitSemanticEventFixture(ctx, pg, eventtest.RunCreatingRootIngress(evtID,
		"test.event",
		"tester", "", []byte(`{}`), 0, eventtest.UUID("persisted-projection-run"), "", events.EventEnvelope{}, time.Now())); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}
	if err := pg.InsertEventDeliveries(ctx, evtID, []string{aid}); err != nil {
		t.Fatalf("InsertEventDeliveries: %v", err)
	}
	if err := pg.UpsertEventReceipt(ctx, evtID, aid, "processed", nil); err != nil {
		t.Fatalf("UpsertEventReceipt: %v", err)
	}
}
