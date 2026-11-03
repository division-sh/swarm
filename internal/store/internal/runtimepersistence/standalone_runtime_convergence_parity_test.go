package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	agentfixture "github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

type standaloneConvergenceAgentStore interface {
	agentfixture.Store
}

func seedStandaloneConvergenceAgent(t *testing.T, selected any, ctx context.Context, identity agentidentity.Identity) {
	t.Helper()
	store, ok := selected.(standaloneConvergenceAgentStore)
	if !ok {
		t.Fatalf("standalone convergence store %T cannot persist agents", selected)
	}
	agentID := identity.AgentID()
	if err := agentfixture.UpsertStatic(t, ctx, store, runtimemanager.PersistedAgent{
		Config: withRuntimePersistenceTestIntent(t, runtimeactors.AgentConfig{
			ID: agentID, Identity: identity, Role: "observer", FlowID: "global", Type: "stub", Model: "regular",
			ExecutionMode: "live", Config: json.RawMessage(`{}`),
		}),
		Status: "active", HiredBy: "test", StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("UpsertAgent(%s): %v", agentID, err)
	}
}

func TestSameLabelCausalAndRunScopedEventsCannotConvergeExistingRunParity(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) authorActivityReceiptFixture
	}{
		{name: "sqlite", open: openSQLiteAuthorActivityReceiptFixture},
		{name: "postgres", open: openPostgresAuthorActivityReceiptFixture},
	} {
		for _, intent := range []string{"causal", "run_scoped"} {
			backend, intent := backend, intent
			t.Run(backend.name+"/"+intent, func(t *testing.T) {
				fixture := backend.open(t)
				ctx := testAuthorActivityContext()
				ctx = originalStoreFixtureContext(t, ctx, fixture.store)
				rooted, _ := sourceartifactfixture.Rooted(t, "fixture", sourceartifactfixture.Artifact())
				sourceartifactfixture.RequireArtifact(t, ctx, fixture.store.(sourceartifactfixture.Writer), rooted.Artifact())
				ctx = runtimecorrelation.WithRootAdmission(ctx, rooted.Admission())
				at := time.Date(2026, 7, 14, 10, 30, 0, 0, time.UTC)
				runID := uuid.NewString()
				root := eventtest.RunCreatingRootIngress(uuid.NewString(), "test.trigger", "ingress", "", json.RawMessage(`{}`), 0, runID, "", events.EventEnvelope{}, at)
				if err := commitSemanticEventFixture(ctx, fixture.store, root); err != nil {
					t.Fatalf("commit root: %v", err)
				}
				parentID := ""
				if intent == "causal" {
					parentID = root.ID()
				}
				event := eventtest.RuntimeControl(uuid.NewString(), "platform.paused", "runtime", "", json.RawMessage(`{}`), 0, runID, parentID, events.EventEnvelope{}, at.Add(time.Second))
				eventBus, err := newRunConvergenceEventBus(t, ctx, fixture.store)
				if err != nil {
					t.Fatalf("NewEventBus: %v", err)
				}
				if err := eventBus.Publish(ctx, event); err != nil {
					t.Fatalf("Publish: %v", err)
				}
				status, gotRunID, triggerID, triggerType := loadRunConvergenceFacts(t, fixture, ctx, event.ID())
				if status != "running" || gotRunID != runID || triggerID != root.ID() || triggerType != string(root.Type()) {
					t.Fatalf("existing-run convergence = status:%q run:%q trigger:%q/%q", status, gotRunID, triggerID, triggerType)
				}
			})
		}
	}
}

func TestConcurrentTerminalReceiptsConvergeAdmittedStandaloneRuntimeRun(t *testing.T) {
	fixture := openPostgresAuthorActivityReceiptFixture(t)
	pg := fixture.store.(*PostgresStore)
	ctx := testAuthorActivityContext()
	ctx = originalStoreFixtureContext(t, ctx, fixture.store)
	rooted, _ := sourceartifactfixture.Rooted(t, "fixture", sourceartifactfixture.Artifact())
	sourceartifactfixture.RequireArtifact(t, ctx, pg, rooted.Artifact())
	ctx = runtimecorrelation.WithRootAdmission(ctx, rooted.Admission())
	at := time.Date(2026, 7, 19, 21, 0, 0, 0, time.UTC)
	runID := uuid.NewString()
	root := eventtest.RunCreatingRootIngress(uuid.NewString(), "test.trigger", "ingress", "", json.RawMessage(`{}`), 0, runID, "", events.EventEnvelope{}, at)
	if err := commitSemanticEventFixture(ctx, pg, root); err != nil {
		t.Fatalf("commit admitted run root: %v", err)
	}
	candidate := eventtest.RuntimeControl(
		uuid.NewString(), "platform.paused", "runtime", "", json.RawMessage(`{}`), 0,
		runID, "", events.EventEnvelope{}, at.Add(time.Second),
	)
	admitted, err := events.AdmitForPublish(candidate, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		t.Fatalf("admit standalone runtime event: %v", err)
	}
	agents := []string{"standalone-agent-a", "standalone-agent-b"}
	for _, agentID := range agents {
		seedStandaloneConvergenceAgent(t, pg, ctx, runtimebustest.IdentityForRun(t, runID, agentID, ""))
	}
	routes := []events.DeliveryRoute{
		testAgentDeliveryRoute(t, admitted.Event().RunID(), agents[0], ""),
		testAgentDeliveryRoute(t, admitted.Event().RunID(), agents[1], ""),
	}
	outcome, err := commitAdmittedSemanticEventFixtureOutcome(ctx, pg, admitted, routes, runtimepipelineobligation.ScopeSubscribed)
	if err != nil || outcome != runtimebus.EventAppendInserted {
		t.Fatalf("commit standalone runtime event: outcome=%v err=%v", outcome, err)
	}
	event := admitted.Event()
	status, runID, triggerID, triggerType := loadRunConvergenceFacts(t, fixture, ctx, event.ID())
	if status != "running" || runID != event.RunID() || triggerID != root.ID() || triggerType != string(root.Type()) {
		t.Fatalf("run-scoped routed authority = status:%q run:%q trigger:%q/%q", status, runID, triggerID, triggerType)
	}
	claims := make([]runtimedelivery.Claim, 0, len(routes))
	for _, route := range routes {
		claimed, err := claimDeliveryFixture(ctx, pg, event, route)
		if err != nil {
			t.Fatalf("ClaimAgentDelivery(%s): %v", route.Recipient.ID(), err)
		}
		claims = append(claims, claimed.Claim)
	}

	if _, err := pg.backend.ExecContext(ctx, `
		CREATE OR REPLACE FUNCTION slow_receipt_delivery_sync()
		RETURNS trigger
		LANGUAGE plpgsql
		AS $$
		BEGIN
			PERFORM pg_sleep(0.2);
			RETURN NEW;
		END;
		$$;
	`); err != nil {
		t.Fatalf("create slow trigger function: %v", err)
	}
	if _, err := pg.backend.ExecContext(ctx, `
		CREATE TRIGGER event_deliveries_slow_terminal_sync
		BEFORE UPDATE ON event_deliveries
		FOR EACH ROW
		EXECUTE FUNCTION slow_receipt_delivery_sync()
	`); err != nil {
		t.Fatalf("create slow trigger: %v", err)
	}

	errCh := make(chan error, len(agents))
	for _, claim := range claims {
		claim := claim
		go func() {
			_, err := pg.SettleSuccess(ctx, claim, nil, 0, runtimedelivery.NotApplicableHandlerRuleSelection())
			errCh <- err
		}()
	}
	for i := range agents {
		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("concurrent processed receipt #%d: %v", i+1, err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for concurrent processed receipts")
		}
	}
	status, _, _, _ = loadRunConvergenceFacts(t, fixture, ctx, event.ID())
	if status != "running" {
		t.Fatalf("settled receipts changed live landing run status = %q", status)
	}
	var pending, delivered int
	if err := pg.backend.QueryRowContext(ctx, `
		SELECT COUNT(*) FILTER (WHERE status IN ('pending', 'in_progress')),
		       COUNT(*) FILTER (WHERE status = 'delivered')
		FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'agent'
	`, event.ID()).Scan(&pending, &delivered); err != nil {
		t.Fatalf("load terminal delivery counts: %v", err)
	}
	if pending != 0 || delivered != len(agents) {
		t.Fatalf("delivery counts = pending:%d delivered:%d", pending, delivered)
	}
}

func newRunConvergenceEventBus(t *testing.T, ctx context.Context, selected any) (*runtimebus.EventBus, error) {
	t.Helper()
	ctx = originalStoreFixtureContext(t, ctx, selected)
	options := runtimebus.EventBusOptions{MutationContext: func(work context.Context) (context.Context, error) {
		return startupownership.FreezeMutationAuthority(startupownership.CarryMutationAuthority(work, ctx))
	}}
	var bus *runtimebus.EventBus
	var err error
	switch store := selected.(type) {
	case *PostgresStore:
		bus, err = newStoreTestEventBus(t, store, options)
	case *SQLiteRuntimeStore:
		bus, err = newStoreTestEventBus(t, store, options)
	default:
		return nil, fmt.Errorf("unsupported run convergence store %T", selected)
	}
	if err != nil {
		return nil, err
	}
	bus.SetCommittedAgentReadinessFinalizer(runtimebus.CommittedAgentReadinessFinalizerFunc(func(_ context.Context, event events.Event, routes []events.DeliveryRoute) error {
		for _, route := range routes {
			if route.Recipient.IsAgent() && route.AgentIdentity.RunID != event.RunID() {
				return fmt.Errorf("committed route run %s disagrees with event run %s", route.AgentIdentity.RunID, event.RunID())
			}
		}
		return nil
	}))
	return bus, nil
}

func loadRunConvergenceFacts(t *testing.T, fixture authorActivityReceiptFixture, ctx context.Context, eventID string) (status, runID, triggerID, triggerType string) {
	t.Helper()
	query := `
		SELECT COALESCE(r.status, ''), COALESCE(CAST(e.run_id AS TEXT), ''),
		       COALESCE(CAST(r.trigger_event_id AS TEXT), ''), COALESCE(r.trigger_event_type, '')
		FROM events e JOIN runs r ON r.run_id = e.run_id
		WHERE e.event_id = ?
	`
	if fixture.dialect == "postgres" {
		query = `
			SELECT COALESCE(r.status, ''), COALESCE(e.run_id::text, ''),
			       COALESCE(r.trigger_event_id::text, ''), COALESCE(r.trigger_event_type, '')
			FROM events e JOIN runs r ON r.run_id = e.run_id
			WHERE e.event_id = $1::uuid
		`
	}
	err := fixture.db.QueryRowContext(ctx, query, eventID).Scan(&status, &runID, &triggerID, &triggerType)
	if err != nil {
		if err == sql.ErrNoRows {
			t.Fatalf("event %s has no run convergence facts", eventID)
		}
		t.Fatalf("load run convergence facts: %v", err)
	}
	return status, runID, triggerID, triggerType
}
