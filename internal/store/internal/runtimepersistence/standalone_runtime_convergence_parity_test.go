package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	agentfixture "github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/google/uuid"
)

func TestStandaloneRuntimeManifestationsConvergeThroughEventBusParity(t *testing.T) {
	tests := standaloneRuntimeManifestations(t)
	for _, backend := range []struct {
		name string
		open func(*testing.T) authorActivityReceiptFixture
	}{
		{name: "sqlite", open: openSQLiteAuthorActivityReceiptFixture},
		{name: "postgres", open: openPostgresAuthorActivityReceiptFixture},
	} {
		for _, routed := range []bool{false, true} {
			backend, routed := backend, routed
			mode := "no_recipient"
			if routed {
				mode = "routed_final_receipt"
			}
			t.Run(backend.name+"/"+mode, func(t *testing.T) {
				fixture := backend.open(t)
				ctx := testAuthorActivityContext()
				eventBus, err := newRunConvergenceEventBus(t, fixture.store)
				if err != nil {
					t.Fatalf("NewEventBus: %v", err)
				}
				agentID := "standalone-convergence-agent"
				agentIdentity := runtimebustest.Identity(t, agentID, "")
				if routed {
					seedStandaloneConvergenceAgent(t, fixture.store, ctx, agentIdentity)
				}
				for index, test := range tests {
					test := test
					t.Run(test.name, func(t *testing.T) {
						event := test.make(uuid.NewString(), time.Date(2026, 7, 14, 10, index, 0, 0, time.UTC))
						var delivery <-chan *runtimebus.LocalDelivery
						if routed {
							delivery = runtimebustest.Subscribe(t, eventBus, agentID, event.Type())
							if delivery == nil {
								t.Fatal("Subscribe returned nil")
							}
							defer runtimebustest.Unsubscribe(eventBus, agentID)
						}
						if err := eventBus.Publish(ctx, event); err != nil {
							t.Fatalf("Publish: %v", err)
						}
						if routed {
							select {
							case delivered := <-delivery:
								got := delivered.Event()
								_ = delivered.Complete()
								if got.ID() != event.ID() {
									t.Fatalf("delivered event = %s, want %s", got.ID(), event.ID())
								}
								event = got
							case <-time.After(5 * time.Second):
								t.Fatal("timed out waiting for routed standalone event")
							}
							status, _, _, _ := loadRunConvergenceFacts(t, fixture, ctx, event.ID())
							if status != "running" || countRunConvergenceDeliveries(t, fixture, ctx, event.ID()) != 1 {
								t.Fatalf("pre-receipt state = run:%q deliveries:%d, want running/1", status, countRunConvergenceDeliveries(t, fixture, ctx, event.ID()))
							}
							route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(agentID), AgentIdentity: agentIdentity}
							claimed, err := claimDeliveryFixture(ctx, fixture.store, event, route)
							if err != nil {
								t.Fatalf("ClaimAgentDelivery: %v", err)
							}
							if _, err := fixture.store.SettleSuccess(ctx, claimed.Claim, nil, 0, runtimedelivery.NotApplicableHandlerRuleSelection()); err != nil {
								t.Fatalf("SettleSuccess: %v", err)
							}
						}
						_, persistedRunID, _, _ := loadRunConvergenceFacts(t, fixture, ctx, event.ID())
						executeStandaloneCompletionCandidate(t, ctx, fixture.store, persistedRunID)
						status, runID, triggerID, triggerType := loadRunConvergenceFacts(t, fixture, ctx, event.ID())
						if status != "completed" || runID == "" || triggerID != event.ID() || triggerType != string(event.Type()) {
							t.Fatalf("standalone convergence = status:%q run:%q trigger:%q/%q", status, runID, triggerID, triggerType)
						}
						wantDeliveries := 0
						if routed {
							wantDeliveries = 1
						}
						if got := countRunConvergenceDeliveries(t, fixture, ctx, event.ID()); got != wantDeliveries {
							t.Fatalf("delivery count = %d, want %d", got, wantDeliveries)
						}
					})
				}
			})
		}
	}
}

func standaloneRuntimeManifestations(t *testing.T) []struct {
	name string
	make func(string, time.Time) events.Event
} {
	t.Helper()
	control := func(eventType events.EventType) func(string, time.Time) events.Event {
		return func(eventID string, at time.Time) events.Event {
			return eventtest.RuntimeControl(eventID, eventType, "runtime", "", json.RawMessage(`{}`), 0, "", "", events.EventEnvelope{}, at)
		}
	}
	diagnostic := func(eventType events.EventType) func(string, time.Time) events.Event {
		return func(eventID string, at time.Time) events.Event {
			return eventtest.RuntimeDiagnostic(eventID, eventType, "runtime", "", standaloneRuntimeManifestationPayload(t, eventType), 0, "", "", events.EventEnvelope{}, at)
		}
	}
	return []struct {
		name string
		make func(string, time.Time) events.Event
	}{
		{name: "boot", make: control("platform.boot")},
		{name: "recovery_failed", make: diagnostic("platform.recovery_failed")},
		{name: "reset", make: control("platform.reset")},
		{name: "paused", make: control("platform.paused")},
		{name: "resumed", make: control("platform.resumed")},
		{name: "budget_threshold", make: diagnostic("platform.budget_threshold_crossed")},
		{name: "agent_panic", make: diagnostic("platform.agent_panic")},
		{name: "agent_failed", make: diagnostic("platform.agent_failed")},
		{name: "agent_started", make: diagnostic("platform.agent_started")},
	}
}

func standaloneRuntimeManifestationPayload(t *testing.T, eventType events.EventType) json.RawMessage {
	t.Helper()
	payload := map[string]any{}
	switch eventType {
	case "platform.recovery_failed", "platform.agent_panic", "platform.agent_failed":
		failure := runtimefailures.Normalize(runtimefailures.New(
			runtimefailures.ClassInternalFailure,
			"unclassified_runtime_error",
			"standalone-convergence-test",
			"publish_platform_signal",
			nil,
		), "standalone-convergence-test", "publish_platform_signal")
		payload["failure"] = &failure
	case "platform.budget_threshold_crossed":
		payload["level"] = "warning"
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal %s payload: %v", eventType, err)
	}
	return raw
}

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
				eventBus, err := newRunConvergenceEventBus(t, fixture.store)
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
	candidate := eventtest.RuntimeControl(
		uuid.NewString(), "platform.paused", "runtime", "", json.RawMessage(`{}`), 0,
		"", "", events.EventEnvelope{}, time.Date(2026, 7, 19, 21, 0, 0, 0, time.UTC),
	)
	admitted, err := events.AdmitForPublish(candidate, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		t.Fatalf("admit standalone runtime event: %v", err)
	}
	agents := []string{"standalone-agent-a", "standalone-agent-b"}
	routes := []events.DeliveryRoute{
		testAgentDeliveryRoute(t, agents[0], ""),
		testAgentDeliveryRoute(t, agents[1], ""),
	}
	outcome, err := commitAdmittedSemanticEventFixtureOutcome(ctx, pg, admitted, routes, runtimepipelineobligation.ScopeSubscribed)
	if err != nil || outcome != runtimebus.EventAppendInserted {
		t.Fatalf("commit standalone runtime event: outcome=%v err=%v", outcome, err)
	}
	event := admitted.Event()
	status, runID, triggerID, triggerType := loadRunConvergenceFacts(t, fixture, ctx, event.ID())
	if status != "running" || runID != event.RunID() || triggerID != event.ID() || triggerType != string(event.Type()) {
		t.Fatalf("standalone routed authority = status:%q run:%q trigger:%q/%q", status, runID, triggerID, triggerType)
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
	executeStandaloneCompletionCandidate(t, ctx, pg, event.RunID())
	status, _, _, _ = loadRunConvergenceFacts(t, fixture, ctx, event.ID())
	if status != "completed" {
		t.Fatalf("run status = %q, want completed", status)
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

func executeStandaloneCompletionCandidate(t *testing.T, ctx context.Context, selected any, runID string) {
	executeStandaloneCompletionCandidateWithCatalog(t, ctx, selected, runID, runtimerunlifecycle.TerminalCatalog{})
}

func executeStandaloneCompletionCandidateWithCatalog(t *testing.T, ctx context.Context, selected any, runID string, catalog runtimerunlifecycle.TerminalCatalog) {
	t.Helper()
	owner, ok := selected.(runtimerunlifecycle.CandidateStore)
	if !ok {
		t.Fatalf("standalone completion store %T does not expose typed candidates", selected)
	}
	page, err := owner.ListCompletionCandidates(
		ctx,
		runtimerunlifecycle.CandidateScope{BundleHash: authorActivityTestBundleHash},
		runtimerunlifecycle.CandidateCursor{},
		128,
	)
	if err != nil {
		t.Fatalf("ListCompletionCandidates: %v", err)
	}
	for _, candidate := range page.Candidates {
		if candidate.RunID != runID {
			continue
		}
		result, err := owner.ExecuteCompletionCandidate(ctx, candidate, catalog)
		if err != nil {
			t.Fatalf("ExecuteCompletionCandidate: %v", err)
		}
		if result.Outcome != runtimerunlifecycle.OutcomeTerminallyEligible {
			t.Fatalf("completion result = %#v retryable=%v, want outcome %s", result, result.Retryable, runtimerunlifecycle.OutcomeTerminallyEligible)
		}
		return
	}
	t.Fatalf("completion candidate for run %s was not persisted", runID)
}

func newRunConvergenceEventBus(t *testing.T, selected any) (*runtimebus.EventBus, error) {
	t.Helper()
	switch store := selected.(type) {
	case *PostgresStore:
		return newStoreTestEventBus(t, store)
	case *SQLiteRuntimeStore:
		return newStoreTestEventBus(t, store)
	default:
		return nil, fmt.Errorf("unsupported run convergence store %T", selected)
	}
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

func countRunConvergenceDeliveries(t *testing.T, fixture authorActivityReceiptFixture, ctx context.Context, eventID string) int {
	t.Helper()
	query := `SELECT COUNT(*) FROM event_deliveries WHERE event_id = ? AND subscriber_type = 'agent'`
	if fixture.dialect == "postgres" {
		query = `SELECT COUNT(*) FROM event_deliveries WHERE event_id = $1::uuid AND subscriber_type = 'agent'`
	}
	var count int
	if err := fixture.db.QueryRowContext(ctx, query, eventID).Scan(&count); err != nil {
		t.Fatalf("count event deliveries: %v", err)
	}
	return count
}
