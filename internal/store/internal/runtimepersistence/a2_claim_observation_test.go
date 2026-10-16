package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestA2ClaimObserversKeepExactNodeIdentityAndSettledAggregateBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, state := newCompetingRevisionPairFixture(t, backend)
			f.ctx = preparePositiveAgentClaimFixture(t, f.ctx, f.store.(agentFixtureFlowStore),
				mustTestAgentIdentityForRun(state.Identity.RunID, "a2-observer-agent", "a2-observer/agent"),
				mustTestAgentIdentityForRun(state.Identity.RunID, "a2-delivered-agent", "a2-delivered/agent"))
			node := mustPersistenceNode("review", "progress")
			event := eventtest.ExistingRunRootIngress(uuid.NewString(), "review/finish.requested", "fixture", "", []byte(`{}`), 0, state.Identity.RunID, events.EventEnvelope{}, time.Now().UTC())
			route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "review", FlowInstance: state.Identity.Route.InstancePath, EntityID: state.EntityID})}
			owner := f.store.(deliveryFixtureStore)
			if err := commitSemanticEventFixtureWithRoutes(f.ctx, owner, event, []events.DeliveryRoute{route}); err != nil {
				t.Fatal(err)
			}
			initial, err := ReadA2EarlyRefusalForTest(f.ctx, f.store, event.ID(), node.Key())
			if err != nil || initial.Status != "pending" || initial.Failure != "{}" || initial.SettledAttempts != 0 || initial.Outcome != "" {
				t.Fatalf("pending delivery lost LEFT JOIN/NULL coalescence: %+v %v", initial, err)
			}
			claimed, err := claimDeliveryFixture(f.ctx, owner, event, route)
			if err != nil {
				t.Fatal(err)
			}
			if n, err := CountGateNodeSettledAttemptsForTest(f.ctx, f.store, event.ID(), node.Key()); err != nil || n != 0 {
				t.Fatalf("live attempt was misclassified as settled: %d %v", n, err)
			}
			failure := testRetryableFailure()
			if _, err := owner.SettleFailure(f.ctx, claimed.Claim, deliverylifecycle.Settlement{Disposition: deliverylifecycle.FailureDeadLetter, ReasonCode: failure.Detail.Code, Failure: failure, RuleSelection: handlerselection.NotReached()}); err != nil {
				t.Fatal(err)
			}
			var want A2EarlyRefusalStorage
			if err := f.db.QueryRowContext(f.ctx, `SELECT d.status, CAST(COALESCE(d.failure, '{}') AS TEXT), COUNT(a.delivery_id), COALESCE(MAX(a.outcome), '')
FROM event_deliveries d LEFT JOIN event_delivery_attempts a ON a.delivery_id=d.delivery_id AND a.closure_kind='settled'
WHERE d.event_id=$1 AND d.subscriber_type='node' AND d.subscriber_id=$2 GROUP BY d.status, d.failure`, event.ID(), node.Key()).
				Scan(&want.Status, &want.Failure, &want.SettledAttempts, &want.Outcome); err != nil || want.SettledAttempts != 1 {
				t.Fatalf("real settlement lacks positive exact physical evidence: %+v %v", want, err)
			}
			var deliveryID string
			if err := f.db.QueryRowContext(f.ctx, `SELECT delivery_id FROM event_deliveries WHERE event_id=$1 AND subscriber_type='node' AND subscriber_id=$2`, event.ID(), node.Key()).Scan(&deliveryID); err != nil || deliveryID == "" {
				t.Fatalf("real delivery has no physical identity: %q %v", deliveryID, err)
			}
			before := competingRevisionPairSnapshot(t, f)
			probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			if got, err := ReadA2EarlyRefusalForTest(f.ctx, f.store, event.ID(), node.Key()); err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("refusal changed exact physical bytes/grouping/settlement aggregate: %+v %v", got, err)
			}
			if got, err := ReadGateNodeDeliveryIdentityForTest(f.ctx, f.store, event.ID(), node.Key()); err != nil || got != deliveryID {
				t.Fatalf("claim read changed delivery identity: %q %v", got, err)
			}
			if got, err := CountGateNodeSettledAttemptsForTest(f.ctx, f.store, event.ID(), node.Key()); err != nil || got != 1 {
				t.Fatalf("settled count changed original disposition-independent evidence: %d %v", got, err)
			}
			if counts := probe.Snapshot(); counts.Active != 0 || counts.Total.Begun != 3 || counts.Total.ReadCommits != 3 || counts.Total.WriteCommits != 0 {
				t.Fatalf("claim observations merged cuts or bypassed the exact coordinator: %+v", counts)
			}
			if got, err := CountGateEventNodeDeliveriesForTest(f.ctx, f.store, event.ID()); err != nil || got != 1 {
				t.Fatalf("event-level node count changed settled failure visibility: %d %v", got, err)
			}
			if got, err := ReadGateEventNodeDeliveryIdentityForTest(f.ctx, f.store, event.ID()); err != nil || got != deliveryID {
				t.Fatalf("event-level node identity changed its physical scope: %q %v", got, err)
			}
			if got, err := CountGateEventDeliveredRowsForTest(f.ctx, f.store, event.ID()); err != nil || got != 0 {
				t.Fatalf("event delivered count borrowed a dead-letter settlement: %d %v", got, err)
			}
			if counts := probe.Snapshot(); counts.Active != 0 || counts.Total.Begun != 6 || counts.Total.ReadCommits != 6 || counts.Total.WriteCommits != 0 {
				t.Fatalf("event-level observations merged cuts or bypassed the original coordinator: %+v", counts)
			}
			for _, scope := range [][2]string{{uuid.NewString(), node.Key()}, {event.ID(), "foreign-node"}} {
				assertA2ClaimObservationAbsent(t, f.ctx, f.store, scope[0], scope[1])
			}
			cancelled, cancel := context.WithCancel(f.ctx)
			cancel()
			assertA2ClaimObservationFailure(t, cancelled, f.store, event.ID(), node.Key(), context.Canceled)
			if !reflect.DeepEqual(before, competingRevisionPairSnapshot(t, f)) {
				t.Fatal("claim observation changed application, history, lease or clocks")
			}
			restore()
			assertA2ClaimObserversExcludeActualAgent(t, f, owner, state.Identity.RunID)
			assertA2DeliveredCountIncludesActualAgent(t, f, owner, state.Identity.RunID)
		})
	}
}

func assertA2DeliveredCountIncludesActualAgent(t *testing.T, f receiverConfigActivationFixture, owner deliveryFixtureStore, runID string) {
	t.Helper()
	identity := mustTestAgentIdentityForRun(runID, "a2-delivered-agent", "a2-delivered/agent")
	event := eventtest.ExistingRunRootIngress(uuid.NewString(), "review/finish.requested", "fixture", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
	route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(identity.AgentID()), AgentIdentity: identity}
	if err := commitSemanticEventFixtureWithRoutes(f.ctx, owner, event, []events.DeliveryRoute{route}); err != nil {
		t.Fatal(err)
	}
	if got, err := CountGateEventDeliveredRowsForTest(f.ctx, f.store, event.ID()); err != nil || got != 0 {
		t.Fatalf("pending agent became a delivered witness: %d %v", got, err)
	}
	claimed, err := claimDeliveryFixture(f.ctx, owner, event, route)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := owner.SettleSuccess(f.ctx, claimed.Claim, nil, 0, handlerselection.NotApplicable()); err != nil {
		t.Fatal(err)
	}
	before := competingRevisionPairSnapshot(t, f)
	if got, err := CountGateEventDeliveredRowsForTest(f.ctx, f.store, event.ID()); err != nil || got != 1 {
		t.Fatalf("event delivered count added a node-only filter or fabricated zero: %d %v", got, err)
	}
	if got, err := CountGateEventNodeDeliveriesForTest(f.ctx, f.store, event.ID()); err != nil || got != 0 {
		t.Fatalf("node-only count borrowed actual agent delivery: %d %v", got, err)
	}
	if got, err := ReadGateEventNodeDeliveryIdentityForTest(f.ctx, f.store, event.ID()); !errors.Is(err, sql.ErrNoRows) || got != "" {
		t.Fatalf("node-only identity borrowed actual agent delivery: %q %v", got, err)
	}
	if !reflect.DeepEqual(before, competingRevisionPairSnapshot(t, f)) {
		t.Fatal("event-level delivery observations changed data, history, lease or clocks")
	}
}

func assertA2ClaimObservationAbsent(t *testing.T, ctx context.Context, owner any, event, node string) {
	t.Helper()
	if got, err := ReadA2EarlyRefusalForTest(ctx, owner, event, node); !errors.Is(err, sql.ErrNoRows) || got != (A2EarlyRefusalStorage{}) {
		t.Fatalf("absent node/event supplied refusal evidence: %+v %v", got, err)
	}
	if got, err := ReadGateNodeDeliveryIdentityForTest(ctx, owner, event, node); !errors.Is(err, sql.ErrNoRows) || got != "" {
		t.Fatalf("absent node/event supplied claim identity: %q %v", got, err)
	}
	if got, err := CountGateNodeSettledAttemptsForTest(ctx, owner, event, node); err != nil || got != 0 {
		t.Fatalf("absent node/event supplied settled attempts: %d %v", got, err)
	}
}

func assertA2ClaimObservationFailure(t *testing.T, ctx context.Context, owner any, event, node string, cause error) {
	t.Helper()
	check := func(err error) bool { return err != nil && (cause == nil || errors.Is(err, cause)) }
	if got, err := ReadA2EarlyRefusalForTest(ctx, owner, event, node); !check(err) || got != (A2EarlyRefusalStorage{}) {
		t.Fatalf("failed owner supplied refusal evidence: %+v %v", got, err)
	}
	if got, err := ReadGateNodeDeliveryIdentityForTest(ctx, owner, event, node); !check(err) || got != "" {
		t.Fatalf("failed owner supplied claim identity: %q %v", got, err)
	}
	if got, err := CountGateNodeSettledAttemptsForTest(ctx, owner, event, node); !check(err) || got != 0 {
		t.Fatalf("failed owner supplied settled attempts: %d %v", got, err)
	}
	if got, err := CountGateEventNodeDeliveriesForTest(ctx, owner, event); !check(err) || got != 0 {
		t.Fatalf("failed owner supplied event-level node count: %d %v", got, err)
	}
	if got, err := ReadGateEventNodeDeliveryIdentityForTest(ctx, owner, event); !check(err) || got != "" {
		t.Fatalf("failed owner supplied event-level node identity: %q %v", got, err)
	}
	if got, err := CountGateEventDeliveredRowsForTest(ctx, owner, event); !check(err) || got != 0 {
		t.Fatalf("failed owner supplied delivered evidence: %d %v", got, err)
	}
}

func assertA2ClaimObserversExcludeActualAgent(t *testing.T, f receiverConfigActivationFixture, owner deliveryFixtureStore, runID string) {
	t.Helper()
	identity := mustTestAgentIdentityForRun(runID, "a2-observer-agent", "a2-observer/agent")
	event := eventtest.ExistingRunRootIngress(uuid.NewString(), "review/finish.requested", "fixture", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
	route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(identity.AgentID()), AgentIdentity: identity}
	if err := commitSemanticEventFixtureWithRoutes(f.ctx, owner, event, []events.DeliveryRoute{route}); err != nil {
		t.Fatal(err)
	}
	claimed, err := claimDeliveryFixture(f.ctx, owner, event, route)
	if err != nil {
		t.Fatal(err)
	}
	failure := testRetryableFailure()
	if _, err := owner.SettleFailure(f.ctx, claimed.Claim, deliverylifecycle.Settlement{Disposition: deliverylifecycle.FailureDeadLetter, ReasonCode: failure.Detail.Code, Failure: failure, RuleSelection: deliverylifecycle.NotApplicableHandlerRuleObservation()}); err != nil {
		t.Fatal(err)
	}
	assertA2ClaimObservationAbsent(t, f.ctx, f.store, event.ID(), identity.AgentID())
}

func TestA2ClaimObserversRefuseRawAndClosedOwners(t *testing.T) {
	for _, selected := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		assertA2ClaimObservationFailure(t, context.Background(), selected, uuid.NewString(), "node", nil)
	}
	f, _ := newCompetingRevisionPairFixture(t, "sqlite")
	if err := f.store.(*SQLiteRuntimeStore).Close(); err != nil {
		t.Fatal(err)
	}
	assertA2ClaimObservationFailure(t, f.ctx, f.store, uuid.NewString(), "node", nil)
}
