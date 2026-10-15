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

func TestExactJoinDeliveryObserversKeepPhysicalTextCountAndNodeScopeBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, state := newCompetingRevisionPairFixture(t, backend)
			node := mustPersistenceNode("review", "progress")
			event := eventtest.ExistingRunRootIngress(uuid.NewString(), "review/finish.requested", "fixture", "", []byte(`{}`), 0, state.Identity.RunID, events.EventEnvelope{}, time.Now().UTC())
			route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "review", FlowInstance: state.Identity.Route.InstancePath, EntityID: state.EntityID})}
			owner := f.store.(deliveryFixtureStore)
			if err := commitSemanticEventFixtureWithRoutes(f.ctx, owner, event, []events.DeliveryRoute{route}); err != nil {
				t.Fatal(err)
			}
			if got, err := ReadExactJoinNodeDeliveryStorageForTest(f.ctx, f.store, event.ID(), node.Key()); err != nil || got.Status != "pending" || got.Failure != "{}" {
				t.Fatalf("pending join witness lost physical NULL/default: %+v %v", got, err)
			}
			claimed, err := claimDeliveryFixture(f.ctx, owner, event, route)
			if err != nil {
				t.Fatal(err)
			}
			failure := testRetryableFailure()
			if _, err := owner.SettleFailure(f.ctx, claimed.Claim, deliverylifecycle.Settlement{Disposition: deliverylifecycle.FailureDeadLetter, ReasonCode: failure.Detail.Code, Failure: failure, RuleSelection: handlerselection.NotReached()}); err != nil {
				t.Fatal(err)
			}
			var want ExactJoinNodeDeliveryStorage
			if err := f.db.QueryRowContext(f.ctx, `SELECT status, CAST(COALESCE(failure, '{}') AS TEXT) FROM event_deliveries WHERE event_id=$1 AND subscriber_type='node' AND subscriber_id=$2`, event.ID(), node.Key()).Scan(&want.Status, &want.Failure); err != nil || want.Status != "dead_letter" || want.Failure == "{}" {
				t.Fatalf("actual join settlement lacks physical positive failure evidence: %+v %v", want, err)
			}
			before := competingRevisionPairSnapshot(t, f)
			probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			if got, err := ReadExactJoinNodeDeliveryStorageForTest(f.ctx, f.store, event.ID(), node.Key()); err != nil || got != want {
				t.Fatalf("join observation changed status/physical failure text: %+v %v", got, err)
			}
			if got, err := CountTemplateNodeDeliveryForTest(f.ctx, f.store, event.ID(), node.Key()); err != nil || got != 1 {
				t.Fatalf("join count inferred delivery eligibility instead of storage: %d %v", got, err)
			}
			if counts := probe.Snapshot(); counts.Active != 0 || counts.Total.Begun != 2 || counts.Total.ReadCommits != 2 || counts.Total.WriteCommits != 0 {
				t.Fatalf("join observations changed independent selected read cuts: %+v", counts)
			}
			for _, scope := range [][2]string{{uuid.NewString(), node.Key()}, {event.ID(), "foreign-node"}} {
				assertExactJoinObserversAbsent(t, f.ctx, f.store, scope[0], scope[1])
			}
			if !reflect.DeepEqual(before, competingRevisionPairSnapshot(t, f)) {
				t.Fatal("join reads changed complete data/history/lease/clocks")
			}
			restore()
			identity := mustTestAgentIdentityForRun(state.Identity.RunID, "join-observer-agent", "join-observer/agent")
			agentEvent := eventtest.ExistingRunRootIngress(uuid.NewString(), "review/finish.requested", "fixture", "", []byte(`{}`), 0, state.Identity.RunID, events.EventEnvelope{}, time.Now().UTC())
			agentRoute := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(identity.AgentID()), AgentIdentity: identity}
			if err := commitSemanticEventFixtureWithRoutes(f.ctx, owner, agentEvent, []events.DeliveryRoute{agentRoute}); err != nil {
				t.Fatal(err)
			}
			assertExactJoinObserversAbsent(t, f.ctx, f.store, agentEvent.ID(), identity.AgentID())
		})
	}
}

func assertExactJoinObserversAbsent(t *testing.T, ctx context.Context, selected any, eventID, subscriberKey string) {
	t.Helper()
	if got, err := ReadExactJoinNodeDeliveryStorageForTest(ctx, selected, eventID, subscriberKey); !errors.Is(err, sql.ErrNoRows) || got != (ExactJoinNodeDeliveryStorage{}) {
		t.Fatalf("foreign/non-node identity borrowed join status/failure: %+v %v", got, err)
	}
	if got, err := CountTemplateNodeDeliveryForTest(ctx, selected, eventID, subscriberKey); err != nil || got != 0 {
		t.Fatalf("foreign/non-node identity borrowed join count: %d %v", got, err)
	}
}

func assertExactJoinObserversFailure(t *testing.T, ctx context.Context, selected any, cause error) {
	t.Helper()
	failed := func(err error) bool { return err != nil && (cause == nil || errors.Is(err, cause)) }
	id := uuid.NewString()
	if got, err := ReadExactJoinNodeDeliveryStorageForTest(ctx, selected, id, "join-node"); !failed(err) || got != (ExactJoinNodeDeliveryStorage{}) {
		t.Fatalf("failed owner manufactured join status/failure: %+v %v", got, err)
	}
	if got, err := CountTemplateNodeDeliveryForTest(ctx, selected, id, "join-node"); !failed(err) || got != 0 {
		t.Fatalf("failed owner manufactured successful zero join count: %d %v", got, err)
	}
}

func TestExactJoinDeliveryObserversRefuseCancelledRawAndClosedOwnersBothStores(t *testing.T) {
	for _, selected := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		assertExactJoinObserversFailure(t, context.Background(), selected, nil)
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, _ := newCompetingRevisionPairFixture(t, backend)
			ctx, cancel := context.WithCancel(f.ctx)
			cancel()
			assertExactJoinObserversFailure(t, ctx, f.store, context.Canceled)
			if err := f.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			assertExactJoinObserversFailure(t, f.ctx, f.store, nil)
		})
	}
}
