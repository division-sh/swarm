package storetest

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func servedSettlementOwners() []struct {
	name string
	open func(*testing.T) RunFixtureStore
} {
	return []struct {
		name string
		open func(*testing.T) RunFixtureStore
	}{
		{"sqlite", func(t *testing.T) RunFixtureStore { return StartSQLiteRuntimeStore(t) }},
		{"postgres", func(t *testing.T) RunFixtureStore { return StartPostgresRuntimeStore(t) }},
	}
}

func TestServedSettlementCountsPreserveExactRunNodeSubscriberAndReplayScopesBothStores(t *testing.T) {
	for _, backend := range servedSettlementOwners() {
		t.Run(backend.name, func(t *testing.T) {
			owner := backend.open(t)
			ctx := semanticFixtureContext(context.Background(), sourceartifactfixture.Fact())
			run, otherRun := uuid.NewString(), uuid.NewString()
			for _, id := range []string{run, otherRun} {
				RequireRun(t, ctx, owner, RunFixture{Origin: ScenarioSetupOrigin(), RunID: id})
			}
			node := identitytest.RootNode(t, "served-count")
			makeEvent := func(run string) events.Event {
				t.Helper()
				event := eventtest.ExistingRunRootIngress(uuid.NewString(), "fixture.served", "fixture", "", []byte(`{}`), 0, run, events.EventEnvelope{}, time.Now().UTC())
				CommitSemanticEventWithInitialFacts(t, ctx, owner, event,
					[]events.DeliveryRoute{{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: "fixture", FlowInstance: "fixture/served-count"})}},
					pipelineobligation.ScopeSubscribed, AcknowledgedPipelineDisposition())
				return event
			}
			event, other := makeEvent(run), makeEvent(otherRun)
			probe := CollectTransactions(t, owner, TransactionProbeOptions{})
			if count, err := CountExactRunNodeEventDeliveryStorage(ctx, owner, run, event.ID(), node.Key()); err != nil || count != 1 {
				t.Fatalf("exact physical node count=%d error=%v", count, err)
			}
			if count, err := CountSubscriberEventReceiptOutcomeStorage(ctx, owner, event.ID(), "platform", "pipeline", "success"); err != nil || count != 1 {
				t.Fatalf("exact physical receipt count=%d error=%v", count, err)
			}
			if count, err := CountExactRunEventReplayScopeStorage(ctx, owner, run, event.ID(), string(pipelineobligation.ScopeSubscribed)); err != nil || count != 1 {
				t.Fatalf("exact physical replay count=%d error=%v", count, err)
			}
			if got := probe.Snapshot(); got.Active != 0 || got.Total.ReadCommits != 3 || got.Total.WriteCommits != 0 {
				t.Fatalf("independent observations escaped original native read coordinator: %+v", got)
			}
			assertServedSettlementCountScopes(t, ctx, owner, run, otherRun, event.ID(), other.ID(), node.Key())
		})
	}
}

func assertServedSettlementCountScopes(t *testing.T, ctx context.Context, owner any, run, otherRun, event, otherEvent, node string) {
	t.Helper()
	for _, scope := range [][3]string{{otherRun, event, node}, {run, otherEvent, node}, {run, event, "foreign-node"}} {
		if count, err := CountExactRunNodeEventDeliveryStorage(ctx, owner, scope[0], scope[1], scope[2]); err != nil || count != 0 {
			t.Fatalf("node witness borrowed foreign run/event/node: scope=%v count=%d error=%v", scope, count, err)
		}
	}
	for _, scope := range [][3]string{{"node", "pipeline", "success"}, {"platform", "foreign-subscriber", "success"}, {"platform", "pipeline", "failure"}} {
		if count, err := CountSubscriberEventReceiptOutcomeStorage(ctx, owner, event, scope[0], scope[1], scope[2]); err != nil || count != 0 {
			t.Fatalf("receipt witness borrowed subscriber/outcome: %v count=%d error=%v", scope, count, err)
		}
	}
	for _, scope := range [][3]string{{otherRun, event, string(pipelineobligation.ScopeSubscribed)}, {run, otherEvent, string(pipelineobligation.ScopeSubscribed)}, {run, event, "foreign-scope"}} {
		if count, err := CountExactRunEventReplayScopeStorage(ctx, owner, scope[0], scope[1], scope[2]); err != nil || count != 0 {
			t.Fatalf("replay witness borrowed run/event/scope: %v count=%d error=%v", scope, count, err)
		}
	}
}

func TestServedCurrentDeliveryOutcomeCountRejectsHistoricalAttemptBorrowingBothStores(t *testing.T) {
	for _, backend := range servedSettlementOwners() {
		t.Run(backend.name, func(t *testing.T) {
			owner := backend.open(t)
			ctx := semanticFixtureContext(context.Background(), sourceartifactfixture.Fact())
			run := uuid.NewString()
			RequireRun(t, ctx, owner, RunFixture{Origin: ScenarioSetupOrigin(), RunID: run})
			node := identitytest.RootNode(t, "served-count")
			event := eventtest.ExistingRunRootIngress(uuid.NewString(), "fixture.served", "fixture", "", []byte(`{}`), 0, run, events.EventEnvelope{}, time.Now().UTC())
			route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: "fixture", FlowInstance: "fixture/served-count"})}
			CommitSemanticEventWithRoutes(t, ctx, owner, event, []events.DeliveryRoute{route}, pipelineobligation.ScopeSubscribed)
			actual := owner.(deliverylifecycle.Store)
			first, err := ClaimDelivery(ctx, actual, event, route)
			if err != nil {
				t.Fatal(err)
			}
			failure := failures.Normalize(failures.New(failures.ClassConnectorFailure, "fixture_retry", "served-observer", "retry", nil), "served-observer", "retry")
			if _, err := actual.SettleFailure(ctx, first.Claim, deliverylifecycle.Settlement{Disposition: deliverylifecycle.FailureRetry, Failure: &failure, RetryBase: time.Nanosecond, RuleSelection: handlerselection.NotReached()}); err != nil {
				t.Fatal(err)
			}
			assertServedCurrentOutcomeCount(t, ctx, owner, event.ID(), "node", node.Key(), "retry_scheduled", 1)
			second, err := ClaimDelivery(ctx, actual, event, route)
			if err != nil || second.Claim.Version() != first.Claim.Version()+1 {
				t.Fatalf("genuine successor claim=%v error=%v", second, err)
			}
			assertServedCurrentOutcomeCount(t, ctx, owner, event.ID(), "node", node.Key(), "retry_scheduled", 0)
			assertServedCurrentOutcomeCount(t, ctx, owner, event.ID(), "node", node.Key(), "delivered", 0)
			if _, err := actual.SettleSuccess(ctx, second.Claim, nil, 0, handlerselection.NotApplicable()); err != nil {
				t.Fatal(err)
			}
			assertServedCurrentOutcomeCount(t, ctx, owner, event.ID(), "node", node.Key(), "delivered", 1)
			assertServedCurrentOutcomeCount(t, ctx, owner, event.ID(), "agent", node.Key(), "delivered", 0)
			assertServedCurrentOutcomeCount(t, ctx, owner, event.ID(), "node", "foreign-node", "delivered", 0)
			assertServedCurrentOutcomeCount(t, ctx, owner, uuid.NewString(), "node", node.Key(), "delivered", 0)
		})
	}
}

func assertServedCurrentOutcomeCount(t *testing.T, ctx context.Context, owner any, event, subscriberType, subscriber, outcome string, want int) {
	t.Helper()
	if count, err := CountCurrentEventDeliveryOutcomeStorage(ctx, owner, event, subscriberType, subscriber, outcome); err != nil || count != want {
		t.Fatalf("current attempt outcome %s/%s/%s=%d error=%v want=%d", subscriberType, subscriber, outcome, count, err, want)
	}
}

func TestServedSettlementCountsRefuseForeignCancelledAndClosedOwnershipBothStores(t *testing.T) {
	id := uuid.NewString()
	reads := []func(context.Context, any) (int, error){
		func(ctx context.Context, owner any) (int, error) {
			return CountExactRunNodeEventDeliveryStorage(ctx, owner, id, id, "node")
		},
		func(ctx context.Context, owner any) (int, error) {
			return CountCurrentEventDeliveryOutcomeStorage(ctx, owner, id, "node", "node", "delivered")
		},
		func(ctx context.Context, owner any) (int, error) {
			return CountSubscriberEventReceiptOutcomeStorage(ctx, owner, id, "platform", "pipeline", "success")
		},
		func(ctx context.Context, owner any) (int, error) {
			return CountExactRunEventReplayScopeStorage(ctx, owner, id, id, "subscribed")
		},
	}
	for _, backend := range servedSettlementOwners() {
		t.Run(backend.name, func(t *testing.T) {
			owner := backend.open(t)
			assertServedSettlementPortRefusals(t, owner, reads)
			if err := owner.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			for _, read := range reads {
				if count, err := read(context.Background(), owner); err == nil || count != 0 {
					t.Fatalf("closed owner manufactured evidence=%d error=%v", count, err)
				}
			}
		})
	}
}

func assertServedSettlementPortRefusals(t *testing.T, owner any, reads []func(context.Context, any) (int, error)) {
	t.Helper()
	for _, read := range reads {
		if count, err := read(context.Background(), owner); err != nil || count != 0 {
			t.Fatalf("empty native count=%d error=%v", count, err)
		}
		for _, foreign := range []any{nil, &sql.DB{}, &sql.Tx{}, struct{}{}} {
			if count, err := read(context.Background(), foreign); err == nil || count != 0 {
				t.Fatalf("foreign %T manufactured evidence=%d error=%v", foreign, count, err)
			}
		}
		cancelled, cancel := context.WithCancel(context.Background())
		cancel()
		if count, err := read(cancelled, owner); !errors.Is(err, context.Canceled) || count != 0 {
			t.Fatalf("cancelled count=%d error=%v", count, err)
		}
	}
}
