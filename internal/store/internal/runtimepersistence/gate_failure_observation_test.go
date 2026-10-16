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

type gateFailureReaderCase struct {
	name string
	read func(context.Context, any, string, string) (string, error)
}

func gateFailureReaderCases() []gateFailureReaderCase {
	return []gateFailureReaderCase{
		{"event", func(ctx context.Context, selected any, event, _ string) (string, error) {
			return ReadGateEventFailureForTest(ctx, selected, event)
		}},
		{"subscriber", ReadGateSubscriberFailureForTest},
		{"node", ReadGateNodeFailureForTest},
		{"coalesced", ReadGateSubscriberFailureOrEmptyObjectForTest},
		{"dead_letter", func(ctx context.Context, selected any, event, _ string) (string, error) {
			return ReadGateDeadLetterFailureForTest(ctx, selected, event)
		}},
	}
}

func TestGateFailureReadersPreserveNullAbsenceScopesAndPhysicalBytesBothStores(t *testing.T) {
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
			for _, tc := range gateFailureReaderCases() {
				value, err := tc.read(f.ctx, f.store, event.ID(), node.Key())
				if tc.name == "coalesced" {
					if err != nil || value != "{}" {
						t.Fatalf("explicit NULL coalescence changed: %q %v", value, err)
					}
				} else if tc.name == "dead_letter" {
					if !errors.Is(err, sql.ErrNoRows) || value != "" {
						t.Fatalf("absent dead letter synthesized evidence: %q %v", value, err)
					}
				} else if err == nil || errors.Is(err, sql.ErrNoRows) || value != "" {
					t.Fatalf("%s hid the original NULL scan refusal: %q %v", tc.name, value, err)
				}
			}
			claimed, err := claimDeliveryFixture(f.ctx, owner, event, route)
			if err != nil {
				t.Fatal(err)
			}
			failure := testRetryableFailure()
			if _, err := owner.SettleFailure(f.ctx, claimed.Claim, deliverylifecycle.Settlement{Disposition: deliverylifecycle.FailureDeadLetter, ReasonCode: failure.Detail.Code, Failure: failure, RuleSelection: handlerselection.NotReached()}); err != nil {
				t.Fatal(err)
			}
			var deliveryFailure, deadLetterFailure string
			if err := f.db.QueryRowContext(f.ctx, `SELECT CAST(failure AS TEXT) FROM event_deliveries WHERE event_id=$1`, event.ID()).Scan(&deliveryFailure); err != nil {
				t.Fatal(err)
			}
			if err := f.db.QueryRowContext(f.ctx, `SELECT CAST(failure AS TEXT) FROM dead_letters WHERE original_event_id=$1`, event.ID()).Scan(&deadLetterFailure); err != nil {
				t.Fatal(err)
			}
			before := competingRevisionPairSnapshot(t, f)
			probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			for _, tc := range gateFailureReaderCases() {
				want := deliveryFailure
				if tc.name == "dead_letter" {
					want = deadLetterFailure
				}
				if got, err := tc.read(f.ctx, f.store, event.ID(), node.Key()); err != nil || got != want {
					t.Fatalf("%s changed exact persisted failure text: %q want %q err=%v", tc.name, got, want, err)
				}
			}
			if counts := probe.Snapshot(); counts.Total.Begun != 5 || counts.Total.ReadCommits != 5 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("independent failure reads escaped original read owner: %+v", counts)
			}
			for _, tc := range gateFailureReaderCases() {
				if got, err := tc.read(f.ctx, f.store, uuid.NewString(), node.Key()); !errors.Is(err, sql.ErrNoRows) || got != "" {
					t.Fatalf("%s borrowed foreign-event evidence: %q %v", tc.name, got, err)
				}
				if tc.name == "subscriber" || tc.name == "node" || tc.name == "coalesced" {
					if got, err := tc.read(f.ctx, f.store, event.ID(), "foreign-subscriber"); !errors.Is(err, sql.ErrNoRows) || got != "" {
						t.Fatalf("%s widened subscriber scope: %q %v", tc.name, got, err)
					}
				}
				cancelled, cancel := context.WithCancel(f.ctx)
				cancel()
				if got, err := tc.read(cancelled, f.store, event.ID(), node.Key()); !errors.Is(err, context.Canceled) || got != "" {
					t.Fatalf("%s ignored cancellation: %q %v", tc.name, got, err)
				}
			}
			if !reflect.DeepEqual(before, competingRevisionPairSnapshot(t, f)) {
				t.Fatal("failure observation changed data, history, lease or clocks")
			}
			restore()
			assertGateNodeFailureExcludesActualAgentDelivery(t, f, owner, state.Identity.RunID)
		})
	}
}

func assertGateNodeFailureExcludesActualAgentDelivery(t *testing.T, f receiverConfigActivationFixture, owner deliveryFixtureStore, runID string) {
	t.Helper()
	identity := mustTestAgentIdentityForRun(runID, "failure-reader-agent", "failure-read/agent")
	ctx := preparePositiveAgentClaimFixture(t, f.ctx, f.store, identity)
	event := eventtest.ExistingRunRootIngress(uuid.NewString(), "review/finish.requested", "fixture", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
	route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(identity.AgentID()), AgentIdentity: identity}
	if err := commitSemanticEventFixtureWithRoutes(ctx, owner, event, []events.DeliveryRoute{route}); err != nil {
		t.Fatal(err)
	}
	claimed, err := claimDeliveryFixture(ctx, owner, event, route)
	if err != nil {
		t.Fatal(err)
	}
	failure := testRetryableFailure()
	if _, err := owner.SettleFailure(ctx, claimed.Claim, deliverylifecycle.Settlement{Disposition: deliverylifecycle.FailureDeadLetter, ReasonCode: failure.Detail.Code, Failure: failure, RuleSelection: deliverylifecycle.NotApplicableHandlerRuleObservation()}); err != nil {
		t.Fatal(err)
	}
	if got, err := ReadGateSubscriberFailureForTest(f.ctx, f.store, event.ID(), identity.AgentID()); err != nil || got == "" {
		t.Fatalf("actual agent settlement lost subscriber evidence: %q %v", got, err)
	}
	if got, err := ReadGateNodeFailureForTest(f.ctx, f.store, event.ID(), identity.AgentID()); !errors.Is(err, sql.ErrNoRows) || got != "" {
		t.Fatalf("node-only read borrowed actual agent failure: %q %v", got, err)
	}
}

func TestGateFailureReadersRefuseRawAndClosedOwners(t *testing.T) {
	var postgres *PostgresStore
	var sqlite *SQLiteRuntimeStore
	for _, selected := range []any{nil, postgres, sqlite, &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		for _, tc := range gateFailureReaderCases() {
			if got, err := tc.read(context.Background(), selected, uuid.NewString(), "node"); err == nil || got != "" {
				t.Fatalf("%s accepted invalid owner %T: %q %v", tc.name, selected, got, err)
			}
		}
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, _ := newCompetingRevisionPairFixture(t, backend)
			if err := f.manager.Shutdown(); err != nil {
				t.Fatal(err)
			}
			if err := f.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			for _, tc := range gateFailureReaderCases() {
				if got, err := tc.read(f.ctx, f.store, uuid.NewString(), "node"); err == nil || got != "" {
					t.Fatalf("%s accepted closed native owner: %q %v", tc.name, got, err)
				}
			}
		})
	}
}
