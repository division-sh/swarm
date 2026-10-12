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

func TestCanonicalClaimAndAppearanceObserversKeepExactAttemptCutsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, state := newCompetingRevisionPairFixture(t, backend)
			owner := f.store.(deliveryFixtureStore)
			for _, kind := range []string{"node-success", "node-refusal", "agent-success"} {
				t.Run(kind, func(t *testing.T) {
					node := mustPersistenceNode("review", "progress")
					event := eventtest.ExistingRunRootIngress(uuid.NewString(), "review/finish.requested", "fixture", "", []byte(`{}`), 0, state.Identity.RunID, events.EventEnvelope{}, time.Now().UTC())
					route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "review", FlowInstance: state.Identity.Route.InstancePath, EntityID: state.EntityID})}
					if kind == "agent-success" {
						identity := mustTestAgentIdentityForRun(state.Identity.RunID, "canonical-observer", "canonical/observer")
						route = events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(identity.AgentID()), AgentIdentity: identity}
					}
					if err := commitSemanticEventFixtureWithRoutes(f.ctx, owner, event, []events.DeliveryRoute{route}); err != nil {
						t.Fatal(err)
					}
					assertCanonicalClaimStorageCut(t, f, event.ID(), kind, "pending", 0, 0, 0)
					claimed, err := claimDeliveryFixture(f.ctx, owner, event, route)
					if err != nil {
						t.Fatal(err)
					}
					assertCanonicalClaimStorageCut(t, f, event.ID(), kind, "in_progress", 1, 0, 1)
					status := "delivered"
					if kind == "node-refusal" {
						failure := testRetryableFailure()
						_, err = owner.SettleFailure(f.ctx, claimed.Claim, deliverylifecycle.Settlement{Disposition: deliverylifecycle.FailureDeadLetter, ReasonCode: failure.Detail.Code, Failure: failure, RuleSelection: handlerselection.NotReached()})
						status = "dead_letter"
					} else {
						_, err = owner.SettleSuccess(f.ctx, claimed.Claim, nil, 0, handlerselection.NotApplicable())
					}
					if err != nil {
						t.Fatal(err)
					}
					assertCanonicalClaimStorageCut(t, f, event.ID(), kind, status, 1, 1, 0)
				})
			}
			assertCanonicalClaimObserversFail(t, f.ctx, f.store, uuid.NewString(), sql.ErrNoRows)
			cancelled, cancel := context.WithCancel(f.ctx)
			cancel()
			assertCanonicalClaimObserversFail(t, cancelled, f.store, uuid.NewString(), context.Canceled)
		})
	}
}

func assertCanonicalClaimStorageCut(t *testing.T, f receiverConfigActivationFixture, eventID, kind, status string, attempts, settled, open int) {
	t.Helper()
	before := competingRevisionPairSnapshot(t, f)
	probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer restore()
	claim, claimErr := ReadCanonicalAccumulatorClaimStorageForTest(f.ctx, f.store, eventID)
	refusal, refusalErr := ReadCanonicalAccumulatorRefusalStorageForTest(f.ctx, f.store, eventID)
	appearance, appearanceErr := ReadReceiverAppearanceSettlementStorageForTest(f.ctx, f.store, eventID)
	if appearanceErr != nil || appearance.Status != status || appearance.Outcomes != settled {
		t.Fatalf("appearance borrowed another status/settlement cut: %+v %v", appearance, appearanceErr)
	}
	if kind == "agent-success" {
		if !errors.Is(claimErr, sql.ErrNoRows) || claim != (CanonicalAccumulatorClaimStorage{}) || !errors.Is(refusalErr, sql.ErrNoRows) || refusal != (CanonicalAccumulatorRefusalStorage{}) {
			t.Fatalf("node-only evidence borrowed actual agent: %+v/%v %+v/%v", claim, claimErr, refusal, refusalErr)
		}
	} else {
		if claimErr != nil || claim.DeliveryID == "" || claim.Status != status || claim.Attempts != attempts || claim.Settled != settled || refusalErr != nil || refusal != (CanonicalAccumulatorRefusalStorage{Status: status, Attempts: attempts, Settled: settled, Open: open}) {
			t.Fatalf("canonical node evidence changed identity/claim/open/settled predicates: %+v/%v %+v/%v", claim, claimErr, refusal, refusalErr)
		}
		var exact CanonicalAccumulatorClaimStorage
		if err := f.db.QueryRowContext(f.ctx, `SELECT d.delivery_id, d.status,
 (SELECT COUNT(*) FROM event_delivery_attempts a WHERE a.delivery_id=d.delivery_id AND a.claim_token IS NOT NULL),
 (SELECT COUNT(*) FROM event_delivery_attempts a WHERE a.delivery_id=d.delivery_id AND a.closure_kind='settled' AND a.open_marker=FALSE)
 FROM event_deliveries d WHERE d.event_id=$1 AND d.subscriber_type='node'`, eventID).Scan(&exact.DeliveryID, &exact.Status, &exact.Attempts, &exact.Settled); err != nil || exact != claim {
			t.Fatalf("canonical native read changed original physical row: %+v %+v %v", exact, claim, err)
		}
	}
	committed, failed := uint64(3), uint64(0)
	if kind == "agent-success" {
		committed, failed = 1, 2
	}
	if counts := probe.Snapshot(); counts.Active != 0 || counts.Total.Begun != 3 || counts.Total.ReadCommits != committed || counts.Total.Failed != failed || counts.Total.RollbackAttempts != failed || counts.Total.WriteCommits != 0 {
		t.Fatalf("three original read cuts collapsed or bypassed coordinator: %+v", counts)
	}
	restore()
	if !reflect.DeepEqual(before, competingRevisionPairSnapshot(t, f)) {
		t.Fatal("claim/settlement observation changed data, history, ownership or clocks")
	}
}

func assertCanonicalClaimObserversFail(t *testing.T, ctx context.Context, owner any, eventID string, cause error) {
	t.Helper()
	check := func(err error) bool { return err != nil && (cause == nil || errors.Is(err, cause)) }
	if got, err := ReadCanonicalAccumulatorClaimStorageForTest(ctx, owner, eventID); !check(err) || got != (CanonicalAccumulatorClaimStorage{}) {
		t.Fatalf("invalid owner/identity supplied canonical claim: %+v %v", got, err)
	}
	if got, err := ReadCanonicalAccumulatorRefusalStorageForTest(ctx, owner, eventID); !check(err) || got != (CanonicalAccumulatorRefusalStorage{}) {
		t.Fatalf("invalid owner/identity supplied canonical refusal: %+v %v", got, err)
	}
	if got, err := ReadReceiverAppearanceSettlementStorageForTest(ctx, owner, eventID); !check(err) || got != (ReceiverAppearanceSettlementStorage{}) {
		t.Fatalf("invalid owner/identity supplied appearance settlement: %+v %v", got, err)
	}
}

func TestCanonicalClaimAndAppearanceObserversRefuseRawInvalidAndClosedOwnersBothStores(t *testing.T) {
	for _, owner := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		assertCanonicalClaimObserversFail(t, context.Background(), owner, uuid.NewString(), nil)
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, _ := newCompetingRevisionPairFixture(t, backend)
			assertCanonicalClaimObserversFail(t, f.ctx, f.store, "invalid-event", nil)
			if err := f.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			assertCanonicalClaimObserversFail(t, f.ctx, f.store, uuid.NewString(), nil)
		})
	}
}
