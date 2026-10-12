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

func TestA2CompoundFootprintsKeepOriginalSlotsAndOneSnapshotBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, state := newCompetingRevisionPairFixture(t, backend)
			owner := f.store.(deliveryFixtureStore)
			for _, disposition := range []string{"success", "refusal"} {
				event := eventtest.ExistingRunRootIngress(uuid.NewString(), "review/finish.requested", "fixture", "", []byte(`{}`), 0, state.Identity.RunID, events.EventEnvelope{}, time.Now().UTC())
				route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(mustPersistenceNode("review", "progress")), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "review", FlowInstance: state.Identity.Route.InstancePath, EntityID: state.EntityID})}
				if err := commitSemanticEventFixtureWithRoutes(f.ctx, owner, event, []events.DeliveryRoute{route}); err != nil {
					t.Fatal(err)
				}
				assertA2CompoundFootprintCut(t, f, state.Identity.RunID, 1)
				claimed, err := claimDeliveryFixture(f.ctx, owner, event, route)
				if err != nil {
					t.Fatal(err)
				}
				assertA2CompoundFootprintCut(t, f, state.Identity.RunID, 1)
				if disposition == "success" {
					_, err = owner.SettleSuccess(f.ctx, claimed.Claim, nil, 0, handlerselection.NotApplicable())
				} else {
					failure := testRetryableFailure()
					_, err = owner.SettleFailure(f.ctx, claimed.Claim, deliverylifecycle.Settlement{Disposition: deliverylifecycle.FailureDeadLetter, ReasonCode: failure.Detail.Code, Failure: failure, RuleSelection: handlerselection.NotReached()})
				}
				if err != nil {
					t.Fatal(err)
				}
				assertA2CompoundFootprintCut(t, f, state.Identity.RunID, 0)
			}
			for _, tc := range a2CompoundFootprintReadCases(state.Identity.RunID) {
				cancelled, cancel := context.WithCancel(f.ctx)
				cancel()
				if got, err := tc.read(cancelled, f.store); !errors.Is(err, context.Canceled) || !reflect.DeepEqual(got, tc.empty) {
					t.Fatalf("cancelled %s exposed partial footprint: %+v %v", tc.name, got, err)
				}
			}
			for _, tc := range a2CompoundFootprintReadCases(uuid.NewString()) {
				got, err := tc.read(f.ctx, f.store)
				if (err != nil) != tc.absentError || !reflect.DeepEqual(got, tc.empty) {
					t.Fatalf("foreign %s borrowed footprint/header: %+v %v", tc.name, got, err)
				}
			}
		})
	}
}

func assertA2CompoundFootprintCut(t *testing.T, f receiverConfigActivationFixture, run string, pending int) {
	t.Helper()
	var hydration, cancellation [5]int
	var history A2MapHistoryFootprintStorage
	if err := f.db.QueryRowContext(f.ctx, `SELECT
		(SELECT COUNT(*) FROM entity_mutations WHERE run_id=$1),
		(SELECT COUNT(*) FROM run_fork_revisions WHERE run_id=$1),
		(SELECT COUNT(*) FROM timers WHERE run_id=$1),
		(SELECT COUNT(*) FROM events WHERE run_id=$1),
		(SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1)`, run).Scan(&hydration[0], &hydration[1], &hydration[2], &hydration[3], &hydration[4]); err != nil {
		t.Fatal(err)
	}
	if err := f.db.QueryRowContext(f.ctx, `SELECT
		(SELECT COUNT(*) FROM events WHERE run_id=$1),
		(SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1),
		(SELECT COUNT(*) FROM event_delivery_attempts WHERE delivery_id IN (SELECT delivery_id FROM event_deliveries WHERE run_id=$1)),
		(SELECT COUNT(*) FROM entity_mutations WHERE run_id=$1),
		(SELECT COUNT(*) FROM run_fork_revisions WHERE run_id=$1)`, run).Scan(&cancellation[0], &cancellation[1], &cancellation[2], &cancellation[3], &cancellation[4]); err != nil {
		t.Fatal(err)
	}
	if err := f.db.QueryRowContext(f.ctx, `SELECT
		(SELECT COUNT(*) FROM entity_mutations WHERE run_id=$1),
		(SELECT COUNT(*) FROM run_fork_fact_revisions WHERE run_id=$1),
		(SELECT COUNT(*) FROM run_fork_revisions WHERE run_id=$1)`, run).Scan(&history.Mutations, &history.Facts, &history.Revisions); err != nil || history.Mutations == 0 || history.Revisions == 0 || cancellation[0] == 0 || cancellation[1] == 0 {
		t.Fatalf("native construction/publication has no positive original physical footprint: %+v/%v %v", history, cancellation, err)
	}
	before := competingRevisionPairSnapshot(t, f)
	probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer restore()
	if got, err := ReadA2JoinHydrationFootprintStorageForTest(f.ctx, f.store, run); err != nil || got != hydration {
		t.Fatalf("hydration reordered slots or changed physical scope: %v/%v %v", got, hydration, err)
	}
	if got, err := ReadA2CancellationFootprintStorageForTest(f.ctx, f.store, run); err != nil || got != cancellation {
		t.Fatalf("cancellation changed event/delivery/attempt/history scope: %v/%v %v", got, cancellation, err)
	}
	if got, err := ReadA2MapHistoryFootprintStorageForTest(f.ctx, f.store, run); err != nil || got != history {
		t.Fatalf("map history swapped fact/revision slots: %+v/%+v %v", got, history, err)
	}
	if got, err := CountRunNonterminalDeliveryStorageForTest(f.ctx, f.store, run); err != nil || got != pending {
		t.Fatalf("nonterminal scope lost pending/in-progress or retained delivered/dead-letter work: %d/%d %v", got, pending, err)
	}
	if counts := probe.Snapshot(); counts.Active != 0 || counts.Total.Begun != 4 || counts.Total.ReadCommits != 4 || counts.Total.WriteCommits != 0 {
		t.Fatalf("footprints split compound snapshots or merged independent cuts: %+v", counts)
	}
	// Earlier components are positive, but the original top-level header is absent.
	if got, err := ReadA2RetainedSourceFootprintStorageForTest(f.ctx, f.store, run); !errors.Is(err, sql.ErrNoRows) || !reflect.DeepEqual(got, A2RetainedSourceFootprintStorage{}) {
		t.Fatalf("missing fan-out header became partial count evidence: %+v %v", got, err)
	}
	if counts := probe.Snapshot(); counts.Active != 0 || counts.Total.Begun != 5 || counts.Total.ReadCommits != 4 || counts.Total.WriteCommits != 0 {
		t.Fatalf("missing header published a committed footprint: %+v", counts)
	}
	restore()
	if !reflect.DeepEqual(before, competingRevisionPairSnapshot(t, f)) {
		t.Fatal("footprint changed complete application/history/ownership/clocks")
	}
}

func a2CompoundFootprintReadCases(run string) []fanOutSemanticReadCase {
	return []fanOutSemanticReadCase{
		{"hydration", func(ctx context.Context, owner any) (any, error) {
			return ReadA2JoinHydrationFootprintStorageForTest(ctx, owner, run)
		}, [5]int{}, false},
		{"cancellation", func(ctx context.Context, owner any) (any, error) {
			return ReadA2CancellationFootprintStorageForTest(ctx, owner, run)
		}, [5]int{}, false},
		{"map_history", func(ctx context.Context, owner any) (any, error) {
			return ReadA2MapHistoryFootprintStorageForTest(ctx, owner, run)
		}, A2MapHistoryFootprintStorage{}, false},
		{"source", func(ctx context.Context, owner any) (any, error) {
			return ReadA2RetainedSourceFootprintStorageForTest(ctx, owner, run)
		}, A2RetainedSourceFootprintStorage{}, true},
		{"nonterminal", func(ctx context.Context, owner any) (any, error) {
			return CountRunNonterminalDeliveryStorageForTest(ctx, owner, run)
		}, 0, false},
	}
}

func TestA2CompoundFootprintsRefuseRawInvalidAndClosedOwnersBothStores(t *testing.T) {
	refuse := func(ctx context.Context, owner any, run string) {
		for _, tc := range a2CompoundFootprintReadCases(run) {
			if got, err := tc.read(ctx, owner); err == nil || !reflect.DeepEqual(got, tc.empty) {
				t.Fatalf("invalid owner/run supplied %s footprint: %+v %v", tc.name, got, err)
			}
		}
	}
	for _, owner := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		refuse(context.Background(), owner, uuid.NewString())
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, state := newCompetingRevisionPairFixture(t, backend)
			refuse(f.ctx, f.store, "invalid-run")
			if err := f.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			refuse(f.ctx, f.store, state.Identity.RunID)
		})
	}
}
