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
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestRunPublicationHistoryObserverKeepsOneSnapshotAndBothOriginalCutsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, state := newCompetingRevisionPairFixture(t, backend)
			event := eventtest.ExistingRunRootIngress(uuid.NewString(), "review/finish.requested", "fixture", "", []byte(`{}`), 0, state.Identity.RunID, events.EventEnvelope{}, time.Now().UTC())
			route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(mustPersistenceNode("review", "progress")), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "review", FlowInstance: state.Identity.Route.InstancePath, EntityID: state.EntityID})}
			event, err := events.ResolveEnvelope(event, events.EnvelopeForTargetRoute(event.NormalizedEnvelope(), route.Target.Route()))
			if err != nil {
				t.Fatal(err)
			}
			owner := f.store.(deliveryFixtureStore)
			if err := commitSemanticEventFixtureWithRoutes(f.ctx, owner, event, []events.DeliveryRoute{route}); err != nil {
				t.Fatal(err)
			}
			var want RunPublicationHistoryStorage
			if err := f.db.QueryRowContext(f.ctx, `SELECT (SELECT COUNT(*) FROM events WHERE run_id=$1),
				(SELECT COUNT(*) FROM run_fork_revisions WHERE run_id=$1)`, state.Identity.RunID).Scan(&want.Events, &want.Revisions); err != nil || want.Events == 0 || want.Revisions == 0 {
				t.Fatalf("native compiled/publication fixture has no positive original physical history: %+v %v", want, err)
			}
			before := competingRevisionPairSnapshot(t, f)
			probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			for cut := 1; cut <= 2; cut++ {
				if got, err := ReadRunPublicationHistoryStorageForTest(f.ctx, f.store, state.Identity.RunID); err != nil || got != want {
					t.Fatalf("cut%d changed original whole-run int counts or scope: %+v/%+v %v", cut, got, want, err)
				}
				if counts := probe.Snapshot(); counts.Active != 0 || counts.Total.Begun != uint64(cut) || counts.Total.ReadCommits != uint64(cut) || counts.Total.WriteCommits != 0 {
					t.Fatalf("cut%d split components or merged independent native snapshots: %+v", cut, counts)
				}
			}
			if err := commitSemanticEventFixtureWithRoutes(f.ctx, owner, event, []events.DeliveryRoute{route}); err != nil {
				t.Fatal(err)
			}
			if got, err := ReadRunPublicationHistoryStorageForTest(f.ctx, f.store, state.Identity.RunID); err != nil || got != want {
				t.Fatalf("real duplicate changed publication/history witness: %+v %v", got, err)
			}
			if got, err := ReadRunPublicationHistoryStorageForTest(f.ctx, f.store, uuid.NewString()); err != nil || got != (RunPublicationHistoryStorage{}) {
				t.Fatalf("foreign run borrowed current publications/revisions: %+v %v", got, err)
			}
			cancelled, cancel := context.WithCancel(f.ctx)
			cancel()
			if got, err := ReadRunPublicationHistoryStorageForTest(cancelled, f.store, state.Identity.RunID); !errors.Is(err, context.Canceled) || got != (RunPublicationHistoryStorage{}) {
				t.Fatalf("cancelled snapshot returned partial evidence: %+v %v", got, err)
			}
			restore()
			if !reflect.DeepEqual(before, competingRevisionPairSnapshot(t, f)) {
				t.Fatal("read or exact duplicate changed application/history/ownership/clocks")
			}
		})
	}
}

func TestRunPublicationHistoryObserverRefusesRawInvalidAndClosedOwnersBothStores(t *testing.T) {
	for _, owner := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		if got, err := ReadRunPublicationHistoryStorageForTest(context.Background(), owner, uuid.NewString()); err == nil || got != (RunPublicationHistoryStorage{}) {
			t.Fatalf("raw owner %T supplied publication/history witness: %+v %v", owner, got, err)
		}
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, state := newCompetingRevisionPairFixture(t, backend)
			if got, err := ReadRunPublicationHistoryStorageForTest(f.ctx, f.store, "invalid-run"); err == nil || got != (RunPublicationHistoryStorage{}) {
				t.Fatalf("invalid run supplied publication/history witness: %+v %v", got, err)
			}
			if err := f.manager.Shutdown(); err != nil {
				t.Fatal(err)
			}
			if err := f.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			if got, err := ReadRunPublicationHistoryStorageForTest(f.ctx, f.store, state.Identity.RunID); err == nil || got != (RunPublicationHistoryStorage{}) {
				t.Fatalf("closed owner supplied publication/history witness: %+v %v", got, err)
			}
		})
	}
}
