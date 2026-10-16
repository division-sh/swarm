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

func TestGateCausalMutationCountKeepsExactRunAndEventHistoryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, state := newCompetingRevisionPairFixture(t, backend)
			admitted := eventtest.ExistingRunRootIngress(uuid.NewString(), "review/finish.requested", "fixture", "", []byte(`{}`), 0, state.Identity.RunID, events.EventEnvelope{}, time.Now().UTC())
			if err := commitSemanticEventFixtureWithRoutes(f.ctx, f.store.(deliveryFixtureStore), admitted, nil); err != nil {
				t.Fatal(err)
			}
			event := admitted.ID()
			var physical int
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM entity_mutations WHERE run_id=$1 AND caused_by_event=$2`, state.Identity.RunID, event).Scan(&physical); err != nil || physical != 0 {
				t.Fatalf("unexecuted admitted event unexpectedly owns construction history: %d %v", physical, err)
			}
			if construction, err := CountRunMutationStorageForTest(f.ctx, f.store, state.Identity.RunID); err != nil || construction == 0 {
				t.Fatalf("NULL-cause construction history did not distinguish the event predicate: %d %v", construction, err)
			}
			before := competingRevisionPairSnapshot(t, f)
			probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			for _, scope := range []struct {
				run, event string
				want       int
			}{{state.Identity.RunID, event, physical}, {uuid.NewString(), event, 0}, {state.Identity.RunID, uuid.NewString(), 0}} {
				if got, err := CountRunEventMutationStorageForTest(f.ctx, f.store, scope.run, scope.event); err != nil || got != scope.want {
					t.Fatalf("causal count widened or lost exact run/event: %+v got=%d err=%v", scope, got, err)
				}
			}
			if counters := probe.Snapshot(); counters.Active != 0 || counters.Total.Begun != 3 || counters.Total.ReadCommits != 3 || counters.Total.WriteCommits != 0 {
				t.Fatalf("causal reads escaped original coordinator or changed temporal cuts: %+v", counters)
			}
			cancelled, cancel := context.WithCancel(f.ctx)
			cancel()
			if got, err := CountRunEventMutationStorageForTest(cancelled, f.store, state.Identity.RunID, event); !errors.Is(err, context.Canceled) || got != 0 {
				t.Fatalf("cancelled causal read supplied evidence: %d %v", got, err)
			}
			if !reflect.DeepEqual(before, competingRevisionPairSnapshot(t, f)) {
				t.Fatal("causal observation changed complete data, history, leases or clocks")
			}
		})
	}
}

func TestGateCausalCountsRefuseRawOwners(t *testing.T) {
	for _, owner := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		if got, err := CountRunEventMutationStorageForTest(context.Background(), owner, uuid.NewString(), uuid.NewString()); err == nil || got != 0 {
			t.Fatalf("causal mutation count accepted invalid owner %T: %d %v", owner, got, err)
		}
		if got, err := CountRunEventTimerStorageForTest(context.Background(), owner, uuid.NewString(), "event"); err == nil || got != 0 {
			t.Fatalf("run/event timer count accepted invalid owner %T: %d %v", owner, got, err)
		}
	}
	f, state := newCompetingRevisionPairFixture(t, "sqlite")
	if err := f.manager.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if err := f.store.(*SQLiteRuntimeStore).Close(); err != nil {
		t.Fatal(err)
	}
	if got, err := CountRunEventMutationStorageForTest(f.ctx, f.store, state.Identity.RunID, uuid.NewString()); err == nil || got != 0 {
		t.Fatalf("closed owner supplied causal mutation evidence: %d %v", got, err)
	}
}
