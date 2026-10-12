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
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestA2WorkerOutputObserverKeepsRunParentNameMultiplicityAndMinimumBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, map[string]string{
				"schema.yaml": "name: a2-worker-output-observation\n",
				"events.yaml": "work.requested:\nitem.completed:\nforeign.completed:\n",
			}, nil)
			runID := correlation.RunIDFromContext(f.ctx)
			parent := eventtest.ExistingRunRootIngress(uuid.NewString(), "work.requested", "fixture", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
			other := eventtest.ExistingRunRootIngress(uuid.NewString(), "work.requested", "fixture", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
			for _, event := range []events.Event{parent, other} {
				if err := commitSemanticEventFixtureWithRoutes(f.ctx, f.store, event, nil); err != nil {
					t.Fatal(err)
				}
			}
			minimum := "11111111-1111-4111-8111-111111111111"
			for _, child := range []struct {
				id     string
				name   events.EventType
				parent events.Event
			}{
				{"11111111-1111-4111-8111-111111111112", "item.completed", parent},
				{minimum, "item.completed", parent},
				{uuid.NewString(), "foreign.completed", parent},
				{uuid.NewString(), "item.completed", other},
			} {
				event := eventtest.Child(child.id, child.name, "fixture", "", []byte(`{}`), 1, child.parent, events.EventEnvelope{}, time.Now().UTC())
				if err := commitSemanticEventFixtureWithRoutes(f.ctx, f.store, event, nil); err != nil {
					t.Fatal(err)
				}
			}
			var want A2WorkerOutputStorage
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*), COALESCE(MIN(CAST(event_id AS TEXT)), '') FROM events WHERE run_id=$1 AND source_event_id=$2 AND event_name='item.completed'`, runID, parent.ID()).Scan(&want.Count, &want.EventID); err != nil || want.Count != 2 || want.EventID != minimum {
				t.Fatalf("real source-linked events lost positive multiplicity/minimum evidence: %+v %v", want, err)
			}
			before := competingRevisionPairSnapshot(t, f)
			probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			if got, err := ReadA2WorkerOutputsForTest(f.ctx, f.store, runID, parent.ID()); err != nil || got != want {
				t.Fatalf("worker output aggregate changed exact count/lexical minimum: %+v %v", got, err)
			}
			if counts := probe.Snapshot(); counts.Active != 0 || counts.Total.Begun != 1 || counts.Total.ReadCommits != 1 || counts.Total.WriteCommits != 0 {
				t.Fatalf("worker output read escaped exact selected coordinator: %+v", counts)
			}
			for _, scope := range [][2]string{{uuid.NewString(), parent.ID()}, {runID, uuid.NewString()}} {
				if got, err := ReadA2WorkerOutputsForTest(f.ctx, f.store, scope[0], scope[1]); err != nil || got != (A2WorkerOutputStorage{}) {
					t.Fatalf("worker output widened run/source or changed zero/minimum coalescence: %+v %v", got, err)
				}
			}
			cancelled, cancel := context.WithCancel(f.ctx)
			cancel()
			if got, err := ReadA2WorkerOutputsForTest(cancelled, f.store, runID, parent.ID()); !errors.Is(err, context.Canceled) || got != (A2WorkerOutputStorage{}) {
				t.Fatalf("cancelled worker read supplied output evidence: %+v %v", got, err)
			}
			if !reflect.DeepEqual(before, competingRevisionPairSnapshot(t, f)) {
				t.Fatal("worker output read changed application/history/lease/clock evidence")
			}
		})
	}
}

func TestA2WorkerOutputObserverRefusesRawAndClosedOwners(t *testing.T) {
	for _, selected := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		if got, err := ReadA2WorkerOutputsForTest(context.Background(), selected, uuid.NewString(), uuid.NewString()); err == nil || got != (A2WorkerOutputStorage{}) {
			t.Fatalf("worker output accepted non-native owner %T: %+v %v", selected, got, err)
		}
	}
	f, _ := newCompetingRevisionPairFixture(t, "sqlite")
	if err := f.store.(*SQLiteRuntimeStore).Close(); err != nil {
		t.Fatal(err)
	}
	if got, err := ReadA2WorkerOutputsForTest(f.ctx, f.store, uuid.NewString(), uuid.NewString()); err == nil || got != (A2WorkerOutputStorage{}) {
		t.Fatalf("worker output accepted closed owner: %+v %v", got, err)
	}
}
