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
			onlyOtherOutput := uuid.NewString()
			for _, child := range []struct {
				id     string
				name   events.EventType
				parent events.Event
			}{
				{"11111111-1111-4111-8111-111111111112", "item.completed", parent},
				{minimum, "item.completed", parent},
				{uuid.NewString(), "foreign.completed", parent},
				{onlyOtherOutput, "item.completed", other},
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
			assertA2SourceChildDiscovery(t, f, parent.ID(), other.ID(), onlyOtherOutput)
			if counts := probe.Snapshot(); counts.Active != 0 || counts.Total.Begun != 4 || counts.Total.ReadCommits != 4 || counts.Total.WriteCommits != 0 {
				t.Fatalf("source child/count/identity reads merged cuts or escaped the original coordinator: %+v", counts)
			}
			missing := uuid.NewString()
			if got, err := CountSourceEventPublicationsForTest(f.ctx, f.store, missing); err != nil || got != 0 {
				t.Fatalf("absent parent supplied a publication count: %d %v", got, err)
			}
			if got, err := ReadSourceEventPublicationIdentityForTest(f.ctx, f.store, missing); !errors.Is(err, sql.ErrNoRows) || got != "" {
				t.Fatalf("absent parent supplied a child identity: %q %v", got, err)
			}
			for _, scope := range [][2]string{{missing, "item.completed"}, {parent.ID(), "absent.completed"}} {
				if got, err := ReadSourceNamedChildEventIDsForTest(f.ctx, f.store, scope[0], scope[1]); err != nil || got != nil {
					t.Fatalf("child discovery widened parent/name or changed nil absence: %v %v", got, err)
				}
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
			assertA2SourceChildReadFailure(t, cancelled, f.store, parent.ID(), context.Canceled)
			if !reflect.DeepEqual(before, competingRevisionPairSnapshot(t, f)) {
				t.Fatal("worker output read changed application/history/lease/clock evidence")
			}
		})
	}
}

func assertA2SourceChildDiscovery(t *testing.T, f receiverConfigActivationFixture, parent, otherParent, soleOutput string) {
	t.Helper()
	if got, err := CountSourceEventPublicationsForTest(f.ctx, f.store, parent); err != nil || got != 3 {
		t.Fatalf("source-only publication count added a name/run/status filter: %d %v", got, err)
	}
	if got, err := ReadSourceEventPublicationIdentityForTest(f.ctx, f.store, otherParent); err != nil || got != soleOutput {
		t.Fatalf("source-only identity changed the actual stored child: %q %v", got, err)
	}
	want := []string{"11111111-1111-4111-8111-111111111111", "11111111-1111-4111-8111-111111111112"}
	if got, err := ReadSourceNamedChildEventIDsForTest(f.ctx, f.store, parent, "item.completed"); err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("child discovery changed immediate parent/name scope, multiplicity or event-id ordering: %v %v", got, err)
	}
}

func assertA2SourceChildReadFailure(t *testing.T, ctx context.Context, owner any, source string, cause error) {
	t.Helper()
	check := func(err error) bool { return err != nil && (cause == nil || errors.Is(err, cause)) }
	if got, err := CountSourceEventPublicationsForTest(ctx, owner, source); !check(err) || got != 0 {
		t.Fatalf("failed source read supplied a publication count: %d %v", got, err)
	}
	if got, err := ReadSourceEventPublicationIdentityForTest(ctx, owner, source); !check(err) || got != "" {
		t.Fatalf("failed source read supplied a child identity: %q %v", got, err)
	}
	if got, err := ReadSourceNamedChildEventIDsForTest(ctx, owner, source, "item.completed"); !check(err) || got != nil {
		t.Fatalf("failed source read supplied ordered children: %v %v", got, err)
	}
}

func TestA2WorkerOutputObserverRefusesRawAndClosedOwners(t *testing.T) {
	for _, selected := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		if got, err := ReadA2WorkerOutputsForTest(context.Background(), selected, uuid.NewString(), uuid.NewString()); err == nil || got != (A2WorkerOutputStorage{}) {
			t.Fatalf("worker output accepted non-native owner %T: %+v %v", selected, got, err)
		}
		assertA2SourceChildReadFailure(t, context.Background(), selected, uuid.NewString(), nil)
	}
	f, _ := newCompetingRevisionPairFixture(t, "sqlite")
	if err := f.manager.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if err := f.store.(*SQLiteRuntimeStore).Close(); err != nil {
		t.Fatal(err)
	}
	if got, err := ReadA2WorkerOutputsForTest(f.ctx, f.store, uuid.NewString(), uuid.NewString()); err == nil || got != (A2WorkerOutputStorage{}) {
		t.Fatalf("worker output accepted closed owner: %+v %v", got, err)
	}
	assertA2SourceChildReadFailure(t, f.ctx, f.store, uuid.NewString(), nil)
}
