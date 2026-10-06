package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestRunForkSnapshotFixtureUsesOriginalCoordinatorBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx := testAuthorActivityContext()
			event := runForkSnapshotFixtureEvent(t, ctx, fixture.store)
			before, err := ReadSemanticEventFixtureEvidenceForTest(ctx, fixture.store, event.RunID(), event.ID())
			if err != nil {
				t.Fatal(err)
			}
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			if err := CaptureRunForkSnapshotForTest(ctx, fixture.store, event.RunID()); err != nil {
				t.Fatal(err)
			}
			if counts := probe.Snapshot(); counts.Total.Begun != 1 || counts.Total.WriteCommits != 1 || counts.Total.Revision.Finalizations != 1 || counts.Active != 0 {
				t.Fatalf("snapshot bypassed selected mutation/revision owners: %+v", counts)
			}
			after, err := ReadSemanticEventFixtureEvidenceForTest(ctx, fixture.store, event.RunID(), event.ID())
			if err != nil || after.RevisionCount <= before.RevisionCount || !reflect.DeepEqual(after.Record, before.Record) {
				t.Fatalf("snapshot changed the event or omitted historical capture: before=%+v after=%+v err=%v", before, after, err)
			}
			planner := fixture.store.(interface {
				PlanRunFork(context.Context, runfork.RunForkPlanRequest) (runfork.RunForkPlan, error)
			})
			plan, err := planner.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: event.RunID(), At: event.ID()})
			if err != nil || plan.ForkPoint.EventID != event.ID() {
				t.Fatalf("captured historical event is not plannable: plan=%+v err=%v", plan, err)
			}
			if err := CaptureRunForkSnapshotForTest(ctx, fixture.store, event.RunID()); err != nil {
				t.Fatal(err)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if err := CaptureRunForkSnapshotForTest(cancelled, fixture.store, event.RunID()); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled snapshot = %v", err)
			}
			for _, runID := range []string{"", "invalid", uuid.NewString()} {
				if err := CaptureRunForkSnapshotForTest(ctx, fixture.store, runID); err == nil {
					t.Fatalf("snapshot accepted absent or invalid run %q", runID)
				}
			}
			got, err := ReadSemanticEventFixtureEvidenceForTest(ctx, fixture.store, event.RunID(), event.ID())
			if err != nil || !reflect.DeepEqual(got, after) {
				t.Fatalf("replay/refusal changed stored snapshot: got=%+v want=%+v err=%v", got, after, err)
			}
			if counts := probe.Snapshot(); counts.Total.WriteCommits != 2 || counts.Active != 0 {
				t.Fatalf("refused snapshot committed or retained an attempt: %+v", counts)
			}
			switch selected := fixture.store.(type) {
			case *PostgresStore:
				err = selected.backend.Close()
			case *SQLiteRuntimeStore:
				err = selected.backend.Close()
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := CaptureRunForkSnapshotForTest(ctx, fixture.store, event.RunID()); err == nil {
				t.Fatal("closed store admitted a historical snapshot")
			}
		})
	}
	for _, selected := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		if err := CaptureRunForkSnapshotForTest(context.Background(), selected, uuid.NewString()); err == nil {
			t.Errorf("invalid selected owner %T admitted snapshot authority", selected)
		}
	}
}

func runForkSnapshotFixtureEvent(t *testing.T, ctx context.Context, selected any) events.Event {
	t.Helper()
	runID := uuid.NewString()
	requireRunFixtureForTest(t, ctx, selected, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID})
	event := eventtest.ExistingRunRootIngress(uuid.NewString(), "snapshot.fixture", "fixture", "", []byte(`{"value":1}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
	event, err := bindSemanticEventFixturePayload(event)
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := events.AdmitForPersistence(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		t.Fatal(err)
	}
	if inserted, err := CommitSemanticEventFixtureForTest(ctx, selected, admitted, testRouteSettlement(event, nil), nil, pipelineobligation.ScopeDirect, nil); err != nil || !inserted {
		t.Fatalf("seed unrevisioned historical event: inserted=%t err=%v", inserted, err)
	}
	return event
}

func TestRunForkSnapshotFixtureSQLiteQueuedCancellationUsesOriginalWriter(t *testing.T) {
	fixture := openSQLiteAuthorActivityReceiptFixture(t)
	selected := fixture.store.(*SQLiteRuntimeStore)
	ctx, cancel := context.WithTimeout(testAuthorActivityContext(), 10*time.Second)
	defer cancel()
	event := runForkSnapshotFixtureEvent(t, ctx, selected)
	before, err := ReadSemanticEventFixtureEvidenceForTest(ctx, selected, event.RunID(), event.ID())
	if err != nil {
		t.Fatal(err)
	}
	entered, release, writerDone := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		writerDone <- selected.backend.RunTransaction(ctx, "hold snapshot sibling writer", func(context.Context, *sql.Tx) error {
			close(entered)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer func() {
		unblock()
		if err := <-writerDone; err != nil {
			t.Error(err)
		}
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	queued, cancelQueued := context.WithCancel(ctx)
	defer cancelQueued()
	observed := &forkFixtureAdmissionContext{Context: queued, entered: make(chan struct{})}
	done := make(chan error, 1)
	go func() { done <- CaptureRunForkSnapshotForTest(observed, selected, event.RunID()) }()
	select {
	case <-observed.entered:
	case err := <-done:
		t.Fatalf("snapshot bypassed original writer admission: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	cancelQueued()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("queued cancellation = %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if got, err := ReadSemanticEventFixtureEvidenceForTest(ctx, selected, event.RunID(), event.ID()); err != nil || !reflect.DeepEqual(got, before) {
		t.Fatalf("queued cancellation committed snapshot facts: got=%+v before=%+v err=%v", got, before, err)
	}
	unblock()
}
