package construction_test

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/construction"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestReleaseProcessReadOnlyInspectionRetainsNativeEvidenceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			writer, location, db := releaseInspectionWriter(t, backend)
			identity := runtimeflowidentity.RunScopedFlowInstance{
				RunID: "b3c04b72-07f1-4097-a1f0-779955000001",
				Route: runtimeflowidentity.DeriveRoute("inspect", "one"),
			}
			entityID := runtimeidentity.NormalizeEntityID("b3c04b72-07f1-4097-a1f0-779955000002")
			storetest.RequireRunningRun(t, ctx, writer.(storetest.RunFixtureStore), identity.RunID, time.Now().UTC())
			if _, err := db.ExecContext(ctx, `INSERT INTO entity_state
				(run_id,entity_id,flow_instance,entity_type,current_state,revision,fields,entered_state_at,created_at,updated_at)
				VALUES ($1,$2,$3,'inspection_entity','ready',1,$4,$5,$5,$5)`,
				identity.RunID, entityID.String(), identity.Route.InstancePath,
				`{"quantity":9007199254740993,"optional":null}`, time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			before, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, writer)
			if err != nil {
				t.Fatal(err)
			}
			want, err := writer.(pipeline.WorkflowTargetPersistenceReader).LoadWorkflowTargetPersistence(ctx, identity, entityID)
			if err != nil || want.Presence != pipeline.WorkflowTargetPersistenceStateOnly || !bytes.Contains(want.State.Fields, []byte("9007199254740993")) {
				t.Fatalf("native writer evidence: %+v %v", want, err)
			}
			observer, err := storetest.OpenReleaseProcessReadOnlyInspection(backend, location)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := observer.Close(); err != nil {
					t.Error(err)
				}
			})
			got, err := readReleaseInspectionTarget(ctx, observer, identity, entityID)
			if err != nil || !reflect.DeepEqual(want, got) {
				t.Fatalf("independent inspection lost canonical target evidence: want=%+v got=%+v err=%v", want, got, err)
			}
			lifecycle, err := storetest.ReadLifecycleBusinessStorage(ctx, writer, identity.RunID)
			if err != nil {
				t.Fatal(err)
			}
			if got, err := readReleaseInspectionLifecycle(ctx, observer, identity.RunID); err != nil || !reflect.DeepEqual(lifecycle, got) {
				t.Fatalf("independent unaccepted inspection lost physical lifecycle rows: %+v %v", got, err)
			}
			requireReleaseInspectionMailboxAndDiagnostics(t, writer, observer, identity.RunID)
			var retained context.Context
			if err := observer.InspectSnapshot(ctx, func(scoped context.Context) error {
				retained = scoped
				_, err := observer.LoadWorkflowTargetPersistence(scoped, identity, entityID)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if target, err := observer.LoadWorkflowTargetPersistence(retained, identity, entityID); err == nil || !reflect.DeepEqual(target, pipeline.WorkflowTargetPersistenceRecord{}) {
				t.Fatalf("retired snapshot context remained executable: %+v %v", target, err)
			}
			if snapshot, err := storetest.ReadSelectedForkApplicationStorageSnapshot(retained, observer); err == nil || snapshot != nil {
				t.Fatalf("retired snapshot returned physical evidence: %v %v", snapshot, err)
			}
			if rows, err := storetest.ReadLifecycleBusinessStorage(retained, observer, identity.RunID); err == nil || rows != nil {
				t.Fatalf("retired inspection returned lifecycle rows: %+v %v", rows, err)
			}
			if value, err := storetest.ReadServedEventRunIDStorage(retained, observer, "b3c04b72-07f1-4097-a1f0-779955000005"); err == nil || errors.Is(err, sql.ErrNoRows) || value != "" {
				t.Fatalf("retired inspection reached event-run SQL: %s/%v", value, err)
			}
			if cut, err := storetest.ReadReceiverEntityAtEventCut(retained, observer, identity.RunID, entityID.String(), "b3c04b72-07f1-4097-a1f0-779955000005"); err == nil || !reflect.DeepEqual(cut, runfork.RunForkEntityState{}) {
				t.Fatalf("retired inspection snapshot returned historical evidence: %+v %v", cut, err)
			}
			requireReleaseInspectionUnaccepted(t, ctx, observer)
			after, err := readReleaseInspectionStorageSnapshot(ctx, observer)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("opening/reading inspection mutated or lost whole-store evidence: %v", err)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if snapshot, err := readReleaseInspectionStorageSnapshot(cancelled, observer); !errors.Is(err, context.Canceled) || snapshot != nil {
				t.Fatalf("cancelled observer returned partial evidence: %v %v", snapshot, err)
			}
			if rows, err := readReleaseInspectionLifecycle(cancelled, observer, identity.RunID); !errors.Is(err, context.Canceled) || rows != nil {
				t.Fatalf("cancelled inspection returned lifecycle rows: %+v %v", rows, err)
			}
			foreign := identity
			foreign.RunID = "b3c04b72-07f1-4097-a1f0-779955000003"
			if target, err := readReleaseInspectionTarget(ctx, observer, foreign, entityID); err != nil || target.Presence != pipeline.WorkflowTargetPersistenceAbsent {
				t.Fatalf("inspection borrowed a foreign run's target: %+v %v", target, err)
			}
			if err := observer.Close(); err != nil {
				t.Fatal(err)
			}
			if snapshot, err := readReleaseInspectionStorageSnapshot(ctx, observer); err == nil || snapshot != nil {
				t.Fatalf("closed observer returned storage evidence: %v %v", snapshot, err)
			}
			if rows, err := readReleaseInspectionLifecycle(ctx, observer, identity.RunID); err == nil || rows != nil {
				t.Fatalf("closed inspection returned lifecycle rows: %+v %v", rows, err)
			}
			if target, err := readReleaseInspectionTarget(ctx, observer, identity, entityID); err == nil || !reflect.DeepEqual(target, pipeline.WorkflowTargetPersistenceRecord{}) {
				t.Fatalf("closed observer returned target evidence: %+v %v", target, err)
			}
			if cut, err := storetest.ReadReceiverEntityAtEventCut(ctx, observer, identity.RunID, entityID.String(), "b3c04b72-07f1-4097-a1f0-779955000005"); err == nil || !reflect.DeepEqual(cut, runfork.RunForkEntityState{}) {
				t.Fatalf("closed inspection returned historical evidence: %+v %v", cut, err)
			}
			unchanged, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, writer)
			if err != nil || !reflect.DeepEqual(before, unchanged) {
				t.Fatalf("inspection lifetime changed writer storage: %v", err)
			}
		})
	}
}

func requireReleaseInspectionMailboxAndDiagnostics(t *testing.T, writer any, observer storetest.ReleaseProcessReadOnlyInspection, runID string) {
	t.Helper()
	ctx := context.Background()
	want, err := storetest.ReadMailboxCompletionRunEffects(ctx, writer, runID)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	if err := observer.InspectSnapshot(ctx, func(scoped context.Context) error {
		var err error
		got, err = storetest.ReadMailboxCompletionRunEffects(scoped, observer, runID)
		return err
	}); err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("independent unaccepted inspection changed full mailbox rows: %v %v", got, err)
	}
	summary, err := storetest.ReadServedRunDebugSummary(ctx, writer, runID)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := storetest.ReadServedRunDebugSummary(ctx, observer, runID); err != nil || got != summary || len(strings.Split(got, "\n")) != 11 {
		t.Fatalf("independent unaccepted inspection lost diagnostic sections/fields: %s %v", got, err)
	}
	logs, err := storetest.ReadServedRuntimeLogDebugSummary(ctx, observer, runID)
	if err != nil || logs != "runtime_logs: []" || !strings.HasSuffix(summary, logs) {
		t.Fatalf("independent unaccepted log section changed physical output: %s/%v", logs, err)
	}
	if value, err := storetest.ReadServedEventRunIDStorage(ctx, observer, "b3c04b72-07f1-4097-a1f0-779955000005"); !errors.Is(err, sql.ErrNoRows) || value != "" {
		t.Fatalf("unaccepted observer did not reach exact physical event absence: %s/%v", value, err)
	}
	requireReleaseInspectionUnaccepted(t, ctx, observer)
	requireReleaseInspectionWriterStage(t, observer, runID)
}

func requireReleaseInspectionWriterStage(t *testing.T, observer storetest.ReleaseProcessReadOnlyInspection, runID string) {
	t.Helper()
	if err := observer.InspectSnapshot(context.Background(), func(scoped context.Context) error {
		out := storetest.ObserveWriterStage(t, scoped, observer, runID, "b3c04b72-07f1-4097-a1f0-779955000002", "wrong-filter")
		if out.EntityID != "b3c04b72-07f1-4097-a1f0-779955000002" || out.State != "" {
			t.Fatalf("independent header-absence observation lost requested exact entity: %+v", out)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	requireReleaseInspectionUnaccepted(t, context.Background(), observer)
}

func readReleaseInspectionLifecycle(ctx context.Context, observer storetest.ReleaseProcessReadOnlyInspection, runID string) ([]storetest.LifecycleBusinessStorageRow, error) {
	var rows []storetest.LifecycleBusinessStorageRow
	err := observer.InspectSnapshot(ctx, func(scoped context.Context) error {
		var err error
		rows, err = storetest.ReadLifecycleBusinessStorage(scoped, observer, runID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func readReleaseInspectionTarget(ctx context.Context, observer storetest.ReleaseProcessReadOnlyInspection, identity runtimeflowidentity.RunScopedFlowInstance, entityID runtimeidentity.EntityID) (pipeline.WorkflowTargetPersistenceRecord, error) {
	var record pipeline.WorkflowTargetPersistenceRecord
	err := observer.InspectSnapshot(ctx, func(scoped context.Context) error {
		var err error
		record, err = observer.LoadWorkflowTargetPersistence(scoped, identity, entityID)
		return err
	})
	if err != nil {
		return pipeline.WorkflowTargetPersistenceRecord{}, err
	}
	return record, nil
}

func readReleaseInspectionStorageSnapshot(ctx context.Context, observer storetest.ReleaseProcessReadOnlyInspection) (map[string]storetest.SelectedForkStorageTableSnapshot, error) {
	var snapshot map[string]storetest.SelectedForkStorageTableSnapshot
	err := observer.InspectSnapshot(ctx, func(scoped context.Context) error {
		var err error
		snapshot, err = storetest.ReadSelectedForkApplicationStorageSnapshot(scoped, observer)
		return err
	})
	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

func TestReleaseProcessReadOnlyInspectionRefusesInvalidLocations(t *testing.T) {
	for _, input := range [][2]string{{"sqlite", ""}, {"postgres", ""}, {"unknown", "somewhere"}, {"", "somewhere"}, {"sqlite", t.TempDir() + "/missing.db"}} {
		if observer, err := storetest.OpenReleaseProcessReadOnlyInspection(input[0], input[1]); err == nil || observer != nil {
			t.Fatalf("invalid inspection %q returned an owner: %T %v", input, observer, err)
		}
	}
}

func releaseInspectionWriter(t *testing.T, backend string) (any, string, *sql.DB) {
	t.Helper()
	if backend == "sqlite" {
		writer := storetest.StartSQLiteRuntimeStore(t)
		setup, db, err := construction.OpenSQLiteRuntime(writer.Path())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := setup.Close(); err != nil {
				t.Error(err)
			}
		})
		return writer, writer.Path(), db
	}
	dsn := testutil.StartPostgresDSN(t)
	writer, db, err := construction.OpenPostgres(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := writer.Close(); err != nil {
			t.Error(err)
		}
	})
	storetest.BootstrapPostgresRuntimeStore(t, writer)
	return writer, dsn, db
}

func requireReleaseInspectionUnaccepted(t *testing.T, ctx context.Context, observer storetest.ReleaseProcessReadOnlyInspection) {
	t.Helper()
	var err error
	switch selected := observer.(type) {
	case *private.SQLiteRuntimeStore:
		_, err = selected.ListActiveAgentDescriptors(ctx, "b3c04b72-07f1-4097-a1f0-779955000004")
	case *private.PostgresStore:
		_, err = selected.ListActiveAgentDescriptors(ctx, "b3c04b72-07f1-4097-a1f0-779955000004")
	default:
		t.Fatalf("inspection replaced the native owner: %T", observer)
	}
	if err == nil || !strings.Contains(err.Error(), "schema is unaccepted") {
		t.Fatalf("inspection gained runtime admission: %v", err)
	}
}
