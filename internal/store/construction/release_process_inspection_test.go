package construction_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
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
				(run_id,entity_id,flow_instance,entity_type,fields,created_at,updated_at)
				VALUES ($1,$2,$3,'inspection_entity',$4,$5,$5)`,
				identity.RunID, entityID.String(), identity.Route.InstancePath,
				`{"quantity":9007199254740993,"optional":null}`, time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			hostileIdentity, hostileEntityID := identity, entityID
			identity.Route = runtimeflowidentity.DeriveRoute("inspect", "valid")
			entityID = runtimeidentity.NormalizeEntityID("b3c04b72-07f1-4097-a1f0-779955000006")
			if _, err := db.ExecContext(ctx, `INSERT INTO entity_state
				(run_id,entity_id,flow_instance,entity_type,fields,created_at,updated_at)
				VALUES ($1,$2,$3,'inspection_entity',$4,$5,$5)`, identity.RunID, entityID.String(), identity.Route.InstancePath,
				`{"quantity":7,"optional":null}`, time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			before, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, writer)
			if err != nil {
				t.Fatal(err)
			}
			requireReleaseInspectionPhysicalNumber(t, before, hostileEntityID.String())
			hostile, hostileErr := writer.(pipeline.WorkflowTargetPersistenceReader).LoadWorkflowTargetPersistence(ctx, hostileIdentity, hostileEntityID)
			if hostileErr == nil || !strings.Contains(hostileErr.Error(), "safe range") || !reflect.DeepEqual(hostile, pipeline.WorkflowTargetPersistenceRecord{}) {
				t.Fatalf("writer admitted unsafe semantic number: %+v %v", hostile, hostileErr)
			}
			want, err := writer.(pipeline.WorkflowTargetPersistenceReader).LoadWorkflowTargetPersistence(ctx, identity, entityID)
			if err != nil || want.Presence != pipeline.WorkflowTargetPersistenceStateOnly || !bytes.Contains(want.State.Fields, []byte(`"quantity"`)) {
				t.Fatalf("native writer evidence: %+v %v", want, err)
			}
			var validFields map[string]any
			decoder := json.NewDecoder(bytes.NewReader(want.State.Fields))
			decoder.UseNumber()
			if err := decoder.Decode(&validFields); err != nil || !reflect.DeepEqual(validFields, map[string]any{"quantity": json.Number("7"), "optional": nil}) {
				t.Fatalf("valid semantic control changed: %#v %v", validFields, err)
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
			hostile, inspectionErr := readReleaseInspectionTarget(ctx, observer, hostileIdentity, hostileEntityID)
			if inspectionErr == nil || inspectionErr.Error() != hostileErr.Error() || !reflect.DeepEqual(hostile, pipeline.WorkflowTargetPersistenceRecord{}) {
				t.Fatalf("observer semantic refusal differs from writer: %+v %v, writer=%v", hostile, inspectionErr, hostileErr)
			}
			got, err := readReleaseInspectionTarget(ctx, observer, identity, entityID)
			if err != nil || !reflect.DeepEqual(want, got) {
				t.Fatalf("independent inspection lost canonical target evidence: want=%+v got=%+v err=%v", want, got, err)
			}
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
			if cut, err := storetest.ReadReceiverEntityAtEventCut(retained, observer, identity.RunID, entityID.String(), "b3c04b72-07f1-4097-a1f0-779955000005"); err == nil || !reflect.DeepEqual(cut, runfork.RunForkEntityState{}) {
				t.Fatalf("retired inspection snapshot returned historical evidence: %+v %v", cut, err)
			}
			requireReleaseInspectionUnaccepted(t, ctx, observer)
			after, err := readReleaseInspectionStorageSnapshot(ctx, observer)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("opening/reading inspection mutated or lost whole-store evidence: %v", err)
			}
			requireReleaseInspectionPhysicalNumber(t, after, hostileEntityID.String())
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if snapshot, err := readReleaseInspectionStorageSnapshot(cancelled, observer); !errors.Is(err, context.Canceled) || snapshot != nil {
				t.Fatalf("cancelled observer returned partial evidence: %v %v", snapshot, err)
			}
			if target, err := readReleaseInspectionTarget(cancelled, observer, identity, entityID); !errors.Is(err, context.Canceled) || !reflect.DeepEqual(target, pipeline.WorkflowTargetPersistenceRecord{}) {
				t.Fatalf("cancelled observer returned valid target evidence: %+v %v", target, err)
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
			requireReleaseInspectionPhysicalNumber(t, unchanged, hostileEntityID.String())
		})
	}
}

func requireReleaseInspectionPhysicalNumber(t *testing.T, snapshot map[string]storetest.SelectedForkStorageTableSnapshot, entityID string) {
	t.Helper()
	rows, err := private.DecodeSelectedForkSnapshotRowsForTest(snapshot["entity_state"])
	if err != nil {
		t.Fatal(err)
	}
	found := 0
	for _, row := range rows {
		id, err := row["entity_id"].Text()
		if err != nil {
			t.Fatal(err)
		}
		if id != entityID {
			continue
		}
		found++
		raw, err := row["fields"].Text()
		if err != nil {
			t.Fatal(err)
		}
		decoder := json.NewDecoder(strings.NewReader(raw))
		decoder.UseNumber()
		var fields map[string]any
		if err := decoder.Decode(&fields); err != nil {
			t.Fatal(err)
		}
		number, ok := fields["quantity"].(json.Number)
		optional, present := fields["optional"]
		if !ok || number.String() != "9007199254740993" || !present || optional != nil || len(fields) != 2 {
			t.Fatalf("physical number/null evidence changed: %s", raw)
		}
	}
	if found != 1 {
		t.Fatalf("physical hostile row count = %d, want 1", found)
	}
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
