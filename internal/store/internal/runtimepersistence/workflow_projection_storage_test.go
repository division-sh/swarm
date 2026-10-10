package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestWorkflowProjectionStorageUsesOriginalSnapshotAndExactPredicatesBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, plan := newWorkflowTargetConstructionFixture(t, backend)
			committed, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, plan)
			if err != nil || !committed.Created || !committed.Acknowledged {
				t.Fatalf("construct actual projection: %+v %v", committed, err)
			}
			run, entity := correlation.RunIDFromContext(f.ctx), plan.Instance.EntityID
			status, name := "json_extract(fi.config, '$.status')", "json_extract(es.fields, '$.name')"
			if backend == "postgres" {
				status, name = "fi.config->>'status'", "es.fields->>'name'"
			}
			var control WorkflowControlProjectionStorage
			var controlFields []byte
			if err := f.db.QueryRowContext(f.ctx, `SELECT es.current_state,es.fields,COALESCE(`+status+`,'') FROM entity_state es JOIN flow_instances fi ON fi.run_id=es.run_id AND fi.instance_path=es.flow_instance WHERE es.run_id=$1 AND es.entity_id=$2`, run, entity).
				Scan(&control.CurrentState, &controlFields, &control.ControlStatus); err != nil {
				t.Fatal(err)
			}
			control.Fields = controlFields
			var duplicate WorkflowDuplicateProjectionStorage
			var duplicateFields []byte
			if err := f.db.QueryRowContext(f.ctx, `SELECT es.revision,COALESCE(`+name+`,''),es.fields FROM entity_state es JOIN flow_instances fi ON fi.run_id=es.run_id AND fi.instance_path=es.flow_instance WHERE es.run_id=$1 AND es.entity_id=$2`, run, entity).
				Scan(&duplicate.Revision, &duplicate.FieldName, &duplicateFields); err != nil {
				t.Fatal(err)
			}
			duplicate.Fields = duplicateFields
			probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			if actual, err := ReadWorkflowControlProjectionStorageForTest(f.ctx, f.store, run, entity); err != nil || !reflect.DeepEqual(actual, control) {
				t.Fatalf("control physical evidence changed: got=%+v want=%+v err=%v", actual, control, err)
			}
			if actual, err := ReadWorkflowDuplicateProjectionStorageForTest(f.ctx, f.store, run, entity); err != nil || !reflect.DeepEqual(actual, duplicate) {
				t.Fatalf("duplicate physical evidence changed: got=%+v want=%+v err=%v", actual, duplicate, err)
			}
			if counts := probe.Snapshot(); counts.Total.Begun != 2 || counts.Total.ReadCommits != 2 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("projection witnesses escaped original read coordinator: %+v", counts)
			}
			for _, key := range [][2]string{{uuid.NewString(), entity}, {run, uuid.NewString()}} {
				assertWorkflowProjectionStorageRefused(t, f.ctx, f.store, key[0], key[1])
			}
			cancelled, cancel := context.WithCancel(f.ctx)
			cancel()
			if got, err := ReadWorkflowControlProjectionStorageForTest(cancelled, f.store, run, entity); !errors.Is(err, context.Canceled) || !reflect.DeepEqual(got, WorkflowControlProjectionStorage{}) {
				t.Fatalf("cancelled control retained evidence: %+v %v", got, err)
			}
			if got, err := ReadWorkflowDuplicateProjectionStorageForTest(cancelled, f.store, run, entity); !errors.Is(err, context.Canceled) || !reflect.DeepEqual(got, WorkflowDuplicateProjectionStorage{}) {
				t.Fatalf("cancelled duplicate retained evidence: %+v %v", got, err)
			}
		})
	}
}

func assertWorkflowProjectionStorageRefused(t *testing.T, ctx context.Context, selected any, run, entity string) {
	t.Helper()
	if got, err := ReadWorkflowControlProjectionStorageForTest(ctx, selected, run, entity); err == nil || !reflect.DeepEqual(got, WorkflowControlProjectionStorage{}) {
		t.Fatalf("invalid control owner supplied partial evidence: %+v %v", got, err)
	}
	if got, err := ReadWorkflowDuplicateProjectionStorageForTest(ctx, selected, run, entity); err == nil || !reflect.DeepEqual(got, WorkflowDuplicateProjectionStorage{}) {
		t.Fatalf("invalid duplicate owner supplied partial evidence: %+v %v", got, err)
	}
}

func TestWorkflowProjectionStorageRefusesInvalidAndClosedOwnersBothStores(t *testing.T) {
	run, entity := uuid.NewString(), uuid.NewString()
	for _, selected := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		assertWorkflowProjectionStorageRefused(t, context.Background(), selected, run, entity)
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			f := backend.open(t)
			for _, key := range []string{"", "invalid", uuid.Nil.String()} {
				assertWorkflowProjectionStorageRefused(t, testAuthorActivityContext(), f.store, key, entity)
				assertWorkflowProjectionStorageRefused(t, testAuthorActivityContext(), f.store, run, key)
			}
			if err := f.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			assertWorkflowProjectionStorageRefused(t, testAuthorActivityContext(), f.store, run, entity)
		})
	}
}
