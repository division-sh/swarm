package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestWorkflowProjectionMissingHeaderKeepsExactScopeAndNativeTransactionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, plan := newWorkflowTargetConstructionFixture(t, backend)
			result, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, plan)
			if err != nil || !result.Created || !result.Acknowledged {
				t.Fatalf("construct actual target: %+v %v", result, err)
			}
			run, path := correlation.RunIDFromContext(f.ctx), plan.Instance.StorageRef
			owner := flowidentity.RunScopedFlowInstance{RunID: run, Route: plan.Identity.Route()}
			reader := f.store.(pipeline.WorkflowTargetPersistenceReader)
			before, err := reader.LoadWorkflowTargetPersistence(f.ctx, owner, identity.NormalizeEntityID(plan.Instance.EntityID))
			if err != nil || before.Presence != pipeline.WorkflowTargetPersistenceComplete {
				t.Fatalf("pre-fault target not complete: %+v %v", before, err)
			}
			for _, coordinate := range [][2]string{{uuid.NewString(), path}, {run, uuid.NewString()}} {
				if changed, err := RemoveWorkflowProjectionHeaderForTest(f.ctx, f.store, coordinate[0], coordinate[1]); err == nil || changed != 0 {
					t.Fatalf("missing-header fault changed absent coordinate: rows=%d err=%v", changed, err)
				}
			}
			cancelled, cancel := context.WithCancel(f.ctx)
			cancel()
			if changed, err := RemoveWorkflowProjectionHeaderForTest(cancelled, f.store, run, path); !errors.Is(err, context.Canceled) || changed != 0 {
				t.Fatalf("cancelled header fault succeeded: rows=%d err=%v", changed, err)
			}
			unchanged, err := reader.LoadWorkflowTargetPersistence(f.ctx, owner, identity.NormalizeEntityID(plan.Instance.EntityID))
			if err != nil || !reflect.DeepEqual(before, unchanged) {
				t.Fatalf("refused fault changed prestate: before=%+v after=%+v err=%v", before, unchanged, err)
			}
			probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			if changed, err := RemoveWorkflowProjectionHeaderForTest(f.ctx, f.store, run, path); err != nil || changed != 1 {
				t.Fatalf("remove exact native header: rows=%d err=%v", changed, err)
			}
			if counts := probe.Snapshot(); counts.Total.Begun != 1 || counts.Total.WriteCommits != 1 || counts.Total.ReadCommits != 0 || counts.Active != 0 {
				t.Fatalf("header fault escaped original writer: %+v", counts)
			}
			after, err := reader.LoadWorkflowTargetPersistence(f.ctx, owner, identity.NormalizeEntityID(plan.Instance.EntityID))
			if err != nil || after.Presence != pipeline.WorkflowTargetPersistenceStateOnly || !reflect.DeepEqual(before.State, after.State) || !reflect.DeepEqual(after.Lifecycle, pipeline.WorkflowLifecycleCompanionPersistenceRecord{}) {
				t.Fatalf("header-only cut changed field row: before=%+v after=%+v err=%v", before, after, err)
			}
			if changed, err := RemoveWorkflowProjectionHeaderForTest(f.ctx, f.store, run, path); err == nil || changed != 0 {
				t.Fatalf("missing-header replay invented success: rows=%d err=%v", changed, err)
			}
		})
	}
}

func TestWorkflowProjectionMissingHeaderRefusesInvalidAndClosedOwnersBothStores(t *testing.T) {
	run, path := uuid.NewString(), "review/instance"
	for _, selected := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		if changed, err := RemoveWorkflowProjectionHeaderForTest(context.Background(), selected, run, path); err == nil || changed != 0 {
			t.Fatalf("invalid header fault owner accepted: %T rows=%d err=%v", selected, changed, err)
		}
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			f := backend.open(t)
			for _, coordinate := range [][2]string{{"", path}, {"invalid", path}, {uuid.Nil.String(), path}, {run, ""}, {run, " spaced "}} {
				if changed, err := RemoveWorkflowProjectionHeaderForTest(testAuthorActivityContext(), f.store, coordinate[0], coordinate[1]); err == nil || changed != 0 {
					t.Fatalf("invalid header fault coordinate accepted: rows=%d err=%v", changed, err)
				}
			}
			if err := f.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			if changed, err := RemoveWorkflowProjectionHeaderForTest(testAuthorActivityContext(), f.store, run, path); err == nil || changed != 0 {
				t.Fatalf("closed header fault owner accepted: rows=%d err=%v", changed, err)
			}
		})
	}
}

func TestWorkflowProjectionMissingHeaderRespectsOriginalSQLiteWriterAdmission(t *testing.T) {
	verifyWorkflowProjectionFaultSQLiteAdmission(t, projectionShapeFaultCase{
		name: "MissingHeader", apply: RemoveWorkflowProjectionHeaderForTest,
	})
}
