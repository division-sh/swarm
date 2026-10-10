package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func workflowFieldRowShadow(t *testing.T, f receiverConfigActivationFixture, run, entity string) (string, [4]any) {
	t.Helper()
	var state string
	var raw [4][]byte
	if err := f.db.QueryRowContext(f.ctx, `SELECT current_state,gates,bookkeeping,accumulator,fields FROM entity_state WHERE run_id=$1 AND entity_id=$2`, run, entity).Scan(&state, &raw[0], &raw[1], &raw[2], &raw[3]); err != nil {
		t.Fatal(err)
	}
	var fields [4]any
	for i := range raw {
		if err := json.Unmarshal(raw[i], &fields[i]); err != nil {
			t.Fatal(err)
		}
	}
	return state, fields
}

func TestWorkflowProjectionObsoleteFieldRowsKeepExactRunScopeAndNativeTransactionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, plan := newWorkflowTargetConstructionFixture(t, backend)
			if out, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, plan); err != nil || !out.Created || !out.Acknowledged {
				t.Fatalf("actual construction: %+v %v", out, err)
			}
			run, entity := correlation.RunIDFromContext(f.ctx), plan.Instance.EntityID
			beforeState, before := workflowFieldRowShadow(t, f, run, entity)
			if changed, err := SetWorkflowProjectionObsoleteFieldRowsForTest(f.ctx, f.store, uuid.NewString()); err == nil || changed != 0 {
				t.Fatalf("absent-run fault: rows=%d err=%v", changed, err)
			}
			ctx, cancel := context.WithCancel(f.ctx)
			cancel()
			if changed, err := SetWorkflowProjectionObsoleteFieldRowsForTest(ctx, f.store, run); !errors.Is(err, context.Canceled) || changed != 0 {
				t.Fatalf("cancelled shadow fault: rows=%d err=%v", changed, err)
			}
			if state, after := workflowFieldRowShadow(t, f, run, entity); state != beforeState || !reflect.DeepEqual(after, before) {
				t.Fatal("refused shadow fault changed fields")
			}
			probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			if changed, err := SetWorkflowProjectionObsoleteFieldRowsForTest(f.ctx, f.store, run); err != nil || changed != 1 {
				t.Fatalf("exact shadow fault: rows=%d err=%v", changed, err)
			}
			if counts := probe.Snapshot(); counts.Total.Begun != 1 || counts.Total.WriteCommits != 1 || counts.Total.ReadCommits != 0 || counts.Active != 0 {
				t.Fatalf("shadow fault escaped exact native coordinator: %+v", counts)
			}
			state, after := workflowFieldRowShadow(t, f, run, entity)
			empty := map[string]any{}
			want := [4]any{empty, empty, empty, before[3]}
			if state != "obsolete" || !reflect.DeepEqual(after, want) {
				t.Fatalf("shadow fault payload or untouched fields changed: state=%s got=%+v want=%+v", state, after, want)
			}
		})
	}
}

func TestWorkflowProjectionObsoleteFieldRowsRefuseInvalidAndClosedOwnersBothStores(t *testing.T) {
	run := uuid.NewString()
	for _, owner := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		if changed, err := SetWorkflowProjectionObsoleteFieldRowsForTest(context.Background(), owner, run); err == nil || changed != 0 {
			t.Fatalf("invalid shadow owner: %T rows=%d err=%v", owner, changed, err)
		}
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			f := backend.open(t)
			for _, run := range []string{"", "invalid", uuid.Nil.String()} {
				if changed, err := SetWorkflowProjectionObsoleteFieldRowsForTest(testAuthorActivityContext(), f.store, run); err == nil || changed != 0 {
					t.Fatalf("invalid shadow run: rows=%d err=%v", changed, err)
				}
			}
			if err := f.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			if changed, err := SetWorkflowProjectionObsoleteFieldRowsForTest(testAuthorActivityContext(), f.store, run); err == nil || changed != 0 {
				t.Fatalf("closed shadow owner: rows=%d err=%v", changed, err)
			}
		})
	}
}

func TestWorkflowProjectionObsoleteFieldRowsRespectOriginalSQLiteWriterAdmission(t *testing.T) {
	verifyWorkflowProjectionFaultSQLiteAdmission(t, projectionShapeFaultCase{
		name: "ObsoleteFieldRows",
		apply: func(ctx context.Context, selected any, run, _ string) (int64, error) {
			return SetWorkflowProjectionObsoleteFieldRowsForTest(ctx, selected, run)
		},
	})
}
