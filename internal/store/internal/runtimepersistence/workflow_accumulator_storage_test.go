package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestWorkflowAccumulatorStorageKeepsRawBytesRevisionAndIndependentReadBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, state := newCompetingRevisionPairFixture(t, backend)
			runID, path := state.Identity.RunID, state.Identity.Route.InstancePath
			var want WorkflowAccumulatorStorage
			if err := f.db.QueryRowContext(f.ctx, `SELECT revision, fields, accumulator FROM entity_state WHERE run_id=$1 AND flow_instance=$2`, runID, path).
				Scan(&want.Revision, &want.Fields, &want.Accumulator); err != nil {
				t.Fatal(err)
			}
			if want.Revision == 0 || len(want.Fields) == 0 || len(want.Accumulator) == 0 {
				t.Fatalf("native construction did not provide positive revision and JSON byte witnesses: %+v", want)
			}
			before := competingRevisionPairSnapshot(t, f)
			probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			got, err := ReadWorkflowAccumulatorStorageForTest(f.ctx, f.store, runID, path)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("raw JSON bytes or persisted revision changed: got=%+v want=%+v err=%v", got, want, err)
			}
			if counts := probe.Snapshot(); counts.Active != 0 || counts.Total.Begun != 1 || counts.Total.ReadCommits != 1 || counts.Total.WriteCommits != 0 {
				t.Fatalf("accumulator read bypassed the exact coordinator or combined independent evidence: %+v", counts)
			}
			for _, scope := range [][2]string{{uuid.NewString(), path}, {runID, "foreign/path"}} {
				if got, err := ReadWorkflowAccumulatorStorageForTest(f.ctx, f.store, scope[0], scope[1]); !errors.Is(err, sql.ErrNoRows) || !reflect.DeepEqual(got, WorkflowAccumulatorStorage{}) {
					t.Fatalf("foreign run/path borrowed accumulator evidence or returned partial data: %+v %v", got, err)
				}
			}
			cancelled, cancel := context.WithCancel(f.ctx)
			cancel()
			if got, err := ReadWorkflowAccumulatorStorageForTest(cancelled, f.store, runID, path); !errors.Is(err, context.Canceled) || !reflect.DeepEqual(got, WorkflowAccumulatorStorage{}) {
				t.Fatalf("cancelled accumulator read returned evidence: %+v %v", got, err)
			}
			if !reflect.DeepEqual(before, competingRevisionPairSnapshot(t, f)) {
				t.Fatal("accumulator observation changed data, history, leases or clocks")
			}
		})
	}
}

func TestWorkflowAccumulatorStorageRefusesRawAndClosedOwners(t *testing.T) {
	for _, selected := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		if got, err := ReadWorkflowAccumulatorStorageForTest(context.Background(), selected, uuid.NewString(), "review"); err == nil || !reflect.DeepEqual(got, WorkflowAccumulatorStorage{}) {
			t.Fatalf("accumulator storage accepted non-native owner %T: %+v %v", selected, got, err)
		}
	}
	f, state := newCompetingRevisionPairFixture(t, "sqlite")
	if err := f.manager.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if err := f.store.(*SQLiteRuntimeStore).Close(); err != nil {
		t.Fatal(err)
	}
	if got, err := ReadWorkflowAccumulatorStorageForTest(f.ctx, f.store, state.Identity.RunID, state.Identity.Route.InstancePath); err == nil || !reflect.DeepEqual(got, WorkflowAccumulatorStorage{}) {
		t.Fatalf("closed accumulator owner returned evidence: %+v %v", got, err)
	}
}
