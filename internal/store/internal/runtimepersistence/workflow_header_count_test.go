package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
)

func TestWorkflowHeaderCountUsesOriginalReadCoordinatorAndWholePhysicalScopeBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, plan := newWorkflowTargetConstructionFixture(t, backend)
			if out, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, plan); err != nil || !out.Created || !out.Acknowledged {
				t.Fatalf("lawful header construction: %+v %v", out, err)
			}
			var expected int64
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM flow_instances`).Scan(&expected); err != nil {
				t.Fatal(err)
			}
			if expected < 1 {
				t.Fatal("physical header census cannot be vacuous")
			}
			probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			if count, err := CountWorkflowInstanceHeadersForTest(f.ctx, f.store); err != nil || count != expected {
				t.Fatalf("whole physical header scope changed: got=%d want=%d err=%v", count, expected, err)
			}
			if counts := probe.Snapshot(); counts.Total.Begun != 1 || counts.Total.ReadCommits != 1 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("header count escaped original native read coordinator: %+v", counts)
			}
			ctx, cancel := context.WithCancel(f.ctx)
			cancel()
			if count, err := CountWorkflowInstanceHeadersForTest(ctx, f.store); !errors.Is(err, context.Canceled) || count != 0 {
				t.Fatalf("cancelled count supplied evidence: count=%d err=%v", count, err)
			}
		})
	}
}

func TestWorkflowHeaderCountRefusesInvalidAndClosedNativeOwnersBothStores(t *testing.T) {
	for _, selected := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		if count, err := CountWorkflowInstanceHeadersForTest(context.Background(), selected); err == nil || count != 0 {
			t.Fatalf("invalid header count owner: %T count=%d err=%v", selected, count, err)
		}
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			f := backend.open(t)
			if err := f.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			if count, err := CountWorkflowInstanceHeadersForTest(testAuthorActivityContext(), f.store); err == nil || count != 0 {
				t.Fatalf("closed count supplied evidence: count=%d err=%v", count, err)
			}
		})
	}
}
