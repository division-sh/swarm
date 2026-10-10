package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
)

func TestWorkflowEnginePhysicalCountsRetainWholeStorageAndOriginalReadOwnerBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, plan := newWorkflowTargetConstructionFixture(t, backend)
			if out, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, plan); err != nil || !out.Created || !out.Acknowledged {
				t.Fatalf("native count witness construction: %+v %v", out, err)
			}
			var expected WorkflowEnginePhysicalCounts
			for _, row := range []struct {
				query string
				value *int64
			}{
				{`SELECT COUNT(*) FROM entity_state`, &expected.EntityStates},
				{`SELECT COUNT(*) FROM flow_instances`, &expected.ConstructedHeaders},
				{`SELECT COUNT(*) FROM entity_mutations`, &expected.MutationJournal},
			} {
				if err := f.db.QueryRowContext(f.ctx, row.query).Scan(row.value); err != nil {
					t.Fatal(err)
				}
			}
			if expected.EntityStates < 1 || expected.ConstructedHeaders < 1 || expected.MutationJournal < 1 {
				t.Fatalf("physical count witness is vacuous: %+v", expected)
			}
			probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			if got, err := ReadWorkflowEnginePhysicalCountsForTest(f.ctx, f.store); err != nil || got != expected {
				t.Fatalf("physical scope changed: got=%+v want=%+v err=%v", got, expected, err)
			}
			if counts := probe.Snapshot(); counts.Total.Begun != 1 || counts.Total.ReadCommits != 1 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("count escaped original read transaction: %+v", counts)
			}
			ctx, cancel := context.WithCancel(f.ctx)
			cancel()
			if got, err := ReadWorkflowEnginePhysicalCountsForTest(ctx, f.store); !errors.Is(err, context.Canceled) || got != (WorkflowEnginePhysicalCounts{}) {
				t.Fatalf("cancelled count supplied partial facts: %+v %v", got, err)
			}
			if _, err := f.db.ExecContext(f.ctx, `ALTER TABLE entity_mutations RENAME TO unavailable_mutation_count`); err != nil {
				t.Fatal(err)
			}
			if got, err := ReadWorkflowEnginePhysicalCountsForTest(f.ctx, f.store); err == nil || got != (WorkflowEnginePhysicalCounts{}) {
				t.Fatalf("late count error supplied partial facts: %+v %v", got, err)
			}
		})
	}
}

func TestWorkflowEnginePhysicalCountsRefuseRawInvalidAndClosedOwnersBothStores(t *testing.T) {
	for _, selected := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		if got, err := ReadWorkflowEnginePhysicalCountsForTest(context.Background(), selected); err == nil || got != (WorkflowEnginePhysicalCounts{}) {
			t.Fatalf("invalid count owner supplied facts: %T %+v %v", selected, got, err)
		}
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			f := backend.open(t)
			if err := f.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			if got, err := ReadWorkflowEnginePhysicalCountsForTest(testAuthorActivityContext(), f.store); err == nil || got != (WorkflowEnginePhysicalCounts{}) {
				t.Fatalf("closed count supplied facts: %+v %v", got, err)
			}
		})
	}
}
