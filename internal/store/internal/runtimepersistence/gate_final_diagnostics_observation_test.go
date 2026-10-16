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
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestGateFinalDiagnosticsRetainExactOriginalPhysicalReadsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, record := constructWorkflowMutationFixture(t, backend, "review", time.Now().UTC().Truncate(time.Microsecond))
			run := record.Identity.RunID
			event := eventtest.ExistingRunRootIngress(uuid.NewString(), "review/construct.requested", "fixture", "", []byte(`{}`), 0, run, events.EventEnvelope{}, time.Now().UTC())
			if err := commitSemanticEventFixtureWithRoutes(f.ctx, f.store.(deliveryFixtureStore), event, nil); err != nil {
				t.Fatal(err)
			}
			current, found, err := f.store.(pipeline.WorkflowPersistenceOwner).LoadWorkflowInstance(f.ctx, record.Identity)
			if err != nil || !found {
				t.Fatalf("load actual diagnostic mutation source: found=%v %v", found, err)
			}
			record.CurrentState, record.EnteredStageAt = current.CurrentState, current.EnteredStageAt
			record.Fields = []byte(`{"receiver_key":"receiver","account_id":"changed"}`)
			if _, err := f.store.(pipeline.WorkflowEngineMutationOwner).CommitWorkflowEngineMutation(correlation.WithInboundEvent(f.ctx, event), pipeline.WorkflowEngineMutationCommand{State: record}); err != nil {
				t.Fatal(err)
			}
			want, referenceErr := gateDuplicateMutationReference(t, f, event.ID())
			if referenceErr != nil || len(want) == 0 {
				t.Fatalf("real existing-value mutation lacks positive strict diagnostic text: %+v %v", want, referenceErr)
			}
			var fields []byte
			rootErr := f.db.QueryRowContext(f.ctx, `SELECT fields FROM entity_state WHERE run_id=$1 AND entity_id=$2`, run, run).Scan(&fields)
			before := competingRevisionPairSnapshot(t, f)
			probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			if got, err := ReadGateDuplicateMutationDiagnosticsForTest(f.ctx, f.store, event.ID()); err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("mutation diagnostics changed event-only scope, native text or rows: %+v/%+v %v", got, want, err)
			}
			gotFields, readErr := ReadGateRunRootFieldsStorageForTest(f.ctx, f.store, run)
			if (readErr != nil) != (rootErr != nil) || !reflect.DeepEqual(gotFields, fields) {
				t.Fatalf("root diagnostics borrowed keyed child fields or changed strict absence: %s/%s %v/%v", gotFields, fields, readErr, rootErr)
			}
			commits := uint64(1)
			if rootErr == nil {
				commits++
			}
			if counts := probe.Snapshot(); counts.Active != 0 || counts.Total.Begun != 2 || counts.Total.ReadCommits != commits || counts.Total.WriteCommits != 0 {
				t.Fatalf("diagnostics merged independent cuts or escaped the original owner: %+v", counts)
			}
			for _, ctx := range []context.Context{f.ctx, correlation.WithRunID(f.ctx, uuid.NewString())} {
				if got, err := ReadGateDuplicateMutationDiagnosticsForTest(ctx, f.store, event.ID()); err != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("event-only diagnostic acquired an incidental context-run filter: %+v %v", got, err)
				}
			}
			if got, err := ReadGateDuplicateMutationDiagnosticsForTest(f.ctx, f.store, uuid.NewString()); err != nil || got != nil {
				t.Fatalf("missing event borrowed unrelated construction/business history: %+v %v", got, err)
			}
			if got, err := ReadGateRunRootFieldsStorageForTest(f.ctx, f.store, uuid.NewString()); !errors.Is(err, sql.ErrNoRows) || got != nil {
				t.Fatalf("foreign root borrowed current field bytes: %s %v", got, err)
			}
			restore()
			if !reflect.DeepEqual(before, competingRevisionPairSnapshot(t, f)) {
				t.Fatal("diagnostics changed complete data/history/ownership/clocks")
			}
			event = eventtest.ExistingRunRootIngress(uuid.NewString(), "review/construct.requested", "fixture", "", []byte(`{}`), 0, run, events.EventEnvelope{}, time.Now().UTC())
			if err := commitSemanticEventFixtureWithRoutes(f.ctx, f.store.(deliveryFixtureStore), event, nil); err != nil {
				t.Fatal(err)
			}
			record.ExpectedRevision++
			record.Fields = []byte(`{"receiver_key":"receiver","account_id":"changed","handled":true}`)
			record.UpdatedAt = record.UpdatedAt.Add(time.Second)
			if _, err := f.store.(pipeline.WorkflowEngineMutationOwner).CommitWorkflowEngineMutation(correlation.WithInboundEvent(f.ctx, event), pipeline.WorkflowEngineMutationCommand{State: record}); err != nil {
				t.Fatal(err)
			}
			if rows, err := gateDuplicateMutationReference(t, f, event.ID()); err == nil || rows != nil {
				t.Fatalf("new optional field did not establish original strict SQL-NULL refusal: %+v %v", rows, err)
			}
			before = competingRevisionPairSnapshot(t, f)
			if rows, err := ReadGateDuplicateMutationDiagnosticsForTest(f.ctx, f.store, event.ID()); err == nil || rows != nil {
				t.Fatalf("diagnostic normalized NULL or exposed partial rows after a scan failure: %+v %v", rows, err)
			}
			if !reflect.DeepEqual(before, competingRevisionPairSnapshot(t, f)) {
				t.Fatal("strict NULL refusal changed persisted history")
			}
		})
	}
}

func gateDuplicateMutationReference(t *testing.T, f receiverConfigActivationFixture, event string) ([]GateDuplicateMutationDiagnosticStorage, error) {
	t.Helper()
	rows, err := f.db.QueryContext(f.ctx, `SELECT domain,path,CAST(old_value AS TEXT),CAST(new_value AS TEXT) FROM entity_mutations WHERE caused_by_event=$1`, event)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []GateDuplicateMutationDiagnosticStorage
	for rows.Next() {
		var row GateDuplicateMutationDiagnosticStorage
		if err := rows.Scan(&row.Domain, &row.Path, &row.OldValue, &row.NewValue); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, errors.Join(rows.Err(), rows.Close())
}

func TestGateFinalDiagnosticsRefuseRawInvalidCancelledAndClosedOwnersBothStores(t *testing.T) {
	refuse := func(ctx context.Context, owner any, id string, cause error) {
		matches := func(err error) bool { return err != nil && (cause == nil || errors.Is(err, cause)) }
		if got, err := ReadGateDuplicateMutationDiagnosticsForTest(ctx, owner, id); !matches(err) || got != nil {
			t.Fatalf("unavailable diagnostic exposed mutation rows: %+v %v", got, err)
		}
		if got, err := ReadGateRunRootFieldsStorageForTest(ctx, owner, id); !matches(err) || got != nil {
			t.Fatalf("unavailable diagnostic exposed root fields: %s %v", got, err)
		}
	}
	for _, owner := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		refuse(context.Background(), owner, uuid.NewString(), nil)
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, record := constructWorkflowMutationFixture(t, backend, "review", time.Now().UTC())
			refuse(f.ctx, f.store, "invalid", nil)
			cancelled, cancel := context.WithCancel(f.ctx)
			cancel()
			refuse(cancelled, f.store, record.Identity.RunID, context.Canceled)
			if err := f.manager.Shutdown(); err != nil {
				t.Fatal(err)
			}
			if err := f.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			refuse(f.ctx, f.store, record.Identity.RunID, nil)
		})
	}
}
