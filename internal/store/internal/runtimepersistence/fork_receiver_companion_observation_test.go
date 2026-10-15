package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestForkReceiverCompanionStoragePreservesWholeRowsAndSeparateCutsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, map[string]string{
				"schema.yaml":          "name: companion-witness\n",
				"review/schema.yaml":   "name: review\nstages:\n  queued: {}\n",
				"review/entities.yaml": "test_entity:\n  literal: {type: text, initial: '${x}'}\n",
			}, nil)
			runID := correlation.RunIDFromContext(f.ctx)
			req := sqliteFlowActivationRequest(f.bundle, ".", runID, "", runID)
			req.Instance = flowidentity.Stored(req.ContractBundle, ".", runID, runID, runID, "")
			plan, err := f.manager.PrepareFlowInstanceActivation(f.ctx, req)
			if err != nil || len(plan.Children) != 1 {
				t.Fatalf("native companion setup: %+v %v", plan, err)
			}
			if _, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, plan); err != nil {
				t.Fatal(err)
			}
			child := plan.Children[0]
			probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			got, err := ReadForkReceiverCompanionStorageForTest(f.ctx, f.store, runID, child.Identity.InstancePath)
			if err != nil {
				t.Fatal(err)
			}
			if counts := probe.Snapshot(); counts.Total.Begun != 2 || counts.Total.ReadCommits != 2 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("companion observation changed original per-table cuts: %+v", counts)
			}
			db := f.db
			want := originalForkReceiverCompanionRows(t, db, runID, child.Identity.InstancePath)
			if !reflect.DeepEqual(got, want) || len(got["flow_instances"]) != 1 || len(got["flow_instance_runtime_readiness"]) != 1 {
				t.Fatalf("full companion columns/row values/NULL/text/clocks differ: got=%v want=%v", got, want)
			}
			for _, scope := range []struct {
				run   string
				paths []string
			}{
				{runID, nil}, {runID, []string{child.Identity.InstancePath, "ignored-original-tail"}},
				{uuid.NewString(), nil}, {runID, []string{"foreign-path"}},
			} {
				got, err := ReadForkReceiverCompanionStorageForTest(f.ctx, f.store, scope.run, scope.paths...)
				if err != nil || !reflect.DeepEqual(got, originalForkReceiverCompanionRows(t, db, scope.run, scope.paths...)) {
					t.Fatalf("run/path/empty/multiplicity scope changed: %v %v", got, err)
				}
			}
			if probe.Snapshot().Total.WriteCommits != 0 {
				t.Fatal("companion witness mutated history")
			}
		})
	}
}

func originalForkReceiverCompanionRows(t *testing.T, db *sql.DB, runID string, paths ...string) map[string][]string {
	t.Helper()
	out := map[string][]string{}
	for _, table := range []string{"flow_instances", "flow_instance_runtime_readiness"} {
		query, args := `SELECT * FROM `+table+` WHERE run_id=$1`, []any{runID}
		if len(paths) != 0 {
			query += ` AND instance_path=$2`
			args = append(args, paths[0])
		}
		rows, err := db.Query(query, args...)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := json.Marshal(columns)
		if err != nil {
			t.Fatal(err)
		}
		out[table+"/columns"], out[table] = []string{string(encoded)}, []string{}
		for rows.Next() {
			values, pointers := make([]any, len(columns)), make([]any, len(columns))
			for i := range values {
				pointers[i] = &values[i]
			}
			if err := rows.Scan(pointers...); err != nil {
				t.Fatal(err)
			}
			for i, value := range values {
				if raw, ok := value.([]byte); ok {
					values[i] = string(raw)
				}
			}
			encoded, err := json.Marshal(values)
			if err != nil {
				t.Fatal(err)
			}
			out[table] = append(out[table], string(encoded))
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
		sort.Strings(out[table])
	}
	return out
}

func TestForkReceiverCompanionStorageRefusesRawCancelledClosedAndPartialReadsBothStores(t *testing.T) {
	ctx, runID := context.Background(), uuid.NewString()
	for _, selected := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		if got, err := ReadForkReceiverCompanionStorageForTest(ctx, selected, runID); err == nil || got != nil {
			t.Fatalf("invalid companion authority leaked evidence: %v %v", got, err)
		}
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if got, err := ReadForkReceiverCompanionStorageForTest(cancelled, fixture.store, runID); !errors.Is(err, context.Canceled) || got != nil {
				t.Fatalf("cancelled companion authority leaked evidence: %v %v", got, err)
			}
			if err := readServedDeliveryObservation(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
				if _, _, err := pipelinepersistence.ReadForkReceiverCompanionTableStorageTx(ctx, tx, "entity_state", runID); err == nil {
					t.Fatal("unowned table escaped finite companion catalog")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `ALTER TABLE flow_instance_runtime_readiness RENAME TO retained_companion_readiness`)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			restored := false
			restore := func() error {
				if restored {
					return nil
				}
				err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
					_, err := tx.ExecContext(ctx, `ALTER TABLE retained_companion_readiness RENAME TO flow_instance_runtime_readiness`)
					return err
				})
				if err == nil {
					restored = true
				}
				return err
			}
			defer func() {
				if err := restore(); err != nil {
					t.Error(err)
				}
			}()
			if got, err := ReadForkReceiverCompanionStorageForTest(ctx, fixture.store, runID); err == nil || got != nil {
				t.Fatalf("late table failure leaked partial companion columns/rows: %v %v", got, err)
			}
			if err := restore(); err != nil {
				t.Fatal(err)
			}
			for _, invalid := range []string{"", "foreign-run", uuid.Nil.String()} {
				if got, err := ReadForkReceiverCompanionStorageForTest(ctx, fixture.store, invalid); err == nil || got != nil {
					t.Fatalf("invalid run leaked companion evidence: %v %v", got, err)
				}
			}
			if err := fixture.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			if got, err := ReadForkReceiverCompanionStorageForTest(ctx, fixture.store, runID); err == nil || got != nil {
				t.Fatalf("closed owner leaked companion evidence: %v %v", got, err)
			}
		})
	}
}
