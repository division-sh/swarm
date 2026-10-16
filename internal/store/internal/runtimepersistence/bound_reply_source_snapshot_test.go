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

func referenceBoundReplyRows(t *testing.T, f receiverConfigActivationFixture, query, runID, path string) [][]any {
	t.Helper()
	rows, err := f.db.QueryContext(f.ctx, query, runID, path)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	var out [][]any
	for rows.Next() {
		values, destinations := make([]any, len(columns)), make([]any, len(columns))
		for index := range values {
			destinations[index] = &values[index]
		}
		if err := rows.Scan(destinations...); err != nil {
			t.Fatal(err)
		}
		for index, value := range values {
			if raw, ok := value.([]byte); ok {
				values[index] = string(raw)
			}
		}
		out = append(out, values)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestBoundReplySourceSnapshotRetainsAllColumnsTypesAndIndependentCutsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, state := newCompetingRevisionPairFixture(t, backend)
			runID, path := state.Identity.RunID, state.Identity.Route.InstancePath
			want := map[string][][]any{
				"flow_instances": referenceBoundReplyRows(t, f, `SELECT * FROM flow_instances WHERE run_id=$1 AND instance_path=$2 ORDER BY instance_path`, runID, path),
				"entity_state":   referenceBoundReplyRows(t, f, `SELECT * FROM entity_state WHERE run_id=$1 AND flow_instance=$2 ORDER BY entity_id`, runID, path),
			}
			if len(want["flow_instances"]) == 0 || len(want["entity_state"]) == 0 {
				t.Fatal("native construction did not establish both physical halves")
			}
			before := competingRevisionPairSnapshot(t, f)
			probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			got, err := ReadBoundReplySourceStorageForTest(f.ctx, f.store, runID, path)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("complete physical columns/order/NULL/bytes/type/clock witness changed: got=%#v want=%#v err=%v", got, want, err)
			}
			if counts := probe.Snapshot(); counts.Active != 0 || counts.Total.Begun != 2 || counts.Total.ReadCommits != 2 || counts.Total.WriteCommits != 0 {
				t.Fatalf("source snapshot merged read cuts or bypassed original coordinator: %+v", counts)
			}
			for _, scope := range [][2]string{{uuid.NewString(), path}, {runID, "foreign/path"}} {
				if got, err := ReadBoundReplySourceStorageForTest(f.ctx, f.store, scope[0], scope[1]); err != nil || !reflect.DeepEqual(got, map[string][][]any{"flow_instances": nil, "entity_state": nil}) {
					t.Fatalf("snapshot borrowed another run/path or changed empty-row representation: %#v %v", got, err)
				}
			}
			cancelled, cancel := context.WithCancel(f.ctx)
			cancel()
			if got, err := ReadBoundReplySourceStorageForTest(cancelled, f.store, runID, path); !errors.Is(err, context.Canceled) || got != nil {
				t.Fatalf("cancelled source snapshot returned partial evidence: %#v %v", got, err)
			}
			if !reflect.DeepEqual(before, competingRevisionPairSnapshot(t, f)) {
				t.Fatal("source observation changed complete data, history, lease or clocks")
			}
			restore()
			assertBoundReplyMissingCompanionRetainsHeader(t, f, runID, path, state.EntityID, want["flow_instances"])
			assertBoundReplySecondReadFailureDiscardsHeader(t, f, runID, path)
		})
	}
}

func assertBoundReplySecondReadFailureDiscardsHeader(t *testing.T, f receiverConfigActivationFixture, runID, path string) {
	t.Helper()
	if err := runWorkflowProjectionFault(f.ctx, f.store, func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `ALTER TABLE entity_state RENAME TO unavailable_bound_reply_entity_state`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	restore := func() error {
		return runWorkflowProjectionFault(f.ctx, f.store, func(ctx context.Context, tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, `ALTER TABLE unavailable_bound_reply_entity_state RENAME TO entity_state`)
			return err
		})
	}
	defer func() {
		if err := restore(); err != nil {
			t.Errorf("restore exact bound-reply companion table: %v", err)
		}
	}()
	if got, err := ReadBoundReplySourceStorageForTest(f.ctx, f.store, runID, path); err == nil || got != nil {
		t.Fatalf("failed second read exposed a provisional header snapshot: %#v %v", got, err)
	}
}

func assertBoundReplyMissingCompanionRetainsHeader(t *testing.T, f receiverConfigActivationFixture, runID, path, entityID string, header [][]any) {
	t.Helper()
	if _, err := RemoveWorkflowProjectionFieldsForTest(f.ctx, f.store, runID, entityID); err != nil {
		t.Fatal(err)
	}
	got, err := ReadBoundReplySourceStorageForTest(f.ctx, f.store, runID, path)
	if err != nil || !reflect.DeepEqual(got, map[string][][]any{"flow_instances": header, "entity_state": nil}) {
		t.Fatalf("physical missing companion synthesized state or dropped the header: %#v %v", got, err)
	}
}

func TestBoundReplySourceSnapshotRefusesRawClosedAndEmptyPath(t *testing.T) {
	for _, selected := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		if got, err := ReadBoundReplySourceStorageForTest(context.Background(), selected, uuid.NewString(), "review"); err == nil || got != nil {
			t.Fatalf("source snapshot accepted non-native owner %T: %#v %v", selected, got, err)
		}
	}
	f, state := newCompetingRevisionPairFixture(t, "sqlite")
	if got, err := ReadBoundReplySourceStorageForTest(f.ctx, f.store, state.Identity.RunID, ""); err == nil || got != nil {
		t.Fatalf("empty concrete source was accepted: %#v %v", got, err)
	}
	if err := f.manager.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if err := f.store.(*SQLiteRuntimeStore).Close(); err != nil {
		t.Fatal(err)
	}
	if got, err := ReadBoundReplySourceStorageForTest(f.ctx, f.store, state.Identity.RunID, state.Identity.Route.InstancePath); err == nil || got != nil {
		t.Fatalf("closed source owner returned physical rows: %#v %v", got, err)
	}
}
