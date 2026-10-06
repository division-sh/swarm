package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	storeschema "github.com/division-sh/swarm/internal/store/internal/schemastore"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestSelectedForkPreparationSnapshotRetainsWholePhysicalInventoryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected, _ := decisionCardTestStore(t, backend)
			db, _ := decisionCardStoreDB(t, selected)
			ctx := context.Background()
			for _, query := range []string{
				`CREATE TABLE "snapshot""rows" (id TEXT,payload TEXT,amount INTEGER,optional TEXT)`,
				`CREATE TABLE snapshot_empty (id TEXT,payload TEXT)`,
				`CREATE TABLE snapshot_column_identity ("a,b" TEXT,c TEXT)`,
				`CREATE VIEW snapshot_view AS SELECT id FROM "snapshot""rows"`,
			} {
				if _, err := db.ExecContext(ctx, query); err != nil {
					t.Fatal(err)
				}
			}
			for _, row := range [][]any{
				{"b", `{ "lex": 1.00 }`, 2, nil},
				{"a", `[]`, 1, ""},
				{"a", `[]`, 1, ""},
			} {
				if _, err := db.ExecContext(ctx, `INSERT INTO "snapshot""rows" VALUES ($1,$2,$3,$4)`, row...); err != nil {
					t.Fatal(err)
				}
			}
			probe, restore, err := InstallTransactionProbeForTest(selected, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			before, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected)
			if err != nil {
				t.Fatal(err)
			}
			want := SelectedForkStorageTableSnapshot{
				Columns: []string{"id", "payload", "amount", "optional"},
				Rows:    []string{`["a","[]",1,""]`, `["a","[]",1,""]`, `["b","{ \"lex\": 1.00 }",2,null]`},
			}
			if !reflect.DeepEqual(before[`snapshot"rows`], want) {
				t.Fatalf("physical columns, duplicates, lexical JSON or sorting changed: %q want=%q", before[`snapshot"rows`], want)
			}
			if !reflect.DeepEqual(before["snapshot_empty"], SelectedForkStorageTableSnapshot{Columns: []string{"id", "payload"}, Rows: []string{}}) {
				t.Fatalf("empty table disappeared or lost columns: %q", before["snapshot_empty"])
			}
			if _, present := before["snapshot_view"]; present {
				t.Fatal("base-table snapshot included a view")
			}
			for _, table := range []string{"runs", "events", "entity_state", "entity_mutations", "event_receipts", "agents", "timers", "run_fork_selected_contract_runtime_executions"} {
				if len(before[table].Columns) == 0 {
					t.Fatalf("whole-store snapshot omitted %s", table)
				}
			}
			if counts := probe.Snapshot(); counts.Total.Begun != 1 || counts.Total.ReadCommits != 1 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("snapshot escaped original consistent read owner: %+v", counts)
			}
			if _, err := db.ExecContext(ctx, `UPDATE "snapshot""rows" SET payload='changed' WHERE id='b'`); err != nil {
				t.Fatal(err)
			}
			for _, query := range []string{
				`ALTER TABLE snapshot_column_identity RENAME COLUMN "a,b" TO a`,
				`ALTER TABLE snapshot_column_identity RENAME COLUMN c TO "b,c"`,
			} {
				if _, err := db.ExecContext(ctx, query); err != nil {
					t.Fatal(err)
				}
			}
			after, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected)
			if err != nil || reflect.DeepEqual(before, after) || !reflect.DeepEqual(before[`snapshot"rows`], want) {
				t.Fatalf("snapshot missed changed evidence or aliased prior values: before=%q after=%q err=%v", before[`snapshot"rows`], after[`snapshot"rows`], err)
			}
			if !reflect.DeepEqual(before["snapshot_column_identity"].Columns, []string{"a,b", "c"}) ||
				!reflect.DeepEqual(after["snapshot_column_identity"].Columns, []string{"a", "b,c"}) {
				t.Fatalf("exact column arrays were coalesced or aliased: before=%q after=%q", before["snapshot_column_identity"].Columns, after["snapshot_column_identity"].Columns)
			}
		})
	}
}

func TestSelectedForkPreparationSnapshotRefusesInvalidAndPartialEvidenceBothStores(t *testing.T) {
	ctx := context.Background()
	for _, selected := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		if snapshot, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected); err == nil || snapshot != nil {
			t.Fatalf("invalid owner returned snapshot: %v %v", snapshot, err)
		}
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected, _ := decisionCardTestStore(t, backend)
			db, _ := decisionCardStoreDB(t, selected)
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if snapshot, err := ReadSelectedForkApplicationStorageSnapshotForTest(cancelled, selected); !errors.Is(err, context.Canceled) || snapshot != nil {
				t.Fatalf("cancelled observation returned evidence: %v %v", snapshot, err)
			}
			if _, err := db.ExecContext(ctx, `CREATE TABLE zz_snapshot_unencodable (amount DOUBLE PRECISION)`); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, `INSERT INTO zz_snapshot_unencodable VALUES ($1)`, math.Inf(1)); err != nil {
				t.Fatal(err)
			}
			if snapshot, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected); err == nil || snapshot != nil {
				t.Fatalf("late encoding failure returned partial earlier tables: %v %v", snapshot, err)
			}
			if _, err := db.ExecContext(ctx, `DROP TABLE zz_snapshot_unencodable`); err != nil {
				t.Fatal(err)
			}
			if snapshot, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected); err != nil || len(snapshot) == 0 {
				t.Fatalf("refused read retained transaction or poisoned next snapshot: %v %v", snapshot, err)
			}
			if err := selected.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			if snapshot, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected); err == nil || snapshot != nil {
				t.Fatalf("closed owner returned evidence: %v %v", snapshot, err)
			}
		})
	}
}

func TestSelectedForkApplicationSnapshotObserverDoesNotAcquireRuntimeAuthorityBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			var observer any
			var runtimeCall func() error
			if backend == "sqlite" {
				path := filepath.Join(t.TempDir(), "observer.sqlite")
				writer := newBootstrappedSQLiteRuntimeStoreForPath(t, path)
				before, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, writer)
				if err != nil {
					t.Fatal(err)
				}
				schema, native, err := storeschema.OpenSQLiteReadOnlyForInspection(path)
				if err != nil {
					t.Fatal(err)
				}
				selected, err := ComposeSQLiteRuntimeStore(schema, native)
				if err != nil {
					_ = schema.Close()
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := selected.Close(); err != nil {
						t.Error(err)
					}
				})
				observer = selected
				runtimeCall = func() error {
					_, err := selected.ListActiveAgentDescriptors(ctx, unacceptedAdmissionEventID)
					return err
				}
				if err := native.RunTransaction(ctx, "read-only observer refusal", func(ctx context.Context, tx *sql.Tx) error {
					_, err := tx.ExecContext(ctx, `CREATE TABLE snapshot_forbidden_writer (id TEXT)`)
					return err
				}); err == nil {
					t.Fatal("read-only physical observer acquired write authority")
				}
				after, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, observer)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("read-only observer changed or lost physical evidence: err=%v", err)
				}
			} else {
				selected, err := NewPostgresStore(testutil.StartPostgresDSN(t))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := selected.Close(); err != nil {
						t.Error(err)
					}
				})
				observer = selected
				runtimeCall = func() error {
					_, err := selected.ListActiveAgentDescriptors(ctx, unacceptedAdmissionEventID)
					return err
				}
			}
			requireUnacceptedAdmissionFailure(t, runtimeCall())
			before, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, observer)
			if err != nil || len(before["events"].Columns) == 0 {
				t.Fatalf("physical observer could not inspect native storage: err=%v", err)
			}
			requireUnacceptedAdmissionFailure(t, runtimeCall())
			after, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, observer)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("runtime refusal changed inspection evidence: err=%v", err)
			}
		})
	}
}
