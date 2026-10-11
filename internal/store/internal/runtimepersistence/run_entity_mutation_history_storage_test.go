package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestRunEntityMutationHistoryStoragePreservesWholeRunOrderAndNullBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			f, ctx := backend.open(t), testAuthorActivityContext()
			run, foreignRun, entity, sibling := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
			seedEntityToolStorageWitness(t, f, run, entity)
			seedEntityToolStorageWitness(t, f, run, sibling)
			seedEntityToolStorageWitness(t, f, foreignRun, entity)
			probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			got, err := ReadRunEntityMutationHistoryStorageForTest(ctx, f.store, run)
			if err != nil || len(got) != 8 {
				t.Fatalf("complete run history: %+v/%v", got, err)
			}
			if counts := probe.Snapshot(); counts.Total.ReadCommits != 1 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("history escaped original read owner: %+v", counts)
			}
			want := readRunEntityMutationHistoryWitness(t, f, ctx, run)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("run scope, descending order or null representation changed:\n%+v\n%+v", got, want)
			}
			seen, nulls := map[string]int{}, 0
			for _, mutation := range got {
				seen[mutation.EntityID]++
				if string(mutation.NewValue) == "null" {
					nulls++
				}
			}
			if seen[entity] != 4 || seen[sibling] != 4 || nulls != 2 {
				t.Fatalf("history omitted sibling or null witnesses: entities=%v nulls=%d", seen, nulls)
			}
			if empty, err := ReadRunEntityMutationHistoryStorageForTest(ctx, f.store, uuid.NewString()); err != nil || len(empty) != 0 {
				t.Fatalf("absent run must preserve empty physical evidence: %+v/%v", empty, err)
			}
		})
	}
}

func readRunEntityMutationHistoryWitness(t *testing.T, f authorActivityReceiptFixture, ctx context.Context, run string) []operatorread.RunDebugMutation {
	t.Helper()
	rows, err := f.db.QueryContext(ctx, `SELECT entity_id,domain,path,COALESCE(new_value,'null') FROM entity_mutations WHERE run_id=$1 ORDER BY created_at DESC,mutation_id DESC`, run)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var want []operatorread.RunDebugMutation
	for rows.Next() {
		var row operatorread.RunDebugMutation
		var value []byte
		if err := rows.Scan(&row.EntityID, &row.Domain, &row.Path, &value); err != nil {
			t.Fatal(err)
		}
		row.NewValue = append(json.RawMessage(nil), value...)
		want = append(want, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return want
}

func TestRunEntityMutationHistoryStorageRefusesRawInvalidCancelledClosedAndUnavailableBothStores(t *testing.T) {
	for _, owner := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		if got, err := ReadRunEntityMutationHistoryStorageForTest(context.Background(), owner, uuid.NewString()); err == nil || got != nil {
			t.Fatalf("raw owner returned evidence: %T %+v/%v", owner, got, err)
		}
	}
	for _, backend := range eventRecordContractBackends() {
		for _, cut := range []string{"invalid-run", "nil-run", "cancelled", "closed", "unavailable"} {
			t.Run(backend.name+"/"+cut, func(t *testing.T) {
				f, ctx := backend.open(t), testAuthorActivityContext()
				run := uuid.NewString()
				seedEntityToolStorageWitness(t, f, run, uuid.NewString())
				switch cut {
				case "invalid-run":
					run = "bad"
				case "nil-run":
					run = uuid.Nil.String()
				case "cancelled":
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				case "closed":
					if err := f.store.(interface{ Close() error }).Close(); err != nil {
						t.Fatal(err)
					}
				case "unavailable":
					if err := renameRunHistoryWitness(ctx, f.store, false); err != nil {
						t.Fatal(err)
					}
					defer func() {
						if err := renameRunHistoryWitness(ctx, f.store, true); err != nil {
							t.Error(err)
						}
					}()
				}
				got, err := ReadRunEntityMutationHistoryStorageForTest(ctx, f.store, run)
				if err == nil || got != nil || cut == "cancelled" && !errors.Is(err, context.Canceled) {
					t.Fatalf("failed read supplied evidence at %s: %+v/%v", cut, got, err)
				}
			})
		}
	}
}

func renameRunHistoryWitness(ctx context.Context, selected any, restore bool) error {
	return runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		statement := `ALTER TABLE entity_mutations RENAME TO unavailable_run_history_mutations`
		if restore {
			statement = `ALTER TABLE unavailable_run_history_mutations RENAME TO entity_mutations`
		}
		_, err := tx.ExecContext(ctx, statement)
		return err
	})
}
