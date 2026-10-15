package runtimepersistence

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestTrackedMutationProjectionPreservesScopeOrderAndNullBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			run, entity, foreignRun, foreignEntity := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
			seedEntityToolStorageWitness(t, fixture, run, entity, true, true)
			seedEntityToolStorageWitness(t, fixture, foreignRun, entity, true, true)
			seedEntityToolStorageWitness(t, fixture, run, foreignEntity, true, true)
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			got, err := ReadTrackedEntityMutationProjectionStorageForTest(ctx, fixture.store, run, entity)
			if err != nil || got.EntityType != "storage_witness" || got.Revision == nil || *got.Revision != 1 || got.CurrentState == nil || *got.CurrentState != "queued" || len(got.Mutations) != 4 || !bytes.Equal(got.Gates, []byte(`{}`)) {
				t.Fatalf("projection=%+v/%v", got, err)
			}
			var want []TrackedProjectionMutationStorage
			rows, err := fixture.db.QueryContext(ctx, `SELECT domain,path,old_value,new_value,writer_type,writer_id,COALESCE(handler_step,'') FROM entity_mutations WHERE run_id=$1 AND entity_id=$2 ORDER BY created_at ASC,mutation_id ASC`, run, entity)
			if err != nil {
				t.Fatal(err)
			}
			for rows.Next() {
				var item TrackedProjectionMutationStorage
				var oldValue, value []byte
				if err := rows.Scan(&item.Domain, &item.Path, &oldValue, &value, &item.WriterType, &item.WriterID, &item.HandlerStep); err != nil {
					t.Fatal(err)
				}
				item.NewValue = append([]byte(nil), value...)
				item.OldValue = append([]byte(nil), oldValue...)
				want = append(want, item)
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if err := rows.Close(); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got.Mutations, want) || got.Mutations[0].NewValue != nil || string(got.Mutations[1].NewValue) != `""` || string(got.Mutations[2].NewValue) != "7" {
				t.Fatalf("order/null/scope changed: %+v / %+v", got.Mutations, want)
			}
			if counts := probe.Snapshot(); counts.Total.ReadCommits != 1 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("projection escaped original read owner: %+v", counts)
			}
			// Exact same source coordinates with no history remain empty evidence;
			// the reconstruction consumer must reject it, not this physical reader.
			if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `DELETE FROM entity_mutations WHERE run_id=$1 AND entity_id=$2`, run, entity)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if empty, err := ReadTrackedEntityMutationProjectionStorageForTest(ctx, fixture.store, run, entity); err != nil || len(empty.Mutations) != 0 || empty.CurrentState == nil || *empty.CurrentState != "queued" {
				t.Fatalf("empty physical history=%+v/%v", empty, err)
			}
		})
	}
}

func TestTrackedMutationProjectionRefusesRawInvalidCancelledClosedAndLateFailureBothStores(t *testing.T) {
	for _, owner := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		got, err := ReadTrackedEntityMutationProjectionStorageForTest(context.Background(), owner, uuid.NewString(), uuid.NewString())
		if err == nil || !reflect.DeepEqual(got, TrackedEntityMutationProjectionStorage{}) {
			t.Fatalf("raw owner=%T evidence=%+v/%v", owner, got, err)
		}
	}
	for _, backend := range eventRecordContractBackends() {
		for _, cut := range []string{"invalid-run", "invalid-entity", "absent", "cancelled", "closed", "late-unavailable"} {
			t.Run(backend.name+"/"+cut, func(t *testing.T) {
				fixture, ctx := backend.open(t), testAuthorActivityContext()
				run, entity := uuid.NewString(), uuid.NewString()
				seedEntityToolStorageWitness(t, fixture, run, entity, true, true)
				switch cut {
				case "invalid-run":
					run = "bad"
				case "invalid-entity":
					entity = "bad"
				case "absent":
					entity = uuid.NewString()
				case "cancelled":
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				case "closed":
					if err := fixture.store.(interface{ Close() error }).Close(); err != nil {
						t.Fatal(err)
					}
				case "late-unavailable":
					if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
						_, err := tx.ExecContext(ctx, `ALTER TABLE entity_mutations RENAME TO unavailable_tracked_projection_mutations`)
						return err
					}); err != nil {
						t.Fatal(err)
					}
					defer func() {
						if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
							_, err := tx.ExecContext(ctx, `ALTER TABLE unavailable_tracked_projection_mutations RENAME TO entity_mutations`)
							return err
						}); err != nil {
							t.Error(err)
						}
					}()
				}
				if got, err := ReadTrackedEntityMutationProjectionStorageForTest(ctx, fixture.store, run, entity); err == nil || !reflect.DeepEqual(got, TrackedEntityMutationProjectionStorage{}) || cut == "cancelled" && !errors.Is(err, context.Canceled) {
					t.Fatalf("partial evidence at %s: %+v/%v", cut, got, err)
				}
			})
		}
	}
}

func TestTrackedMutationProjectionPreservesAbsenceAndPrivateCurrentOperandsBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			run := uuid.NewString()
			for _, shape := range []struct {
				name                 string
				constructed, fielded bool
			}{{"fielded", true, true}, {"fieldless", true, false}, {"data-only", false, true}} {
				t.Run(shape.name, func(t *testing.T) {
					entity := uuid.NewString()
					seedEntityToolStorageWitness(t, fixture, run, entity, shape.constructed, shape.fielded)
					got, err := ReadTrackedEntityMutationProjectionStorageForTest(ctx, fixture.store, run, entity)
					if err != nil {
						t.Fatal(err)
					}
					if shape.fielded {
						if string(got.Fields) != `{}` {
							t.Fatalf("current fields were synthesized from history: %s", got.Fields)
						}
					} else if got.Fields != nil {
						t.Fatalf("fieldless header acquired a data operand: %s", got.Fields)
					}
					if shape.constructed {
						var accumulator map[string]any
						if err := json.Unmarshal(got.Accumulator, &accumulator); err != nil {
							t.Fatal(err)
						}
						if got.CurrentState == nil || *got.CurrentState != "queued" || string(got.Gates) != `{}` || string(got.Bookkeeping) != `{}` ||
							!reflect.DeepEqual(accumulator, map[string]any{"handler_loops": map[string]any{}, "evidence": map[string]any{"score": float64(2)}}) {
							t.Fatalf("private current lifecycle facts changed: %+v", got)
						}
					} else if got.CurrentState != nil || got.Gates != nil || got.Bookkeeping != nil || got.Accumulator != nil {
						t.Fatalf("data-only member acquired lifecycle: %+v", got)
					}
					var last map[string]any
					if len(got.Mutations) != 4 || json.Unmarshal(got.Mutations[3].NewValue, &last) != nil || last["name"] != "second" {
						t.Fatalf("independent physical history changed: %+v", got.Mutations)
					}
				})
			}
		})
	}
}
