package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"
	"time"

	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

func TestIssue2269MutationEvidencePreservesOriginalOwnerAndNullContractBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, record := constructWorkflowMutationFixture(t, backend, "review", time.Now().UTC().Truncate(time.Microsecond))
			owner := f.store.(runtimepipeline.WorkflowPersistenceOwner)
			before, found, err := owner.LoadWorkflowInstance(f.ctx, record.Identity)
			if err != nil || !found {
				t.Fatalf("load original construction: %+v found=%v err=%v", before, found, err)
			}
			evidence, err := ObserveEntityMutationHistoryForTest(f.ctx, f.store, record.Identity.RunID)
			if err != nil || len(evidence) == 0 {
				t.Fatalf("read original construction history: %+v err=%v", evidence, err)
			}
			rows, err := f.db.QueryContext(f.ctx, `SELECT mutation_id,entity_id,domain,path,old_value,new_value,writer_type,writer_id,handler_step,caused_by_event
				FROM entity_mutations WHERE run_id=$1 ORDER BY created_at DESC,mutation_id DESC`, record.Identity.RunID)
			if err != nil {
				t.Fatal(err)
			}
			i, nulls := 0, 0
			for rows.Next() {
				var id, entity, domain, path string
				var oldValue, newValue, writerType, writerID, step, cause sql.NullString
				if err := rows.Scan(&id, &entity, &domain, &path, &oldValue, &newValue, &writerType, &writerID, &step, &cause); err != nil {
					t.Fatal(errors.Join(err, rows.Close()))
				}
				if i >= len(evidence) {
					t.Fatal("owned history omitted physical mutation rows")
				}
				row := evidence[i]
				if row.MutationID != id || row.EntityID != entity || row.Domain != domain || row.Path != path || row.WriterType != writerType.String || row.WriterID != writerID.String || row.HandlerStep != step.String || row.CausedByEvent != cause.String {
					t.Fatalf("owned history changed exact metadata/order: %+v", row)
				}
				for _, value := range []struct {
					stored sql.NullString
					got    []byte
				}{{oldValue, row.OldValue}, {newValue, row.NewValue}} {
					want := value.stored.String
					if !value.stored.Valid {
						want = "null"
						nulls++
					}
					if string(value.got) != want {
						t.Fatalf("owned history changed bytes/NULL contract: got=%s want=%s", value.got, want)
					}
				}
				i++
			}
			if err := errors.Join(rows.Err(), rows.Close()); err != nil {
				t.Fatal(err)
			}
			if i != len(evidence) || nulls == 0 {
				t.Fatalf("history cardinality/nullable witness: physical=%d owned=%d nulls=%d", i, len(evidence), nulls)
			}
			fresh, err := ObserveEntityMutationHistoryForTest(f.ctx, f.store, record.Identity.RunID)
			if err != nil || !reflect.DeepEqual(evidence, fresh) {
				t.Fatalf("repeated read changed evidence: %+v err=%v", fresh, err)
			}
			evidence[0].Path = "changed"
			evidence[0].NewValue[0] = 'x'
			again, err := ObserveEntityMutationHistoryForTest(f.ctx, f.store, record.Identity.RunID)
			if err != nil || !reflect.DeepEqual(fresh, again) {
				t.Fatalf("read exposed mutable retained evidence: %+v err=%v", again, err)
			}
			after, found, err := owner.LoadWorkflowInstance(f.ctx, record.Identity)
			if err != nil || !found || !reflect.DeepEqual(before, after) {
				t.Fatalf("observation changed construction: %+v found=%v err=%v", after, found, err)
			}
		})
	}
}

func TestIssue2269MutationEvidenceRefusesUnavailableAndCanceledOwnersBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, record := constructWorkflowMutationFixture(t, backend, "review", time.Now().UTC())
			for _, invalid := range []any{nil, f.db, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}} {
				if partial, err := ObserveEntityMutationHistoryForTest(f.ctx, invalid, record.Identity.RunID); err == nil || partial != nil {
					t.Fatalf("unavailable/raw owner admitted: %T %+v err=%v", invalid, partial, err)
				}
			}
			if partial, err := ObserveEntityMutationHistoryForTest(f.ctx, f.store, "invalid"); err == nil || partial != nil {
				t.Fatalf("invalid run admitted: %+v err=%v", partial, err)
			}
			canceled, cancel := context.WithCancel(f.ctx)
			cancel()
			if partial, err := ObserveEntityMutationHistoryForTest(canceled, f.store, record.Identity.RunID); !errors.Is(err, context.Canceled) || partial != nil {
				t.Fatalf("canceled read returned evidence: %+v err=%v", partial, err)
			}
		})
	}
}
