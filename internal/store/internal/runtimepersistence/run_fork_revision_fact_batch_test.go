package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func revisionBatchEntities(t *testing.T, ctx context.Context, tx *sql.Tx, f runForkRevisionMatrixFixture) ([]string, *runforkrevision.Effects) {
	t.Helper()
	ids := make([]string, 257)
	for i := range ids {
		ids[i] = uuid.NewString()
	}
	sort.Strings(ids)
	effects := runforkrevision.NewEffects()
	for _, id := range ids {
		mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO entity_state (run_id,entity_id,flow_instance,entity_type,slug,name,current_state,fields,created_at,updated_at) VALUES ($1,$2,'batch/1','fork_entity','batch-slug','Original','ready','{}',$3,$3)`, f.runID, id, f.at)
		if err := effects.AddFact(f.runID, runforkrevision.FamilyEntityMetadata, id); err != nil {
			t.Fatal(err)
		}
	}
	return ids, effects
}

func revisionBatchFinalizeObserved(t *testing.T, ctx context.Context, tx *sql.Tx, s exactFactStore, effects *runforkrevision.Effects, wantExec uint64) (map[string]runforkrevision.Result, error) {
	t.Helper()
	var slot transactiontest.Slot
	collector, restore, err := slot.Install(transactiontest.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer restore()
	attempt := slot.Begin(false, false)
	attempt.Begun()
	results, finalErr := finalizeRunForkRevisionMatrix(transactiontest.WithAttempt(ctx, attempt), tx, s.postgres, effects)
	attempt.Finish(finalErr)
	counts := collector.Snapshot().Total.Revision
	if counts.Finalizations != 1 || counts.LockPhases != 1 || counts.ExecCalls != wantExec {
		t.Fatalf("physical revision SQL=%+v, want %d Exec calls", counts, wantExec)
	}
	return results, finalErr
}

func TestRunForkRevisionFactBatchBothStores(t *testing.T) {
	eachExactFactStore(t, func(t *testing.T, s exactFactStore) {
		f := newExactFactFixture(t, s)
		var ids []string
		var effects *runforkrevision.Effects
		var initial []exactLedgerRow
		overhead := uint64(3) // SQLite: ensure head, increment head, insert revision.
		if s.postgres {
			overhead = 2 // PostgreSQL increments via QueryRow RETURNING instead.
		}
		exactTransaction(t, s, func(ctx context.Context, tx *sql.Tx) {
			ids, effects = revisionBatchEntities(t, ctx, tx, f)
			got, err := revisionBatchFinalizeObserved(t, ctx, tx, s, effects, overhead+3)
			if err != nil || got[f.runID] != (runforkrevision.Result{Revision: 1, Changed: true}) {
				t.Fatalf("initial batch=%v err=%v", got, err)
			}
			for _, id := range ids {
				initial = append(initial, exactLedgerRow{Revision: 1, Family: string(runforkrevision.FamilyEntityMetadata), Key: id, Present: true, Body: map[string]any{
					"entity_id": id, "flow_instance": "batch/1", "entity_type": "fork_entity", "slug": "batch-slug", "name": "Original", "created_at": f.at.UTC().Format(time.RFC3339Nano), "flow_config": nil,
				}})
			}
			if ledger := exactLedger(t, ctx, tx, f.runID); !reflect.DeepEqual(ledger, initial) {
				t.Fatalf("257-row batch differs from independently constructed facts: got=%d want=%d", len(ledger), len(initial))
			}
		})
		exactTransaction(t, s, func(ctx context.Context, tx *sql.Tx) {
			mustExecRunForkRevisionMatrix(t, ctx, tx, `UPDATE entity_state SET current_state='excluded' WHERE run_id=$1`, f.runID)
			got, err := revisionBatchFinalizeObserved(t, ctx, tx, s, effects, 1)
			if err != nil || got[f.runID] != (runforkrevision.Result{Revision: 1}) || !reflect.DeepEqual(initial, exactLedger(t, ctx, tx, f.runID)) {
				t.Fatalf("excluded-field/no-op changed history: result=%v err=%v", got, err)
			}
		})
		var mixed []exactLedgerRow
		exactTransaction(t, s, func(ctx context.Context, tx *sql.Tx) {
			mixed = append(mixed, initial...)
			for i, id := range ids[:256] {
				row := exactLedgerRow{Revision: 2, Family: string(runforkrevision.FamilyEntityMetadata), Key: id, Body: map[string]any{}}
				if i < 128 {
					mustExecRunForkRevisionMatrix(t, ctx, tx, `UPDATE entity_state SET name='Changed' WHERE run_id=$1 AND entity_id=$2`, f.runID, id)
					row.Present = true
					row.Body = map[string]any{"entity_id": id, "flow_instance": "batch/1", "entity_type": "fork_entity", "slug": "batch-slug", "name": "Changed", "created_at": f.at.UTC().Format(time.RFC3339Nano), "flow_config": nil}
				} else {
					mustExecRunForkRevisionMatrix(t, ctx, tx, `DELETE FROM entity_state WHERE run_id=$1 AND entity_id=$2`, f.runID, id)
				}
				mixed = append(mixed, row)
			}
			got, err := revisionBatchFinalizeObserved(t, ctx, tx, s, effects, overhead+2)
			if err != nil || got[f.runID] != (runforkrevision.Result{Revision: 2, Changed: true}) || !reflect.DeepEqual(mixed, exactLedger(t, ctx, tx, f.runID)) {
				t.Fatalf("mixed updates/tombstones/unchanged sibling: result=%v err=%v", got, err)
			}
			if err := validateRunForkRevisionMatrix(ctx, tx, s.postgres, f.runID); err != nil {
				t.Fatal(err)
			}
		})
		exactTransaction(t, s, func(ctx context.Context, tx *sql.Tx) {
			whole, err := runforkrevision.ForRun(f.runID, runforkrevision.FamilyEntityMetadata)
			if err != nil {
				t.Fatal(err)
			}
			for _, selection := range []*runforkrevision.Effects{effects, whole} {
				got, err := revisionBatchFinalizeObserved(t, ctx, tx, s, selection, 1)
				if err != nil || got[f.runID] != (runforkrevision.Result{Revision: 2}) || !reflect.DeepEqual(mixed, exactLedger(t, ctx, tx, f.runID)) {
					t.Fatalf("repeated exact/whole selection changed history: result=%v err=%v", got, err)
				}
			}
		})
	})
}

func TestRunForkRevisionFactBatchLateFailureRollsBackBothStores(t *testing.T) {
	eachExactFactStore(t, func(t *testing.T, s exactFactStore) {
		f := newExactFactFixture(t, s)
		exactRollbackTransaction(t, s, func(ctx context.Context, tx *sql.Tx) {
			ids, effects := revisionBatchEntities(t, ctx, tx, f)
			name := "batch_failure_" + strings.ReplaceAll(uuid.NewString(), "-", "")
			prior := fmt.Sprintf("(SELECT COUNT(*) FROM run_fork_fact_revisions WHERE run_id='%s' AND revision=1)=256 AND (SELECT COUNT(*) FROM entity_state WHERE run_id='%s')=257", f.runID, f.runID)
			query := fmt.Sprintf("CREATE TRIGGER %s BEFORE INSERT ON run_fork_fact_revisions WHEN NEW.run_id='%s' AND NEW.fact_key='%s' BEGIN SELECT CASE WHEN %s THEN RAISE(ABORT,'batch_after_256_real_facts') ELSE RAISE(ABORT,'batch_missing_prior_facts') END; END", name, f.runID, ids[256], prior)
			overhead := uint64(3)
			if s.postgres {
				overhead = 2
				mustExecRunForkRevisionMatrix(t, ctx, tx, fmt.Sprintf("CREATE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF %s THEN RAISE EXCEPTION 'batch_after_256_real_facts'; ELSE RAISE EXCEPTION 'batch_missing_prior_facts'; END IF; END $$", name, prior))
				query = fmt.Sprintf("CREATE TRIGGER %s BEFORE INSERT ON run_fork_fact_revisions FOR EACH ROW WHEN (NEW.run_id='%s' AND NEW.fact_key='%s') EXECUTE FUNCTION %s()", name, f.runID, ids[256], name)
			}
			mustExecRunForkRevisionMatrix(t, ctx, tx, query)
			got, err := revisionBatchFinalizeObserved(t, ctx, tx, s, effects, overhead+3)
			if got != nil || err == nil || !strings.Contains(err.Error(), "batch_after_256_real_facts") || !strings.Contains(err.Error(), ids[256]) {
				t.Fatalf("late native failure lost real earlier chunks or error: results=%v err=%v", got, err)
			}
		})
		for _, table := range []string{"entity_state", "run_fork_revision_heads", "run_fork_revisions", "run_fork_fact_revisions"} {
			var count int
			if err := s.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM "+table+" WHERE run_id=$1", f.runID).Scan(&count); err != nil || count != 0 {
				t.Fatalf("rollback leaked %s: count=%d err=%v", table, count, err)
			}
		}
	})
}
