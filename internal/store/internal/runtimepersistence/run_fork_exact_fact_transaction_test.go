package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/google/uuid"
)

func TestRunForkExactFactsTransactionBothStores(t *testing.T) {
	eachExactFactStore(t, func(t *testing.T, s exactFactStore) {
		t.Run("nested_contributions_one_multi_run_finalizer", func(t *testing.T) {
			fixtures := []runForkRevisionMatrixFixture{newExactFactFixture(t, s), newExactFactFixture(t, s)}
			exactTransaction(t, s, func(ctx context.Context, tx *sql.Tx) {
				effects, whole := runforkrevision.NewEffects(), runforkrevision.NewEffects()
				for i := len(fixtures) - 1; i >= 0; i-- {
					f := fixtures[i]
					mustExecRunForkRevisionMatrix(t, ctx, tx, `SAVEPOINT nested_writer`)
					seedRunForkRevisionMatrixEvent(t, ctx, tx, f.runID, f.eventID, f.at, s.postgres)
					if err := effects.AddFacts(f.runID, exactFactRef(t, runforkrevision.FamilyEvents, f.eventID)); err != nil {
						t.Fatal(err)
					}
					if err := whole.Add(f.runID, runforkrevision.AllFamilies()...); err != nil {
						t.Fatal(err)
					}
					mustExecRunForkRevisionMatrix(t, ctx, tx, `RELEASE SAVEPOINT nested_writer`)
					if len(exactLedger(t, ctx, tx, f.runID)) != 0 {
						t.Fatal("nested contribution finalized before outer boundary")
					}
				}
				mustExecRunForkRevisionMatrix(t, ctx, tx, `SAVEPOINT multi_run_oracle`)
				want, err := finalizeRunForkRevisionMatrix(ctx, tx, s.postgres, whole)
				if err != nil {
					t.Fatal(err)
				}
				ledgers := make(map[string][]exactLedgerRow)
				for _, f := range fixtures {
					ledgers[f.runID] = exactLedger(t, ctx, tx, f.runID)
				}
				mustExecRunForkRevisionMatrix(t, ctx, tx, `ROLLBACK TO SAVEPOINT multi_run_oracle`)
				mustExecRunForkRevisionMatrix(t, ctx, tx, `RELEASE SAVEPOINT multi_run_oracle`)
				got, err := finalizeRunForkRevisionMatrix(ctx, tx, s.postgres, effects)
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("multi-run exact=%#v whole=%#v err=%v", got, want, err)
				}
				for _, f := range fixtures {
					if got[f.runID] != (runforkrevision.Result{Revision: 1, Changed: true}) || !reflect.DeepEqual(ledgers[f.runID], exactLedger(t, ctx, tx, f.runID)) {
						t.Fatalf("multi-run result mismatch for %s", f.runID)
					}
					if err := validateRunForkRevisionMatrix(ctx, tx, s.postgres, f.runID); err != nil {
						t.Fatal(err)
					}
				}
			})
		})
		for _, nativeFailure := range []bool{false, true} {
			t.Run(fmt.Sprintf("rollback_native_finalizer_failure_%v", nativeFailure), func(t *testing.T) {
				f := newExactFactFixture(t, s)
				before := snapshotForkHistoricalExecutionTables(t, s.db, s.postgres)
				exactRollbackTransaction(t, s, func(ctx context.Context, tx *sql.Tx) {
					seedRunForkRevisionMatrixEvent(t, ctx, tx, f.runID, f.eventID, f.at, s.postgres)
					if nativeFailure {
						name := "exact_finalizer_" + strings.ReplaceAll(uuid.NewString(), "-", "")
						prefix := fmt.Sprintf("EXISTS (SELECT 1 FROM events WHERE event_id='%s' AND run_id='%s')", f.eventID, f.runID)
						query := fmt.Sprintf("CREATE TRIGGER %s AFTER INSERT ON run_fork_fact_revisions WHEN NEW.run_id='%s' BEGIN SELECT CASE WHEN %s THEN RAISE(ABORT,'exact_after_domain') ELSE RAISE(ABORT,'exact_missing_domain') END; END", name, f.runID, prefix)
						if s.postgres {
							mustExecRunForkRevisionMatrix(t, ctx, tx, fmt.Sprintf("CREATE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF %s THEN RAISE EXCEPTION 'exact_after_domain'; ELSE RAISE EXCEPTION 'exact_missing_domain'; END IF; END $$", name, prefix))
							query = fmt.Sprintf("CREATE TRIGGER %s AFTER INSERT ON run_fork_fact_revisions FOR EACH ROW WHEN (NEW.run_id='%s') EXECUTE FUNCTION %s()", name, f.runID, name)
						}
						mustExecRunForkRevisionMatrix(t, ctx, tx, query)
					}
					result, err := finalizeRunForkRevisionMatrix(ctx, tx, s.postgres, exactEffects(t, f.runID, exactFactRef(t, runforkrevision.FamilyEvents, f.eventID)))
					if nativeFailure {
						if err == nil || !strings.Contains(err.Error(), "exact_after_domain") {
							t.Fatalf("native finalizer fault=%v", err)
						}
					} else if err != nil || result[f.runID] != (runforkrevision.Result{Revision: 1, Changed: true}) {
						t.Fatalf("pre-rollback finalizer result=%#v err=%v", result, err)
					}
				})
				if after := snapshotForkHistoricalExecutionTables(t, s.db, s.postgres); !reflect.DeepEqual(after, before) {
					t.Fatal("rollback leaked domain, revision head, revision or fact rows")
				}
			})
		}
	})
}

func TestRunForkExactFactsFixedEventMaterializationBothStores(t *testing.T) {
	eachExactFactStore(t, func(t *testing.T, s exactFactStore) {
		f := newRunForkRevisionMatrixFixture()
		event := eventtest.ExistingRunRootIngress(f.eventID, "matrix.event", "revision-matrix", "", []byte(`{"matrix":true,"items":["matrix"]}`), 0, f.runID, events.EventEnvelope{Scope: events.EventScopeGlobal}, f.at)
		native := prepareFixedForkHistoryFixture(t, s.selected.(agentFixtureFlowStore), "exact-facts", f.runID, f.entityID, event)
		if err := runSelectedFixtureMutation(native.ctx, s.selected, "exact fixed construction and first event", func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
			if err := commitSemanticEventFixtureWithRoutesStoryTx(ctx, s.selected.(eventCommitTxStore), attempt, event, nil); err != nil {
				return err
			}
			if _, err := commitFixedForkHistoryFixture(ctx, attempt, native); err != nil {
				return err
			}
			return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
				refs := []runforkrevision.FactRef{exactFactRef(t, runforkrevision.FamilyEvents, f.eventID),
					exactFactRef(t, runforkrevision.FamilyCommittedReplayScopes, f.eventID),
					exactFactRef(t, runforkrevision.FamilyEntityMetadata, f.runID), exactFactRef(t, runforkrevision.FamilyEntityMetadata, f.entityID)}
				rows, err := tx.QueryContext(ctx, `SELECT mutation_id FROM entity_mutations WHERE run_id=$1 ORDER BY mutation_id`, f.runID)
				if err != nil {
					return err
				}
				births := 0
				for rows.Next() {
					var id string
					if err := rows.Scan(&id); err != nil {
						rows.Close()
						return err
					}
					refs = append(refs, exactFactRef(t, runforkrevision.FamilyEntityMutations, id))
					births++
				}
				if err := rows.Err(); err != nil {
					rows.Close()
					return err
				}
				if err := rows.Close(); err != nil {
					return err
				}
				if births < 3 {
					t.Fatalf("native constructor omitted parent/child/name birth evidence: %d", births)
				}
				compareExactWithWhole(t, ctx, tx, s, f.runID, exactEffects(t, f.runID, refs...), 1, true)
				return nil
			})
		}); err != nil {
			t.Fatal(err)
		}
		laterEvent, laterMutation := uuid.NewString(), uuid.NewString()
		exactTransaction(t, s, func(ctx context.Context, tx *sql.Tx) {
			seedRunForkRevisionMatrixEvent(t, ctx, tx, f.runID, laterEvent, f.at.Add(time.Second), s.postgres)
			mustExecRunForkRevisionMatrix(t, ctx, tx, `UPDATE entity_state SET name='Later Entity',fields=$1,updated_at=$4 WHERE run_id=$2 AND entity_id=$3`, `{"name":"Later Entity"}`, f.runID, f.entityID, f.at.Add(time.Second))
			mustExecRunForkRevisionMatrix(t, ctx, tx, `UPDATE flow_instances SET name='Later Entity',revision=revision+1,updated_at=$3 WHERE run_id=$1 AND entity_id=$2`, f.runID, f.entityID, f.at.Add(time.Second))
			mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO entity_mutations (mutation_id,run_id,entity_id,domain,path,new_value,caused_by_event,writer_type,writer_id,created_at) VALUES ($1,$2,$3,'authored_field','name',$4,$5,'platform','exact-fork',$6)`, laterMutation, f.runID, f.entityID, `"Later Entity"`, laterEvent, f.at.Add(time.Second))
			compareExactWithWhole(t, ctx, tx, s, f.runID, exactEffects(t, f.runID,
				exactFactRef(t, runforkrevision.FamilyEvents, laterEvent), exactFactRef(t, runforkrevision.FamilyEntityMetadata, f.entityID), exactFactRef(t, runforkrevision.FamilyEntityMutations, laterMutation)), 2, true)
		})
		ctx := native.ctx
		for _, point := range []struct {
			event, name string
			revision    int64
		}{{f.eventID, "Snapshot Entity", 1}, {laterEvent, "Later Entity", 2}} {
			plan, err := s.selected.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: f.runID, At: point.event})
			if err != nil || plan.ForkPoint.Revision != point.revision {
				t.Fatalf("fixed event plan=%#v err=%v", plan.ForkPoint, err)
			}
			business := fixedForkBusinessEntity(t, plan, native)
			if business.Fields["name"] != point.name {
				t.Fatalf("fixed cut lost exact business name: %+v", business)
			}
			ids, ok := plan.HistoricalEventIDs(point.revision)
			if !ok || len(ids) != int(point.revision) {
				t.Fatalf("inclusive history=%v admitted=%v", ids, ok)
			}
			for _, id := range ids {
				if id != f.eventID && (point.revision == 1 || id != laterEvent) {
					t.Fatalf("wrong historical event %s", id)
				}
			}
			child, err := s.selected.MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{SourceRunID: f.runID, At: point.event})
			if err != nil {
				t.Fatalf("materialize exact fixed event: %v", err)
			}
			if child.ForkPoint.Revision != point.revision || child.MaterializedEntityCount != 2 {
				t.Fatalf("materialized cut=%#v", child)
			}
			var name, state, sourceRun, sourceEvent string
			if err := s.db.QueryRowContext(ctx, `SELECT e.name,fi.current_state,r.forked_from_run_id,r.forked_from_event_id FROM entity_state e JOIN runs r ON r.run_id=e.run_id JOIN flow_instances fi ON fi.run_id=e.run_id AND fi.instance_path=e.flow_instance AND fi.entity_id=e.entity_id WHERE e.run_id=$1 AND e.entity_id=$2`, child.ForkRunID, f.entityID).Scan(&name, &state, &sourceRun, &sourceEvent); err != nil {
				t.Fatal(err)
			}
			if name != point.name || state != "ready" || sourceRun != f.runID || sourceEvent != point.event {
				t.Fatalf("materialized readback=%q %q %q %q", name, state, sourceRun, sourceEvent)
			}
		}
	})
}
