package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type exactFactStore struct {
	db       *sql.DB
	postgres bool
	selected runForkSelectedLifecycleStore
}

func eachExactFactStore(t *testing.T, prove func(*testing.T, exactFactStore)) {
	t.Helper()
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			if backend == "sqlite" {
				store := newBootstrappedSQLiteRuntimeStoreForTest(t)
				prove(t, exactFactStore{db: store.backend.ConstructionHandle(), selected: store})
				return
			}
			_, db, _ := testutil.StartPostgres(t)
			prove(t, exactFactStore{db: db, postgres: true, selected: newPostgresStoreWithBackend(mustPostgresBackend(db))})
		})
	}
}

func newExactFactFixture(t *testing.T, s exactFactStore) runForkRevisionMatrixFixture {
	t.Helper()
	f := newRunForkRevisionMatrixFixture()
	ctx := testAuthorActivityContext()
	requireRunFixtureForTest(t, ctx, s.selected, semanticRunFixture{
		Origin: semanticScenarioSetupRunOriginForTest(), RunID: f.runID, StartedAt: f.at,
	})
	seedTestAgentRow(t, ctx, s.db, s.postgres, mustTestAgentIdentityForRun(f.runID, "revision-matrix-agent", ""), "active")
	return f
}

func exactFactRef(t *testing.T, family runforkrevision.Family, key string) runforkrevision.FactRef {
	t.Helper()
	ref, err := runforkrevision.NewFactRef(family, key)
	if err != nil {
		t.Fatalf("admit exact %s/%s: %v", family, key, err)
	}
	return ref
}

func exactMatrixIntentKey(f runForkRevisionMatrixFixture) fanoutobligation.IntentKey {
	return fanoutobligation.IntentKey{RunID: f.runID, TriggeringDeliveryID: f.deliveryID,
		ElementRef: contracts.FanOutElementRef{FlowPath: "root", Family: "handler_rule", SemanticPath: `handlers["items.ready"].rules[0]`}}
}

func exactMatrixRefs(t *testing.T, f runForkRevisionMatrixFixture) []runforkrevision.FactRef {
	t.Helper()
	refs := []runforkrevision.FactRef{
		exactFactRef(t, runforkrevision.FamilyEvents, f.eventID),
		exactFactRef(t, runforkrevision.FamilyEntityMutations, f.mutationID),
		exactFactRef(t, runforkrevision.FamilyEntityMetadata, f.entityID),
		exactFactRef(t, runforkrevision.FamilyEventDeliveries, f.deliveryID),
		exactFactRef(t, runforkrevision.FamilyCommittedReplayScopes, f.eventID),
		exactFactRef(t, runforkrevision.FamilyEventReceipts, f.receiptID),
		exactFactRef(t, runforkrevision.FamilyDeadLetters, f.deadLetterID),
		exactFactRef(t, runforkrevision.FamilyTimers, f.timerID),
		exactFactRef(t, runforkrevision.FamilyAgentSessions, f.sessionID),
		exactFactRef(t, runforkrevision.FamilyAgentTurns, f.turnID),
		exactFactRef(t, runforkrevision.FamilyAgentConversationAudits, f.auditID),
		exactFactRef(t, runforkrevision.FamilyReplyContexts, f.replyID),
	}
	intent, err := runforkrevision.FanOutIntentFact(exactMatrixIntentKey(f))
	if err != nil {
		t.Fatal(err)
	}
	return append(refs, intent)
}

func exactEffects(t *testing.T, runID string, refs ...runforkrevision.FactRef) *runforkrevision.Effects {
	t.Helper()
	effects := runforkrevision.NewEffects()
	if err := effects.AddFacts(runID, refs...); err != nil {
		t.Fatal(err)
	}
	return effects
}

type exactLedgerRow struct {
	Revision int64
	Family   string
	Key      string
	Body     any
	Present  bool
}

func exactLedger(t *testing.T, ctx context.Context, tx *sql.Tx, runID string) []exactLedgerRow {
	t.Helper()
	rows, err := tx.QueryContext(ctx, `SELECT revision,family,fact_key,fact,present FROM run_fork_fact_revisions WHERE run_id=$1 ORDER BY revision,family,fact_key`, runID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var result []exactLedgerRow
	for rows.Next() {
		var row exactLedgerRow
		var body []byte
		if err := rows.Scan(&row.Revision, &row.Family, &row.Key, &body, &row.Present); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(body, &row.Body); err != nil {
			t.Fatal(err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

// Both selections see the identical domain mutation and prior ledger. Only the
// whole-family oracle's finalization is rolled back; the exact result is kept.
func compareExactWithWhole(t *testing.T, ctx context.Context, tx *sql.Tx, s exactFactStore, runID string, exact *runforkrevision.Effects, wantRevision int64, wantChanged bool) {
	t.Helper()
	whole, err := runforkrevision.ForRun(runID, runforkrevision.AllFamilies()...)
	if err != nil {
		t.Fatal(err)
	}
	mustExecRunForkRevisionMatrix(t, ctx, tx, `SAVEPOINT exact_oracle`)
	want, err := finalizeRunForkRevisionMatrix(ctx, tx, s.postgres, whole)
	if err != nil {
		t.Fatalf("whole oracle: %v", err)
	}
	wantLedger := exactLedger(t, ctx, tx, runID)
	mustExecRunForkRevisionMatrix(t, ctx, tx, `ROLLBACK TO SAVEPOINT exact_oracle`)
	mustExecRunForkRevisionMatrix(t, ctx, tx, `RELEASE SAVEPOINT exact_oracle`)
	got, err := finalizeRunForkRevisionMatrix(ctx, tx, s.postgres, exact)
	if err != nil {
		t.Fatalf("exact finalization: %v", err)
	}
	if !reflect.DeepEqual(got, want) || got[runID] != (runforkrevision.Result{Revision: wantRevision, Changed: wantChanged}) {
		t.Fatalf("exact=%#v whole=%#v, want revision=%d changed=%v", got, want, wantRevision, wantChanged)
	}
	if gotLedger := exactLedger(t, ctx, tx, runID); !reflect.DeepEqual(gotLedger, wantLedger) {
		t.Fatalf("exact/full ledger differs:\nexact=%#v\nwhole=%#v", gotLedger, wantLedger)
	}
	if err := validateRunForkRevisionMatrix(ctx, tx, s.postgres, runID); err != nil {
		t.Fatalf("complete validation after exact capture: %v", err)
	}
}

func exactTransaction(t *testing.T, s exactFactStore, fn func(context.Context, *sql.Tx)) {
	t.Helper()
	ctx := testAuthorActivityContext()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	fn(ctx, tx)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestRunForkExactFactsThirteenFamilyDifferentialBothStores(t *testing.T) {
	eachExactFactStore(t, func(t *testing.T, s exactFactStore) {
		f := newExactFactFixture(t, s)
		refs := exactMatrixRefs(t, f)
		exactTransaction(t, s, func(ctx context.Context, tx *sql.Tx) {
			seedRunForkRevisionMatrixFacts(t, ctx, tx, f, true, s.postgres)
			compareExactWithWhole(t, ctx, tx, s, f.runID, exactEffects(t, f.runID, refs...), 1, true)
		})
		assertRunForkRevisionMatrixShape(t, loadRunForkRevisionMatrixFacts(t, testAuthorActivityContext(), s.db, f.runID, 1), true)
		t.Run("duplicate_union_and_excluded_churn", func(t *testing.T) {
			exactTransaction(t, s, func(ctx context.Context, tx *sql.Tx) {
				mustExecRunForkRevisionMatrix(t, ctx, tx, `UPDATE entity_state SET current_state='excluded',updated_at=$1 WHERE entity_id=$2 AND run_id=$3`, f.at.Add(time.Second), f.entityID, f.runID)
				mustExecRunForkRevisionMatrix(t, ctx, tx, `UPDATE event_receipts SET side_effects=$1 WHERE receipt_id=$2`, `{"excluded":true}`, f.receiptID)
				effects := exactEffects(t, f.runID, refs...)
				if err := effects.AddFacts(f.runID, refs...); err != nil {
					t.Fatal(err)
				}
				compareExactWithWhole(t, ctx, tx, s, f.runID, effects, 1, false)
			})
		})
		t.Run("partial_update_preserves_unselected_facts", func(t *testing.T) {
			exactTransaction(t, s, func(ctx context.Context, tx *sql.Tx) {
				mustExecRunForkRevisionMatrix(t, ctx, tx, `UPDATE entity_state SET name='Exact Updated' WHERE run_id=$1 AND entity_id=$2`, f.runID, f.entityID)
				mustExecRunForkRevisionMatrix(t, ctx, tx, `UPDATE event_receipts SET reason_code='exact_update' WHERE receipt_id=$1`, f.receiptID)
				compareExactWithWhole(t, ctx, tx, s, f.runID, exactEffects(t, f.runID,
					exactFactRef(t, runforkrevision.FamilyEntityMetadata, f.entityID), exactFactRef(t, runforkrevision.FamilyEventReceipts, f.receiptID)), 2, true)
			})
		})
		generatedReceipt := uuid.NewString()
		t.Run("generated_receipt_and_sibling", func(t *testing.T) {
			exactTransaction(t, s, func(ctx context.Context, tx *sql.Tx) {
				mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO event_receipts (receipt_id,event_id,subscriber_type,subscriber_id,outcome,side_effects,processed_at) VALUES ($1,$2,'platform','exact-generated','success',$3,$4)`, generatedReceipt, f.eventID, `{}`, f.at)
				compareExactWithWhole(t, ctx, tx, s, f.runID, exactEffects(t, f.runID, exactFactRef(t, runforkrevision.FamilyEventReceipts, generatedReceipt)), 3, true)
			})
		})
		refs = append(refs, exactFactRef(t, runforkrevision.FamilyEventReceipts, generatedReceipt))
		t.Run("whole_dominates_in_both_composition_orders", func(t *testing.T) {
			for _, wholeFirst := range []bool{false, true} {
				exactTransaction(t, s, func(ctx context.Context, tx *sql.Tx) {
					effects := runforkrevision.NewEffects()
					addWhole := func() {
						if err := effects.Add(f.runID, runforkrevision.FamilyEventReceipts); err != nil {
							t.Fatal(err)
						}
					}
					if wholeFirst {
						addWhole()
					}
					// An absent valid coordinate proves whole dominance, not exact fallback.
					if err := effects.AddFacts(f.runID, exactFactRef(t, runforkrevision.FamilyEventReceipts, uuid.NewString())); err != nil {
						t.Fatal(err)
					}
					if !wholeFirst {
						addWhole()
					}
					compareExactWithWhole(t, ctx, tx, s, f.runID, effects, 3, false)
				})
			}
		})
		t.Run("retained_run_tombstones_and_repeat_noop", func(t *testing.T) {
			exactTransaction(t, s, func(ctx context.Context, tx *sql.Tx) {
				deleteRunForkRevisionMatrixFacts(t, ctx, tx, f)
				effects := exactEffects(t, f.runID, refs...)
				compareExactWithWhole(t, ctx, tx, s, f.runID, effects, 4, true)
				compareExactWithWhole(t, ctx, tx, s, f.runID, effects, 4, false)
			})
			facts := loadRunForkRevisionMatrixFacts(t, testAuthorActivityContext(), s.db, f.runID, 4)
			if len(facts) != 14 {
				t.Fatalf("tombstones=%d, want thirteen families plus generated sibling", len(facts))
			}
			for _, fact := range facts {
				if fact.Present {
					t.Fatalf("not tombstoned: %#v", fact)
				}
			}
		})
	})
}
