package runtimepersistence

import (
	"context"
	"database/sql"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

func TestRunForkExactFactsAdmittedAuditMoveBothStores(t *testing.T) {
	eachExactFactStore(t, func(t *testing.T, s exactFactStore) {
		old, next := newExactFactFixture(t, s), newExactFactFixture(t, s)
		ensure := func(ctx context.Context, tx *sql.Tx, effects *runforkrevision.Effects, f runForkRevisionMatrixFixture) error {
			identity := mustTestAgentIdentityForRun(f.runID, "revision-matrix-agent", "")
			record := llm.AgentTurnRecord{RunID: f.runID, AgentID: identity.AgentID(), Identity: identity, Memory: agentmemory.PlatformDefault(), SessionID: old.auditID}
			// This existing private writer already admits stateless-audit ownership
			// replacement and itself contributes both previous and destination runs.
			switch selected := s.selected.(type) {
			case *PostgresStore:
				return selected.lLMPostgresOwner.EnsureCompletionTurnMemoryTx(ctx, tx, effects, record)
			case *SQLiteRuntimeStore:
				return selected.lLMSQLiteOwner.EnsureCompletionTurnMemoryTx(ctx, tx, effects, record, f.at)
			}
			panic("unsupported selected store")
		}
		exactTransaction(t, s, func(ctx context.Context, tx *sql.Tx) {
			effects := runforkrevision.NewEffects()
			if err := ensure(ctx, tx, effects, old); err != nil {
				t.Fatal(err)
			}
			compareExactWithWhole(t, ctx, tx, s, old.runID, effects, 1, true)
		})
		t.Run("old_only_omits_current_owner", func(t *testing.T) {
			exactRollbackTransaction(t, s, func(ctx context.Context, tx *sql.Tx) {
				if err := ensure(ctx, tx, runforkrevision.NewEffects(), next); err != nil {
					t.Fatal(err)
				}
				oldOnly := exactEffects(t, old.runID, exactFactRef(t, runforkrevision.FamilyAgentConversationAudits, old.auditID))
				if _, err := finalizeRunForkRevisionMatrix(ctx, tx, s.postgres, oldOnly); err == nil {
					t.Fatal("old-only audit move omitted existing destination owner")
				}
			})
		})
		t.Run("actual_writer_contributes_both_runs", func(t *testing.T) {
			exactTransaction(t, s, func(ctx context.Context, tx *sql.Tx) {
				effects := runforkrevision.NewEffects()
				if err := ensure(ctx, tx, effects, next); err != nil {
					t.Fatal(err)
				}
				whole := runforkrevision.NewEffects()
				for _, runID := range []string{next.runID, old.runID} {
					if err := whole.Add(runID, runforkrevision.AllFamilies()...); err != nil {
						t.Fatal(err)
					}
				}
				mustExecRunForkRevisionMatrix(t, ctx, tx, `SAVEPOINT audit_move_oracle`)
				want, err := finalizeRunForkRevisionMatrix(ctx, tx, s.postgres, whole)
				if err != nil {
					t.Fatal(err)
				}
				oldLedger, nextLedger := exactLedger(t, ctx, tx, old.runID), exactLedger(t, ctx, tx, next.runID)
				mustExecRunForkRevisionMatrix(t, ctx, tx, `ROLLBACK TO SAVEPOINT audit_move_oracle`)
				mustExecRunForkRevisionMatrix(t, ctx, tx, `RELEASE SAVEPOINT audit_move_oracle`)
				got, err := finalizeRunForkRevisionMatrix(ctx, tx, s.postgres, effects)
				if err != nil || !reflect.DeepEqual(got, want) || len(got) != 2 {
					t.Fatalf("actual audit move effects=%#v whole=%#v err=%v", got, want, err)
				}
				if got[old.runID] != (runforkrevision.Result{Revision: 2, Changed: true}) || got[next.runID] != (runforkrevision.Result{Revision: 1, Changed: true}) {
					t.Fatalf("audit move revisions=%#v", got)
				}
				if !reflect.DeepEqual(oldLedger, exactLedger(t, ctx, tx, old.runID)) || !reflect.DeepEqual(nextLedger, exactLedger(t, ctx, tx, next.runID)) {
					t.Fatal("actual audit move ledger differs from whole capture")
				}
				if len(oldLedger) != 2 || oldLedger[1].Present || oldLedger[1].Key != old.auditID || len(nextLedger) != 1 || !nextLedger[0].Present || nextLedger[0].Key != old.auditID {
					t.Fatalf("audit move tombstone/presence old=%#v next=%#v", oldLedger, nextLedger)
				}
				for _, runID := range []string{old.runID, next.runID} {
					if err := validateRunForkRevisionMatrix(ctx, tx, s.postgres, runID); err != nil {
						t.Fatal(err)
					}
				}
				var currentRun string
				var turns int
				if err := tx.QueryRowContext(ctx, `SELECT CAST(run_id AS TEXT),turn_count FROM agent_conversation_audits WHERE session_id=$1`, old.auditID).Scan(&currentRun, &turns); err != nil {
					t.Fatal(err)
				}
				if currentRun != next.runID || turns != 2 {
					t.Fatalf("existing audit move current run=%s turns=%d", currentRun, turns)
				}
			})
		})
	})
}
