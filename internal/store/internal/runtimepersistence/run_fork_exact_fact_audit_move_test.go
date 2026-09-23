package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

func TestRunForkExactFactsAdmittedAuditMoveBothStores(t *testing.T) {
	eachExactFactStore(t, func(t *testing.T, s exactFactStore) {
		old, next := newExactFactFixture(t, s), newExactFactFixture(t, s)
		ctx := testAuthorActivityContext()
		ensure := func(ctx context.Context, attempt *mutationprotocol.Attempt, f runForkRevisionMatrixFixture) error {
			identity := mustTestAgentIdentityForRun(f.runID, "revision-matrix-agent", "")
			record := llm.AgentTurnRecord{RunID: f.runID, AgentID: identity.AgentID(), Identity: identity, Memory: agentmemory.PlatformDefault(), SessionID: old.auditID}
			// The actual writer contributes both the previous and destination owners.
			switch selected := s.selected.(type) {
			case *PostgresStore:
				return selected.lLMPostgresOwner.EnsureCompletionTurnMemoryTx(ctx, attempt, record)
			case *SQLiteRuntimeStore:
				return selected.lLMSQLiteOwner.EnsureCompletionTurnMemoryTx(ctx, attempt, record, f.at)
			default:
				return fmt.Errorf("unsupported exact fact store %T", s.selected)
			}
		}
		capture := func(ctx context.Context, attempt *mutationprotocol.Attempt, runIDs ...string) (map[string][]exactLedgerRow, map[string]runforkrevision.Result, error) {
			ledgers := make(map[string][]exactLedgerRow, len(runIDs))
			var results map[string]runforkrevision.Result
			err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
				whole := runforkrevision.NewEffects()
				for _, runID := range runIDs {
					if err := whole.Add(runID, runforkrevision.AllFamilies()...); err != nil {
						return err
					}
				}
				mustExecRunForkRevisionMatrix(t, ctx, tx, `SAVEPOINT audit_move_oracle`)
				var err error
				results, err = finalizeRunForkRevisionMatrix(ctx, tx, s.postgres, whole)
				if err != nil {
					return err
				}
				for _, runID := range runIDs {
					ledgers[runID] = exactLedger(t, ctx, tx, runID)
				}
				mustExecRunForkRevisionMatrix(t, ctx, tx, `ROLLBACK TO SAVEPOINT audit_move_oracle`)
				mustExecRunForkRevisionMatrix(t, ctx, tx, `RELEASE SAVEPOINT audit_move_oracle`)
				return nil
			})
			return ledgers, results, err
		}
		first := runExactFactProtocol(ctx, s, func(ctx context.Context, attempt *mutationprotocol.Attempt) ([]exactLedgerRow, error) {
			if err := ensure(ctx, attempt, old); err != nil {
				return nil, err
			}
			ledgers, results, err := capture(ctx, attempt, old.runID)
			if err != nil || results[old.runID] != (runforkrevision.Result{Revision: 1, Changed: true}) {
				return nil, fmt.Errorf("initial audit oracle=%#v err=%v", results, err)
			}
			return ledgers[old.runID], nil
		})
		oldLedger, acknowledged := first.Value()
		if !acknowledged || first.Err() != nil {
			t.Fatalf("initial actual writer failed: acknowledged=%v err=%v", acknowledged, first.Err())
		}
		assertExactAuditLedger(t, s, old.runID, oldLedger)
		t.Run("old_only_omits_current_owner", func(t *testing.T) {
			rollback := errors.New("expected old-only audit move rollback")
			result := runExactFactProtocol(ctx, s, func(ctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
				if err := ensure(ctx, attempt, next); err != nil {
					return struct{}{}, err
				}
				err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
					oldOnly := exactEffects(t, old.runID, exactFactRef(t, runforkrevision.FamilyAgentConversationAudits, old.auditID))
					if _, err := finalizeRunForkRevisionMatrix(ctx, tx, s.postgres, oldOnly); err == nil {
						return errors.New("old-only audit move omitted existing destination owner")
					}
					return nil
				})
				if err != nil {
					return struct{}{}, err
				}
				return struct{}{}, rollback
			})
			if result.Acknowledged() || !errors.Is(result.Err(), rollback) {
				t.Fatalf("negative audit move changed commit outcome: %+v", result)
			}
			assertExactAuditLedger(t, s, old.runID, oldLedger)
		})
		t.Run("actual_writer_contributes_both_runs", func(t *testing.T) {
			type proof struct {
				ledgers map[string][]exactLedgerRow
				results map[string]runforkrevision.Result
			}
			result := runExactFactProtocol(ctx, s, func(ctx context.Context, attempt *mutationprotocol.Attempt) (proof, error) {
				if err := ensure(ctx, attempt, next); err != nil {
					return proof{}, err
				}
				ledgers, results, err := capture(ctx, attempt, old.runID, next.runID)
				return proof{ledgers: ledgers, results: results}, err
			})
			want, acknowledged := result.Value()
			if !acknowledged || result.Err() != nil {
				t.Fatalf("actual audit move: acknowledged=%v err=%v", acknowledged, result.Err())
			}
			if want.results[old.runID] != (runforkrevision.Result{Revision: 2, Changed: true}) || want.results[next.runID] != (runforkrevision.Result{Revision: 1, Changed: true}) {
				t.Fatalf("audit move oracle revisions=%#v", want.results)
			}
			assertExactAuditLedger(t, s, old.runID, want.ledgers[old.runID])
			assertExactAuditLedger(t, s, next.runID, want.ledgers[next.runID])
			oldRows, nextRows := want.ledgers[old.runID], want.ledgers[next.runID]
			if len(oldRows) != 2 || oldRows[1].Present || oldRows[1].Key != old.auditID || len(nextRows) != 1 || !nextRows[0].Present || nextRows[0].Key != old.auditID {
				t.Fatalf("audit move tombstone/presence old=%#v next=%#v", oldRows, nextRows)
			}
			exactTransaction(t, s, func(ctx context.Context, tx *sql.Tx) {
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

func assertExactAuditLedger(t *testing.T, s exactFactStore, runID string, want []exactLedgerRow) {
	t.Helper()
	exactTransaction(t, s, func(ctx context.Context, tx *sql.Tx) {
		if got := exactLedger(t, ctx, tx, runID); !reflect.DeepEqual(got, want) {
			t.Fatalf("actual writer ledger differs from full capture: got=%#v want=%#v", got, want)
		}
	})
}
