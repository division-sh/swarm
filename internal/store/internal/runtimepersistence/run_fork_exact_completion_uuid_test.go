package runtimepersistence

import (
	"context"
	"database/sql"
	"reflect"
	"strings"
	"testing"
	"time"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/google/uuid"
)

func TestRunForkExactCompletionWriterPreservesTurnUUIDSpellingsBothStores(t *testing.T) {
	eachExactFactStore(t, func(t *testing.T, s exactFactStore) {
		store := s.selected.(completionSettlementTestStore)
		if s.postgres {
			store = admitTestPostgresStore(t, s.db)
		}
		for _, spelling := range []string{"canonical", "uppercase", "compact"} {
			t.Run(spelling, func(t *testing.T) {
				fixture := newCompletionSettlementFixture(t, store, s.db, !s.postgres)
				inputID := fixture.authority.Target.ID
				switch spelling {
				case "uppercase":
					inputID = strings.ToUpper(inputID)
				case "compact":
					inputID = strings.ReplaceAll(inputID, "-", "")
				}
				if inputID != fixture.authority.Target.ID {
					fixture.authority.Target.ID = inputID
					// The origin event is derived from the chosen turn spelling.
					// Admit its real delivery before authorizing the completion.
					fixture.origin = claimCompletionOriginForTest(t, testAuthorActivityContext(), fixture.store, fixture.authority, time.Now().UTC())
				}
				ctx := runtimeeffects.WithLogicalOperationIdentity(fixture.contextFor(fixture.authority), "completion-uuid:"+spelling)
				handle := beginObservedCompletionForSettlementTest(t, ctx, "claude_cli", "completion-uuid:"+spelling)
				if got := handle.Attempt().Authority.Target.ID; got != inputID {
					t.Fatalf("authorization rewrote turn UUID: got=%q want=%q", got, inputID)
				}
				settlement := completionSettlementForTest(t, handle.Attempt().Authority.Target, fixture, "claude_cli", "provider-head-current", "provider-head-next")
				if settlement.AgentTurn.TurnID != inputID {
					t.Fatal("settlement fixture rewrote turn UUID")
				}
				result, err := handle.SettleCompletion(ctx, settlement)
				if err != nil || !result.Committed || result.Disposition != runtimeeffects.CompletionSettlementCurrent || result.Drained() {
					t.Fatalf("actual completion settlement: result=%+v err=%v", result, err)
				}
				requireProviderHead(t, s.db, !s.postgres, fixture.sessionID, "provider-head-next")
				requireCompletionSettlementRows(t, fixture, handle.Attempt().AttemptID, inputID, runtimeeffects.StateSettled, 1, 0)
				operationFrame := loadCompletionFrameBytes(t, fixture, "runtime_external_effect_operations", "operation_id", handle.Attempt().OperationID)
				assertCompletionFrameBytes(t, fixture, "agent_turns", "turn_id", inputID, operationFrame)

				exactTransaction(t, s, func(ctx context.Context, tx *sql.Tx) {
					var storedTurnID, storedRunID string
					if err := tx.QueryRowContext(ctx, `SELECT CAST(turn_id AS TEXT), CAST(run_id AS TEXT) FROM agent_turns WHERE turn_id=$1`, inputID).Scan(&storedTurnID, &storedRunID); err != nil {
						t.Fatal(err)
					}
					wantTurnID := inputID
					if s.postgres {
						wantTurnID = uuid.MustParse(inputID).String()
					}
					if storedTurnID != wantTurnID || storedRunID != fixture.authority.Target.RunID {
						t.Fatalf("stored coordinates: turn=%q want=%q run=%q want=%q", storedTurnID, wantTurnID, storedRunID, fixture.authority.Target.RunID)
					}
					before := exactLedger(t, ctx, tx, storedRunID)
					var turns int
					for _, row := range before {
						if row.Family != string(runforkrevision.FamilyAgentTurns) {
							continue
						}
						turns++
						if row.Key != storedTurnID || !row.Present {
							t.Fatalf("actual completion writer ledger: %+v", row)
						}
					}
					if turns != 1 {
						t.Fatalf("completion writer captured %d turn facts, want one", turns)
					}
					// Whole-family readback must find no omission or byte difference
					// in the ledger produced by the real completion transaction.
					whole, err := runforkrevision.ForRun(storedRunID, runforkrevision.FamilyAgentTurns)
					if err != nil {
						t.Fatal(err)
					}
					verified, err := finalizeRunForkRevisionMatrix(ctx, tx, s.postgres, whole)
					if err != nil || len(verified) != 1 || verified[storedRunID].Changed || !reflect.DeepEqual(before, exactLedger(t, ctx, tx, storedRunID)) {
						t.Fatalf("whole-family control changed actual completion capture: result=%+v err=%v", verified, err)
					}
					t.Logf("actual completion+exact ledger+whole control PASS: input_turn=%q stored_turn=%q stored_run=%q", inputID, storedTurnID, storedRunID)
				})
			})
		}
	})
}
