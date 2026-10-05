package serveapp

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestIssue2564DecisionCardReevaluatesSameLeaseAfterRealCASLossBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			rt, owner := newMailboxCompletionRuntime(t, backend)
			f := mailboxCompletionFixtureInRuntime(t, rt, owner)
			params := mailboxPrincipalMutationParams(t, f, decisioncard.AnchorKindStageGate, "mailbox.decide")
			req := mailboxPrincipalRequest(t, rt, "mailbox.decide", params)
			mutation, err := mailboxCardMutation(req, params)
			if err != nil {
				t.Fatal(err)
			}
			losses, attempts := 0, 0
			fault := &mailboxPostCommitFault{beforeCommit: func(ctx context.Context, command pipeline.DecisionCardMutationCommand) error {
				attempts++
				if command.GateState == nil {
					return fmt.Errorf("test did not reach the gate write")
				}
				if losses == 9 {
					return nil
				}
				state := command.GateState
				// This is a fault cut between R1 preparation and the real SQL
				// CAS, not a forged typed error or changed request input.
				result, err := rt.DB.ExecContext(ctx, `UPDATE flow_instances SET revision=revision+1 WHERE run_id=$1 AND entity_id=$2 AND revision=$3`, state.Identity.RunID, state.EntityID, state.ExpectedRevision)
				if err != nil {
					return err
				}
				if changed, err := result.RowsAffected(); err != nil || changed != 1 {
					return fmt.Errorf("CAS fault did not change exact header: rows=%d err=%v", changed, err)
				}
				losses++
				return nil
			}}
			response, replayed, err := mailboxFaultCoordinator(t, rt, fault).CommitDecisionCardMutation(f.ctx, req, mutation)
			if err != nil || replayed || attempts != 10 || losses != 9 {
				t.Fatalf("same request did not survive contention: attempts=%d losses=%d replay=%v err=%v", attempts, losses, replayed, err)
			}
			var result map[string]any
			if err := json.Unmarshal(response, &result); err != nil || result["ok"] != true {
				t.Fatalf("committed response=%s err=%v", response, err)
			}
			var count int
			if err := rt.DB.QueryRow(`SELECT count(*) FROM api_idempotency WHERE idempotency_key=$1`, req.IdempotencyKey).Scan(&count); err != nil || count != 1 {
				t.Fatalf("request completion count=%d err=%v", count, err)
			}
			if err := rt.DB.QueryRow(`SELECT count(*) FROM decision_card_changes WHERE card_id=$1 AND change_type='decided'`, req.ResourceID).Scan(&count); err != nil || count != 1 {
				t.Fatalf("partial or duplicate decision effects=%d err=%v", count, err)
			}
			var replay map[string]any
			requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.decide", params, &replay)
			if replay["idempotency_replayed"] != true {
				t.Fatalf("request lost durable replay: %v", replay)
			}
			delete(replay, "idempotency_replayed")
			if !equalJSONMaps(result, replay) {
				t.Fatalf("replayed different result: original=%v replay=%v", result, replay)
			}
		})
	}
}

func equalJSONMaps(a, b map[string]any) bool {
	left, leftErr := json.Marshal(a)
	right, rightErr := json.Marshal(b)
	return leftErr == nil && rightErr == nil && string(left) == string(right)
}
