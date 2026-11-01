package runtimepersistence

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

func proveClaudeBackingOrigin(t *testing.T, fixture completionSettlementFixture) {
	t.Helper()
	if _, err := fixture.db.Exec(`UPDATE agent_sessions SET runtime_state='{}' WHERE session_id=$1`, fixture.sessionID); err != nil {
		t.Fatal(err)
	}
	identity := fixture.authority.Target.AgentIdentity
	root, ok := correlation.SourceArtifactFactFromContext(fixture.context)
	if !ok {
		t.Fatal("missing admitted source")
	}
	head := ""
	first := ""
	var request []byte
	for turn := 1; turn <= 100; turn++ {
		authority := fixture.authority
		authority.Target.ID, authority.BudgetScopes = uuid.NewString(), nil
		fixture.origin = claimCompletionOriginForTest(t, fixture.original, fixture.store, authority, time.Now().UTC())
		ctx := effects.WithLogicalOperationIdentity(fixture.contextFor(authority), fmt.Sprintf("backing-origin:%d", turn))
		handle := beginObservedCompletionForSettlementTest(t, ctx, "claude_cli", fmt.Sprintf("backing-request:%d", turn))
		settlement := completionSettlementForTest(t, authority.Target, fixture, "claude_cli", head, handle.Attempt().AttemptID)
		request, _ = json.Marshal(map[string]string{"provider_session_id": head})
		settlement.AgentTurn.RequestPayload = request
		result, err := handle.SettleCompletion(ctx, settlement)
		if err != nil || !result.Committed || result.NoCurrentProjection() {
			t.Fatalf("commit backing lineage turn %d: %+v: %v", turn, result, err)
		}
		head = handle.Attempt().AttemptID
		if turn == 1 {
			first = head
		}
		if turn == 1 || turn == 10 || turn == 100 {
			for sample := 0; sample < 5; sample++ {
				started := time.Now()
				origin, err := effects.ClaudeBackingOrigin(ctx, identity, fixture.sessionID, head)
				elapsed := time.Since(started)
				if err != nil || origin != root.BundleHash() {
					t.Fatalf("resolve exact backing at %d: origin=%q err=%v", turn, origin, err)
				}
				t.Logf("claude_backing_read sqlite=%t depth=%d sample=%d queries=1 duration=%s", fixture.sqlite, turn, sample, elapsed)
			}
		}
	}
	for _, fault := range []string{"missing_head", "stale_head", "foreign_actor", "foreign_session", "missing_parent", "cycle", "contradictory_parent", "contradictory_bundle"} {
		t.Run("backing_"+fault, func(t *testing.T) {
			actor, session, current := identity, fixture.sessionID, head
			query := `UPDATE agent_turns SET request_payload=$1 WHERE completion_attempt_id=$2`
			switch fault {
			case "missing_head":
				current = uuid.NewString()
			case "stale_head":
				current = first
			case "foreign_actor":
				actor.RunID = uuid.NewString()
			case "foreign_session":
				session = uuid.NewString()
			case "missing_parent", "cycle", "contradictory_parent":
				parent := uuid.NewString()
				if fault == "cycle" {
					parent = head
				}
				changed, _ := json.Marshal(map[string]string{"provider_session_id": parent})
				if fault == "contradictory_parent" {
					changed = []byte(`{}`)
				}
				if _, err := fixture.db.Exec(query, string(changed), head); err != nil {
					t.Fatal(err)
				}
				defer func() {
					if _, err := fixture.db.Exec(query, string(request), head); err != nil {
						t.Error(err)
					}
				}()
			case "contradictory_bundle":
				query = `UPDATE runtime_external_effect_operations SET bundle_hash=$1 WHERE operation_id=(SELECT operation_id FROM runtime_external_effect_attempts WHERE attempt_id=$2)`
				if _, err := fixture.db.Exec(query, "contradictory", head); err != nil {
					t.Fatal(err)
				}
				defer func() {
					if _, err := fixture.db.Exec(query, root.BundleHash(), head); err != nil {
						t.Error(err)
					}
				}()
			}
			origin, err := effects.ClaudeBackingOrigin(fixture.context, actor, session, current)
			failure, typed := failures.As(err)
			if origin != "" || !typed || failure.Failure.Detail.Code != "claude_provider_backing_lineage_invalid" {
				t.Fatalf("%s accepted or lost typed refusal: origin=%q err=%v", fault, origin, err)
			}
		})
	}
}
