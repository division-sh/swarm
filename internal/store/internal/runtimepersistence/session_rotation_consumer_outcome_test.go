package runtimepersistence

import (
	"context"
	"database/sql"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/testutil"
)

// Rotation adopts the exact native successor, independently of retired
// run-completion callbacks.
func TestPostgresRuntimeSessionRotationPreservesCommittedHandoffOutcome(t *testing.T) {
	for _, operation := range []string{"turn", "parse", "prepare"} {
		t.Run(operation, func(t *testing.T) {
			_, db, _ := testutil.StartPostgres(t)
			store := admitTestPostgresStore(t, db)
			base := runtimeeffects.WithDifferentOwner(testAuthorActivityContext(), runtimeeffects.OwnerBuildTestInfrastructure)
			base = originalStoreFixtureContext(t, base, store)
			seedSpecAgent(t, base, store, "a1", "", "")
			seedSpecMemoryRun(t, base, db)
			identity := specMemoryIdentity("a1", "global")
			memory := agentmemory.Plan{Enabled: true}
			ctx := agentmemory.WithExecution(base, memory, identity)
			ctx = originalStoreFixtureContext(t, ctx, store)
			lease, err := store.Acquire(ctx, identity, "worker-1")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.ReleaseOutcome(ctx, lease); err != nil {
				t.Fatal(err)
			}
			oldID := lease.SessionID
			var predecessor *sessions.Lease
			if operation == "turn" || operation == "parse" {
				predecessor, err = store.Acquire(ctx, identity, "worker-1")
				if err != nil {
					t.Fatal(err)
				}
			}
			session := &runtimellm.Session{ID: oldID, ProviderSessionID: "provider-old", AgentID: identity.AgentID(), Memory: memory, MemoryIdentity: identity, TurnCount: 1, ParseFailures: 1, Messages: []runtimellm.Message{{Role: "assistant", Content: "done"}}}
			var result *sessions.Lease
			switch operation {
			case "turn":
				result, err = runtimellm.MaybeRotateAfterTurn(ctx, session, store, predecessor, 1, nil)
			case "parse":
				result, err = runtimellm.MaybeRotateAfterParseFailures(ctx, session, store, predecessor, 1, nil)
			default:
				cfg := &config.Config{}
				cfg.LLM.Session.RotateAfterTurns = 1
				err = runtimellm.NewMockRuntime(cfg, store, "worker-1", nil, nil, nil, runtimellm.MockRuntimeOptions{}).PrepareManagedSession(ctx, session)
			}
			if err != nil {
				t.Fatalf("rotate native session: %v", err)
			}
			prepared := operation == "prepare"
			var committedID, predecessorStatus string
			var holder sql.NullString
			if err := db.QueryRowContext(base, `SELECT session_id::text, lease_holder FROM agent_sessions WHERE run_id=$1::uuid AND status='active'`, identity.RunID).Scan(&committedID, &holder); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRowContext(base, `SELECT status FROM agent_sessions WHERE session_id=$1::uuid`, oldID).Scan(&predecessorStatus); err != nil || predecessorStatus != "terminated" || holder.Valid == prepared {
				t.Fatalf("rotation lost predecessor/release conservation: status=%s holder=%+v err=%v", predecessorStatus, holder, err)
			}
			var count int
			if err := db.QueryRowContext(base, `SELECT COUNT(*) FROM agent_sessions WHERE run_id=$1::uuid`, identity.RunID).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != 2 {
				t.Fatalf("session rows=%d want=2", count)
			}
			if committedID == "" || session.ID != committedID || session.ProviderSessionID != "" || session.TurnCount != 0 || session.ParseFailures != 0 || len(session.Messages) != 1 || session.Messages[0].Role != "system" {
				t.Fatalf("committed successor not adopted: %+v durable=%s", session, committedID)
			}
			if !prepared && (result == nil || result.SessionID != committedID || result.RetriesFromSessionID != oldID) {
				t.Fatalf("committed lease not returned: %+v", result)
			}
			if !prepared {
				if _, err := store.ReleaseOutcome(ctx, result); err != nil {
					t.Fatal(err)
				}
			}
			current, err := store.Acquire(ctx, identity, "worker-2")
			if err != nil {
				t.Fatalf("committed session still leased: %v", err)
			}
			if current.SessionID != session.ID {
				t.Fatalf("successor changed on reacquire: %+v session=%s", current, session.ID)
			}
			if _, err := store.ReleaseOutcome(context.WithoutCancel(ctx), current); err != nil {
				t.Fatal(err)
			}
		})
	}
}
