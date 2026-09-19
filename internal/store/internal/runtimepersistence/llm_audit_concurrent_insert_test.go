package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestLLMPostgresConcurrentFirstAuditInsertCapturesBothOwners(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	store := newPostgresStoreWithBackend(mustPostgresBackend(db))
	s := exactFactStore{db: db, postgres: true, selected: store}
	first, next := newExactFactFixture(t, s), newExactFactFixture(t, s)
	ctx, cancel := context.WithTimeout(testAuthorActivityContext(), 15*time.Second)
	defer cancel()
	sessionID := uuid.NewString()
	record := func(run string) runtimellm.AgentTurnRecord {
		identity := mustTestAgentIdentityForRun(run, "revision-matrix-agent", "")
		return runtimellm.AgentTurnRecord{SessionID: sessionID, RunID: run, Identity: identity,
			AgentID: identity.AgentID(), FlowInstance: identity.FlowInstance(), Memory: agentmemory.PlatformDefault()}
	}
	a, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Rollback()
	b, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Rollback()
	var aPID, bPID int
	if err := a.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&aPID); err != nil {
		t.Fatal(err)
	}
	if err := b.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&bPID); err != nil {
		t.Fatal(err)
	}
	aEffects := runforkrevision.NewEffects()
	if err := store.lLMPostgresOwner.EnsureCompletionTurnMemoryTx(ctx, a, aEffects, record(first.runID)); err != nil {
		t.Fatal(err)
	}
	finished := make(chan error, 1)
	go func() {
		effects := runforkrevision.NewEffects()
		if err := store.lLMPostgresOwner.EnsureCompletionTurnMemoryTx(ctx, b, effects, record(next.runID)); err != nil {
			finished <- err
			return
		}
		results, err := runforkrevision.FinalizePostgres(ctx, b, effects)
		if err != nil {
			finished <- err
			return
		}
		if len(results) != 2 || !results[first.runID].Changed || !results[next.runID].Changed {
			finished <- fmt.Errorf("concurrent first-insert lost prior/resulting owner: %+v", results)
			return
		}
		finished <- b.Commit()
	}()
	// Wait on the actual unique-insert lock, not a timing guess or a hook. B's
	// preceding SELECT saw no committed audit; A is still its uncommitted writer.
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var query string
		var blocked bool
		if err := db.QueryRowContext(ctx, `SELECT query, $2::int = ANY(pg_blocking_pids(pid))
			FROM pg_stat_activity WHERE pid=$1`, bPID, aPID).Scan(&query, &blocked); err != nil {
			t.Fatal(err)
		}
		if blocked && strings.Contains(query, "INSERT INTO agent_conversation_audits") {
			break
		}
		select {
		case err := <-finished:
			t.Fatalf("second writer did not wait on first insert: %v", err)
		case <-ctx.Done():
			t.Fatal("second writer never reached first-insert conflict", ctx.Err())
		case <-ticker.C:
		}
	}
	if _, err := runforkrevision.FinalizePostgres(ctx, a, aEffects); err != nil {
		t.Fatal(err)
	}
	if err := a.Commit(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("second audit writer did not finish", ctx.Err())
	}
	var run string
	var turns int
	if err := db.QueryRowContext(ctx, `SELECT run_id::text,turn_count FROM agent_conversation_audits WHERE session_id=$1::uuid`, sessionID).Scan(&run, &turns); err != nil || run != next.runID || turns != 2 {
		t.Fatalf("audit final readback run=%s turns=%d err=%v", run, turns, err)
	}
	exactRollbackTransaction(t, s, func(ctx context.Context, tx *sql.Tx) {
		oldLedger, newLedger := exactLedger(t, ctx, tx, first.runID), exactLedger(t, ctx, tx, next.runID)
		if len(oldLedger) != 2 || oldLedger[0].Key != sessionID || !oldLedger[0].Present || oldLedger[1].Key != sessionID || oldLedger[1].Present ||
			len(newLedger) != 1 || newLedger[0].Key != sessionID || !newLedger[0].Present {
			t.Fatalf("audit history old=%+v new=%+v", oldLedger, newLedger)
		}
		for _, run := range []string{first.runID, next.runID} {
			if err := runforkrevision.ValidateCompletePostgres(ctx, tx, run); err != nil {
				t.Fatal(err)
			}
		}
	})
}
