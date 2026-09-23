package runtimepersistence

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
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
	runMutation := func(write func(context.Context, *mutationprotocol.Attempt) error) error {
		return mutationprotocol.RunPostgres(ctx, store.backend, mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, store.runLifecycleCandidates,
			func(txctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
				return struct{}{}, write(txctx, attempt)
			}).Err()
	}
	aStarted := make(chan int, 1)
	bStarted := make(chan int, 1)
	releaseA := make(chan struct{})
	aFinished := make(chan error, 1)
	finished := make(chan error, 1)
	go func() {
		aFinished <- runMutation(func(txctx context.Context, attempt *mutationprotocol.Attempt) error {
			var pid int
			if err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
				return tx.QueryRowContext(txctx, `SELECT pg_backend_pid()`).Scan(&pid)
			}); err != nil {
				return err
			}
			if err := store.lLMPostgresOwner.EnsureCompletionTurnMemoryTx(txctx, attempt, record(first.runID)); err != nil {
				return err
			}
			aStarted <- pid
			select {
			case <-releaseA:
				return nil
			case <-txctx.Done():
				return txctx.Err()
			}
		})
	}()
	aPID := <-aStarted
	go func() {
		finished <- runMutation(func(txctx context.Context, attempt *mutationprotocol.Attempt) error {
			var pid int
			if err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
				return tx.QueryRowContext(txctx, `SELECT pg_backend_pid()`).Scan(&pid)
			}); err != nil {
				return err
			}
			bStarted <- pid
			return store.lLMPostgresOwner.EnsureCompletionTurnMemoryTx(txctx, attempt, record(next.runID))
		})
	}()
	bPID := <-bStarted
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
	close(releaseA)
	if err := <-aFinished; err != nil {
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
