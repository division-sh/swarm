package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/google/uuid"
)

func requireSelectedRotationRefusal(t *testing.T, err error, want sessions.RotationRefusalReason) {
	t.Helper()
	var refusal *sessions.RotationRefusal
	if !errors.As(err, &refusal) || refusal.Reason != want {
		t.Fatalf("rotation refusal=%v want=%s", err, want)
	}
}

func TestSelectedManagedRotationRequiresLifecycleAuthorityBothStores(t *testing.T) {
	eachExactFactStore(t, func(t *testing.T, selected exactFactStore) {
		fixture := newExactFactFixture(t, selected)
		identity := agentmemory.Identity(mustTestAgentIdentityForRun(fixture.runID, "managed-agent", "support/instance-1"))
		seedTestAgentRow(t, testAuthorActivityContext(), selected.db, selected.postgres, identity, "active")
		token := runtimeeffects.LifecycleToken{Identity: identity, AgentID: identity.AgentID(), RuntimeEpoch: 1, Generation: 1}
		ctx, cancel := context.WithTimeout(runtimeeffects.WithLifecycleToken(testAuthorActivityContext(), token), 20*time.Second)
		defer cancel()
		owner := selected.selected.(llmSessionAttemptJourneyOwner)
		registry := selected.selected.(sessions.Registry)
		acquired, _, err := owner.AcquireLiveSession(ctx, identity, "managed-worker")
		if err != nil || acquired == nil {
			t.Fatalf("lifecycle-authorized acquire=%+v err=%v", acquired, err)
		}
		session := &runtimellm.Session{ID: acquired.SessionID, AgentID: identity.AgentID(), Memory: agentmemory.Authored(true), MemoryIdentity: identity,
			TurnCount: 1, Messages: []runtimellm.Message{{Role: "assistant", Content: "first turn"}}}
		rotated, err := runtimellm.MaybeRotateAfterTurn(ctx, session, registry, "managed-worker", 1, nil)
		if err != nil || rotated == nil || rotated.SessionID == acquired.SessionID || session.ID != rotated.SessionID {
			t.Fatalf("managed turn rotation=%+v session=%+v err=%v", rotated, session, err)
		}
		var reason string
		if err := selected.db.QueryRowContext(ctx, `SELECT termination_reason FROM agent_sessions WHERE session_id=$1`, acquired.SessionID).Scan(&reason); err != nil || reason != "normal" {
			t.Fatalf("turn-limit reason=%q err=%v", reason, err)
		}
		stale := token
		stale.Generation++
		staleCtx := runtimeeffects.WithLifecycleToken(testAuthorActivityContext(), stale)
		beforeRows, beforeFacts := rotationCounts(t, ctx, selected.db, fixture.runID)
		_, err = owner.Rotate(staleCtx, identity, "managed-worker", sessions.RotationMetadata{OperationID: "lifecycle-replay"})
		if err == nil {
			t.Fatal("stale lifecycle token admitted rotation")
		}
		if rows, facts := rotationCounts(t, ctx, selected.db, fixture.runID); rows != beforeRows || facts != beforeFacts {
			t.Fatalf("stale token mutated rows/facts: before=%d/%d after=%d/%d", beforeRows, beforeFacts, rows, facts)
		}
		session.ParseFailures = 1
		second, err := runtimellm.MaybeRotateAfterParseFailures(ctx, session, registry, "managed-worker", 1, nil)
		if err != nil || second == nil || second.SessionID == rotated.SessionID {
			t.Fatalf("managed parse-failure rotation=%+v err=%v", second, err)
		}
		if err := selected.db.QueryRowContext(ctx, `SELECT termination_reason FROM agent_sessions WHERE session_id=$1`, rotated.SessionID).Scan(&reason); err != nil || reason != "failed" {
			t.Fatalf("parse-failure reason=%q err=%v", reason, err)
		}
		if _, err := selected.db.ExecContext(ctx, `UPDATE runs SET status='completed', completion_due_at=NULL, ended_at=$2 WHERE run_id=$1`, fixture.runID, time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
		beforeRows, beforeFacts = rotationCounts(t, ctx, selected.db, fixture.runID)
		if _, err := owner.Rotate(ctx, identity, "managed-worker", sessions.RotationMetadata{OperationID: "terminal-replay"}); err == nil {
			t.Fatal("terminal run admitted rotation")
		}
		if rows, facts := rotationCounts(t, ctx, selected.db, fixture.runID); rows != beforeRows || facts != beforeFacts {
			t.Fatalf("terminal run mutated rows/facts: before=%d/%d after=%d/%d", beforeRows, beforeFacts, rows, facts)
		}
	})
}

func rotationCounts(t *testing.T, ctx context.Context, db *sql.DB, runID string) (sessionsCount, factsCount int) {
	t.Helper()
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_sessions WHERE run_id=$1`, runID).Scan(&sessionsCount); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_fact_revisions WHERE run_id=$1 AND family='agent_sessions'`, runID).Scan(&factsCount); err != nil {
		t.Fatal(err)
	}
	return
}

func TestSelectedRotationReceiptBothStores(t *testing.T) {
	eachExactFactStore(t, func(t *testing.T, selected exactFactStore) {
		fixture := newExactFactFixture(t, selected)
		identity := agentmemory.Identity(mustTestAgentIdentityForRun(fixture.runID, "revision-matrix-agent", ""))
		ctx, cancel := context.WithTimeout(runtimeeffects.WithDifferentOwner(testAuthorActivityContext(), runtimeeffects.OwnerBuildTestInfrastructure), 20*time.Second)
		defer cancel()
		owner := selected.selected.(llmSessionAttemptJourneyOwner)
		acquired, _, err := owner.AcquireLiveSession(ctx, identity, "receipt-worker")
		if err != nil || acquired == nil {
			t.Fatalf("acquire=%+v err=%v", acquired, err)
		}
		request := sessions.RotationMetadata{OperationID: "receipt-a", CheckpointSummary: "checkpoint", RetryReason: "session in use", TerminationReason: " NORMAL "}
		first, err := owner.Rotate(ctx, identity, " receipt-worker ", request)
		if err != nil || first == nil {
			t.Fatalf("rotate=%+v err=%v", first, err)
		}
		var reason, storedKey, digest string
		if err := selected.db.QueryRowContext(ctx, `SELECT termination_reason FROM agent_sessions WHERE session_id=$1`, acquired.SessionID).Scan(&reason); err != nil {
			t.Fatal(err)
		}
		if err := selected.db.QueryRowContext(ctx, `SELECT rotation_operation_id, rotation_request_digest FROM agent_sessions WHERE session_id=$1`, first.SessionID).Scan(&storedKey, &digest); err != nil {
			t.Fatal(err)
		}
		if reason != "normal" || storedKey != "receipt-a" || digest == "" {
			t.Fatalf("stored reason=%q key=%q digest=%q", reason, storedKey, digest)
		}
		var stateRaw []byte
		if err := selected.db.QueryRowContext(ctx, `SELECT runtime_state FROM agent_sessions WHERE session_id=$1`, first.SessionID).Scan(&stateRaw); err != nil {
			t.Fatal(err)
		}
		var state map[string]any
		if err := json.Unmarshal(stateRaw, &state); err != nil {
			t.Fatal(err)
		}
		if _, found := state["rotation_operation_id"]; found {
			t.Fatal("registry replay key survived in runtime_state as a second authority")
		}
		beforeRows, beforeFacts := rotationCounts(t, ctx, selected.db, fixture.runID)
		var reopened llmSessionAttemptJourneyOwner
		if selected.postgres {
			reopened = newPostgresStoreWithBackend(mustPostgresBackend(selected.db))
		} else {
			reopened = newBootstrappedSQLiteRuntimeStoreForPath(t, selected.selected.(*SQLiteRuntimeStore).Path())
		}
		replayed, err := reopened.Rotate(ctx, identity, "receipt-worker", sessions.RotationMetadata{
			OperationID: " receipt-a ", CheckpointSummary: " checkpoint ", RetryReason: " session in use ",
		})
		if err != nil || replayed == nil || replayed.SessionID != first.SessionID ||
			replayed.ProviderSessionID != first.ProviderSessionID || replayed.Identity != first.Identity ||
			replayed.RetryReason != first.RetryReason || replayed.RetriesFromSessionID != first.RetriesFromSessionID ||
			replayed.LockOwner != first.LockOwner || !replayed.ExpiresAt.Equal(first.ExpiresAt) {
			t.Fatalf("reopened complete replay=%+v first=%+v err=%v", replayed, first, err)
		}
		for _, changed := range []sessions.RotationMetadata{
			{OperationID: "receipt-a", CheckpointSummary: "different", RetryReason: "session in use"},
			{OperationID: "receipt-a", CheckpointSummary: "checkpoint", RetryReason: "different"},
			{OperationID: "receipt-a", CheckpointSummary: "checkpoint", RetryReason: "session in use", TerminationReason: sessions.TerminationReasonFailed},
		} {
			_, err := reopened.Rotate(ctx, identity, "receipt-worker", changed)
			requireSelectedRotationRefusal(t, err, sessions.RotationRequestConflict)
		}
		_, err = reopened.Rotate(ctx, identity, "different-worker", request)
		requireSelectedRotationRefusal(t, err, sessions.RotationRequestConflict)
		if rows, facts := rotationCounts(t, ctx, selected.db, fixture.runID); rows != beforeRows || facts != beforeFacts {
			t.Fatalf("replay/conflict mutated rows/facts: before=%d/%d after=%d/%d", beforeRows, beforeFacts, rows, facts)
		}
		other := agentmemory.Identity(mustTestAgentIdentityForRun(fixture.runID, "other-agent", ""))
		seedTestAgentRow(t, testAuthorActivityContext(), selected.db, selected.postgres, other, "active")
		if _, _, err := owner.AcquireLiveSession(ctx, other, "receipt-worker"); err != nil {
			t.Fatal(err)
		}
		beforeRows, beforeFacts = rotationCounts(t, ctx, selected.db, fixture.runID)
		_, err = reopened.Rotate(ctx, other, "receipt-worker", request)
		requireSelectedRotationRefusal(t, err, sessions.RotationRequestConflict)
		flow := agentmemory.Identity(mustTestAgentIdentityForRun(fixture.runID, "flow-agent", "support/instance-1"))
		seedTestAgentRow(t, testAuthorActivityContext(), selected.db, selected.postgres, flow, "active")
		if _, _, err := owner.AcquireLiveSession(ctx, flow, "receipt-worker"); err != nil {
			t.Fatal(err)
		}
		_, err = reopened.Rotate(ctx, flow, "receipt-worker", request)
		requireSelectedRotationRefusal(t, err, sessions.RotationRequestConflict)
		otherRunID := uuid.NewString()
		requireRunFixtureForTest(t, testAuthorActivityContext(), selected.selected, semanticRunFixture{
			Origin: semanticScenarioSetupRunOriginForTest(), RunID: otherRunID, StartedAt: time.Now().UTC(),
		})
		otherRun := agentmemory.Identity(mustTestAgentIdentityForRun(otherRunID, "other-run-agent", ""))
		seedTestAgentRow(t, testAuthorActivityContext(), selected.db, selected.postgres, otherRun, "active")
		if _, _, err := owner.AcquireLiveSession(ctx, otherRun, "receipt-worker"); err != nil {
			t.Fatal(err)
		}
		_, err = reopened.Rotate(ctx, otherRun, "receipt-worker", request)
		requireSelectedRotationRefusal(t, err, sessions.RotationRequestConflict)
		beforeRows, beforeFacts = rotationCounts(t, ctx, selected.db, fixture.runID)
		for _, invalid := range []sessions.RotationMetadata{
			{OperationID: "receipt-a", TerminationReason: "unknown"},
			{OperationID: "receipt-a", TerminationReason: sessions.TerminationReasonLegacy},
		} {
			if _, err := reopened.Rotate(ctx, identity, "receipt-worker", invalid); err == nil {
				t.Fatalf("accepted invalid reason %q", invalid.TerminationReason)
			}
		}
		if _, err := reopened.Rotate(ctx, identity, "  ", request); err == nil {
			t.Fatal("accepted blank owner on known key")
		}
		if rows, facts := rotationCounts(t, ctx, selected.db, fixture.runID); rows != beforeRows || facts != beforeFacts {
			t.Fatalf("replay/conflict mutated rows/facts: before=%d/%d after=%d/%d", beforeRows, beforeFacts, rows, facts)
		}
		if released, err := owner.ReleaseOutcome(ctx, first); err != nil || !released.Acknowledged {
			t.Fatalf("release=%+v err=%v", released, err)
		}
		_, err = reopened.Rotate(ctx, identity, "receipt-worker", request)
		requireSelectedRotationRefusal(t, err, sessions.RotationSuccessorNotCurrent)
		if rows, _ := rotationCounts(t, ctx, selected.db, fixture.runID); rows != beforeRows {
			t.Fatalf("stale replay created row: %d want=%d", rows, beforeRows)
		}
		if _, _, err := owner.AcquireLiveSession(ctx, identity, "receipt-worker"); err != nil {
			t.Fatal(err)
		}
		_, err = reopened.Rotate(ctx, identity, "receipt-worker", request)
		requireSelectedRotationRefusal(t, err, sessions.RotationSuccessorNotCurrent)
		if _, err := owner.Rotate(ctx, identity, "receipt-worker", sessions.RotationMetadata{OperationID: "receipt-b"}); err != nil {
			t.Fatal(err)
		}
		_, err = reopened.Rotate(ctx, identity, "receipt-worker", request)
		requireSelectedRotationRefusal(t, err, sessions.RotationSuccessorNotCurrent)
		third, err := owner.Rotate(ctx, identity, "receipt-worker", sessions.RotationMetadata{OperationID: "receipt-c"})
		if err != nil {
			t.Fatal(err)
		}
		adopter := selected.selected.(interface {
			AdoptSessionID(context.Context, agentmemory.Identity, string, string) error
		})
		if err := adopter.AdoptSessionID(ctx, identity, "receipt-worker", "provider-child-c"); err != nil {
			t.Fatal(err)
		}
		_, err = reopened.Rotate(ctx, identity, "receipt-worker", sessions.RotationMetadata{OperationID: "receipt-c"})
		requireSelectedRotationRefusal(t, err, sessions.RotationSuccessorNotCurrent)
		if third.SessionID == "" {
			t.Fatal("missing third successor")
		}
		fourth, err := owner.Rotate(ctx, identity, "receipt-worker", sessions.RotationMetadata{OperationID: "receipt-d"})
		if err != nil || fourth == nil {
			t.Fatalf("fourth rotation=%+v err=%v", fourth, err)
		}
		if _, _, err := owner.AcquireLiveSession(ctx, identity, "receipt-worker"); err != nil {
			t.Fatal(err)
		}
		_, err = reopened.Rotate(ctx, identity, "receipt-worker", sessions.RotationMetadata{OperationID: "receipt-d"})
		requireSelectedRotationRefusal(t, err, sessions.RotationSuccessorNotCurrent)
		fifth, err := owner.Rotate(ctx, identity, "receipt-worker", sessions.RotationMetadata{OperationID: "receipt-e"})
		if err != nil || fifth == nil {
			t.Fatalf("fifth rotation=%+v err=%v", fifth, err)
		}
		if _, err := selected.db.ExecContext(ctx, `UPDATE agent_sessions SET lease_expires_at=$2 WHERE session_id=$1`, fifth.SessionID, time.Now().Add(-time.Minute).UTC()); err != nil {
			t.Fatal(err)
		}
		beforeRows, beforeFacts = rotationCounts(t, ctx, selected.db, fixture.runID)
		_, err = reopened.Rotate(ctx, identity, "receipt-worker", sessions.RotationMetadata{OperationID: "receipt-e"})
		requireSelectedRotationRefusal(t, err, sessions.RotationSuccessorNotCurrent)
		if rows, facts := rotationCounts(t, ctx, selected.db, fixture.runID); rows != beforeRows || facts != beforeFacts {
			t.Fatalf("expired replay mutated rows/facts: before=%d/%d after=%d/%d", beforeRows, beforeFacts, rows, facts)
		}
	})
}

func TestSelectedRotationSameKeyConcurrentBothStores(t *testing.T) {
	eachExactFactStore(t, func(t *testing.T, selected exactFactStore) {
		for _, mode := range []string{"same", "changed"} {
			t.Run(mode, func(t *testing.T) {
				fixture := newExactFactFixture(t, selected)
				identity := agentmemory.Identity(mustTestAgentIdentityForRun(fixture.runID, "revision-matrix-agent", ""))
				ctx, cancel := context.WithTimeout(runtimeeffects.WithDifferentOwner(testAuthorActivityContext(), runtimeeffects.OwnerBuildTestInfrastructure), 20*time.Second)
				defer cancel()
				first := selected.selected.(llmSessionAttemptJourneyOwner)
				if _, _, err := first.AcquireLiveSession(ctx, identity, "concurrent-worker"); err != nil {
					t.Fatal(err)
				}
				var second llmSessionAttemptJourneyOwner
				if selected.postgres {
					second = newPostgresStoreWithBackend(mustPostgresBackend(selected.db))
				} else {
					second = newBootstrappedSQLiteRuntimeStoreForPath(t, selected.selected.(*SQLiteRuntimeStore).Path())
				}
				owners := []llmSessionAttemptJourneyOwner{first, second}
				type outcome struct {
					lease *sessions.Lease
					err   error
				}
				results := make([]outcome, len(owners))
				start := make(chan struct{})
				var wg sync.WaitGroup
				for i, owner := range owners {
					wg.Add(1)
					go func(i int, owner llmSessionAttemptJourneyOwner) {
						defer wg.Done()
						<-start
						metadata := sessions.RotationMetadata{OperationID: fixture.runID}
						if mode == "changed" && i == 1 {
							metadata.CheckpointSummary = "different"
						}
						results[i].lease, results[i].err = owner.Rotate(ctx, identity, "concurrent-worker", metadata)
					}(i, owner)
				}
				close(start)
				wg.Wait()
				if mode == "same" {
					if results[0].err != nil || results[1].err != nil || results[0].lease == nil || results[1].lease == nil ||
						results[0].lease.SessionID != results[1].lease.SessionID {
						t.Fatalf("concurrent same-key results=%+v", results)
					}
				} else {
					successes, conflicts := 0, 0
					for _, result := range results {
						if result.err == nil && result.lease != nil {
							successes++
						} else {
							var refusal *sessions.RotationRefusal
							if errors.As(result.err, &refusal) && refusal.Reason == sessions.RotationRequestConflict {
								conflicts++
							}
						}
					}
					if successes != 1 || conflicts != 1 {
						t.Fatalf("concurrent changed-key results=%+v", results)
					}
				}
				rows, _ := rotationCounts(t, ctx, selected.db, fixture.runID)
				if rows != 2 {
					t.Fatalf("concurrent same-key rows=%d want=2", rows)
				}
			})
		}
	})
}
