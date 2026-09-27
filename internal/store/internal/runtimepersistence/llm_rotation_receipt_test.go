package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	runlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
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
		base := agentmemory.WithExecution(runtimeeffects.WithLifecycleToken(testAuthorActivityContext(), token), agentmemory.Authored(true), identity)
		ctx, cancel := context.WithTimeout(base, 20*time.Second)
		defer cancel()
		owner := selected.selected.(llmSessionAttemptJourneyOwner)
		registry := selected.selected.(sessions.Registry)
		acquired, _, err := owner.AcquireLiveSession(ctx, identity, "managed-worker")
		if err != nil || acquired == nil {
			t.Fatalf("lifecycle-authorized acquire=%+v err=%v", acquired, err)
		}
		session := &runtimellm.Session{ID: acquired.SessionID, AgentID: identity.AgentID(), Memory: agentmemory.Authored(true), MemoryIdentity: identity,
			TurnCount: 1, Messages: []runtimellm.Message{{Role: "assistant", Content: "first turn"}}}
		adapter := runtimellm.NewMockRuntime(&config.Config{LLM: config.LLMConfig{Session: config.LLMSessionConfig{RotateAfterTurns: 1}}}, registry, "managed-worker", nil, nil, nil)
		if err := adapter.PrepareManagedSession(ctx, session); err != nil || session.ID == acquired.SessionID {
			t.Fatalf("managed adapter turn rotation: session=%+v err=%v", session, err)
		}
		rotatedID := session.ID
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
		if err != nil || second == nil || second.SessionID == rotatedID {
			t.Fatalf("managed parse-failure rotation=%+v err=%v", second, err)
		}
		if err := selected.db.QueryRowContext(ctx, `SELECT termination_reason FROM agent_sessions WHERE session_id=$1`, rotatedID).Scan(&reason); err != nil || reason != "failed" {
			t.Fatalf("parse-failure reason=%q err=%v", reason, err)
		}
		known := sessions.RotationMetadata{OperationID: "committed-lifecycle-key"}
		committed, err := owner.Rotate(ctx, identity, "managed-worker", known)
		if err != nil {
			t.Fatalf("commit known receipt: %v", err)
		}
		beforeReplay := snapshotRotationEffects(t, ctx, selected.db, fixture.runID)
		replayed, err := owner.Rotate(ctx, identity, "managed-worker", known)
		if err != nil || replayed == nil || *replayed != *committed {
			t.Fatalf("healthy known receipt replay=%+v first=%+v err=%v", replayed, committed, err)
		}
		requireRotationEffectsUnchanged(t, beforeReplay, snapshotRotationEffects(t, ctx, selected.db, fixture.runID))
		beforeRows, beforeFacts = rotationCounts(t, ctx, selected.db, fixture.runID)
		_, err = owner.Rotate(staleCtx, identity, "managed-worker", known)
		if err == nil {
			t.Fatal("stale lifecycle token consumed known receipt")
		}
		if rows, facts := rotationCounts(t, ctx, selected.db, fixture.runID); rows != beforeRows || facts != beforeFacts {
			t.Fatalf("stale known receipt mutated rows/facts: before=%d/%d after=%d/%d", beforeRows, beforeFacts, rows, facts)
		}
		requireRotationEffectsUnchanged(t, beforeReplay, snapshotRotationEffects(t, ctx, selected.db, fixture.runID))
		if _, err := markRunTerminalStatusForTest(ctx, selected.selected, fixture.runID, "cancelled", nil, time.Now().UTC()); err != nil {
			t.Fatalf("cancel run through lifecycle owner: %v", err)
		}
		beforeRows, beforeFacts = rotationCounts(t, ctx, selected.db, fixture.runID)
		beforeTerminalReplay := snapshotRotationEffects(t, ctx, selected.db, fixture.runID)
		if _, err := owner.Rotate(ctx, identity, "managed-worker", known); err == nil {
			t.Fatal("terminal run consumed known receipt")
		}
		if rows, facts := rotationCounts(t, ctx, selected.db, fixture.runID); rows != beforeRows || facts != beforeFacts {
			t.Fatalf("terminal run mutated rows/facts: before=%d/%d after=%d/%d", beforeRows, beforeFacts, rows, facts)
		}
		requireRotationEffectsUnchanged(t, beforeTerminalReplay, snapshotRotationEffects(t, ctx, selected.db, fixture.runID))
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

type rotationEffectSnapshot struct {
	Sessions []string
	Run      []string
	Story    []string
	Revision mutationProtocolHistorySnapshot
}

func snapshotRotationEffects(t *testing.T, ctx context.Context, db *sql.DB, runID string) rotationEffectSnapshot {
	t.Helper()
	return rotationEffectSnapshot{
		Sessions: snapshotRotationTableRows(t, ctx, db, `SELECT * FROM agent_sessions WHERE run_id=$1 ORDER BY session_id`, runID),
		Run:      snapshotRotationTableRows(t, ctx, db, `SELECT * FROM runs WHERE run_id=$1`, runID),
		Story:    snapshotRotationTableRows(t, ctx, db, `SELECT * FROM author_activity_occurrences WHERE run_id=$1 ORDER BY sequence`, runID),
		Revision: snapshotMutationProtocolHistory(t, ctx, db, runID),
	}
}

func snapshotRotationTableRows(t *testing.T, ctx context.Context, db *sql.DB, query, runID string) []string {
	t.Helper()
	rows, err := db.QueryContext(ctx, query, runID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	var result []string
	for rows.Next() {
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			t.Fatal(err)
		}
		encoded, err := json.Marshal(values)
		if err != nil {
			t.Fatal(err)
		}
		result = append(result, string(encoded))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func requireRotationEffectsUnchanged(t *testing.T, before, after rotationEffectSnapshot) {
	t.Helper()
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("rotation replay/refusal changed durable effects:\nbefore=%+v\nafter=%+v", before, after)
	}
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
		beforeReplay := snapshotRotationEffects(t, ctx, selected.db, fixture.runID)
		var reopened llmSessionAttemptJourneyOwner
		if selected.postgres {
			reopened = newPostgresStoreWithBackend(mustPostgresBackend(selected.db))
		} else {
			reopened = newBootstrappedSQLiteRuntimeStoreForPath(t, selected.selected.(*SQLiteRuntimeStore).Path())
		}
		refuseWithoutEffects := func(runID string, target agentmemory.Identity, lockOwner string, metadata sessions.RotationMetadata, want sessions.RotationRefusalReason) {
			t.Helper()
			before := snapshotRotationEffects(t, ctx, selected.db, runID)
			_, err := reopened.Rotate(ctx, target, lockOwner, metadata)
			requireSelectedRotationRefusal(t, err, want)
			requireRotationEffectsUnchanged(t, before, snapshotRotationEffects(t, ctx, selected.db, runID))
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
			refuseWithoutEffects(fixture.runID, identity, "receipt-worker", changed, sessions.RotationRequestConflict)
		}
		refuseWithoutEffects(fixture.runID, identity, "different-worker", request, sessions.RotationRequestConflict)
		if rows, facts := rotationCounts(t, ctx, selected.db, fixture.runID); rows != beforeRows || facts != beforeFacts {
			t.Fatalf("replay/conflict mutated rows/facts: before=%d/%d after=%d/%d", beforeRows, beforeFacts, rows, facts)
		}
		requireRotationEffectsUnchanged(t, beforeReplay, snapshotRotationEffects(t, ctx, selected.db, fixture.runID))
		other := agentmemory.Identity(mustTestAgentIdentityForRun(fixture.runID, "other-agent", ""))
		seedTestAgentRow(t, testAuthorActivityContext(), selected.db, selected.postgres, other, "active")
		if _, _, err := owner.AcquireLiveSession(ctx, other, "receipt-worker"); err != nil {
			t.Fatal(err)
		}
		beforeRows, beforeFacts = rotationCounts(t, ctx, selected.db, fixture.runID)
		refuseWithoutEffects(fixture.runID, other, "receipt-worker", request, sessions.RotationRequestConflict)
		flow := agentmemory.Identity(mustTestAgentIdentityForRun(fixture.runID, "revision-matrix-agent", "support/instance-1"))
		seedTestAgentRow(t, testAuthorActivityContext(), selected.db, selected.postgres, flow, "active")
		if _, _, err := owner.AcquireLiveSession(ctx, flow, "receipt-worker"); err != nil {
			t.Fatal(err)
		}
		refuseWithoutEffects(fixture.runID, flow, "receipt-worker", request, sessions.RotationRequestConflict)
		otherRunID := uuid.NewString()
		requireRunFixtureForTest(t, testAuthorActivityContext(), selected.selected, semanticRunFixture{
			Origin: semanticScenarioSetupRunOriginForTest(), RunID: otherRunID, StartedAt: time.Now().UTC(),
		})
		otherRun := agentmemory.Identity(mustTestAgentIdentityForRun(otherRunID, "revision-matrix-agent", ""))
		seedTestAgentRow(t, testAuthorActivityContext(), selected.db, selected.postgres, otherRun, "active")
		if _, _, err := owner.AcquireLiveSession(ctx, otherRun, "receipt-worker"); err != nil {
			t.Fatal(err)
		}
		refuseWithoutEffects(otherRunID, otherRun, "receipt-worker", request, sessions.RotationRequestConflict)
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
		refuseWithoutEffects(fixture.runID, identity, "receipt-worker", request, sessions.RotationSuccessorNotCurrent)
		if rows, _ := rotationCounts(t, ctx, selected.db, fixture.runID); rows != beforeRows {
			t.Fatalf("stale replay created row: %d want=%d", rows, beforeRows)
		}
		otherLease, _, err := owner.AcquireLiveSession(ctx, identity, "other-worker")
		if err != nil || otherLease == nil {
			t.Fatalf("other-owner acquire=%+v err=%v", otherLease, err)
		}
		refuseWithoutEffects(fixture.runID, identity, "receipt-worker", request, sessions.RotationSuccessorNotCurrent)
		if released, err := owner.ReleaseOutcome(ctx, otherLease); err != nil || !released.Acknowledged {
			t.Fatalf("other-owner release=%+v err=%v", released, err)
		}
		if _, _, err := owner.AcquireLiveSession(ctx, identity, "receipt-worker"); err != nil {
			t.Fatal(err)
		}
		refuseWithoutEffects(fixture.runID, identity, "receipt-worker", request, sessions.RotationSuccessorNotCurrent)
		if _, err := owner.Rotate(ctx, identity, "receipt-worker", sessions.RotationMetadata{OperationID: "receipt-b"}); err != nil {
			t.Fatal(err)
		}
		refuseWithoutEffects(fixture.runID, identity, "receipt-worker", request, sessions.RotationSuccessorNotCurrent)
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
		refuseWithoutEffects(fixture.runID, identity, "receipt-worker", sessions.RotationMetadata{OperationID: "receipt-c"}, sessions.RotationSuccessorNotCurrent)
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
		refuseWithoutEffects(fixture.runID, identity, "receipt-worker", sessions.RotationMetadata{OperationID: "receipt-d"}, sessions.RotationSuccessorNotCurrent)
		fifth, err := owner.Rotate(ctx, identity, "receipt-worker", sessions.RotationMetadata{OperationID: "receipt-e"})
		if err != nil || fifth == nil {
			t.Fatalf("fifth rotation=%+v err=%v", fifth, err)
		}
		if _, err := selected.db.ExecContext(ctx, `UPDATE agent_sessions SET lease_expires_at=$2 WHERE session_id=$1`, fifth.SessionID, time.Now().Add(-time.Minute).UTC()); err != nil {
			t.Fatal(err)
		}
		beforeRows, beforeFacts = rotationCounts(t, ctx, selected.db, fixture.runID)
		refuseWithoutEffects(fixture.runID, identity, "receipt-worker", sessions.RotationMetadata{OperationID: "receipt-e"}, sessions.RotationSuccessorNotCurrent)
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

func TestSelectedRotationReceiptPrecisionBothStores(t *testing.T) {
	eachExactFactStore(t, func(t *testing.T, selected exactFactStore) {
		fixture := newExactFactFixture(t, selected)
		identity := agentmemory.Identity(mustTestAgentIdentityForRun(fixture.runID, "revision-matrix-agent", ""))
		ctx := runtimeeffects.WithDifferentOwner(testAuthorActivityContext(), runtimeeffects.OwnerBuildTestInfrastructure)
		selected.selected.(interface{ SetSessionLockTTL(time.Duration) }).SetSessionLockTTL(time.Minute + 123*time.Nanosecond)
		owner := selected.selected.(llmSessionAttemptJourneyOwner)
		if _, _, err := owner.AcquireLiveSession(ctx, identity, "precision-worker"); err != nil {
			t.Fatal(err)
		}
		request := sessions.RotationMetadata{OperationID: "precision-key"}
		first, err := owner.Rotate(ctx, identity, "precision-worker", request)
		if err != nil {
			t.Fatal(err)
		}
		var reopened llmSessionAttemptJourneyOwner
		if selected.postgres {
			reopened = newPostgresStoreWithBackend(mustPostgresBackend(selected.db))
		} else {
			reopened = newBootstrappedSQLiteRuntimeStoreForPath(t, selected.selected.(*SQLiteRuntimeStore).Path())
		}
		replayed, err := reopened.Rotate(ctx, identity, "precision-worker", request)
		if err != nil || replayed == nil || *replayed != *first {
			t.Fatalf("non-microsecond TTL replay=%+v first=%+v err=%v", replayed, first, err)
		}
	})
}

func TestSelectedRotationElapsedExpiryBothStores(t *testing.T) {
	eachExactFactStore(t, func(t *testing.T, selected exactFactStore) {
		fixture := newExactFactFixture(t, selected)
		identity := agentmemory.Identity(mustTestAgentIdentityForRun(fixture.runID, "revision-matrix-agent", ""))
		ctx := runtimeeffects.WithDifferentOwner(testAuthorActivityContext(), runtimeeffects.OwnerBuildTestInfrastructure)
		selected.selected.(interface{ SetSessionLockTTL(time.Duration) }).SetSessionLockTTL(150 * time.Millisecond)
		owner := selected.selected.(llmSessionAttemptJourneyOwner)
		if _, _, err := owner.AcquireLiveSession(ctx, identity, "expiry-worker"); err != nil {
			t.Fatal(err)
		}
		request := sessions.RotationMetadata{OperationID: "elapsed-expiry-key"}
		first, err := owner.Rotate(ctx, identity, "expiry-worker", request)
		if err != nil {
			t.Fatal(err)
		}
		var rawExpiry any
		if err := selected.db.QueryRowContext(ctx, `SELECT lease_expires_at FROM agent_sessions WHERE session_id=$1`, first.SessionID).Scan(&rawExpiry); err != nil {
			t.Fatal(err)
		}
		storedExpiry, valid, err := sqliteTimeValue(rawExpiry)
		if err != nil || !valid {
			t.Fatalf("parse stored lease expiry: value=%v err=%v", rawExpiry, err)
		}
		if !storedExpiry.Equal(first.ExpiresAt) {
			t.Fatalf("receipt/current expiry differs before time advances: stored=%s first=%s", storedExpiry, first.ExpiresAt)
		}
		time.Sleep(max(0, time.Until(first.ExpiresAt)) + 10*time.Millisecond)
		beforeRows, beforeFacts := rotationCounts(t, ctx, selected.db, fixture.runID)
		beforeReplay := snapshotRotationEffects(t, ctx, selected.db, fixture.runID)
		_, err = owner.Rotate(ctx, identity, "expiry-worker", request)
		requireSelectedRotationRefusal(t, err, sessions.RotationSuccessorNotCurrent)
		if rows, facts := rotationCounts(t, ctx, selected.db, fixture.runID); rows != beforeRows || facts != beforeFacts {
			t.Fatalf("elapsed expiry replay mutated rows/facts: before=%d/%d after=%d/%d", beforeRows, beforeFacts, rows, facts)
		}
		requireRotationEffectsUnchanged(t, beforeReplay, snapshotRotationEffects(t, ctx, selected.db, fixture.runID))
	})
}

func TestSelectedUnkeyedRotationUsesCanonicalNormalBothStores(t *testing.T) {
	eachExactFactStore(t, func(t *testing.T, selected exactFactStore) {
		fixture := newExactFactFixture(t, selected)
		identity := agentmemory.Identity(mustTestAgentIdentityForRun(fixture.runID, "revision-matrix-agent", ""))
		ctx := runtimeeffects.WithDifferentOwner(testAuthorActivityContext(), runtimeeffects.OwnerBuildTestInfrastructure)
		owner := selected.selected.(llmSessionAttemptJourneyOwner)
		initial, _, err := owner.AcquireLiveSession(ctx, identity, "unkeyed-worker")
		if err != nil {
			t.Fatal(err)
		}
		first, err := owner.Rotate(ctx, identity, "unkeyed-worker", sessions.RotationMetadata{RetryReason: "session not found"})
		if err != nil {
			t.Fatal(err)
		}
		second, err := owner.Rotate(ctx, identity, "unkeyed-worker", sessions.RotationMetadata{OperationID: " ", RetryReason: "session not found"})
		if err != nil || second.SessionID == first.SessionID {
			t.Fatalf("second unkeyed rotation=%+v first=%+v err=%v", second, first, err)
		}
		for _, predecessor := range []string{initial.SessionID, first.SessionID} {
			var reason, detail string
			if err := selected.db.QueryRowContext(ctx, `SELECT termination_reason, termination_detail FROM agent_sessions WHERE session_id=$1`, predecessor).Scan(&reason, &detail); err != nil {
				t.Fatal(err)
			}
			if reason != sessions.TerminationReasonNormal.String() || detail != "session not found" {
				t.Fatalf("unkeyed predecessor %s reason=%q detail=%q", predecessor, reason, detail)
			}
		}
		var receipts int
		if err := selected.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_sessions WHERE run_id=$1 AND rotation_operation_id IS NOT NULL`, fixture.runID).Scan(&receipts); err != nil {
			t.Fatal(err)
		}
		if rows, _ := rotationCounts(t, ctx, selected.db, fixture.runID); rows != 3 || receipts != 0 {
			t.Fatalf("unkeyed rotation rows=%d receipts=%d", rows, receipts)
		}
	})
}

func TestSelectedRotationReceiptResetAndDiscardBothStores(t *testing.T) {
	eachExactFactStore(t, func(t *testing.T, selected exactFactStore) {
		ctx := runtimeeffects.WithDifferentOwner(testAuthorActivityContext(), runtimeeffects.OwnerBuildTestInfrastructure)
		fixture := newExactFactFixture(t, selected)
		identity := agentmemory.Identity(mustTestAgentIdentityForRun(fixture.runID, "revision-matrix-agent", ""))
		owner := selected.selected.(llmSessionAttemptJourneyOwner)
		reset := selected.selected.(interface {
			ResetAll(sessions.ResetMetadata) (sessions.ResetSummary, error)
		})
		if _, _, err := owner.AcquireLiveSession(ctx, identity, "reset-worker"); err != nil {
			t.Fatal(err)
		}
		request := sessions.RotationMetadata{OperationID: "retained-before-reset"}
		first, err := owner.Rotate(ctx, identity, "reset-worker", request)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := reset.ResetAll(sessions.ResetMetadata{Source: "rotation-receipt-test"}); err != nil {
			t.Fatal(err)
		}
		var status, retainedKey string
		if err := selected.db.QueryRowContext(ctx, `SELECT status, rotation_operation_id FROM agent_sessions WHERE session_id=$1`, first.SessionID).Scan(&status, &retainedKey); err != nil || status != "terminated" || retainedKey != request.OperationID {
			t.Fatalf("reset receipt status=%q key=%q err=%v", status, retainedKey, err)
		}
		if _, _, err := owner.AcquireLiveSession(ctx, identity, "reset-worker"); err != nil {
			t.Fatal(err)
		}
		before := snapshotRotationEffects(t, ctx, selected.db, fixture.runID)
		_, err = owner.Rotate(ctx, identity, "reset-worker", request)
		requireSelectedRotationRefusal(t, err, sessions.RotationSuccessorNotCurrent)
		_, err = owner.Rotate(ctx, identity, "reset-worker", sessions.RotationMetadata{OperationID: request.OperationID, CheckpointSummary: "changed"})
		requireSelectedRotationRefusal(t, err, sessions.RotationRequestConflict)
		requireRotationEffectsUnchanged(t, before, snapshotRotationEffects(t, ctx, selected.db, fixture.runID))

		other := newExactFactFixture(t, selected)
		otherIdentity := agentmemory.Identity(mustTestAgentIdentityForRun(other.runID, "revision-matrix-agent", ""))
		if _, _, err := owner.AcquireLiveSession(ctx, otherIdentity, "reset-worker"); err != nil {
			t.Fatal(err)
		}
		otherReceipt, err := owner.Rotate(ctx, otherIdentity, "reset-worker", sessions.RotationMetadata{OperationID: "unrelated-retained-key"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := reset.ResetAll(sessions.ResetMetadata{Source: "prepare-discard"}); err != nil {
			t.Fatal(err)
		}
		if _, err := transitionRunForTest(ctx, selected.selected, runlifecycle.ActiveTransitionRequest{RunID: fixture.runID, State: runlifecycle.StatePaused}); err != nil {
			t.Fatal(err)
		}
		discard := selected.selected.(interface {
			DiscardMaterializedSelectedContractExecutionFork(context.Context, string) error
		})
		if err := discard.DiscardMaterializedSelectedContractExecutionFork(ctx, fixture.runID); err != nil {
			t.Fatal(err)
		}
		var deleted, untouched int
		if err := selected.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_sessions WHERE rotation_operation_id=$1`, request.OperationID).Scan(&deleted); err != nil {
			t.Fatal(err)
		}
		if err := selected.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_sessions WHERE session_id=$1 AND rotation_operation_id=$2`, otherReceipt.SessionID, "unrelated-retained-key").Scan(&untouched); err != nil {
			t.Fatal(err)
		}
		if deleted != 0 || untouched != 1 {
			t.Fatalf("discard receipt ownership: deleted-key rows=%d unrelated rows=%d", deleted, untouched)
		}
	})
}

func TestPostgresRotationReciprocalKeysConflictWithoutDeadlock(t *testing.T) {
	eachExactFactStore(t, func(t *testing.T, selected exactFactStore) {
		if !selected.postgres {
			t.Skip("PostgreSQL row-lock ordering")
		}
		ctx, cancel := context.WithTimeout(runtimeeffects.WithDifferentOwner(testAuthorActivityContext(), runtimeeffects.OwnerBuildTestInfrastructure), 20*time.Second)
		defer cancel()
		owner := selected.selected.(llmSessionAttemptJourneyOwner)
		var identities [2]agentmemory.Identity
		for i := range identities {
			fixture := newExactFactFixture(t, selected)
			identities[i] = agentmemory.Identity(mustTestAgentIdentityForRun(fixture.runID, "revision-matrix-agent", ""))
			if _, _, err := owner.AcquireLiveSession(ctx, identities[i], "reciprocal-worker"); err != nil {
				t.Fatal(err)
			}
			if _, err := owner.Rotate(ctx, identities[i], "reciprocal-worker", sessions.RotationMetadata{OperationID: identities[i].RunID}); err != nil {
				t.Fatal(err)
			}
		}
		var before [2][2]int
		for i := range identities {
			before[i][0], before[i][1] = rotationCounts(t, ctx, selected.db, identities[i].RunID)
		}
		for attempt := 0; attempt < 20; attempt++ {
			start := make(chan struct{})
			var wg sync.WaitGroup
			var errs [2]error
			for i := range identities {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					<-start
					_, errs[i] = owner.Rotate(ctx, identities[i], "reciprocal-worker", sessions.RotationMetadata{OperationID: identities[1-i].RunID})
				}(i)
			}
			close(start)
			wg.Wait()
			for i, err := range errs {
				requireSelectedRotationRefusal(t, err, sessions.RotationRequestConflict)
				rows, facts := rotationCounts(t, ctx, selected.db, identities[i].RunID)
				if rows != before[i][0] || facts != before[i][1] {
					t.Fatalf("reciprocal conflict changed run %d on attempt %d: before=%v after=%d/%d", i, attempt, before[i], rows, facts)
				}
			}
		}
	})
}
