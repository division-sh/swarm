package sessions

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

func testIdentity(t testing.TB, agentID, runID, instanceID string) agentmemory.Identity {
	t.Helper()
	return agentidentitytest.RuntimeForRun(t, runID, agentID, "test-fixture", "support", instanceID, "support/"+instanceID)
}

func TestInMemoryRegistryLeaseConflictAndRelease(t *testing.T) {
	sr := NewInMemoryRegistry(0)
	identity := testIdentity(t, "agent-a", "run-a", "chat-a")
	leaseA, err := sr.Acquire(context.Background(), identity, "worker-a")
	if err != nil {
		t.Fatalf("acquire A: %v", err)
	}
	if _, err := sr.Acquire(context.Background(), identity, "worker-b"); err == nil {
		t.Fatal("expected lease conflict")
	}
	if _, err := sr.ReleaseOutcome(context.Background(), leaseA); err != nil {
		t.Fatalf("release A: %v", err)
	}
	leaseB, err := sr.Acquire(context.Background(), identity, "worker-b")
	if err != nil {
		t.Fatalf("acquire B: %v", err)
	}
	if leaseB.SessionID != leaseA.SessionID {
		t.Fatalf("session changed across release: %s != %s", leaseB.SessionID, leaseA.SessionID)
	}
}

func TestInMemoryRegistryStaleSameOwnerGrantCannotMutateSuccessor(t *testing.T) {
	ctx := context.Background()
	registry := NewInMemoryRegistry(time.Minute)
	identity := testIdentity(t, "agent-a", "run-a", "chat-a")
	first, err := registry.Acquire(ctx, identity, "worker")
	if err != nil {
		t.Fatal(err)
	}
	second, err := registry.Acquire(ctx, identity, "worker")
	if err != nil {
		t.Fatal(err)
	}
	if first.SessionID != second.SessionID || first.GrantID == second.GrantID {
		t.Fatalf("same-owner reacquire did not replace grant: first=%+v second=%+v", first, second)
	}
	first.ExpiresAt = second.ExpiresAt
	if _, err := registry.Renew(ctx, first); !errors.Is(err, ErrSessionLeased) {
		t.Fatalf("stale renew=%v", err)
	}
	if result, err := registry.ReleaseOutcome(ctx, first); result.Acknowledged || !errors.Is(err, ErrSessionLeased) {
		t.Fatalf("stale release=%+v err=%v", result, err)
	}
	if result, err := registry.IncrementTurnOutcome(ctx, first); result.Acknowledged || !errors.Is(err, ErrSessionLeased) {
		t.Fatalf("stale turn=%+v err=%v", result, err)
	}
	if _, err := registry.Rotate(ctx, first, RotationMetadata{RetryReason: "stale"}); !errors.Is(err, ErrSessionLeased) {
		t.Fatalf("stale rotate=%v", err)
	}
	current, found := registry.Snapshot(identity)
	if !found || current.GrantID != second.GrantID || current.TurnCount != 0 || current.SessionID != second.SessionID {
		t.Fatalf("stale grant changed current session: %+v found=%v", current, found)
	}
	renewed, err := registry.Renew(ctx, second)
	if err != nil || renewed.GrantID != second.GrantID || renewed.ExpiresAt.Before(second.ExpiresAt) {
		t.Fatalf("renew=%+v err=%v", renewed, err)
	}
	if result, err := registry.IncrementTurnOutcome(ctx, second); err != nil || !result.Acknowledged {
		t.Fatalf("current turn=%+v err=%v", result, err)
	}
	third, err := registry.Rotate(ctx, second, RotationMetadata{RetryReason: "current"})
	if err != nil || third.GrantID == second.GrantID || third.SessionID == second.SessionID {
		t.Fatalf("current rotate=%+v err=%v", third, err)
	}
	if result, err := registry.ReleaseOutcome(ctx, second); result.Acknowledged || !errors.Is(err, ErrSessionLeased) {
		t.Fatalf("retired predecessor release=%+v err=%v", result, err)
	}
	if result, err := registry.ReleaseOutcome(ctx, third); err != nil || !result.Acknowledged {
		t.Fatalf("current release=%+v err=%v", result, err)
	}
	oldExpiry, err := registry.Acquire(ctx, identity, "worker")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Renew(ctx, oldExpiry); err != nil {
		t.Fatal(err)
	}
	if result, err := registry.ReleaseOutcome(ctx, oldExpiry); err != nil || !result.Acknowledged {
		t.Fatalf("renewed exact grant refused old-expiry cleanup: result=%+v err=%v", result, err)
	}
	expired, err := registry.Acquire(ctx, identity, "worker")
	if err != nil {
		t.Fatal(err)
	}
	registry.mu.Lock()
	registry.byKey[registryKey(identity)].LockExpiresAt = time.Now().Add(-time.Second)
	registry.mu.Unlock()
	if result, err := registry.ReleaseOutcome(ctx, expired); err != nil || !result.Acknowledged {
		t.Fatalf("expired exact grant cleanup: result=%+v err=%v", result, err)
	}
	if result, err := registry.ReleaseOutcome(ctx, expired); result.Acknowledged || !errors.Is(err, ErrSessionLeased) {
		t.Fatalf("duplicate expired cleanup: result=%+v err=%v", result, err)
	}
}

func TestLeaseHeartbeatCannotRenewSameOwnerReplacement(t *testing.T) {
	process := worklifetime.NewProcess()
	runtime, err := process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{RuntimeInstanceID: "heartbeat-test", BundleHash: "heartbeat-test"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := runtime.RetireAndWait(context.Background()); err != nil {
			t.Errorf("retire runtime: %v", err)
		}
		process.Retire()
		if _, err := process.Join(context.Background()); err != nil {
			t.Errorf("join process: %v", err)
		}
	}()
	work, err := runtime.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = work.Done() }()
	ctx := worklifetime.WithOccurrence(work.Context(), runtime)
	registry := NewInMemoryRegistry(10 * time.Second)
	identity := testIdentity(t, "agent-a", "run-a", "chat-a")
	first, err := registry.Acquire(ctx, identity, "worker")
	if err != nil {
		t.Fatal(err)
	}
	heartbeatFailure := make(chan error, 1)
	stop := StartLeaseHeartbeatWithErrorHandler(ctx, registry, first, func(err error) { heartbeatFailure <- err })
	defer stop()
	second, err := registry.Acquire(ctx, identity, "worker")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-heartbeatFailure:
		if !errors.Is(err, ErrSessionLeased) {
			t.Fatalf("heartbeat error=%v, want stale grant", err)
		}
	case <-time.After(7 * time.Second):
		t.Fatal("heartbeat did not report lost grant")
	}
	stop()
	current, found := registry.Snapshot(identity)
	if !found || current.GrantID != second.GrantID || !current.LockExpiresAt.Equal(second.ExpiresAt) {
		t.Fatalf("heartbeat changed replacement grant: %+v found=%v", current, found)
	}
}

func TestInMemoryRegistryExactIdentityIsolation(t *testing.T) {
	sr := NewInMemoryRegistry(0)
	base, err := sr.Acquire(context.Background(), testIdentity(t, "agent-a", "run-a", "chat-a"), "worker")
	if err != nil {
		t.Fatalf("base acquire: %v", err)
	}
	otherRun, err := sr.Acquire(context.Background(), testIdentity(t, "agent-a", "run-b", "chat-a"), "worker")
	if err != nil {
		t.Fatalf("other run acquire: %v", err)
	}
	otherFlow, err := sr.Acquire(context.Background(), testIdentity(t, "agent-a", "run-a", "chat-b"), "worker")
	if err != nil {
		t.Fatalf("other flow acquire: %v", err)
	}
	if base.SessionID == otherRun.SessionID || base.SessionID == otherFlow.SessionID || otherRun.SessionID == otherFlow.SessionID {
		t.Fatal("distinct memory identities shared a session")
	}
}

func TestInMemoryRegistryRotateGrant(t *testing.T) {
	sr := NewInMemoryRegistry(0)
	identity := testIdentity(t, "agent-a", "run-a", "chat-a")
	lease, err := sr.Acquire(context.Background(), identity, "worker")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	rotated, err := sr.Rotate(context.Background(), lease, RotationMetadata{CheckpointSummary: "checkpoint", RetryReason: "provider history invalid"})
	if err != nil {
		t.Fatalf("rotate: %v", err)
	}
	if rotated.SessionID == lease.SessionID || rotated.RetriesFromSessionID != lease.SessionID {
		t.Fatalf("rotation lineage = %#v", rotated)
	}
	rec, ok := sr.Snapshot(identity)
	if !ok || rec.GrantID != rotated.GrantID || rec.GrantID == lease.GrantID {
		t.Fatalf("snapshot = %#v, ok=%v", rec, ok)
	}
}

func TestInMemoryRegistryLifecycleProjectionOwnsCompleteSet(t *testing.T) {
	sr := NewInMemoryRegistry(0)
	active := testIdentity(t, "agent-a", "run-a", "chat-a")
	suspended := testIdentity(t, "agent-a", "run-a", "chat-b")
	sr.byKey[registryKey(active)] = &Record{SessionID: "session-a", Identity: active, Status: "active"}
	sr.byKey[registryKey(suspended)] = &Record{SessionID: "session-b", Identity: suspended, Status: "suspended"}
	now := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	initial := runtimeeffects.LifecycleToken{Identity: active, RuntimeEpoch: 11, AgentID: "agent-a", Generation: 1}
	if _, replayed, err := sr.ApplyLifecycleProjection(context.Background(), LifecycleProjectionRequest{
		OperationID: "op-register", RequestHash: "register", Target: initial, TargetPhase: "running", Now: now,
	}); err != nil || replayed {
		t.Fatalf("register replayed=%v err=%v", replayed, err)
	}
	target := runtimeeffects.LifecycleToken{Identity: active, RuntimeEpoch: 11, AgentID: "agent-a", Generation: 2}
	outcome, replayed, err := sr.ApplyLifecycleProjection(context.Background(), LifecycleProjectionRequest{
		OperationID: "op-rotate", RequestHash: "rotate", Expected: initial, Target: target,
		TargetPhase: "running", Plan: LifecycleMutationPlan{Action: LifecycleMutationRotateCurrentSet, TerminationReason: TerminationReasonNormal}, Now: now.Add(time.Second),
	})
	if err != nil || replayed || len(outcome.Sessions) != 1 {
		t.Fatalf("rotate outcome=%#v replayed=%v err=%v", outcome, replayed, err)
	}
	if sibling, ok := sr.Snapshot(suspended); !ok || sibling.SessionID != "session-b" || sibling.Status != "suspended" {
		t.Fatalf("sibling identity changed during exact lifecycle rotation: %#v ok=%v", sibling, ok)
	}
	staleCtx := runtimeeffects.WithLifecycleToken(context.Background(), initial)
	if _, err := sr.Acquire(staleCtx, active, "worker"); err == nil {
		t.Fatal("stale generation acquired memory")
	} else {
		var failure *runtimefailures.Error
		if !errors.As(err, &failure) || failure.Failure.Class != runtimefailures.ClassLifecycleConflict {
			t.Fatalf("stale error = %v", err)
		}
	}
	currentCtx := runtimeeffects.WithLifecycleToken(context.Background(), target)
	if _, err := sr.Acquire(currentCtx, active, "worker"); err != nil {
		t.Fatalf("current generation acquire: %v", err)
	}
}

func TestInMemoryRegistryResetTerminatesAllMemory(t *testing.T) {
	sr := NewInMemoryRegistry(0)
	for _, identity := range []agentmemory.Identity{
		testIdentity(t, "agent-a", "run-a", "chat-a"),
		testIdentity(t, "agent-b", "run-b", "chat-b"),
	} {
		if _, err := sr.Acquire(context.Background(), identity, "worker"); err != nil {
			t.Fatalf("acquire: %v", err)
		}
	}
	summary, err := sr.ResetAll(ResetMetadata{Source: "runtime reset"})
	if err != nil {
		t.Fatalf("ResetAll: %v", err)
	}
	if len(summary.OrphanedSessions) != 2 || len(sr.byKey) != 0 {
		t.Fatalf("reset summary=%#v live=%#v", summary, sr.byKey)
	}
}
