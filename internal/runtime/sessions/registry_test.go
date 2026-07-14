package sessions

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

func testIdentity(agentID, runID, flowInstance string) agentmemory.Identity {
	return agentmemory.Identity{AgentID: agentID, RunID: runID, FlowInstance: flowInstance}
}

func TestInMemoryRegistryLeaseConflictAndRelease(t *testing.T) {
	sr := NewInMemoryRegistry(0)
	identity := testIdentity("agent-a", "run-a", "support/chat-a")
	leaseA, err := sr.Acquire(context.Background(), identity, "worker-a")
	if err != nil {
		t.Fatalf("acquire A: %v", err)
	}
	if _, err := sr.Acquire(context.Background(), identity, "worker-b"); err == nil {
		t.Fatal("expected lease conflict")
	}
	if err := sr.Release(context.Background(), leaseA); err != nil {
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

func TestInMemoryRegistryExactIdentityIsolation(t *testing.T) {
	sr := NewInMemoryRegistry(0)
	base, err := sr.Acquire(context.Background(), testIdentity("agent-a", "run-a", "support/chat-a"), "worker")
	if err != nil {
		t.Fatalf("base acquire: %v", err)
	}
	otherRun, err := sr.Acquire(context.Background(), testIdentity("agent-a", "run-b", "support/chat-a"), "worker")
	if err != nil {
		t.Fatalf("other run acquire: %v", err)
	}
	otherFlow, err := sr.Acquire(context.Background(), testIdentity("agent-a", "run-a", "support/chat-b"), "worker")
	if err != nil {
		t.Fatalf("other flow acquire: %v", err)
	}
	if base.SessionID == otherRun.SessionID || base.SessionID == otherFlow.SessionID || otherRun.SessionID == otherFlow.SessionID {
		t.Fatal("distinct memory identities shared a session")
	}
}

func TestInMemoryRegistryRotateAndAdopt(t *testing.T) {
	sr := NewInMemoryRegistry(0)
	identity := testIdentity("agent-a", "run-a", "support/chat-a")
	lease, err := sr.Acquire(context.Background(), identity, "worker")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	rotated, err := sr.Rotate(context.Background(), identity, "worker", RotationMetadata{CheckpointSummary: "checkpoint", RetryReason: "provider history invalid"})
	if err != nil {
		t.Fatalf("rotate: %v", err)
	}
	if rotated.SessionID == lease.SessionID || rotated.RetriesFromSessionID != lease.SessionID {
		t.Fatalf("rotation lineage = %#v", rotated)
	}
	if err := sr.AdoptSessionID(context.Background(), identity, "worker", "provider-session-a"); err != nil {
		t.Fatalf("adopt: %v", err)
	}
	rec, ok := sr.Snapshot(identity.AgentID)
	if !ok || rec.ProviderSessionID != "provider-session-a" {
		t.Fatalf("snapshot = %#v, ok=%v", rec, ok)
	}
}

func TestInMemoryRegistryLifecycleProjectionOwnsCompleteSet(t *testing.T) {
	sr := NewInMemoryRegistry(0)
	active := testIdentity("agent-a", "run-a", "support/chat-a")
	suspended := testIdentity("agent-a", "run-a", "support/chat-b")
	sr.byKey[registryKey(active)] = &Record{SessionID: "session-a", Identity: active, Status: "active"}
	sr.byKey[registryKey(suspended)] = &Record{SessionID: "session-b", Identity: suspended, Status: "suspended"}
	now := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	initial := runtimeeffects.LifecycleToken{RuntimeEpoch: 11, AgentID: "agent-a", Generation: 1}
	if _, replayed, err := sr.ApplyLifecycleProjection(context.Background(), LifecycleProjectionRequest{
		OperationID: "op-register", RequestHash: "register", AgentID: "agent-a", Target: initial, TargetPhase: "running", Now: now,
	}); err != nil || replayed {
		t.Fatalf("register replayed=%v err=%v", replayed, err)
	}
	target := runtimeeffects.LifecycleToken{RuntimeEpoch: 11, AgentID: "agent-a", Generation: 2}
	outcome, replayed, err := sr.ApplyLifecycleProjection(context.Background(), LifecycleProjectionRequest{
		OperationID: "op-rotate", RequestHash: "rotate", AgentID: "agent-a", Expected: initial, Target: target,
		TargetPhase: "running", Plan: LifecycleMutationPlan{Action: LifecycleMutationRotateCurrentSet, TerminationReason: TerminationReasonNormal}, Now: now.Add(time.Second),
	})
	if err != nil || replayed || len(outcome.Sessions) != 2 {
		t.Fatalf("rotate outcome=%#v replayed=%v err=%v", outcome, replayed, err)
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
		testIdentity("agent-a", "run-a", "support/chat-a"),
		testIdentity("agent-b", "run-b", "support/chat-b"),
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
