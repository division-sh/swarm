package sessions

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

func TestInMemorySessionRegistryLeaseConflictAndRelease(t *testing.T) {
	sr := NewInMemoryRegistry(0)

	leaseA, err := sr.Acquire(context.Background(), "agent-a", RuntimeModeSession, SessionScopeGlobal, "worker-a", "global")
	if err != nil {
		t.Fatalf("acquire A: %v", err)
	}

	if _, err := sr.Acquire(context.Background(), "agent-a", RuntimeModeSession, SessionScopeGlobal, "worker-b", "global"); err == nil {
		t.Fatalf("expected lease conflict for worker-b")
	}

	if err := sr.Release(context.Background(), leaseA); err != nil {
		t.Fatalf("release A: %v", err)
	}

	leaseB, err := sr.Acquire(context.Background(), "agent-a", RuntimeModeSession, SessionScopeGlobal, "worker-b", "global")
	if err != nil {
		t.Fatalf("acquire B after release: %v", err)
	}
	if leaseB.SessionID != leaseA.SessionID {
		t.Fatalf("expected same active session id, got %s vs %s", leaseB.SessionID, leaseA.SessionID)
	}
}

func TestInMemorySessionRegistryRotate(t *testing.T) {
	sr := NewInMemoryRegistry(0)

	lease, err := sr.Acquire(context.Background(), "agent-a", RuntimeModeSession, SessionScopeGlobal, "worker-a", "global")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	old := lease.SessionID

	rotated, err := sr.Rotate(context.Background(), "agent-a", RuntimeModeSession, SessionScopeGlobal, "worker-a", RotationMetadata{
		CheckpointSummary: "checkpoint",
		RetryReason:       "session not found",
	}, "global")
	if err != nil {
		t.Fatalf("rotate: %v", err)
	}
	if rotated.SessionID == old {
		t.Fatalf("expected new session id after rotate")
	}
	if rotated.RetryReason != "session not found" {
		t.Fatalf("RetryReason = %q, want session not found", rotated.RetryReason)
	}
	if rotated.RetriesFromSessionID != old {
		t.Fatalf("RetriesFromSessionID = %q, want %q", rotated.RetriesFromSessionID, old)
	}
	rec, ok := sr.Snapshot("agent-a")
	if !ok {
		t.Fatal("expected snapshot record")
	}
	if rec.RetryReason != "session not found" || rec.RetriesFromSessionID != old {
		t.Fatalf("unexpected retry lineage in record: %+v", rec)
	}
	history := sr.History("agent-a")
	if len(history) != 1 {
		t.Fatalf("history len = %d, want 1", len(history))
	}
	if history[0].Status != "terminated" || history[0].TerminationReason != TerminationReasonContaminated.String() {
		t.Fatalf("terminated history = %+v", history[0])
	}
	if history[0].SuccessorSessionID != rotated.SessionID {
		t.Fatalf("SuccessorSessionID = %q, want %q", history[0].SuccessorSessionID, rotated.SessionID)
	}
}

func TestInMemorySessionRegistryAdoptSessionID(t *testing.T) {
	sr := NewInMemoryRegistry(0)
	_, err := sr.Acquire(context.Background(), "agent-a", RuntimeModeSession, SessionScopeGlobal, "worker-a", "global")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if err := sr.AdoptSessionID(context.Background(), "agent-a", RuntimeModeSession, SessionScopeGlobal, "worker-a", "claude-session-1", "global"); err != nil {
		t.Fatalf("adopt: %v", err)
	}
	rec, ok := sr.Snapshot("agent-a")
	if !ok {
		t.Fatal("expected snapshot record")
	}
	if rec.ProviderSessionID != "claude-session-1" {
		t.Fatalf("expected adopted provider session id, got %q", rec.ProviderSessionID)
	}
}

func TestInMemorySessionRegistry_TaskModeIsStateless(t *testing.T) {
	sr := NewInMemoryRegistry(0)
	if _, err := sr.Acquire(context.Background(), "agent-a", RuntimeModeTask, "", "worker-a", ""); err == nil {
		t.Fatal("expected task mode acquire to reject stateless sessions")
	}
	if _, err := sr.ResetAll(RuntimeModeTask, ResetMetadata{}); err != nil {
		t.Fatalf("ResetAll(task): %v", err)
	}
}

func TestInMemorySessionRegistry_SessionScopeRequiresExplicitDeclaration(t *testing.T) {
	sr := NewInMemoryRegistry(0)
	if _, err := sr.Acquire(context.Background(), "agent-a", RuntimeModeSession, "", "worker-a", ""); err == nil {
		t.Fatal("expected session acquire without explicit scope to fail closed")
	}
}

func TestInMemorySessionRegistry_AcquireSuspendedReturnsErrSessionSuspended(t *testing.T) {
	sr := NewInMemoryRegistry(0)
	sr.byKey[registryKey("agent-a", RuntimeModeSession, "global")] = &Record{
		SessionID:   "sess-1",
		AgentID:     "agent-a",
		RuntimeMode: RuntimeModeSession,
		ScopeKey:    "global",
		Status:      "suspended",
	}
	if _, err := sr.Acquire(context.Background(), "agent-a", RuntimeModeSession, SessionScopeGlobal, "worker-a", "global"); err != ErrSessionSuspended {
		t.Fatalf("expected ErrSessionSuspended, got %v", err)
	}
}

func TestInMemorySessionRegistryLifecycleProjectionOwnsCompleteSetAndExactReplay(t *testing.T) {
	sr := NewInMemoryRegistry(0)
	activeID := "10000000-0000-0000-0000-000000000001"
	suspendedID := "20000000-0000-0000-0000-000000000002"
	activeKey := registryKey("agent-a", RuntimeModeSession, "global")
	suspendedKey := registryKey("agent-a", RuntimeModeSession, "beta")
	sr.byKey[activeKey] = &Record{
		SessionID: activeID, ProviderSessionID: "provider-active", AgentID: "agent-a",
		RuntimeMode: RuntimeModeSession, ScopeKey: "global", Status: "active", TurnCount: 4,
	}
	sr.byKey[suspendedKey] = &Record{
		SessionID: suspendedID, ProviderSessionID: "provider-suspended", AgentID: "agent-a",
		RuntimeMode: RuntimeModeSession, ScopeKey: "beta", Status: "suspended", TurnCount: 7,
	}
	now := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	initial := runtimeeffects.LifecycleToken{RuntimeEpoch: 11, AgentID: "agent-a", Generation: 1}
	if _, replayed, err := sr.ApplyLifecycleProjection(context.Background(), LifecycleProjectionRequest{
		OperationID: "00000000-0000-0000-0000-000000001001", RequestHash: "register-hash", AgentID: "agent-a",
		Target: initial, TargetPhase: "running", Plan: LifecycleMutationPlan{}, Now: now,
	}); err != nil || replayed {
		t.Fatalf("initial projection replayed=%v err=%v", replayed, err)
	}

	operationID := "00000000-0000-0000-0000-000000001002"
	target := runtimeeffects.LifecycleToken{RuntimeEpoch: 11, AgentID: "agent-a", Generation: 2}
	req := LifecycleProjectionRequest{
		OperationID: operationID, RequestHash: "rotate-hash", AgentID: "agent-a", Expected: initial, Target: target,
		TargetPhase: "running", Plan: LifecycleMutationPlan{
			Action: LifecycleMutationRotateCurrentSet, TerminationReason: TerminationReasonNormal,
			TerminationDetail: "restart", CheckpointSummary: "clean restart",
		},
		Now: now.Add(time.Second),
	}
	outcome, replayed, err := sr.ApplyLifecycleProjection(context.Background(), req)
	if err != nil || replayed {
		t.Fatalf("rotate projection replayed=%v err=%v", replayed, err)
	}
	if len(outcome.Sessions) != 2 {
		t.Fatalf("rotated session count = %d, want 2", len(outcome.Sessions))
	}
	for _, mutation := range outcome.Sessions {
		want := LifecycleSuccessorSessionID(operationID, mutation.PreviousSessionID)
		if mutation.SuccessorSessionID != want {
			t.Fatalf("successor for %s = %q, want %q", mutation.PreviousSessionID, mutation.SuccessorSessionID, want)
		}
	}
	for key, previousID := range map[string]string{activeKey: activeID, suspendedKey: suspendedID} {
		rec := sr.byKey[key]
		if rec == nil || rec.SessionID != LifecycleSuccessorSessionID(operationID, previousID) {
			t.Fatalf("successor at %s = %#v", key, rec)
		}
		if rec.ProviderSessionID != "" || rec.TurnCount != 0 || rec.RetriesFromSessionID != previousID {
			t.Fatalf("successor retained mutable state: %#v", rec)
		}
	}
	replayedOutcome, replayed, err := sr.ApplyLifecycleProjection(context.Background(), req)
	if err != nil || !replayed || !reflect.DeepEqual(replayedOutcome, outcome) {
		t.Fatalf("exact replay outcome=%#v replayed=%v err=%v, want %#v", replayedOutcome, replayed, err, outcome)
	}
	changed := req
	changed.RequestHash = "changed-plan-hash"
	if _, _, err := sr.ApplyLifecycleProjection(context.Background(), changed); err == nil {
		t.Fatal("changed replay request was accepted")
	} else {
		var failure *runtimefailures.Error
		if !errors.As(err, &failure) || failure.Failure.Class != runtimefailures.ClassConflictingDuplicate {
			t.Fatalf("changed replay error = %v, want conflicting duplicate", err)
		}
	}
	staleCtx := runtimeeffects.WithLifecycleToken(context.Background(), initial)
	if _, err := sr.Acquire(staleCtx, "agent-a", RuntimeModeSession, SessionScopeGlobal, "worker", "global"); err == nil {
		t.Fatal("stale lifecycle token acquired rotated session")
	}
	currentCtx := runtimeeffects.WithLifecycleToken(context.Background(), target)
	if _, err := sr.Acquire(currentCtx, "agent-a", RuntimeModeSession, SessionScopeGlobal, "worker", "global"); err != nil {
		t.Fatalf("current lifecycle token acquire: %v", err)
	}

	terminateOutcome, replayed, err := sr.ApplyLifecycleProjection(context.Background(), LifecycleProjectionRequest{
		OperationID: "00000000-0000-0000-0000-000000001003", RequestHash: "terminate-hash", AgentID: "agent-a",
		Expected: target, Target: runtimeeffects.LifecycleToken{RuntimeEpoch: 11, AgentID: "agent-a", Generation: 3},
		TargetPhase: "terminated", Plan: LifecycleMutationPlan{
			Action: LifecycleMutationTerminateCurrentSet, TerminationReason: TerminationReasonNormal,
		},
		Now: now.Add(2 * time.Second),
	})
	if err != nil || replayed || len(terminateOutcome.Sessions) != 2 {
		t.Fatalf("terminate outcome=%#v replayed=%v err=%v", terminateOutcome, replayed, err)
	}
	if len(sr.byKey) != 0 {
		t.Fatalf("live set survived termination: %#v", sr.byKey)
	}
}
