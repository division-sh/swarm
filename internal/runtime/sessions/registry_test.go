package sessions

import (
	"context"
	"testing"
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
	if err := sr.ResetAll(RuntimeModeTask); err != nil {
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
