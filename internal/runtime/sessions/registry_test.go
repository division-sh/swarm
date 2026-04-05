package sessions

import (
	"context"
	"testing"
)

func TestInMemorySessionRegistryLeaseConflictAndRelease(t *testing.T) {
	sr := NewInMemoryRegistry(0)

	leaseA, err := sr.Acquire(context.Background(), "agent-a", RuntimeModeSession, "worker-a", "global")
	if err != nil {
		t.Fatalf("acquire A: %v", err)
	}

	if _, err := sr.Acquire(context.Background(), "agent-a", RuntimeModeSession, "worker-b", "global"); err == nil {
		t.Fatalf("expected lease conflict for worker-b")
	}

	if err := sr.Release(context.Background(), leaseA); err != nil {
		t.Fatalf("release A: %v", err)
	}

	leaseB, err := sr.Acquire(context.Background(), "agent-a", RuntimeModeSession, "worker-b", "global")
	if err != nil {
		t.Fatalf("acquire B after release: %v", err)
	}
	if leaseB.SessionID != leaseA.SessionID {
		t.Fatalf("expected same active session id, got %s vs %s", leaseB.SessionID, leaseA.SessionID)
	}
}

func TestInMemorySessionRegistryRotate(t *testing.T) {
	sr := NewInMemoryRegistry(0)

	lease, err := sr.Acquire(context.Background(), "agent-a", RuntimeModeSession, "worker-a", "global")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	old := lease.SessionID

	rotated, err := sr.Rotate(context.Background(), "agent-a", RuntimeModeSession, "worker-a", "checkpoint", "global")
	if err != nil {
		t.Fatalf("rotate: %v", err)
	}
	if rotated.SessionID == old {
		t.Fatalf("expected new session id after rotate")
	}
}

func TestInMemorySessionRegistryAdoptSessionID(t *testing.T) {
	sr := NewInMemoryRegistry(0)
	_, err := sr.Acquire(context.Background(), "agent-a", RuntimeModeSession, "worker-a", "global")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if err := sr.AdoptSessionID(context.Background(), "agent-a", RuntimeModeSession, "worker-a", "claude-session-1", "global"); err != nil {
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
	if _, err := sr.Acquire(context.Background(), "agent-a", RuntimeModeTask, "worker-a", ""); err == nil {
		t.Fatal("expected task mode acquire to reject stateless sessions")
	}
	if err := sr.ResetAll(RuntimeModeTask); err != nil {
		t.Fatalf("ResetAll(task): %v", err)
	}
}

func TestInMemorySessionRegistry_SessionScopeRequiresExplicitDeclaration(t *testing.T) {
	sr := NewInMemoryRegistry(0)
	if _, err := sr.Acquire(context.Background(), "agent-a", RuntimeModeSession, "worker-a", ""); err == nil {
		t.Fatal("expected session acquire without explicit scope to fail closed")
	}
}
