package runtime

import "testing"

func TestInMemorySessionRegistryLeaseConflictAndRelease(t *testing.T) {
	sr := NewInMemorySessionRegistry(0)

	leaseA, err := sr.Acquire("agent-a", "cli_test", "worker-a", "")
	if err != nil {
		t.Fatalf("acquire A: %v", err)
	}

	if _, err := sr.Acquire("agent-a", "cli_test", "worker-b", ""); err == nil {
		t.Fatalf("expected lease conflict for worker-b")
	}

	if err := sr.Release(leaseA); err != nil {
		t.Fatalf("release A: %v", err)
	}

	leaseB, err := sr.Acquire("agent-a", "cli_test", "worker-b", "")
	if err != nil {
		t.Fatalf("acquire B after release: %v", err)
	}
	if leaseB.SessionID != leaseA.SessionID {
		t.Fatalf("expected same active session id, got %s vs %s", leaseB.SessionID, leaseA.SessionID)
	}
}

func TestInMemorySessionRegistryRotate(t *testing.T) {
	sr := NewInMemorySessionRegistry(0)

	lease, err := sr.Acquire("agent-a", "cli_test", "worker-a", "")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	old := lease.SessionID

	rotated, err := sr.Rotate("agent-a", "cli_test", "worker-a", "checkpoint", "")
	if err != nil {
		t.Fatalf("rotate: %v", err)
	}
	if rotated.SessionID == old {
		t.Fatalf("expected new session id after rotate")
	}
}

func TestInMemorySessionRegistryAdoptSessionID(t *testing.T) {
	sr := NewInMemorySessionRegistry(0)
	_, err := sr.Acquire("agent-a", "cli_test", "worker-a", "")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if err := sr.AdoptSessionID("agent-a", "cli_test", "worker-a", "claude-session-1", ""); err != nil {
		t.Fatalf("adopt: %v", err)
	}
	rec, ok := sr.Snapshot("agent-a")
	if !ok {
		t.Fatal("expected snapshot record")
	}
	if rec.SessionID != "claude-session-1" {
		t.Fatalf("expected adopted session id, got %q", rec.SessionID)
	}
}
