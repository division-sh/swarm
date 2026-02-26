package runtime

import (
	"context"
	"testing"
)

type rotateStubRegistry struct {
	rotations int
}

func (r *rotateStubRegistry) Acquire(_, runtimeMode, _ string, scopeKey string) (*SessionLease, error) {
	return &SessionLease{SessionID: "sess-1", AgentID: "a1", RuntimeMode: runtimeMode, LockOwner: "owner", ScopeKey: scopeKey}, nil
}

func (r *rotateStubRegistry) Release(_ *SessionLease) error { return nil }

func (r *rotateStubRegistry) Rotate(agentID, runtimeMode, lockOwner, _ string, scopeKey string) (*SessionLease, error) {
	r.rotations++
	return &SessionLease{SessionID: "sess-rotated", AgentID: agentID, RuntimeMode: runtimeMode, LockOwner: lockOwner, ScopeKey: scopeKey}, nil
}

func (r *rotateStubRegistry) IncrementTurn(_, _, _, _ string) error { return nil }

type turnCapture struct {
	records []AgentTurnRecord
}

func (t *turnCapture) AppendAgentTurn(_ context.Context, rec AgentTurnRecord) error {
	t.records = append(t.records, rec)
	return nil
}

func TestMaybeRotateAfterTurn(t *testing.T) {
	s := &Session{
		ID:          "sess-1",
		AgentID:     "a1",
		RuntimeMode: "api",
		TurnCount:   3,
		Messages: []Message{
			{Role: "user", Content: "hello"},
			{Role: "assistant", Content: "world"},
		},
	}
	reg := &rotateStubRegistry{}

	lease, err := maybeRotateAfterTurn(s, "api", reg, "owner", 3)
	if err != nil {
		t.Fatalf("rotate error: %v", err)
	}
	if lease == nil {
		t.Fatalf("expected rotation lease")
	}
	if s.ID != "sess-rotated" {
		t.Fatalf("expected rotated session id, got %s", s.ID)
	}
	if s.TurnCount != 0 {
		t.Fatalf("expected turn count reset, got %d", s.TurnCount)
	}
	if len(s.Messages) != 1 || s.Messages[0].Role != "system" {
		t.Fatalf("expected summary message after rotation")
	}
}

func TestMaybeRotateAfterParseFailures(t *testing.T) {
	s := &Session{
		ID:            "sess-1",
		AgentID:       "a1",
		RuntimeMode:   "cli_test",
		ParseFailures: 2,
		Messages: []Message{
			{Role: "user", Content: "x"},
			{Role: "assistant", Content: "y"},
		},
	}
	reg := &rotateStubRegistry{}

	lease, err := maybeRotateAfterParseFailures(s, "cli_test", reg, "owner", 2)
	if err != nil {
		t.Fatalf("rotate error: %v", err)
	}
	if lease == nil {
		t.Fatalf("expected rotation lease")
	}
	if s.ParseFailures != 0 {
		t.Fatalf("expected parse failure reset, got %d", s.ParseFailures)
	}
	if s.ID != "sess-rotated" {
		t.Fatalf("expected rotated session id, got %s", s.ID)
	}
}
