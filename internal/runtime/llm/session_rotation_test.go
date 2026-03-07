package llm

import (
	"bytes"
	"context"
	"log"
	"strings"
	"testing"

	"empireai/internal/runtime/sessions"
)

type rotateStubRegistry struct {
	rotations int
}

func (r *rotateStubRegistry) Acquire(_ context.Context, _, runtimeMode, _ string, scopeKey string) (*sessions.Lease, error) {
	return &sessions.Lease{SessionID: "sess-1", AgentID: "a1", RuntimeMode: runtimeMode, LockOwner: "owner", ScopeKey: scopeKey}, nil
}

func (r *rotateStubRegistry) Release(_ context.Context, _ *sessions.Lease) error { return nil }

func (r *rotateStubRegistry) Rotate(_ context.Context, agentID, runtimeMode, lockOwner, _ string, scopeKey string) (*sessions.Lease, error) {
	r.rotations++
	return &sessions.Lease{SessionID: "sess-rotated", AgentID: agentID, RuntimeMode: runtimeMode, LockOwner: lockOwner, ScopeKey: scopeKey}, nil
}

func (r *rotateStubRegistry) IncrementTurn(_ context.Context, _, _, _, _ string) error { return nil }

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

	lease, err := MaybeRotateAfterTurn(context.Background(), s, "api", reg, "owner", 3)
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

	lease, err := MaybeRotateAfterParseFailures(context.Background(), s, "cli_test", reg, "owner", 2)
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
func TestBuildRotationCheckpoint(t *testing.T) {
	s := &Session{
		Messages: []Message{
			{Role: "user", Content: "hello"},
			{Role: "assistant", Content: "world"},
		},
	}
	got := BuildRotationCheckpoint("session not found", s)
	if !strings.Contains(got, "rotation_reason=session not found") {
		t.Fatalf("checkpoint missing reason: %q", got)
	}
	if !strings.Contains(got, "user: hello") {
		t.Fatalf("checkpoint missing summary content: %q", got)
	}
}

func TestBuildRotationCheckpoint_EmptyReasonUsesSummaryOnly(t *testing.T) {
	s := &Session{
		Messages: []Message{
			{Role: "user", Content: "single turn"},
		},
	}
	got := BuildRotationCheckpoint("", s)
	if strings.Contains(got, "rotation_reason=") {
		t.Fatalf("expected no rotation_reason prefix when reason empty, got %q", got)
	}
	if !strings.Contains(got, "user: single turn") {
		t.Fatalf("expected summary content preserved, got %q", got)
	}
}

func TestLogSessionRotated_EmitsStructuredFields(t *testing.T) {
	var buf bytes.Buffer
	prevWriter := log.Writer()
	prevFlags := log.Flags()
	log.SetOutput(&buf)
	log.SetFlags(0)
	t.Cleanup(func() {
		log.SetOutput(prevWriter)
		log.SetFlags(prevFlags)
	})

	LogSessionRotated("a1", "cli_test", "old-sid", "new-sid", "scope-1", "session not found", 11, 2)
	out := buf.String()
	if !strings.Contains(out, "session.rotated") {
		t.Fatalf("expected session.rotated marker, got %q", out)
	}
	if !strings.Contains(out, `agent="a1"`) || !strings.Contains(out, `runtime="cli_test"`) {
		t.Fatalf("expected agent/runtime fields, got %q", out)
	}
	if !strings.Contains(out, `old="old-sid"`) || !strings.Contains(out, `new="new-sid"`) {
		t.Fatalf("expected old/new session fields, got %q", out)
	}
}

func TestLogSessionRotated_TruncatesLongReason(t *testing.T) {
	var buf bytes.Buffer
	prevWriter := log.Writer()
	prevFlags := log.Flags()
	log.SetOutput(&buf)
	log.SetFlags(0)
	t.Cleanup(func() {
		log.SetOutput(prevWriter)
		log.SetFlags(prevFlags)
	})

	longReason := strings.Repeat("x", 260)
	LogSessionRotated("a2", "api", "old", "new", "", longReason, 3, 1)
	out := buf.String()
	if !strings.Contains(out, "session.rotated") {
		t.Fatalf("expected session.rotated marker, got %q", out)
	}
	if strings.Contains(out, longReason) {
		t.Fatalf("expected long reason to be truncated, got %q", out)
	}
	if !strings.Contains(out, `reason="`) || !strings.Contains(out, `..."`) {
		t.Fatalf("expected quoted truncated reason with ellipsis, got %q", out)
	}
}

func TestLogSessionAdopted_EmitsStructuredFields(t *testing.T) {
	var buf bytes.Buffer
	prevWriter := log.Writer()
	prevFlags := log.Flags()
	log.SetOutput(&buf)
	log.SetFlags(0)
	t.Cleanup(func() {
		log.SetOutput(prevWriter)
		log.SetFlags(prevFlags)
	})

	LogSessionAdopted("agent-9", "cli_test", "sess-old", "sess-new", "scope-z")
	out := buf.String()
	if !strings.Contains(out, "session.adopted") {
		t.Fatalf("expected session.adopted marker, got %q", out)
	}
	for _, want := range []string{`agent="agent-9"`, `runtime="cli_test"`, `old="sess-old"`, `new="sess-new"`, `scope="scope-z"`} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected field %s in output %q", want, out)
		}
	}
}
