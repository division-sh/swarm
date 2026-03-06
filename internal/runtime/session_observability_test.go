package runtime

import (
	"bytes"
	"log"
	"strings"
	"testing"
)

func TestBuildRotationCheckpoint(t *testing.T) {
	s := &Session{
		Messages: []Message{
			{Role: "user", Content: "hello"},
			{Role: "assistant", Content: "world"},
		},
	}
	got := buildRotationCheckpoint("session not found", s)
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
	got := buildRotationCheckpoint("", s)
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

	logSessionRotated("a1", "cli_test", "old-sid", "new-sid", "scope-1", "session not found", 11, 2)
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
	logSessionRotated("a2", "api", "old", "new", "", longReason, 3, 1)
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

	logSessionAdopted("agent-9", "cli_test", "sess-old", "sess-new", "scope-z")
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
