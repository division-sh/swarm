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

