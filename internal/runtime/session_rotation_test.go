package runtime

import (
	"bytes"
	"context"
	"empireai/internal/models"
	"log"
	"strings"
	"testing"

	llm "empireai/internal/runtime/llm"
	"empireai/internal/runtime/sessions"
	"time"
)

type rotateStubRegistry struct {
	rotations int
}

func (r *rotateStubRegistry) Acquire(_, runtimeMode, _ string, scopeKey string) (*sessions.Lease, error) {
	return &sessions.Lease{SessionID: "sess-1", AgentID: "a1", RuntimeMode: runtimeMode, LockOwner: "owner", ScopeKey: scopeKey}, nil
}

func (r *rotateStubRegistry) Release(_ *sessions.Lease) error { return nil }

func (r *rotateStubRegistry) Rotate(agentID, runtimeMode, lockOwner, _ string, scopeKey string) (*sessions.Lease, error) {
	r.rotations++
	return &sessions.Lease{SessionID: "sess-rotated", AgentID: agentID, RuntimeMode: runtimeMode, LockOwner: lockOwner, ScopeKey: scopeKey}, nil
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
	s := &llm.Session{
		ID:          "sess-1",
		AgentID:     "a1",
		RuntimeMode: "api",
		TurnCount:   3,
		Messages: []llm.Message{
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
	s := &llm.Session{
		ID:            "sess-1",
		AgentID:       "a1",
		RuntimeMode:   "cli_test",
		ParseFailures: 2,
		Messages: []llm.Message{
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
func TestMCPTurnRegistry_ExpiresEntriesByTTL(t *testing.T) {
	reg := newMCPTurnRegistry()
	now := time.Now().UTC()
	reg.put("expired", mcpTurnContext{
		Actor:     models.AgentConfig{ID: "a1"},
		CreatedAt: now.Add(-2 * time.Hour),
		ExpiresAt: now.Add(-1 * time.Minute),
	})
	reg.put("active", mcpTurnContext{
		Actor:     models.AgentConfig{ID: "a2"},
		CreatedAt: now,
		ExpiresAt: now.Add(20 * time.Minute),
	})

	reg.mu.Lock()
	reg.pruneLocked(now)
	reg.mu.Unlock()

	if _, ok := reg.get("expired"); ok {
		t.Fatal("expected expired token to be pruned")
	}
	if _, ok := reg.get("active"); !ok {
		t.Fatal("expected active token to remain")
	}
}

func TestRegisterMCPTurnContextWithTTL_RespectsCustomTTL(t *testing.T) {
	resetMCPTurnContexts()
	defer resetMCPTurnContexts()

	ctx := WithActor(context.Background(), models.AgentConfig{
		ID:   "agent-test",
		Role: "analysis-agent",
		Mode: "factory",
	})

	token := registerMCPTurnContextWithTTL(ctx, 50*time.Millisecond)
	if token == "" {
		t.Fatal("expected token")
	}
	if _, ok := resolveMCPTurnContext(token); !ok {
		t.Fatal("expected token to resolve immediately")
	}

	globalMCPTurnRegistry.mu.Lock()
	globalMCPTurnRegistry.pruneLocked(time.Now().UTC().Add(75 * time.Millisecond))
	globalMCPTurnRegistry.mu.Unlock()
	if _, ok := resolveMCPTurnContext(token); ok {
		t.Fatal("expected token to expire after custom TTL")
	}
}

func TestMCPTurnRegistry_UnregisterAndReset(t *testing.T) {
	resetMCPTurnContexts()
	defer resetMCPTurnContexts()

	ctx := WithActor(context.Background(), models.AgentConfig{
		ID:   "agent-cleanup",
		Role: "analysis-agent",
		Mode: "factory",
	})

	token1 := registerMCPTurnContextWithTTL(ctx, 2*time.Minute)
	token2 := registerMCPTurnContextWithTTL(ctx, 2*time.Minute)
	if token1 == "" || token2 == "" {
		t.Fatalf("expected non-empty tokens, got token1=%q token2=%q", token1, token2)
	}

	unregisterMCPTurnContext(token1)
	if _, ok := resolveMCPTurnContext(token1); ok {
		t.Fatal("expected unregister to remove token1")
	}
	if _, ok := resolveMCPTurnContext(token2); !ok {
		t.Fatal("expected token2 to remain before reset")
	}

	resetMCPTurnContexts()
	if _, ok := resolveMCPTurnContext(token2); ok {
		t.Fatal("expected reset to clear all tokens")
	}
}
func TestBuildRotationCheckpoint(t *testing.T) {
	s := &llm.Session{
		Messages: []llm.Message{
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
	s := &llm.Session{
		Messages: []llm.Message{
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
