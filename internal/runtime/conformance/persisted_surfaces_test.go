package conformance

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"swarm/internal/config"
	dashboardserver "swarm/internal/dashboard/server"
	"swarm/internal/events"
	runtimepkg "swarm/internal/runtime"
	runtimebus "swarm/internal/runtime/bus"
	runtimecontracts "swarm/internal/runtime/contracts"
	runtimeactors "swarm/internal/runtime/core/actors"
	runtimeownership "swarm/internal/runtime/core/ownership"
	runtimecorrelation "swarm/internal/runtime/correlation"
	runtimediaglog "swarm/internal/runtime/diaglog"
	runtimellm "swarm/internal/runtime/llm"
	runtimemanager "swarm/internal/runtime/manager"
	runtimemutationlog "swarm/internal/runtime/mutationlog"
	runtimepipeline "swarm/internal/runtime/pipeline"
	runtimereplayclaim "swarm/internal/runtime/replayclaim"
	runtimesemanticview "swarm/internal/runtime/semanticview"
	runtimesessions "swarm/internal/runtime/sessions"
	runtimetools "swarm/internal/runtime/tools"
	runtimeworkspace "swarm/internal/runtime/workspace"
	"swarm/internal/store"
	"swarm/internal/testutil"
)

type staticWorkspaceResolver struct {
	target *runtimeworkspace.Target
	err    error
}

func (s staticWorkspaceResolver) ResolveWorkspace(context.Context, runtimeactors.AgentConfig) (*runtimeworkspace.Target, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.target, nil
}

func acquireLiveConversationSession(t *testing.T, ctx context.Context, db *sql.DB, agentID string, runtimeMode runtimesessions.RuntimeMode, sessionScope runtimesessions.SessionScope, scopeKey string) string {
	t.Helper()
	registry := runtimesessions.NewPostgresRegistry(db, 30*time.Second)
	lease, err := registry.Acquire(ctx, agentID, runtimeMode, sessionScope, "test-owner", scopeKey)
	if err != nil {
		t.Fatalf("Acquire(%s,%s,%s): %v", agentID, runtimeMode, scopeKey, err)
	}
	if err := registry.Release(ctx, lease); err != nil {
		t.Fatalf("Release(%s,%s): %v", agentID, lease.SessionID, err)
	}
	return lease.SessionID
}

func TestCanonicalTurnSummarySurface_RoundTripsThroughConversationReader(t *testing.T) {
	ctx := context.Background()
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}

	requireCanonicalConversationSurface(t, ctx, pg)
	seedConformanceAgent(t, ctx, pg, "agent-1")

	sessionID := uuid.NewString()
	if err := pg.UpsertConversation(ctx, runtimellm.ConversationRecord{
		SessionID: sessionID,
		AgentID:   "agent-1",
		ScopeKey:  "global",
		Mode:      "task",
		Messages: []runtimellm.Message{
			{Role: "assistant", Content: "Parking for manual review."},
		},
		Summary:   "14-day review scheduled.",
		TurnCount: 1,
		Status:    "active",
	}); err != nil {
		t.Fatalf("UpsertConversation: %v", err)
	}

	if err := pg.AppendAgentTurn(ctx, runtimellm.AgentTurnRecord{
		AgentID:     "agent-1",
		RuntimeMode: "task",
		SessionID:   sessionID,
		ScopeKey:    "global",
		TurnBlocks: []runtimellm.TurnBlock{
			{Kind: "dispatch", Title: "task.run"},
			{Kind: "tool_use", ToolName: "schedule", Input: json.RawMessage(`{"delay_seconds":1209600}`), Data: json.RawMessage(`{"tool_use_id":"toolu_1"}`)},
			{Kind: "tool_result", Output: json.RawMessage(`{"status":"scheduled"}`), Data: json.RawMessage(`{"tool_use_id":"toolu_1"}`)},
			{Kind: "assistant_text", Text: "Parking for manual review."},
			{Kind: "outcome", Text: "14-day review scheduled."},
		},
		RequestPayload: []byte(`{"kind":"task"}`),
		ResponseRaw:    []byte(`{"result":"stale fallback text"}`),
		ParseOK:        true,
		Latency:        5 * time.Millisecond,
	}); err != nil {
		t.Fatalf("AppendAgentTurn: %v", err)
	}

	reader := dashboardserver.NewSQLConversationReader(db, pg)
	if reader == nil {
		t.Fatal("NewSQLConversationReader returned nil")
	}
	item, ok, err := reader.Get(ctx, sessionID)
	if err != nil {
		t.Fatalf("Get conversation: %v", err)
	}
	if !ok {
		t.Fatalf("conversation %s not found", sessionID)
	}
	if len(item.Turns) != 1 {
		t.Fatalf("conversation turns = %d, want 1", len(item.Turns))
	}
	turn := item.Turns[0]
	if got := countTurnSummaryBlocks(turn.TurnBlocks); got != 1 {
		t.Fatalf("turn_summary block count = %d, want 1 in %#v", got, turn.TurnBlocks)
	}
	if turn.AssistantVisibleOutput != "Parking for manual review." {
		t.Fatalf("assistant_visible_output = %q, want %q", turn.AssistantVisibleOutput, "Parking for manual review.")
	}
	if turn.Outcome != "14-day review scheduled." {
		t.Fatalf("outcome = %q, want %q", turn.Outcome, "14-day review scheduled.")
	}
	if len(turn.ToolResults) != 1 {
		t.Fatalf("tool_results = %#v, want 1 canonical summary tool result", turn.ToolResults)
	}
	if strings.TrimSpace(turn.ToolResults[0].ToolName) != "schedule" {
		t.Fatalf("tool_result.tool_name = %#v, want schedule", turn.ToolResults[0].ToolName)
	}
}

func TestCanonicalSessionWatchdogSurface_RoundTripsThroughConversationReader(t *testing.T) {
	ctx := context.Background()
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}

	requireCanonicalConversationSurface(t, ctx, pg)
	seedConformanceAgent(t, ctx, pg, "agent-1")

	sessionID := acquireLiveConversationSession(t, ctx, db, "agent-1", runtimesessions.RuntimeModeSession, runtimesessions.SessionScopeGlobal, "global")
	if err := pg.UpsertConversation(ctx, runtimellm.ConversationRecord{
		SessionID:    sessionID,
		AgentID:      "agent-1",
		SessionScope: "global",
		ScopeKey:     "global",
		Mode:         "session",
		Messages: []runtimellm.Message{
			{Role: "assistant", Content: "Still working on it."},
		},
		Summary:   "Working",
		TurnCount: 3,
		Status:    "active",
	}); err != nil {
		t.Fatalf("UpsertConversation(session): %v", err)
	}
	if err := pg.UpdateLiveSessionWatchdog(ctx, runtimellm.ConversationWatchdogUpdate{
		SessionID:    sessionID,
		AgentID:      "agent-1",
		SessionScope: "global",
		ScopeKey:     "global",
		Mode:         "session",
		Watchdog: &runtimellm.ConversationWatchdog{
			State:         "no_output",
			BlockingLayer: "session_execution",
			Action:        "session_no_output",
			Outcome:       "warning_emitted",
			RecordedAt:    "2026-04-10T12:00:30Z",
		},
	}); err != nil {
		t.Fatalf("UpdateLiveSessionWatchdog: %v", err)
	}

	reader := dashboardserver.NewSQLConversationReader(db, pg)
	if reader == nil {
		t.Fatal("NewSQLConversationReader returned nil")
	}
	item, ok, err := reader.Get(ctx, sessionID)
	if err != nil {
		t.Fatalf("Get conversation: %v", err)
	}
	if !ok {
		t.Fatalf("conversation %s not found", sessionID)
	}
	if item.RuntimeState.Watchdog == nil {
		t.Fatal("expected runtime_state.watchdog to round-trip")
	}
	if item.RuntimeState.Watchdog.State != "no_output" || item.RuntimeState.Watchdog.Action != "session_no_output" {
		t.Fatalf("unexpected runtime_state.watchdog: %+v", item.RuntimeState.Watchdog)
	}
	items, err := reader.List(ctx, 10)
	if err != nil {
		t.Fatalf("List conversations: %v", err)
	}
	if len(items) == 0 || items[0].Metadata.Watchdog == nil {
		t.Fatalf("expected list metadata watchdog, got %#v", items)
	}
	if items[0].Metadata.Watchdog.Outcome != "warning_emitted" {
		t.Fatalf("list watchdog outcome = %q, want warning_emitted", items[0].Metadata.Watchdog.Outcome)
	}
}

func TestReusedLiveSessionKeepsDeliveryFrontierBoundToCanonicalSession(t *testing.T) {
	ctx := context.Background()
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	pg := &store.PostgresStore{DB: db}

	requireCanonicalConversationSurface(t, ctx, pg)
	requireCanonicalDeliveryLifecycleSurface(t, ctx, pg)
	seedConformanceAgent(t, ctx, pg, "agent-1")

	runID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}

	event1 := events.Event{
		ID:          uuid.NewString(),
		RunID:       runID,
		Type:        events.EventType("review.requested"),
		SourceAgent: "runtime",
		Payload:     []byte(`{"turn":1}`),
		CreatedAt:   time.Now().Add(-2 * time.Minute).UTC(),
	}
	event2 := events.Event{
		ID:          uuid.NewString(),
		RunID:       runID,
		Type:        events.EventType("review.requested"),
		SourceAgent: "runtime",
		Payload:     []byte(`{"turn":2}`),
		CreatedAt:   time.Now().Add(-1 * time.Minute).UTC(),
	}
	for _, evt := range []events.Event{event1, event2} {
		if err := pg.AppendEvent(ctx, evt); err != nil {
			t.Fatalf("AppendEvent(%s): %v", evt.ID, err)
		}
		if err := pg.InsertEventDeliveries(ctx, evt.ID, []string{"agent-1"}); err != nil {
			t.Fatalf("InsertEventDeliveries(%s): %v", evt.ID, err)
		}
	}

	previousTransport := http.DefaultTransport
	http.DefaultTransport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header: http.Header{
				"Content-Type": []string{"application/json"},
			},
			Body: io.NopCloser(strings.NewReader(`{
				"model":"claude-test",
				"usage":{"input_tokens":1,"output_tokens":1},
				"content":[{"type":"text","text":"ok"}]
			}`)),
			Request: req,
		}, nil
	})
	defer func() {
		http.DefaultTransport = previousTransport
	}()
	t.Setenv("ANTHROPIC_API_KEY", "test-key")

	bus, err := runtimebus.NewEventBus(pg)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	registry := runtimesessions.NewPostgresRegistry(db, 30*time.Second)
	runtime := runtimellm.NewAnthropicAPIRuntime(&config.Config{
		LLM: config.LLMConfig{
			ClaudeAPI: config.ClaudeAPIConfig{
				DefaultModel: "claude-test",
			},
		},
	}, registry, "worker-1", nil, pg, nil, bus)

	newTurnContext := func(evt events.Event) context.Context {
		base := runtimesessions.WithScope(context.Background(), runtimesessions.RuntimeModeSession.String(), runtimesessions.SessionScopeGlobal.String(), "global")
		base = runtimecorrelation.WithRunID(base, runID)
		base = runtimebus.WithInboundEvent(base, evt)
		return runtimeactors.WithActor(base, runtimeactors.AgentConfig{
			ID:               "agent-1",
			Type:             "stub",
			SessionScope:     runtimesessions.SessionScopeGlobal.String(),
			ConversationMode: runtimesessions.RuntimeModeSession.String(),
		})
	}

	session, err := runtime.StartSession(newTurnContext(event1), "agent-1", "system", nil)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	if _, err := runtime.ContinueSession(newTurnContext(event1), session, runtimellm.Message{Role: "user", Content: "first"}); err != nil {
		t.Fatalf("ContinueSession(first): %v", err)
	}
	if err := pg.UpsertEventReceipt(newTurnContext(event1), event1.ID, "agent-1", runtimemanager.ReceiptStatusProcessed, ""); err != nil {
		t.Fatalf("UpsertEventReceipt(first): %v", err)
	}

	if err := pg.MarkEventDeliveryInProgress(newTurnContext(event2), event2.ID, "agent-1", ""); err != nil {
		t.Fatalf("MarkEventDeliveryInProgress(second prelaunch): %v", err)
	}
	if _, err := runtime.ContinueSession(newTurnContext(event2), session, runtimellm.Message{Role: "user", Content: "second"}); err != nil {
		t.Fatalf("ContinueSession(second): %v", err)
	}

	var (
		deliveryStatus   string
		activeSessionID  string
		sessionStatus    string
		sessionScopeKey  string
		liveSessionCount int
	)
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(status, ''), COALESCE(active_session_id::text, '')
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'agent'
		  AND subscriber_id = 'agent-1'
	`, event2.ID).Scan(&deliveryStatus, &activeSessionID); err != nil {
		t.Fatalf("load second delivery: %v", err)
	}
	if deliveryStatus != "in_progress" {
		t.Fatalf("second delivery status = %q, want in_progress", deliveryStatus)
	}
	if activeSessionID != session.ID {
		t.Fatalf("second delivery active_session_id = %q, want %q", activeSessionID, session.ID)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT
			COALESCE(status, ''),
			COALESCE(scope_key, '')
		FROM agent_sessions
		WHERE session_id = $1::uuid
	`, session.ID).Scan(&sessionStatus, &sessionScopeKey); err != nil {
		t.Fatalf("load live session row: %v", err)
	}
	if sessionStatus != "active" {
		t.Fatalf("session status = %q, want active", sessionStatus)
	}
	if sessionScopeKey != "global" {
		t.Fatalf("session scope_key = %q, want global", sessionScopeKey)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM agent_sessions
		WHERE agent_id = 'agent-1'
		  AND scope_key = 'global'
		  AND runtime_mode = 'session'
		  AND status = 'active'
	`).Scan(&liveSessionCount); err != nil {
		t.Fatalf("count live session lineage: %v", err)
	}
	if liveSessionCount != 1 {
		t.Fatalf("live session lineage count = %d, want 1", liveSessionCount)
	}

	facts, err := pg.ListAgentLifecycleFacts(ctx, []string{"agent-1"})
	if err != nil {
		t.Fatalf("ListAgentLifecycleFacts: %v", err)
	}
	if got := facts["agent-1"].CurrentState; got != "active" {
		t.Fatalf("lifecycle current_state = %q, want active", got)
	}
	if got := facts["agent-1"].BlockingLayer; got != "session_execution" {
		t.Fatalf("lifecycle blocking_layer = %q, want session_execution", got)
	}
}

func TestRotatedLiveSessionRebindsDeliveryFrontierToSuccessorSession(t *testing.T) {
	ctx := context.Background()
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	pg := &store.PostgresStore{DB: db}

	requireCanonicalConversationSurface(t, ctx, pg)
	requireCanonicalDeliveryLifecycleSurface(t, ctx, pg)
	seedConformanceAgent(t, ctx, pg, "agent-1")

	runID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}

	eventID := uuid.NewString()
	evt := events.Event{
		ID:          eventID,
		RunID:       runID,
		Type:        events.EventType("review.requested"),
		SourceAgent: "runtime",
		Payload:     []byte(`{"turn":1}`),
		CreatedAt:   time.Now().UTC(),
	}
	if err := pg.AppendEvent(ctx, evt); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}
	if err := pg.InsertEventDeliveries(ctx, evt.ID, []string{"agent-1"}); err != nil {
		t.Fatalf("InsertEventDeliveries: %v", err)
	}

	dockerState := filepath.Join(t.TempDir(), "fake-docker-count")
	fakeDocker := filepath.Join(t.TempDir(), "docker")
	script := `#!/bin/sh
state="$SWARM_FAKE_DOCKER_STATE"
count=0
if [ -f "$state" ]; then
  count=$(cat "$state")
fi
count=$((count + 1))
printf "%s" "$count" > "$state"
if [ "$count" -eq 1 ]; then
  printf "session not found" >&2
  exit 1
fi
printf '{"result":"ok"}'
`
	if err := os.WriteFile(fakeDocker, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake docker: %v", err)
	}
	t.Setenv("SWARM_DOCKER_BIN", fakeDocker)
	t.Setenv("SWARM_FAKE_DOCKER_STATE", dockerState)
	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "oauth-token")

	registry := runtimesessions.NewPostgresRegistry(db, 30*time.Second)
	bus, err := runtimebus.NewEventBus(pg)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	runtime := runtimellm.NewClaudeCLIRuntime(&config.Config{
		LLM: config.LLMConfig{
			ClaudeCLI: config.ClaudeCLIConfig{
				Command:      "claude",
				OutputFormat: "json",
			},
		},
	}, registry, "worker-1", nil, nil, staticWorkspaceResolver{
		target: &runtimeworkspace.Target{
			Container: "swarm-agent-1",
			Workdir:   "/workspace",
		},
	}, pg, bus)

	newTurnContext := func(evt events.Event) context.Context {
		base := runtimesessions.WithScope(context.Background(), runtimesessions.RuntimeModeSession.String(), runtimesessions.SessionScopeGlobal.String(), "global")
		base = runtimecorrelation.WithRunID(base, runID)
		base = runtimebus.WithInboundEvent(base, evt)
		return runtimeactors.WithActor(base, runtimeactors.AgentConfig{
			ID:               "agent-1",
			Type:             "stub",
			SessionScope:     runtimesessions.SessionScopeGlobal.String(),
			ConversationMode: runtimesessions.RuntimeModeSession.String(),
		})
	}

	session, err := runtime.StartSession(newTurnContext(evt), "agent-1", "system", nil)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	originalSessionID := session.ID
	if err := pg.MarkEventDeliveryInProgress(newTurnContext(evt), evt.ID, "agent-1", ""); err != nil {
		t.Fatalf("MarkEventDeliveryInProgress(prelaunch): %v", err)
	}
	resp, err := runtime.ContinueSession(newTurnContext(evt), session, runtimellm.Message{Role: "user", Content: "rotate me"})
	if err != nil {
		t.Fatalf("ContinueSession: %v", err)
	}
	if resp == nil || strings.TrimSpace(resp.Message.Content) != "ok" {
		t.Fatalf("response = %#v, want assistant ok", resp)
	}
	if session.ID == originalSessionID {
		t.Fatalf("session ID did not rotate; got %q", session.ID)
	}

	var (
		deliveryStatus         string
		activeSessionID        string
		predecessorStatus      string
		successorStatus        string
		successorRetriesFromID string
	)
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(status, ''), COALESCE(active_session_id::text, '')
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'agent'
		  AND subscriber_id = 'agent-1'
	`, evt.ID).Scan(&deliveryStatus, &activeSessionID); err != nil {
		t.Fatalf("load delivery: %v", err)
	}
	if deliveryStatus != "in_progress" {
		t.Fatalf("delivery status = %q, want in_progress", deliveryStatus)
	}
	if activeSessionID != session.ID {
		t.Fatalf("delivery active_session_id = %q, want %q", activeSessionID, session.ID)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(status, '')
		FROM agent_sessions
		WHERE session_id = $1::uuid
	`, originalSessionID).Scan(&predecessorStatus); err != nil {
		t.Fatalf("load predecessor session: %v", err)
	}
	if predecessorStatus != "terminated" {
		t.Fatalf("predecessor status = %q, want terminated", predecessorStatus)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT
			COALESCE(status, ''),
			COALESCE(runtime_state->>'retries_from_session_id', '')
		FROM agent_sessions
		WHERE session_id = $1::uuid
	`, session.ID).Scan(&successorStatus, &successorRetriesFromID); err != nil {
		t.Fatalf("load successor session: %v", err)
	}
	if successorStatus != "active" {
		t.Fatalf("successor status = %q, want active", successorStatus)
	}
	if successorRetriesFromID != originalSessionID {
		t.Fatalf("successor retries_from_session_id = %q, want %q", successorRetriesFromID, originalSessionID)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

func TestConversationPersistenceDoesNotPromoteAuditRowsIntoLiveSessions(t *testing.T) {
	ctx := context.Background()
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}

	requireCanonicalConversationSurface(t, ctx, pg)
	seedConformanceAgent(t, ctx, pg, "agent-1")

	err := pg.UpsertConversation(ctx, runtimellm.ConversationRecord{
		SessionID:    uuid.NewString(),
		AgentID:      "agent-1",
		Mode:         "session",
		SessionScope: "global",
		ScopeKey:     "global",
		Messages:     []runtimellm.Message{{Role: "assistant", Content: "should fail"}},
		Summary:      "should fail",
		TurnCount:    1,
		Status:       "active",
	})
	if err == nil {
		t.Fatal("expected live conversation persistence without a live session row to fail")
	}

	taskSessionID := uuid.NewString()
	if err := pg.UpsertConversation(ctx, runtimellm.ConversationRecord{
		SessionID: taskSessionID,
		AgentID:   "agent-1",
		Mode:      "task",
		Messages:  []runtimellm.Message{{Role: "assistant", Content: "done"}},
		Summary:   "done",
		TurnCount: 1,
		Status:    "active",
	}); err != nil {
		t.Fatalf("UpsertConversation(task): %v", err)
	}

	reader := dashboardserver.NewSQLConversationReader(db, pg)
	if reader == nil {
		t.Fatal("NewSQLConversationReader returned nil")
	}
	items, err := reader.List(ctx, 10)
	if err != nil {
		t.Fatalf("List conversations: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("conversation count = %d, want 1", len(items))
	}
	if items[0].Kind != "turn_audit" || items[0].SessionID != taskSessionID {
		t.Fatalf("unexpected conversation summary: %+v", items[0])
	}
}

func TestTaskConversationReader_HidesLegacyTaskRowsWithoutCanonicalAudit(t *testing.T) {
	ctx := context.Background()
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}

	requireCanonicalConversationSurface(t, ctx, pg)
	seedConformanceAgent(t, ctx, pg, "agent-1")

	sessionID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `ALTER TABLE agent_sessions DROP CONSTRAINT IF EXISTS agent_sessions_runtime_mode_check`); err != nil {
		t.Fatalf("drop task-runtime_mode check for mixed-rollout fixture: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, agent_id, scope_key, scope, conversation, turn_count, runtime_mode, runtime_state, status, created_at, updated_at
		) VALUES (
			$1::uuid,
			'agent-1',
			'',
			'global',
			'[{"role":"assistant","content":"legacy task"}]'::jsonb,
			1,
			'task',
			'{"summary":"legacy task"}'::jsonb,
			'active',
			now() - interval '5 minutes',
			now() - interval '5 minutes'
		)
	`, sessionID); err != nil {
		t.Fatalf("seed legacy task session row: %v", err)
	}

	reader := dashboardserver.NewSQLConversationReader(db, pg)
	if reader == nil {
		t.Fatal("NewSQLConversationReader returned nil")
	}
	items, err := reader.List(ctx, 10)
	if err != nil {
		t.Fatalf("List conversations: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("conversation count = %d, want 0", len(items))
	}

	item, ok, err := reader.Get(ctx, sessionID)
	if err != nil {
		t.Fatalf("Get conversation: %v", err)
	}
	if ok {
		t.Fatalf("expected legacy-only task conversation %s to stay hidden, got %+v", sessionID, item)
	}

	if err := pg.UpsertConversation(ctx, runtimellm.ConversationRecord{
		SessionID: sessionID,
		AgentID:   "agent-1",
		Mode:      "task",
		Messages: []runtimellm.Message{
			{Role: "assistant", Content: "canonical task"},
		},
		Summary:   "canonical task",
		TurnCount: 2,
		Status:    "active",
	}); err != nil {
		t.Fatalf("UpsertConversation(task): %v", err)
	}

	items, err = reader.List(ctx, 10)
	if err != nil {
		t.Fatalf("List conversations after canonical write: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("conversation count after canonical write = %d, want 1", len(items))
	}
	if items[0].SessionID != sessionID || items[0].Summary != "canonical task" {
		t.Fatalf("unexpected canonical summary: %+v", items[0])
	}

	item, ok, err = reader.Get(ctx, sessionID)
	if err != nil {
		t.Fatalf("Get conversation after canonical write: %v", err)
	}
	if !ok {
		t.Fatalf("canonical conversation %s not found", sessionID)
	}
	if len(item.Messages) != 1 || item.Messages[0].Content != "canonical task" || item.TurnCount != 2 {
		t.Fatalf("unexpected canonical detail: %+v", item)
	}
}

func TestCanonicalRuntimeLogSurface_RoundTripsThroughObservabilityReader(t *testing.T) {
	ctx := context.Background()
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}

	requireCanonicalRuntimeLogSurface(t, ctx, pg)

	entityID := uuid.NewString()
	parentEventID := uuid.NewString()
	logger := runtimepkg.NewRuntimeLogger(db, pg)
	if err := logger.Log(ctx, runtimepkg.RuntimeLogEntry{
		Level:      "warn",
		Message:    "Tool execution was denied for save_entity_field",
		Component:  "tool-executor",
		Action:     "tool_execution_denied",
		EventID:    "evt-1",
		EventType:  "validation/requested",
		AgentID:    "agent-1",
		EntityID:   entityID,
		SessionID:  "session-1",
		Error:      "runtime_error code=cross_flow_write_forbidden",
		DurationUS: 1200,
		Detail: map[string]any{
			"tool_name":       "save_entity_field",
			"denial_layer":    "executor",
			"error_code":      "cross_flow_write_forbidden",
			"handler_id":      "tool-handler",
			"parent_event_id": parentEventID,
		},
	}); err != nil {
		t.Fatalf("logger.Log() error = %v", err)
	}

	reader := dashboardserver.NewSQLObservabilityReader(db, pg)
	if reader == nil {
		t.Fatal("NewSQLObservabilityReader returned nil")
	}
	logs, err := reader.ListRuntimeLogs(ctx, dashboardserver.RuntimeLogFilter{
		Component: "tool-executor",
		Level:     "warn",
	}, 10)
	if err != nil {
		t.Fatalf("ListRuntimeLogs: %v", err)
	}
	if len(logs) != 1 {
		t.Fatalf("runtime log rows = %d, want 1: %#v", len(logs), logs)
	}
	log := logs[0]
	if log.Level != "warn" {
		t.Fatalf("log level = %q, want warn", log.Level)
	}
	if log.Component != "tool-executor" {
		t.Fatalf("log component = %q, want tool-executor", log.Component)
	}
	if log.Action != "tool_execution_denied" {
		t.Fatalf("log action = %q, want tool_execution_denied", log.Action)
	}
	if log.EventType != "validation/requested" {
		t.Fatalf("log event_type = %q, want validation/requested", log.EventType)
	}
	if log.EntityID != entityID {
		t.Fatalf("log entity_id = %q, want %q", log.EntityID, entityID)
	}
	if log.SessionID != "session-1" {
		t.Fatalf("log session_id = %q, want session-1", log.SessionID)
	}
	if log.ErrorCode != "cross_flow_write_forbidden" {
		t.Fatalf("log error_code = %q, want cross_flow_write_forbidden", log.ErrorCode)
	}
	if log.Message != "Tool execution was denied for save_entity_field" {
		t.Fatalf("log message = %q", log.Message)
	}
	detail, _ := log.Detail.(map[string]any)
	if strings.TrimSpace(readString(detail["tool_name"])) != "save_entity_field" {
		t.Fatalf("log detail.tool_name = %#v, want save_entity_field", detail["tool_name"])
	}
}

func TestAccumulatorCompletionOutcomeSurface_RoundTripsThroughObservabilityReader(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	runID := uuid.NewString()
	ctx := runtimecorrelation.WithRunID(context.Background(), runID)
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "false")
	pg := &store.PostgresStore{DB: db}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status)
		VALUES ($1::uuid, 'running')
	`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}

	requireCanonicalRuntimeLogSurface(t, ctx, pg)

	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier2-accumulation", "test-accumulate-on-complete-rollback")
	module := loadConformanceWorkflowFixtureModule(t, fixtureRoot)

	entityID := "11111111-1111-4111-8111-111111111111"
	workflowStore := runtimepipeline.NewWorkflowInstanceStore(db)
	if err := workflowStore.Upsert(ctx, runtimepipeline.WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      entityID,
		WorkflowName:    module.SemanticSource().WorkflowName(),
		WorkflowVersion: module.SemanticSource().WorkflowVersion(),
		CurrentState:    "collecting",
		Metadata: map[string]any{
			"expected_count": 3,
		},
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}

	rt, err := runtimepkg.NewRuntime(ctx, runtimepkg.RuntimeDeps{Config: &config.Config{
		Runtime: config.RuntimeConfig{},
		LLM:     config.LLMConfig{RuntimeMode: "api"},
	}, Stores: runtimepkg.Stores{
		SQLDB:         db,
		EventStore:    pg,
		ManagerStore:  pg,
		ScheduleStore: pg,
	}, Options: runtimepkg.RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: module,
		LLMRuntime:     conformanceNoopLLMRuntime{},
	}})

	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	if err := rt.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() {
		if err := rt.Shutdown(); err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	}()

	for idx, score := range []int{80, 90, 70} {
		payload, err := json.Marshal(map[string]any{"entity_id": entityID, "score": score})
		if err != nil {
			t.Fatalf("marshal payload: %v", err)
		}
		if err := rt.Bus.Publish(ctx, (events.Event{
			ID:        uuid.NewString(),
			Type:      events.EventType("score.received"),
			Payload:   payload,
			CreatedAt: time.Now().UTC().Add(time.Duration(idx) * time.Millisecond),
		}).WithEntityID(entityID)); err != nil {
			t.Fatalf("Publish(%d): %v", idx, err)
		}
	}
	if err := rt.Bus.WaitForQuiescence(ctx); err != nil {
		t.Fatalf("WaitForQuiescence: %v", err)
	}

	instance, ok, err := workflowStore.Load(ctx, entityID)
	if err != nil {
		t.Fatalf("Load workflow instance: %v", err)
	}
	if !ok {
		t.Fatal("expected workflow instance to persist")
	}
	if got := strings.TrimSpace(instance.CurrentState); got != "verified" {
		t.Fatalf("current_state = %q, want verified", got)
	}

	reader := dashboardserver.NewSQLObservabilityReader(db, pg)
	if reader == nil {
		t.Fatal("NewSQLObservabilityReader returned nil")
	}
	logs, err := reader.ListRuntimeLogs(ctx, dashboardserver.RuntimeLogFilter{
		Component: "workflow-runtime",
		Type:      "accumulator_completion_outcome",
	}, 10)
	if err != nil {
		t.Fatalf("ListRuntimeLogs: %v", err)
	}
	if len(logs) != 1 {
		t.Fatalf("runtime log rows = %d, want 1: %#v", len(logs), logs)
	}
	log := logs[0]
	if log.Action != "accumulator_completion_outcome" {
		t.Fatalf("log action = %q, want accumulator_completion_outcome", log.Action)
	}
	detail, _ := log.Detail.(map[string]any)
	if got := readString(detail["decision_outcome"]); got != "completed" {
		t.Fatalf("detail.decision_outcome = %q, want completed", got)
	}
	if got := readString(detail["decision_reason_code"]); got != "completion_committed" {
		t.Fatalf("detail.decision_reason_code = %q, want completion_committed", got)
	}
	if got := readString(detail["evaluation_outcome"]); got != "succeeded" {
		t.Fatalf("detail.evaluation_outcome = %q, want succeeded", got)
	}
	if got := readString(detail["commit_outcome"]); got != "committed" {
		t.Fatalf("detail.commit_outcome = %q, want committed", got)
	}
	if got := readString(detail["selected_rule_id"]); got != "" {
		t.Fatalf("detail.selected_rule_id = %q, want empty for anonymous true branch", got)
	}
	if got, _ := detail["received_count"].(float64); int(got) != 3 {
		t.Fatalf("detail.received_count = %v, want 3", detail["received_count"])
	}
	if got, _ := detail["expected_count"].(float64); int(got) != 3 {
		t.Fatalf("detail.expected_count = %v, want 3", detail["expected_count"])
	}
}

type conformanceSemanticOnlyWorkflowRuntime struct {
	source runtimesemanticview.Source
}

func (s conformanceSemanticOnlyWorkflowRuntime) SemanticSource() runtimesemanticview.Source {
	return s.source
}

func (conformanceSemanticOnlyWorkflowRuntime) WorkflowDefinition() *runtimepipeline.WorkflowDefinition {
	return nil
}

func (conformanceSemanticOnlyWorkflowRuntime) WorkflowNodes() []runtimepipeline.WorkflowNode {
	return nil
}
func (conformanceSemanticOnlyWorkflowRuntime) GuardRegistry() runtimepipeline.GuardRegistry {
	return nil
}
func (conformanceSemanticOnlyWorkflowRuntime) ActionRegistry() runtimepipeline.ActionRegistry {
	return nil
}

type conformanceLoadedWorkflowModule struct {
	source   runtimesemanticview.Source
	workflow *runtimepipeline.WorkflowDefinition
	nodes    []runtimepipeline.WorkflowNode
	guards   runtimepipeline.GuardRegistry
	actions  runtimepipeline.ActionRegistry
}

func (m conformanceLoadedWorkflowModule) SemanticSource() runtimesemanticview.Source {
	return m.source
}

func (m conformanceLoadedWorkflowModule) WorkflowDefinition() *runtimepipeline.WorkflowDefinition {
	return m.workflow
}

func (m conformanceLoadedWorkflowModule) WorkflowNodes() []runtimepipeline.WorkflowNode {
	return append([]runtimepipeline.WorkflowNode(nil), m.nodes...)
}

func (m conformanceLoadedWorkflowModule) GuardRegistry() runtimepipeline.GuardRegistry {
	return m.guards
}

func (m conformanceLoadedWorkflowModule) ActionRegistry() runtimepipeline.ActionRegistry {
	return m.actions
}

type conformanceNoopLLMRuntime struct{}

func (conformanceNoopLLMRuntime) StartSession(context.Context, string, string, []runtimellm.ToolDefinition) (*runtimellm.Session, error) {
	return &runtimellm.Session{}, nil
}

func (conformanceNoopLLMRuntime) ContinueSession(context.Context, *runtimellm.Session, runtimellm.Message) (*runtimellm.Response, error) {
	return &runtimellm.Response{}, nil
}

type conformanceScheduleClaim struct {
	claimed bool
	err     error
}

type conformanceTimerRecoveryScheduleStore struct {
	active []runtimepipeline.Schedule
	claims []conformanceScheduleClaim
}

func (*conformanceTimerRecoveryScheduleStore) UpsertSchedule(context.Context, runtimepipeline.Schedule) error {
	return nil
}

func (*conformanceTimerRecoveryScheduleStore) CancelScheduleExact(context.Context, runtimepipeline.Schedule) error {
	return nil
}

func (*conformanceTimerRecoveryScheduleStore) CancelScheduleExactTerminal(context.Context, runtimepipeline.Schedule) error {
	return nil
}

func (s *conformanceTimerRecoveryScheduleStore) LoadActiveSchedules(context.Context) ([]runtimepipeline.Schedule, error) {
	return append([]runtimepipeline.Schedule(nil), s.active...), nil
}

func (s *conformanceTimerRecoveryScheduleStore) ClaimSchedule(context.Context, runtimepipeline.Schedule) (bool, error) {
	if len(s.claims) == 0 {
		return true, nil
	}
	claim := s.claims[0]
	s.claims = s.claims[1:]
	return claim.claimed, claim.err
}

func (*conformanceTimerRecoveryScheduleStore) ReleaseSchedule(context.Context, runtimepipeline.Schedule) error {
	return nil
}

func (*conformanceTimerRecoveryScheduleStore) ReleaseScheduleClaims(context.Context) error {
	return nil
}

func (*conformanceTimerRecoveryScheduleStore) MarkScheduleFiredExact(context.Context, runtimepipeline.Schedule) error {
	return nil
}

func (*conformanceTimerRecoveryScheduleStore) CompleteScheduleFireExact(context.Context, runtimepipeline.Schedule) error {
	return nil
}

type conformanceTimerRecoveryEventStore struct{}

func (conformanceTimerRecoveryEventStore) CanonicalRuntimeLogCapability(context.Context) (bool, bool, error) {
	return true, true, nil
}

func (conformanceTimerRecoveryEventStore) AppendEvent(context.Context, events.Event) error {
	return nil
}

func (conformanceTimerRecoveryEventStore) InsertEventDeliveries(context.Context, string, []string) error {
	return nil
}

func (conformanceTimerRecoveryEventStore) ListEventDeliveryRecipients(context.Context, string) ([]string, error) {
	return nil, nil
}

func (conformanceTimerRecoveryEventStore) SupportsPersistedReplay() bool { return false }

type conformanceRecoveryFailureEventStore struct {
	store    *store.PostgresStore
	missing  []events.PersistedReplayEvent
	claimErr error
}

func (s conformanceRecoveryFailureEventStore) CanonicalRuntimeLogCapability(context.Context) (bool, bool, error) {
	return true, true, nil
}

func (s conformanceRecoveryFailureEventStore) AppendEvent(ctx context.Context, evt events.Event) error {
	return s.store.AppendEvent(ctx, evt)
}

func (s conformanceRecoveryFailureEventStore) InsertEventDeliveries(ctx context.Context, eventID string, recipients []string) error {
	return s.store.InsertEventDeliveries(ctx, eventID, recipients)
}

func (s conformanceRecoveryFailureEventStore) ListEventDeliveryRecipients(ctx context.Context, eventID string) ([]string, error) {
	return s.store.ListEventDeliveryRecipients(ctx, eventID)
}

func (conformanceRecoveryFailureEventStore) UpsertCommittedReplayScope(context.Context, string, runtimereplayclaim.CommittedReplayScope) error {
	return nil
}

func (s conformanceRecoveryFailureEventStore) ListEventsMissingPipelineReceipt(context.Context, time.Time, int) ([]events.PersistedReplayEvent, error) {
	return append([]events.PersistedReplayEvent(nil), s.missing...), nil
}

func (s conformanceRecoveryFailureEventStore) ClaimPipelineReplay(context.Context, string) (runtimeownership.Lease, bool, error) {
	if s.claimErr != nil {
		return nil, false, s.claimErr
	}
	return conformanceRecoveryReplayLease{}, true, nil
}

func (conformanceRecoveryFailureEventStore) SupportsPersistedReplay() bool { return true }

type conformanceManagerReplayStore struct {
	agents   []runtimemanager.PersistedAgent
	pending  map[string][]events.Event
	receipts map[string]runtimemanager.EventReceipt
}

func (*conformanceManagerReplayStore) UpsertAgent(context.Context, runtimemanager.PersistedAgent) error {
	return nil
}

func (s *conformanceManagerReplayStore) LoadAgents(context.Context) ([]runtimemanager.PersistedAgent, error) {
	return append([]runtimemanager.PersistedAgent(nil), s.agents...), nil
}

func (*conformanceManagerReplayStore) MarkAgentTerminated(context.Context, string) error { return nil }
func (*conformanceManagerReplayStore) EnsureEntitySchema(context.Context, string) error  { return nil }
func (s *conformanceManagerReplayStore) UpsertEventReceipt(_ context.Context, eventID, agentID string, status runtimemanager.ReceiptStatus, errText string) error {
	if s.receipts == nil {
		s.receipts = map[string]runtimemanager.EventReceipt{}
	}
	s.receipts[strings.TrimSpace(eventID)+"|"+strings.TrimSpace(agentID)] = runtimemanager.EventReceipt{
		EventID: eventID,
		AgentID: agentID,
		Status:  status,
		Error:   errText,
	}
	return nil
}
func (s *conformanceManagerReplayStore) ListPendingEventsForAgent(_ context.Context, agentID string, _ time.Time, _ int) ([]events.Event, error) {
	return append([]events.Event(nil), s.pending[strings.TrimSpace(agentID)]...), nil
}
func (*conformanceManagerReplayStore) ListPendingSubscribedEvents(context.Context, string, []events.EventType, time.Time, int) ([]events.Event, error) {
	return nil, nil
}
func (s *conformanceManagerReplayStore) GetEventReceipt(_ context.Context, eventID, agentID string) (runtimemanager.EventReceipt, bool, error) {
	receipt, ok := s.receipts[strings.TrimSpace(eventID)+"|"+strings.TrimSpace(agentID)]
	return receipt, ok, nil
}

type conformanceManagerReplayAgent struct{ id string }

func (a conformanceManagerReplayAgent) ID() string                      { return a.id }
func (conformanceManagerReplayAgent) Type() string                      { return "generic" }
func (conformanceManagerReplayAgent) Subscriptions() []events.EventType { return nil }
func (conformanceManagerReplayAgent) OnEvent(_ context.Context, evt events.Event) ([]events.Event, error) {
	switch evt.Type {
	case events.EventType("support.replay.drop"):
		return nil, fmt.Errorf("boom")
	case events.EventType("support.replay.leased"):
		return nil, fmt.Errorf("session currently leased")
	default:
		return nil, nil
	}
}

func loadConformanceRuntimeWorkflowModule(t *testing.T) conformanceSemanticOnlyWorkflowRuntime {
	t.Helper()
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier8-boot-verification", "test-boot-success")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("load bundle: %v", err)
	}
	return conformanceSemanticOnlyWorkflowRuntime{source: runtimesemanticview.Wrap(bundle)}
}

func loadConformanceWorkflowFixtureModule(t *testing.T, fixtureRoot string) conformanceLoadedWorkflowModule {
	t.Helper()
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("load bundle: %v", err)
	}
	source := runtimesemanticview.Wrap(bundle)
	workflow, err := runtimepipeline.LoadWorkflowDefinition(source)
	if err != nil {
		t.Fatalf("LoadWorkflowDefinition: %v", err)
	}
	nodes, err := runtimepipeline.LoadWorkflowNodes(source)
	if err != nil {
		t.Fatalf("LoadWorkflowNodes: %v", err)
	}
	return conformanceLoadedWorkflowModule{
		source:   source,
		workflow: workflow,
		nodes:    nodes,
		guards:   runtimepipeline.NewContractGuardRegistry(source),
		actions:  runtimepipeline.NewContractActionRegistry(source),
	}
}

func TestStartupRecoveryDecisionSurface_RoundTripsThroughObservabilityReader(t *testing.T) {
	ctx := context.Background()
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	pg := &store.PostgresStore{DB: db}
	requireCanonicalRuntimeLogSurface(t, ctx, pg)

	if err := pg.UpsertSchedule(ctx, runtimepipeline.Schedule{
		AgentID:   "runtime",
		EventType: "timer.check",
		Mode:      "once",
		At:        time.Now().Add(time.Minute),
		TaskID:    "recover-me",
	}); err != nil {
		t.Fatalf("UpsertSchedule: %v", err)
	}

	rt, err := runtimepkg.NewRuntime(ctx, runtimepkg.RuntimeDeps{Config: &config.Config{
		Runtime: config.RuntimeConfig{
			RecoveryOnStartup: false,
		},
		LLM: config.LLMConfig{
			RuntimeMode: "api",
		},
	}, Stores: runtimepkg.Stores{
		SQLDB:         db,
		EventStore:    pg,
		ManagerStore:  pg,
		ScheduleStore: pg,
	}, Options: runtimepkg.RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: loadConformanceRuntimeWorkflowModule(t),
		LLMRuntime:     conformanceNoopLLMRuntime{},
	}})

	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}

	startErr := rt.Start(ctx)
	if startErr == nil || !strings.Contains(startErr.Error(), "runtime.recovery_on_startup=false") {
		t.Fatalf("Start error = %v, want explicit recovery denial", startErr)
	}

	reader := dashboardserver.NewSQLObservabilityReader(db, pg)
	if reader == nil {
		t.Fatal("NewSQLObservabilityReader returned nil")
	}
	logs, err := reader.ListRuntimeLogs(ctx, dashboardserver.RuntimeLogFilter{
		Component: "runtime",
		Type:      "startup_recovery_decision",
	}, 10)
	if err != nil {
		t.Fatalf("ListRuntimeLogs: %v", err)
	}
	if len(logs) != 1 {
		t.Fatalf("runtime log rows = %d, want 1: %#v", len(logs), logs)
	}
	log := logs[0]
	if log.Level != "warn" {
		t.Fatalf("log level = %q, want warn", log.Level)
	}
	if log.Action != "startup_recovery_decision" {
		t.Fatalf("log action = %q, want startup_recovery_decision", log.Action)
	}
	if log.ErrorCode != "recovery_disabled_with_persisted_work" {
		t.Fatalf("log error_code = %q, want recovery_disabled_with_persisted_work", log.ErrorCode)
	}
	detail, _ := log.Detail.(map[string]any)
	if got := readString(detail["decision_outcome"]); got != "denied" {
		t.Fatalf("detail.decision_outcome = %q, want denied", got)
	}
	if got := readString(detail["decision_reason_code"]); got != "recovery_disabled_with_persisted_work" {
		t.Fatalf("detail.decision_reason_code = %q, want recovery_disabled_with_persisted_work", got)
	}
	if got, _ := detail["active_schedule_count"].(float64); int(got) != 1 {
		t.Fatalf("detail.active_schedule_count = %v, want 1", detail["active_schedule_count"])
	}
	classes, _ := detail["recoverable_work_classes"].([]any)
	if len(classes) != 1 || readString(classes[0]) != "active schedules" {
		t.Fatalf("detail.recoverable_work_classes = %#v, want [active schedules]", detail["recoverable_work_classes"])
	}
}

func TestStartupRecoveryFailurePlatformEventSurface_PreservesRecoveryFailedWithoutPlatformReset(t *testing.T) {
	ctx := context.Background()
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	pg := &store.PostgresStore{DB: db}

	requireCanonicalRuntimeLogSurface(t, ctx, pg)
	module := loadConformanceRuntimeWorkflowModule(t)
	eventStore := conformanceRecoveryFailureEventStore{
		store: pg,
		missing: []events.PersistedReplayEvent{{
			Event: events.Event{
				ID:        uuid.NewString(),
				Type:      events.EventType("support.item_created"),
				CreatedAt: time.Now().Add(-time.Minute).UTC(),
			},
		}},
		claimErr: errors.New("claim failed"),
	}

	rt, err := runtimepkg.NewRuntime(ctx, runtimepkg.RuntimeDeps{Config: &config.Config{
		Runtime: config.RuntimeConfig{
			RecoveryOnStartup: true,
		},
		LLM: config.LLMConfig{
			RuntimeMode: "api",
		},
	}, Stores: runtimepkg.Stores{
		SQLDB:        db,
		EventStore:   eventStore,
		ManagerStore: &conformanceManagerReplayStore{},
	}, Options: runtimepkg.RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: module,
		LLMRuntime:     conformanceNoopLLMRuntime{},
	}})

	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	if err := rt.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() {
		if err := rt.Shutdown(); err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	}()

	var resetCount int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM events
		WHERE event_name = 'platform.reset'
	`).Scan(&resetCount); err != nil {
		t.Fatalf("count platform.reset events: %v", err)
	}
	if resetCount != 0 {
		t.Fatalf("platform.reset event count = %d, want 0", resetCount)
	}

	var recoveryFailedPayloadRaw []byte
	if err := db.QueryRowContext(ctx, `
		SELECT payload
		FROM events
		WHERE event_name = 'platform.recovery_failed'
		ORDER BY created_at DESC
		LIMIT 1
	`).Scan(&recoveryFailedPayloadRaw); err != nil {
		t.Fatalf("load platform.recovery_failed event: %v", err)
	}
	var recoveryFailedPayload map[string]any
	if err := json.Unmarshal(recoveryFailedPayloadRaw, &recoveryFailedPayload); err != nil {
		t.Fatalf("unmarshal platform.recovery_failed payload: %v", err)
	}
	if got := readString(recoveryFailedPayload["error"]); !strings.Contains(got, "claim replay event") || !strings.Contains(got, "claim failed") {
		t.Fatalf("platform.recovery_failed payload.error = %q, want replay claim failure", got)
	}
}

func TestStartupTimerRecoveryAftermathSurface_RoundTripsThroughObservabilityReader(t *testing.T) {
	ctx := context.Background()
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	pg := &store.PostgresStore{DB: db}

	requireCanonicalRuntimeLogSurface(t, ctx, pg)

	scheduleStore := &conformanceTimerRecoveryScheduleStore{
		active: []runtimepipeline.Schedule{
			{
				AgentID:   "runtime",
				EventType: "timer.replay",
				Mode:      "once",
				At:        time.Now().Add(time.Minute),
				TaskID:    "replay-me",
			},
			{
				AgentID:   "runtime",
				EventType: "timer.skip",
				Mode:      "once",
				At:        time.Now().Add(2 * time.Minute),
				TaskID:    "skip-me",
			},
			{
				AgentID:   "runtime",
				EventType: "timer.drop",
				Mode:      "once",
				At:        time.Now().Add(3 * time.Minute),
				TaskID:    "drop-me",
			},
		},
		claims: []conformanceScheduleClaim{
			{claimed: true},
			{claimed: false},
			{err: fmt.Errorf("claim failed")},
		},
	}

	rt, err := runtimepkg.NewRuntime(ctx, runtimepkg.RuntimeDeps{Config: &config.Config{
		Runtime: config.RuntimeConfig{
			RecoveryOnStartup: true,
		},
		LLM: config.LLMConfig{
			RuntimeMode: "api",
		},
	}, Stores: runtimepkg.Stores{
		SQLDB:         db,
		EventStore:    conformanceTimerRecoveryEventStore{},
		ManagerStore:  pg,
		ScheduleStore: scheduleStore,
	}, Options: runtimepkg.RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: loadConformanceRuntimeWorkflowModule(t),
		LLMRuntime:     conformanceNoopLLMRuntime{},
	}})

	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	if err := rt.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() {
		if err := rt.Shutdown(); err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	}()

	reader := dashboardserver.NewSQLObservabilityReader(db, pg)
	if reader == nil {
		t.Fatal("NewSQLObservabilityReader returned nil")
	}
	logs, err := reader.ListRuntimeLogs(ctx, dashboardserver.RuntimeLogFilter{
		Component: "scheduler",
		Type:      "startup_recovery_timer_aftermath",
	}, 10)
	if err != nil {
		t.Fatalf("ListRuntimeLogs: %v", err)
	}
	if len(logs) != 3 {
		t.Fatalf("runtime log rows = %d, want 3: %#v", len(logs), logs)
	}
	findByEventType := func(eventType string) int {
		t.Helper()
		for idx, log := range logs {
			if strings.TrimSpace(log.EventType) == eventType {
				return idx
			}
		}
		t.Fatalf("missing runtime log for event type %q in %#v", eventType, logs)
		return -1
	}

	replayed := logs[findByEventType("timer.replay")]
	if replayed.Action != "startup_recovery_timer_aftermath" {
		t.Fatalf("replayed action = %q, want startup_recovery_timer_aftermath", replayed.Action)
	}
	replayedDetail, _ := replayed.Detail.(map[string]any)
	if got := readString(replayedDetail["decision_outcome"]); got != "replayed" {
		t.Fatalf("replayed detail.decision_outcome = %q, want replayed", got)
	}
	if got := readString(replayedDetail["decision_reason_code"]); got != "persisted_schedule_restored" {
		t.Fatalf("replayed detail.decision_reason_code = %q, want persisted_schedule_restored", got)
	}

	skipped := logs[findByEventType("timer.skip")]
	skippedDetail, _ := skipped.Detail.(map[string]any)
	if got := readString(skippedDetail["decision_outcome"]); got != "skipped" {
		t.Fatalf("skipped detail.decision_outcome = %q, want skipped", got)
	}
	if got := readString(skippedDetail["decision_reason_code"]); got != "schedule_claim_not_acquired" {
		t.Fatalf("skipped detail.decision_reason_code = %q, want schedule_claim_not_acquired", got)
	}

	dropped := logs[findByEventType("timer.drop")]
	droppedDetail, _ := dropped.Detail.(map[string]any)
	if got := readString(droppedDetail["decision_outcome"]); got != "dropped" {
		t.Fatalf("dropped detail.decision_outcome = %q, want dropped", got)
	}
	if got := readString(droppedDetail["decision_reason_code"]); got != "schedule_restore_failed" {
		t.Fatalf("dropped detail.decision_reason_code = %q, want schedule_restore_failed", got)
	}
	if readString(droppedDetail["error_code"]) != "schedule_restore_failed" {
		t.Fatalf("dropped detail.error_code = %q, want schedule_restore_failed", readString(droppedDetail["error_code"]))
	}
	if strings.TrimSpace(dropped.Error) == "" {
		t.Fatal("dropped timer recovery log missing explicit error text")
	}
}

type conformanceRuntimeLoggerHook struct {
	logger *runtimepkg.RuntimeLogger
}

func (h conformanceRuntimeLoggerHook) Log(ctx context.Context, level runtimediaglog.Level, message, component, action, eventID, eventType, agentID, entityID, sessionID string, correlation map[string]string, detail any, errText string, durationUS int) error {
	if h.logger == nil {
		return nil
	}
	return h.logger.Log(ctx, runtimepkg.RuntimeLogEntry{
		Level:       level,
		Message:     message,
		Component:   component,
		Action:      action,
		EventID:     eventID,
		EventType:   eventType,
		AgentID:     agentID,
		EntityID:    entityID,
		SessionID:   sessionID,
		Correlation: correlation,
		Detail:      detail,
		Error:       errText,
		DurationUS:  durationUS,
	})
}

func TestResetOrphanedSessionAftermathSurface_RoundTripsThroughObservabilityReader(t *testing.T) {
	ctx := context.Background()
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	pg := &store.PostgresStore{DB: db}

	requireCanonicalRuntimeLogSurface(t, ctx, pg)
	seedConformanceAgent(t, ctx, pg, "agent-1")

	logger := runtimepkg.NewRuntimeLogger(db, pg)
	bus, err := runtimebus.NewEventBusWithOptions(pg, runtimebus.EventBusOptions{
		Logger: conformanceRuntimeLoggerHook{logger: logger},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	registry := runtimesessions.NewPostgresRegistry(db, 30*time.Second)
	lease, err := registry.Acquire(ctx, "agent-1", runtimesessions.RuntimeModeSession, runtimesessions.SessionScopeGlobal, "conformance", "global")
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	am := runtimemanager.NewAgentManagerWithOptions(bus, nil, runtimemanager.AgentManagerOptions{
		RuntimeMode: runtimesessions.RuntimeModeSession.String(),
		Sessions:    registry,
	})
	if err := am.ResetRuntimeStateWithSource("admin_cli"); err != nil {
		t.Fatalf("ResetRuntimeStateWithSource: %v", err)
	}

	reader := dashboardserver.NewSQLObservabilityReader(db, pg)
	if reader == nil {
		t.Fatal("NewSQLObservabilityReader returned nil")
	}
	logs, err := reader.ListRuntimeLogs(ctx, dashboardserver.RuntimeLogFilter{
		Component: "runtime",
		Type:      "reset_orphaned_sessions",
	}, 10)
	if err != nil {
		t.Fatalf("ListRuntimeLogs: %v", err)
	}
	if len(logs) != 1 {
		t.Fatalf("runtime log rows = %d, want 1: %#v", len(logs), logs)
	}
	log := logs[0]
	if log.Level != "warn" {
		t.Fatalf("log level = %q, want warn", log.Level)
	}
	if log.Action != "reset_orphaned_sessions" {
		t.Fatalf("log action = %q, want reset_orphaned_sessions", log.Action)
	}
	detail, _ := log.Detail.(map[string]any)
	if got := readString(detail["source"]); got != "admin_cli" {
		t.Fatalf("detail.source = %q, want admin_cli", got)
	}
	if got, _ := detail["orphaned_session_count"].(float64); int(got) != 1 {
		t.Fatalf("detail.orphaned_session_count = %v, want 1", detail["orphaned_session_count"])
	}
	orphanedSessions, _ := detail["orphaned_sessions"].([]any)
	if len(orphanedSessions) != 1 {
		t.Fatalf("detail.orphaned_sessions = %#v, want one row", detail["orphaned_sessions"])
	}
	sessionRow, ok := orphanedSessions[0].(map[string]any)
	if !ok {
		t.Fatalf("detail.orphaned_sessions[0] = %#v, want object", orphanedSessions[0])
	}
	if got := readString(sessionRow["session_id"]); got != lease.SessionID {
		t.Fatalf("detail.orphaned_sessions[0].session_id = %q, want %q", got, lease.SessionID)
	}
	if got := readString(sessionRow["agent_id"]); got != "agent-1" {
		t.Fatalf("detail.orphaned_sessions[0].agent_id = %q, want agent-1", got)
	}
	if got := readString(sessionRow["termination_reason"]); got != "orphaned" {
		t.Fatalf("detail.orphaned_sessions[0].termination_reason = %q, want orphaned", got)
	}
	if got := readString(sessionRow["termination_detail"]); got != "admin_cli" {
		t.Fatalf("detail.orphaned_sessions[0].termination_detail = %q, want admin_cli", got)
	}

	var (
		status            string
		terminationReason string
		terminationDetail string
	)
	if err := db.QueryRowContext(ctx, `
		SELECT
			COALESCE(status, ''),
			COALESCE(termination_reason, ''),
			COALESCE(termination_detail, '')
		FROM agent_sessions
		WHERE session_id = $1::uuid
	`, lease.SessionID).Scan(&status, &terminationReason, &terminationDetail); err != nil {
		t.Fatalf("load reset session row: %v", err)
	}
	if status != "terminated" {
		t.Fatalf("status = %q, want terminated", status)
	}
	if terminationReason != "orphaned" {
		t.Fatalf("termination_reason = %q, want orphaned", terminationReason)
	}
	if terminationDetail != "admin_cli" {
		t.Fatalf("termination_detail = %q, want admin_cli", terminationDetail)
	}

	var resetPayloadRaw []byte
	if err := db.QueryRowContext(ctx, `
		SELECT payload
		FROM events
		WHERE event_name = 'platform.reset'
		ORDER BY created_at DESC
		LIMIT 1
	`).Scan(&resetPayloadRaw); err != nil {
		t.Fatalf("load platform.reset event: %v", err)
	}
	var resetPayload map[string]any
	if err := json.Unmarshal(resetPayloadRaw, &resetPayload); err != nil {
		t.Fatalf("unmarshal platform.reset payload: %v", err)
	}
	if got := readString(resetPayload["source"]); got != "admin_cli" {
		t.Fatalf("platform.reset payload.source = %q, want admin_cli", got)
	}
}

func TestStartupManagerReplayAftermathSurface_RoundTripsThroughObservabilityReader(t *testing.T) {
	ctx := context.Background()
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	pg := &store.PostgresStore{DB: db}

	requireCanonicalRuntimeLogSurface(t, ctx, pg)
	module := loadConformanceRuntimeWorkflowModule(t)
	managerStore := &conformanceManagerReplayStore{
		agents: []runtimemanager.PersistedAgent{{
			Config:    runtimeactors.AgentConfig{ID: "agent-a"},
			StartedAt: time.Now().UTC(),
		}},
		pending: map[string][]events.Event{
			"agent-a": {
				{ID: "evt-replay", Type: events.EventType("support.replay.ok"), CreatedAt: time.Now().Add(-4 * time.Minute).UTC()},
				{ID: "evt-skip", Type: events.EventType("support.replay.skip"), CreatedAt: time.Now().Add(-3 * time.Minute).UTC()},
				{ID: "evt-leased", Type: events.EventType("support.replay.leased"), CreatedAt: time.Now().Add(-2 * time.Minute).UTC()},
				{ID: "evt-drop", Type: events.EventType("support.replay.drop"), CreatedAt: time.Now().Add(-time.Minute).UTC()},
			},
		},
		receipts: map[string]runtimemanager.EventReceipt{
			"evt-skip|agent-a": {
				EventID: "evt-skip",
				AgentID: "agent-a",
				Status:  runtimemanager.ReceiptStatusProcessed,
			},
		},
	}

	rt, err := runtimepkg.NewRuntime(ctx, runtimepkg.RuntimeDeps{Config: &config.Config{
		Runtime: config.RuntimeConfig{
			RecoveryOnStartup: true,
		},
		LLM: config.LLMConfig{
			RuntimeMode: "api",
		},
	}, Stores: runtimepkg.Stores{
		SQLDB:        db,
		EventStore:   conformanceTimerRecoveryEventStore{},
		ManagerStore: managerStore,
	}, Options: runtimepkg.RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: module,
		LLMRuntime:     conformanceNoopLLMRuntime{},
	}})

	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	rt.Manager = runtimemanager.NewAgentManager(rt.Bus, func(cfg runtimeactors.AgentConfig) (runtimemanager.Agent, error) {
		return conformanceManagerReplayAgent{id: cfg.ID}, nil
	}, managerStore)

	if err := rt.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() {
		if err := rt.Shutdown(); err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	}()

	reader := dashboardserver.NewSQLObservabilityReader(db, pg)
	if reader == nil {
		t.Fatal("NewSQLObservabilityReader returned nil")
	}
	logs, err := reader.ListRuntimeLogs(ctx, dashboardserver.RuntimeLogFilter{
		Component: "agent-manager",
		Type:      "startup_recovery_manager_replay_aftermath",
	}, 10)
	if err != nil {
		t.Fatalf("ListRuntimeLogs: %v", err)
	}
	if len(logs) != 4 {
		t.Fatalf("runtime log rows = %d, want 4: %#v", len(logs), logs)
	}
	findByEventID := func(eventID string) int {
		t.Helper()
		for idx, log := range logs {
			if strings.TrimSpace(log.EventID) == strings.TrimSpace(eventID) {
				return idx
			}
		}
		t.Fatalf("missing runtime log for event %q in %#v", eventID, logs)
		return -1
	}

	replayed := logs[findByEventID("evt-replay")]
	replayedDetail, _ := replayed.Detail.(map[string]any)
	if got := readString(replayedDetail["decision_outcome"]); got != "replayed" {
		t.Fatalf("replayed detail.decision_outcome = %q, want replayed", got)
	}
	if got := readString(replayedDetail["decision_reason_code"]); got != "persisted_event_replayed" {
		t.Fatalf("replayed detail.decision_reason_code = %q, want persisted_event_replayed", got)
	}

	skippedReceipt := logs[findByEventID("evt-skip")]
	skippedReceiptDetail, _ := skippedReceipt.Detail.(map[string]any)
	if got := readString(skippedReceiptDetail["decision_outcome"]); got != "skipped" {
		t.Fatalf("receipt skip detail.decision_outcome = %q, want skipped", got)
	}
	if got := readString(skippedReceiptDetail["decision_reason_code"]); got != "event_receipt_already_processed" {
		t.Fatalf("receipt skip detail.decision_reason_code = %q, want event_receipt_already_processed", got)
	}

	skippedLeased := logs[findByEventID("evt-leased")]
	skippedLeasedDetail, _ := skippedLeased.Detail.(map[string]any)
	if got := readString(skippedLeasedDetail["decision_reason_code"]); got != "session_currently_leased" {
		t.Fatalf("leased skip detail.decision_reason_code = %q, want session_currently_leased", got)
	}

	dropped := logs[findByEventID("evt-drop")]
	droppedDetail, _ := dropped.Detail.(map[string]any)
	if got := readString(droppedDetail["decision_outcome"]); got != "dropped" {
		t.Fatalf("dropped detail.decision_outcome = %q, want dropped", got)
	}
	if got := readString(droppedDetail["decision_reason_code"]); got != "event_processing_failed" {
		t.Fatalf("dropped detail.decision_reason_code = %q, want event_processing_failed", got)
	}
	if readString(droppedDetail["error_code"]) != "event_processing_failed" {
		t.Fatalf("dropped detail.error_code = %q, want event_processing_failed", readString(droppedDetail["error_code"]))
	}
	if strings.TrimSpace(dropped.Error) == "" {
		t.Fatal("dropped startup manager replay log missing explicit error text")
	}
}

func TestStartupPipelineReplayAftermathSurface_RoundTripsThroughObservabilityReader(t *testing.T) {
	ctx := context.Background()
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	pg := &store.PostgresStore{DB: db}

	requireCanonicalRuntimeLogSurface(t, ctx, pg)

	logger := runtimepkg.NewRuntimeLogger(db, pg)
	bus, err := runtimebus.NewEventBusWithOptions(pg, runtimebus.EventBusOptions{
		Logger: conformanceRuntimeLoggerHook{logger: logger},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	replayRecipient := "agent-replay"
	_ = bus.Subscribe(replayRecipient)

	replayRunID := uuid.NewString()
	replayParentID := uuid.NewString()
	replayChildID := uuid.NewString()
	if err := pg.AppendEvent(ctx, events.Event{
		ID:          replayParentID,
		Type:        events.EventType("system.parent"),
		SourceAgent: "runtime",
		Payload:     []byte(`{"ok":true}`),
		RunID:       replayRunID,
		CreatedAt:   time.Now().Add(-3 * time.Minute).UTC(),
	}); err != nil {
		t.Fatalf("AppendEvent(replay parent): %v", err)
	}
	if err := pg.AppendEvent(ctx, events.Event{
		ID:            replayChildID,
		Type:          events.EventType("system.recover.replay"),
		SourceAgent:   "runtime",
		Payload:       []byte(`{"ok":true}`),
		RunID:         replayRunID,
		ParentEventID: replayParentID,
		CreatedAt:     time.Now().Add(-2 * time.Minute).UTC(),
	}); err != nil {
		t.Fatalf("AppendEvent(replay child): %v", err)
	}
	if err := pg.InsertEventDeliveries(ctx, replayChildID, []string{replayRecipient}); err != nil {
		t.Fatalf("InsertEventDeliveries(replay child): %v", err)
	}
	if err := pg.UpsertCommittedReplayScope(ctx, replayChildID, runtimereplayclaim.CommittedReplayScopeSubscribed); err != nil {
		t.Fatalf("UpsertCommittedReplayScope(replay child): %v", err)
	}
	if err := pg.UpsertPipelineReceipt(ctx, replayParentID, "processed", ""); err != nil {
		t.Fatalf("UpsertPipelineReceipt(replay parent): %v", err)
	}

	skipRunID := uuid.NewString()
	skipParentID := uuid.NewString()
	skipChildID := uuid.NewString()
	if err := pg.AppendEvent(ctx, events.Event{
		ID:          skipParentID,
		Type:        events.EventType("system.parent"),
		SourceAgent: "runtime",
		Payload:     []byte(`{"ok":true}`),
		RunID:       skipRunID,
		CreatedAt:   time.Now().Add(-3 * time.Minute).UTC(),
	}); err != nil {
		t.Fatalf("AppendEvent(skip parent): %v", err)
	}
	if err := pg.AppendEvent(ctx, events.Event{
		ID:            skipChildID,
		Type:          events.EventType("system.recover.skip"),
		SourceAgent:   "runtime",
		Payload:       []byte(`{"ok":true}`),
		RunID:         skipRunID,
		ParentEventID: skipParentID,
		CreatedAt:     time.Now().Add(-2 * time.Minute).UTC(),
	}); err != nil {
		t.Fatalf("AppendEvent(skip child): %v", err)
	}
	if err := pg.UpsertCommittedReplayScope(ctx, skipChildID, runtimereplayclaim.CommittedReplayScopeDirect); err != nil {
		t.Fatalf("UpsertCommittedReplayScope(skip child): %v", err)
	}
	if err := pg.UpsertPipelineReceipt(ctx, skipParentID, "processed", ""); err != nil {
		t.Fatalf("UpsertPipelineReceipt(skip parent): %v", err)
	}

	droppedEventID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			event_id, event_name, scope, payload, produced_by, produced_by_type, created_at
		) VALUES (
			$1::uuid, 'system.recover.drop', 'global', '{}'::jsonb, 'runtime', 'platform', now()
		)
	`, droppedEventID); err != nil {
		t.Fatalf("seed dropped replay event: %v", err)
	}

	rm := runtimepipeline.NewRecoveryManagerWith(pg, bus)
	if err := rm.Recover(ctx); err != nil {
		t.Fatalf("Recover: %v", err)
	}

	reader := dashboardserver.NewSQLObservabilityReader(db, pg)
	if reader == nil {
		t.Fatal("NewSQLObservabilityReader returned nil")
	}
	logs, err := reader.ListRuntimeLogs(ctx, dashboardserver.RuntimeLogFilter{
		Component: "pipeline-recovery",
		Type:      "startup_recovery_pipeline_replay_aftermath",
	}, 10)
	if err != nil {
		t.Fatalf("ListRuntimeLogs: %v", err)
	}
	if len(logs) != 3 {
		t.Fatalf("runtime log rows = %d, want 3: %#v", len(logs), logs)
	}
	findLogIndex := func(eventID string) int {
		t.Helper()
		for idx, log := range logs {
			if strings.TrimSpace(log.EventID) == strings.TrimSpace(eventID) {
				return idx
			}
		}
		t.Fatalf("missing runtime log for event %q in %#v", eventID, logs)
		return -1
	}

	replayed := logs[findLogIndex(replayChildID)]
	if replayed.Action != "startup_recovery_pipeline_replay_aftermath" {
		t.Fatalf("replayed action = %q, want startup_recovery_pipeline_replay_aftermath", replayed.Action)
	}
	replayedDetail, _ := replayed.Detail.(map[string]any)
	if got := readString(replayedDetail["decision_outcome"]); got != "replayed" {
		t.Fatalf("replayed detail.decision_outcome = %q, want replayed", got)
	}
	if got := readString(replayedDetail["decision_reason_code"]); got != "persisted_recipients_replayed" {
		t.Fatalf("replayed detail.decision_reason_code = %q, want persisted_recipients_replayed", got)
	}

	skipped := logs[findLogIndex(skipChildID)]
	skippedDetail, _ := skipped.Detail.(map[string]any)
	if got := readString(skippedDetail["decision_outcome"]); got != "skipped" {
		t.Fatalf("skipped detail.decision_outcome = %q, want skipped", got)
	}
	if got := readString(skippedDetail["decision_reason_code"]); got != "no_persisted_recipients" {
		t.Fatalf("skipped detail.decision_reason_code = %q, want no_persisted_recipients", got)
	}

	dropped := logs[findLogIndex(droppedEventID)]
	droppedDetail, _ := dropped.Detail.(map[string]any)
	if got := readString(droppedDetail["decision_outcome"]); got != "dropped" {
		t.Fatalf("dropped detail.decision_outcome = %q, want dropped", got)
	}
	if got := readString(droppedDetail["decision_reason_code"]); got != "replay_quarantined" {
		t.Fatalf("dropped detail.decision_reason_code = %q, want replay_quarantined", got)
	}
	if readString(droppedDetail["error_code"]) != "replay_quarantined" {
		t.Fatalf("dropped detail.error_code = %q, want replay_quarantined", readString(droppedDetail["error_code"]))
	}
	if strings.TrimSpace(dropped.Error) == "" {
		t.Fatal("dropped recovery aftermath log missing explicit error text")
	}
}

func TestCanonicalRuntimeLogTurnBlockSurface_RoundTripsThroughConversationReader(t *testing.T) {
	ctx := context.Background()
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}

	requireCanonicalConversationSurface(t, ctx, pg)
	seedConformanceAgent(t, ctx, pg, "agent-1")

	sessionID := uuid.NewString()
	if err := pg.UpsertConversation(ctx, runtimellm.ConversationRecord{
		SessionID: sessionID,
		AgentID:   "agent-1",
		Mode:      "task",
		Messages: []runtimellm.Message{
			{Role: "assistant", Content: "done"},
		},
		Summary:   "done",
		TurnCount: 1,
		Status:    "active",
	}); err != nil {
		t.Fatalf("UpsertConversation(task): %v", err)
	}

	if err := pg.AppendAgentTurn(ctx, runtimellm.AgentTurnRecord{
		AgentID:     "agent-1",
		RuntimeMode: "task",
		SessionID:   sessionID,
		TurnBlocks: []runtimellm.TurnBlock{
			{
				Kind:  "runtime_log",
				Title: "Tool execution was denied for save_entity_field",
				Data: json.RawMessage(`{
					"log_level":"warn",
					"message":"Tool execution was denied for save_entity_field",
					"details":{
						"component":"tool-executor",
						"action":"tool_execution_denied",
						"tool_name":"save_entity_field"
					}
				}`),
			},
		},
		ParseOK: true,
		Latency: 5 * time.Millisecond,
	}); err != nil {
		t.Fatalf("AppendAgentTurn(task runtime_log block): %v", err)
	}

	reader := dashboardserver.NewSQLConversationReader(db, pg)
	if reader == nil {
		t.Fatal("NewSQLConversationReader returned nil")
	}
	item, ok, err := reader.Get(ctx, sessionID)
	if err != nil {
		t.Fatalf("Get conversation: %v", err)
	}
	if !ok {
		t.Fatalf("conversation %s not found", sessionID)
	}
	if len(item.Turns) != 1 {
		t.Fatalf("conversation turns = %d, want 1", len(item.Turns))
	}
	blocks := item.Turns[0].TurnBlocks
	if len(blocks) != 1 || blocks[0].Kind != "runtime_log" {
		t.Fatalf("runtime_log turn blocks = %#v", blocks)
	}
	var data map[string]any
	if err := json.Unmarshal(blocks[0].Data, &data); err != nil {
		t.Fatalf("decode runtime_log turn block data: %v", err)
	}
	if readString(data["log_level"]) != "warn" || readString(data["message"]) != "Tool execution was denied for save_entity_field" {
		t.Fatalf("runtime_log turn block data = %#v", data)
	}
	details, _ := data["details"].(map[string]any)
	if readString(details["component"]) != "tool-executor" || readString(details["action"]) != "tool_execution_denied" {
		t.Fatalf("runtime_log turn block details = %#v", details)
	}
}

func TestCanonicalMutationSurface_ReconstructsTrackedEntityStateForWorkflowWrites(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	runID := uuid.NewString()
	ctx := runtimecorrelation.WithRunID(context.Background(), runID)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status)
		VALUES ($1::uuid, 'running')
	`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}

	requireMutationSurface(t, db)

	instanceStore := runtimepipeline.NewWorkflowInstanceStore(db)
	entityID := uuid.NewString()
	if err := instanceStore.Upsert(ctx, runtimepipeline.WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      entityID,
		WorkflowName:    "mutation-flow",
		WorkflowVersion: "1.0.0",
		CurrentState:    "queued",
		Metadata: map[string]any{
			"status": "open",
			"gates": map[string]any{
				"g_ready": true,
			},
		},
		StateBuckets: map[string]any{
			"evidence": map[string]any{"score": 1},
		},
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}
	if err := instanceStore.Upsert(ctx, runtimepipeline.WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      entityID,
		WorkflowName:    "mutation-flow",
		WorkflowVersion: "1.0.0",
		CurrentState:    "done",
		Metadata: map[string]any{
			"status": "closed",
			"gates": map[string]any{
				"g_done": true,
			},
		},
		StateBuckets: map[string]any{
			"evidence": map[string]any{"score": 2},
			"notes":    map[string]any{"count": 1},
		},
	}); err != nil {
		t.Fatalf("update workflow instance: %v", err)
	}

	if err := trackedMutationStateMatchesEntityState(db, runID, entityID); err != nil {
		t.Fatalf("trackedMutationStateMatchesEntityState(workflow): %v", err)
	}
}

func TestCanonicalMutationSurface_ReconstructsTrackedEntityStateForToolWrites(t *testing.T) {
	ctx, exec, db, runID := newEntityToolConformanceHarness(t)

	requireMutationSurface(t, db)

	createOut, err := exec.Execute(ctx, "create_entity", map[string]any{
		"flow_instance": "review/inst-1",
		"fields": map[string]any{
			"status": "open",
			"score":  10.0,
		},
	})
	if err != nil {
		t.Fatalf("create_entity: %v", err)
	}
	created, ok := createOut.(map[string]any)
	if !ok {
		t.Fatalf("create_entity output = %#v, want map", createOut)
	}
	entityID := readString(created["entity_id"])
	if entityID == "" {
		t.Fatal("create_entity did not return entity_id")
	}
	if _, err := exec.Execute(ctx, "save_entity_field", map[string]any{
		"entity_id": entityID,
		"field":     "status",
		"value":     "closed",
	}); err != nil {
		t.Fatalf("save_entity_field: %v", err)
	}

	if err := trackedMutationStateMatchesEntityState(db, runID, entityID); err != nil {
		t.Fatalf("trackedMutationStateMatchesEntityState(tool): %v", err)
	}
}

func TestCanonicalMutationSurface_FailsOnMalformedCanonicalMutationField(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	runID := uuid.NewString()
	ctx := runtimecorrelation.WithRunID(context.Background(), runID)

	requireMutationSurface(t, db)

	entityID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id) VALUES ($1::uuid)`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, current_state, gates, fields, accumulator
		)
		VALUES (
			$1::uuid, $2::uuid, 'mutation-flow/inst-1', 'default', 'queued', '{}'::jsonb, '{}'::jsonb, '{}'::jsonb
		)
	`, runID, entityID); err != nil {
		t.Fatalf("seed entity_state: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			run_id, entity_id, field, old_value, new_value, writer_type, writer_id, handler_step
		) VALUES (
			$1::uuid, $2::uuid, 'gates.', 'null'::jsonb, 'true'::jsonb, 'platform', 'test', 'seed'
		)
	`, runID, entityID); err != nil {
		t.Fatalf("seed malformed mutation: %v", err)
	}

	err := trackedMutationStateMatchesEntityState(db, runID, entityID)
	if err == nil || !strings.Contains(err.Error(), "gates mutation key is required") {
		t.Fatalf("trackedMutationStateMatchesEntityState error = %v, want malformed gates failure", err)
	}
}

func requireCanonicalConversationSurface(t *testing.T, ctx context.Context, pg *store.PostgresStore) {
	t.Helper()
	caps, err := pg.BindSchemaCapabilities(ctx)
	if err != nil {
		t.Fatalf("BindSchemaCapabilities: %v", err)
	}
	if caps.Conversations.Turns != store.SchemaFlavorCanonical {
		t.Fatalf("agent_turns capability = %s, want canonical", caps.Conversations.Turns)
	}
	if !caps.Conversations.TurnBlocks {
		t.Fatal("agent_turns.turn_blocks capability is missing; canonical turn-summary surface is not enforceable")
	}
	if caps.Conversations.Audits != store.SchemaFlavorCanonical {
		t.Fatalf("agent_conversation_audits capability = %s, want canonical", caps.Conversations.Audits)
	}
}

func requireCanonicalRuntimeLogSurface(t *testing.T, ctx context.Context, pg *store.PostgresStore) {
	t.Helper()
	caps, err := pg.BindSchemaCapabilities(ctx)
	if err != nil {
		t.Fatalf("BindSchemaCapabilities: %v", err)
	}
	if caps.Events.Log != store.SchemaFlavorCanonical {
		t.Fatalf("events capability = %s, want canonical", caps.Events.Log)
	}
	requireTableColumns(t, ctx, pg.DB, "events", "event_id", "event_name", "payload", "scope", "created_at")
}

func requireMutationSurface(t *testing.T, db *sql.DB) {
	t.Helper()
	requireTableColumns(t, context.Background(), db, "entity_state", "entity_id", "current_state", "fields", "gates", "accumulator")
	requireTableColumns(t, context.Background(), db, "entity_mutations", "entity_id", "field", "old_value", "new_value", "writer_type", "writer_id", "handler_step", "created_at")
}

func requireTableColumns(t *testing.T, ctx context.Context, db *sql.DB, tableName string, columns ...string) {
	t.Helper()
	for _, column := range columns {
		var exists bool
		if err := db.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM information_schema.columns
				WHERE table_schema = 'public'
				  AND table_name = $1
				  AND column_name = $2
			)
		`, strings.TrimSpace(tableName), strings.TrimSpace(column)).Scan(&exists); err != nil {
			t.Fatalf("inspect column %s.%s: %v", tableName, column, err)
		}
		if !exists {
			t.Fatalf("missing required canonical column %s.%s", tableName, column)
		}
	}
}

func seedConformanceAgent(t *testing.T, ctx context.Context, pg *store.PostgresStore, agentID string) {
	t.Helper()
	if err := pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:     agentID,
			Role:   "tester",
			Mode:   "global",
			Type:   "stub",
			Config: []byte(`{"system_prompt":"x"}`),
		},
		Status:    "active",
		HiredBy:   "conformance-test",
		StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("UpsertAgent(%s): %v", agentID, err)
	}
}

func countTurnSummaryBlocks(blocks []dashboardserver.ConversationTurnBlock) int {
	count := 0
	for _, block := range blocks {
		if strings.TrimSpace(block.Kind) == "turn_summary" {
			count++
		}
	}
	return count
}

func trackedMutationStateMatchesEntityState(db *sql.DB, runID, entityID string) error {
	var (
		currentState string
		fieldsRaw    []byte
		gatesRaw     []byte
		accRaw       []byte
	)
	if err := db.QueryRowContext(context.Background(), `
		SELECT
			COALESCE(current_state, ''),
			COALESCE(fields, '{}'::jsonb),
			COALESCE(gates, '{}'::jsonb),
			COALESCE(accumulator, '{}'::jsonb)
			FROM entity_state
			WHERE run_id = $1::uuid AND entity_id = $2::uuid
		`, runID, entityID).Scan(&currentState, &fieldsRaw, &gatesRaw, &accRaw); err != nil {
		return fmt.Errorf("load entity_state projection: %w", err)
	}

	want := runtimemutationlog.EntityStateProjection{
		CurrentState: strings.TrimSpace(currentState),
		Fields:       map[string]any{},
		Gates:        map[string]any{},
		Accumulator:  map[string]any{},
	}
	var err error
	if want.Fields, err = decodeJSONMapErr(fieldsRaw); err != nil {
		return fmt.Errorf("decode entity_state fields: %w", err)
	}
	if want.Gates, err = decodeJSONMapErr(gatesRaw); err != nil {
		return fmt.Errorf("decode entity_state gates: %w", err)
	}
	if want.Accumulator, err = decodeJSONMapErr(accRaw); err != nil {
		return fmt.Errorf("decode entity_state accumulator: %w", err)
	}
	records := make([]runtimemutationlog.ProjectionMutation, 0, 8)

	rows, err := db.QueryContext(context.Background(), `
		SELECT field, new_value
			FROM entity_mutations
			WHERE run_id = $1::uuid AND entity_id = $2::uuid
			ORDER BY created_at ASC, mutation_id ASC
		`, runID, entityID)
	if err != nil {
		return fmt.Errorf("query mutations: %w", err)
	}
	defer rows.Close()

	rowCount := 0
	for rows.Next() {
		rowCount++
		var (
			field    string
			newValue []byte
		)
		if err := rows.Scan(&field, &newValue); err != nil {
			return fmt.Errorf("scan mutation: %w", err)
		}
		value, err := decodeJSONValueErr(newValue)
		if err != nil {
			return fmt.Errorf("decode mutation value: %w", err)
		}
		records = append(records, runtimemutationlog.ProjectionMutation{
			Field:    strings.TrimSpace(field),
			NewValue: value,
		})
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read mutations: %w", err)
	}
	if rowCount == 0 {
		return fmt.Errorf("entity_mutations is empty; canonical mutation surface is missing")
	}
	got, err := runtimemutationlog.ReconstructEntityStateProjection(records)
	if err != nil {
		return fmt.Errorf("reconstruct mutation state: %w", err)
	}
	if !trackedStatesEqual(got, want) {
		return fmt.Errorf("mutation reconstruction mismatch:\n got=%s\nwant=%s", mustCanonicalJSON(nil, got), mustCanonicalJSON(nil, want))
	}
	return nil
}

func trackedStatesEqual(left, right runtimemutationlog.EntityStateProjection) bool {
	return mustCanonicalJSON(nil, left) == mustCanonicalJSON(nil, right)
}

func decodeJSONMap(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	out, err := decodeJSONMapErr(raw)
	if err != nil {
		t.Fatalf("json.Unmarshal map: %v", err)
	}
	return out
}

func decodeJSONMapErr(raw []byte) (map[string]any, error) {
	if len(raw) == 0 {
		return map[string]any{}, nil
	}
	out := map[string]any{}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func decodeJSONValue(t *testing.T, raw []byte) any {
	t.Helper()
	out, err := decodeJSONValueErr(raw)
	if err != nil {
		t.Fatalf("json.Unmarshal value: %v", err)
	}
	return out
}

func decodeJSONValueErr(raw []byte) (any, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var out any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func mustCanonicalJSON(t *testing.T, value any) string {
	if t != nil {
		t.Helper()
	}
	raw, err := json.Marshal(value)
	if err != nil {
		if t != nil {
			t.Fatalf("json.Marshal canonical: %v", err)
		}
		return ""
	}
	return string(raw)
}

func newEntityToolConformanceHarness(t *testing.T) (context.Context, *runtimetools.Executor, *sql.DB, string) {
	t.Helper()
	_, db, _ := testutil.StartPostgres(t)
	runID := uuid.NewString()
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO runs (run_id, status)
		VALUES ($1::uuid, 'running')
	`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	exec := runtimetools.NewExecutorWithOptions(nil, nil, runtimetools.ExecutorOptions{
		SQLDB:                          db,
		AllowInternalLegacyEntityTools: true,
		WorkflowSource: runtimesemanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
			RootEntities: runtimecontracts.EntityContractsDocument{
				"accounts": {
					Fields: map[string]runtimecontracts.EntityFieldDecl{
						"score":  {Type: "numeric(10,2)"},
						"status": {Type: "text"},
					},
				},
			},
			Semantics: runtimecontracts.WorkflowSemanticView{
				Name:         "review",
				InitialStage: "queued",
			},
		}),
	})
	ctx := runtimecorrelation.WithRunID(context.Background(), runID)
	ctx = runtimetools.WithActor(ctx, runtimeactors.AgentConfig{
		ID:    "tester",
		Type:  "internal",
		Role:  "operator",
		Tools: []string{"create_entity", "save_entity_field"},
	})
	return ctx, exec, db, runID
}

func readString(value any) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	default:
		return ""
	}
}
