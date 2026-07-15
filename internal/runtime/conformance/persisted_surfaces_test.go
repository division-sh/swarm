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

	"github.com/division-sh/swarm/internal/config"
	dashboardserver "github.com/division-sh/swarm/internal/dashboard/server"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeownership "github.com/division-sh/swarm/internal/runtime/core/ownership"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimediaglog "github.com/division-sh/swarm/internal/runtime/diaglog"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimemutationlog "github.com/division-sh/swarm/internal/runtime/mutationlog"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimereplayclaim "github.com/division-sh/swarm/internal/runtime/replayclaim"
	runtimesemanticview "github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	runtimeworkspace "github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type staticWorkspaceResolver struct {
	target *runtimeworkspace.Target
	err    error
}

type discardCompletionSpendProjection struct{}

func (discardCompletionSpendProjection) ProjectCommittedCompletionSpend(context.Context, runtimeeffects.CompletionSpendProjection) {
}

type conformanceToolExecutor struct{}

func (conformanceToolExecutor) Execute(context.Context, string, any) (any, error) {
	return nil, fmt.Errorf("conformance runtime declares no callable tools")
}

func (conformanceToolExecutor) ToolCapabilitiesForActor(runtimeactors.AgentConfig, []string, map[string]struct{}) toolcapabilities.Set {
	return toolcapabilities.NewSet(nil)
}

func (s staticWorkspaceResolver) ResolveWorkspace(context.Context, runtimeactors.AgentConfig) (*runtimeworkspace.Target, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.target, nil
}

func conformanceProviderCredentialResolver(t testing.TB, key, value string) runtimellm.ProviderCredentialResolver {
	t.Helper()
	store, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "provider-credentials.json"))
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	if err := store.Set(context.Background(), key, value); err != nil {
		t.Fatalf("Set provider credential: %v", err)
	}
	return runtimellm.NewProviderCredentialResolver(store)
}

func acquireLiveConversationSession(t *testing.T, ctx context.Context, db *sql.DB, identity agentmemory.Identity) string {
	t.Helper()
	registry := &store.PostgresStore{DB: db}
	registry.SetSessionLockTTL(30 * time.Second)
	ctx = runtimeeffects.WithDifferentOwner(ctx, runtimeeffects.OwnerBuildTestInfrastructure)
	lease, err := registry.Acquire(ctx, identity, "test-owner")
	if err != nil {
		t.Fatalf("Acquire(%+v): %v", identity, err)
	}
	if err := registry.Release(ctx, lease); err != nil {
		t.Fatalf("Release(%s,%s): %v", identity.AgentID, lease.SessionID, err)
	}
	return lease.SessionID
}

func TestCanonicalTurnSummarySurface_RoundTripsThroughConversationReader(t *testing.T) {
	ctx := runtimeeffects.WithExecutionMode(context.Background(), runtimeeffects.ExecutionModeLive)
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}

	requireCanonicalConversationSurface(t, ctx, pg)
	seedConformanceAgent(t, ctx, pg, "agent-1")

	runID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	sessionID := uuid.NewString()
	if err := pg.AppendAgentTurn(ctx, managedConformanceTurnRecord(t, runtimellm.AgentTurnRecord{
		AgentID:   "agent-1",
		Memory:    agentmemory.PlatformDefault(),
		SessionID: sessionID,
		RunID:     runID,
		TurnBlocks: []runtimellm.TurnBlock{
			{Kind: "dispatch", Title: "task.run", Data: json.RawMessage(`{"trigger_event_id":"event-1","trigger_event_type":"task.run"}`)},
			{Kind: "tool_use", ToolName: "schedule", Input: json.RawMessage(`{"delay_seconds":1209600}`), Data: json.RawMessage(`{"tool_use_id":"toolu_1"}`)},
			{Kind: "tool_result", ToolName: "schedule", Output: json.RawMessage(`{"status":"scheduled"}`), Data: json.RawMessage(`{"tool_use_id":"toolu_1"}`)},
			{Kind: "assistant_text", Text: "Parking for manual review."},
			{Kind: "outcome", Text: "14-day review scheduled."},
		},
		RequestPayload: []byte(`{"kind":"task"}`),
		ResponseRaw:    []byte(`{"result":"stale fallback text"}`),
		ParseOK:        true,
		Latency:        5 * time.Millisecond,
	})); err != nil {
		t.Fatalf("AppendAgentTurn: %v", err)
	}

	reader := dashboardserver.NewSQLConversationReader(db, pg)
	if reader == nil {
		t.Fatal("NewSQLConversationReader returned nil")
	}
	page, err := reader.ListOperatorConversationTurns(ctx, store.OperatorConversationTurnListOptions{SessionID: sessionID, Limit: 1})
	if err != nil {
		t.Fatalf("ListOperatorConversationTurns: %v", err)
	}
	if len(page.Turns) != 1 {
		t.Fatalf("conversation turns = %d, want 1", len(page.Turns))
	}
	item, err := reader.LoadOperatorPublicConversationTurn(ctx, sessionID, page.Turns[0].TurnID)
	if err != nil {
		t.Fatalf("LoadOperatorPublicConversationTurn: %v", err)
	}
	turn := item.Turn
	if turn.AssistantVisibleOutput != "Parking for manual review." {
		t.Fatalf("assistant_visible_output = %q, want %q", turn.AssistantVisibleOutput, "Parking for manual review.")
	}
	if turn.Outcome != "14-day review scheduled." {
		t.Fatalf("outcome = %q, want %q", turn.Outcome, "14-day review scheduled.")
	}
	if len(turn.Activity) != 4 {
		t.Fatalf("public activity = %#v, want dispatch/tool/tool_result/output", turn.Activity)
	}
	if turn.Activity[2].Kind != "tool_result" || strings.TrimSpace(turn.Activity[2].ToolName) != "schedule" || turn.Activity[2].ToolUseID != "toolu_1" {
		t.Fatalf("public tool result activity = %#v", turn.Activity[2])
	}
	raw, err := json.Marshal(turn)
	if err != nil {
		t.Fatalf("marshal public turn: %v", err)
	}
	for _, forbidden := range []string{"delay_seconds", `"scheduled"`, "stale fallback text", "request_payload", "response_payload", "turn_blocks"} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatalf("public turn leaked %q: %s", forbidden, raw)
		}
	}
}

func TestCanonicalSessionWatchdogSurface_RoundTripsThroughConversationReader(t *testing.T) {
	ctx := context.Background()
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}

	requireCanonicalConversationSurface(t, ctx, pg)
	lifecycleToken := seedConformanceRunningAgent(t, ctx, pg, "agent-1")
	ctx = runtimeeffects.WithLifecycleToken(ctx, lifecycleToken)

	identity := agentmemory.Identity{RunID: uuid.NewString(), AgentID: "agent-1", FlowInstance: "support/inst-1"}
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id) VALUES ($1::uuid)`, identity.RunID); err != nil {
		t.Fatalf("seed memory run: %v", err)
	}
	sessionID := acquireLiveConversationSession(t, ctx, db, identity)
	if err := pg.UpsertConversation(ctx, runtimellm.ConversationRecord{
		SessionID: sessionID,
		AgentID:   "agent-1",
		Identity:  identity,
		Memory:    agentmemory.Authored(true),
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
		SessionID: sessionID,
		AgentID:   "agent-1",
		Identity:  identity,
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
	page, err := reader.ListOperatorConversationTurns(ctx, store.OperatorConversationTurnListOptions{SessionID: sessionID, Limit: 10})
	if err != nil {
		t.Fatalf("ListOperatorConversationTurns: %v", err)
	}
	if page.Conversation.Metadata.Watchdog == nil {
		t.Fatal("expected runtime_state.watchdog to round-trip")
	}
	if page.Conversation.Metadata.Watchdog.State != "no_output" || page.Conversation.Metadata.Watchdog.Action != "session_no_output" {
		t.Fatalf("unexpected runtime_state.watchdog: %+v", page.Conversation.Metadata.Watchdog)
	}
	list, err := reader.ListOperatorConversations(ctx, store.OperatorConversationListOptions{Limit: 10})
	if err != nil {
		t.Fatalf("ListOperatorConversations: %v", err)
	}
	items := list.Conversations
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
	lifecycleToken := seedConformanceRunningAgent(t, ctx, pg, "agent-1")

	runID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}

	event1 := eventtest.PersistedProjection(uuid.NewString(),

		events.EventType("review.requested"),
		"runtime", "", []byte(`{"turn":1}`), 0, runID, "", events.EventEnvelope{}, time.Now().Add(-2*time.Minute).UTC())

	event2 := eventtest.PersistedProjection(uuid.NewString(),

		events.EventType("review.requested"),
		"runtime", "", []byte(`{"turn":2}`), 0, runID, "", events.EventEnvelope{}, time.Now().Add(-1*time.Minute).UTC())

	for _, evt := range []events.Event{event1, event2} {
		if err := pg.AppendEvent(ctx, evt); err != nil {
			t.Fatalf("AppendEvent(%s): %v", evt.ID(), err)
		}
		if err := pg.InsertEventDeliveries(ctx, evt.ID(), []string{"agent-1"}); err != nil {
			t.Fatalf("InsertEventDeliveries(%s): %v", evt.ID(), err)
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
	bus, err := runtimebus.NewEventBus(pg)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	pg.SetSessionLockTTL(30 * time.Second)
	credentials := conformanceProviderCredentialResolver(t, "ANTHROPIC_API_KEY", "test-key")
	runtime, err := (runtimellm.RuntimeFactory{
		Cfg:                  &config.Config{},
		Sessions:             pg,
		Conversations:        pg,
		LockOwner:            "worker-1",
		Events:               bus,
		Credentials:          credentials.Store,
		CompletionController: runtimeeffects.NewCompletionController(pg, discardCompletionSpendProjection{}),
	}).Build()
	if err != nil {
		t.Fatalf("Build LLM runtime: %v", err)
	}

	newTurnContext := func(evt events.Event) context.Context {
		base := runtimeeffects.WithLifecycleToken(context.Background(), lifecycleToken)
		base = managedConformanceExecutionContext(t, base, "reused-live-session")
		base = agentmemory.WithExecution(base, agentmemory.Authored(true), agentmemory.Identity{RunID: runID, AgentID: "agent-1", FlowInstance: "support/inst-1"})
		base = runtimecorrelation.WithRunID(base, runID)
		base = runtimebus.WithInboundEvent(base, evt)
		return runtimeactors.WithActor(base, runtimeactors.AgentConfig{
			ExecutionMode: "live",
			ID:            "agent-1",
			Type:          "stub",
			Model:         "regular",
			Memory:        agentmemory.Authored(true),
			FlowPath:      "support/inst-1",
		})
	}

	conversation := runtimellm.NewConversation("agent-1", "", "system", nil, agentmemory.Authored(true), 10, runtime)
	conversation.SetToolExecutor(conformanceToolExecutor{})
	if _, err := conversation.Step(newTurnContext(event1), "first"); err != nil {
		t.Fatalf("ContinueSession(first): %v", err)
	}
	session := conversation.Session
	if err := pg.UpsertEventReceipt(newTurnContext(event1), event1.ID(), "agent-1", runtimemanager.ReceiptStatusProcessed, nil); err != nil {
		t.Fatalf("UpsertEventReceipt(first): %v", err)
	}

	if err := pg.MarkEventDeliveryInProgress(newTurnContext(event2), event2.ID(), "agent-1", ""); err != nil {
		t.Fatalf("MarkEventDeliveryInProgress(second prelaunch): %v", err)
	}
	if _, err := conversation.Step(newTurnContext(event2), "second"); err != nil {
		t.Fatalf("ContinueSession(second): %v", err)
	}

	var (
		deliveryStatus   string
		activeSessionID  string
		sessionStatus    string
		sessionFlow      string
		liveSessionCount int
	)
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(status, ''), COALESCE(active_session_id::text, '')
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'agent'
		  AND subscriber_id = 'agent-1'
	`, event2.ID()).Scan(&deliveryStatus, &activeSessionID); err != nil {
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
			COALESCE(flow_instance, '')
		FROM agent_sessions
		WHERE session_id = $1::uuid
	`, session.ID).Scan(&sessionStatus, &sessionFlow); err != nil {
		t.Fatalf("load live session row: %v", err)
	}
	if sessionStatus != "active" {
		t.Fatalf("session status = %q, want active", sessionStatus)
	}
	if sessionFlow != "support/inst-1" {
		t.Fatalf("session flow_instance = %q, want support/inst-1", sessionFlow)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM agent_sessions
		WHERE run_id = $1::uuid
		  AND agent_id = 'agent-1'
		  AND flow_instance = 'support/inst-1'
		  AND memory_enabled = TRUE
		  AND status = 'active'
	`, runID).Scan(&liveSessionCount); err != nil {
		t.Fatalf("count live session lineage: %v", err)
	}
	if liveSessionCount != 1 {
		t.Fatalf("live session lineage count = %d, want 1", liveSessionCount)
	}

	facts, err := pg.ListAgentDeliveryLifecycleFacts(ctx, []string{"agent-1"})
	if err != nil {
		t.Fatalf("ListAgentDeliveryLifecycleFacts: %v", err)
	}
	if got := facts["agent-1"].CurrentState; got != "active" {
		t.Fatalf("lifecycle current_state = %q, want active", got)
	}
	if got := facts["agent-1"].BlockingLayer; got != "session_execution" {
		t.Fatalf("lifecycle blocking_layer = %q, want session_execution", got)
	}
}

func TestCLISessionFailureDoesNotRotateFromStderrProse(t *testing.T) {
	ctx := context.Background()
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	pg := &store.PostgresStore{DB: db}

	requireCanonicalConversationSurface(t, ctx, pg)
	requireCanonicalDeliveryLifecycleSurface(t, ctx, pg)
	lifecycleToken := seedConformanceRunningAgent(t, ctx, pg, "agent-1")

	runID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}

	eventID := uuid.NewString()
	evt := eventtest.PersistedProjection(eventID,

		events.EventType("review.requested"),
		"runtime", "", []byte(`{"turn":1}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())

	if err := pg.AppendEvent(ctx, evt); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}
	if err := pg.InsertEventDeliveries(ctx, evt.ID(), []string{"agent-1"}); err != nil {
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
	t.Setenv("SWARM_FAKE_DOCKER_STATE", dockerState)
	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "stale-oauth-token")

	pg.SetSessionLockTTL(30 * time.Second)
	registry := runtimesessions.Registry(pg)
	bus, err := runtimebus.NewEventBus(pg)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	credentials := conformanceProviderCredentialResolver(t, "CLAUDE_CODE_OAUTH_TOKEN", "oauth-token")
	runtime, err := (runtimellm.RuntimeFactory{
		Cfg: &config.Config{
			Workspace: config.WorkspaceConfig{
				DockerBin: fakeDocker,
			},
			LLM: config.LLMConfig{
				Backend: "claude_cli",
				ClaudeCLI: config.ClaudeCLIConfig{
					Command:      "claude",
					OutputFormat: "json",
				},
			},
		},
		Sessions:  registry,
		LockOwner: "worker-1",
		Workspaces: staticWorkspaceResolver{
			target: &runtimeworkspace.Target{
				Container: "swarm-agent-1",
				Workdir:   "/workspace",
			},
		},
		Conversations:        pg,
		Events:               bus,
		Credentials:          credentials.Store,
		CompletionController: runtimeeffects.NewCompletionController(pg, discardCompletionSpendProjection{}),
	}).Build()
	if err != nil {
		t.Fatalf("Build LLM runtime: %v", err)
	}

	newTurnContext := func(evt events.Event) context.Context {
		base := runtimeeffects.WithLifecycleToken(context.Background(), lifecycleToken)
		base = managedConformanceExecutionContext(t, base, "cli-session-failure")
		base = agentmemory.WithExecution(base, agentmemory.Authored(true), agentmemory.Identity{RunID: runID, AgentID: "agent-1", FlowInstance: "support/inst-1"})
		base = runtimecorrelation.WithRunID(base, runID)
		base = runtimebus.WithInboundEvent(base, evt)
		return runtimeactors.WithActor(base, runtimeactors.AgentConfig{
			ExecutionMode: "live",
			ID:            "agent-1",
			Type:          "stub",
			Memory:        agentmemory.Authored(true),
			FlowPath:      "support/inst-1",
		})
	}

	session, err := runtime.StartSession(newTurnContext(evt), "agent-1", "system", nil)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	originalSessionID := session.ID
	conversation := runtimellm.NewConversation("agent-1", "", "system", nil, agentmemory.Authored(true), 10, runtime)
	conversation.Session = session
	conversation.SetToolExecutor(conformanceToolExecutor{})
	if err := pg.MarkEventDeliveryInProgress(newTurnContext(evt), evt.ID(), "agent-1", ""); err != nil {
		t.Fatalf("MarkEventDeliveryInProgress(prelaunch): %v", err)
	}
	_, err = conversation.Step(newTurnContext(evt), "do not classify stderr")
	if conversation.Session == nil {
		t.Fatal("conversation did not retain the acquired session after provider failure")
	}
	session = conversation.Session
	failure, ok := runtimefailures.As(err)
	if !ok || failure.Failure.Class != runtimefailures.ClassConnectorFailure || failure.Failure.Detail.Code != "claude_cli_process_failed" {
		t.Fatalf("ContinueSession failure = %v, want generic connector failure", err)
	}
	if session.ID != originalSessionID {
		t.Fatalf("session ID rotated from stderr prose: got %q, want %q", session.ID, originalSessionID)
	}

	var (
		deliveryStatus  string
		activeSessionID string
	)
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(status, ''), COALESCE(active_session_id::text, '')
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'agent'
		  AND subscriber_id = 'agent-1'
	`, evt.ID()).Scan(&deliveryStatus, &activeSessionID); err != nil {
		t.Fatalf("load delivery: %v", err)
	}
	if deliveryStatus != "in_progress" {
		t.Fatalf("delivery status = %q, want in_progress", deliveryStatus)
	}
	if activeSessionID != originalSessionID {
		t.Fatalf("delivery active_session_id = %q, want original %q", activeSessionID, originalSessionID)
	}
	var sessionCount int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_sessions WHERE agent_id = 'agent-1'`).Scan(&sessionCount); err != nil {
		t.Fatalf("count sessions: %v", err)
	}
	if sessionCount != 1 {
		t.Fatalf("session count = %d, want no prose-triggered successor", sessionCount)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

func TestConversationPersistenceDoesNotPromoteAuditRowsIntoLiveSessions(t *testing.T) {
	ctx := runtimeeffects.WithExecutionMode(context.Background(), runtimeeffects.ExecutionModeLive)
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}

	requireCanonicalConversationSurface(t, ctx, pg)
	seedConformanceAgent(t, ctx, pg, "agent-1")
	runID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id) VALUES ($1::uuid)`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}

	err := pg.UpsertConversation(ctx, runtimellm.ConversationRecord{
		SessionID: uuid.NewString(),
		AgentID:   "agent-1",
		Identity:  agentmemory.Identity{RunID: runID, AgentID: "agent-1", FlowInstance: "support/inst-1"},
		Memory:    agentmemory.Authored(true),
		Messages:  []runtimellm.Message{{Role: "assistant", Content: "should fail"}},
		Summary:   "should fail",
		TurnCount: 1,
		Status:    "active",
	})
	if err == nil {
		t.Fatal("expected live conversation persistence without a live session row to fail")
	}

	auditSessionID := uuid.NewString()
	if err := pg.AppendAgentTurn(ctx, managedConformanceTurnRecord(t, runtimellm.AgentTurnRecord{
		SessionID: auditSessionID,
		AgentID:   "agent-1",
		RunID:     runID,
		Memory:    agentmemory.Authored(false),
		TurnBlocks: []runtimellm.TurnBlock{
			{Kind: "assistant_text", Text: "done"},
			{Kind: "outcome", Text: "done"},
		},
		ParseOK: true,
	})); err != nil {
		t.Fatalf("AppendAgentTurn(stateless): %v", err)
	}

	reader := dashboardserver.NewSQLConversationReader(db, pg)
	if reader == nil {
		t.Fatal("NewSQLConversationReader returned nil")
	}
	page, err := reader.ListOperatorConversations(ctx, store.OperatorConversationListOptions{Limit: 10})
	if err != nil {
		t.Fatalf("ListOperatorConversations: %v", err)
	}
	items := page.Conversations
	if len(items) != 1 {
		t.Fatalf("conversation count = %d, want 1", len(items))
	}
	if items[0].Kind != "turn_audit" || items[0].SessionID != auditSessionID {
		t.Fatalf("unexpected conversation summary: %+v", items[0])
	}
}

func TestCanonicalRuntimeLogSurface_RoundTripsThroughObservabilityReader(t *testing.T) {
	ctx := context.Background()
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}

	requireCanonicalRuntimeLogSurface(t, ctx, pg)

	entityID := uuid.NewString()
	parentEventID := uuid.NewString()
	logger := runtimepkg.NewRuntimeLogger(pg)
	failure := runtimefailures.Normalize(runtimefailures.New(
		runtimefailures.ClassAuthorizationDenied,
		"cross_flow_write_forbidden",
		"tool-executor",
		"tool_execution_denied",
		map[string]any{"action": "entity_write"},
	), "tool-executor", "tool_execution_denied")
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
		Failure:    &failure,
		DurationUS: 1200,
		Detail: map[string]any{
			"tool_name":       "save_entity_field",
			"denial_layer":    "executor",
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

func TestRetiredAccumulatorCompletionOutcomeSurfaceHasNoPositiveFixture(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	retired := filepath.Join(repoRoot, "tests", "tier2-accumulation", "test-accumulate-on-complete-rollback")
	for _, name := range []string{"package.yaml", "schema.yaml", "nodes.yaml", "events.yaml"} {
		path := filepath.Join(retired, name)
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("retired finite-accumulator fixture artifact still exists at %s: %v", path, err)
		}
	}
}

func TestRetiredAccumulationTimeoutSurfaceHasNoProductionConsumer(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	retiredTokens := []string{
		"TimerHandleAccumulationTimeout",
		"accumulate_timeout:",
		"accumulate.timeout",
		"accumulation_timeout",
	}
	for _, root := range []string{"cmd", "internal"} {
		err := filepath.WalkDir(filepath.Join(repoRoot, root), func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			for _, token := range retiredTokens {
				if strings.Contains(string(raw), token) {
					t.Errorf("retired accumulation-timeout token %q survives in production source %s", token, path)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk production source %s: %v", root, err)
		}
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

type conformanceRecoveryReplayLease struct{}

func (conformanceRecoveryReplayLease) Key() string                   { return "conformance-replay" }
func (conformanceRecoveryReplayLease) Refresh(context.Context) error { return nil }
func (conformanceRecoveryReplayLease) Release(context.Context) error { return nil }

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

func (s conformanceRecoveryFailureEventStore) ClaimPipelinePublication(ctx context.Context, eventID string) (runtimeownership.Lease, bool, error) {
	return s.store.ClaimPipelinePublication(ctx, eventID)
}

func (conformanceRecoveryFailureEventStore) SupportsPersistedReplay() bool { return true }

type conformanceManagerReplayStore struct {
	agents   []runtimemanager.PersistedAgent
	pending  map[string][]events.Event
	receipts map[string]runtimemanager.EventReceipt
}

func (*conformanceManagerReplayStore) CommitAgentLifecycleTransition(_ context.Context, req runtimemanager.AgentLifecycleTransition) (runtimemanager.AgentLifecycleTransitionResult, error) {
	return runtimemanager.AgentLifecycleTransitionResult{
		OperationID: req.OperationID, TransitionID: req.OperationID, AgentID: req.AgentID,
		PreviousEpoch: req.ExpectedEpoch, RuntimeEpoch: req.TargetEpoch,
		PreviousGeneration: req.ExpectedGeneration, Generation: req.TargetGeneration,
		PreviousPhase: req.ExpectedPhase, Phase: req.TargetPhase,
		ConfigRevision: req.ConfigRevision, RunMode: req.RunMode,
	}, nil
}

func (*conformanceManagerReplayStore) UpsertAgent(context.Context, runtimemanager.PersistedAgent) error {
	return nil
}

func (s *conformanceManagerReplayStore) LoadAgents(context.Context) ([]runtimemanager.PersistedAgent, error) {
	return append([]runtimemanager.PersistedAgent(nil), s.agents...), nil
}

func (*conformanceManagerReplayStore) MarkAgentTerminated(context.Context, string) error { return nil }
func (*conformanceManagerReplayStore) EnsureEntitySchema(context.Context, string) error  { return nil }
func (s *conformanceManagerReplayStore) UpsertEventReceipt(_ context.Context, eventID, agentID string, status runtimemanager.ReceiptStatus, failure *runtimefailures.Envelope) error {
	if s.receipts == nil {
		s.receipts = map[string]runtimemanager.EventReceipt{}
	}
	s.receipts[strings.TrimSpace(eventID)+"|"+strings.TrimSpace(agentID)] = runtimemanager.EventReceipt{
		EventID: eventID,
		AgentID: agentID,
		Status:  status,
		Failure: failure,
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
	switch evt.Type() {
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
			Backend: "anthropic",
		},
	}, Stores: runtimepkg.Stores{
		SQLDB:           db,
		PipelineStore:   runtimepipeline.NewWorkflowInstanceStore(db),
		EventStore:      pg,
		RuntimeLogStore: pg,
		ManagerStore:    pg,
		ScheduleStore:   pg,
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
	if log.ErrorCode != "startup_recovery_disabled_with_work" {
		t.Fatalf("log error_code = %q, want canonical failure detail", log.ErrorCode)
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
			Event: eventtest.PersistedProjection(uuid.NewString(),
				events.EventType("support.item_created"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().Add(-time.Minute).UTC()),
		}},
		claimErr: errors.New("claim failed"),
	}

	rt, err := runtimepkg.NewRuntime(ctx, runtimepkg.RuntimeDeps{Config: &config.Config{
		Runtime: config.RuntimeConfig{
			RecoveryOnStartup: true,
		},
		LLM: config.LLMConfig{
			Backend: "anthropic",
		},
	}, Stores: runtimepkg.Stores{
		SQLDB:           db,
		PipelineStore:   runtimepipeline.NewWorkflowInstanceStore(db),
		EventStore:      eventStore,
		RuntimeLogStore: pg,
		ManagerStore:    &conformanceManagerReplayStore{},
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
	failureRaw, err := json.Marshal(recoveryFailedPayload["failure"])
	if err != nil {
		t.Fatalf("marshal platform.recovery_failed failure: %v", err)
	}
	recoveryFailure, err := runtimefailures.UnmarshalEnvelope(failureRaw)
	if err != nil || recoveryFailure.Detail.Code != "startup_manager_recovery_failed" {
		t.Fatalf("platform.recovery_failed failure = %#v err=%v, want startup_manager_recovery_failed", recoveryFailedPayload["failure"], err)
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
			Backend: "anthropic",
		},
	}, Stores: runtimepkg.Stores{
		SQLDB:           db,
		PipelineStore:   runtimepipeline.NewWorkflowInstanceStore(db),
		EventStore:      conformanceTimerRecoveryEventStore{},
		RuntimeLogStore: pg,
		ManagerStore:    pg,
		ScheduleStore:   scheduleStore,
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
	if dropped.Failure == nil || dropped.Failure.Detail.Code != "schedule_restore_failed" {
		t.Fatalf("dropped timer recovery failure = %#v", dropped.Failure)
	}
}

type conformanceRuntimeLoggerHook struct {
	logger *runtimepkg.RuntimeLogger
}

func (h conformanceRuntimeLoggerHook) Log(ctx context.Context, level runtimediaglog.Level, message, component, action, eventID, eventType, agentID, entityID, sessionID string, correlation map[string]string, detail any, failure *runtimefailures.Envelope, durationUS int) error {
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
		Failure:     runtimefailures.CloneEnvelope(failure),
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

	logger := runtimepkg.NewRuntimeLogger(pg)
	bus, err := runtimebus.NewEventBusWithOptions(pg, runtimebus.EventBusOptions{
		Logger: conformanceRuntimeLoggerHook{logger: logger},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	pg.SetSessionLockTTL(30 * time.Second)
	registry := runtimesessions.Registry(pg)
	leaseCtx := runtimeeffects.WithDifferentOwner(ctx, runtimeeffects.OwnerBuildTestInfrastructure)
	identity := agentmemory.Identity{RunID: uuid.NewString(), AgentID: "agent-1", FlowInstance: "support/inst-1"}
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id) VALUES ($1::uuid)`, identity.RunID); err != nil {
		t.Fatalf("seed memory run: %v", err)
	}
	lease, err := registry.Acquire(leaseCtx, identity, "conformance")
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	am := runtimemanager.NewAgentManagerWithOptions(bus, nil, runtimemanager.AgentManagerOptions{
		Sessions: registry,
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
			Config:    runtimeactors.AgentConfig{ExecutionMode: "live", ID: "agent-a"},
			StartedAt: time.Now().UTC(),
		}},
		pending: map[string][]events.Event{
			"agent-a": {
				eventtest.PersistedProjection("evt-replay", events.EventType("support.replay.ok"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().Add(-4*time.Minute).UTC()),
				eventtest.PersistedProjection("evt-skip", events.EventType("support.replay.skip"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().Add(-3*time.Minute).UTC()),
				eventtest.PersistedProjection("evt-leased", events.EventType("support.replay.leased"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().Add(-2*time.Minute).UTC()),
				eventtest.PersistedProjection("evt-drop", events.EventType("support.replay.drop"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().Add(-time.Minute).UTC()),
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
			Backend: "anthropic",
		},
	}, Stores: runtimepkg.Stores{
		SQLDB:           db,
		PipelineStore:   runtimepipeline.NewWorkflowInstanceStore(db),
		EventStore:      conformanceTimerRecoveryEventStore{},
		RuntimeLogStore: pg,
		ManagerStore:    managerStore,
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

	droppedLeased := logs[findByEventID("evt-leased")]
	droppedLeasedDetail, _ := droppedLeased.Detail.(map[string]any)
	if got := readString(droppedLeasedDetail["decision_outcome"]); got != "dropped" {
		t.Fatalf("leased detail.decision_outcome = %q, want dropped without prose classification", got)
	}
	if got := readString(droppedLeasedDetail["decision_reason_code"]); got != "event_processing_failed" {
		t.Fatalf("leased detail.decision_reason_code = %q, want event_processing_failed", got)
	}
	if droppedLeased.Failure == nil || droppedLeased.Failure.Detail.Code != "unclassified_runtime_error" {
		t.Fatalf("leased failure = %#v, want generic internal failure", droppedLeased.Failure)
	}

	dropped := logs[findByEventID("evt-drop")]
	droppedDetail, _ := dropped.Detail.(map[string]any)
	if got := readString(droppedDetail["decision_outcome"]); got != "dropped" {
		t.Fatalf("dropped detail.decision_outcome = %q, want dropped", got)
	}
	if got := readString(droppedDetail["decision_reason_code"]); got != "event_processing_failed" {
		t.Fatalf("dropped detail.decision_reason_code = %q, want event_processing_failed", got)
	}
	if dropped.Failure == nil || dropped.Failure.Detail.Code != "unclassified_runtime_error" {
		t.Fatalf("dropped startup manager replay failure = %#v", dropped.Failure)
	}
}

func TestStartupPipelineReplayAftermathSurface_RoundTripsThroughObservabilityReader(t *testing.T) {
	ctx := context.Background()
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	pg := &store.PostgresStore{DB: db}

	requireCanonicalRuntimeLogSurface(t, ctx, pg)

	logger := runtimepkg.NewRuntimeLogger(pg)
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
	if err := pg.AppendEvent(ctx, eventtest.PersistedProjection(replayParentID,
		events.EventType("system.parent"),
		"runtime", "", []byte(`{"ok":true}`), 0, replayRunID, "", events.EventEnvelope{}, time.Now().Add(-3*time.Minute).UTC())); err != nil {
		t.Fatalf("AppendEvent(replay parent): %v", err)
	}
	if err := pg.AppendEvent(ctx, eventtest.PersistedProjection(replayChildID,
		events.EventType("system.recover.replay"),
		"runtime", "", []byte(`{"ok":true}`), 0, replayRunID,
		replayParentID, events.EventEnvelope{}, time.Now().Add(-2*time.Minute).UTC())); err != nil {
		t.Fatalf("AppendEvent(replay child): %v", err)
	}
	if err := pg.InsertEventDeliveries(ctx, replayChildID, []string{replayRecipient}); err != nil {
		t.Fatalf("InsertEventDeliveries(replay child): %v", err)
	}
	if err := pg.UpsertCommittedReplayScope(ctx, replayChildID, runtimereplayclaim.CommittedReplayScopeSubscribed); err != nil {
		t.Fatalf("UpsertCommittedReplayScope(replay child): %v", err)
	}
	if err := pg.UpsertPipelineReceipt(ctx, replayParentID, "processed", nil); err != nil {
		t.Fatalf("UpsertPipelineReceipt(replay parent): %v", err)
	}

	skipRunID := uuid.NewString()
	skipParentID := uuid.NewString()
	skipChildID := uuid.NewString()
	if err := pg.AppendEvent(ctx, eventtest.PersistedProjection(skipParentID,
		events.EventType("system.parent"),
		"runtime", "", []byte(`{"ok":true}`), 0, skipRunID, "", events.EventEnvelope{}, time.Now().Add(-3*time.Minute).UTC())); err != nil {
		t.Fatalf("AppendEvent(skip parent): %v", err)
	}
	if err := pg.AppendEvent(ctx, eventtest.PersistedProjection(skipChildID,
		events.EventType("system.recover.skip"),
		"runtime", "", []byte(`{"ok":true}`), 0, skipRunID,
		skipParentID, events.EventEnvelope{}, time.Now().Add(-2*time.Minute).UTC())); err != nil {
		t.Fatalf("AppendEvent(skip child): %v", err)
	}
	if err := pg.UpsertCommittedReplayScope(ctx, skipChildID, runtimereplayclaim.CommittedReplayScopeDirect); err != nil {
		t.Fatalf("UpsertCommittedReplayScope(skip child): %v", err)
	}
	if err := pg.UpsertPipelineReceipt(ctx, skipParentID, "processed", nil); err != nil {
		t.Fatalf("UpsertPipelineReceipt(skip parent): %v", err)
	}

	droppedEventID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (execution_mode,
			event_id, event_name, scope, payload, produced_by, produced_by_type, created_at
		) VALUES ('live',
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
	if dropped.Failure == nil {
		t.Fatal("dropped recovery aftermath log missing canonical failure")
	}
}

func TestCanonicalRuntimeLogTurnBlockSurface_IsOmittedFromPublicConversationProjection(t *testing.T) {
	ctx := runtimeeffects.WithExecutionMode(context.Background(), runtimeeffects.ExecutionModeLive)
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}

	requireCanonicalConversationSurface(t, ctx, pg)
	seedConformanceAgent(t, ctx, pg, "agent-1")

	runID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	sessionID := uuid.NewString()
	if err := pg.AppendAgentTurn(ctx, managedConformanceTurnRecord(t, runtimellm.AgentTurnRecord{
		AgentID:   "agent-1",
		Memory:    agentmemory.PlatformDefault(),
		RunID:     runID,
		SessionID: sessionID,
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
	})); err != nil {
		t.Fatalf("AppendAgentTurn(task runtime_log block): %v", err)
	}

	reader := dashboardserver.NewSQLConversationReader(db, pg)
	if reader == nil {
		t.Fatal("NewSQLConversationReader returned nil")
	}
	page, err := reader.ListOperatorConversationTurns(ctx, store.OperatorConversationTurnListOptions{SessionID: sessionID, Limit: 10})
	if err != nil {
		t.Fatalf("ListOperatorConversationTurns: %v", err)
	}
	if len(page.Turns) != 1 {
		t.Fatalf("conversation turns = %d, want 1", len(page.Turns))
	}
	detail, err := reader.LoadOperatorPublicConversationTurn(ctx, sessionID, page.Turns[0].TurnID)
	if err != nil {
		t.Fatalf("LoadOperatorPublicConversationTurn: %v", err)
	}
	turn := detail.Turn
	if len(turn.Activity) != 0 {
		t.Fatalf("public activity exposed runtime log block: %#v", turn.Activity)
	}
	raw, err := json.Marshal(turn)
	if err != nil {
		t.Fatalf("marshal public turn: %v", err)
	}
	for _, forbidden := range []string{"runtime_log", "tool-executor", "tool_execution_denied", "save_entity_field"} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatalf("public turn leaked private runtime evidence %q: %s", forbidden, raw)
		}
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
			ID:            agentID,
			Role:          "tester",
			FlowID:        "global",
			Type:          "stub",
			Model:         "regular",
			ExecutionMode: "live",
			Config:        []byte(`{"system_prompt":"x"}`),
		},
		Status:    "active",
		HiredBy:   "conformance-test",
		StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("UpsertAgent(%s): %v", agentID, err)
	}
}

func seedConformanceRunningAgent(t *testing.T, ctx context.Context, pg *store.PostgresStore, agentID string) runtimeeffects.LifecycleToken {
	t.Helper()
	seedConformanceAgent(t, ctx, pg, agentID)
	result, err := pg.CommitAgentLifecycleTransition(ctx, runtimemanager.AgentLifecycleTransition{
		OperationID:      uuid.NewString(),
		OperationKind:    "start",
		RequestHash:      "conformance-start-" + agentID,
		AgentID:          agentID,
		Trigger:          "conformance_test",
		ExpectedPhase:    runtimemanager.AgentLifecycleRegistered,
		TargetEpoch:      1,
		TargetGeneration: 1,
		TargetPhase:      runtimemanager.AgentLifecycleRunning,
		ConfigRevision:   "conformance-revision",
		RunMode:          runtimemanager.AgentRunModeStandard,
		Now:              time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("start agent lifecycle %s: %v", agentID, err)
	}
	return runtimeeffects.LifecycleToken{
		RuntimeEpoch: result.RuntimeEpoch,
		AgentID:      result.AgentID,
		Generation:   result.Generation,
	}
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
	pg := &store.PostgresStore{DB: db}
	exec := runtimetools.NewExecutorWithOptions(nil, nil, runtimetools.ExecutorOptions{
		EntityStore:                    pg,
		HumanTaskStore:                 pg,
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
		ExecutionMode: "live",
		ID:            "tester",
		Type:          "internal",
		Role:          "operator",
		Tools:         []string{"create_entity", "save_entity_field"},
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
