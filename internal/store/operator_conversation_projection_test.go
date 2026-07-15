package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/testutil"
)

type operatorConversationProjectionTestBackend struct {
	store interface {
		ListOperatorConversationTurns(context.Context, OperatorConversationTurnListOptions) (OperatorConversationTurnListResult, error)
		LoadOperatorPublicConversationTurn(context.Context, string, string) (OperatorPublicConversationTurnDetail, error)
	}
	owner  conversationForkStore
	db     *sql.DB
	sqlite bool
}

type operatorConversationProjectionFixture struct {
	sessionID          string
	malformedSessionID string
	turnIDs            []string
	sharedEventID      string
	firstAt            time.Time
	tieAt              time.Time
}

type operatorConversationProjectionParityResult struct {
	Pages          [][]OperatorConversationTurnListItem
	Exact          OperatorPublicConversationTurn
	Mixed          OperatorPublicConversationTurn
	TurnCoordinate ConversationForkPointDescriptor
	TimeCoordinate ConversationForkPointDescriptor
	AmbiguousEvent string
	BeforeHistory  string
	MalformedTurn  string
}

func TestOperatorConversationProjectionBackendParity(t *testing.T) {
	var sqliteResult operatorConversationProjectionParityResult
	for _, backendName := range []string{"sqlite", "postgres"} {
		t.Run(backendName, func(t *testing.T) {
			backend := newOperatorConversationProjectionTestBackend(t, backendName)
			fixture := seedOperatorConversationProjectionFixture(t, backend)
			result := proveOperatorConversationProjectionBackend(t, backend, fixture)
			if backendName == "sqlite" {
				sqliteResult = result
				return
			}
			if !reflect.DeepEqual(result, sqliteResult) {
				t.Fatalf("PostgreSQL projection differs from SQLite:\npostgres=%#v\nsqlite=%#v", result, sqliteResult)
			}
		})
	}
}

func newOperatorConversationProjectionTestBackend(t *testing.T, name string) operatorConversationProjectionTestBackend {
	t.Helper()
	switch name {
	case "sqlite":
		store := newBootstrappedSQLiteRuntimeStoreForTest(t)
		owner, err := sqliteConversationForkStore(store)
		if err != nil {
			t.Fatalf("sqlite conversation owner: %v", err)
		}
		return operatorConversationProjectionTestBackend{store: store, owner: owner, db: store.DB, sqlite: true}
	case "postgres":
		_, db, _ := testutil.StartPostgres(t)
		store := &PostgresStore{DB: db}
		owner, err := postgresConversationForkStore(store)
		if err != nil {
			t.Fatalf("postgres conversation owner: %v", err)
		}
		return operatorConversationProjectionTestBackend{store: store, owner: owner, db: db}
	default:
		t.Fatalf("unknown projection backend %q", name)
		return operatorConversationProjectionTestBackend{}
	}
}

func seedOperatorConversationProjectionFixture(t *testing.T, backend operatorConversationProjectionTestBackend) operatorConversationProjectionFixture {
	t.Helper()
	const (
		runID              = "00000000-0000-4000-8000-000000000001"
		sessionID          = "00000000-0000-4000-8000-000000000002"
		malformedSessionID = "00000000-0000-4000-8000-000000000003"
		agentID            = "agent-public-conversation-parity"
		malformedAgentID   = "agent-public-conversation-malformed"
		entityID           = "00000000-0000-4000-8000-000000000004"
		sharedEventID      = "00000000-0000-4000-8000-000000000005"
		publishEventID     = "00000000-0000-4000-8000-000000000006"
		malformedEventID   = "00000000-0000-4000-8000-000000000007"
	)
	turnIDs := []string{
		"00000000-0000-4000-8000-000000000011",
		"00000000-0000-4000-8000-000000000012",
		"00000000-0000-4000-8000-000000000013",
		"00000000-0000-4000-8000-000000000014",
		"00000000-0000-4000-8000-000000000015",
	}
	base := time.Date(2026, 7, 13, 16, 0, 0, 0, time.UTC)
	firstAt := base.Add(time.Minute)
	tieAt := base.Add(2 * time.Minute)
	lastAt := base.Add(3 * time.Minute)

	privateBlocks := `[
		{"kind":"reasoning","text":"private-reasoning"},
		{"kind":"progress","text":"private-progress"},
		{"kind":"runtime_log","data":{"message":"private-runtime-log","details":{"secret":"private-log-detail"}}},
		{"kind":"assistant_text","text":"author-visible-one"},
		{"kind":"unknown_private","text":"private-unknown"}
	]`
	toolBlocks := `[
		{"kind":"tool_use","tool_name":"inspect","input":{"secret":"private-tool-input"},"data":{"tool_use_id":"tool-use-2"}},
		{"kind":"tool_result","tool_name":"inspect","output":{"secret":"private-tool-output"},"data":{"tool_use_id":"tool-use-2"}},
		{"kind":"turn_summary","data":{"assistant_visible_output":"author-visible-two","outcome":"completed","reasoning_blocks":["private-summary-reasoning"],"progress_updates":["private-summary-progress"],"tool_results":[{"tool_name":"inspect","tool_use_id":"tool-use-2","output":{"secret":"private-summary-output"}}]}}
	]`
	mixedBlocks := `[
		{"kind":"dispatch","title":"task.done","data":{"trigger_event_id":"` + publishEventID + `","trigger_event_type":"task.done","entity_id":"` + entityID + `","task_id":"task-4"}},
		{"kind":"tool_use","tool_name":"deliver","input":{"secret":"private-mixed-input"},"data":{"tool_use_id":"tool-use-4"}},
		{"kind":"tool_result","tool_name":"deliver","output":{"secret":"private-mixed-output"},"data":{"tool_use_id":"tool-use-4"}},
		{"kind":"publish","title":"task.done","data":{"event_id":"` + publishEventID + `","entity_id":"` + entityID + `","routed_recipients":[{"subscriber_id":"private-recipient"}]}},
		{"kind":"assistant_text","text":"author-visible-mixed"},
		{"kind":"turn_summary","data":{"assistant_visible_output":"author-visible-mixed","outcome":"failed"}}
	]`
	mixedFailure := mustMarshalTestFailure(t, testFailureEnvelope(runtimefailures.ClassInternalFailure, "mixed_failure", nil))
	malformedBlocks := `[{"kind":"tool_use","input":{"secret":"private-malformed-input"}}]`
	capabilityStore, ok := backend.store.(managedCapabilityTestStore)
	if !ok {
		t.Fatal("operator conversation projection backend lacks managed capability persistence")
	}
	seedCapabilities := func() []string {
		out := make([]string, len(turnIDs))
		for i, turnID := range turnIDs {
			turnSessionID := sessionID
			turnAgentID := agentID
			if i == len(turnIDs)-1 {
				turnSessionID = malformedSessionID
				turnAgentID = malformedAgentID
			}
			out[i] = seedManagedAgentTurnCapabilitySurface(t, capabilityStore, runID, turnAgentID, turnSessionID, turnID, "session", "global")
		}
		return out
	}

	if backend.sqlite {
		operatorConversationProjectionExec(t, backend.db, `INSERT INTO runs (run_id, status, started_at) VALUES (?, 'running', ?)`, runID, base)
		capabilityIDs := seedCapabilities()
		operatorConversationProjectionExec(t, backend.db, `INSERT INTO agents (agent_id, flow_instance, role, model, memory_enabled, memory_source) VALUES (?, 'conversation', 'researcher', 'cheap', 1, 'authored'), (?, 'conversation-malformed', 'researcher', 'cheap', 1, 'authored')`, agentID, malformedAgentID)
		operatorConversationProjectionExec(t, backend.db, `INSERT INTO agent_sessions (session_id, run_id, agent_id, flow_instance, memory_enabled, memory_source, status, turn_count, conversation, runtime_state, created_at, updated_at) VALUES (?, ?, ?, 'conversation', 1, 'authored', 'active', 4, '[]', '{}', ?, ?)`, sessionID, runID, agentID, base, lastAt)
		operatorConversationProjectionExec(t, backend.db, `INSERT INTO agent_sessions (session_id, run_id, agent_id, flow_instance, memory_enabled, memory_source, status, turn_count, conversation, runtime_state, created_at, updated_at) VALUES (?, ?, ?, 'conversation-malformed', 1, 'authored', 'active', 1, '[]', '{}', ?, ?)`, malformedSessionID, runID, malformedAgentID, base, lastAt)
		operatorConversationProjectionExec(t, backend.db, `INSERT INTO agent_turns (turn_id, run_id, agent_id, session_id, flow_instance, memory_enabled, memory_source, entity_id, trigger_event_id, trigger_event_type, task_id, capability_surface_id, turn_blocks, parse_ok, latency_ms, retry_count, execution_mode, created_at) VALUES (?, ?, ?, ?, 'conversation', 1, 'authored', ?, ?, 'task.one', 'task-1', ?, ?, 1, 101, 0, 'live', ?)`, turnIDs[0], runID, agentID, sessionID, entityID, turnIDs[0], capabilityIDs[0], privateBlocks, firstAt)
		operatorConversationProjectionExec(t, backend.db, `INSERT INTO agent_turns (turn_id, run_id, agent_id, session_id, flow_instance, memory_enabled, memory_source, entity_id, trigger_event_id, trigger_event_type, task_id, capability_surface_id, turn_blocks, parse_ok, latency_ms, retry_count, usage_exactness, input_tokens, output_tokens, execution_mode, created_at) VALUES (?, ?, ?, ?, 'conversation', 1, 'authored', ?, ?, 'task.shared', 'task-2', ?, ?, 1, 202, 1, 'exact', 12, 4, 'live', ?)`, turnIDs[1], runID, agentID, sessionID, entityID, sharedEventID, capabilityIDs[1], toolBlocks, tieAt)
		operatorConversationProjectionExec(t, backend.db, `INSERT INTO agent_turns (turn_id, run_id, agent_id, session_id, flow_instance, memory_enabled, memory_source, entity_id, trigger_event_id, trigger_event_type, task_id, capability_surface_id, turn_blocks, parse_ok, latency_ms, retry_count, usage_exactness, execution_mode, created_at) VALUES (?, ?, ?, ?, 'conversation', 1, 'authored', ?, ?, 'task.shared', 'task-3', ?, '[]', 1, 303, 0, 'unavailable', 'live', ?)`, turnIDs[2], runID, agentID, sessionID, entityID, sharedEventID, capabilityIDs[2], tieAt)
		operatorConversationProjectionExec(t, backend.db, `INSERT INTO agent_turns (turn_id, run_id, agent_id, session_id, flow_instance, memory_enabled, memory_source, entity_id, trigger_event_id, trigger_event_type, task_id, capability_surface_id, turn_blocks, parse_ok, latency_ms, retry_count, failure, execution_mode, created_at) VALUES (?, ?, ?, ?, 'conversation', 1, 'authored', ?, ?, 'task.done', 'task-4', ?, ?, 0, 404, 0, ?, 'live', ?)`, turnIDs[3], runID, agentID, sessionID, entityID, publishEventID, capabilityIDs[3], mixedBlocks, mixedFailure, lastAt)
		operatorConversationProjectionExec(t, backend.db, `INSERT INTO agent_turns (turn_id, run_id, agent_id, session_id, flow_instance, memory_enabled, memory_source, trigger_event_id, trigger_event_type, task_id, capability_surface_id, turn_blocks, parse_ok, latency_ms, retry_count, execution_mode, created_at) VALUES (?, ?, ?, ?, 'conversation-malformed', 1, 'authored', ?, 'task.malformed', 'task-malformed', ?, ?, 1, 1, 0, 'live', ?)`, turnIDs[4], runID, malformedAgentID, malformedSessionID, malformedEventID, capabilityIDs[4], malformedBlocks, lastAt)
	} else {
		operatorConversationProjectionExec(t, backend.db, `INSERT INTO runs (run_id, status, started_at) VALUES ($1::uuid, 'running', $2)`, runID, base)
		capabilityIDs := seedCapabilities()
		operatorConversationProjectionExec(t, backend.db, `INSERT INTO agents (agent_id, flow_instance, role, model, memory_enabled, memory_source) VALUES ($1, 'conversation', 'researcher', 'cheap', TRUE, 'authored'), ($2, 'conversation-malformed', 'researcher', 'cheap', TRUE, 'authored')`, agentID, malformedAgentID)
		operatorConversationProjectionExec(t, backend.db, `INSERT INTO agent_sessions (session_id, run_id, agent_id, flow_instance, memory_enabled, memory_source, status, turn_count, conversation, runtime_state, created_at, updated_at) VALUES ($1::uuid, $2::uuid, $3, 'conversation', TRUE, 'authored', 'active', 4, '[]'::jsonb, '{}'::jsonb, $4, $5)`, sessionID, runID, agentID, base, lastAt)
		operatorConversationProjectionExec(t, backend.db, `INSERT INTO agent_sessions (session_id, run_id, agent_id, flow_instance, memory_enabled, memory_source, status, turn_count, conversation, runtime_state, created_at, updated_at) VALUES ($1::uuid, $2::uuid, $3, 'conversation-malformed', TRUE, 'authored', 'active', 1, '[]'::jsonb, '{}'::jsonb, $4, $5)`, malformedSessionID, runID, malformedAgentID, base, lastAt)
		operatorConversationProjectionExec(t, backend.db, `INSERT INTO agent_turns (turn_id, run_id, agent_id, session_id, flow_instance, memory_enabled, memory_source, entity_id, trigger_event_id, trigger_event_type, task_id, capability_surface_id, turn_blocks, parse_ok, latency_ms, retry_count, execution_mode, created_at) VALUES ($1::uuid, $2::uuid, $3, $4::uuid, 'conversation', TRUE, 'authored', $5::uuid, $6::uuid, 'task.one', 'task-1', $7::uuid, $8::jsonb, true, 101, 0, 'live', $9)`, turnIDs[0], runID, agentID, sessionID, entityID, turnIDs[0], capabilityIDs[0], privateBlocks, firstAt)
		operatorConversationProjectionExec(t, backend.db, `INSERT INTO agent_turns (turn_id, run_id, agent_id, session_id, flow_instance, memory_enabled, memory_source, entity_id, trigger_event_id, trigger_event_type, task_id, capability_surface_id, turn_blocks, parse_ok, latency_ms, retry_count, usage_exactness, input_tokens, output_tokens, execution_mode, created_at) VALUES ($1::uuid, $2::uuid, $3, $4::uuid, 'conversation', TRUE, 'authored', $5::uuid, $6::uuid, 'task.shared', 'task-2', $7::uuid, $8::jsonb, true, 202, 1, 'exact', 12, 4, 'live', $9)`, turnIDs[1], runID, agentID, sessionID, entityID, sharedEventID, capabilityIDs[1], toolBlocks, tieAt)
		operatorConversationProjectionExec(t, backend.db, `INSERT INTO agent_turns (turn_id, run_id, agent_id, session_id, flow_instance, memory_enabled, memory_source, entity_id, trigger_event_id, trigger_event_type, task_id, capability_surface_id, turn_blocks, parse_ok, latency_ms, retry_count, usage_exactness, execution_mode, created_at) VALUES ($1::uuid, $2::uuid, $3, $4::uuid, 'conversation', TRUE, 'authored', $5::uuid, $6::uuid, 'task.shared', 'task-3', $7::uuid, '[]'::jsonb, true, 303, 0, 'unavailable', 'live', $8)`, turnIDs[2], runID, agentID, sessionID, entityID, sharedEventID, capabilityIDs[2], tieAt)
		operatorConversationProjectionExec(t, backend.db, `INSERT INTO agent_turns (turn_id, run_id, agent_id, session_id, flow_instance, memory_enabled, memory_source, entity_id, trigger_event_id, trigger_event_type, task_id, capability_surface_id, turn_blocks, parse_ok, latency_ms, retry_count, failure, execution_mode, created_at) VALUES ($1::uuid, $2::uuid, $3, $4::uuid, 'conversation', TRUE, 'authored', $5::uuid, $6::uuid, 'task.done', 'task-4', $7::uuid, $8::jsonb, false, 404, 0, $9::jsonb, 'live', $10)`, turnIDs[3], runID, agentID, sessionID, entityID, publishEventID, capabilityIDs[3], mixedBlocks, mixedFailure, lastAt)
		operatorConversationProjectionExec(t, backend.db, `INSERT INTO agent_turns (turn_id, run_id, agent_id, session_id, flow_instance, memory_enabled, memory_source, trigger_event_id, trigger_event_type, task_id, capability_surface_id, turn_blocks, parse_ok, latency_ms, retry_count, execution_mode, created_at) VALUES ($1::uuid, $2::uuid, $3, $4::uuid, 'conversation-malformed', TRUE, 'authored', $5::uuid, 'task.malformed', 'task-malformed', $6::uuid, $7::jsonb, true, 1, 0, 'live', $8)`, turnIDs[4], runID, malformedAgentID, malformedSessionID, malformedEventID, capabilityIDs[4], malformedBlocks, lastAt)
	}

	return operatorConversationProjectionFixture{
		sessionID:          sessionID,
		malformedSessionID: malformedSessionID,
		turnIDs:            turnIDs,
		sharedEventID:      sharedEventID,
		firstAt:            firstAt,
		tieAt:              tieAt,
	}
}

func operatorConversationProjectionExec(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(), query, args...); err != nil {
		t.Fatalf("seed operator conversation projection: %v\nquery: %s", err, query)
	}
}

func proveOperatorConversationProjectionBackend(t *testing.T, backend operatorConversationProjectionTestBackend, fixture operatorConversationProjectionFixture) operatorConversationProjectionParityResult {
	t.Helper()
	ctx := context.Background()
	first, err := backend.store.ListOperatorConversationTurns(ctx, OperatorConversationTurnListOptions{SessionID: fixture.sessionID, Limit: 2})
	if err != nil {
		t.Fatalf("list first turn page: %v", err)
	}
	if len(first.Turns) != 2 || first.NextCursor == "" || first.Turns[0].TurnID != fixture.turnIDs[3] || first.Turns[0].Ordinal != 4 || first.Turns[1].TurnID != fixture.turnIDs[2] || first.Turns[1].Ordinal != 3 {
		t.Fatalf("first turn page = %#v", first)
	}
	if first.Turns[0].ActivityCounts != (OperatorConversationActivityCounts{Dispatch: 1, Tool: 1, ToolResult: 1, Publish: 1, Output: 1, Failure: 1}) {
		t.Fatalf("mixed compact activity counts = %#v", first.Turns[0].ActivityCounts)
	}
	second, err := backend.store.ListOperatorConversationTurns(ctx, OperatorConversationTurnListOptions{SessionID: fixture.sessionID, Limit: 2, Cursor: first.NextCursor})
	if err != nil {
		t.Fatalf("list second turn page: %v", err)
	}
	if len(second.Turns) != 2 || second.NextCursor != "" || second.Turns[0].TurnID != fixture.turnIDs[1] || second.Turns[0].Ordinal != 2 || second.Turns[1].TurnID != fixture.turnIDs[0] || second.Turns[1].Ordinal != 1 {
		t.Fatalf("second turn page = %#v", second)
	}
	if second.Turns[0].Tokens == nil || second.Turns[0].Tokens.Exactness != "exact" || second.Turns[0].Tokens.Input != 12 || second.Turns[0].Tokens.Output != 4 {
		t.Fatalf("exact token fact = %#v", second.Turns[0].Tokens)
	}
	if first.Turns[1].Tokens != nil || second.Turns[1].Tokens != nil {
		t.Fatalf("nullable/unavailable token facts leaked values: first=%#v second=%#v", first.Turns[1].Tokens, second.Turns[1].Tokens)
	}
	seen := map[string]bool{}
	for _, page := range [][]OperatorConversationTurnListItem{first.Turns, second.Turns} {
		for _, turn := range page {
			if seen[turn.TurnID] {
				t.Fatalf("turn %s repeated across cursor pages", turn.TurnID)
			}
			seen[turn.TurnID] = true
		}
	}
	if len(seen) != 4 {
		t.Fatalf("cursor pages covered %d turns, want 4", len(seen))
	}
	listJSON, err := json.Marshal([]any{first.Turns, second.Turns})
	if err != nil {
		t.Fatalf("marshal compact turn pages: %v", err)
	}
	for _, forbidden := range []string{`"activity":`, `"assistant_visible_output":`, `"entity_id":`, `"task_id":`, `"retry_count":`, "author-visible-mixed", "private-mixed"} {
		if strings.Contains(string(listJSON), forbidden) {
			t.Fatalf("compact list leaked %q: %s", forbidden, listJSON)
		}
	}

	detail, err := backend.store.LoadOperatorPublicConversationTurn(ctx, fixture.sessionID, fixture.turnIDs[1])
	if err != nil {
		t.Fatalf("load exact turn: %v", err)
	}
	if detail.Turn.TurnID != fixture.turnIDs[1] || detail.Turn.Ordinal != 2 || detail.Turn.AssistantVisibleOutput != "author-visible-two" || detail.Turn.Outcome != "completed" || len(detail.Turn.Activity) != 2 {
		t.Fatalf("exact turn detail = %#v", detail.Turn)
	}
	mixed, err := backend.store.LoadOperatorPublicConversationTurn(ctx, fixture.sessionID, fixture.turnIDs[3])
	if err != nil {
		t.Fatalf("load mixed turn: %v", err)
	}
	wantMixedKinds := []string{"dispatch", "tool", "tool_result", "publish", "output", "failure"}
	gotMixedKinds := make([]string, 0, len(mixed.Turn.Activity))
	for _, item := range mixed.Turn.Activity {
		gotMixedKinds = append(gotMixedKinds, item.Kind)
	}
	if !reflect.DeepEqual(gotMixedKinds, wantMixedKinds) || mixed.Turn.AssistantVisibleOutput != "author-visible-mixed" || mixed.Turn.Outcome != "failed" || mixed.Turn.Failure == nil {
		t.Fatalf("mixed turn detail = %#v, kinds=%v", mixed.Turn, gotMixedKinds)
	}
	projectionJSON, err := json.Marshal([]any{first.Turns, second.Turns, detail.Turn, mixed.Turn})
	if err != nil {
		t.Fatalf("marshal public turn projection: %v", err)
	}
	if strings.Contains(string(projectionJSON), "private-") || strings.Contains(string(projectionJSON), "request_payload") || strings.Contains(string(projectionJSON), "response_payload") {
		t.Fatalf("public turn projection leaked private evidence: %s", projectionJSON)
	}

	turnCoordinate, err := backend.owner.resolveConversationForkPoint(ctx, fixture.sessionID, ConversationForkPointSelector{Kind: "turn", TurnID: fixture.turnIDs[1]})
	if err != nil || turnCoordinate.TurnIndex != 2 {
		t.Fatalf("turn coordinate = %#v, err=%v", turnCoordinate, err)
	}
	tieAt := fixture.tieAt
	timeCoordinate, err := backend.owner.resolveConversationForkPoint(ctx, fixture.sessionID, ConversationForkPointSelector{Kind: "time", At: &tieAt})
	if err != nil || timeCoordinate.TurnIndex != 3 || timeCoordinate.TurnID != fixture.turnIDs[2] {
		t.Fatalf("time coordinate = %#v, err=%v", timeCoordinate, err)
	}
	_, ambiguousErr := backend.owner.resolveConversationForkPoint(ctx, fixture.sessionID, ConversationForkPointSelector{Kind: "event", EventID: fixture.sharedEventID})
	if ambiguousErr == nil || !strings.Contains(ambiguousErr.Error(), "event matches multiple source turns") {
		t.Fatalf("ambiguous event error = %v", ambiguousErr)
	}
	before := fixture.firstAt.Add(-time.Millisecond)
	_, beforeErr := backend.owner.resolveConversationForkPoint(ctx, fixture.sessionID, ConversationForkPointSelector{Kind: "time", At: &before})
	if beforeErr == nil || !strings.Contains(beforeErr.Error(), "does not select a source turn") {
		t.Fatalf("before-history error = %v", beforeErr)
	}
	_, malformedErr := backend.store.ListOperatorConversationTurns(ctx, OperatorConversationTurnListOptions{SessionID: fixture.malformedSessionID})
	if malformedErr == nil || !strings.Contains(malformedErr.Error(), "tool_name is required") {
		t.Fatalf("malformed public turn error = %v", malformedErr)
	}

	return operatorConversationProjectionParityResult{
		Pages:          [][]OperatorConversationTurnListItem{first.Turns, second.Turns},
		Exact:          detail.Turn,
		Mixed:          mixed.Turn,
		TurnCoordinate: turnCoordinate,
		TimeCoordinate: timeCoordinate,
		AmbiguousEvent: ambiguousErr.Error(),
		BeforeHistory:  beforeErr.Error(),
		MalformedTurn:  malformedErr.Error(),
	}
}
