package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/testutil"
)

type operatorConversationProjectionTestBackend struct {
	store interface {
		ListOperatorConversationTurns(context.Context, operatorread.OperatorConversationTurnListOptions) (operatorread.OperatorConversationTurnListResult, error)
		LoadOperatorPublicConversationTurn(context.Context, string, string) (operatorread.OperatorPublicConversationTurnDetail, error)
	}
	owner interface {
		ResolveConversationForkPoint(context.Context, string, runfork.ConversationForkPointSelector) (runfork.ConversationForkPointDescriptor, error)
	}
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
	Pages          [][]operatorread.OperatorConversationTurnListItem
	Exact          operatorread.OperatorPublicConversationTurn
	Mixed          operatorread.OperatorPublicConversationTurn
	TurnCoordinate runfork.ConversationForkPointDescriptor
	TimeCoordinate runfork.ConversationForkPointDescriptor
	AmbiguousEvent string
	BeforeHistory  string
	MalformedTurn  string
}

type operatorConversationProjectionTurnSeed struct {
	identity       agentidentity.Identity
	turnID         string
	runID          string
	sessionID      string
	entityID       string
	triggerEventID string
	triggerType    string
	taskID         string
	capabilityID   string
	turnBlocks     string
	parseOK        bool
	latencyMS      int
	retryCount     int
	usageExactness string
	inputTokens    any
	outputTokens   any
	failure        string
	createdAt      time.Time
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
		return operatorConversationProjectionTestBackend{store: store, owner: store, db: store.backend.ConstructionHandle(), sqlite: true}
	case "postgres":
		_, db, _ := testutil.StartPostgres(t)
		store := admitTestPostgresStore(t, db)
		return operatorConversationProjectionTestBackend{store: store, owner: store, db: db}
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
	identity := testAgentIdentity(t, agentID, "conversation")
	malformedIdentity := testAgentIdentity(t, malformedAgentID, "conversation-malformed")
	capabilityStore, ok := backend.store.(managedCapabilityTestStore)
	if !ok {
		t.Fatal("operator conversation projection backend lacks managed capability persistence")
	}
	seedCapabilities := func() []string {
		out := make([]string, len(turnIDs))
		for i, turnID := range turnIDs {
			turnSessionID := sessionID
			turnIdentity := identity
			if i == len(turnIDs)-1 {
				turnSessionID = malformedSessionID
				turnIdentity = malformedIdentity
			}
			out[i] = seedManagedAgentTurnCapabilitySurface(
				t, capabilityStore, runID, turnIdentity, turnSessionID, turnID, "session", "global",
			)
		}
		return out
	}

	requireRunFixtureForTest(t, context.Background(), backend.store, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(),
		RunID: runID, StartedAt: base,
	})
	if backend.sqlite {
		capabilityIDs := seedCapabilities()
		seedTestAgentRow(t, testAuthorActivityContext(), backend.db, false, identity, "active")
		seedTestAgentRow(t, testAuthorActivityContext(), backend.db, false, malformedIdentity, "active")
		seedOperatorConversationProjectionSession(t, backend, runID, sessionID, identity, 4, base, lastAt)
		seedOperatorConversationProjectionSession(t, backend, runID, malformedSessionID, malformedIdentity, 1, base, lastAt)
		seedOperatorConversationProjectionTurn(t, backend, operatorConversationProjectionTurnSeed{identity: identity, turnID: turnIDs[0], runID: runID, sessionID: sessionID, entityID: entityID, triggerEventID: turnIDs[0], triggerType: "task.one", taskID: "task-1", capabilityID: capabilityIDs[0], turnBlocks: privateBlocks, parseOK: true, latencyMS: 101, createdAt: firstAt})
		seedOperatorConversationProjectionTurn(t, backend, operatorConversationProjectionTurnSeed{identity: identity, turnID: turnIDs[1], runID: runID, sessionID: sessionID, entityID: entityID, triggerEventID: sharedEventID, triggerType: "task.shared", taskID: "task-2", capabilityID: capabilityIDs[1], turnBlocks: toolBlocks, parseOK: true, latencyMS: 202, retryCount: 1, usageExactness: "exact", inputTokens: 12, outputTokens: 4, createdAt: tieAt})
		seedOperatorConversationProjectionTurn(t, backend, operatorConversationProjectionTurnSeed{identity: identity, turnID: turnIDs[2], runID: runID, sessionID: sessionID, entityID: entityID, triggerEventID: sharedEventID, triggerType: "task.shared", taskID: "task-3", capabilityID: capabilityIDs[2], turnBlocks: "[]", parseOK: true, latencyMS: 303, usageExactness: "unavailable", createdAt: tieAt})
		seedOperatorConversationProjectionTurn(t, backend, operatorConversationProjectionTurnSeed{identity: identity, turnID: turnIDs[3], runID: runID, sessionID: sessionID, entityID: entityID, triggerEventID: publishEventID, triggerType: "task.done", taskID: "task-4", capabilityID: capabilityIDs[3], turnBlocks: mixedBlocks, latencyMS: 404, failure: mixedFailure, createdAt: lastAt})
		seedOperatorConversationProjectionTurn(t, backend, operatorConversationProjectionTurnSeed{identity: malformedIdentity, turnID: turnIDs[4], runID: runID, sessionID: malformedSessionID, triggerEventID: malformedEventID, triggerType: "task.malformed", taskID: "task-malformed", capabilityID: capabilityIDs[4], turnBlocks: malformedBlocks, parseOK: true, latencyMS: 1, createdAt: lastAt})
	} else {
		capabilityIDs := seedCapabilities()
		seedTestAgentRow(t, testAuthorActivityContext(), backend.db, true, identity, "active")
		seedTestAgentRow(t, testAuthorActivityContext(), backend.db, true, malformedIdentity, "active")
		seedOperatorConversationProjectionSession(t, backend, runID, sessionID, identity, 4, base, lastAt)
		seedOperatorConversationProjectionSession(t, backend, runID, malformedSessionID, malformedIdentity, 1, base, lastAt)
		seedOperatorConversationProjectionTurn(t, backend, operatorConversationProjectionTurnSeed{identity: identity, turnID: turnIDs[0], runID: runID, sessionID: sessionID, entityID: entityID, triggerEventID: turnIDs[0], triggerType: "task.one", taskID: "task-1", capabilityID: capabilityIDs[0], turnBlocks: privateBlocks, parseOK: true, latencyMS: 101, createdAt: firstAt})
		seedOperatorConversationProjectionTurn(t, backend, operatorConversationProjectionTurnSeed{identity: identity, turnID: turnIDs[1], runID: runID, sessionID: sessionID, entityID: entityID, triggerEventID: sharedEventID, triggerType: "task.shared", taskID: "task-2", capabilityID: capabilityIDs[1], turnBlocks: toolBlocks, parseOK: true, latencyMS: 202, retryCount: 1, usageExactness: "exact", inputTokens: 12, outputTokens: 4, createdAt: tieAt})
		seedOperatorConversationProjectionTurn(t, backend, operatorConversationProjectionTurnSeed{identity: identity, turnID: turnIDs[2], runID: runID, sessionID: sessionID, entityID: entityID, triggerEventID: sharedEventID, triggerType: "task.shared", taskID: "task-3", capabilityID: capabilityIDs[2], turnBlocks: "[]", parseOK: true, latencyMS: 303, usageExactness: "unavailable", createdAt: tieAt})
		seedOperatorConversationProjectionTurn(t, backend, operatorConversationProjectionTurnSeed{identity: identity, turnID: turnIDs[3], runID: runID, sessionID: sessionID, entityID: entityID, triggerEventID: publishEventID, triggerType: "task.done", taskID: "task-4", capabilityID: capabilityIDs[3], turnBlocks: mixedBlocks, latencyMS: 404, failure: mixedFailure, createdAt: lastAt})
		seedOperatorConversationProjectionTurn(t, backend, operatorConversationProjectionTurnSeed{identity: malformedIdentity, turnID: turnIDs[4], runID: runID, sessionID: malformedSessionID, triggerEventID: malformedEventID, triggerType: "task.malformed", taskID: "task-malformed", capabilityID: capabilityIDs[4], turnBlocks: malformedBlocks, parseOK: true, latencyMS: 1, createdAt: lastAt})
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
	if _, err := db.ExecContext(testAuthorActivityContext(), query, args...); err != nil {
		t.Fatalf("seed operator conversation projection: %v\nquery: %s", err, query)
	}
}

func seedOperatorConversationProjectionSession(
	t *testing.T,
	backend operatorConversationProjectionTestBackend,
	runID, sessionID string,
	identity agentidentity.Identity,
	turnCount int,
	createdAt, updatedAt time.Time,
) {
	t.Helper()
	fields, err := identity.StorageFields()
	if err != nil {
		t.Fatalf("seed operator conversation session identity: %v", err)
	}
	query := `
		INSERT INTO agent_sessions (
			session_id, run_id, agent_id, agent_name_owner, agent_name_source,
			agent_route_presence, flow_scope_key, flow_instance_id, flow_instance,
			memory_enabled, memory_source, status, turn_count, conversation, runtime_state,
			created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 'authored', 'active', ?, '[]', '{}', ?, ?)
	`
	if !backend.sqlite {
		query = `
			INSERT INTO agent_sessions (
				session_id, run_id, agent_id, agent_name_owner, agent_name_source,
				agent_route_presence, flow_scope_key, flow_instance_id, flow_instance,
				memory_enabled, memory_source, status, turn_count, conversation, runtime_state,
				created_at, updated_at
			) VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8, $9, TRUE, 'authored', 'active', $10, '[]'::jsonb, '{}'::jsonb, $11, $12)
		`
	}
	operatorConversationProjectionExec(
		t,
		backend.db,
		query,
		sessionID,
		runID,
		fields.AgentID,
		fields.NameOwner,
		fields.NameSource,
		fields.RoutePresence,
		fields.FlowScopeKey,
		fields.FlowInstanceID,
		fields.FlowInstancePath,
		turnCount,
		createdAt,
		updatedAt,
	)
}

func seedOperatorConversationProjectionTurn(
	t *testing.T,
	backend operatorConversationProjectionTestBackend,
	seed operatorConversationProjectionTurnSeed,
) {
	t.Helper()
	fields, err := seed.identity.StorageFields()
	if err != nil {
		t.Fatalf("seed operator conversation turn identity: %v", err)
	}
	entityID := any(sqliteNullUUID(seed.entityID))
	usageExactness := any(sqliteNullString(seed.usageExactness))
	failure := any(sqliteNullString(seed.failure))
	query := `
		INSERT INTO agent_turns (
			turn_id, run_id, agent_id, agent_name_owner, agent_name_source,
			agent_route_presence, flow_scope_key, flow_instance_id,
			session_id, flow_instance, memory_enabled, memory_source,
			entity_id, trigger_event_id, trigger_event_type, task_id,
			capability_surface_id, turn_blocks, parse_ok, latency_ms, retry_count,
			usage_exactness, input_tokens, output_tokens, failure, execution_mode, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 'authored', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'live', ?)
	`
	if !backend.sqlite {
		query = `
			INSERT INTO agent_turns (
				turn_id, run_id, agent_id, agent_name_owner, agent_name_source,
				agent_route_presence, flow_scope_key, flow_instance_id,
				session_id, flow_instance, memory_enabled, memory_source,
				entity_id, trigger_event_id, trigger_event_type, task_id,
				capability_surface_id, turn_blocks, parse_ok, latency_ms, retry_count,
				usage_exactness, input_tokens, output_tokens, failure, execution_mode, created_at
			) VALUES (
				$1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8,
				$9::uuid, $10, TRUE, 'authored',
				$11::uuid, $12::uuid, $13, $14, $15::uuid, $16::jsonb,
				$17, $18, $19, $20, $21, $22, $23::jsonb, 'live', $24
			)
		`
		entityID = sqliteNullUUID(seed.entityID)
		usageExactness = sqliteNullString(seed.usageExactness)
		failure = sqliteNullString(seed.failure)
	}
	operatorConversationProjectionExec(
		t,
		backend.db,
		query,
		seed.turnID,
		seed.runID,
		fields.AgentID,
		fields.NameOwner,
		fields.NameSource,
		fields.RoutePresence,
		fields.FlowScopeKey,
		fields.FlowInstanceID,
		seed.sessionID,
		fields.FlowInstancePath,
		entityID,
		seed.triggerEventID,
		seed.triggerType,
		seed.taskID,
		seed.capabilityID,
		seed.turnBlocks,
		seed.parseOK,
		seed.latencyMS,
		seed.retryCount,
		usageExactness,
		seed.inputTokens,
		seed.outputTokens,
		failure,
		seed.createdAt,
	)
}

func proveOperatorConversationProjectionBackend(t *testing.T, backend operatorConversationProjectionTestBackend, fixture operatorConversationProjectionFixture) operatorConversationProjectionParityResult {
	t.Helper()
	ctx := testAuthorActivityContext()
	first, err := backend.store.ListOperatorConversationTurns(ctx, operatorread.OperatorConversationTurnListOptions{SessionID: fixture.sessionID, Limit: 2})
	if err != nil {
		t.Fatalf("list first turn page: %v", err)
	}
	if len(first.Turns) != 2 || first.NextCursor == "" || first.Turns[0].TurnID != fixture.turnIDs[3] || first.Turns[0].Ordinal != 4 || first.Turns[1].TurnID != fixture.turnIDs[2] || first.Turns[1].Ordinal != 3 {
		t.Fatalf("first turn page = %#v", first)
	}
	if first.Turns[0].ActivityCounts != (operatorread.OperatorConversationActivityCounts{Dispatch: 1, Tool: 1, ToolResult: 1, Publish: 1, Output: 1, Failure: 1}) {
		t.Fatalf("mixed compact activity counts = %#v", first.Turns[0].ActivityCounts)
	}
	second, err := backend.store.ListOperatorConversationTurns(ctx, operatorread.OperatorConversationTurnListOptions{SessionID: fixture.sessionID, Limit: 2, Cursor: first.NextCursor})
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
	for _, page := range [][]operatorread.OperatorConversationTurnListItem{first.Turns, second.Turns} {
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

	turnCoordinate, err := backend.owner.ResolveConversationForkPoint(ctx, fixture.sessionID, runfork.ConversationForkPointSelector{Kind: "turn", TurnID: fixture.turnIDs[1]})
	if err != nil || turnCoordinate.TurnIndex != 2 {
		t.Fatalf("turn coordinate = %#v, err=%v", turnCoordinate, err)
	}
	tieAt := fixture.tieAt
	timeCoordinate, err := backend.owner.ResolveConversationForkPoint(ctx, fixture.sessionID, runfork.ConversationForkPointSelector{Kind: "time", At: &tieAt})
	if err != nil || timeCoordinate.TurnIndex != 3 || timeCoordinate.TurnID != fixture.turnIDs[2] {
		t.Fatalf("time coordinate = %#v, err=%v", timeCoordinate, err)
	}
	_, ambiguousErr := backend.owner.ResolveConversationForkPoint(ctx, fixture.sessionID, runfork.ConversationForkPointSelector{Kind: "event", EventID: fixture.sharedEventID})
	if ambiguousErr == nil || !strings.Contains(ambiguousErr.Error(), "event matches multiple source turns") {
		t.Fatalf("ambiguous event error = %v", ambiguousErr)
	}
	before := fixture.firstAt.Add(-time.Millisecond)
	_, beforeErr := backend.owner.ResolveConversationForkPoint(ctx, fixture.sessionID, runfork.ConversationForkPointSelector{Kind: "time", At: &before})
	if beforeErr == nil || !strings.Contains(beforeErr.Error(), "does not select a source turn") {
		t.Fatalf("before-history error = %v", beforeErr)
	}
	_, malformedErr := backend.store.ListOperatorConversationTurns(ctx, operatorread.OperatorConversationTurnListOptions{SessionID: fixture.malformedSessionID})
	if malformedErr == nil || !strings.Contains(malformedErr.Error(), "tool_name is required") {
		t.Fatalf("malformed public turn error = %v", malformedErr)
	}

	return operatorConversationProjectionParityResult{
		Pages:          [][]operatorread.OperatorConversationTurnListItem{first.Turns, second.Turns},
		Exact:          detail.Turn,
		Mixed:          mixed.Turn,
		TurnCoordinate: turnCoordinate,
		TimeCoordinate: timeCoordinate,
		AmbiguousEvent: ambiguousErr.Error(),
		BeforeHistory:  beforeErr.Error(),
		MalformedTurn:  malformedErr.Error(),
	}
}
