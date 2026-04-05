package conformance

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	dashboardserver "swarm/internal/dashboard/server"
	runtimepkg "swarm/internal/runtime"
	runtimecontracts "swarm/internal/runtime/contracts"
	runtimeactors "swarm/internal/runtime/core/actors"
	runtimellm "swarm/internal/runtime/llm"
	runtimemanager "swarm/internal/runtime/manager"
	runtimepipeline "swarm/internal/runtime/pipeline"
	runtimesemanticview "swarm/internal/runtime/semanticview"
	runtimetools "swarm/internal/runtime/tools"
	"swarm/internal/store"
	"swarm/internal/testutil"
)

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
			{Kind: "tool_use", ToolName: "schedule", Input: map[string]any{"delay_seconds": 1209600}, Data: map[string]any{"tool_use_id": "toolu_1"}},
			{Kind: "tool_result", Output: map[string]any{"status": "scheduled"}, Data: map[string]any{"tool_use_id": "toolu_1"}},
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
	result, _ := turn.ToolResults[0].(map[string]any)
	if strings.TrimSpace(readString(result["tool_name"])) != "schedule" {
		t.Fatalf("tool_result.tool_name = %#v, want schedule", result["tool_name"])
	}
}

func TestCanonicalRuntimeLogSurface_RoundTripsThroughObservabilityReader(t *testing.T) {
	ctx := context.Background()
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}

	requireCanonicalRuntimeLogSurface(t, ctx, pg)

	entityID := uuid.NewString()
	parentEventID := uuid.NewString()
	logger := runtimepkg.NewRuntimeLogger(db)
	logger.Log(ctx, runtimepkg.RuntimeLogEntry{
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
	})

	reader := dashboardserver.NewSQLObservabilityReader(db)
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

func TestCanonicalMutationSurface_ReconstructsTrackedEntityStateForWorkflowWrites(t *testing.T) {
	ctx := context.Background()
	_, db, _ := testutil.StartPostgres(t)

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

	assertTrackedMutationStateMatchesEntityState(t, db, entityID)
}

func TestCanonicalMutationSurface_ReconstructsTrackedEntityStateForToolWrites(t *testing.T) {
	ctx, exec, db := newEntityToolConformanceHarness(t)

	requireMutationSurface(t, db)

	entityID := uuid.NewString()
	if _, err := exec.Execute(ctx, "create_entity", map[string]any{
		"entity_id":     entityID,
		"flow_instance": "review/inst-1",
		"entity_type":   "accounts",
		"fields": map[string]any{
			"status": "open",
			"score":  10.0,
		},
	}); err != nil {
		t.Fatalf("create_entity: %v", err)
	}
	if _, err := exec.Execute(ctx, "save_entity_field", map[string]any{
		"entity_id": entityID,
		"field":     "status",
		"value":     "closed",
	}); err != nil {
		t.Fatalf("save_entity_field: %v", err)
	}

	assertTrackedMutationStateMatchesEntityState(t, db, entityID)
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

func countTurnSummaryBlocks(blocks []any) int {
	count := 0
	for _, raw := range blocks {
		entry, _ := raw.(map[string]any)
		if strings.TrimSpace(readString(entry["kind"])) == "turn_summary" {
			count++
		}
	}
	return count
}

type trackedMutationState struct {
	CurrentState string         `json:"current_state"`
	Fields       map[string]any `json:"fields"`
	Gates        map[string]any `json:"gates"`
	Accumulator  map[string]any `json:"accumulator"`
}

func assertTrackedMutationStateMatchesEntityState(t *testing.T, db *sql.DB, entityID string) {
	t.Helper()
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
		WHERE entity_id = $1::uuid
	`, entityID).Scan(&currentState, &fieldsRaw, &gatesRaw, &accRaw); err != nil {
		t.Fatalf("load entity_state projection: %v", err)
	}

	want := trackedMutationState{
		CurrentState: strings.TrimSpace(currentState),
		Fields:       decodeJSONMap(t, fieldsRaw),
		Gates:        decodeJSONMap(t, gatesRaw),
		Accumulator:  decodeJSONMap(t, accRaw),
	}
	got := trackedMutationState{
		Fields:      map[string]any{},
		Gates:       map[string]any{},
		Accumulator: map[string]any{},
	}

	rows, err := db.QueryContext(context.Background(), `
		SELECT field, new_value
		FROM entity_mutations
		WHERE entity_id = $1::uuid
		ORDER BY created_at ASC, mutation_id ASC
	`, entityID)
	if err != nil {
		t.Fatalf("query mutations: %v", err)
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
			t.Fatalf("scan mutation: %v", err)
		}
		applyTrackedMutation(&got, strings.TrimSpace(field), decodeJSONValue(t, newValue))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read mutations: %v", err)
	}
	if rowCount == 0 {
		t.Fatal("entity_mutations is empty; canonical mutation surface is missing")
	}
	if !trackedStatesEqual(got, want) {
		t.Fatalf("mutation reconstruction mismatch:\n got=%s\nwant=%s", mustCanonicalJSON(t, got), mustCanonicalJSON(t, want))
	}
}

func applyTrackedMutation(state *trackedMutationState, field string, value any) {
	if state == nil {
		return
	}
	switch {
	case field == "current_state":
		state.CurrentState = strings.TrimSpace(readString(value))
	case strings.HasPrefix(field, "gates."):
		key := strings.TrimSpace(strings.TrimPrefix(field, "gates."))
		if key == "" {
			return
		}
		if value == nil {
			delete(state.Gates, key)
			return
		}
		state.Gates[key] = value
	case strings.HasPrefix(field, "accumulator."):
		key := strings.TrimSpace(strings.TrimPrefix(field, "accumulator."))
		if key == "" {
			return
		}
		if value == nil {
			delete(state.Accumulator, key)
			return
		}
		state.Accumulator[key] = value
	default:
		key := strings.TrimSpace(field)
		if key == "" {
			return
		}
		if value == nil {
			delete(state.Fields, key)
			return
		}
		state.Fields[key] = value
	}
}

func trackedStatesEqual(left, right trackedMutationState) bool {
	return mustCanonicalJSON(nil, left) == mustCanonicalJSON(nil, right)
}

func decodeJSONMap(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	if len(raw) == 0 {
		return map[string]any{}
	}
	out := map[string]any{}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("json.Unmarshal map: %v", err)
	}
	return out
}

func decodeJSONValue(t *testing.T, raw []byte) any {
	t.Helper()
	if len(raw) == 0 {
		return nil
	}
	var out any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("json.Unmarshal value: %v", err)
	}
	return out
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

func newEntityToolConformanceHarness(t *testing.T) (context.Context, *runtimetools.Executor, *sql.DB) {
	t.Helper()
	_, db, _ := testutil.StartPostgres(t)
	exec := runtimetools.NewExecutorWithOptions(nil, nil, runtimetools.ExecutorOptions{
		SQLDB: db,
		WorkflowSource: runtimesemanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
			Semantics: runtimecontracts.WorkflowSemanticView{
				InitialStage: "queued",
				EntitySchema: runtimecontracts.EntitySchema{
					Groups: []runtimecontracts.EntitySchemaGroup{{
						Name: "accounts",
						Fields: []runtimecontracts.EntitySchemaField{
							{Name: "status", Type: "string", Indexed: true},
							{Name: "score", Type: "numeric(10,2)"},
						},
					}},
				},
			},
		}),
	})
	ctx := runtimetools.WithActor(context.Background(), runtimeactors.AgentConfig{
		ID:    "tester",
		Role:  "operator",
		Tools: []string{"create_entity", "save_entity_field"},
	})
	return ctx, exec, db
}

func readString(value any) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	default:
		return ""
	}
}
