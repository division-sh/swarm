package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/mutationlog"
)

const (
	ConversationForkChatSnapshotOwner = "conversation.fork_chat.snapshot.v1"
	ConversationForkChatSandboxOwner  = "conversation.fork_chat.sandbox.v1"
)

type ConversationForkChatPrepareRequest struct {
	ForkID string
	Now    time.Time
}

type ConversationForkChatRecordRequest struct {
	ForkID       string
	Message      string
	ActorTokenID string
	Execution    ConversationForkChatExecution
	Now          time.Time
}

type ConversationForkChatPrepared struct {
	Fork           OperatorConversationForkSession
	Snapshot       ConversationForkSnapshot
	SandboxPolicy  ConversationForkSandboxPolicy
	AvailableTools []string
}

type ConversationForkChatResult struct {
	ForkID              string                        `json:"fork_id"`
	Turn                OperatorConversationTurn      `json:"turn"`
	Snapshot            ConversationForkSnapshot      `json:"snapshot"`
	SandboxPolicy       ConversationForkSandboxPolicy `json:"sandbox_policy"`
	IdempotencyReplayed bool                          `json:"idempotency_replayed"`
}

type ConversationForkSnapshot struct {
	ForkID          string                           `json:"fork_id"`
	SourceSessionID string                           `json:"source_session_id"`
	SourceRunID     string                           `json:"source_run_id,omitempty"`
	SourceAgentID   string                           `json:"source_agent_id"`
	SourceTurn      ConversationForkSourceTurn       `json:"source_turn"`
	EntitySnapshot  []ConversationForkEntitySnapshot `json:"entity_snapshot"`
	SnapshotOwner   string                           `json:"snapshot_owner"`
	CreatedAt       time.Time                        `json:"created_at"`
}

type ConversationForkSourceTurn struct {
	TurnID          string          `json:"turn_id"`
	TurnIndex       int             `json:"turn_index"`
	SelectedAt      time.Time       `json:"selected_at"`
	CreatedAt       time.Time       `json:"created_at"`
	RequestPayload  json.RawMessage `json:"request_payload,omitempty"`
	ResponsePayload json.RawMessage `json:"response_payload,omitempty"`
	ToolCalls       json.RawMessage `json:"tool_calls,omitempty"`
	AvailableTools  json.RawMessage `json:"available_tools,omitempty"`
}

type ConversationForkEntitySnapshot struct {
	EntityID       string         `json:"entity_id"`
	CurrentState   string         `json:"current_state,omitempty"`
	EnteredStateAt *time.Time     `json:"entered_state_at,omitempty"`
	Fields         map[string]any `json:"fields,omitempty"`
	Gates          map[string]any `json:"gates,omitempty"`
	Accumulator    map[string]any `json:"accumulator,omitempty"`
}

type ConversationForkSandboxPolicy struct {
	Owner              string   `json:"owner"`
	ReadPolicy         string   `json:"read_policy"`
	WritePolicy        string   `json:"write_policy"`
	SideEffectingTools []string `json:"side_effecting_tools"`
	StubbedTools       []string `json:"stubbed_tools"`
}

type ConversationForkChatExecution struct {
	AssistantMessage string
	ToolCalls        []OperatorConversationToolCall
	ToolResults      []OperatorConversationToolResult
	AvailableTools   []string
}

func (s *PostgresStore) PrepareOperatorConversationForkChat(ctx context.Context, req ConversationForkChatPrepareRequest) (ConversationForkChatPrepared, error) {
	owner, err := postgresConversationForkStore(s)
	if err != nil {
		return ConversationForkChatPrepared{}, err
	}
	return owner.prepareOperatorConversationForkChat(ctx, req)
}

func (s *SQLiteRuntimeStore) PrepareOperatorConversationForkChat(ctx context.Context, req ConversationForkChatPrepareRequest) (ConversationForkChatPrepared, error) {
	owner, err := sqliteConversationForkStore(s)
	if err != nil {
		return ConversationForkChatPrepared{}, err
	}
	return owner.prepareOperatorConversationForkChat(ctx, req)
}

func (s conversationForkStore) prepareOperatorConversationForkChat(ctx context.Context, req ConversationForkChatPrepareRequest) (ConversationForkChatPrepared, error) {
	forkID, err := normalizeUUIDParam(req.ForkID, "fork_id")
	if err != nil {
		return ConversationForkChatPrepared{}, err
	}
	now := req.Now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}

	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return ConversationForkChatPrepared{}, err
	}
	catalog, err := s.schemaColumnCatalog(ctx)
	if err != nil {
		return ConversationForkChatPrepared{}, err
	}
	if err := requireConversationForkChatCapabilities(caps, catalog); err != nil {
		return ConversationForkChatPrepared{}, err
	}

	var prepared ConversationForkChatPrepared
	err = s.runForkMutation(ctx, forkID, true, func(txctx context.Context, tx *sql.Tx) error {
		fork, err := loadActiveConversationForkForChat(txctx, s, tx, forkID, now)
		if err != nil {
			return err
		}
		snapshot, err := ensureConversationForkSnapshot(txctx, s, tx, fork, now)
		if err != nil {
			return err
		}
		policy := defaultConversationForkSandboxPolicy()
		prepared = ConversationForkChatPrepared{
			Fork:           fork,
			Snapshot:       snapshot,
			SandboxPolicy:  policy,
			AvailableTools: conversationForkSandboxAvailableTools(policy),
		}
		return nil
	})
	if err != nil {
		return ConversationForkChatPrepared{}, fmt.Errorf("prepare conversation fork chat: %w", err)
	}
	return prepared, nil
}

func (s *PostgresStore) RecordOperatorConversationForkChat(ctx context.Context, req ConversationForkChatRecordRequest) (ConversationForkChatResult, error) {
	owner, err := postgresConversationForkStore(s)
	if err != nil {
		return ConversationForkChatResult{}, err
	}
	return owner.recordOperatorConversationForkChat(ctx, req)
}

func (s *SQLiteRuntimeStore) RecordOperatorConversationForkChat(ctx context.Context, req ConversationForkChatRecordRequest) (ConversationForkChatResult, error) {
	owner, err := sqliteConversationForkStore(s)
	if err != nil {
		return ConversationForkChatResult{}, err
	}
	return owner.recordOperatorConversationForkChat(ctx, req)
}

func (s conversationForkStore) recordOperatorConversationForkChat(ctx context.Context, req ConversationForkChatRecordRequest) (ConversationForkChatResult, error) {
	forkID, err := normalizeUUIDParam(req.ForkID, "fork_id")
	if err != nil {
		return ConversationForkChatResult{}, err
	}
	message := strings.TrimSpace(req.Message)
	if message == "" {
		return ConversationForkChatResult{}, &EntityReadParamError{Field: "message", Reason: "is required"}
	}
	actorTokenID := strings.TrimSpace(req.ActorTokenID)
	if actorTokenID == "" {
		return ConversationForkChatResult{}, &EntityReadParamError{Field: "actor_token_id", Reason: "is required"}
	}
	execution := req.Execution
	execution.AssistantMessage = strings.TrimSpace(execution.AssistantMessage)
	if execution.AssistantMessage == "" {
		return ConversationForkChatResult{}, &EntityReadParamError{Field: "execution.assistant_message", Reason: "is required"}
	}
	now := req.Now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}

	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return ConversationForkChatResult{}, err
	}
	catalog, err := s.schemaColumnCatalog(ctx)
	if err != nil {
		return ConversationForkChatResult{}, err
	}
	if err := requireConversationForkChatCapabilities(caps, catalog); err != nil {
		return ConversationForkChatResult{}, err
	}

	var result ConversationForkChatResult
	err = s.runForkMutation(ctx, forkID, true, func(txctx context.Context, tx *sql.Tx) error {
		if _, err := loadActiveConversationForkForChat(txctx, s, tx, forkID, now); err != nil {
			return err
		}
		snapshot, err := loadConversationForkSnapshot(txctx, s, tx, forkID)
		if errors.Is(err, sql.ErrNoRows) {
			return &EntityReadParamError{Field: "fork_id", Reason: "forkchat snapshot is unavailable"}
		}
		if err != nil {
			return err
		}
		policy := defaultConversationForkSandboxPolicy()
		if len(execution.AvailableTools) == 0 {
			execution.AvailableTools = conversationForkSandboxAvailableTools(policy)
		}
		requestPayload, err := conversationForkChatRequestPayload(message, snapshot, execution.AvailableTools)
		if err != nil {
			return err
		}
		responsePayload, err := conversationForkChatResponsePayload(execution, policy)
		if err != nil {
			return err
		}
		turn, err := insertConversationForkTurn(txctx, s, tx, forkID, actorTokenID, message, execution, requestPayload, responsePayload, policy, now)
		if err != nil {
			return err
		}
		result = ConversationForkChatResult{ForkID: forkID, Turn: turn, Snapshot: snapshot, SandboxPolicy: policy}
		return nil
	})
	if err != nil {
		return ConversationForkChatResult{}, fmt.Errorf("record conversation fork chat: %w", err)
	}
	return result, nil
}

func requireConversationForkChatCapabilities(caps StoreSchemaCapabilities, catalog schemaColumnCatalog) error {
	if err := requireConversationForkLifecycleCapabilities(caps); err != nil {
		return err
	}
	if caps.Conversations.ForkSnapshots != SchemaFlavorCanonical {
		return fmt.Errorf("store: conversation_fork_snapshots schema is unavailable")
	}
	if caps.Conversations.ForkTurns != SchemaFlavorCanonical {
		return fmt.Errorf("store: conversation_fork_turns schema is unavailable")
	}
	if caps.EntityState != SchemaFlavorCanonical || !caps.EntityRunID {
		return fmt.Errorf("store: conversation fork chat requires canonical run-scoped entity_state")
	}
	if !catalog.hasColumns("entity_mutations", "mutation_id", "run_id", "entity_id", "field", "new_value", "created_at") {
		return fmt.Errorf("store: conversation fork chat requires canonical entity_mutations")
	}
	return nil
}

func loadActiveConversationForkForChat(ctx context.Context, owner conversationForkStore, tx *sql.Tx, forkID string, now time.Time) (OperatorConversationForkSession, error) {
	row := owner.queryRow(ctx, tx, `
		SELECT
			CAST(fork_id AS TEXT), CAST(source_session_id AS TEXT), COALESCE(CAST(source_run_id AS TEXT), ''),
			source_agent_id, fork_point_kind, fork_point_turn_index,
			COALESCE(CAST(fork_point_turn_id AS TEXT), ''), COALESCE(CAST(fork_point_event_id AS TEXT), ''),
			fork_point_at, fork_point_selected_at, created_by, created_at, expires_at, deleted_at
		FROM conversation_forks
		WHERE fork_id = ?`+owner.forUpdate()+`
	`, forkID)
	item, err := scanConversationForkSession(row, now)
	if errors.Is(err, sql.ErrNoRows) {
		return OperatorConversationForkSession{}, ErrConversationForkNotFound
	}
	if err != nil {
		return OperatorConversationForkSession{}, err
	}
	if item.State != "active" {
		return OperatorConversationForkSession{}, &EntityReadParamError{Field: "fork_id", Reason: "must reference an active fork"}
	}
	return item, nil
}

func ensureConversationForkSnapshot(ctx context.Context, owner conversationForkStore, tx *sql.Tx, fork OperatorConversationForkSession, now time.Time) (ConversationForkSnapshot, error) {
	snapshot, err := loadConversationForkSnapshot(ctx, owner, tx, fork.ForkID)
	if err == nil {
		return snapshot, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return ConversationForkSnapshot{}, err
	}
	sourceTurn, err := loadConversationForkSourceTurn(ctx, owner, tx, fork)
	if err != nil {
		return ConversationForkSnapshot{}, err
	}
	entities, err := loadConversationForkEntitySnapshot(ctx, owner, tx, fork)
	if err != nil {
		return ConversationForkSnapshot{}, err
	}
	snapshot = ConversationForkSnapshot{
		ForkID:          fork.ForkID,
		SourceSessionID: fork.SourceSessionID,
		SourceRunID:     fork.SourceRunID,
		SourceAgentID:   fork.SourceAgentID,
		SourceTurn:      sourceTurn,
		EntitySnapshot:  entities,
		SnapshotOwner:   ConversationForkChatSnapshotOwner,
		CreatedAt:       now,
	}
	sourceTurnJSON, err := json.Marshal(sourceTurn)
	if err != nil {
		return ConversationForkSnapshot{}, err
	}
	entitySnapshotJSON, err := json.Marshal(entities)
	if err != nil {
		return ConversationForkSnapshot{}, err
	}
	if _, err := owner.exec(ctx, tx, `
		INSERT INTO conversation_fork_snapshots (
			fork_id, source_session_id, source_run_id, source_agent_id,
			fork_point_turn_id, fork_point_turn_index, fork_point_selected_at,
			source_turn, entity_snapshot, snapshot_owner, created_at
		)
		VALUES (
			?, ?, ?, ?,
			?, ?, ?,
			?, ?, ?, ?
		)
	`, snapshot.ForkID, snapshot.SourceSessionID, nullableConversationForkID(snapshot.SourceRunID), snapshot.SourceAgentID,
		sourceTurn.TurnID, sourceTurn.TurnIndex, sourceTurn.SelectedAt,
		string(sourceTurnJSON), string(entitySnapshotJSON), snapshot.SnapshotOwner, snapshot.CreatedAt); err != nil {
		return ConversationForkSnapshot{}, fmt.Errorf("insert conversation fork snapshot: %w", err)
	}
	return snapshot, nil
}

func loadConversationForkSnapshot(ctx context.Context, owner conversationForkStore, tx *sql.Tx, forkID string) (ConversationForkSnapshot, error) {
	row := owner.queryRow(ctx, tx, `
		SELECT
			CAST(fork_id AS TEXT), CAST(source_session_id AS TEXT), COALESCE(CAST(source_run_id AS TEXT), ''),
			source_agent_id, source_turn, entity_snapshot, snapshot_owner, created_at
		FROM conversation_fork_snapshots
		WHERE fork_id = ?
	`, forkID)
	var out ConversationForkSnapshot
	var sourceTurnRaw []byte
	var entitiesRaw []byte
	var createdAt conversationForkTimeValue
	if err := row.Scan(&out.ForkID, &out.SourceSessionID, &out.SourceRunID, &out.SourceAgentID, &sourceTurnRaw, &entitiesRaw, &out.SnapshotOwner, &createdAt); err != nil {
		return ConversationForkSnapshot{}, err
	}
	if err := json.Unmarshal(sourceTurnRaw, &out.SourceTurn); err != nil {
		return ConversationForkSnapshot{}, fmt.Errorf("decode conversation fork source turn snapshot: %w", err)
	}
	if err := json.Unmarshal(entitiesRaw, &out.EntitySnapshot); err != nil {
		return ConversationForkSnapshot{}, fmt.Errorf("decode conversation fork entity snapshot: %w", err)
	}
	if out.EntitySnapshot == nil {
		out.EntitySnapshot = []ConversationForkEntitySnapshot{}
	}
	out.CreatedAt = createdAt.Time
	return out, nil
}

func loadConversationForkSourceTurn(ctx context.Context, owner conversationForkStore, tx *sql.Tx, fork OperatorConversationForkSession) (ConversationForkSourceTurn, error) {
	row := owner.queryRow(ctx, tx, `
		SELECT
			CAST(turn_id AS TEXT),
			COALESCE(request_payload, '{}'),
			COALESCE(response_payload, '{}'),
			COALESCE(tool_calls, '[]'),
			COALESCE(available_tools, '[]'),
			created_at
		FROM agent_turns
		WHERE session_id = ?
		  AND turn_id = ?
	`, fork.SourceSessionID, fork.ForkPoint.TurnID)
	var out ConversationForkSourceTurn
	var requestRaw, responseRaw, toolCallsRaw, availableToolsRaw []byte
	var createdAt conversationForkTimeValue
	if err := row.Scan(&out.TurnID, &requestRaw, &responseRaw, &toolCallsRaw, &availableToolsRaw, &createdAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ConversationForkSourceTurn{}, &EntityReadParamError{Field: "fork_id", Reason: "source turn is unavailable"}
		}
		return ConversationForkSourceTurn{}, fmt.Errorf("load conversation fork source turn: %w", err)
	}
	out.TurnIndex = fork.ForkPoint.TurnIndex
	out.SelectedAt = fork.ForkPoint.SelectedAt
	out.CreatedAt = createdAt.Time
	out.RequestPayload = cloneRawMessage(requestRaw)
	out.ResponsePayload = cloneRawMessage(responseRaw)
	out.ToolCalls = cloneRawMessage(toolCallsRaw)
	out.AvailableTools = cloneRawMessage(availableToolsRaw)
	return out, nil
}

func loadConversationForkEntitySnapshot(ctx context.Context, owner conversationForkStore, tx *sql.Tx, fork OperatorConversationForkSession) ([]ConversationForkEntitySnapshot, error) {
	if strings.TrimSpace(fork.SourceRunID) == "" {
		return []ConversationForkEntitySnapshot{}, nil
	}
	rows, err := owner.query(ctx, tx, `
		SELECT CAST(entity_id AS TEXT), field, new_value, created_at
		FROM entity_mutations
		WHERE run_id = ?
		  AND created_at <= ?
		ORDER BY entity_id ASC, created_at ASC, mutation_id ASC
	`, fork.SourceRunID, fork.ForkPoint.SelectedAt)
	if err != nil {
		return nil, fmt.Errorf("load conversation fork entity mutations: %w", err)
	}
	defer rows.Close()

	type timedProjectionMutation struct {
		mutationlog.ProjectionMutation
		CreatedAt time.Time
	}
	grouped := map[string][]timedProjectionMutation{}
	entityOrder := []string{}
	seen := map[string]struct{}{}
	for rows.Next() {
		var entityID, field string
		var raw []byte
		var createdAt conversationForkTimeValue
		if err := rows.Scan(&entityID, &field, &raw, &createdAt); err != nil {
			return nil, fmt.Errorf("scan conversation fork entity mutation: %w", err)
		}
		var value any
		if err := json.Unmarshal(raw, &value); err != nil {
			return nil, fmt.Errorf("decode conversation fork entity mutation %s/%s: %w", entityID, field, err)
		}
		entityID = strings.TrimSpace(entityID)
		if _, ok := seen[entityID]; !ok {
			seen[entityID] = struct{}{}
			entityOrder = append(entityOrder, entityID)
		}
		grouped[entityID] = append(grouped[entityID], timedProjectionMutation{
			ProjectionMutation: mutationlog.ProjectionMutation{Field: field, NewValue: value},
			CreatedAt:          createdAt.Time,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read conversation fork entity mutations: %w", err)
	}

	out := make([]ConversationForkEntitySnapshot, 0, len(entityOrder))
	for _, entityID := range entityOrder {
		mutations := grouped[entityID]
		projectionMutations := make([]mutationlog.ProjectionMutation, 0, len(mutations))
		var enteredStateAt *time.Time
		for _, mutation := range mutations {
			projectionMutations = append(projectionMutations, mutation.ProjectionMutation)
			if strings.TrimSpace(mutation.Field) == "current_state" {
				tm := mutation.CreatedAt
				enteredStateAt = &tm
			}
		}
		projection, err := mutationlog.ReconstructEntityStateProjection(projectionMutations)
		if err != nil {
			return nil, fmt.Errorf("reconstruct conversation fork entity %s at fork point: %w", entityID, err)
		}
		out = append(out, ConversationForkEntitySnapshot{
			EntityID:       entityID,
			CurrentState:   projection.CurrentState,
			EnteredStateAt: enteredStateAt,
			Fields:         projection.Fields,
			Gates:          projection.Gates,
			Accumulator:    projection.Accumulator,
		})
	}
	if out == nil {
		out = []ConversationForkEntitySnapshot{}
	}
	return out, nil
}

func insertConversationForkTurn(
	ctx context.Context,
	owner conversationForkStore,
	tx *sql.Tx,
	forkID string,
	actorTokenID string,
	message string,
	execution ConversationForkChatExecution,
	requestPayload json.RawMessage,
	responsePayload json.RawMessage,
	policy ConversationForkSandboxPolicy,
	now time.Time,
) (OperatorConversationTurn, error) {
	var nextIndex int
	if err := owner.queryRow(ctx, tx, `
		SELECT COALESCE(MAX(turn_index), 0) + 1
		FROM conversation_fork_turns
		WHERE fork_id = ?
	`, forkID).Scan(&nextIndex); err != nil {
		return OperatorConversationTurn{}, fmt.Errorf("allocate conversation fork turn index: %w", err)
	}
	policyJSON, err := json.Marshal(policy)
	if err != nil {
		return OperatorConversationTurn{}, err
	}
	toolCallsJSON, err := json.Marshal(execution.ToolCalls)
	if err != nil {
		return OperatorConversationTurn{}, err
	}
	var turn OperatorConversationTurn
	var createdAt conversationForkTimeValue
	if err := owner.queryRow(ctx, tx, `
		INSERT INTO conversation_fork_turns (
			fork_id, turn_index, actor_token_id, message, assistant_message,
			request_payload, response_payload, tool_calls, sandbox_policy,
			snapshot_owner, created_at
		)
		VALUES (
			?, ?, ?, ?, ?,
			?, ?, ?, ?,
			?, ?
		)
		RETURNING CAST(fork_turn_id AS TEXT), created_at
	`, forkID, nextIndex, actorTokenID, message, execution.AssistantMessage,
		string(requestPayload), string(responsePayload), string(toolCallsJSON), string(policyJSON),
		ConversationForkChatSnapshotOwner, now).Scan(&turn.TurnID, &createdAt); err != nil {
		return OperatorConversationTurn{}, fmt.Errorf("insert conversation fork turn: %w", err)
	}
	turn.TurnIndex = nextIndex
	turn.RequestPayload = cloneRawMessage(requestPayload)
	turn.ResponsePayload = cloneRawMessage(responsePayload)
	turn.ToolCalls = cloneConversationToolCalls(execution.ToolCalls)
	turn.ToolResults = cloneConversationToolResults(execution.ToolResults)
	turn.TurnBlocks = conversationForkSandboxTurnBlocks(execution)
	turn.ParseOK = true
	turn.LatencyMS = 0
	turn.CreatedAt = createdAt.Time
	turn.AssistantVisibleOutput = execution.AssistantMessage
	return turn, nil
}

func loadConversationForkTurns(ctx context.Context, owner conversationForkStore, db *sql.DB, forkID string) ([]OperatorConversationTurn, error) {
	rows, err := owner.query(ctx, db, `
		SELECT
			CAST(fork_turn_id AS TEXT), turn_index,
			request_payload, response_payload, tool_calls,
			assistant_message, created_at
		FROM conversation_fork_turns
		WHERE fork_id = ?
		ORDER BY turn_index ASC, created_at ASC, fork_turn_id ASC
	`, forkID)
	if err != nil {
		return nil, fmt.Errorf("load conversation fork turns: %w", err)
	}
	defer rows.Close()
	turns := []OperatorConversationTurn{}
	for rows.Next() {
		var turn OperatorConversationTurn
		var requestRaw, responseRaw, toolCallsRaw []byte
		var assistant string
		var createdAt conversationForkTimeValue
		if err := rows.Scan(&turn.TurnID, &turn.TurnIndex, &requestRaw, &responseRaw, &toolCallsRaw, &assistant, &createdAt); err != nil {
			return nil, fmt.Errorf("scan conversation fork turn: %w", err)
		}
		if len(toolCallsRaw) > 0 {
			if err := json.Unmarshal(toolCallsRaw, &turn.ToolCalls); err != nil {
				return nil, fmt.Errorf("decode conversation fork turn tool calls: %w", err)
			}
		}
		if turn.ToolCalls == nil {
			turn.ToolCalls = []OperatorConversationToolCall{}
		}
		turn.RequestPayload = cloneRawMessage(requestRaw)
		turn.ResponsePayload = cloneRawMessage(responseRaw)
		turn.ToolResults = conversationForkToolResultsFromCalls(turn.ToolCalls)
		turn.TurnBlocks = conversationForkSandboxTurnBlocks(ConversationForkChatExecution{
			AssistantMessage: assistant,
			ToolCalls:        turn.ToolCalls,
			ToolResults:      turn.ToolResults,
		})
		turn.ParseOK = true
		turn.CreatedAt = createdAt.Time
		turn.AssistantVisibleOutput = assistant
		turns = append(turns, turn)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read conversation fork turns: %w", err)
	}
	return turns, nil
}

func defaultConversationForkSandboxPolicy() ConversationForkSandboxPolicy {
	sideEffecting := []string{
		"save_entity_field",
		"create_entity",
		"emit_event",
		"mailbox.approve",
		"mailbox.reject",
		"mailbox.defer",
		"run.start",
		"run.continue",
		"run.pause",
		"run.stop",
	}
	return ConversationForkSandboxPolicy{
		Owner:              ConversationForkChatSandboxOwner,
		ReadPolicy:         "fork_snapshot_only",
		WritePolicy:        "stub_record_only_no_live_mutation",
		SideEffectingTools: append([]string(nil), sideEffecting...),
		StubbedTools:       append([]string(nil), sideEffecting...),
	}
}

func conversationForkSandboxAvailableTools(policy ConversationForkSandboxPolicy) []string {
	out := []string{"fork_snapshot_read_entities"}
	for _, name := range policy.StubbedTools {
		out = append(out, conversationForkSandboxToolName(name))
	}
	return out
}

func conversationForkToolResultsFromCalls(calls []OperatorConversationToolCall) []OperatorConversationToolResult {
	if len(calls) == 0 {
		return []OperatorConversationToolResult{}
	}
	out := make([]OperatorConversationToolResult, 0, len(calls))
	for _, call := range calls {
		if len(call.Result) == 0 {
			continue
		}
		out = append(out, OperatorConversationToolResult{
			ToolName:  call.Name,
			ToolUseID: call.ToolUseID,
			Output:    cloneRawMessage(call.Result),
		})
	}
	return out
}

func conversationForkSandboxTurnBlocks(execution ConversationForkChatExecution) []OperatorConversationTurnBlock {
	blocks := []OperatorConversationTurnBlock{{
		Kind:  "turn_summary",
		Title: "Forkchat sandbox response",
		Text:  execution.AssistantMessage,
	}}
	for _, call := range execution.ToolCalls {
		blocks = append(blocks, OperatorConversationTurnBlock{
			Kind:     "tool_result",
			Title:    call.Name,
			ToolName: call.Name,
			Input:    cloneRawMessage(call.Arguments),
			Output:   cloneRawMessage(call.Result),
		})
	}
	return blocks
}

func conversationForkChatRequestPayload(message string, snapshot ConversationForkSnapshot, availableTools []string) (json.RawMessage, error) {
	raw, err := json.Marshal(map[string]any{
		"message":         message,
		"snapshot_owner":  snapshot.SnapshotOwner,
		"snapshot":        snapshot,
		"available_tools": availableTools,
	})
	if err != nil {
		return nil, err
	}
	return raw, nil
}

func conversationForkChatResponsePayload(execution ConversationForkChatExecution, policy ConversationForkSandboxPolicy) (json.RawMessage, error) {
	raw, err := json.Marshal(map[string]any{
		"message":        execution.AssistantMessage,
		"sandbox_policy": policy,
		"tool_calls":     execution.ToolCalls,
		"tool_results":   execution.ToolResults,
	})
	if err != nil {
		return nil, err
	}
	return raw, nil
}

func conversationForkSandboxToolName(name string) string {
	return strings.NewReplacer(".", "_", "-", "_").Replace(strings.TrimSpace(name))
}
