package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
)

type sqliteOperatorAgentConversationReadSurface struct {
	store     *SQLiteRuntimeStore
	db        *sql.DB
	turnLimit int
}

func newSQLiteOperatorAgentConversationReadSurface(s *SQLiteRuntimeStore, turnLimit int) *sqliteOperatorAgentConversationReadSurface {
	if s == nil || s.DB == nil {
		return nil
	}
	return &sqliteOperatorAgentConversationReadSurface{store: s, db: s.DB, turnLimit: maxStoreInt(turnLimit, 0)}
}

func (s *SQLiteRuntimeStore) ListOperatorAgents(ctx context.Context, opts OperatorAgentListOptions) (OperatorAgentListResult, error) {
	return newSQLiteOperatorAgentConversationReadSurface(s, 0).ListOperatorAgents(ctx, opts)
}

func (s *SQLiteRuntimeStore) LoadOperatorAgent(ctx context.Context, agentID string) (OperatorAgentDetail, error) {
	return newSQLiteOperatorAgentConversationReadSurface(s, 0).LoadOperatorAgent(ctx, agentID)
}

func (s *SQLiteRuntimeStore) LoadOperatorAgentDiagnosis(ctx context.Context, agentID string, opts OperatorAgentDiagnosisOptions) (OperatorAgentDiagnosis, error) {
	return newSQLiteOperatorAgentConversationReadSurface(s, 0).LoadOperatorAgentDiagnosis(ctx, agentID, opts)
}

func (s *SQLiteRuntimeStore) LoadOperatorAgentDeliveryDiagnostics(ctx context.Context, agentID string, opts OperatorAgentDeliveryDiagnosticsOptions) (OperatorAgentDeliveryDiagnostics, error) {
	return newSQLiteOperatorAgentConversationReadSurface(s, 0).LoadOperatorAgentDeliveryDiagnostics(ctx, agentID, opts)
}

func (s *SQLiteRuntimeStore) ListOperatorConversations(ctx context.Context, opts OperatorConversationListOptions) (OperatorConversationListResult, error) {
	return newSQLiteOperatorAgentConversationReadSurface(s, 0).ListOperatorConversations(ctx, opts)
}

func (s *SQLiteRuntimeStore) LoadOperatorConversation(ctx context.Context, sessionID string) (OperatorConversationDetail, error) {
	return newSQLiteOperatorAgentConversationReadSurface(s, 0).LoadOperatorConversation(ctx, sessionID)
}

func (s *SQLiteRuntimeStore) LoadOperatorConversationTurn(ctx context.Context, sessionID string, turnIndex int) (OperatorConversationTurnDetail, error) {
	return newSQLiteOperatorAgentConversationReadSurface(s, 0).LoadOperatorConversationTurn(ctx, sessionID, turnIndex)
}

func (s *SQLiteRuntimeStore) LoadCurrentOperatorConversationForAgent(ctx context.Context, agentID string) (*OperatorConversationDetail, error) {
	return newSQLiteOperatorAgentConversationReadSurface(s, 0).LoadCurrentOperatorConversationForAgent(ctx, agentID)
}

func (s *SQLiteRuntimeStore) ListAgentLifecycleFacts(ctx context.Context, agentIDs []string) (map[string]AgentLifecycleFacts, error) {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return nil, err
	}
	if err := RequireCanonicalAgentLifecycleCapabilities(caps); err != nil {
		return nil, err
	}
	normalized := normalizePendingAgentIDs(agentIDs)
	out := make(map[string]AgentLifecycleFacts, len(normalized))
	for _, agentID := range normalized {
		out[agentID] = AgentLifecycleFacts{}
	}
	if len(normalized) == 0 {
		return out, nil
	}
	records, err := s.listSQLiteAgentLifecycleRecords(ctx, normalized)
	if err != nil {
		return nil, err
	}
	grouped := make(map[string][]agentLifecycleDeliveryRecord, len(normalized))
	for _, record := range records {
		grouped[record.AgentID] = append(grouped[record.AgentID], record)
	}
	for _, agentID := range normalized {
		out[agentID] = canonicalAgentLifecycleFactsFromRecords(grouped[agentID])
	}
	return out, nil
}

func (s *SQLiteRuntimeStore) listSQLiteAgentLifecycleRecords(ctx context.Context, agentIDs []string) ([]agentLifecycleDeliveryRecord, error) {
	placeholders := make([]string, 0, len(agentIDs))
	args := make([]any, 0, len(agentIDs))
	for _, agentID := range agentIDs {
		placeholders = append(placeholders, "?")
		args = append(args, agentID)
	}
	rows, err := s.DB.QueryContext(ctx, fmt.Sprintf(`
		SELECT
			d.subscriber_id,
			COALESCE(d.status, ''),
			COALESCE(d.active_session_id, ''),
			d.created_at,
			d.delivered_at
		FROM event_deliveries d
		WHERE d.subscriber_type = 'agent'
		  AND d.subscriber_id IN (%s)
		  AND COALESCE(d.status, '') IN ('pending', 'in_progress', 'failed', 'dead_letter')
	`, strings.Join(placeholders, ",")), args...)
	if err != nil {
		return nil, fmt.Errorf("query sqlite agent lifecycle records: %w", err)
	}
	defer rows.Close()

	out := make([]agentLifecycleDeliveryRecord, 0)
	for rows.Next() {
		var record agentLifecycleDeliveryRecord
		if err := rows.Scan(
			&record.AgentID,
			&record.Status,
			&record.ActiveSessionID,
			&record.CreatedAt,
			&record.DeliveredAt,
		); err != nil {
			return nil, fmt.Errorf("scan sqlite agent lifecycle record: %w", err)
		}
		record.AgentID = strings.TrimSpace(record.AgentID)
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read sqlite agent lifecycle rows: %w", err)
	}
	return out, nil
}

func (r *sqliteOperatorAgentConversationReadSurface) ListOperatorAgents(ctx context.Context, opts OperatorAgentListOptions) (OperatorAgentListResult, error) {
	if err := r.requireAgentCapabilities(ctx); err != nil {
		return OperatorAgentListResult{}, err
	}
	opts.Flow = strings.Trim(strings.TrimSpace(opts.Flow), "/")
	opts.Role = strings.TrimSpace(opts.Role)
	baseRows, err := r.store.LoadAgents(ctx)
	if err != nil {
		return OperatorAgentListResult{}, err
	}
	projections, err := r.loadAgentOperatorProjections(ctx)
	if err != nil {
		return OperatorAgentListResult{}, err
	}
	agents := make([]OperatorAgentSummary, 0, len(baseRows))
	for _, row := range baseRows {
		if opts.Role != "" && strings.TrimSpace(row.Config.Role) != opts.Role {
			continue
		}
		if opts.Flow != "" && !operatorAgentFlowMatches(row.Config.CanonicalFlowPath(), opts.Flow) {
			continue
		}
		id := strings.TrimSpace(row.Config.ID)
		projection, ok := projections[id]
		if !ok {
			return OperatorAgentListResult{}, fmt.Errorf("missing sqlite agent operator projection: %s", id)
		}
		agents = append(agents, operatorAgentSummaryFromPersisted(row, projection, r.turnLimit))
	}
	if agents == nil {
		agents = []OperatorAgentSummary{}
	}
	return OperatorAgentListResult{Agents: agents}, nil
}

func (r *sqliteOperatorAgentConversationReadSurface) LoadOperatorAgent(ctx context.Context, agentID string) (OperatorAgentDetail, error) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return OperatorAgentDetail{}, ErrAgentNotFound
	}
	result, err := r.ListOperatorAgents(ctx, OperatorAgentListOptions{})
	if err != nil {
		return OperatorAgentDetail{}, err
	}
	for _, agent := range result.Agents {
		if strings.TrimSpace(agent.AgentID) == agentID {
			return OperatorAgentDetail{
				Agent:             agent,
				CurrentSessionRef: agent.CurrentSessionRef,
				LastTurnRef:       agent.LastTurnRef,
			}, nil
		}
	}
	return OperatorAgentDetail{}, ErrAgentNotFound
}

func (r *sqliteOperatorAgentConversationReadSurface) LoadOperatorAgentDiagnosis(ctx context.Context, agentID string, opts OperatorAgentDiagnosisOptions) (OperatorAgentDiagnosis, error) {
	detail, err := r.LoadOperatorAgent(ctx, agentID)
	if err != nil {
		return OperatorAgentDiagnosis{}, err
	}
	diagnosis, err := operatorAgentDiagnosisFromDetail(detail)
	if err != nil {
		return OperatorAgentDiagnosis{}, err
	}
	queue, err := r.store.ListPendingAgentDeliveryDetails(ctx, PendingAgentDeliveryListOptions{
		AgentID: strings.TrimSpace(agentID),
		Limit:   opts.QueueLimit,
		Cursor:  opts.QueueCursor,
	})
	if err != nil {
		return OperatorAgentDiagnosis{}, err
	}
	diagnosis.Queue = operatorAgentDiagnosisQueueFromPendingPage(queue)
	if err := validateOperatorAgentDiagnosis(diagnosis); err != nil {
		return OperatorAgentDiagnosis{}, err
	}
	return diagnosis, nil
}

func (r *sqliteOperatorAgentConversationReadSurface) ListOperatorConversations(ctx context.Context, opts OperatorConversationListOptions) (OperatorConversationListResult, error) {
	if err := r.requireConversationCapabilities(ctx); err != nil {
		return OperatorConversationListResult{}, err
	}
	opts, err := defaultOperatorConversationListOptions(opts)
	if err != nil {
		return OperatorConversationListResult{}, err
	}
	caps, err := r.resolveConversationCapabilities(ctx)
	if err != nil {
		return OperatorConversationListResult{}, err
	}
	if opts.RunID != "" && !caps.Conversations.SessionRunID && !caps.Conversations.AuditRunID {
		return OperatorConversationListResult{}, operatorConversationRunIDCapabilityError("run_id filtering requires agent_sessions.run_id or agent_conversation_audits.run_id")
	}
	sources := sqliteOperatorConversationQuerySources(caps)
	if len(sources) == 0 {
		return OperatorConversationListResult{Conversations: []OperatorConversationSummary{}}, nil
	}
	args := make([]any, 0, 8)
	where := []string{"1=1"}
	if opts.AgentID != "" {
		where = append(where, "conversations.agent_id = ?")
		args = append(args, opts.AgentID)
	}
	if opts.RunID != "" {
		where = append(where, "conversations.run_id = ?")
		args = append(args, opts.RunID)
	}
	if opts.Cursor != "" {
		cursor, err := decodeConversationPositionCursor(opts.Cursor, "conversation.list")
		if err != nil {
			return OperatorConversationListResult{}, err
		}
		updatedAt, err := time.Parse(time.RFC3339Nano, cursor.UpdatedAt)
		if err != nil || strings.TrimSpace(cursor.SessionID) == "" {
			return OperatorConversationListResult{}, ErrInvalidConversationCursor
		}
		where = append(where, `(conversations.updated_at < ? OR (conversations.updated_at = ? AND conversations.session_id > ?))`)
		args = append(args, updatedAt.UTC(), updatedAt.UTC(), cursor.SessionID)
	}
	turnBlocksExpr := "'[]'"
	if caps.Conversations.TurnBlocks {
		turnBlocksExpr = "COALESCE(latest_turn.turn_blocks, '[]')"
	}
	args = append(args, opts.Limit+1)
	rows, err := r.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT
			conversations.session_id,
			conversations.agent_id,
			conversations.run_id,
			conversations.kind,
			COALESCE(conversations.scope_key, ''),
			COALESCE(conversations.scope, ''),
			COALESCE(conversations.runtime_mode, ''),
			COALESCE(conversations.status, ''),
			COALESCE(conversations.turn_count, 0),
			COALESCE(conversations.message_count, 0),
			COALESCE(conversations.runtime_state, '{}'),
			COALESCE(latest_turn.turn_id, ''),
			COALESCE(latest_turn.task_id, ''),
			COALESCE(latest_turn.parse_ok, 0),
			%s AS turn_blocks,
			conversations.started_at,
			conversations.ended_at,
			conversations.updated_at
		FROM (
			%s
		) conversations
		LEFT JOIN agent_turns latest_turn ON latest_turn.turn_id = (
			SELECT turn_id
			FROM agent_turns t
			WHERE t.agent_id = conversations.agent_id
			  AND t.session_id = conversations.session_id
			ORDER BY t.created_at DESC, t.turn_id DESC
			LIMIT 1
		)
		WHERE %s
		ORDER BY conversations.updated_at DESC, conversations.session_id ASC
		LIMIT ?
	`, turnBlocksExpr, strings.Join(sources, "\nUNION ALL\n"), strings.Join(where, " AND ")), args...)
	if err != nil {
		return OperatorConversationListResult{}, operatorConversationReadQueryError("list sqlite operator conversations", err)
	}
	defer rows.Close()

	conversations := []OperatorConversationSummary{}
	for rows.Next() {
		item, err := scanSQLiteOperatorConversationSummary(rows)
		if err != nil {
			return OperatorConversationListResult{}, err
		}
		conversations = append(conversations, item)
	}
	if err := rows.Err(); err != nil {
		return OperatorConversationListResult{}, operatorConversationReadQueryError("read sqlite operator conversations", err)
	}
	nextCursor := ""
	if len(conversations) > opts.Limit {
		conversations = conversations[:opts.Limit]
		last := conversations[len(conversations)-1]
		nextCursor = encodeConversationPositionCursor(conversationPositionCursor{
			Kind:      "conversation.list",
			UpdatedAt: last.UpdatedAt.UTC().Format(time.RFC3339Nano),
			SessionID: last.SessionID,
		})
	}
	if conversations == nil {
		conversations = []OperatorConversationSummary{}
	}
	return OperatorConversationListResult{Conversations: conversations, NextCursor: nextCursor}, nil
}

func (r *sqliteOperatorAgentConversationReadSurface) LoadOperatorConversation(ctx context.Context, sessionID string) (OperatorConversationDetail, error) {
	if err := r.requireConversationCapabilities(ctx); err != nil {
		return OperatorConversationDetail{}, err
	}
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return OperatorConversationDetail{}, ErrSessionNotFound
	}
	caps, err := r.resolveConversationCapabilities(ctx)
	if err != nil {
		return OperatorConversationDetail{}, err
	}
	sources := sqliteOperatorConversationQuerySources(caps)
	if len(sources) == 0 {
		return OperatorConversationDetail{}, ErrSessionNotFound
	}
	row := r.db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT
			session_id,
			agent_id,
			run_id,
			kind,
			COALESCE(scope_key, ''),
			COALESCE(scope, ''),
			COALESCE(runtime_mode, ''),
			COALESCE(status, ''),
			COALESCE(turn_count, 0),
			COALESCE(message_count, 0),
			COALESCE(runtime_state, '{}'),
			COALESCE(conversation, '[]'),
			started_at,
			ended_at,
			updated_at
		FROM (
			%s
		) conversations
		WHERE session_id = ?
		LIMIT 1
	`, strings.Join(sources, "\nUNION ALL\n")), sessionID)
	item, err := scanSQLiteOperatorConversationDetail(row)
	if errors.Is(err, sql.ErrNoRows) {
		return OperatorConversationDetail{}, ErrSessionNotFound
	}
	if err != nil {
		return OperatorConversationDetail{}, operatorConversationReadQueryError("load sqlite operator conversation", err)
	}
	item.Turns, err = r.loadConversationTurns(ctx, item.Conversation.AgentID, item.Conversation.SessionID)
	if err != nil {
		return OperatorConversationDetail{}, fmt.Errorf("load sqlite operator conversation turns: %w", err)
	}
	if item.Turns == nil {
		item.Turns = []OperatorConversationTurn{}
	}
	return item, nil
}

func (r *sqliteOperatorAgentConversationReadSurface) LoadOperatorConversationTurn(ctx context.Context, sessionID string, turnIndex int) (OperatorConversationTurnDetail, error) {
	if turnIndex < 1 {
		return OperatorConversationTurnDetail{}, ErrTurnNotFound
	}
	detail, err := r.LoadOperatorConversation(ctx, sessionID)
	if err != nil {
		return OperatorConversationTurnDetail{}, err
	}
	if turnIndex > len(detail.Turns) {
		return OperatorConversationTurnDetail{}, ErrTurnNotFound
	}
	selected := detail.Turns[turnIndex-1]
	completedAt := selected.CreatedAt.UTC()
	startedAt := completedAt
	if selected.LatencyMS > 0 {
		startedAt = completedAt.Add(-time.Duration(selected.LatencyMS) * time.Millisecond)
	}
	windowStart := startedAt.Add(-time.Nanosecond)
	windowEnd := completedAt
	out := OperatorConversationTurnDetail{
		Session:               detail.Conversation,
		TurnBlocksRaw:         cloneConversationTurnBlocks(selected.TurnBlocks),
		RuntimeLogWindowStart: windowStart,
		RuntimeLogWindowEnd:   &windowEnd,
		Turn: OperatorConversationDeepTurn{
			TurnIndex:   turnIndex,
			TurnID:      selected.TurnID,
			Scope:       selected.ScopeKey,
			StartedAt:   startedAt,
			CompletedAt: completedAt,
			DurationMS:  selected.LatencyMS,
			Outcome:     selected.Outcome,
			ParseOK:     selected.ParseOK,
			Error:       selected.Error,
			RetryCount:  selected.RetryCount,
			DispatchMetadata: OperatorConversationDispatchMetadata{
				TriggerEventID:   selected.TriggerEventID,
				TriggerEventType: selected.TriggerEventType,
				EntityID:         selected.EntityID,
				TaskID:           selected.TaskID,
				RunID:            detail.Conversation.RunID,
			},
			AdvertisedTools:             cloneStrings(selected.AvailableTools),
			MCPToolsListed:              cloneStrings(selected.MCPToolsListed),
			MCPToolsVisible:             cloneStrings(selected.MCPToolsVisible),
			ReasoningBlocks:             cloneStrings(selected.ReasoningBlocks),
			ProgressUpdates:             cloneStrings(selected.ProgressUpdates),
			ToolCalls:                   cloneConversationToolCalls(selected.ToolCalls),
			ToolResults:                 cloneConversationToolResults(selected.ToolResults),
			EmittedEvents:               cloneStrings(selected.EmittedEvents),
			RuntimeLogEntries:           []OperatorRuntimeLogEntry{},
			ProviderMetadata:            OperatorConversationProviderMetadata{LatencyMS: selected.LatencyMS},
			RequestPayload:              cloneRawMessage(selected.RequestPayload),
			ResponsePayload:             cloneRawMessage(selected.ResponsePayload),
			FullPromptContext:           nil,
			FullPromptContextV2Reserved: true,
			RawLLMResponse:              nil,
			RawLLMResponseV2Reserved:    true,
			AssistantVisibleOutput:      selected.AssistantVisibleOutput,
		},
	}
	if out.Turn.AdvertisedTools == nil {
		out.Turn.AdvertisedTools = []string{}
	}
	return out, nil
}

func (r *sqliteOperatorAgentConversationReadSurface) LoadCurrentOperatorConversationForAgent(ctx context.Context, agentID string) (*OperatorConversationDetail, error) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return nil, ErrAgentNotFound
	}
	if _, err := r.LoadOperatorAgent(ctx, agentID); err != nil {
		return nil, err
	}
	return r.loadCurrentActiveOperatorConversationForAgent(ctx, agentID)
}

func (r *sqliteOperatorAgentConversationReadSurface) requireAgentCapabilities(ctx context.Context) error {
	if r == nil || r.db == nil || r.store == nil {
		return fmt.Errorf("operator agent read surface requires sqlite store")
	}
	caps, err := r.resolveConversationCapabilities(ctx)
	if err != nil {
		return err
	}
	if caps.Conversations.Turns != SchemaFlavorCanonical {
		return unsupportedSchemaCapability("agent_turns", caps.Conversations.Turns)
	}
	return RequireCanonicalPendingAgentDeliveryCapabilities(caps)
}

func (r *sqliteOperatorAgentConversationReadSurface) requireConversationCapabilities(ctx context.Context) error {
	if r == nil || r.db == nil || r.store == nil {
		return fmt.Errorf("operator conversation read surface requires sqlite store")
	}
	caps, err := r.resolveConversationCapabilities(ctx)
	if err != nil {
		return err
	}
	if caps.Conversations.Sessions != SchemaFlavorCanonical && caps.Conversations.Audits != SchemaFlavorCanonical {
		if caps.Conversations.Audits != SchemaFlavorUnavailable {
			return unsupportedSchemaCapability("agent_conversation_audits", caps.Conversations.Audits)
		}
		return unsupportedSchemaCapability("agent_sessions", caps.Conversations.Sessions)
	}
	if caps.Conversations.Turns != SchemaFlavorCanonical {
		return unsupportedSchemaCapability("agent_turns", caps.Conversations.Turns)
	}
	return nil
}

func (r *sqliteOperatorAgentConversationReadSurface) resolveConversationCapabilities(ctx context.Context) (StoreSchemaCapabilities, error) {
	if r == nil || r.store == nil {
		return StoreSchemaCapabilities{}, fmt.Errorf("operator conversation read surface requires sqlite schema capabilities")
	}
	return r.store.ResolveSchemaCapabilities(ctx)
}

func (r *sqliteOperatorAgentConversationReadSurface) loadCurrentActiveOperatorConversationForAgent(ctx context.Context, agentID string) (*OperatorConversationDetail, error) {
	caps, err := r.resolveConversationCapabilities(ctx)
	if err != nil {
		return nil, err
	}
	if caps.Conversations.Sessions != SchemaFlavorCanonical {
		return nil, unsupportedSchemaCapability("agent_sessions", caps.Conversations.Sessions)
	}
	if caps.Conversations.Turns != SchemaFlavorCanonical {
		return nil, unsupportedSchemaCapability("agent_turns", caps.Conversations.Turns)
	}
	sessionRunID := "''"
	if caps.Conversations.SessionRunID {
		sessionRunID = "COALESCE(run_id, '')"
	}
	row := r.db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT
			session_id,
			agent_id,
			%s AS run_id,
			'live_session' AS kind,
			COALESCE(scope_key, ''),
			COALESCE(scope, ''),
			COALESCE(runtime_mode, ''),
			COALESCE(status, ''),
			COALESCE(turn_count, 0),
			json_array_length(COALESCE(conversation, '[]')) AS message_count,
			COALESCE(runtime_state, '{}'),
			COALESCE(conversation, '[]'),
			created_at AS started_at,
			NULL AS ended_at,
			updated_at
		FROM agent_sessions
		WHERE agent_id = ?
		  AND status = 'active'
		  AND runtime_mode IN (?, ?)
		ORDER BY updated_at DESC, created_at DESC, session_id ASC
		LIMIT 1
	`, sessionRunID), agentID, runtimesessions.RuntimeModeSession, runtimesessions.RuntimeModeSessionPerEntity)
	item, err := scanSQLiteOperatorConversationDetail(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load current sqlite operator conversation: %w", err)
	}
	item.Turns, err = r.loadConversationTurns(ctx, item.Conversation.AgentID, item.Conversation.SessionID)
	if err != nil {
		return nil, fmt.Errorf("load current sqlite operator conversation turns: %w", err)
	}
	if item.Turns == nil {
		item.Turns = []OperatorConversationTurn{}
	}
	return &item, nil
}

func (r *sqliteOperatorAgentConversationReadSurface) loadAgentOperatorProjections(ctx context.Context) (map[string]operatorAgentProjection, error) {
	caps, err := r.resolveConversationCapabilities(ctx)
	if err != nil {
		return nil, err
	}
	turnBlocksExpr := "'[]'"
	if caps.Conversations.TurnBlocks {
		turnBlocksExpr = "COALESCE(latest_turn.turn_blocks, '[]')"
	}
	rows, err := r.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT
			a.agent_id,
			COALESCE(a.status, 'active'),
			COALESCE(sess.session_id, ''),
			sess.created_at,
			COALESCE(sess.turn_count, 0),
			COALESCE(sess.lease_holder, ''),
			sess.lease_expires_at,
			COALESCE(sess.runtime_state, '{}'),
			0,
			0,
			COALESCE(latest_turn.turn_id, ''),
			COALESCE(latest_turn.task_id, ''),
			COALESCE(latest_turn.entity_id, ''),
			COALESCE(latest_turn.parse_ok, 0),
			COALESCE(latest_turn.error, ''),
			latest_turn.created_at,
			%s AS turn_blocks
		FROM agents a
		LEFT JOIN agent_sessions sess ON sess.session_id = (
			SELECT session_id
			FROM agent_sessions s
			WHERE s.agent_id = a.agent_id
			  AND s.status = 'active'
			  AND s.runtime_mode IN (?, ?)
			ORDER BY s.updated_at DESC, s.created_at DESC, s.session_id ASC
			LIMIT 1
		)
		LEFT JOIN agent_turns latest_turn ON latest_turn.turn_id = (
			SELECT turn_id
			FROM agent_turns t
			WHERE t.agent_id = a.agent_id
			  AND sess.session_id IS NOT NULL
			  AND t.session_id = sess.session_id
			ORDER BY t.created_at DESC, t.turn_id DESC
			LIMIT 1
		)
		WHERE a.status NOT IN ('terminated', 'ephemeral')
		ORDER BY a.created_at ASC, a.agent_id ASC
	`, turnBlocksExpr), runtimesessions.RuntimeModeSession, runtimesessions.RuntimeModeSessionPerEntity)
	if err != nil {
		return nil, fmt.Errorf("query sqlite agent operator projections: %w", err)
	}
	defer rows.Close()

	out := map[string]operatorAgentProjection{}
	agentIDs := make([]string, 0)
	for rows.Next() {
		var (
			id                 string
			projection         operatorAgentProjection
			lockExpiresAtRaw   any
			sessionStartedRaw  any
			runtimeStateRaw    []byte
			latestTurnID       string
			latestTaskID       string
			latestEntityID     string
			latestParseOK      bool
			latestError        string
			latestCompletedRaw any
			latestTurnRaw      []byte
		)
		if err := rows.Scan(
			&id,
			&projection.Status,
			&projection.SessionID,
			&sessionStartedRaw,
			&projection.TurnCount,
			&projection.LockOwner,
			&lockExpiresAtRaw,
			&runtimeStateRaw,
			&projection.PendingEvents,
			&projection.OldestPendingAgeSec,
			&latestTurnID,
			&latestTaskID,
			&latestEntityID,
			&latestParseOK,
			&latestError,
			&latestCompletedRaw,
			&latestTurnRaw,
		); err != nil {
			return nil, fmt.Errorf("scan sqlite agent operator projection: %w", err)
		}
		if at, ok, err := sqliteTimeValue(sessionStartedRaw); err != nil {
			return nil, fmt.Errorf("scan sqlite agent session started_at: %w", err)
		} else if ok {
			projection.SessionStartedAt = at
		}
		if at, ok, err := sqliteTimeValue(lockExpiresAtRaw); err != nil {
			return nil, fmt.Errorf("scan sqlite agent session lease_expires_at: %w", err)
		} else if ok {
			projection.LockExpiresAt = at
		}
		latestCompleted, latestCompletedOK, err := sqliteTimeValue(latestCompletedRaw)
		if err != nil {
			return nil, fmt.Errorf("scan sqlite latest turn created_at: %w", err)
		}
		if latestCompletedOK && strings.TrimSpace(latestTurnID) != "" {
			projection.LastTurnRef = &OperatorTurnRef{
				TurnID:      strings.TrimSpace(latestTurnID),
				CompletedAt: latestCompleted,
				ParseOK:     latestParseOK,
				Error:       strings.TrimSpace(latestError),
			}
		}
		if err := enrichOperatorAgentProjectionFromLatestTurn(&projection, runtimeStateRaw, latestTurnID, latestTaskID, latestParseOK, latestTurnRaw); err != nil {
			return nil, err
		}
		projection.DiagnosisActive = operatorAgentDiagnosisActiveFromLatestTurn(latestTurnID, latestTaskID, latestEntityID)
		id = strings.TrimSpace(id)
		out[id] = projection
		agentIDs = append(agentIDs, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read sqlite agent operator projection rows: %w", err)
	}
	factsByAgent, err := r.store.ListPendingAgentDeliveryFacts(ctx, agentIDs, time.Time{})
	if err != nil {
		return nil, err
	}
	lifecycleByAgent, err := r.store.ListAgentLifecycleFacts(ctx, agentIDs)
	if err != nil {
		return nil, err
	}
	for agentID, facts := range factsByAgent {
		projection := out[strings.TrimSpace(agentID)]
		projection.PendingEvents = facts.PendingCount
		projection.OldestPendingAgeSec = facts.OldestPendingAgeSec
		out[strings.TrimSpace(agentID)] = projection
	}
	for agentID, facts := range lifecycleByAgent {
		projection := out[strings.TrimSpace(agentID)]
		projection.LifecycleState = strings.TrimSpace(facts.CurrentState)
		projection.BlockingLayer = strings.TrimSpace(facts.BlockingLayer)
		out[strings.TrimSpace(agentID)] = projection
	}
	return out, nil
}

func (r *sqliteOperatorAgentConversationReadSurface) loadConversationTurns(ctx context.Context, agentID, sessionID string) ([]OperatorConversationTurn, error) {
	agentID = strings.TrimSpace(agentID)
	sessionID = strings.TrimSpace(sessionID)
	if agentID == "" || sessionID == "" {
		return []OperatorConversationTurn{}, nil
	}
	caps, err := r.resolveConversationCapabilities(ctx)
	if err != nil {
		return nil, err
	}
	if caps.Conversations.Turns != SchemaFlavorCanonical {
		return nil, unsupportedSchemaCapability("agent_turns", caps.Conversations.Turns)
	}
	turnBlocksExpr := "COALESCE(turn_blocks, '[]')"
	if !caps.Conversations.TurnBlocks {
		turnBlocksExpr = "'[]'"
	}
	rows, err := r.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT
			turn_id,
			agent_id,
			session_id,
			COALESCE(runtime_mode, ''),
			COALESCE(scope_key, ''),
			COALESCE(entity_id, ''),
			COALESCE(trigger_event_id, ''),
			COALESCE(trigger_event_type, ''),
			COALESCE(task_id, ''),
			COALESCE(available_tools, '[]'),
			COALESCE(tool_calls, '[]'),
			COALESCE(emitted_events, '[]'),
			COALESCE(mcp_servers, '{}'),
			COALESCE(mcp_tools_listed, '[]'),
			COALESCE(mcp_tools_visible, '[]'),
			COALESCE(request_payload, '{}'),
			COALESCE(response_payload, '{}'),
			%s AS turn_blocks,
			parse_ok,
			COALESCE(latency_ms, 0),
			COALESCE(retry_count, 0),
			COALESCE(error, ''),
			created_at
		FROM agent_turns
		WHERE agent_id = ?
		  AND session_id = ?
		ORDER BY created_at ASC, turn_id ASC
	`, turnBlocksExpr), agentID, sessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []OperatorConversationTurn{}
	for rows.Next() {
		item, err := scanSQLiteOperatorConversationTurn(rows)
		if err != nil {
			return nil, err
		}
		item.TurnIndex = len(out) + 1
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func scanSQLiteOperatorConversationSummary(scanner operatorRowScanner) (OperatorConversationSummary, error) {
	var (
		item            OperatorConversationSummary
		runtimeStateRaw []byte
		turnID          string
		taskID          string
		parseOK         bool
		turnBlocksRaw   []byte
		startedAtRaw    any
		endedAtRaw      any
		updatedAtRaw    any
	)
	if err := scanner.Scan(
		&item.SessionID,
		&item.AgentID,
		&item.RunID,
		&item.Kind,
		&item.ScopeKey,
		&item.Scope,
		&item.RuntimeMode,
		&item.Status,
		&item.TurnCount,
		&item.MessageCount,
		&runtimeStateRaw,
		&turnID,
		&taskID,
		&parseOK,
		&turnBlocksRaw,
		&startedAtRaw,
		&endedAtRaw,
		&updatedAtRaw,
	); err != nil {
		return OperatorConversationSummary{}, err
	}
	if at, ok, err := sqliteTimeValue(startedAtRaw); err != nil {
		return OperatorConversationSummary{}, fmt.Errorf("scan sqlite conversation started_at: %w", err)
	} else if ok {
		item.StartedAt = at
	}
	if at, ok, err := sqliteTimeValue(endedAtRaw); err != nil {
		return OperatorConversationSummary{}, fmt.Errorf("scan sqlite conversation ended_at: %w", err)
	} else if ok {
		item.EndedAt = &at
	}
	if at, ok, err := sqliteTimeValue(updatedAtRaw); err != nil {
		return OperatorConversationSummary{}, fmt.Errorf("scan sqlite conversation updated_at: %w", err)
	} else if ok {
		item.UpdatedAt = at
	}
	runtimeState, err := DecodeConversationRuntimeStateDescriptor(runtimeStateRaw)
	if err != nil {
		return OperatorConversationSummary{}, fmt.Errorf("decode conversation runtime_state: %w", err)
	}
	item.Summary = runtimeState.Summary
	item.Metadata = projectOperatorConversationSummaryMetadata(runtimeState)
	item.Metadata.LiveTurn, err = projectOperatorLatestTurn(taskID, parseOK, turnID, turnBlocksRaw)
	if err != nil {
		return OperatorConversationSummary{}, fmt.Errorf("decode conversation live_turn: %w", err)
	}
	return item, nil
}

func scanSQLiteOperatorConversationDetail(scanner operatorRowScanner) (OperatorConversationDetail, error) {
	var (
		item            OperatorConversationDetail
		runtimeStateRaw []byte
		messagesRaw     []byte
		startedAtRaw    any
		endedAtRaw      any
		updatedAtRaw    any
	)
	if err := scanner.Scan(
		&item.Conversation.SessionID,
		&item.Conversation.AgentID,
		&item.Conversation.RunID,
		&item.Conversation.Kind,
		&item.Conversation.ScopeKey,
		&item.Conversation.Scope,
		&item.Conversation.RuntimeMode,
		&item.Conversation.Status,
		&item.Conversation.TurnCount,
		&item.Conversation.MessageCount,
		&runtimeStateRaw,
		&messagesRaw,
		&startedAtRaw,
		&endedAtRaw,
		&updatedAtRaw,
	); err != nil {
		return OperatorConversationDetail{}, err
	}
	if at, ok, err := sqliteTimeValue(startedAtRaw); err != nil {
		return OperatorConversationDetail{}, fmt.Errorf("scan sqlite conversation started_at: %w", err)
	} else if ok {
		item.Conversation.StartedAt = at
	}
	if at, ok, err := sqliteTimeValue(endedAtRaw); err != nil {
		return OperatorConversationDetail{}, fmt.Errorf("scan sqlite conversation ended_at: %w", err)
	} else if ok {
		item.Conversation.EndedAt = &at
	}
	if at, ok, err := sqliteTimeValue(updatedAtRaw); err != nil {
		return OperatorConversationDetail{}, fmt.Errorf("scan sqlite conversation updated_at: %w", err)
	} else if ok {
		item.Conversation.UpdatedAt = at
	}
	runtimeState, err := DecodeConversationRuntimeStateDescriptor(runtimeStateRaw)
	if err != nil {
		return OperatorConversationDetail{}, fmt.Errorf("decode conversation runtime_state: %w", err)
	}
	item.Conversation.Summary = runtimeState.Summary
	item.Conversation.Metadata = projectOperatorConversationSummaryMetadata(runtimeState)
	item.RuntimeState = projectOperatorConversationState(runtimeState)
	item.Messages, err = decodeStoreJSONArray[OperatorConversationMessage](messagesRaw)
	if err != nil {
		return OperatorConversationDetail{}, fmt.Errorf("decode conversation messages: %w", err)
	}
	if item.Messages == nil {
		item.Messages = []OperatorConversationMessage{}
	}
	return item, nil
}

func scanSQLiteOperatorConversationTurn(scanner operatorRowScanner) (OperatorConversationTurn, error) {
	var (
		item                                  OperatorConversationTurn
		availableToolsRaw, toolCallsRaw       []byte
		emittedEventsRaw, mcpServersRaw       []byte
		mcpToolsListedRaw, mcpToolsVisibleRaw []byte
		requestPayloadRaw, responsePayloadRaw []byte
		turnBlocksRaw                         []byte
		createdAtRaw                          any
	)
	if err := scanner.Scan(
		&item.TurnID,
		&item.AgentID,
		&item.SessionID,
		&item.RuntimeMode,
		&item.ScopeKey,
		&item.EntityID,
		&item.TriggerEventID,
		&item.TriggerEventType,
		&item.TaskID,
		&availableToolsRaw,
		&toolCallsRaw,
		&emittedEventsRaw,
		&mcpServersRaw,
		&mcpToolsListedRaw,
		&mcpToolsVisibleRaw,
		&requestPayloadRaw,
		&responsePayloadRaw,
		&turnBlocksRaw,
		&item.ParseOK,
		&item.LatencyMS,
		&item.RetryCount,
		&item.Error,
		&createdAtRaw,
	); err != nil {
		return OperatorConversationTurn{}, err
	}
	if at, ok, err := sqliteTimeValue(createdAtRaw); err != nil {
		return OperatorConversationTurn{}, fmt.Errorf("scan sqlite conversation turn created_at: %w", err)
	} else if ok {
		item.CreatedAt = at
	}
	summary, hasSummary, err := decodeOperatorTurnSummaryProjection(turnBlocksRaw)
	if err != nil {
		return OperatorConversationTurn{}, err
	}
	if item.AvailableTools, err = decodeStoreJSONArray[string](availableToolsRaw); err != nil {
		return OperatorConversationTurn{}, fmt.Errorf("decode turn available_tools: %w", err)
	}
	if item.ToolCalls, err = decodeStoreJSONArray[OperatorConversationToolCall](toolCallsRaw); err != nil {
		return OperatorConversationTurn{}, fmt.Errorf("decode turn tool_calls: %w", err)
	}
	if item.EmittedEvents, err = decodeStoreJSONArray[string](emittedEventsRaw); err != nil {
		return OperatorConversationTurn{}, fmt.Errorf("decode turn emitted_events: %w", err)
	}
	if item.MCPToolsListed, err = decodeStoreJSONArray[string](mcpToolsListedRaw); err != nil {
		return OperatorConversationTurn{}, fmt.Errorf("decode turn mcp_tools_listed: %w", err)
	}
	if item.MCPToolsVisible, err = decodeStoreJSONArray[string](mcpToolsVisibleRaw); err != nil {
		return OperatorConversationTurn{}, fmt.Errorf("decode turn mcp_tools_visible: %w", err)
	}
	if item.MCPServers, err = decodeStoreJSONStringMap(mcpServersRaw); err != nil {
		return OperatorConversationTurn{}, fmt.Errorf("decode turn mcp_servers: %w", err)
	}
	if item.RequestPayload, err = decodeStoreJSONObjectRaw(requestPayloadRaw); err != nil {
		return OperatorConversationTurn{}, fmt.Errorf("decode turn request_payload: %w", err)
	}
	if item.ResponsePayload, err = decodeStoreJSONObjectRaw(responsePayloadRaw); err != nil {
		return OperatorConversationTurn{}, fmt.Errorf("decode turn response_payload: %w", err)
	}
	if len(turnBlocksRaw) > 0 {
		if _, err := runtimellm.DecodeCanonicalRuntimeLogTurnBlocksJSON(turnBlocksRaw); err != nil {
			return OperatorConversationTurn{}, fmt.Errorf("decode canonical runtime_log turn_blocks: %w", err)
		}
		if item.TurnBlocks, err = decodeStoreJSONArray[OperatorConversationTurnBlock](turnBlocksRaw); err != nil {
			return OperatorConversationTurn{}, fmt.Errorf("decode turn turn_blocks: %w", err)
		}
	}
	if hasSummary {
		item.AssistantVisibleOutput, item.Outcome, item.ReasoningBlocks, item.ProgressUpdates, item.ToolResults = projectedOperatorTurnSummaryConversationFields(summary)
	}
	return item, nil
}

func sqliteOperatorConversationQuerySources(caps StoreSchemaCapabilities) []string {
	sources := []string{}
	if caps.Conversations.Sessions == SchemaFlavorCanonical {
		sessionRunID := "''"
		if caps.Conversations.SessionRunID {
			sessionRunID = "COALESCE(run_id, '')"
		}
		sources = append(sources, fmt.Sprintf(`
			SELECT
				session_id AS session_id,
				agent_id,
				%s AS run_id,
				'live_session' AS kind,
				scope_key,
				scope,
				runtime_mode,
				CASE WHEN status = 'terminated' THEN 'terminated' ELSE 'active' END AS status,
				turn_count,
				json_array_length(COALESCE(conversation, '[]')) AS message_count,
				runtime_state,
				conversation,
				created_at AS started_at,
				CASE WHEN status = 'terminated' THEN terminated_at ELSE NULL END AS ended_at,
				updated_at,
				created_at
			FROM agent_sessions
			WHERE status IN ('active', 'terminated')
			  AND runtime_mode IN ('session', 'session_per_entity')
		`, sessionRunID))
	}
	if caps.Conversations.Audits == SchemaFlavorCanonical {
		auditRunID := "''"
		if caps.Conversations.AuditRunID {
			auditRunID = "COALESCE(run_id, '')"
		}
		sources = append(sources, fmt.Sprintf(`
			SELECT
				session_id AS session_id,
				agent_id,
				%s AS run_id,
				'turn_audit' AS kind,
				COALESCE(scope_key, '') AS scope_key,
				COALESCE(scope, '') AS scope,
				COALESCE(runtime_mode, '') AS runtime_mode,
				CASE WHEN status = 'terminated' THEN 'terminated' ELSE 'active' END AS status,
				COALESCE(turn_count, 0) AS turn_count,
				json_array_length(COALESCE(conversation, '[]')) AS message_count,
				COALESCE(runtime_state, '{}') AS runtime_state,
				COALESCE(conversation, '[]') AS conversation,
				created_at AS started_at,
				NULL AS ended_at,
				updated_at,
				created_at
			FROM agent_conversation_audits
			WHERE status = 'active'
		`, auditRunID))
	}
	return sources
}

func (r *sqliteOperatorAgentConversationReadSurface) LoadOperatorAgentDeliveryDiagnostics(ctx context.Context, agentID string, opts OperatorAgentDeliveryDiagnosticsOptions) (OperatorAgentDeliveryDiagnostics, error) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return OperatorAgentDeliveryDiagnostics{}, ErrAgentNotFound
	}
	if err := r.requireAgentDeliveryDiagnosticsCapabilities(ctx); err != nil {
		return OperatorAgentDeliveryDiagnostics{}, err
	}
	if err := r.ensureAgentDeliveryDiagnosticsAgentExists(ctx, agentID); err != nil {
		return OperatorAgentDeliveryDiagnostics{}, err
	}

	opts = defaultOperatorAgentDeliveryDiagnosticsOptions(opts)
	summary, err := r.loadAgentDeliveryDiagnosticsSummary(ctx, agentID)
	if err != nil {
		return OperatorAgentDeliveryDiagnostics{}, err
	}
	failures, failuresNext, err := r.listAgentDeliveryFailures(ctx, agentID, opts.FailureLimit, opts.FailureCursor)
	if err != nil {
		return OperatorAgentDeliveryDiagnostics{}, err
	}
	if err := r.assertAgentDeadLetterDeliveriesHaveRecords(ctx, agentID); err != nil {
		return OperatorAgentDeliveryDiagnostics{}, err
	}
	deadLetters, deadLettersNext, err := r.listAgentDeadLetterDeliveries(ctx, agentID, opts.DeadLetterLimit, opts.DeadLetterCursor)
	if err != nil {
		return OperatorAgentDeliveryDiagnostics{}, err
	}
	result := OperatorAgentDeliveryDiagnostics{
		AgentID:               agentID,
		Summary:               summary,
		Failures:              failures,
		FailuresNextCursor:    failuresNext,
		DeadLetters:           deadLetters,
		DeadLettersNextCursor: deadLettersNext,
	}
	if result.Failures == nil {
		result.Failures = []OperatorAgentDeliveryFailure{}
	}
	if result.DeadLetters == nil {
		result.DeadLetters = []OperatorAgentDeadLetterDelivery{}
	}
	return result, nil
}

func (r *sqliteOperatorAgentConversationReadSurface) requireAgentDeliveryDiagnosticsCapabilities(ctx context.Context) error {
	if r == nil || r.db == nil {
		return fmt.Errorf("operator agent delivery diagnostics read owner requires sqlite store")
	}
	caps, err := r.resolveConversationCapabilities(ctx)
	if err != nil {
		return err
	}
	switch {
	case caps.Agents != SchemaFlavorCanonical:
		return unsupportedSchemaCapability("agents", caps.Agents)
	case caps.Events.Log != SchemaFlavorCanonical:
		return unsupportedSchemaCapability("events", caps.Events.Log)
	case !caps.Events.LogRunID:
		return fmt.Errorf("agent delivery diagnostics read owner requires canonical events.run_id")
	case caps.Events.Deliveries != SchemaFlavorCanonical:
		return unsupportedSchemaCapability("event_deliveries", caps.Events.Deliveries)
	}
	catalog, err := loadSQLiteSchemaColumnCatalog(ctx, r.db)
	if err != nil {
		return err
	}
	required := map[string][]string{
		"agents":           {"agent_id", "status"},
		"events":           {"event_id", "run_id", "event_name", "entity_id", "created_at"},
		"event_deliveries": {"delivery_id", "event_id", "subscriber_type", "subscriber_id", "status", "retry_count", "reason_code", "last_error", "delivered_at", "created_at"},
		"dead_letters":     {"dead_letter_id", "original_event_id", "failure_type", "error_message", "retry_count", "chain_depth", "handler_node", "created_at"},
	}
	for table, columns := range required {
		if !catalog.hasColumns(table, columns...) {
			return fmt.Errorf("agent delivery diagnostics read owner requires canonical %s columns: %s", table, strings.Join(columns, ", "))
		}
	}
	return nil
}

func (r *sqliteOperatorAgentConversationReadSurface) ensureAgentDeliveryDiagnosticsAgentExists(ctx context.Context, agentID string) error {
	var exists bool
	if err := r.db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM agents
			WHERE agent_id = ?
			  AND status NOT IN ('terminated', 'ephemeral')
		)
	`, agentID).Scan(&exists); err != nil {
		return fmt.Errorf("load sqlite agent delivery diagnostics agent: %w", err)
	}
	if !exists {
		return ErrAgentNotFound
	}
	return nil
}

func (r *sqliteOperatorAgentConversationReadSurface) loadAgentDeliveryDiagnosticsSummary(ctx context.Context, agentID string) (OperatorAgentDeliveryDiagnosticsSummary, error) {
	var summary OperatorAgentDeliveryDiagnosticsSummary
	if err := r.db.QueryRowContext(ctx, `
		SELECT
			COALESCE(SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN status = 'dead_letter' THEN 1 ELSE 0 END), 0)
		FROM event_deliveries
		WHERE subscriber_type = 'agent'
		  AND subscriber_id = ?
		  AND COALESCE(delivered_at, created_at) >= datetime('now', '-24 hours')
	`, agentID).Scan(&summary.Failures24h, &summary.DeadLetters24h); err != nil {
		return OperatorAgentDeliveryDiagnosticsSummary{}, fmt.Errorf("load sqlite agent delivery diagnostics summary: %w", err)
	}
	return summary, nil
}

func (r *sqliteOperatorAgentConversationReadSurface) listAgentDeliveryFailures(ctx context.Context, agentID string, limit int, cursorRaw string) ([]OperatorAgentDeliveryFailure, string, error) {
	cursorClause, args, err := sqliteAgentDeliveryDiagnosticsCursorClause(agentID, cursorRaw, "agent.delivery_diagnostics.failures", "failure_cursor")
	if err != nil {
		return nil, "", err
	}
	args = append(args, limit+1)
	rows, err := r.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT
			d.delivery_id,
			d.event_id,
			COALESCE(e.event_name, ''),
			COALESCE(e.run_id, ''),
			COALESCE(e.entity_id, ''),
			COALESCE(d.reason_code, ''),
			COALESCE(d.last_error, ''),
			COALESCE(d.retry_count, 0),
			COALESCE(d.delivered_at, d.created_at) AS occurred_at
		FROM event_deliveries d
		INNER JOIN events e ON e.event_id = d.event_id
		WHERE d.subscriber_type = 'agent'
		  AND d.subscriber_id = ?
		  AND d.status = 'failed'
		  %s
		ORDER BY occurred_at DESC, d.delivery_id DESC
		LIMIT ?
	`, cursorClause), args...)
	if err != nil {
		return nil, "", fmt.Errorf("list sqlite agent delivery failures: %w", err)
	}
	defer rows.Close()

	out := []OperatorAgentDeliveryFailure{}
	for rows.Next() {
		var (
			item          OperatorAgentDeliveryFailure
			occurredAtRaw any
		)
		if err := rows.Scan(
			&item.DeliveryID,
			&item.EventID,
			&item.EventName,
			&item.RunID,
			&item.EntityID,
			&item.ReasonCode,
			&item.LastError,
			&item.RetryCount,
			&occurredAtRaw,
		); err != nil {
			return nil, "", fmt.Errorf("scan sqlite agent delivery failure: %w", err)
		}
		if at, ok, err := sqliteTimeValue(occurredAtRaw); err != nil {
			return nil, "", fmt.Errorf("scan sqlite agent delivery failure occurred_at: %w", err)
		} else if ok {
			item.OccurredAt = at
		}
		item.Status = "failed"
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("read sqlite agent delivery failures: %w", err)
	}
	nextCursor := ""
	if len(out) > limit {
		nextCursor = encodeAgentDeliveryDiagnosticsCursor("agent.delivery_diagnostics.failures", out[limit-1].OccurredAt, out[limit-1].DeliveryID)
		out = out[:limit]
	}
	return out, nextCursor, nil
}

func (r *sqliteOperatorAgentConversationReadSurface) assertAgentDeadLetterDeliveriesHaveRecords(ctx context.Context, agentID string) error {
	var deliveryID string
	err := r.db.QueryRowContext(ctx, `
		SELECT d.delivery_id
		FROM event_deliveries d
		WHERE d.subscriber_type = 'agent'
		  AND d.subscriber_id = ?
		  AND d.status = 'dead_letter'
		  AND NOT EXISTS (
		  	SELECT 1
		  	FROM dead_letters dl
		  	WHERE dl.original_event_id = d.event_id
		  )
		ORDER BY COALESCE(d.delivered_at, d.created_at) DESC, d.delivery_id DESC
		LIMIT 1
	`, agentID).Scan(&deliveryID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("check sqlite agent dead-letter delivery reconciliation: %w", err)
	}
	return fmt.Errorf("agent delivery diagnostics owner found dead_letter delivery %s without a dead_letters record", deliveryID)
}

func (r *sqliteOperatorAgentConversationReadSurface) listAgentDeadLetterDeliveries(ctx context.Context, agentID string, limit int, cursorRaw string) ([]OperatorAgentDeadLetterDelivery, string, error) {
	cursorClause, args, err := sqliteAgentDeliveryDiagnosticsCursorClause(agentID, cursorRaw, "agent.delivery_diagnostics.dead_letters", "dead_letter_cursor")
	if err != nil {
		return nil, "", err
	}
	args = append(args, limit+1)
	rows, err := r.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT
			d.delivery_id,
			d.event_id,
			COALESCE(e.event_name, ''),
			COALESCE(e.run_id, ''),
			COALESCE(e.entity_id, ''),
			COALESCE(d.reason_code, ''),
			COALESCE(d.last_error, ''),
			COALESCE(d.retry_count, 0),
			COALESCE(d.delivered_at, d.created_at) AS occurred_at,
			COALESCE((
				SELECT json_group_array(json_object(
					'dead_letter_id', dead_letter_id,
					'failure_type', COALESCE(failure_type, ''),
					'error_message', COALESCE(error_message, ''),
					'retry_count', COALESCE(retry_count, 0),
					'chain_depth', COALESCE(chain_depth, 0),
					'handler_node', COALESCE(handler_node, ''),
					'created_at', strftime('%%Y-%%m-%%dT%%H:%%M:%%fZ', created_at)
				))
				FROM (
					SELECT *
					FROM dead_letters dl
					WHERE dl.original_event_id = d.event_id
					ORDER BY dl.created_at ASC, dl.dead_letter_id ASC
				) ordered_dead_letters
			), '[]') AS dead_letter_records
		FROM event_deliveries d
		INNER JOIN events e ON e.event_id = d.event_id
		WHERE d.subscriber_type = 'agent'
		  AND d.subscriber_id = ?
		  AND d.status = 'dead_letter'
		  %s
		ORDER BY occurred_at DESC, d.delivery_id DESC
		LIMIT ?
	`, cursorClause), args...)
	if err != nil {
		return nil, "", fmt.Errorf("list sqlite agent dead-letter deliveries: %w", err)
	}
	defer rows.Close()

	out := []OperatorAgentDeadLetterDelivery{}
	for rows.Next() {
		var (
			item          OperatorAgentDeadLetterDelivery
			occurredAtRaw any
			recordsRaw    []byte
		)
		if err := rows.Scan(
			&item.DeliveryID,
			&item.EventID,
			&item.EventName,
			&item.RunID,
			&item.EntityID,
			&item.ReasonCode,
			&item.LastError,
			&item.RetryCount,
			&occurredAtRaw,
			&recordsRaw,
		); err != nil {
			return nil, "", fmt.Errorf("scan sqlite agent dead-letter delivery: %w", err)
		}
		if at, ok, err := sqliteTimeValue(occurredAtRaw); err != nil {
			return nil, "", fmt.Errorf("scan sqlite agent dead-letter occurred_at: %w", err)
		} else if ok {
			item.OccurredAt = at
		}
		item.Status = "dead_letter"
		if err := json.Unmarshal(recordsRaw, &item.DeadLetterRecords); err != nil {
			return nil, "", fmt.Errorf("decode sqlite agent dead-letter records: %w", err)
		}
		if len(item.DeadLetterRecords) == 0 {
			return nil, "", fmt.Errorf("agent delivery diagnostics owner returned dead_letter delivery %s without a dead_letters record", item.DeliveryID)
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("read sqlite agent dead-letter deliveries: %w", err)
	}
	nextCursor := ""
	if len(out) > limit {
		nextCursor = encodeAgentDeliveryDiagnosticsCursor("agent.delivery_diagnostics.dead_letters", out[limit-1].OccurredAt, out[limit-1].DeliveryID)
		out = out[:limit]
	}
	return out, nextCursor, nil
}

func sqliteAgentDeliveryDiagnosticsCursorClause(agentID, rawCursor, kind, field string) (string, []any, error) {
	args := []any{agentID}
	rawCursor = strings.TrimSpace(rawCursor)
	if rawCursor == "" {
		return "", args, nil
	}
	cursor, err := decodeAgentDeliveryDiagnosticsCursor(rawCursor, kind, field)
	if err != nil {
		return "", nil, err
	}
	occurredAt, err := time.Parse(time.RFC3339Nano, cursor.OccurredAt)
	if err != nil || strings.TrimSpace(cursor.DeliveryID) == "" {
		return "", nil, AgentDeliveryDiagnosticsCursorError{Field: field}
	}
	args = append(args, occurredAt.UTC(), occurredAt.UTC(), strings.TrimSpace(cursor.DeliveryID))
	return "AND (COALESCE(d.delivered_at, d.created_at) < ? OR (COALESCE(d.delivered_at, d.created_at) = ? AND d.delivery_id < ?))", args, nil
}
