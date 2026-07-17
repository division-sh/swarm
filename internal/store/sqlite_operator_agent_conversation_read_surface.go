package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
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

func (s *SQLiteRuntimeStore) ListAgentDeliveryLifecycleFacts(ctx context.Context, agentIDs []string) (map[string]AgentDeliveryLifecycleFacts, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	normalized := normalizePendingAgentIDs(agentIDs)
	out := make(map[string]AgentDeliveryLifecycleFacts, len(normalized))
	for _, agentID := range normalized {
		out[agentID] = AgentDeliveryLifecycleFacts{}
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
		out[agentID] = canonicalAgentDeliveryLifecycleFactsFromRecords(grouped[agentID])
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
	if err := r.requireAgentAccess(); err != nil {
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
	if err := r.requireConversationAccess(); err != nil {
		return OperatorConversationListResult{}, err
	}
	opts, err := defaultOperatorConversationListOptions(opts)
	if err != nil {
		return OperatorConversationListResult{}, err
	}
	sources := sqliteOperatorConversationQuerySources()
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
	args = append(args, opts.Limit+1)
	rows, err := r.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT
			conversations.session_id,
			conversations.agent_id,
			conversations.run_id,
			conversations.kind,
			COALESCE(conversations.flow_instance, ''),
			conversations.memory_enabled,
			conversations.memory_source,
			COALESCE(conversations.status, ''),
			COALESCE(conversations.turn_count, 0),
			COALESCE(conversations.message_count, 0),
			COALESCE(conversations.runtime_state, '{}'),
			conversations.started_at,
			conversations.ended_at,
			conversations.updated_at
		FROM (
			%s
		) conversations
		WHERE %s
		ORDER BY conversations.updated_at DESC, conversations.session_id ASC
		LIMIT ?
	`, strings.Join(sources, "\nUNION ALL\n"), strings.Join(where, " AND ")), args...)
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
		turn, err := loadOperatorLatestConversationTurn(ctx, r.store, item.SessionID)
		if err != nil {
			return OperatorConversationListResult{}, err
		}
		item.Metadata.LiveTurn = operatorLiveTurnFromPublic(turn)
		if turn != nil {
			item.ExecutionMode = turn.ExecutionMode
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

func (r *sqliteOperatorAgentConversationReadSurface) requireAgentAccess() error {
	if r == nil || r.db == nil || r.store == nil {
		return fmt.Errorf("operator agent read surface requires sqlite store")
	}
	return r.store.requireCurrentSchema()
}

func (r *sqliteOperatorAgentConversationReadSurface) requireConversationAccess() error {
	if r == nil || r.db == nil || r.store == nil {
		return fmt.Errorf("operator conversation read surface requires sqlite store")
	}
	return r.store.requireCurrentSchema()
}

func (r *sqliteOperatorAgentConversationReadSurface) loadAgentOperatorProjections(ctx context.Context) (map[string]operatorAgentProjection, error) {
	rows, err := r.db.QueryContext(ctx, `
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
				0
		FROM agents a
		LEFT JOIN agent_sessions sess ON sess.session_id = (
			SELECT session_id
			FROM agent_sessions s
			WHERE s.agent_id = a.agent_id
			  AND s.status = 'active'
			  AND s.memory_enabled = 1
			ORDER BY s.updated_at DESC, s.created_at DESC, s.session_id ASC
			LIMIT 1
		)
			WHERE a.status NOT IN ('terminated', 'ephemeral')
			ORDER BY a.created_at ASC, a.agent_id ASC
		`)
	if err != nil {
		return nil, fmt.Errorf("query sqlite agent operator projections: %w", err)
	}
	defer rows.Close()

	out := map[string]operatorAgentProjection{}
	agentIDs := make([]string, 0)
	for rows.Next() {
		var (
			id                string
			projection        operatorAgentProjection
			lockExpiresAtRaw  any
			sessionStartedRaw any
			runtimeStateRaw   []byte
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
		if err := enrichOperatorAgentProjectionRuntimeState(&projection, runtimeStateRaw); err != nil {
			return nil, err
		}
		if projection.SessionID != "" {
			turn, err := loadOperatorLatestConversationTurn(ctx, r.store, projection.SessionID)
			if err != nil {
				return nil, fmt.Errorf("load sqlite latest agent turn: %w", err)
			}
			enrichOperatorProjectionWithPublicTurn(&projection, turn)
		}
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
	lifecycleByAgent, err := r.store.ListAgentDeliveryLifecycleFacts(ctx, agentIDs)
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

func scanSQLiteOperatorConversationSummary(scanner operatorRowScanner) (OperatorConversationSummary, error) {
	var (
		item            OperatorConversationSummary
		runtimeStateRaw []byte
		startedAtRaw    any
		endedAtRaw      any
		updatedAtRaw    any
	)
	if err := scanner.Scan(
		&item.SessionID,
		&item.AgentID,
		&item.RunID,
		&item.Kind,
		&item.FlowInstance,
		&item.Memory,
		&item.MemorySource,
		&item.Status,
		&item.TurnCount,
		&item.MessageCount,
		&runtimeStateRaw,
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
	return item, nil
}

func sqliteOperatorConversationQuerySources() []string {
	return []string{`
			SELECT
				session_id AS session_id,
				agent_id,
				COALESCE(run_id, '') AS run_id,
				'live_session' AS kind,
				flow_instance,
				memory_enabled,
				memory_source,
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
			WHERE status IN ('active', 'terminated') AND memory_enabled = 1
		`, `
			SELECT
				session_id AS session_id,
				agent_id,
				COALESCE(run_id, '') AS run_id,
				'turn_audit' AS kind,
				COALESCE(flow_instance, '') AS flow_instance,
				memory_enabled,
				memory_source,
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
		`}
}

func (r *sqliteOperatorAgentConversationReadSurface) LoadOperatorAgentDeliveryDiagnostics(ctx context.Context, agentID string, opts OperatorAgentDeliveryDiagnosticsOptions) (OperatorAgentDeliveryDiagnostics, error) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return OperatorAgentDeliveryDiagnostics{}, ErrAgentNotFound
	}
	if err := r.requireAgentDeliveryDiagnosticsAccess(); err != nil {
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

func (r *sqliteOperatorAgentConversationReadSurface) requireAgentDeliveryDiagnosticsAccess() error {
	if r == nil || r.db == nil {
		return fmt.Errorf("operator agent delivery diagnostics read owner requires sqlite store")
	}
	return r.store.requireCurrentSchema()
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
			COALESCE(d.failure, 'null'),
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
			rawFailure    any
			occurredAtRaw any
		)
		if err := rows.Scan(
			&item.DeliveryID,
			&item.EventID,
			&item.EventName,
			&item.RunID,
			&item.EntityID,
			&item.ReasonCode,
			&rawFailure,
			&item.RetryCount,
			&occurredAtRaw,
		); err != nil {
			return nil, "", fmt.Errorf("scan sqlite agent delivery failure: %w", err)
		}
		item.Failure, err = decodeStoredFailure(rawFailure)
		if err != nil {
			return nil, "", fmt.Errorf("decode sqlite agent delivery failure: %w", err)
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
			COALESCE(d.failure, 'null'),
			COALESCE(d.retry_count, 0),
			COALESCE(d.delivered_at, d.created_at) AS occurred_at,
			COALESCE((
				SELECT json_group_array(json_object(
					'dead_letter_id', dead_letter_id,
					'failure', json(failure),
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
			rawFailure    any
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
			&rawFailure,
			&item.RetryCount,
			&occurredAtRaw,
			&recordsRaw,
		); err != nil {
			return nil, "", fmt.Errorf("scan sqlite agent dead-letter delivery: %w", err)
		}
		item.Failure, err = decodeStoredFailure(rawFailure)
		if err != nil {
			return nil, "", fmt.Errorf("decode sqlite agent dead-letter delivery failure: %w", err)
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
