package store

import (
	"context"
	"database/sql"
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
	return newSQLiteOperatorAgentConversationReadSurface(s, opts.TurnLimit).ListOperatorAgents(ctx, opts)
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
	out := make([]agentLifecycleDeliveryRecord, 0)
	for _, agentID := range agentIDs {
		snapshots, err := s.deliverySnapshotsForAgent(ctx, agentID, time.Unix(0, 0).UTC())
		if err != nil {
			return nil, err
		}
		for _, snapshot := range snapshots {
			if snapshot.Status == "delivered" {
				continue
			}
			record := agentLifecycleDeliveryRecord{
				AgentID: snapshot.SubscriberID, Status: string(snapshot.Status),
				ActiveSessionID: snapshot.ActiveSessionID, CreatedAt: snapshot.CreatedAt,
			}
			if !snapshot.SettledAt.IsZero() {
				record.DeliveredAt = sql.NullTime{Time: snapshot.SettledAt, Valid: true}
			}
			out = append(out, record)
		}
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
	counts, failures, deadLetters, err := loadAgentDeliveryDiagnosticSnapshotPages(ctx, r.store, agentID, opts)
	if err != nil {
		return OperatorAgentDeliveryDiagnostics{}, err
	}
	return buildAgentDeliveryDiagnostics(agentID, counts, failures, deadLetters,
		func(eventID string) (deliveryLifecycleEventMetadata, error) {
			record, found, err := loadSQLiteEventIdentity(ctx, r.db, eventID)
			if err != nil {
				return deliveryLifecycleEventMetadata{}, err
			}
			if !found {
				return deliveryLifecycleEventMetadata{}, fmt.Errorf("delivery event %s not found", eventID)
			}
			admitted, err := decodeEventRecord(record)
			if err != nil {
				return deliveryLifecycleEventMetadata{}, err
			}
			event := admitted.Event()
			return deliveryLifecycleEventMetadata{EventName: string(event.Type()), RunID: event.RunID(), EntityID: event.EntityID()}, nil
		},
		func(deliveryID string, claimVersion int64) ([]OperatorDeadLetterRecord, error) {
			return r.store.sqliteOperatorDeliveryDeadLetters(ctx, deliveryID, claimVersion)
		})
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
