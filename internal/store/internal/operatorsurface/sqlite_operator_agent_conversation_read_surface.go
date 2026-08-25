package operatorsurface

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	storeagentpersistence "github.com/division-sh/swarm/internal/store/internal/backend/agentpersistence"
	"github.com/google/uuid"
)

func (s *AgentSQLite) ListAgentDeliveryLifecycleFacts(ctx context.Context, identities []agentidentity.Identity) (map[agentidentity.Identity]operatorread.AgentDeliveryLifecycleFacts, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	normalized, err := normalizePendingAgentIdentities(identities)
	if err != nil {
		return nil, err
	}
	out := make(map[agentidentity.Identity]operatorread.AgentDeliveryLifecycleFacts, len(normalized))
	for _, identity := range normalized {
		out[identity] = operatorread.AgentDeliveryLifecycleFacts{}
	}
	if len(normalized) == 0 {
		return out, nil
	}
	tx, err := s.backend.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	asOf, err := operatorSQLiteDelivery.CaptureSnapshotTime(ctx, tx)
	if err != nil {
		return nil, err
	}
	records, err := s.listSQLiteAgentLifecycleRecords(ctx, tx, normalized, asOf)
	if err != nil {
		return nil, err
	}
	grouped := make(map[agentidentity.Identity][]agentLifecycleDeliveryRecord, len(normalized))
	for _, record := range records {
		grouped[record.AgentIdentity] = append(grouped[record.AgentIdentity], record)
	}
	for _, identity := range normalized {
		out[identity] = canonicalAgentDeliveryLifecycleFactsFromRecords(grouped[identity])
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit sqlite agent lifecycle facts snapshot: %w", err)
	}
	return out, nil
}

func (s *AgentSQLite) listSQLiteAgentLifecycleRecords(ctx context.Context, tx *sql.Tx, identities []agentidentity.Identity, asOf time.Time) ([]agentLifecycleDeliveryRecord, error) {
	if tx == nil {
		return nil, fmt.Errorf("sqlite agent lifecycle facts transaction is required")
	}
	snapshots, err := operatorSQLiteDelivery.CurrentAgentSnapshots(ctx, tx, identities, asOf)
	if err != nil {
		return nil, err
	}
	return agentLifecycleRecordsFromSnapshots(snapshots), nil
}

func (r *AgentSQLite) ListOperatorAgents(ctx context.Context, opts operatorread.OperatorAgentListOptions) (operatorread.OperatorAgentListResult, error) {
	return r.readOperatorAgentSummarySnapshot(ctx, opts)
}

func (r *AgentSQLite) readOperatorAgentSummarySnapshot(ctx context.Context, opts operatorread.OperatorAgentListOptions) (operatorread.OperatorAgentListResult, error) {
	if err := r.requireAgentAccess(); err != nil {
		return operatorread.OperatorAgentListResult{}, err
	}
	tx, err := r.backend.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return operatorread.OperatorAgentListResult{}, err
	}
	defer tx.Rollback()
	asOf, err := operatorSQLiteDelivery.CaptureSnapshotTime(ctx, tx)
	if err != nil {
		return operatorread.OperatorAgentListResult{}, err
	}
	result, err := r.loadOperatorAgentSummariesTx(ctx, tx, opts, asOf)
	if err != nil {
		return operatorread.OperatorAgentListResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return operatorread.OperatorAgentListResult{}, fmt.Errorf("commit sqlite agent summary snapshot: %w", err)
	}
	return result, nil
}

func (r *AgentSQLite) LoadOperatorAgent(ctx context.Context, identity agentidentity.Identity) (operatorread.OperatorAgentDetail, error) {
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return operatorread.OperatorAgentDetail{}, operatorread.ErrAgentNotFound
	}
	result, err := r.readOperatorAgentSummarySnapshot(ctx, operatorread.OperatorAgentListOptions{})
	if err != nil {
		return operatorread.OperatorAgentDetail{}, err
	}
	for _, agent := range result.Agents {
		if agent.Identity == identity {
			return operatorread.OperatorAgentDetail{
				Agent:             agent,
				CurrentSessionRef: agent.CurrentSessionRef,
				LastTurnRef:       agent.LastTurnRef,
			}, nil
		}
	}
	return operatorread.OperatorAgentDetail{}, operatorread.ErrAgentNotFound
}

func (r *AgentSQLite) LoadOperatorAgentDiagnosis(ctx context.Context, identity agentidentity.Identity, opts operatorread.OperatorAgentDiagnosisOptions) (operatorread.OperatorAgentDiagnosis, error) {
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return operatorread.OperatorAgentDiagnosis{}, operatorread.ErrAgentNotFound
	}
	if err := r.requireAgentAccess(); err != nil {
		return operatorread.OperatorAgentDiagnosis{}, err
	}
	tx, err := r.backend.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return operatorread.OperatorAgentDiagnosis{}, err
	}
	defer tx.Rollback()
	asOf, err := operatorSQLiteDelivery.CaptureSnapshotTime(ctx, tx)
	if err != nil {
		return operatorread.OperatorAgentDiagnosis{}, err
	}
	result, err := r.loadOperatorAgentSummariesTx(ctx, tx, operatorread.OperatorAgentListOptions{}, asOf)
	if err != nil {
		return operatorread.OperatorAgentDiagnosis{}, err
	}
	detail, err := operatorAgentDetailFromSnapshot(result, identity)
	if err != nil {
		return operatorread.OperatorAgentDiagnosis{}, err
	}
	diagnosis, err := operatorAgentDiagnosisFromDetail(detail)
	if err != nil {
		return operatorread.OperatorAgentDiagnosis{}, err
	}
	queue, err := r.listPendingAgentDeliveryDetailsTx(ctx, tx, operatorread.PendingAgentDeliveryListOptions{
		AgentIdentity: identity,
		Limit:         opts.QueueLimit,
		Cursor:        opts.QueueCursor,
	}, asOf)
	if err != nil {
		return operatorread.OperatorAgentDiagnosis{}, err
	}
	diagnosis.Queue = operatorAgentDiagnosisQueueFromPendingPage(queue)
	if err := validateOperatorAgentDiagnosis(diagnosis); err != nil {
		return operatorread.OperatorAgentDiagnosis{}, err
	}
	if err := tx.Commit(); err != nil {
		return operatorread.OperatorAgentDiagnosis{}, fmt.Errorf("commit sqlite agent diagnosis snapshot: %w", err)
	}
	return diagnosis, nil
}

func (r *ConversationSQLite) ListOperatorConversations(ctx context.Context, opts operatorread.OperatorConversationListOptions) (operatorread.OperatorConversationListResult, error) {
	if err := r.requireConversationAccess(); err != nil {
		return operatorread.OperatorConversationListResult{}, err
	}
	opts, err := defaultOperatorConversationListOptions(opts)
	if err != nil {
		return operatorread.OperatorConversationListResult{}, err
	}
	sources := sqliteOperatorConversationQuerySources()
	args := make([]any, 0, 8)
	where := []string{"1=1"}
	if opts.AgentID != "" {
		where = append(where, "conversations.agent_id = ?")
		args = append(args, opts.AgentID)
	}
	if opts.FlowInstance != "" {
		where = append(where, "conversations.flow_instance = ?")
		args = append(args, opts.FlowInstance)
	}
	if opts.RunID != "" {
		where = append(where, "conversations.run_id = ?")
		args = append(args, opts.RunID)
	}
	if opts.Cursor != "" {
		cursor, err := decodeConversationPositionCursor(opts.Cursor, "conversation.list")
		if err != nil {
			return operatorread.OperatorConversationListResult{}, err
		}
		updatedAt, err := time.Parse(time.RFC3339Nano, cursor.UpdatedAt)
		if err != nil || strings.TrimSpace(cursor.SessionID) == "" {
			return operatorread.OperatorConversationListResult{}, operatorread.ErrInvalidConversationCursor
		}
		where = append(where, `(conversations.updated_at < ? OR (conversations.updated_at = ? AND conversations.session_id > ?))`)
		args = append(args, updatedAt.UTC(), updatedAt.UTC(), cursor.SessionID)
	}
	args = append(args, opts.Limit+1)
	rows, err := r.backend.QueryContext(ctx, fmt.Sprintf(`
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
		return operatorread.OperatorConversationListResult{}, operatorConversationReadQueryError("list sqlite operator conversations", err)
	}
	defer rows.Close()

	conversations := []operatorread.OperatorConversationSummary{}
	for rows.Next() {
		item, err := scanSQLiteOperatorConversationSummary(rows)
		if err != nil {
			return operatorread.OperatorConversationListResult{}, err
		}
		turn, err := r.projection().loadLatestPublicConversationTurn(ctx, item.SessionID)
		if err != nil {
			return operatorread.OperatorConversationListResult{}, err
		}
		item.Metadata.LiveTurn = operatorLiveTurnFromPublic(turn)
		if turn != nil {
			item.ExecutionMode = turn.ExecutionMode
		}
		conversations = append(conversations, item)
	}
	if err := rows.Err(); err != nil {
		return operatorread.OperatorConversationListResult{}, operatorConversationReadQueryError("read sqlite operator conversations", err)
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
		conversations = []operatorread.OperatorConversationSummary{}
	}
	return operatorread.OperatorConversationListResult{Conversations: conversations, NextCursor: nextCursor}, nil
}

func (r *AgentSQLite) requireAgentAccess() error {
	if r == nil || r.backend == nil || r.delivery == nil || r.deadLetters == nil {
		return fmt.Errorf("operator agent read surface requires sqlite store")
	}
	return r.requireCurrentSchema()
}

func (r *ConversationSQLite) requireConversationAccess() error {
	if r == nil || r.backend == nil {
		return fmt.Errorf("operator conversation read surface requires sqlite store")
	}
	return r.requireCurrentSchema()
}

func (r *AgentSQLite) loadOperatorAgentSummariesTx(ctx context.Context, tx *sql.Tx, opts operatorread.OperatorAgentListOptions, asOf time.Time) (operatorread.OperatorAgentListResult, error) {
	baseRows, err := storeagentpersistence.LoadSQLiteAgentsTx(ctx, tx)
	if err != nil {
		return operatorread.OperatorAgentListResult{}, err
	}
	projections, err := r.loadAgentOperatorProjectionsTx(ctx, tx, asOf)
	if err != nil {
		return operatorread.OperatorAgentListResult{}, err
	}
	return operatorAgentListResultFromSnapshot(baseRows, projections, opts, "sqlite")
}

func (r *AgentSQLite) loadAgentOperatorProjectionsTx(ctx context.Context, tx *sql.Tx, asOf time.Time) (map[agentidentity.Identity]operatorAgentProjection, error) {
	if tx == nil {
		return nil, fmt.Errorf("load sqlite agent projections: transaction is required")
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT
			a.agent_id,
			a.agent_name_owner,
			a.agent_name_source,
			a.agent_route_presence,
			a.flow_scope_key,
			a.flow_instance_id,
			a.flow_instance,
			a.status,
			COALESCE(sess.session_id, ''),
			sess.created_at,
			CASE WHEN sess.session_id IS NULL THEN 0 ELSE sess.turn_count END,
			COALESCE(sess.lease_holder, ''),
			sess.lease_expires_at,
				CASE WHEN sess.session_id IS NULL THEN '{}' ELSE sess.runtime_state END,
				0,
				0
		FROM agents a
		LEFT JOIN agent_sessions sess ON sess.session_id = (
			SELECT session_id
			FROM agent_sessions s
			WHERE s.agent_id = a.agent_id
			  AND s.agent_name_owner = a.agent_name_owner
			  AND s.agent_name_source = a.agent_name_source
			  AND s.agent_route_presence = a.agent_route_presence
			  AND s.flow_scope_key = a.flow_scope_key
			  AND s.flow_instance_id = a.flow_instance_id
			  AND s.flow_instance = a.flow_instance
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

	out := map[agentidentity.Identity]operatorAgentProjection{}
	identities := make([]agentidentity.Identity, 0)
	for rows.Next() {
		var (
			fields            agentidentity.StorageFields
			projection        operatorAgentProjection
			lockExpiresAtRaw  any
			sessionStartedRaw any
			runtimeStateRaw   []byte
		)
		if err := rows.Scan(
			&fields.AgentID,
			&fields.NameOwner,
			&fields.NameSource,
			&fields.RoutePresence,
			&fields.FlowScopeKey,
			&fields.FlowInstanceID,
			&fields.FlowInstancePath,
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
		if projection.SessionID != "" && projection.SessionStartedAt.IsZero() {
			return nil, fmt.Errorf("sqlite active agent session started_at is required")
		}
		if projection.SessionID != "" {
			if _, err := uuid.Parse(projection.SessionID); err != nil {
				return nil, fmt.Errorf("sqlite active agent session_id is invalid: %w", err)
			}
		}
		if at, ok, err := sqliteTimeValue(lockExpiresAtRaw); err != nil {
			return nil, fmt.Errorf("scan sqlite agent session lease_expires_at: %w", err)
		} else if ok {
			projection.LockExpiresAt = at
		}
		if err := enrichOperatorAgentProjectionRuntimeState(&projection, runtimeStateRaw); err != nil {
			return nil, err
		}
		if projection.SessionID != "" && len(runtimeStateRaw) == 0 {
			return nil, fmt.Errorf("sqlite active agent session runtime_state is required")
		}
		identity, err := agentidentity.FromStorageFields(fields)
		if err != nil {
			return nil, fmt.Errorf("scan sqlite agent operator identity: %w", err)
		}
		if _, exists := out[identity]; exists {
			return nil, fmt.Errorf("duplicate sqlite agent operator projection: %s", identity.Description())
		}
		out[identity] = projection
		identities = append(identities, identity)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read sqlite agent operator projection rows: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close sqlite agent operator projection rows: %w", err)
	}
	for _, identity := range identities {
		projection := out[identity]
		if projection.SessionID != "" {
			turn, err := loadLatestSQLitePublicConversationTurnTx(ctx, tx, projection.SessionID)
			if err != nil {
				return nil, fmt.Errorf("load sqlite latest agent turn: %w", err)
			}
			enrichOperatorProjectionWithPublicTurn(&projection, turn)
			out[identity] = projection
		}
	}
	aggregates, err := operatorSQLiteDelivery.AgentPendingAggregates(ctx, tx, identities, time.Time{}, asOf)
	if err != nil {
		return nil, err
	}
	factsByAgent := pendingAgentDeliveryFactsFromAggregates(identities, aggregates, asOf)
	snapshots, err := operatorSQLiteDelivery.CurrentAgentSnapshots(ctx, tx, identities, asOf)
	if err != nil {
		return nil, err
	}
	lifecycleByAgent := agentDeliveryLifecycleFactsFromSnapshots(identities, snapshots)
	for identity, facts := range factsByAgent {
		projection := out[identity]
		projection.PendingEvents = facts.PendingCount
		projection.OldestPendingAgeSec = facts.OldestPendingAgeSec
		out[identity] = projection
	}
	for identity, facts := range lifecycleByAgent {
		projection := out[identity]
		projection.LifecycleState = strings.TrimSpace(facts.CurrentState)
		projection.BlockingLayer = strings.TrimSpace(facts.BlockingLayer)
		out[identity] = projection
	}
	return out, nil
}

func scanSQLiteOperatorConversationSummary(scanner operatorRowScanner) (operatorread.OperatorConversationSummary, error) {
	var (
		item            operatorread.OperatorConversationSummary
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
		return operatorread.OperatorConversationSummary{}, err
	}
	if at, ok, err := sqliteTimeValue(startedAtRaw); err != nil {
		return operatorread.OperatorConversationSummary{}, fmt.Errorf("scan sqlite conversation started_at: %w", err)
	} else if ok {
		item.StartedAt = at
	}
	if at, ok, err := sqliteTimeValue(endedAtRaw); err != nil {
		return operatorread.OperatorConversationSummary{}, fmt.Errorf("scan sqlite conversation ended_at: %w", err)
	} else if ok {
		item.EndedAt = &at
	}
	if at, ok, err := sqliteTimeValue(updatedAtRaw); err != nil {
		return operatorread.OperatorConversationSummary{}, fmt.Errorf("scan sqlite conversation updated_at: %w", err)
	} else if ok {
		item.UpdatedAt = at
	}
	runtimeState, err := DecodeConversationRuntimeStateDescriptor(runtimeStateRaw)
	if err != nil {
		return operatorread.OperatorConversationSummary{}, fmt.Errorf("decode conversation runtime_state: %w", err)
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
				agent_name_owner,
				agent_name_source,
				agent_route_presence,
				flow_scope_key,
				flow_instance_id,
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
				agent_name_owner,
				agent_name_source,
				agent_route_presence,
				flow_scope_key,
				flow_instance_id,
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

func (r *AgentSQLite) LoadOperatorAgentDeliveryDiagnostics(ctx context.Context, identity agentidentity.Identity, opts operatorread.OperatorAgentDeliveryDiagnosticsOptions) (operatorread.OperatorAgentDeliveryDiagnostics, error) {
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return operatorread.OperatorAgentDeliveryDiagnostics{}, operatorread.ErrAgentNotFound
	}
	if err := r.requireAgentDeliveryDiagnosticsAccess(); err != nil {
		return operatorread.OperatorAgentDeliveryDiagnostics{}, err
	}
	if err := r.ensureAgentDeliveryDiagnosticsAgentExists(ctx, identity); err != nil {
		return operatorread.OperatorAgentDeliveryDiagnostics{}, err
	}

	opts = defaultOperatorAgentDeliveryDiagnosticsOptions(opts)
	counts, failures, deadLetters, err := loadAgentDeliveryDiagnosticSnapshotPages(ctx, r.delivery, identity, opts)
	if err != nil {
		return operatorread.OperatorAgentDeliveryDiagnostics{}, err
	}
	return buildAgentDeliveryDiagnostics(identity.AgentID(), counts, failures, deadLetters,
		func(eventID string) (deliveryLifecycleEventMetadata, error) {
			record, found, err := loadSQLiteEventIdentity(ctx, r.backend, eventID)
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
		func(deliveryID string, claimVersion int64) ([]operatorread.OperatorDeadLetterRecord, error) {
			return r.LoadOperatorDeliveryDeadLetters(ctx, deliveryID, claimVersion)
		})
}

func (r *AgentSQLite) requireAgentDeliveryDiagnosticsAccess() error {
	if r == nil || r.backend == nil {
		return fmt.Errorf("operator agent delivery diagnostics read owner requires sqlite store")
	}
	return r.requireCurrentSchema()
}

func (r *AgentSQLite) ensureAgentDeliveryDiagnosticsAgentExists(ctx context.Context, identity agentidentity.Identity) error {
	fields, err := agentIdentityFields(identity)
	if err != nil {
		return operatorread.ErrAgentNotFound
	}
	var exists bool
	if err := r.backend.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM agents
			WHERE agent_id = ?
			  AND agent_name_owner = ?
			  AND agent_name_source = ?
			  AND agent_route_presence = ?
			  AND flow_scope_key = ?
			  AND flow_instance_id = ?
			  AND flow_instance = ?
			  AND status NOT IN ('terminated', 'ephemeral')
		)
	`, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
		fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath).Scan(&exists); err != nil {
		return fmt.Errorf("load sqlite agent delivery diagnostics agent: %w", err)
	}
	if !exists {
		return operatorread.ErrAgentNotFound
	}
	return nil
}
