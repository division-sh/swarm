package operatorsurface

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/google/uuid"
)

func cloneRawMessage(in json.RawMessage) json.RawMessage {
	if in == nil {
		return nil
	}
	out := make(json.RawMessage, len(in))
	copy(out, in)
	return out
}

func cloneConversationToolCalls(in []operatorread.OperatorConversationToolCall) []operatorread.OperatorConversationToolCall {
	if in == nil {
		return nil
	}
	out := make([]operatorread.OperatorConversationToolCall, len(in))
	for i, item := range in {
		out[i] = item
		out[i].Arguments = cloneRawMessage(item.Arguments)
		out[i].Result = cloneRawMessage(item.Result)
	}
	return out
}

func cloneConversationToolResults(in []operatorread.OperatorConversationToolResult) []operatorread.OperatorConversationToolResult {
	if in == nil {
		return nil
	}
	out := make([]operatorread.OperatorConversationToolResult, len(in))
	for i, item := range in {
		out[i] = item
		out[i].Output = cloneRawMessage(item.Output)
	}
	return out
}

type operatorAgentProjection struct {
	Status              string
	LifecycleState      string
	BlockingLayer       string
	PendingEvents       int
	OldestPendingAgeSec int
	LockOwner           string
	LockExpiresAt       time.Time
	TurnCount           int
	SessionID           string
	SessionStartedAt    time.Time
	ProviderSessionID   string
	CurrentTaskID       string
	LastTool            *operatorread.OperatorAgentTool
	LiveTurn            *operatorread.OperatorLiveTurn
	DiagnosisActive     *operatorread.OperatorAgentDiagnosisActive
	LastTurnRef         *operatorread.OperatorTurnRef
	Watchdog            *operatorread.OperatorConversationWatchdog
}

type OperatorAgentProjection = operatorAgentProjection

type conversationPositionCursor struct {
	Kind      string `json:"kind"`
	UpdatedAt string `json:"updated_at"`
	SessionID string `json:"session_id"`
}

type operatorRowScanner interface {
	Scan(dest ...any) error
}

func (r *AgentPostgres) ListOperatorAgents(ctx context.Context, opts operatorread.OperatorAgentListOptions) (operatorread.OperatorAgentListResult, error) {
	if err := r.requireAgentAccess(); err != nil {
		return operatorread.OperatorAgentListResult{}, err
	}
	opts.Flow = strings.Trim(strings.TrimSpace(opts.Flow), "/")
	opts.Role = strings.TrimSpace(opts.Role)
	baseRows, err := r.runtime.LoadAgents(ctx)
	if err != nil {
		return operatorread.OperatorAgentListResult{}, err
	}
	projections, err := r.loadAgentOperatorProjections(ctx)
	if err != nil {
		return operatorread.OperatorAgentListResult{}, err
	}
	agents := make([]operatorread.OperatorAgentSummary, 0, len(baseRows))
	for _, row := range baseRows {
		if opts.Role != "" && strings.TrimSpace(row.Config.Role) != opts.Role {
			continue
		}
		if opts.Flow != "" && !operatorAgentFlowMatches(row.Config.CanonicalFlowPath(), opts.Flow) {
			continue
		}
		identity, err := row.Config.ConcreteIdentity()
		if err != nil {
			return operatorread.OperatorAgentListResult{}, err
		}
		projection, ok := projections[identity]
		if !ok {
			return operatorread.OperatorAgentListResult{}, fmt.Errorf("missing agent operator projection: %s", identity.Description())
		}
		agents = append(agents, operatorAgentSummaryFromPersisted(row, projection, opts.TurnLimit))
	}
	if agents == nil {
		agents = []operatorread.OperatorAgentSummary{}
	}
	return operatorread.OperatorAgentListResult{Agents: agents}, nil
}

func (r *AgentPostgres) LoadOperatorAgent(ctx context.Context, identity agentidentity.Identity) (operatorread.OperatorAgentDetail, error) {
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return operatorread.OperatorAgentDetail{}, operatorread.ErrAgentNotFound
	}
	result, err := r.ListOperatorAgents(ctx, operatorread.OperatorAgentListOptions{})
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

func (r *AgentPostgres) LoadOperatorAgentDiagnosis(ctx context.Context, identity agentidentity.Identity, opts operatorread.OperatorAgentDiagnosisOptions) (operatorread.OperatorAgentDiagnosis, error) {
	detail, err := r.LoadOperatorAgent(ctx, identity)
	if err != nil {
		return operatorread.OperatorAgentDiagnosis{}, err
	}
	diagnosis, err := operatorAgentDiagnosisFromDetail(detail)
	if err != nil {
		return operatorread.OperatorAgentDiagnosis{}, err
	}
	queue, err := r.delivery.ListPendingAgentDeliveryDetails(ctx, operatorread.PendingAgentDeliveryListOptions{
		AgentIdentity: identity,
		Limit:         opts.QueueLimit,
		Cursor:        opts.QueueCursor,
	})
	if err != nil {
		return operatorread.OperatorAgentDiagnosis{}, err
	}
	diagnosis.Queue = operatorAgentDiagnosisQueueFromPendingPage(queue)
	if err := validateOperatorAgentDiagnosis(diagnosis); err != nil {
		return operatorread.OperatorAgentDiagnosis{}, err
	}
	return diagnosis, nil
}

func (r *ConversationPostgres) ListOperatorConversations(ctx context.Context, opts operatorread.OperatorConversationListOptions) (operatorread.OperatorConversationListResult, error) {
	if err := r.requireConversationAccess(); err != nil {
		return operatorread.OperatorConversationListResult{}, err
	}
	opts, err := defaultOperatorConversationListOptions(opts)
	if err != nil {
		return operatorread.OperatorConversationListResult{}, err
	}
	sources := operatorConversationQuerySources()
	args := make([]any, 0, 8)
	where := []string{"TRUE"}
	add := func(value any) int {
		args = append(args, value)
		return len(args)
	}
	if opts.AgentID != "" {
		n := add(opts.AgentID)
		where = append(where, fmt.Sprintf("conversations.agent_id = $%d", n))
	}
	if opts.FlowInstance != "" {
		n := add(opts.FlowInstance)
		where = append(where, fmt.Sprintf("conversations.flow_instance = $%d", n))
	}
	if opts.RunID != "" {
		n := add(opts.RunID)
		where = append(where, fmt.Sprintf("conversations.run_id = $%d", n))
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
		nTime := add(updatedAt.UTC())
		nSession := add(cursor.SessionID)
		where = append(where, fmt.Sprintf(`(
			conversations.updated_at < $%d
			OR (conversations.updated_at = $%d AND conversations.session_id > $%d)
		)`, nTime, nTime, nSession))
	}
	limitArg := add(opts.Limit + 1)
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
				COALESCE(conversations.runtime_state, '{}'::jsonb),
				conversations.started_at,
			conversations.ended_at,
			conversations.updated_at
		FROM (
			%s
		) conversations
			WHERE %s
		ORDER BY conversations.updated_at DESC, conversations.session_id ASC
		LIMIT $%d
		`, strings.Join(sources, "\nUNION ALL\n"), strings.Join(where, " AND "), limitArg), args...)
	if err != nil {
		return operatorread.OperatorConversationListResult{}, operatorConversationReadQueryError("list operator conversations", err)
	}
	defer rows.Close()
	conversations := []operatorread.OperatorConversationSummary{}
	for rows.Next() {
		item, err := scanOperatorConversationSummary(rows)
		if err != nil {
			return operatorread.OperatorConversationListResult{}, err
		}
		turn, err := r.loadLatestPublicConversationTurn(ctx, item.SessionID)
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
		return operatorread.OperatorConversationListResult{}, operatorConversationReadQueryError("read operator conversations", err)
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

func (r *AgentPostgres) requireAgentAccess() error {
	if r == nil || r.backend == nil || r.runtime == nil || r.conversation == nil || r.delivery == nil || r.deadLetters == nil {
		return fmt.Errorf("operator agent read surface requires postgres store")
	}
	return r.requireCurrentSchema()
}

func (r *ConversationPostgres) requireConversationAccess() error {
	if r == nil || r.backend == nil {
		return fmt.Errorf("operator conversation read surface requires postgres store")
	}
	return r.requireCurrentSchema()
}

func (r *ConversationPostgres) loadLatestPublicConversationTurn(ctx context.Context, sessionID string) (*operatorread.OperatorPublicConversationTurn, error) {
	return r.projection().loadLatestPublicConversationTurn(ctx, sessionID)
}

func (r *AgentPostgres) loadAgentOperatorProjections(ctx context.Context) (map[agentidentity.Identity]operatorAgentProjection, error) {
	rows, err := r.backend.QueryContext(ctx, `
			SELECT
			a.agent_id,
			a.agent_name_owner,
			a.agent_name_source,
			a.agent_route_presence,
			a.flow_scope_key,
			a.flow_instance_id,
			a.flow_instance,
			COALESCE(a.status, 'active'),
			COALESCE(sess.session_id::text, ''),
			sess.created_at,
			COALESCE(sess.turn_count, 0),
			COALESCE(sess.lease_holder, ''),
			sess.lease_expires_at,
				COALESCE(sess.runtime_state, '{}'::jsonb),
				0,
				0
		FROM agents a
		LEFT JOIN LATERAL (
			SELECT
				session_id,
				created_at,
				turn_count,
				lease_holder,
				lease_expires_at,
				runtime_state
			FROM agent_sessions
			WHERE agent_id = a.agent_id
			  AND agent_name_owner = a.agent_name_owner
			  AND agent_name_source = a.agent_name_source
			  AND agent_route_presence = a.agent_route_presence
			  AND flow_scope_key = a.flow_scope_key
			  AND flow_instance_id = a.flow_instance_id
			  AND flow_instance = a.flow_instance
			  AND status = 'active'
			  AND memory_enabled = TRUE
			ORDER BY updated_at DESC, created_at DESC, session_id ASC
			LIMIT 1
		) sess ON true
			WHERE a.status NOT IN ('terminated', 'ephemeral')
			ORDER BY a.created_at ASC, a.agent_id ASC
		`)
	if err != nil {
		return nil, fmt.Errorf("query agent operator projections: %w", err)
	}
	defer rows.Close()

	out := map[agentidentity.Identity]operatorAgentProjection{}
	identities := make([]agentidentity.Identity, 0)
	for rows.Next() {
		var (
			fields           agentidentity.StorageFields
			projection       operatorAgentProjection
			lockExpiresAt    sql.NullTime
			sessionStartedAt sql.NullTime
			runtimeStateRaw  []byte
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
			&sessionStartedAt,
			&projection.TurnCount,
			&projection.LockOwner,
			&lockExpiresAt,
			&runtimeStateRaw,
			&projection.PendingEvents,
			&projection.OldestPendingAgeSec,
		); err != nil {
			return nil, fmt.Errorf("scan agent operator projection: %w", err)
		}
		if sessionStartedAt.Valid {
			projection.SessionStartedAt = sessionStartedAt.Time
		}
		if lockExpiresAt.Valid {
			projection.LockExpiresAt = lockExpiresAt.Time
		}
		if err := enrichOperatorAgentProjectionRuntimeState(&projection, runtimeStateRaw); err != nil {
			return nil, err
		}
		if projection.SessionID != "" {
			turn, err := loadOperatorLatestConversationTurn(ctx, r.conversation, projection.SessionID)
			if err != nil {
				return nil, fmt.Errorf("load latest agent turn: %w", err)
			}
			enrichOperatorProjectionWithPublicTurn(&projection, turn)
		}
		identity, err := agentidentity.FromStorageFields(fields)
		if err != nil {
			return nil, fmt.Errorf("scan agent operator identity: %w", err)
		}
		out[identity] = projection
		identities = append(identities, identity)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read agent operator projection rows: %w", err)
	}
	factsByAgent, err := r.delivery.ListPendingAgentDeliveryFacts(ctx, identities, time.Time{})
	if err != nil {
		return nil, err
	}
	lifecycleByAgent, err := r.delivery.ListAgentDeliveryLifecycleFacts(ctx, identities)
	if err != nil {
		return nil, err
	}
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

func operatorAgentSummaryFromPersisted(row runtimemanager.PersistedAgent, projection operatorAgentProjection, turnLimit int) operatorread.OperatorAgentSummary {
	memory, err := row.Config.Memory.Normalize()
	if err != nil {
		memory = agentmemory.PlatformDefault()
	}
	out := operatorread.OperatorAgentSummary{
		AgentID:               strings.TrimSpace(row.Config.ID),
		Identity:              row.Config.Identity.Normalize(),
		Role:                  strings.TrimSpace(row.Config.Role),
		Type:                  operatorAgentPersistedType(row.Config, strings.TrimSpace(row.Config.Model)),
		Model:                 strings.TrimSpace(row.Config.Model),
		ExecutionMode:         string(row.Config.ExecutionMode),
		Memory:                memory.Enabled,
		MemorySource:          string(memory.Source),
		Status:                projection.v1Status(),
		RuntimeFlowID:         strings.TrimSpace(row.Config.FlowID),
		FlowInstance:          strings.TrimSpace(row.Config.CanonicalFlowPath()),
		EntityID:              strings.TrimSpace(row.Config.EffectiveEntityID()),
		ParentAgentID:         strings.TrimSpace(row.ParentAgentID),
		CoordinatorID:         strings.TrimSpace(row.CoordinatorID),
		HiredBy:               strings.TrimSpace(row.HiredBy),
		TemplateVersion:       strings.TrimSpace(row.TemplateVersion),
		BudgetEnvelope:        row.Config.BudgetEnvelope,
		Subscriptions:         append([]string(nil), row.Config.Subscriptions...),
		Permissions:           append([]string(nil), row.Config.Permissions...),
		PendingEvents:         projection.PendingEvents,
		OldestPendingAgeSec:   projection.OldestPendingAgeSec,
		LockOwner:             strings.TrimSpace(projection.LockOwner),
		LockExpiresAt:         projection.LockExpiresAt,
		TurnCount:             projection.TurnCount,
		TurnLimit:             maxStoreInt(turnLimit, 0),
		SessionID:             strings.TrimSpace(projection.SessionID),
		ProviderSessionID:     strings.TrimSpace(projection.ProviderSessionID),
		CurrentTaskID:         strings.TrimSpace(projection.CurrentTaskID),
		LastTool:              projection.LastTool,
		LiveTurn:              projection.LiveTurn,
		DiagnosisActive:       cloneOperatorAgentDiagnosisActive(projection.DiagnosisActive),
		StartedAt:             row.StartedAt,
		DashboardStatus:       strings.TrimSpace(projection.Status),
		DashboardState:        projection.dashboardState(),
		DeliveryLifecycle:     strings.TrimSpace(projection.LifecycleState),
		BlockingLayer:         projection.dashboardBlockingLayer(),
		CurrentSessionRef:     projection.currentSessionRef(),
		LastTurnRef:           projection.LastTurnRef,
		DiagnosisRuntimeState: operatorAgentDiagnosisRuntimeStateFromConversationWatchdog(projection.Watchdog),
	}
	if out.TurnLimit > 0 {
		out.NearBreaker = out.TurnCount*100 >= out.TurnLimit*85
	}
	return out
}

func OperatorAgentSummaryFromPersisted(row runtimemanager.PersistedAgent, projection OperatorAgentProjection, turnLimit int) operatorread.OperatorAgentSummary {
	return operatorAgentSummaryFromPersisted(row, projection, turnLimit)
}

func operatorAgentDiagnosisFromDetail(detail operatorread.OperatorAgentDetail) (operatorread.OperatorAgentDiagnosis, error) {
	agent := detail.Agent
	out := operatorread.OperatorAgentDiagnosis{
		AgentID:           strings.TrimSpace(agent.AgentID),
		Status:            strings.TrimSpace(agent.Status),
		CurrentSessionRef: detail.CurrentSessionRef,
		LastTurnRef:       detail.LastTurnRef,
		Queue: operatorread.OperatorAgentDiagnosisQueue{
			PendingCount:            agent.PendingEvents,
			OldestPendingAgeSeconds: agent.OldestPendingAgeSec,
			PendingDeliveries:       []operatorread.OperatorAgentPendingDelivery{},
		},
		RuntimeState: agent.DiagnosisRuntimeState,
		Active:       cloneOperatorAgentDiagnosisActive(agent.DiagnosisActive),
	}
	lastToolOutcome, err := operatorAgentDiagnosisLastToolOutcomeFromAgent(agent)
	if err != nil {
		return operatorread.OperatorAgentDiagnosis{}, err
	}
	out.LastToolOutcome = lastToolOutcome
	if state := strings.TrimSpace(agent.DeliveryLifecycle); state != "" {
		out.DeliveryLifecycle = &operatorread.OperatorAgentDeliveryLifecycle{
			State:         state,
			BlockingLayer: strings.TrimSpace(agent.BlockingLayer),
		}
	}
	if err := validateOperatorAgentDiagnosis(out); err != nil {
		return operatorread.OperatorAgentDiagnosis{}, err
	}
	return out, nil
}

func operatorAgentDiagnosisQueueFromPendingPage(page operatorread.PendingAgentDeliveryPage) operatorread.OperatorAgentDiagnosisQueue {
	queue := operatorread.OperatorAgentDiagnosisQueue{
		PendingCount:            page.PendingCount,
		OldestPendingAgeSeconds: page.OldestPendingAgeSec,
		PendingDeliveries:       make([]operatorread.OperatorAgentPendingDelivery, 0, len(page.PendingDeliveries)),
		NextCursor:              strings.TrimSpace(page.NextCursor),
	}
	for _, detail := range page.PendingDeliveries {
		queue.PendingDeliveries = append(queue.PendingDeliveries, operatorread.OperatorAgentPendingDelivery{
			DeliveryID: strings.TrimSpace(detail.DeliveryID),
			EventID:    strings.TrimSpace(detail.EventID),
			EventName:  strings.TrimSpace(detail.EventName),
			EnqueuedAt: detail.EnqueuedAt.UTC(),
			Attempts:   detail.Attempts,
		})
	}
	return queue
}

func validateOperatorAgentDiagnosis(item operatorread.OperatorAgentDiagnosis) error {
	if strings.TrimSpace(item.AgentID) == "" {
		return fmt.Errorf("agent diagnosis agent_id is required")
	}
	if !validOperatorAgentDiagnosisStatus(item.Status) {
		return fmt.Errorf("agent diagnosis status %q is not valid", item.Status)
	}
	if item.Queue.PendingCount < 0 {
		return fmt.Errorf("agent diagnosis queue.pending_count must be non-negative")
	}
	if item.Queue.OldestPendingAgeSeconds < 0 {
		return fmt.Errorf("agent diagnosis queue.oldest_pending_age_seconds must be non-negative")
	}
	if item.Queue.PendingDeliveries == nil {
		return fmt.Errorf("agent diagnosis queue.pending_deliveries must be an array")
	}
	for i, detail := range item.Queue.PendingDeliveries {
		if strings.TrimSpace(detail.DeliveryID) == "" {
			return fmt.Errorf("agent diagnosis queue.pending_deliveries[%d].delivery_id is required", i)
		}
		if strings.TrimSpace(detail.EventID) == "" {
			return fmt.Errorf("agent diagnosis queue.pending_deliveries[%d].event_id is required", i)
		}
		if strings.TrimSpace(detail.EventName) == "" {
			return fmt.Errorf("agent diagnosis queue.pending_deliveries[%d].event_name is required", i)
		}
		if detail.EnqueuedAt.IsZero() {
			return fmt.Errorf("agent diagnosis queue.pending_deliveries[%d].enqueued_at is required", i)
		}
		if detail.Attempts < 0 {
			return fmt.Errorf("agent diagnosis queue.pending_deliveries[%d].attempts must be non-negative", i)
		}
	}
	if item.DeliveryLifecycle != nil {
		if !validOperatorAgentDeliveryLifecycleState(item.DeliveryLifecycle.State) {
			return fmt.Errorf("agent diagnosis delivery_lifecycle.state %q is not valid", item.DeliveryLifecycle.State)
		}
		if strings.TrimSpace(item.DeliveryLifecycle.BlockingLayer) == "" {
			return fmt.Errorf("agent diagnosis delivery_lifecycle.blocking_layer is required")
		}
	}
	if err := validateOperatorAgentDiagnosisActive(item.Active); err != nil {
		return err
	}
	if err := validateOperatorAgentDiagnosisRuntimeState(item.RuntimeState); err != nil {
		return err
	}
	if err := validateOperatorAgentDiagnosisLastToolOutcome(item.LastToolOutcome); err != nil {
		return err
	}
	if item.LastToolOutcome != nil {
		if item.Active == nil {
			return fmt.Errorf("agent diagnosis last_tool_outcome requires active selected-turn evidence")
		}
		activeTurnID := strings.TrimSpace(item.Active.TurnID)
		lastToolTurnID := strings.TrimSpace(item.LastToolOutcome.TurnID)
		if activeTurnID != lastToolTurnID {
			return fmt.Errorf("agent diagnosis last_tool_outcome.turn_id %q must match active.turn_id %q", lastToolTurnID, activeTurnID)
		}
	}
	return nil
}

func ValidateOperatorAgentDiagnosis(item operatorread.OperatorAgentDiagnosis) error {
	return validateOperatorAgentDiagnosis(item)
}

func validateOperatorAgentDiagnosisActive(item *operatorread.OperatorAgentDiagnosisActive) error {
	if item == nil {
		return nil
	}
	if strings.TrimSpace(item.TurnID) == "" {
		return fmt.Errorf("agent diagnosis active.turn_id is required")
	}
	return nil
}

func validateOperatorAgentDiagnosisRuntimeState(item *operatorread.OperatorAgentDiagnosisRuntimeState) error {
	if item == nil {
		return nil
	}
	if item.Watchdog == nil {
		return fmt.Errorf("agent diagnosis runtime_state.watchdog is required")
	}
	if err := ValidateConversationRuntimeWatchdogDescriptor(conversationWatchdogDescriptorFromAgentDiagnosis(*item.Watchdog)); err != nil {
		return fmt.Errorf("agent diagnosis runtime_state.watchdog is invalid: %w", err)
	}
	return nil
}

func operatorAgentDiagnosisActiveFromLatestTurn(turnID, taskID, entityID string) *operatorread.OperatorAgentDiagnosisActive {
	turnID = strings.TrimSpace(turnID)
	if turnID == "" {
		return nil
	}
	return &operatorread.OperatorAgentDiagnosisActive{
		TurnID:   turnID,
		TaskID:   strings.TrimSpace(taskID),
		EntityID: strings.TrimSpace(entityID),
	}
}

func cloneOperatorAgentDiagnosisActive(in *operatorread.OperatorAgentDiagnosisActive) *operatorread.OperatorAgentDiagnosisActive {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

func operatorAgentDiagnosisLastToolOutcomeFromAgent(agent operatorread.OperatorAgentSummary) (*operatorread.OperatorAgentLastToolOutcome, error) {
	if agent.DiagnosisActive == nil {
		return nil, nil
	}
	turnID := strings.TrimSpace(agent.DiagnosisActive.TurnID)
	if turnID == "" {
		return nil, nil
	}
	if agent.LiveTurn == nil || agent.LiveTurn.LastTool == nil {
		return nil, nil
	}
	if liveTurnID := strings.TrimSpace(agent.LiveTurn.TurnID); liveTurnID != "" && liveTurnID != turnID {
		return nil, fmt.Errorf("agent diagnosis last_tool_outcome turn_id %q does not match active turn_id %q", liveTurnID, turnID)
	}
	last := agent.LiveTurn.LastTool
	out := &operatorread.OperatorAgentLastToolOutcome{
		TurnID:    turnID,
		ToolName:  strings.TrimSpace(last.Name),
		ToolUseID: strings.TrimSpace(last.ToolUseID),
		OK:        last.OK,
	}
	if err := validateOperatorAgentDiagnosisLastToolOutcome(out); err != nil {
		return nil, err
	}
	return out, nil
}

func validateOperatorAgentDiagnosisLastToolOutcome(item *operatorread.OperatorAgentLastToolOutcome) error {
	if item == nil {
		return nil
	}
	if strings.TrimSpace(item.TurnID) == "" {
		return fmt.Errorf("agent diagnosis last_tool_outcome.turn_id is required")
	}
	if strings.TrimSpace(item.ToolName) == "" {
		return fmt.Errorf("agent diagnosis last_tool_outcome.tool_name is required")
	}
	return nil
}

func validOperatorAgentDiagnosisStatus(status string) bool {
	switch strings.TrimSpace(status) {
	case "idle", "running", "paused", "failed", "terminated":
		return true
	default:
		return false
	}
}

func validOperatorAgentDeliveryLifecycleState(state string) bool {
	switch strings.TrimSpace(state) {
	case "queued", "launching", "active", "retrying", "exhausted":
		return true
	default:
		return false
	}
}

func (p operatorAgentProjection) currentSessionRef() *operatorread.OperatorSessionRef {
	if strings.TrimSpace(p.SessionID) == "" || p.SessionStartedAt.IsZero() {
		return nil
	}
	return &operatorread.OperatorSessionRef{SessionID: strings.TrimSpace(p.SessionID), StartedAt: p.SessionStartedAt}
}

func (p operatorAgentProjection) dashboardState() string {
	status := strings.ToLower(strings.TrimSpace(p.Status))
	if status == "terminated" {
		return "terminated"
	}
	if state := strings.TrimSpace(p.LifecycleState); state != "" {
		return state
	}
	return "idle"
}

func (p operatorAgentProjection) dashboardBlockingLayer() string {
	if layer := strings.TrimSpace(p.BlockingLayer); layer != "" {
		return layer
	}
	return ""
}

func (p operatorAgentProjection) v1Status() string {
	switch strings.TrimSpace(p.Status) {
	case "terminated":
		return "terminated"
	case "paused":
		return "paused"
	}
	switch strings.TrimSpace(p.LifecycleState) {
	case "active", "launching", "retrying":
		return "running"
	case "exhausted":
		return "failed"
	}
	return "idle"
}

func enrichOperatorAgentProjectionRuntimeState(projection *operatorAgentProjection, runtimeStateRaw []byte) error {
	if projection == nil {
		return nil
	}
	runtimeState, err := DecodeConversationRuntimeStateDescriptor(runtimeStateRaw)
	if err != nil {
		return fmt.Errorf("decode latest agent session runtime_state: %w", err)
	}
	projection.ProviderSessionID = strings.TrimSpace(runtimeState.ProviderSessionID)
	projection.Watchdog = operatorConversationWatchdogFromDescriptor(runtimeState.Watchdog)
	return nil
}

func operatorAgentFlowMatches(agentFlow, filter string) bool {
	agentFlow = strings.Trim(strings.TrimSpace(agentFlow), "/")
	filter = strings.Trim(strings.TrimSpace(filter), "/")
	return filter == "" || agentFlow == filter || strings.HasPrefix(agentFlow, filter+"/")
}

func defaultOperatorConversationListOptions(opts operatorread.OperatorConversationListOptions) (operatorread.OperatorConversationListOptions, error) {
	opts.AgentID = strings.TrimSpace(opts.AgentID)
	opts.FlowInstance = strings.Trim(strings.TrimSpace(opts.FlowInstance), "/")
	opts.RunID = strings.TrimSpace(opts.RunID)
	opts.Cursor = strings.TrimSpace(opts.Cursor)
	if opts.RunID != "" {
		if _, err := uuid.Parse(opts.RunID); err != nil {
			return operatorread.OperatorConversationListOptions{}, fmt.Errorf("%w: run_id must be a UUID", operatorread.ErrInvalidEntityReadParam)
		}
	}
	if opts.Limit <= 0 {
		opts.Limit = 50
	}
	if opts.Limit > 500 {
		opts.Limit = 500
	}
	return opts, nil
}

func operatorConversationReadQueryError(operation string, err error) error {
	if err == nil {
		return nil
	}
	operation = strings.TrimSpace(operation)
	if operation == "" {
		operation = "operator conversation read"
	}
	return fmt.Errorf("%s: %w", operation, err)
}

func OperatorConversationReadQueryError(operation string, err error) error {
	return operatorConversationReadQueryError(operation, err)
}

func operatorConversationQuerySources() []string {
	return []string{`
			SELECT
				session_id::text AS session_id,
				agent_id,
				agent_name_owner,
				agent_name_source,
				agent_route_presence,
				flow_scope_key,
				flow_instance_id,
				COALESCE(run_id::text, '') AS run_id,
				'live_session' AS kind,
				flow_instance,
				memory_enabled,
				memory_source,
				CASE WHEN status = 'terminated' THEN 'terminated' ELSE 'active' END AS status,
				turn_count,
				jsonb_array_length(COALESCE(conversation, '[]'::jsonb)) AS message_count,
				runtime_state,
				conversation,
				created_at AS started_at,
				CASE WHEN status = 'terminated' THEN terminated_at ELSE NULL END AS ended_at,
				updated_at,
				created_at
			FROM agent_sessions
			WHERE status IN ('active', 'terminated') AND memory_enabled = TRUE
		`, fmt.Sprintf(`
			SELECT
				session_id::text AS session_id,
				agent_id,
				agent_name_owner,
				agent_name_source,
				agent_route_presence,
				flow_scope_key,
				flow_instance_id,
				COALESCE(run_id::text, '') AS run_id,
				'turn_audit' AS kind,
				COALESCE(flow_instance, '') AS flow_instance,
				memory_enabled,
				memory_source,
				CASE WHEN status = 'terminated' THEN 'terminated' ELSE 'active' END AS status,
				COALESCE(turn_count, 0) AS turn_count,
				jsonb_array_length(COALESCE(conversation, '[]'::jsonb)) AS message_count,
				COALESCE(runtime_state, '{}'::jsonb) AS runtime_state,
				COALESCE(conversation, '[]'::jsonb) AS conversation,
				created_at AS started_at,
				NULL::timestamptz AS ended_at,
				updated_at,
				created_at
			FROM (
				%s
			) task_conversations
		`, CanonicalStatelessConversationVisibilitySourceSQL())}
}

func CloneRawMessage(in json.RawMessage) json.RawMessage {
	return cloneRawMessage(in)
}

func CloneConversationToolCalls(in []operatorread.OperatorConversationToolCall) []operatorread.OperatorConversationToolCall {
	return cloneConversationToolCalls(in)
}

func CloneConversationToolResults(in []operatorread.OperatorConversationToolResult) []operatorread.OperatorConversationToolResult {
	return cloneConversationToolResults(in)
}

func scanOperatorConversationSummary(scanner operatorRowScanner) (operatorread.OperatorConversationSummary, error) {
	var (
		item            operatorread.OperatorConversationSummary
		runtimeStateRaw []byte
		endedAt         sql.NullTime
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
		&item.StartedAt,
		&endedAt,
		&item.UpdatedAt,
	); err != nil {
		return operatorread.OperatorConversationSummary{}, err
	}
	if endedAt.Valid {
		ended := endedAt.Time
		item.EndedAt = &ended
	}
	runtimeState, err := DecodeConversationRuntimeStateDescriptor(runtimeStateRaw)
	if err != nil {
		return operatorread.OperatorConversationSummary{}, fmt.Errorf("decode conversation runtime_state: %w", err)
	}
	item.Summary = runtimeState.Summary
	item.Metadata = projectOperatorConversationSummaryMetadata(runtimeState)
	return item, nil
}

func projectOperatorConversationSummaryMetadata(p ConversationRuntimeStateDescriptor) operatorread.OperatorConversationSummaryMetadata {
	meta := operatorread.OperatorConversationSummaryMetadata{
		ProviderSessionID:    p.ProviderSessionID,
		RetryReason:          p.RetryReason,
		RetriesFromSessionID: p.RetriesFromSessionID,
	}
	meta.Watchdog = operatorConversationWatchdogFromDescriptor(p.Watchdog)
	return meta
}

func ProjectOperatorConversationSummaryMetadata(p ConversationRuntimeStateDescriptor) operatorread.OperatorConversationSummaryMetadata {
	return projectOperatorConversationSummaryMetadata(p)
}

func operatorConversationWatchdogFromDescriptor(p *ConversationRuntimeWatchdogDescriptor) *operatorread.OperatorConversationWatchdog {
	if p == nil {
		return nil
	}
	return &operatorread.OperatorConversationWatchdog{
		State:         p.State,
		BlockingLayer: p.BlockingLayer,
		Action:        p.Action,
		Outcome:       p.Outcome,
		LastOutputAt:  p.LastOutputAt,
		RecordedAt:    p.RecordedAt,
	}
}

func operatorAgentDiagnosisRuntimeStateFromConversationWatchdog(w *operatorread.OperatorConversationWatchdog) *operatorread.OperatorAgentDiagnosisRuntimeState {
	if w == nil {
		return nil
	}
	return &operatorread.OperatorAgentDiagnosisRuntimeState{
		Watchdog: &operatorread.OperatorAgentDiagnosisWatchdog{
			State:         strings.TrimSpace(w.State),
			BlockingLayer: strings.TrimSpace(w.BlockingLayer),
			Action:        strings.TrimSpace(w.Action),
			Outcome:       strings.TrimSpace(w.Outcome),
			LastOutputAt:  strings.TrimSpace(w.LastOutputAt),
			RecordedAt:    strings.TrimSpace(w.RecordedAt),
		},
	}
}

func conversationWatchdogDescriptorFromAgentDiagnosis(w operatorread.OperatorAgentDiagnosisWatchdog) ConversationRuntimeWatchdogDescriptor {
	return ConversationRuntimeWatchdogDescriptor{
		State:         strings.TrimSpace(w.State),
		BlockingLayer: strings.TrimSpace(w.BlockingLayer),
		Action:        strings.TrimSpace(w.Action),
		Outcome:       strings.TrimSpace(w.Outcome),
		LastOutputAt:  strings.TrimSpace(w.LastOutputAt),
		RecordedAt:    strings.TrimSpace(w.RecordedAt),
	}
}

func encodeConversationPositionCursor(cursor conversationPositionCursor) string {
	raw, _ := json.Marshal(cursor)
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodeConversationPositionCursor(raw string, kind string) (conversationPositionCursor, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(raw))
	if err != nil {
		return conversationPositionCursor{}, operatorread.ErrInvalidConversationCursor
	}
	var cursor conversationPositionCursor
	if err := json.Unmarshal(decoded, &cursor); err != nil {
		return conversationPositionCursor{}, operatorread.ErrInvalidConversationCursor
	}
	if strings.TrimSpace(cursor.Kind) != kind {
		return conversationPositionCursor{}, operatorread.ErrInvalidConversationCursor
	}
	return cursor, nil
}

func maxStoreInt(v, floor int) int {
	if v < floor {
		return floor
	}
	return v
}

func operatorAgentPersistedType(cfg runtimeactors.AgentConfig, modelAlias string) string {
	if value := strings.TrimSpace(cfg.Type); value != "" {
		return value
	}
	if value := strings.TrimSpace(modelAlias); value != "" {
		return value
	}
	return "generic"
}
