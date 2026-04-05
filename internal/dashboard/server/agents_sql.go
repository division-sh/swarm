package server

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	runtimemanager "swarm/internal/runtime/manager"
	runtimesessions "swarm/internal/runtime/sessions"
	"swarm/internal/store"
)

type SQLAgentReader struct {
	db        *sql.DB
	base      AgentReader
	turnLimit int
}

func NewSQLAgentReader(db *sql.DB, base AgentReader, turnLimit int) *SQLAgentReader {
	if db == nil && base == nil {
		return nil
	}
	return &SQLAgentReader{
		db:        db,
		base:      base,
		turnLimit: turnLimit,
	}
}

func (r *SQLAgentReader) LoadAgents(ctx context.Context) ([]runtimemanager.PersistedAgent, error) {
	if r == nil || r.base == nil {
		return nil, nil
	}
	return r.base.LoadAgents(ctx)
}

func (r *SQLAgentReader) ListGenericAgents(ctx context.Context) ([]genericAgent, error) {
	baseRows, err := r.LoadAgents(ctx)
	if err != nil {
		return nil, err
	}
	items := make([]genericAgent, 0, len(baseRows))
	for _, row := range baseRows {
		items = append(items, toGenericAgent(row))
	}
	if r == nil || r.db == nil || len(items) == 0 {
		return items, nil
	}
	aggregates, err := r.loadAggregates(ctx)
	if err != nil {
		return nil, err
	}
	for i := range items {
		aggregate, ok := aggregates[items[i].ID]
		if !ok {
			items[i].State = deriveGenericAgentState(items[i], agentRuntimeAggregate{})
			continue
		}
		applyAggregate(&items[i], aggregate, r.turnLimit)
	}
	return items, nil
}

func (r *SQLAgentReader) GetGenericAgent(ctx context.Context, id string) (genericAgent, bool, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return genericAgent{}, false, nil
	}
	rows, err := r.ListGenericAgents(ctx)
	if err != nil {
		return genericAgent{}, false, err
	}
	for _, row := range rows {
		if row.ID == id {
			return row, true, nil
		}
	}
	return genericAgent{}, false, nil
}

type agentRuntimeAggregate struct {
	PendingEvents       int
	OldestPendingAgeSec int
	LockOwner           string
	LockExpiresAt       time.Time
	InFlightTurn        bool
	InFlightSeconds     int
	Failures24h         int
	DeadLetters24h      int
	TurnCount           int
	Turns24h            int
	CurrentTaskID       string
	LastTool            map[string]any
}

func (r *SQLAgentReader) loadAggregates(ctx context.Context) (map[string]agentRuntimeAggregate, error) {
	caps, err := r.resolveCapabilities(ctx)
	if err != nil {
		return nil, err
	}
	latestTurnBlocksExpr := `'[]'::jsonb`
	if caps.Conversations.TurnBlocks {
		latestTurnBlocksExpr = `COALESCE(turn_blocks, '[]'::jsonb)`
	}
	rows, err := r.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT
			a.agent_id,
			COALESCE(sess.turn_count, 0),
			COALESCE(sess.lease_holder, ''),
			sess.lease_expires_at,
			COALESCE(p.pending_count, 0),
			COALESCE(p.oldest_pending_age_sec, 0),
			COALESCE(f.failures_24h, 0),
			COALESCE(f.dead_letters_24h, 0),
			0,
			COALESCE(latest_turn.task_id, ''),
			COALESCE(latest_turn.parse_ok, false),
			COALESCE(latest_turn.turn_blocks, '[]'::jsonb)
		FROM agents a
		LEFT JOIN LATERAL (
			SELECT
				turn_count,
				lease_holder,
				lease_expires_at
			FROM agent_sessions
			WHERE agent_id = a.agent_id
			  AND status = 'active'
			  AND runtime_mode IN ($1, $2)
			ORDER BY updated_at DESC, created_at DESC
			LIMIT 1
		) sess ON true
		LEFT JOIN LATERAL (
			SELECT
				COALESCE(task_id, '') AS task_id,
				parse_ok,
				%s AS turn_blocks
			FROM agent_turns
			WHERE agent_id = a.agent_id
			ORDER BY created_at DESC, turn_id DESC
			LIMIT 1
		) latest_turn ON true
		LEFT JOIN LATERAL (
			SELECT
				COUNT(*)::int AS pending_count,
				COALESCE(MAX(EXTRACT(EPOCH FROM now() - e.created_at))::int, 0) AS oldest_pending_age_sec
			FROM event_deliveries d
			INNER JOIN events e ON e.event_id = d.event_id
			LEFT JOIN event_receipts r
				ON r.event_id = d.event_id
				AND r.subscriber_type = 'agent'
				AND r.subscriber_id = d.subscriber_id
			WHERE d.subscriber_type = 'agent'
			  AND d.subscriber_id = a.agent_id
			  AND (
					r.event_id IS NULL
					OR COALESCE(r.side_effects->>'manager_status', '') = 'error'
				)
		) p ON true
		LEFT JOIN LATERAL (
			SELECT
				COUNT(*) FILTER (WHERE outcome = 'error')::int AS failures_24h,
				COUNT(*) FILTER (WHERE outcome = 'dead_letter')::int AS dead_letters_24h
			FROM event_receipts
			WHERE subscriber_type = 'agent'
			  AND subscriber_id = a.agent_id
			  AND processed_at >= now() - interval '24 hours'
		) f ON true
		WHERE a.status NOT IN ('terminated', 'ephemeral')
		ORDER BY a.created_at ASC, a.agent_id ASC
	`, latestTurnBlocksExpr), runtimesessions.RuntimeModeSession, runtimesessions.RuntimeModeSessionPerEntity)
	if err != nil {
		return nil, fmt.Errorf("query agent aggregates: %w", err)
	}
	defer rows.Close()

	out := map[string]agentRuntimeAggregate{}
	for rows.Next() {
		var (
			id            string
			aggregate     agentRuntimeAggregate
			lockExpiresAt sql.NullTime
			latestTaskID  string
			latestParseOK bool
			latestTurnRaw []byte
		)
		if err := rows.Scan(
			&id,
			&aggregate.TurnCount,
			&aggregate.LockOwner,
			&lockExpiresAt,
			&aggregate.PendingEvents,
			&aggregate.OldestPendingAgeSec,
			&aggregate.Failures24h,
			&aggregate.DeadLetters24h,
			&aggregate.Turns24h,
			&latestTaskID,
			&latestParseOK,
			&latestTurnRaw,
		); err != nil {
			return nil, fmt.Errorf("scan agent aggregate: %w", err)
		}
		if lockExpiresAt.Valid {
			aggregate.LockExpiresAt = lockExpiresAt.Time
			if lockExpiresAt.Time.After(time.Now()) && strings.TrimSpace(aggregate.LockOwner) != "" {
				aggregate.InFlightTurn = true
			}
		}
		if err := enrichAgentAggregateFromLatestTurn(&aggregate, latestTaskID, latestParseOK, latestTurnRaw); err != nil {
			return nil, err
		}
		out[strings.TrimSpace(id)] = aggregate
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read agent aggregate rows: %w", err)
	}
	return out, nil
}

func (r *SQLAgentReader) resolveCapabilities(ctx context.Context) (store.StoreSchemaCapabilities, error) {
	if r != nil {
		if source, ok := r.base.(conversationCapabilitySource); ok && source != nil {
			return source.ResolveSchemaCapabilities(ctx)
		}
	}
	return store.StoreSchemaCapabilities{
		Conversations: store.ConversationSchemaCapabilities{
			Sessions:   store.SchemaFlavorCanonical,
			Audits:     store.SchemaFlavorCanonical,
			Turns:      store.SchemaFlavorCanonical,
			TurnBlocks: true,
		},
	}, nil
}

func applyAggregate(agent *genericAgent, aggregate agentRuntimeAggregate, turnLimit int) {
	if agent == nil {
		return
	}
	agent.PendingEvents = aggregate.PendingEvents
	agent.OldestPendingAgeSec = aggregate.OldestPendingAgeSec
	agent.LockOwner = strings.TrimSpace(aggregate.LockOwner)
	agent.LockExpiresAt = formatTime(aggregate.LockExpiresAt)
	agent.InFlightTurn = aggregate.InFlightTurn
	agent.InFlightSeconds = aggregate.InFlightSeconds
	agent.Failures24h = aggregate.Failures24h
	agent.DeadLetters24h = aggregate.DeadLetters24h
	agent.TurnCount = aggregate.TurnCount
	agent.TurnLimit = maxInt(turnLimit, 0)
	agent.Turns24h = maxInt(aggregate.Turns24h, 0)
	agent.CurrentTaskID = strings.TrimSpace(aggregate.CurrentTaskID)
	agent.LastTool = aggregate.LastTool
	if agent.TurnLimit > 0 {
		agent.NearBreaker = agent.TurnCount*100 >= agent.TurnLimit*85
	}
	agent.State = deriveGenericAgentState(*agent, aggregate)
}

func deriveGenericAgentState(agent genericAgent, aggregate agentRuntimeAggregate) string {
	status := strings.ToLower(strings.TrimSpace(agent.Status))
	if status == "terminated" {
		return "terminated"
	}
	if aggregate.InFlightTurn {
		return "running"
	}
	if aggregate.DeadLetters24h > 0 || aggregate.Failures24h > 0 {
		return "stuck"
	}
	return "idle"
}

func enrichAgentAggregateFromLatestTurn(aggregate *agentRuntimeAggregate, taskID string, parseOK bool, turnBlocksRaw []byte) error {
	if aggregate == nil {
		return nil
	}
	aggregate.CurrentTaskID = strings.TrimSpace(taskID)
	summary, ok, err := decodeTurnSummaryProjection(turnBlocksRaw)
	if err != nil {
		return fmt.Errorf("decode latest agent turn turn_summary: %w", err)
	}
	if ok {
		aggregate.LastTool = summary.lastToolMap(parseOK)
	}
	return nil
}

func maxInt(v, floor int) int {
	if v < floor {
		return floor
	}
	return v
}
