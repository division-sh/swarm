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
	projections, err := r.loadOperatorProjections(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]genericAgent, 0, len(items))
	for _, item := range items {
		projection, ok := projections[item.ID]
		if !ok {
			return nil, fmt.Errorf("missing agent operator projection: %s", strings.TrimSpace(item.ID))
		}
		applyOperatorProjection(&item, projection, r.turnLimit)
		out = append(out, item)
	}
	return out, nil
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

type agentOperatorProjection struct {
	Status              string
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
	LastTool            *AgentLastTool
}

type pendingAgentDeliveryFactSource interface {
	ListPendingAgentDeliveryFacts(ctx context.Context, agentIDs []string, since time.Time) (map[string]store.PendingAgentDeliveryFacts, error)
}

func (r *SQLAgentReader) loadOperatorProjections(ctx context.Context) (map[string]agentOperatorProjection, error) {
	caps, err := r.resolveCapabilities(ctx)
	if err != nil {
		return nil, err
	}
	if err := requireAgentOperatorProjectionCapabilities(caps); err != nil {
		return nil, err
	}
	latestTurnBlocksExpr := `'[]'::jsonb`
	if caps.Conversations.TurnBlocks {
		latestTurnBlocksExpr = `COALESCE(turn_blocks, '[]'::jsonb)`
	}
	rows, err := r.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT
			a.agent_id,
			COALESCE(a.status, 'active'),
			COALESCE(sess.turn_count, 0),
			COALESCE(sess.lease_holder, ''),
			sess.lease_expires_at,
			0,
			0,
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
				COUNT(*) FILTER (WHERE status = 'failed')::int AS failures_24h,
				COUNT(*) FILTER (WHERE status = 'dead_letter')::int AS dead_letters_24h
			FROM event_deliveries
			WHERE subscriber_type = 'agent'
			  AND subscriber_id = a.agent_id
			  AND COALESCE(delivered_at, created_at) >= now() - interval '24 hours'
		) f ON true
		WHERE a.status NOT IN ('terminated', 'ephemeral')
		ORDER BY a.created_at ASC, a.agent_id ASC
	`, latestTurnBlocksExpr), runtimesessions.RuntimeModeSession, runtimesessions.RuntimeModeSessionPerEntity)
	if err != nil {
		return nil, fmt.Errorf("query agent operator projections: %w", err)
	}
	defer rows.Close()

	out := map[string]agentOperatorProjection{}
	agentIDs := make([]string, 0)
	for rows.Next() {
		var (
			id            string
			projection    agentOperatorProjection
			lockExpiresAt sql.NullTime
			latestTaskID  string
			latestParseOK bool
			latestTurnRaw []byte
		)
		if err := rows.Scan(
			&id,
			&projection.Status,
			&projection.TurnCount,
			&projection.LockOwner,
			&lockExpiresAt,
			&projection.PendingEvents,
			&projection.OldestPendingAgeSec,
			&projection.Failures24h,
			&projection.DeadLetters24h,
			&projection.Turns24h,
			&latestTaskID,
			&latestParseOK,
			&latestTurnRaw,
		); err != nil {
			return nil, fmt.Errorf("scan agent operator projection: %w", err)
		}
		if lockExpiresAt.Valid {
			projection.LockExpiresAt = lockExpiresAt.Time
			if lockExpiresAt.Time.After(time.Now()) && strings.TrimSpace(projection.LockOwner) != "" {
				projection.InFlightTurn = true
			}
		}
		if err := enrichAgentOperatorProjectionFromLatestTurn(&projection, latestTaskID, latestParseOK, latestTurnRaw); err != nil {
			return nil, err
		}
		id = strings.TrimSpace(id)
		out[id] = projection
		agentIDs = append(agentIDs, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read agent operator projection rows: %w", err)
	}
	factSource, ok := r.base.(pendingAgentDeliveryFactSource)
	if !ok || factSource == nil {
		return nil, fmt.Errorf("missing pending agent delivery fact source")
	}
	factsByAgent, err := factSource.ListPendingAgentDeliveryFacts(ctx, agentIDs, time.Time{})
	if err != nil {
		return nil, err
	}
	for agentID, facts := range factsByAgent {
		projection := out[strings.TrimSpace(agentID)]
		projection.PendingEvents = facts.PendingCount
		projection.OldestPendingAgeSec = facts.OldestPendingAgeSec
		out[strings.TrimSpace(agentID)] = projection
	}
	return out, nil
}

func (r *SQLAgentReader) resolveCapabilities(ctx context.Context) (store.StoreSchemaCapabilities, error) {
	if r != nil {
		if source, ok := r.base.(schemaCapabilitySource); ok && source != nil {
			return source.ResolveSchemaCapabilities(ctx)
		}
	}
	return store.StoreSchemaCapabilities{}, missingDashboardCapabilityOwner("agent reader")
}

func applyOperatorProjection(agent *genericAgent, projection agentOperatorProjection, turnLimit int) {
	if agent == nil {
		return
	}
	agent.Status = strings.TrimSpace(projection.Status)
	agent.PendingEvents = projection.PendingEvents
	agent.OldestPendingAgeSec = projection.OldestPendingAgeSec
	agent.LockOwner = strings.TrimSpace(projection.LockOwner)
	agent.LockExpiresAt = formatTime(projection.LockExpiresAt)
	agent.InFlightTurn = projection.InFlightTurn
	agent.InFlightSeconds = projection.InFlightSeconds
	agent.Failures24h = projection.Failures24h
	agent.DeadLetters24h = projection.DeadLetters24h
	agent.TurnCount = projection.TurnCount
	agent.TurnLimit = maxInt(turnLimit, 0)
	agent.Turns24h = maxInt(projection.Turns24h, 0)
	agent.CurrentTaskID = strings.TrimSpace(projection.CurrentTaskID)
	agent.LastTool = projection.LastTool
	if agent.TurnLimit > 0 {
		agent.NearBreaker = agent.TurnCount*100 >= agent.TurnLimit*85
	}
	agent.State = projection.state()
}

func (p agentOperatorProjection) state() string {
	status := strings.ToLower(strings.TrimSpace(p.Status))
	if status == "terminated" {
		return "terminated"
	}
	if p.InFlightTurn {
		return "running"
	}
	if p.DeadLetters24h > 0 || p.Failures24h > 0 {
		return "stuck"
	}
	return "idle"
}

func enrichAgentOperatorProjectionFromLatestTurn(projection *agentOperatorProjection, taskID string, parseOK bool, turnBlocksRaw []byte) error {
	if projection == nil {
		return nil
	}
	projection.CurrentTaskID = strings.TrimSpace(taskID)
	summary, ok, err := decodeTurnSummaryProjection(turnBlocksRaw)
	if err != nil {
		return fmt.Errorf("decode latest agent turn turn_summary: %w", err)
	}
	if ok {
		projection.LastTool, err = projectedTurnSummaryLastToolTransport(summary, parseOK)
		if err != nil {
			return fmt.Errorf("decode latest agent turn last_tool: %w", err)
		}
	}
	return nil
}

func maxInt(v, floor int) int {
	if v < floor {
		return floor
	}
	return v
}
