package dashboard

import (
	"database/sql"
	"fmt"
	"net/http"
	"strings"
	"time"
)

func (s *Server) handleAPIAgentPrompt(w http.ResponseWriter, r *http.Request) {
	prefix := "/api/agents/"
	if strings.HasPrefix(r.URL.Path, "/dashboard/api/agents/") {
		prefix = "/dashboard/api/agents/"
	}
	path := strings.TrimPrefix(r.URL.Path, prefix)
	path = strings.Trim(path, "/")
	parts := strings.Split(path, "/")
	if len(parts) < 2 || strings.TrimSpace(parts[0]) == "" || parts[1] != "prompt" {
		http.NotFound(w, r)
		return
	}
	if s.manager == nil {
		writeErr(w, http.StatusServiceUnavailable, fmt.Errorf("agent manager unavailable"))
		return
	}
	agentID := strings.TrimSpace(parts[0])

	// GET /api/agents/:id/prompt/diff
	if len(parts) == 3 && parts[2] == "diff" {
		if !allowMethod(w, r, http.MethodGet) {
			return
		}
		state, err := s.manager.GetAgentPromptState(r.Context(), agentID)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		overridePrompt := ""
		if state.Override != nil {
			overridePrompt = strings.TrimSpace(state.Override.Prompt)
		}
		diff := renderPromptDiff(state.TemplatePrompt, overridePrompt)
		writeJSON(w, http.StatusOK, map[string]any{
			"agent_id":        state.AgentID,
			"role":            state.Role,
			"mode":            state.Mode,
			"template_prompt": state.TemplatePrompt,
			"override_prompt": overridePrompt,
			"has_override":    state.Override != nil,
			"diff":            diff,
			"generated_at":    s.now().UTC(),
		})
		return
	}

	// /api/agents/:id/prompt
	if len(parts) != 2 {
		http.NotFound(w, r)
		return
	}
	switch r.Method {
	case http.MethodGet:
		state, err := s.manager.GetAgentPromptState(r.Context(), agentID)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		override := map[string]any(nil)
		if state.Override != nil {
			override = map[string]any{
				"prompt":          state.Override.Prompt,
				"previous_prompt": state.Override.PreviousPrompt,
				"source":          state.Override.Source,
				"notes":           state.Override.Notes,
				"created_at":      state.Override.CreatedAt,
				"updated_at":      state.Override.UpdatedAt,
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"agent_id":         state.AgentID,
			"role":             state.Role,
			"mode":             state.Mode,
			"template_prompt":  state.TemplatePrompt,
			"effective_prompt": state.EffectivePrompt,
			"has_override":     state.Override != nil,
			"override":         override,
			"generated_at":     s.now().UTC(),
		})
	case http.MethodPut:
		var req struct {
			Prompt string `json:"prompt"`
			Source string `json:"source"`
			Notes  string `json:"notes"`
		}
		if err := decodeJSONBody(r, &req); err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		if strings.TrimSpace(req.Prompt) == "" {
			writeErr(w, http.StatusBadRequest, fmt.Errorf("prompt is required"))
			return
		}
		source := strings.TrimSpace(req.Source)
		if source == "" {
			source = "api"
		}
		if err := s.manager.SetAgentPromptOverride(r.Context(), agentID, req.Prompt, source, req.Notes); err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":       true,
			"agent_id": agentID,
			"action":   "set_override",
		})
	case http.MethodDelete:
		if err := s.manager.RevertAgentPromptOverride(r.Context(), agentID); err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":       true,
			"agent_id": agentID,
			"action":   "revert_override",
		})
	default:
		writeErr(w, http.StatusMethodNotAllowed, fmt.Errorf("method not allowed"))
	}
}

type agentView struct {
	ID                  string     `json:"id"`
	Role                string     `json:"role"`
	Mode                string     `json:"mode"`
	Status              string     `json:"status"`
	VerticalID          string     `json:"vertical_id"`
	VerticalSlug        string     `json:"vertical_slug"`
	CurrentTaskID       string     `json:"current_task_id"`
	StartedAt           time.Time  `json:"started_at"`
	LastActiveAt        time.Time  `json:"last_active_at"`
	RuntimeMode         string     `json:"runtime_mode"`
	SessionID           string     `json:"session_id"`
	TurnCount           int        `json:"turn_count"`
	Turns24h            int        `json:"turns_24h"`
	TurnLimit           int        `json:"turn_limit"`
	NearBreaker         bool       `json:"near_breaker"`
	LockOwner           string     `json:"lock_owner"`
	LockExpiresAt       *time.Time `json:"lock_expires_at,omitempty"`
	LastUsedAt          *time.Time `json:"last_used_at,omitempty"`
	PendingEvents       int        `json:"pending_events"`
	OldestPendingAt     *time.Time `json:"oldest_pending_at,omitempty"`
	OldestPendingAgeSec int        `json:"oldest_pending_age_sec,omitempty"`
	InFlightTurn        bool       `json:"in_flight_turn"`
	InFlightSeconds     int        `json:"in_flight_seconds,omitempty"`
	DeadLetters24h      int        `json:"dead_letters_24h"`
	Failures24h         int        `json:"failures_24h"`
	InputTokens24h      int64      `json:"input_tokens_24h"`
	OutputTokens24h     int64      `json:"output_tokens_24h"`
	TotalTokens24h      int64      `json:"total_tokens_24h"`
	State               string     `json:"state"`
	StuckReason         string     `json:"stuck_reason,omitempty"`
	SystemPrompt        string     `json:"system_prompt,omitempty"`
	CreationEvent       eventRef   `json:"creation_event"`
	LastTool            toolView   `json:"last_tool"`
}

type eventRef struct {
	ID        string     `json:"id,omitempty"`
	Type      string     `json:"type,omitempty"`
	CreatedAt *time.Time `json:"created_at,omitempty"`
}

type toolView struct {
	Name           string     `json:"name,omitempty"`
	OK             *bool      `json:"ok,omitempty"`
	Error          string     `json:"error,omitempty"`
	ErrorCode      string     `json:"error_code,omitempty"`
	ErrorComponent string     `json:"error_component,omitempty"`
	ErrorOperation string     `json:"error_operation,omitempty"`
	ErrorRetryable *bool      `json:"error_retryable,omitempty"`
	Result         string     `json:"result,omitempty"`
	CreatedAt      *time.Time `json:"created_at,omitempty"`
}

func (s *Server) handleAgents(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}
	ctx := r.Context()
	turnLimit := 40
	if s.cfg != nil && s.cfg.LLM.Session.RotateAfterTurns > 0 {
		turnLimit = s.cfg.LLM.Session.RotateAfterTurns
	}

	const q = `
		WITH latest_session AS (
			SELECT DISTINCT ON (agent_id)
				agent_id,
				runtime_mode,
				session_id,
				turn_count,
				COALESCE(lock_owner, '') AS lock_owner,
				lock_expires_at,
				last_used_at,
				COALESCE(checkpoint_summary, '') AS checkpoint_summary
			FROM agent_sessions
			WHERE status = 'active'
			ORDER BY agent_id, last_used_at DESC
		),
		pending AS (
			SELECT d.agent_id, count(*) AS pending_count, min(e.created_at) AS oldest_pending_at
			FROM event_deliveries d
			INNER JOIN events e ON e.id = d.event_id
			LEFT JOIN event_receipts r
				ON r.event_id = d.event_id
				AND r.agent_id = d.agent_id
			WHERE r.event_id IS NULL
				OR (r.status = 'error' AND r.retry_count < 3)
			GROUP BY d.agent_id
		),
		dead AS (
			SELECT agent_id, count(*) AS dead_count
			FROM event_receipts
			WHERE status = 'dead_letter'
			  AND processed_at >= now() - interval '24 hours'
			GROUP BY agent_id
		),
		recent_fail AS (
			SELECT agent_id, count(*) AS fail_count
			FROM agent_turns
			WHERE created_at >= now() - interval '24 hours'
			  AND (parse_ok = false OR COALESCE(error, '') <> '')
			GROUP BY agent_id
		),
		turn_agg AS (
			SELECT agent_id, count(*)::int AS turns_24h
			FROM agent_turns
			WHERE created_at >= now() - interval '24 hours'
			GROUP BY agent_id
		),
		token_turn_agg AS (
			SELECT agent_id,
				sum(COALESCE(NULLIF(regexp_replace(COALESCE(response_payload->'usage'->>'input_tokens', response_payload->'usage'->>'inputTokens', response_payload->>'input_tokens', '0'), '[^0-9]', '', 'g'), '')::bigint, 0)) AS input_tokens,
				sum(COALESCE(NULLIF(regexp_replace(COALESCE(response_payload->'usage'->>'output_tokens', response_payload->'usage'->>'outputTokens', response_payload->>'output_tokens', '0'), '[^0-9]', '', 'g'), '')::bigint, 0)) AS output_tokens
			FROM agent_turns
			WHERE created_at >= now() - interval '24 hours'
			GROUP BY agent_id
		),
		token_spend_agg AS (
			SELECT
				agent_id,
				sum(COALESCE(NULLIF(regexp_replace(COALESCE(metadata->>'input_tokens', '0'), '[^0-9]', '', 'g'), '')::bigint, 0)) AS input_tokens,
				sum(COALESCE(NULLIF(regexp_replace(COALESCE(metadata->>'output_tokens', '0'), '[^0-9]', '', 'g'), '')::bigint, 0)) AS output_tokens
			FROM spend_ledger
			WHERE created_at >= now() - interval '24 hours'
			  AND category = 'llm'
			  AND COALESCE(agent_id, '') <> ''
			GROUP BY agent_id
		)
			SELECT
				a.id,
				a.role,
				a.mode,
				a.status,
				COALESCE(a.vertical_id::text, ''),
				COALESCE(v.slug, ''),
				COALESCE(a.current_task_id::text, ''),
				a.started_at,
				a.last_active_at,
				COALESCE(ls.runtime_mode, ''),
				COALESCE(ls.session_id, ''),
			COALESCE(ls.turn_count, 0),
			COALESCE(ta.turns_24h, 0),
			COALESCE(ls.lock_owner, ''),
			ls.lock_expires_at,
			ls.last_used_at,
			COALESCE(p.pending_count, 0),
			p.oldest_pending_at,
			COALESCE(d.dead_count, 0),
			COALESCE(f.fail_count, 0),
			COALESCE(ts.input_tokens, tt.input_tokens, 0),
			COALESCE(ts.output_tokens, tt.output_tokens, 0),
				COALESCE((SELECT payload->>'tool_name' FROM events e2 WHERE e2.type = 'agent.tool_execution' AND e2.source_agent = a.id ORDER BY e2.created_at DESC LIMIT 1), ''),
				COALESCE((SELECT payload->>'ok' FROM events e2 WHERE e2.type = 'agent.tool_execution' AND e2.source_agent = a.id ORDER BY e2.created_at DESC LIMIT 1), ''),
				COALESCE((SELECT payload->>'error' FROM events e2 WHERE e2.type = 'agent.tool_execution' AND e2.source_agent = a.id ORDER BY e2.created_at DESC LIMIT 1), ''),
				COALESCE((SELECT payload->>'result' FROM events e2 WHERE e2.type = 'agent.tool_execution' AND e2.source_agent = a.id ORDER BY e2.created_at DESC LIMIT 1), ''),
				COALESCE((SELECT e2.id::text FROM events e2 WHERE e2.source_agent = a.id ORDER BY e2.created_at ASC LIMIT 1), ''),
				COALESCE((SELECT e2.type FROM events e2 WHERE e2.source_agent = a.id ORDER BY e2.created_at ASC LIMIT 1), ''),
				(SELECT e2.created_at FROM events e2 WHERE e2.source_agent = a.id ORDER BY e2.created_at ASC LIMIT 1),
				(SELECT e2.created_at FROM events e2 WHERE e2.type = 'agent.tool_execution' AND e2.source_agent = a.id ORDER BY e2.created_at DESC LIMIT 1),
				COALESCE(a.config, '{}'::jsonb)
			FROM agents a
		LEFT JOIN verticals v ON v.id = a.vertical_id
		LEFT JOIN latest_session ls ON ls.agent_id = a.id
		LEFT JOIN pending p ON p.agent_id = a.id
		LEFT JOIN dead d ON d.agent_id = a.id
		LEFT JOIN recent_fail f ON f.agent_id = a.id
		LEFT JOIN turn_agg ta ON ta.agent_id = a.id
		LEFT JOIN token_turn_agg tt ON tt.agent_id = a.id
		LEFT JOIN token_spend_agg ts ON ts.agent_id = a.id
		ORDER BY a.last_active_at DESC, a.id ASC
	`

	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		status := http.StatusInternalServerError
		if strings.Contains(strings.ToLower(err.Error()), "runtime manager unavailable") {
			status = http.StatusServiceUnavailable
		}
		writeErr(w, status, err)
		return
	}
	defer rows.Close()

	agents := make([]agentView, 0, 64)
	stateCounts := map[string]int{"running": 0, "idle": 0, "stuck": 0, "terminated": 0}
	now := s.now()

	for rows.Next() {
		var av agentView
		av.TurnLimit = turnLimit
		var lockExp sql.NullTime
		var lastUsed sql.NullTime
		var oldestPending sql.NullTime
		var toolOK string
		var creationAt sql.NullTime
		var toolAt sql.NullTime
		var configRaw []byte
		if err := rows.Scan(
			&av.ID,
			&av.Role,
			&av.Mode,
			&av.Status,
			&av.VerticalID,
			&av.VerticalSlug,
			&av.CurrentTaskID,
			&av.StartedAt,
			&av.LastActiveAt,
			&av.RuntimeMode,
			&av.SessionID,
			&av.TurnCount,
			&av.Turns24h,
			&av.LockOwner,
			&lockExp,
			&lastUsed,
			&av.PendingEvents,
			&oldestPending,
			&av.DeadLetters24h,
			&av.Failures24h,
			&av.InputTokens24h,
			&av.OutputTokens24h,
			&av.LastTool.Name,
			&toolOK,
			&av.LastTool.Error,
			&av.LastTool.Result,
			&av.CreationEvent.ID,
			&av.CreationEvent.Type,
			&creationAt,
			&toolAt,
			&configRaw,
		); err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		if lockExp.Valid {
			av.LockExpiresAt = &lockExp.Time
		}
		if lastUsed.Valid {
			av.LastUsedAt = &lastUsed.Time
		}
		if oldestPending.Valid {
			av.OldestPendingAt = &oldestPending.Time
			if age := int(now.Sub(oldestPending.Time).Seconds()); age > 0 {
				av.OldestPendingAgeSec = age
			}
		}
		if toolAt.Valid {
			av.LastTool.CreatedAt = &toolAt.Time
		}
		if creationAt.Valid {
			av.CreationEvent.CreatedAt = &creationAt.Time
		}
		if av.CreationEvent.ID == "" {
			av.CreationEvent.Type = "agent.started"
			started := av.StartedAt
			av.CreationEvent.CreatedAt = &started
		}
		if toolOK != "" {
			ok := strings.EqualFold(toolOK, "true")
			av.LastTool.OK = &ok
		}
		toolErrMeta := parseRuntimeErrorMetadata(av.LastTool.Error)
		av.LastTool.ErrorCode = toolErrMeta.Code
		av.LastTool.ErrorComponent = toolErrMeta.Component
		av.LastTool.ErrorOperation = toolErrMeta.Operation
		av.LastTool.ErrorRetryable = toolErrMeta.Retryable
		av.LastTool.Result = truncate(av.LastTool.Result, 300)
		av.SystemPrompt, _, _, _ = parseAgentRuntimeConfig(configRaw)
		av.TotalTokens24h = av.InputTokens24h + av.OutputTokens24h

		if turnLimit > 0 {
			av.NearBreaker = float64(av.TurnCount)/float64(turnLimit) >= 0.9
		}
		if av.LockExpiresAt != nil && strings.TrimSpace(av.LockOwner) != "" && av.LockExpiresAt.After(now) {
			av.InFlightTurn = true
			switch {
			case av.OldestPendingAgeSec > 0:
				av.InFlightSeconds = av.OldestPendingAgeSec
			case av.LastUsedAt != nil:
				if sec := int(now.Sub(*av.LastUsedAt).Seconds()); sec > 0 {
					av.InFlightSeconds = sec
				}
			}
		}
		av.State, av.StuckReason = classifyAgent(av, now)
		stateCounts[av.State]++
		agents = append(agents, av)
	}
	if err := rows.Err(); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"generated_at": s.now().UTC(),
		"turn_limit":   turnLimit,
		"states":       stateCounts,
		"agents":       agents,
	})
}

func classifyAgent(a agentView, now time.Time) (string, string) {
	lastSignalAt := latestAgentSignalAt(a)
	if strings.EqualFold(a.Status, "terminated") {
		return "terminated", ""
	}
	if a.DeadLetters24h > 0 {
		return "stuck", "dead-letter receipts in last 24h"
	}
	if a.Failures24h >= 3 {
		return "stuck", "3+ failed turns in last 24h"
	}
	if a.PendingEvents > 0 && now.Sub(lastSignalAt) > 10*time.Minute {
		return "stuck", "pending deliveries while inactive for >10m"
	}
	if a.NearBreaker && a.Failures24h > 0 {
		return "stuck", "near turn circuit breaker with failures"
	}
	if a.LockOwner != "" || a.PendingEvents > 0 || now.Sub(lastSignalAt) < 2*time.Minute {
		return "running", ""
	}
	return "idle", ""
}

func latestAgentSignalAt(a agentView) time.Time {
	latest := a.LastActiveAt
	if a.LastUsedAt != nil && a.LastUsedAt.After(latest) {
		latest = *a.LastUsedAt
	}
	if a.LastTool.CreatedAt != nil && a.LastTool.CreatedAt.After(latest) {
		latest = *a.LastTool.CreatedAt
	}
	return latest
}
