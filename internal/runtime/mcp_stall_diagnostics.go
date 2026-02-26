package runtime

import (
	"context"
	"database/sql"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

type MCPStallDiagnosticConfig struct {
	Interval      time.Duration
	PendingAge    time.Duration
	Cooldown      time.Duration
	MinPending    int
	ArtifactLines int
}

func DefaultMCPStallDiagnosticConfig() MCPStallDiagnosticConfig {
	return MCPStallDiagnosticConfig{
		Interval:      45 * time.Second,
		PendingAge:    5 * time.Minute,
		Cooldown:      7 * time.Minute,
		MinPending:    3,
		ArtifactLines: 20,
	}
}

type mcpStallCandidate struct {
	AgentID       string
	Role          string
	Mode          string
	VerticalID    string
	PendingCount  int
	OldestPending time.Time
	LastTurnAt    sql.NullTime
	SessionID     string
	RuntimeMode   string
	SessionStatus string
}

var mcpStallDiagSeen sync.Map

func StartMCPStallDiagnosticLoop(ctx context.Context, db *sql.DB, logger *RuntimeLogger, cfg MCPStallDiagnosticConfig) {
	if ctx == nil || db == nil || logger == nil {
		return
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 45 * time.Second
	}
	if cfg.PendingAge <= 0 {
		cfg.PendingAge = 5 * time.Minute
	}
	if cfg.Cooldown <= 0 {
		cfg.Cooldown = 7 * time.Minute
	}
	if cfg.MinPending <= 0 {
		cfg.MinPending = 3
	}
	if cfg.ArtifactLines <= 0 {
		cfg.ArtifactLines = 20
	}

	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			runMCPStallDiagnosticsPass(ctx, db, logger, cfg)
		}
	}
}

func runMCPStallDiagnosticsPass(ctx context.Context, db *sql.DB, logger *RuntimeLogger, cfg MCPStallDiagnosticConfig) {
	candidates := queryMCPStallCandidates(ctx, db, cfg)
	if len(candidates) == 0 {
		return
	}
	now := time.Now().UTC()
	for _, c := range candidates {
		key := strings.TrimSpace(c.AgentID) + "|" + strings.TrimSpace(c.SessionID)
		if key == "|" {
			continue
		}
		if lastAny, ok := mcpStallDiagSeen.Load(key); ok {
			if last, ok := lastAny.(time.Time); ok && now.Sub(last) < cfg.Cooldown {
				continue
			}
		}
		mcpStallDiagSeen.Store(key, now)
		emitMCPStallDiagnostic(ctx, db, logger, c, cfg)
	}
}

func queryMCPStallCandidates(ctx context.Context, db *sql.DB, cfg MCPStallDiagnosticConfig) []mcpStallCandidate {
	rows, err := dbQueryContext(ctx, db, `
		SELECT
			a.id,
			COALESCE(a.role, ''),
			COALESCE(a.mode, ''),
			COALESCE(a.vertical_id::text, ''),
			COUNT(*)::int AS pending_count,
			MIN(d.created_at) AS oldest_pending,
			MAX(t.created_at) AS last_turn_at,
			COALESCE(s.session_id, ''),
			COALESCE(s.runtime_mode, ''),
			COALESCE(s.status, '')
		FROM agents a
		JOIN event_deliveries d ON d.agent_id = a.id
		LEFT JOIN event_receipts r ON r.event_id = d.event_id AND r.agent_id = d.agent_id
		LEFT JOIN LATERAL (
			SELECT created_at
			FROM agent_turns
			WHERE agent_id = a.id
			ORDER BY created_at DESC
			LIMIT 1
		) t ON TRUE
		LEFT JOIN LATERAL (
			SELECT session_id, runtime_mode, status
			FROM agent_sessions
			WHERE agent_id = a.id
			ORDER BY (status = 'active') DESC, last_used_at DESC NULLS LAST, created_at DESC
			LIMIT 1
		) s ON TRUE
		WHERE a.status = 'active'
		  AND r.event_id IS NULL
		GROUP BY a.id, a.role, a.mode, a.vertical_id, s.session_id, s.runtime_mode, s.status
		HAVING COUNT(*) >= $1
		   AND MIN(d.created_at) <= now() - ($2::text)::interval
		   AND (MAX(t.created_at) IS NULL OR MAX(t.created_at) <= now() - ($3::text)::interval)
		ORDER BY MIN(d.created_at) ASC
		LIMIT 40
	`, cfg.MinPending, secondsIntervalLiteral(cfg.PendingAge), secondsIntervalLiteral(cfg.PendingAge))
	if err != nil {
		return nil
	}
	defer rows.Close()

	out := make([]mcpStallCandidate, 0, 16)
	for rows.Next() {
		var c mcpStallCandidate
		if scanErr := rows.Scan(
			&c.AgentID,
			&c.Role,
			&c.Mode,
			&c.VerticalID,
			&c.PendingCount,
			&c.OldestPending,
			&c.LastTurnAt,
			&c.SessionID,
			&c.RuntimeMode,
			&c.SessionStatus,
		); scanErr != nil {
			continue
		}
		out = append(out, c)
	}
	return out
}

func emitMCPStallDiagnostic(ctx context.Context, db *sql.DB, logger *RuntimeLogger, c mcpStallCandidate, cfg MCPStallDiagnosticConfig) {
	recentCodes := recentRuntimeErrorCodes(ctx, db, c.AgentID, 30*time.Minute, 20)
	lastCode := ""
	if len(recentCodes) > 0 {
		lastCode = recentCodes[0]
	}
	detail := map[string]any{
		"agent_id":             c.AgentID,
		"role":                 c.Role,
		"mode":                 c.Mode,
		"vertical_id":          c.VerticalID,
		"pending_count":        c.PendingCount,
		"oldest_pending_at":    c.OldestPending.UTC().Format(time.RFC3339),
		"last_turn_at":         nullableTimeRFC3339(c.LastTurnAt),
		"session_id":           c.SessionID,
		"session_runtime_mode": c.RuntimeMode,
		"session_status":       c.SessionStatus,
		"recent_error_codes":   recentCodes,
		"suspected_root_cause": classifyMCPStallCause(lastCode),
	}
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(c.RuntimeMode)), "cli") && strings.TrimSpace(c.SessionID) != "" {
		container, slug := resolveDiagnosticContainer(ctx, db, c.Role, c.VerticalID)
		artifact := map[string]any{
			"container":     container,
			"vertical_slug": slug,
		}
		if strings.TrimSpace(container) != "" {
			if path, err := findSessionProjectFileInContainer(ctx, container, c.SessionID); err == nil && strings.TrimSpace(path) != "" {
				artifact["project_jsonl_path"] = path
				if tail, tailErr := tailContainerPath(ctx, container, path, cfg.ArtifactLines); tailErr == nil && strings.TrimSpace(tail) != "" {
					artifact["project_jsonl_tail"] = snippetForLog(tail, 1800)
				}
			}
			debugPath := "/home/agent/.claude/debug/" + strings.TrimSpace(c.SessionID) + ".txt"
			if tail, err := tailContainerPath(ctx, container, debugPath, cfg.ArtifactLines); err == nil && strings.TrimSpace(tail) != "" {
				artifact["debug_path"] = debugPath
				artifact["debug_tail"] = snippetForLog(tail, 1800)
			}
		}
		detail["session_artifacts"] = artifact
	}

	err := newMCPRuntimeError(
		ErrCodeMCPStallDetected,
		"diagnostics.auto_stall_probe",
		true,
		nil,
		"agent appears stalled with pending deliveries and no recent turns",
	)
	logger.Log(ctx, RuntimeLogEntry{
		Level:     "warn",
		Component: "mcp-diagnostics",
		Action:    "auto_diagnostic_stall",
		AgentID:   c.AgentID,
		VerticalID: func() string {
			return c.VerticalID
		}(),
		Detail: detail,
		Error:  FormatRuntimeError(err),
	})
}

func recentRuntimeErrorCodes(ctx context.Context, db *sql.DB, agentID string, window time.Duration, limit int) []string {
	if strings.TrimSpace(agentID) == "" || db == nil {
		return nil
	}
	if window <= 0 {
		window = 30 * time.Minute
	}
	if limit <= 0 {
		limit = 20
	}
	rows, err := dbQueryContext(ctx, db, `
		SELECT COALESCE(error, '')
		FROM runtime_log
		WHERE agent_id = $1
		  AND ts >= now() - ($2::text)::interval
		  AND COALESCE(error, '') <> ''
		ORDER BY ts DESC
		LIMIT $3
	`, strings.TrimSpace(agentID), secondsIntervalLiteral(window), limit)
	if err != nil {
		return nil
	}
	defer rows.Close()
	out := make([]string, 0, limit)
	seen := map[string]struct{}{}
	for rows.Next() {
		var raw string
		if scanErr := rows.Scan(&raw); scanErr != nil {
			continue
		}
		code := runtimeErrorCodeFromText(raw)
		if strings.TrimSpace(code) == "" {
			continue
		}
		if _, ok := seen[code]; ok {
			continue
		}
		seen[code] = struct{}{}
		out = append(out, code)
	}
	return out
}

func classifyMCPStallCause(code string) string {
	code = strings.TrimSpace(strings.ToLower(code))
	switch code {
	case ErrCodeMCPContextMissing, ErrCodeMCPContextNotFound, ErrCodeMCPContextStale:
		return "mcp_context_lifecycle"
	case ErrCodeMCPAuthMissingBearer, ErrCodeMCPAuthInvalidBearer:
		return "mcp_gateway_auth"
	case ErrCodeMCPToolNotAllowed:
		return "mcp_tool_allowlist"
	case ErrCodeMCPToolExecFailed:
		return "tool_execution_failure"
	default:
		return "unknown"
	}
}

func nullableTimeRFC3339(v sql.NullTime) string {
	if !v.Valid {
		return ""
	}
	return v.Time.UTC().Format(time.RFC3339)
}

func secondsIntervalLiteral(d time.Duration) string {
	if d <= 0 {
		return "0 seconds"
	}
	return strconv.Itoa(int(d.Seconds())) + " seconds"
}

func resolveDiagnosticContainer(ctx context.Context, db *sql.DB, role, verticalID string) (container, verticalSlug string) {
	role = strings.ToLower(strings.TrimSpace(role))
	switch role {
	case "holding-devops":
		return envOrDefault("EMPIREAI_INFRA_CONTAINER", "empireai-infra"), ""
	case "factory-cto",
		"empire-coordinator",
		"operations-analyst",
		"scanner-agent",
		"analysis-agent",
		"validation-coordinator",
		"pre-brand-agent",
		"business-research-agent",
		"lightweight-spec-agent",
		"spec-reviewer",
		"market-research-agent",
		"trend-research-agent",
		"spec-auditor",
		"discovery-coordinator":
		return envOrDefault("EMPIREAI_FACTORY_CONTAINER", "empireai-factory"), ""
	}
	verticalID = strings.TrimSpace(verticalID)
	if verticalID == "" {
		return "", ""
	}
	slug := ""
	_ = db.QueryRowContext(ctx, `
		SELECT COALESCE(NULLIF(slug, ''), '')
		FROM verticals
		WHERE id = $1::uuid
	`, verticalID).Scan(&slug)
	slug = sanitizeWorkspaceSlug(slug)
	if slug == "" {
		slug = sanitizeWorkspaceSlug(verticalID)
	}
	if slug == "" {
		return "", ""
	}
	return envOrDefault("EMPIREAI_VERTICAL_CONTAINER_PREFIX", "empireai-") + slug, slug
}

func findSessionProjectFileInContainer(ctx context.Context, container, sessionID string) (string, error) {
	sessionID = strings.TrimSpace(sessionID)
	container = strings.TrimSpace(container)
	if sessionID == "" || container == "" {
		return "", nil
	}
	out, err := runDockerInspectCommand(ctx, "exec", container, "find", "/home/agent/.claude/projects", "-type", "f", "-name", sessionID+".jsonl")
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if path := strings.TrimSpace(line); path != "" {
			return path, nil
		}
	}
	return "", nil
}

func tailContainerPath(ctx context.Context, container, path string, lines int) (string, error) {
	container = strings.TrimSpace(container)
	path = strings.TrimSpace(path)
	if container == "" || path == "" {
		return "", nil
	}
	if lines <= 0 {
		lines = 20
	}
	return runDockerInspectCommand(ctx, "exec", container, "tail", "-n", strconv.Itoa(lines), path)
}

func runDockerInspectCommand(ctx context.Context, args ...string) (string, error) {
	dockerBin := strings.TrimSpace(envOrDefault("EMPIREAI_DOCKER_BIN", "docker"))
	cmd := exec.CommandContext(ctx, dockerBin, args...)
	raw, err := cmd.CombinedOutput()
	out := strings.TrimSpace(string(raw))
	if err != nil {
		if out == "" {
			return "", err
		}
		return "", err
	}
	return out, nil
}
