package mcp

import (
	"context"
	"database/sql"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	runtimeproductpolicy "empireai/internal/runtime/productpolicy"
	workspace "empireai/internal/runtime/workspace"
)

type StallDiagnosticConfig struct {
	Interval      time.Duration
	PendingAge    time.Duration
	Cooldown      time.Duration
	MinPending    int
	ArtifactLines int
}

type StallDiagnosticLogger func(ctx context.Context, level, component, action, agentID, verticalID string, detail map[string]any, errText string)

type stallCandidate struct {
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
	LockOwner     string
	LockExpiresAt sql.NullTime
	LastUsedAt    sql.NullTime
}

var stallDiagSeen sync.Map

func DefaultStallDiagnosticConfig() StallDiagnosticConfig {
	return StallDiagnosticConfig{
		Interval:      45 * time.Second,
		PendingAge:    5 * time.Minute,
		Cooldown:      7 * time.Minute,
		MinPending:    3,
		ArtifactLines: 20,
	}
}

func StartStallDiagnosticLoop(ctx context.Context, db *sql.DB, logger StallDiagnosticLogger, cfg StallDiagnosticConfig) {
	if ctx == nil || db == nil || logger == nil {
		return
	}
	cfg = normalizeDiagnosticConfig(cfg)
	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			RunStallDiagnosticsPass(ctx, db, logger, cfg)
		}
	}
}

func RunStallDiagnosticsPass(ctx context.Context, db *sql.DB, logger StallDiagnosticLogger, cfg StallDiagnosticConfig) {
	candidates := queryStallCandidates(ctx, db, normalizeDiagnosticConfig(cfg))
	if len(candidates) == 0 {
		return
	}
	now := time.Now().UTC()
	for _, c := range candidates {
		if strings.TrimSpace(c.LockOwner) != "" && c.LockExpiresAt.Valid && c.LockExpiresAt.Time.After(now.Add(30*time.Second)) {
			continue
		}
		if c.LastUsedAt.Valid && now.Sub(c.LastUsedAt.Time) < cfg.PendingAge {
			continue
		}
		key := strings.TrimSpace(c.AgentID) + "|" + strings.TrimSpace(c.SessionID)
		if key == "|" {
			continue
		}
		if lastAny, ok := stallDiagSeen.Load(key); ok {
			if last, ok := lastAny.(time.Time); ok && now.Sub(last) < cfg.Cooldown {
				continue
			}
		}
		stallDiagSeen.Store(key, now)
		emitStallDiagnostic(ctx, db, logger, c, cfg)
	}
}

func ClassifyStallCause(code string) string {
	code = strings.TrimSpace(strings.ToLower(code))
	switch code {
	case ErrCodeContextMissing, ErrCodeContextNotFound, ErrCodeContextStale:
		return "mcp_context_lifecycle"
	case ErrCodeAuthMissingBearer, ErrCodeAuthInvalidBearer:
		return "mcp_gateway_auth"
	case ErrCodeToolNotAllowed:
		return "mcp_tool_allowlist"
	case ErrCodeToolExecFailed:
		return "tool_execution_failure"
	default:
		return "unknown"
	}
}

func ResetStallDiagnosticsForTest() {
	stallDiagSeen = sync.Map{}
}

func normalizeDiagnosticConfig(cfg StallDiagnosticConfig) StallDiagnosticConfig {
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
	return cfg
}

func queryStallCandidates(ctx context.Context, db *sql.DB, cfg StallDiagnosticConfig) []stallCandidate {
	rows, err := db.QueryContext(ctx, `
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
			COALESCE(s.status, ''),
			COALESCE(s.lock_owner, ''),
			s.lock_expires_at,
			s.last_used_at
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
			SELECT session_id, runtime_mode, status, lock_owner, lock_expires_at, last_used_at
			FROM agent_sessions
			WHERE agent_id = a.id
			ORDER BY (status = 'active') DESC, last_used_at DESC NULLS LAST, created_at DESC
			LIMIT 1
		) s ON TRUE
		WHERE a.status = 'active'
		  AND COALESCE(s.status, '') = 'active'
		  AND r.event_id IS NULL
		GROUP BY a.id, a.role, a.mode, a.vertical_id, s.session_id, s.runtime_mode, s.status, s.lock_owner, s.lock_expires_at, s.last_used_at
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
	out := make([]stallCandidate, 0, 16)
	for rows.Next() {
		var c stallCandidate
		if scanErr := rows.Scan(&c.AgentID, &c.Role, &c.Mode, &c.VerticalID, &c.PendingCount, &c.OldestPending, &c.LastTurnAt, &c.SessionID, &c.RuntimeMode, &c.SessionStatus, &c.LockOwner, &c.LockExpiresAt, &c.LastUsedAt); scanErr == nil {
			out = append(out, c)
		}
	}
	return out
}

func emitStallDiagnostic(ctx context.Context, db *sql.DB, logger StallDiagnosticLogger, c stallCandidate, cfg StallDiagnosticConfig) {
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
		"session_lock_owner":   strings.TrimSpace(c.LockOwner),
		"session_lock_expires": nullableTimeRFC3339(c.LockExpiresAt),
		"session_last_used":    nullableTimeRFC3339(c.LastUsedAt),
		"recent_error_codes":   recentCodes,
		"suspected_root_cause": ClassifyStallCause(lastCode),
	}
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(c.RuntimeMode)), "cli") && strings.TrimSpace(c.SessionID) != "" {
		container, slug := resolveDiagnosticContainer(ctx, db, c.Role, c.VerticalID)
		artifact := map[string]any{"container": container, "vertical_slug": slug}
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
	err := fmt.Errorf("runtime_error code=%s component=mcp-diagnostics operation=diagnostics.auto_stall_probe retryable=true: agent appears stalled with pending deliveries and no recent turns", ErrCodeStallDetected)
	logger(ctx, "warn", "mcp-diagnostics", "auto_diagnostic_stall", c.AgentID, c.VerticalID, detail, err.Error())
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
	rows, err := db.QueryContext(ctx, `
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
		code := RuntimeErrorCodeFromText(raw)
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
	switch diagnosticWorkspaceClass(role) {
	case "infra":
		return workspace.EnvOrDefault("EMPIREAI_INFRA_CONTAINER", "empireai-infra"), ""
	case "factory":
		return workspace.EnvOrDefault("EMPIREAI_FACTORY_CONTAINER", "empireai-factory"), ""
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
	slug = workspace.SanitizeSlug(slug)
	if slug == "" {
		slug = workspace.SanitizeSlug(verticalID)
	}
	if slug == "" {
		return "", ""
	}
	return workspace.EnvOrDefault("EMPIREAI_VERTICAL_CONTAINER_PREFIX", "empireai-") + slug, slug
}

func diagnosticWorkspaceClass(role string) string {
	if policy := runtimeproductpolicy.DefaultOrNil(); policy != nil {
		return strings.TrimSpace(policy.DiagnosticWorkspaceClass(role))
	}
	return ""
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
	dockerBin := strings.TrimSpace(workspace.EnvOrDefault("EMPIREAI_DOCKER_BIN", "docker"))
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

func snippetForLog(raw string, max int) string {
	raw = strings.TrimSpace(raw)
	if max <= 0 || len(raw) <= max {
		return raw
	}
	return raw[:max]
}
