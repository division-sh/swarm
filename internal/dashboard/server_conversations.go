package dashboard

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

func (s *Server) handleConversations(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}
	ctx := r.Context()
	limit := clamp(parseInt(r.URL.Query().Get("limit"), 50), 1, 200)

	rows, err := s.db.QueryContext(ctx, `
		SELECT
			c.agent_id,
			COALESCE(a.role, ''),
			COALESCE(a.vertical_id::text, ''),
			COALESCE(v.slug, ''),
			COALESCE(c.mode, 'task'),
			COALESCE(c.turn_count, 0),
			COALESCE(c.summary, ''),
			COALESCE(c.updated_at, c.created_at)
		FROM conversations c
		LEFT JOIN agents a ON a.id = c.agent_id
		LEFT JOIN verticals v ON v.id = a.vertical_id
		WHERE c.status = 'active'
		ORDER BY c.updated_at DESC
		LIMIT $1
	`, limit)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	defer rows.Close()

	items := make([]map[string]any, 0, limit)
	for rows.Next() {
		var agentID, role, verticalID, verticalSlug, mode, summary string
		var turnCount int
		var updatedAt time.Time
		if err := rows.Scan(&agentID, &role, &verticalID, &verticalSlug, &mode, &turnCount, &summary, &updatedAt); err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		items = append(items, map[string]any{
			"agent_id":      agentID,
			"role":          role,
			"vertical_id":   verticalID,
			"vertical_slug": verticalSlug,
			"mode":          mode,
			"turn_count":    turnCount,
			"summary":       summary,
			"updated_at":    updatedAt,
		})
	}
	if err := rows.Err(); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"conversations": items, "generated_at": s.now().UTC()})
}

func (s *Server) handleConversationDetail(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}
	agentID, subresource, ok := parseConversationPath(r.URL.Path)
	if !ok || agentID == "" {
		http.NotFound(w, r)
		return
	}
	if subresource == "artifacts" {
		s.handleConversationArtifacts(w, r, agentID)
		return
	}
	if subresource != "" {
		http.NotFound(w, r)
		return
	}
	ctx := r.Context()

	var mode, taskID, summary, status string
	var turnCount int
	var messagesRaw []byte
	var createdAt, updatedAt time.Time
	err := s.db.QueryRowContext(ctx, `
		SELECT COALESCE(mode, 'task'), COALESCE(task_id::text, ''), COALESCE(summary, ''), COALESCE(status, ''),
			COALESCE(turn_count, 0), COALESCE(messages, '[]'::jsonb), COALESCE(created_at, now()), COALESCE(updated_at, now())
		FROM conversations
		WHERE agent_id = $1
		ORDER BY updated_at DESC
		LIMIT 1
	`, agentID).Scan(&mode, &taskID, &summary, &status, &turnCount, &messagesRaw, &createdAt, &updatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.NotFound(w, r)
			return
		}
		writeErr(w, http.StatusInternalServerError, err)
		return
	}

	var messages any
	_ = json.Unmarshal(messagesRaw, &messages)

	turnRows, err := s.db.QueryContext(ctx, `
		SELECT
			turn_index,
			created_at,
			COALESCE(latency_ms, 0),
			COALESCE(retry_count, 0),
			parse_ok,
			COALESCE(error, ''),
			COALESCE(request_payload, '{}'::jsonb),
			COALESCE(response_payload, '{}'::jsonb)
		FROM agent_turns
		WHERE agent_id = $1
		ORDER BY created_at DESC
		LIMIT 80
	`, agentID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	defer turnRows.Close()

	turns := make([]map[string]any, 0, 80)
	for turnRows.Next() {
		var idx, latency, retries int
		var created time.Time
		var parseOK bool
		var errText string
		var reqRaw, respRaw []byte
		if err := turnRows.Scan(&idx, &created, &latency, &retries, &parseOK, &errText, &reqRaw, &respRaw); err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		assistantText, toolCalls := extractTurnArtifacts(respRaw)
		toolResult := extractToolResult(reqRaw)
		turns = append(turns, map[string]any{
			"turn_index":       idx,
			"created_at":       created,
			"latency_ms":       latency,
			"retry_count":      retries,
			"parse_ok":         parseOK,
			"error":            errText,
			"assistant_text":   assistantText,
			"tool_calls":       toolCalls,
			"tool_result":      truncate(toolResult, 400),
			"response_payload": json.RawMessage(respRaw),
		})
	}
	if err := turnRows.Err(); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"agent_id":   agentID,
		"mode":       mode,
		"task_id":    taskID,
		"summary":    summary,
		"status":     status,
		"turn_count": turnCount,
		"created_at": createdAt,
		"updated_at": updatedAt,
		"messages":   messages,
		"turns":      turns,
	})
}

func parseConversationPath(path string) (agentID, subresource string, ok bool) {
	prefix := "/dashboard/api/conversations/"
	if strings.HasPrefix(path, "/api/conversations/") {
		prefix = "/api/conversations/"
	}
	trimmed := strings.Trim(strings.TrimPrefix(path, prefix), "/")
	if trimmed == "" {
		return "", "", false
	}
	parts := strings.Split(trimmed, "/")
	agentID = strings.TrimSpace(parts[0])
	if agentID == "" {
		return "", "", false
	}
	if len(parts) > 1 {
		subresource = strings.TrimSpace(parts[1])
	}
	return agentID, subresource, true
}

type sessionArtifactFile struct {
	Path  string `json:"path,omitempty"`
	Found bool   `json:"found"`
	Tail  string `json:"tail,omitempty"`
	Error string `json:"error,omitempty"`
}

func (s *Server) handleConversationArtifacts(w http.ResponseWriter, r *http.Request, agentID string) {
	ctx := r.Context()
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		http.NotFound(w, r)
		return
	}

	lines := clamp(parseInt(r.URL.Query().Get("lines"), 80), 10, 300)
	sessionID, runtimeMode, provider, sessionStatus, err := s.lookupLatestAgentSession(ctx, agentID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.NotFound(w, r)
			return
		}
		writeErr(w, http.StatusInternalServerError, err)
		return
	}

	role, verticalID, err := s.lookupAgentRoleAndVertical(ctx, agentID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}

	container, slug, err := s.resolveWorkspaceContainer(ctx, role, verticalID)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if container == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("no workspace container mapping for agent role=%s", role))
		return
	}

	out := map[string]any{
		"agent_id":    agentID,
		"role":        role,
		"vertical_id": verticalID,
		"vertical_slug": func() string {
			return slug
		}(),
		"session": map[string]any{
			"session_id":   sessionID,
			"runtime_mode": runtimeMode,
			"provider":     provider,
			"status":       sessionStatus,
		},
		"workspace_container": container,
		"generated_at":        s.now().UTC(),
	}

	if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(runtimeMode)), "cli") {
		out["artifacts"] = map[string]any{}
		out["note"] = "session artifacts are only available for cli runtime modes"
		writeJSON(w, http.StatusOK, out)
		return
	}

	projectPath, err := s.findSessionProjectFile(ctx, container, sessionID)
	projectArtifact := sessionArtifactFile{Path: projectPath}
	if err != nil {
		projectArtifact.Error = err.Error()
	} else if strings.TrimSpace(projectPath) != "" {
		projectArtifact.Found = true
		tail, tailErr := s.tailContainerFile(ctx, container, projectPath, lines)
		if tailErr != nil {
			projectArtifact.Error = tailErr.Error()
		} else {
			projectArtifact.Tail = tail
		}
	}

	debugPath := "/home/agent/.claude/debug/" + strings.TrimSpace(sessionID) + ".txt"
	debugArtifact := sessionArtifactFile{Path: debugPath}
	if tail, tailErr := s.tailContainerFile(ctx, container, debugPath, lines); tailErr != nil {
		debugArtifact.Error = tailErr.Error()
	} else {
		debugArtifact.Found = true
		debugArtifact.Tail = tail
	}

	out["artifacts"] = map[string]any{
		"project_jsonl": projectArtifact,
		"debug_log":     debugArtifact,
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) lookupLatestAgentSession(ctx context.Context, agentID string) (sessionID, runtimeMode, provider, status string, err error) {
	err = s.db.QueryRowContext(ctx, `
		SELECT
			COALESCE(session_id, ''),
			COALESCE(runtime_mode, ''),
			COALESCE(provider, ''),
			COALESCE(status, '')
		FROM agent_sessions
		WHERE agent_id = $1
		ORDER BY (status = 'active') DESC, last_used_at DESC NULLS LAST, created_at DESC
		LIMIT 1
	`, strings.TrimSpace(agentID)).Scan(&sessionID, &runtimeMode, &provider, &status)
	if err != nil {
		return "", "", "", "", err
	}
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return "", "", "", "", sql.ErrNoRows
	}
	return sessionID, strings.TrimSpace(runtimeMode), strings.TrimSpace(provider), strings.TrimSpace(status), nil
}

func (s *Server) lookupAgentRoleAndVertical(ctx context.Context, agentID string) (role, verticalID string, err error) {
	err = s.db.QueryRowContext(ctx, `
		SELECT COALESCE(role, ''), COALESCE(vertical_id::text, '')
		FROM agents
		WHERE id = $1
	`, strings.TrimSpace(agentID)).Scan(&role, &verticalID)
	if err != nil {
		return "", "", err
	}
	return strings.TrimSpace(role), strings.TrimSpace(verticalID), nil
}

func (s *Server) resolveWorkspaceContainer(ctx context.Context, role, verticalID string) (container, verticalSlug string, err error) {
	role = strings.ToLower(strings.TrimSpace(role))
	switch role {
	case "holding-devops":
		return envOr("EMPIREAI_INFRA_CONTAINER", "empireai-infra"), "", nil
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
		return envOr("EMPIREAI_FACTORY_CONTAINER", "empireai-factory"), "", nil
	}

	verticalID = strings.TrimSpace(verticalID)
	if verticalID == "" {
		return "", "", nil
	}
	slug := ""
	_ = s.db.QueryRowContext(ctx, `
		SELECT COALESCE(NULLIF(slug, ''), '')
		FROM verticals
		WHERE id = $1::uuid
	`, verticalID).Scan(&slug)
	slug = sanitizeContainerSlug(slug)
	if slug == "" {
		slug = sanitizeContainerSlug(verticalID)
	}
	if slug == "" {
		return "", "", fmt.Errorf("vertical slug unavailable for %s", verticalID)
	}
	return envOr("EMPIREAI_VERTICAL_CONTAINER_PREFIX", "empireai-") + slug, slug, nil
}

func (s *Server) findSessionProjectFile(ctx context.Context, container, sessionID string) (string, error) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return "", fmt.Errorf("session_id is required")
	}
	if !isSafeSessionID(sessionID) {
		return "", fmt.Errorf("invalid session_id format")
	}
	out, err := runDocker(ctx, "exec", container, "find", "/home/agent/.claude/projects", "-type", "f", "-name", sessionID+".jsonl")
	if err != nil {
		return "", err
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	for _, line := range lines {
		path := strings.TrimSpace(line)
		if path != "" {
			return path, nil
		}
	}
	return "", nil
}

func (s *Server) tailContainerFile(ctx context.Context, container, path string, lines int) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", fmt.Errorf("path is required")
	}
	if lines <= 0 {
		lines = 80
	}
	out, err := runDocker(ctx, "exec", container, "tail", "-n", strconv.Itoa(lines), path)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(out), nil
}

func runDocker(ctx context.Context, args ...string) (string, error) {
	dockerBin := strings.TrimSpace(os.Getenv("EMPIREAI_DOCKER_BIN"))
	if dockerBin == "" {
		dockerBin = "docker"
	}
	cmd := exec.CommandContext(ctx, dockerBin, args...)
	raw, err := cmd.CombinedOutput()
	out := strings.TrimSpace(string(raw))
	if err != nil {
		if out == "" {
			return "", err
		}
		return "", fmt.Errorf("%w: %s", err, out)
	}
	return out, nil
}

func isSafeSessionID(v string) bool {
	v = strings.TrimSpace(v)
	if v == "" {
		return false
	}
	for _, r := range v {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			continue
		}
		return false
	}
	return true
}

func sanitizeContainerSlug(raw string) string {
	raw = strings.ToLower(strings.TrimSpace(raw))
	if raw == "" {
		return ""
	}
	var b strings.Builder
	lastDash := false
	for _, r := range raw {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
			lastDash = false
		case r >= '0' && r <= '9':
			b.WriteRune(r)
			lastDash = false
		case r == '-' || r == '_' || r == ' ' || r == '/':
			if !lastDash {
				b.WriteRune('-')
				lastDash = true
			}
		}
	}
	return strings.Trim(b.String(), "-")
}

func envOr(key, fallback string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	return v
}

func extractTurnArtifacts(respRaw []byte) (string, []map[string]any) {
	if len(respRaw) == 0 {
		return "", nil
	}
	var obj map[string]any
	if err := json.Unmarshal(respRaw, &obj); err != nil {
		return "", nil
	}
	textParts := make([]string, 0, 4)
	toolCalls := make([]map[string]any, 0, 4)

	if v, ok := obj["result"].(string); ok && strings.TrimSpace(v) != "" {
		textParts = append(textParts, v)
	}
	if v, ok := obj["content"].(string); ok && strings.TrimSpace(v) != "" {
		textParts = append(textParts, v)
	}
	if arr, ok := obj["content"].([]any); ok {
		for _, item := range arr {
			m, ok := item.(map[string]any)
			if !ok {
				continue
			}
			typeName := strings.TrimSpace(asString(m["type"]))
			if typeName == "text" {
				if txt := strings.TrimSpace(asString(m["text"])); txt != "" {
					textParts = append(textParts, txt)
				}
			}
			if typeName == "tool_use" {
				toolCalls = append(toolCalls, map[string]any{
					"name":      asString(m["name"]),
					"arguments": m["input"],
				})
			}
		}
	}
	if arr, ok := obj["tool_calls"].([]any); ok {
		for _, item := range arr {
			m, ok := item.(map[string]any)
			if !ok {
				continue
			}
			toolCalls = append(toolCalls, map[string]any{
				"name":      asString(m["name"]),
				"arguments": m["arguments"],
			})
		}
	}
	return strings.TrimSpace(strings.Join(textParts, "\n")), toolCalls
}

func extractToolResult(reqRaw []byte) string {
	if len(reqRaw) == 0 {
		return ""
	}
	var obj map[string]any
	if err := json.Unmarshal(reqRaw, &obj); err != nil {
		return ""
	}
	message, ok := obj["message"].(map[string]any)
	if !ok {
		return ""
	}
	role := strings.TrimSpace(strings.ToLower(asString(message["role"])))
	if role != "tool" {
		return ""
	}
	return asString(message["content"])
}
