package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"time"

	models "swarm/internal/runtime/core/actors"
	runtimemcp "swarm/internal/runtime/mcp"
	workspace "swarm/internal/runtime/workspace"
)

const workspaceToolResultRelayDir = ".swarm/tool-results"
const toolResultRelayChunkBytes = 8 * 1024

type readFileRelayPayload struct {
	Content   string `json:"content"`
	SizeBytes int    `json:"size_bytes"`
}

func (e *Executor) PersistOversizedToolResultRelay(ctx context.Context, toolName string, rawJSON []byte) (runtimemcp.ToolResultRelayRef, error) {
	actor, ok := models.ActorFromContext(ctx)
	if !ok || strings.TrimSpace(actor.ID) == "" {
		return runtimemcp.ToolResultRelayRef{}, fmt.Errorf("actor context required for tool result relay")
	}
	target, err := e.resolveNativeWorkspace(ctx, actor)
	if err != nil {
		return runtimemcp.ToolResultRelayRef{}, err
	}
	if relay, ok, err := e.persistChunkedReadFileRelay(ctx, target, actor, toolName, rawJSON); ok {
		return relay, err
	}
	execTarget := target.ExecutionTarget()
	if err := execTarget.Require(workspace.ExecutionCapabilityToolResultRelay); err != nil {
		return runtimemcp.ToolResultRelayRef{}, err
	}
	relayPath, err := toolResultRelayPath(execTarget, actor, toolName)
	if err != nil {
		return runtimemcp.ToolResultRelayRef{}, err
	}
	_, stderr, exitCode, execErr := e.runWorkspaceCommand(ctx, target, 30*time.Second, string(rawJSON), "sh", "-lc", `mkdir -p -- "$(dirname -- "$1")" && cat > "$1"`, "swarm-tool-result-relay", relayPath)
	if execErr != nil || exitCode != 0 {
		msg := strings.TrimSpace(string(stderr))
		if msg == "" && execErr != nil {
			msg = execErr.Error()
		}
		if msg == "" {
			msg = "workspace relay write failed"
		}
		return runtimemcp.ToolResultRelayRef{}, fmt.Errorf("write tool result relay %s: %s", relayPath, msg)
	}
	return runtimemcp.ToolResultRelayRef{
		Path:       relayPath,
		ReadTool:   "read_file",
		Format:     "json",
		Visibility: "workspace_mount",
	}, nil
}

func (e *Executor) persistChunkedReadFileRelay(ctx context.Context, target *workspace.Target, actor models.AgentConfig, toolName string, rawJSON []byte) (runtimemcp.ToolResultRelayRef, bool, error) {
	if strings.TrimSpace(toolName) != "read_file" {
		return runtimemcp.ToolResultRelayRef{}, false, nil
	}
	execTarget := target.ExecutionTarget()
	if err := execTarget.Require(workspace.ExecutionCapabilityToolResultRelay); err != nil {
		return runtimemcp.ToolResultRelayRef{}, true, err
	}
	var payload readFileRelayPayload
	if err := json.Unmarshal(rawJSON, &payload); err != nil {
		return runtimemcp.ToolResultRelayRef{}, false, nil
	}
	if payload.Content == "" {
		return runtimemcp.ToolResultRelayRef{}, false, nil
	}
	chunks := splitRelayChunks(payload.Content, toolResultRelayChunkBytes)
	if len(chunks) <= 1 {
		return runtimemcp.ToolResultRelayRef{}, false, nil
	}
	relayPaths := make([]string, 0, len(chunks))
	for idx, chunk := range chunks {
		relayPath, err := toolResultRelayChunkPath(execTarget, actor, toolName, idx)
		if err != nil {
			return runtimemcp.ToolResultRelayRef{}, true, err
		}
		_, stderr, exitCode, execErr := e.runWorkspaceCommand(ctx, target, 30*time.Second, chunk, "sh", "-lc", `mkdir -p -- "$(dirname -- "$1")" && cat > "$1"`, "swarm-tool-result-relay", relayPath)
		if execErr != nil || exitCode != 0 {
			msg := strings.TrimSpace(string(stderr))
			if msg == "" && execErr != nil {
				msg = execErr.Error()
			}
			if msg == "" {
				msg = "workspace relay write failed"
			}
			return runtimemcp.ToolResultRelayRef{}, true, fmt.Errorf("write tool result relay %s: %s", relayPath, msg)
		}
		relayPaths = append(relayPaths, relayPath)
	}
	return runtimemcp.ToolResultRelayRef{
		Chunks:     relayPaths,
		ReadTool:   "read_file",
		Format:     "text",
		Visibility: "workspace_mount",
	}, true, nil
}

func toolResultRelayPath(target workspace.ExecutionTarget, actor models.AgentConfig, toolName string) (string, error) {
	owner := sanitizeToolResultRelayPathComponent(actor.EffectiveEntityID())
	if owner == "" {
		owner = sanitizeToolResultRelayPathComponent(actor.ID)
	}
	if owner == "" {
		owner = "agent"
	}
	name := sanitizeToolResultRelayPathComponent(toolName)
	if name == "" {
		name = "tool"
	}
	filename := fmt.Sprintf("%s-%d.json", name, time.Now().UnixNano())
	return target.WorkspacePath(path.Join(workspaceToolResultRelayDir, owner, filename))
}

func toolResultRelayChunkPath(target workspace.ExecutionTarget, actor models.AgentConfig, toolName string, idx int) (string, error) {
	owner := sanitizeToolResultRelayPathComponent(actor.EffectiveEntityID())
	if owner == "" {
		owner = sanitizeToolResultRelayPathComponent(actor.ID)
	}
	if owner == "" {
		owner = "agent"
	}
	name := sanitizeToolResultRelayPathComponent(toolName)
	if name == "" {
		name = "tool"
	}
	filename := fmt.Sprintf("%s-chunk-%03d-%d.txt", name, idx+1, time.Now().UnixNano())
	return target.WorkspacePath(path.Join(workspaceToolResultRelayDir, owner, filename))
}

func sanitizeToolResultRelayPathComponent(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	var b strings.Builder
	for _, r := range raw {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r + ('a' - 'A'))
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	return strings.Trim(b.String(), "-")
}

func splitRelayChunks(content string, maxBytes int) []string {
	if content == "" {
		return nil
	}
	if maxBytes <= 0 {
		return []string{content}
	}
	chunks := make([]string, 0, len(content)/maxBytes+1)
	for len(content) > 0 {
		if len(content) <= maxBytes {
			chunks = append(chunks, content)
			break
		}
		chunks = append(chunks, content[:maxBytes])
		content = content[maxBytes:]
	}
	return chunks
}
