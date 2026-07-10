package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimemcp "github.com/division-sh/swarm/internal/runtime/mcp"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
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
		return runtimemcp.ToolResultRelayRef{}, runtimefailures.New(runtimefailures.ClassInternalFailure, "tool_result_relay_actor_missing", "tool-executor", "persist_tool_result_relay", nil)
	}
	target, err := e.resolveNativeWorkspace(ctx, actor)
	if err != nil {
		return runtimemcp.ToolResultRelayRef{}, toolResultRelayFailure(err, "tool_result_relay_workspace_unavailable", "resolve_workspace", nil)
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
	if err := e.writeToolResultRelayFile(ctx, target, execTarget, relayPath, rawJSON); err != nil {
		return runtimemcp.ToolResultRelayRef{}, err
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
		if err := e.writeToolResultRelayFile(ctx, target, execTarget, relayPath, []byte(chunk)); err != nil {
			return runtimemcp.ToolResultRelayRef{}, true, err
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

func (e *Executor) writeToolResultRelayFile(ctx context.Context, target *workspace.Target, execTarget workspace.ExecutionTarget, relayPath string, payload []byte) error {
	switch execTarget.Mode {
	case workspace.ExecutionModeHostLocal:
		resolved, err := execTarget.ResolveHostPath(relayPath, workspace.PathAccessWrite)
		if err != nil {
			return toolResultRelayFailure(err, "tool_result_relay_path_unavailable", "resolve_path", map[string]any{"relay_path": relayPath})
		}
		if err := os.MkdirAll(filepath.Dir(resolved.HostPath), 0o700); err != nil {
			return toolResultRelayFailure(err, "tool_result_relay_write_failed", "create_parent", map[string]any{"relay_path": resolved.LogicalPath})
		}
		if err := os.WriteFile(resolved.HostPath, payload, 0o644); err != nil {
			return toolResultRelayFailure(err, "tool_result_relay_write_failed", "write_file", map[string]any{"relay_path": resolved.LogicalPath})
		}
		return nil
	default:
		_, _, exitCode, execErr := e.runWorkspaceCommand(ctx, target, 30*time.Second, string(payload), "sh", "-lc", `mkdir -p -- "$(dirname -- "$1")" && cat > "$1"`, "swarm-tool-result-relay", relayPath)
		if execErr != nil || exitCode != 0 {
			cause := execErr
			if cause == nil {
				cause = fmt.Errorf("workspace command exited with code %d", exitCode)
			}
			return toolResultRelayFailure(cause, "tool_result_relay_write_failed", "write_workspace_file", map[string]any{"exit_code": exitCode, "relay_path": relayPath})
		}
		return nil
	}
}

func toolResultRelayFailure(err error, code, operation string, attributes map[string]any) error {
	if _, ok := runtimefailures.As(err); ok {
		return err
	}
	return runtimefailures.Wrap(runtimefailures.ClassDependencyUnavailable, code, "tool-executor", operation, attributes, err)
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
