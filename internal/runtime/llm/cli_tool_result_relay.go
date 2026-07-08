package llm

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"path"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/toolidentity"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
)

const workspaceToolResultRelayDir = ".swarm/tool-results"

func (r *ClaudeCLIRuntime) PersistOversizedToolResultRelay(ctx context.Context, session *Session, toolName string, rawJSON []byte) (toolResultRelayRef, error) {
	target, err := r.resolveWorkspace(ctx)
	if err != nil {
		return toolResultRelayRef{}, err
	}
	relayPath := relayWorkspacePath(target, session, toolName)
	if err := r.writeWorkspaceRelayFile(ctx, target, relayPath, rawJSON); err != nil {
		return toolResultRelayRef{}, err
	}
	return toolResultRelayRef{
		Path:       relayPath,
		ReadTool:   "read_file",
		Format:     "json",
		Visibility: "workspace_mount",
	}, nil
}

func relayWorkspacePath(target *workspace.Target, session *Session, toolName string) string {
	sessionID := "session"
	if session != nil && strings.TrimSpace(session.ID) != "" {
		sessionID = sanitizeRelayPathComponent(session.ID)
	}
	name := sanitizeRelayPathComponent(toolidentity.CanonicalName(toolName))
	if name == "" {
		name = "tool"
	}
	filename := fmt.Sprintf("%s-%d.json", name, time.Now().UnixNano())
	relayPath, err := target.ExecutionTarget().WorkspacePath(path.Join(workspaceToolResultRelayDir, sessionID, filename))
	if err != nil {
		return path.Join(workspace.LogicalWorkspaceMount, workspaceToolResultRelayDir, sessionID, filename)
	}
	return relayPath
}

func sanitizeRelayPathComponent(raw string) string {
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

func (r *ClaudeCLIRuntime) writeWorkspaceRelayFile(ctx context.Context, target *workspace.Target, relayPath string, rawJSON []byte) error {
	_, stderr, exitCode, err := r.runWorkspaceCommand(ctx, target, string(rawJSON), "sh", "-lc", `mkdir -p -- "$(dirname -- "$1")" && cat > "$1"`, "swarm-tool-result-relay", relayPath)
	if err != nil || exitCode != 0 {
		msg := strings.TrimSpace(string(stderr))
		if msg == "" && err != nil {
			msg = err.Error()
		}
		if msg == "" {
			msg = "workspace relay write failed"
		}
		return fmt.Errorf("write tool result relay %s: %s", relayPath, msg)
	}
	return nil
}

func (r *ClaudeCLIRuntime) runWorkspaceCommand(ctx context.Context, target *workspace.Target, stdin string, args ...string) ([]byte, []byte, int, error) {
	execTarget := target.ExecutionTarget()
	if err := execTarget.Require(workspace.ExecutionCapabilityClaudeCLI); err != nil {
		if strings.EqualFold(strings.TrimSpace(execTarget.Backend), workspace.BackendHost) {
			return nil, nil, 0, errClaudeHostWorkspaceUnsupported()
		}
		return nil, nil, 0, fmt.Errorf("%w: %s", ErrClaudeWorkspaceRequired, err.Error())
	}
	if err := execTarget.Require(workspace.ExecutionCapabilityToolResultRelay); err != nil {
		if strings.EqualFold(strings.TrimSpace(execTarget.Backend), workspace.BackendHost) {
			return nil, nil, 0, errClaudeHostWorkspaceUnsupported()
		}
		return nil, nil, 0, fmt.Errorf("%w: %s", ErrClaudeWorkspaceRequired, err.Error())
	}
	if len(args) == 0 {
		return nil, nil, 0, fmt.Errorf("workspace command args are required")
	}
	if r != nil && r.execWorkspaceFn != nil {
		return r.execWorkspaceFn(ctx, target, stdin, args...)
	}
	dockerBin := configuredWorkspaceDockerBin(r.cfg)
	dockerArgs := []string{"exec", "-i"}
	if strings.TrimSpace(execTarget.Workdir) != "" {
		dockerArgs = append(dockerArgs, "-w", execTarget.Workdir)
	}
	dockerArgs = append(dockerArgs, strings.TrimSpace(execTarget.Container))
	dockerArgs = append(dockerArgs, args...)
	cmd := exec.CommandContext(ctx, dockerBin, dockerArgs...)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if strings.TrimSpace(stdin) != "" {
		cmd.Stdin = strings.NewReader(stdin)
	}
	err := cmd.Run()
	exitCode := 0
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		}
		if exitCode == 0 {
			exitCode = -1
		}
	}
	return stdout.Bytes(), stderr.Bytes(), exitCode, err
}
