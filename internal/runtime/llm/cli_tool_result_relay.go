package llm

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strings"
	"time"

	"swarm/internal/runtime/core/toolidentity"
	workspace "swarm/internal/runtime/workspace"
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
	base := strings.TrimSpace(target.Workdir)
	if base == "" {
		base = "/workspace"
	}
	sessionID := "session"
	if session != nil && strings.TrimSpace(session.ID) != "" {
		sessionID = sanitizeRelayPathComponent(session.ID)
	}
	name := sanitizeRelayPathComponent(toolidentity.CanonicalName(toolName))
	if name == "" {
		name = "tool"
	}
	filename := fmt.Sprintf("%s-%d.json", name, time.Now().UnixNano())
	return path.Join(base, workspaceToolResultRelayDir, sessionID, filename)
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
	if r != nil && r.execWorkspaceFn != nil {
		return r.execWorkspaceFn(ctx, target, stdin, args...)
	}
	if target == nil || !target.Enabled() {
		return nil, nil, 0, fmt.Errorf("%w: claude sessions must run in a container workspace", ErrClaudeWorkspaceRequired)
	}
	if len(args) == 0 {
		return nil, nil, 0, fmt.Errorf("workspace command args are required")
	}
	dockerBin := strings.TrimSpace(os.Getenv("SWARM_DOCKER_BIN"))
	if dockerBin == "" {
		dockerBin = "docker"
	}
	dockerArgs := []string{"exec", "-i"}
	if strings.TrimSpace(target.Workdir) != "" {
		dockerArgs = append(dockerArgs, "-w", target.Workdir)
	}
	dockerArgs = append(dockerArgs, strings.TrimSpace(target.Container))
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
