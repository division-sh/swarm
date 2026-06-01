package llm

import (
	"fmt"
	"os"
	"strings"

	"github.com/division-sh/swarm/internal/config"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
)

func workspaceCLIDiagnosticError(cfg *config.Config, target *workspace.Target, raw string) error {
	summary := summarizeCLIErrorOutput(raw)
	if summary == "" {
		summary = strings.TrimSpace(raw)
	}
	command := configuredClaudeCLICommand(cfg)
	if !looksLikeMissingWorkspaceCLI(command, summary) {
		return nil
	}

	container := "unknown"
	if target != nil && strings.TrimSpace(target.Container) != "" {
		container = strings.TrimSpace(target.Container)
	}
	image := strings.TrimSpace(os.Getenv("SWARM_WORKSPACE_IMAGE"))
	if image == "" {
		image = workspace.ConfiguredWorkspaceImageFromEnv()
	}
	return fmt.Errorf("%w: local cli_test workspace cannot execute configured Claude CLI command %q in container %q (workspace image %q or an existing stale/incompatible workspace container is missing the CLI); build or pull a workspace image that includes the Claude CLI, remove stale workspace containers, or set SWARM_WORKSPACE_IMAGE to a compatible image: %s", ErrClaudeWorkspaceCLIUnavailable, command, container, image, summary)
}

func configuredClaudeCLICommand(cfg *config.Config) string {
	if cfg != nil {
		if command := strings.TrimSpace(cfg.LLM.ClaudeCLI.Command); command != "" {
			return command
		}
	}
	return "claude"
}

func looksLikeMissingWorkspaceCLI(command, raw string) bool {
	command = strings.ToLower(strings.TrimSpace(command))
	if command == "" {
		command = "claude"
	}
	msg := strings.ToLower(strings.TrimSpace(raw))
	if msg == "" {
		return false
	}

	if strings.Contains(msg, "executable file not found") && messageMentionsCLICommand(msg, command) {
		return true
	}
	if strings.Contains(msg, command+": command not found") || strings.Contains(msg, command+": not found") {
		return true
	}
	if strings.Contains(msg, "exit status 127") && strings.Contains(msg, command) {
		return true
	}
	if strings.Contains(msg, "no such file or directory") && messageMentionsCLICommand(msg, command) {
		return true
	}
	return false
}

func messageMentionsCLICommand(msg, command string) bool {
	command = strings.TrimSpace(command)
	if command == "" {
		return false
	}
	if strings.Contains(msg, command) || strings.Contains(msg, `"`+command+`"`) {
		return true
	}
	if idx := strings.LastIndex(command, "/"); idx >= 0 && idx+1 < len(command) {
		base := strings.TrimSpace(command[idx+1:])
		return base != "" && (strings.Contains(msg, base) || strings.Contains(msg, `"`+base+`"`))
	}
	return false
}
