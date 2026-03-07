package llm

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	runtimeactor "empireai/internal/runtime/actorctx"
	workspace "empireai/internal/runtime/workspace"
)

func (r *ClaudeCLIRuntime) run(ctx context.Context, args []string, target *workspace.Target) (*Response, error) {
	return r.runWithInput(ctx, args, target, "")
}

func (r *ClaudeCLIRuntime) runWithInput(ctx context.Context, args []string, target *workspace.Target, input string) (*Response, error) {
	timeout := r.effectiveCLITimeout(ctx)
	if target != nil && target.Enabled() {
		if strings.TrimSpace(os.Getenv("CLAUDE_CODE_OAUTH_TOKEN")) == "" {
			return nil, fmt.Errorf("%w: CLAUDE_CODE_OAUTH_TOKEN is missing", ErrClaudeAuthRequired)
		}
	}

	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := r.buildCommand(runCtx, args, target)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if strings.TrimSpace(input) != "" {
		cmd.Stdin = strings.NewReader(input)
	}

	if err := cmd.Run(); err != nil {
		stderrText := strings.TrimSpace(stderr.String())
		stdoutText := strings.TrimSpace(stdout.String())
		if errors.Is(runCtx.Err(), context.DeadlineExceeded) {
			errOut := summarizeCLIErrorOutput(stderrText)
			if errOut == "" {
				errOut = summarizeCLIErrorOutput(stdoutText)
			}
			if errOut == "" {
				return nil, fmt.Errorf("claude cli timeout after %s", timeout)
			}
			return nil, fmt.Errorf("claude cli timeout after %s: %s", timeout, errOut)
		}
		if isClaudeAuthOutput(stderrText) || isClaudeAuthOutput(stdoutText) {
			msg := summarizeCLIErrorOutput(stderrText)
			if msg == "" {
				msg = summarizeCLIErrorOutput(stdoutText)
			}
			if msg == "" {
				msg = "not logged in"
			}
			return nil, fmt.Errorf("%w: %s", ErrClaudeAuthRequired, msg)
		}
		errOut := summarizeCLIErrorOutput(stderrText)
		if errOut == "" {
			errOut = summarizeCLIErrorOutput(stdoutText)
		}
		if errOut == "" {
			return nil, fmt.Errorf("claude cli run failed: %w", err)
		}
		return nil, fmt.Errorf("claude cli run failed: %w, stderr=%s", err, errOut)
	}

	raw := bytes.TrimSpace(stdout.Bytes())
	resp := parseCLIResponse(raw)
	resp.Raw = raw
	return resp, nil
}

func (r *ClaudeCLIRuntime) effectiveCLITimeout(ctx context.Context) time.Duration {
	timeout := r.cfg.LLM.ClaudeCLI.Timeout
	if timeout <= 0 {
		timeout = 120 * time.Second
	}
	// Factory agents routinely run deeper research loops; apply a floor to
	// prevent repeated hard-kill timeouts at the process layer.
	if actor, ok := runtimeactor.ActorFromContext(ctx); ok {
		if strings.EqualFold(strings.TrimSpace(actor.Mode), "factory") && timeout < 300*time.Second {
			timeout = 300 * time.Second
		}
	}
	// Explicit env override wins (operator emergency control).
	if raw := strings.TrimSpace(os.Getenv("EMPIREAI_CLAUDE_TIMEOUT_SECONDS")); raw != "" {
		if seconds, err := strconv.Atoi(raw); err == nil && seconds > 0 {
			timeout = time.Duration(seconds) * time.Second
		}
	}
	return timeout
}

func isClaudeAuthOutput(raw string) bool {
	msg := strings.ToLower(strings.TrimSpace(raw))
	if msg == "" {
		return false
	}
	return strings.Contains(msg, "not logged in") ||
		strings.Contains(msg, "please run /login") ||
		strings.Contains(msg, "authentication required") ||
		strings.Contains(msg, "oauth token")
}

func summarizeCLIErrorOutput(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	resp := parseCLIResponse([]byte(raw))
	msg := strings.TrimSpace(resp.Message.Content)
	if msg == "" {
		msg = raw
	}
	msg = strings.Join(strings.Fields(msg), " ")
	const maxLen = 240
	if len(msg) > maxLen {
		msg = msg[:maxLen] + "..."
	}
	return msg
}

func (r *ClaudeCLIRuntime) buildCommand(ctx context.Context, args []string, target *workspace.Target) *exec.Cmd {
	if target != nil && target.Enabled() {
		dockerBin := strings.TrimSpace(os.Getenv("EMPIREAI_DOCKER_BIN"))
		if dockerBin == "" {
			dockerBin = "docker"
		}
		dockerArgs := []string{"exec", "-i"}
		gatewayURL := strings.TrimSpace(os.Getenv("EMPIREAI_TOOL_GATEWAY_URL"))
		if gatewayURL == "" {
			gatewayURL = "http://orchestrator:8090"
		}
		if gatewayURL != "" {
			dockerArgs = append(dockerArgs, "-e", "EMPIREAI_TOOL_GATEWAY_URL="+gatewayURL)
		}
		if gatewayToken := strings.TrimSpace(os.Getenv("EMPIREAI_TOOL_GATEWAY_TOKEN")); gatewayToken != "" {
			dockerArgs = append(dockerArgs, "-e", "EMPIREAI_TOOL_GATEWAY_TOKEN="+gatewayToken)
		}
		if oauthToken := strings.TrimSpace(os.Getenv("CLAUDE_CODE_OAUTH_TOKEN")); oauthToken != "" {
			dockerArgs = append(dockerArgs, "-e", "CLAUDE_CODE_OAUTH_TOKEN="+oauthToken)
		}
		if strings.TrimSpace(target.Workdir) != "" {
			dockerArgs = append(dockerArgs, "-w", target.Workdir)
		}
		dockerArgs = append(dockerArgs, target.Container, r.cfg.LLM.ClaudeCLI.Command)
		dockerArgs = append(dockerArgs, args...)
		return exec.CommandContext(ctx, dockerBin, dockerArgs...)
	}
	return exec.CommandContext(ctx, r.cfg.LLM.ClaudeCLI.Command, args...)
}

func (r *ClaudeCLIRuntime) resolveWorkspace(ctx context.Context) (*workspace.Target, error) {
	if r.workspaces == nil {
		return nil, nil
	}
	actor, ok := runtimeactor.ActorFromContext(ctx)
	if !ok {
		return nil, nil
	}
	return r.workspaces.ResolveWorkspace(ctx, actor)
}
