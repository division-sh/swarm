package llm

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"swarm/internal/config"
	runtimeactors "swarm/internal/runtime/core/actors"
	llmselection "swarm/internal/runtime/llm/selection"
	workspace "swarm/internal/runtime/workspace"
)

func (r *ClaudeCLIRuntime) run(ctx context.Context, args []string, target *workspace.Target) (*Response, error) {
	return r.runWithInput(ctx, args, target, "", MonitorTurnMeta{})
}

func (r *ClaudeCLIRuntime) runWithInput(ctx context.Context, args []string, target *workspace.Target, input string, meta MonitorTurnMeta) (*Response, error) {
	timeout := r.effectiveCLITimeout(ctx)
	if target == nil || !target.Enabled() {
		return nil, fmt.Errorf("%w: claude sessions must run in a container workspace", ErrClaudeWorkspaceRequired)
	}
	profile, _ := llmselection.ResolveActiveBackend(llmselection.BackendClaudeCLI)
	if err := llmselection.RequireCredential(profile, os.LookupEnv); err != nil {
		return nil, fmt.Errorf("%w: %s is missing", ErrClaudeAuthRequired, profile.Credential.EnvVar)
	}

	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := r.buildCommand(runCtx, args, target)
	if configuredCLIOutputFormat(r.cfg) == "stream-json" {
		return r.runStreaming(runCtx, cmd, target, timeout, input, meta)
	}

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
		if diag := workspaceCLIDiagnosticError(r.cfg, target, coalesce(errOut, stderrText, stdoutText)); diag != nil {
			return nil, diag
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

func (r *ClaudeCLIRuntime) runStreaming(ctx context.Context, cmd *exec.Cmd, target *workspace.Target, timeout time.Duration, input string, meta MonitorTurnMeta) (*Response, error) {
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("create claude stdout pipe: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("create claude stderr pipe: %w", err)
	}
	if strings.TrimSpace(input) != "" {
		cmd.Stdin = strings.NewReader(input)
	}

	monitor, monitorErr := r.openMonitorTurn(ctx, meta)
	if monitorErr != nil {
		return nil, monitorErr
	}
	if monitor != nil {
		defer func() { _ = monitor.Close() }()
	}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("claude cli run failed: %w", err)
	}

	stdoutCh := make(chan [][]byte, 1)
	stderrCh := make(chan [][]byte, 1)
	go func() { stdoutCh <- readStreamLines(stdout, monitor, false) }()
	go func() { stderrCh <- readStreamLines(stderr, monitor, true) }()

	waitErr := cmd.Wait()
	stdoutLines := <-stdoutCh
	stderrLines := <-stderrCh
	if waitErr != nil {
		stderrText := strings.TrimSpace(string(joinRawLines(stderrLines)))
		stdoutText := strings.TrimSpace(string(joinRawLines(stdoutLines)))
		if monitor != nil {
			monitor.WriteNotice("turn.end ok=false")
		}
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
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
		if diag := workspaceCLIDiagnosticError(r.cfg, target, coalesce(errOut, stderrText, stdoutText)); diag != nil {
			return nil, diag
		}
		if errOut == "" {
			return nil, fmt.Errorf("claude cli run failed: %w", waitErr)
		}
		return nil, fmt.Errorf("claude cli run failed: %w, stderr=%s", waitErr, errOut)
	}

	acc := newCLIStreamAccumulator()
	for _, line := range stdoutLines {
		acc.AddLine(line)
	}
	resp := acc.Response()
	if monitor != nil {
		monitor.WriteNotice("turn.end ok=true session=%s", strings.TrimSpace(coalesce(resp.SessionID, meta.SessionID)))
	}
	return resp, nil
}

func (r *ClaudeCLIRuntime) openMonitorTurn(ctx context.Context, meta MonitorTurnMeta) (MonitorTurnWriter, error) {
	if r == nil {
		return nil, nil
	}
	var (
		base MonitorTurnWriter
		err  error
	)
	if r.monitor != nil && strings.TrimSpace(meta.AgentID) != "" {
		base, err = r.monitor.OpenTurn(ctx, meta)
		if err != nil {
			return nil, err
		}
	}
	return newSessionWatchdogMonitorWriter(ctx, base, r.conversations, r.events, meta), nil
}

func readStreamLines(rc io.ReadCloser, monitor MonitorTurnWriter, stderr bool) [][]byte {
	defer rc.Close()
	scanner := bufio.NewScanner(rc)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	lines := make([][]byte, 0, 16)
	for scanner.Scan() {
		line := append([]byte(nil), scanner.Bytes()...)
		lines = append(lines, line)
		if monitor != nil {
			if stderr {
				monitor.WriteStderr(line)
			} else {
				monitor.WriteStdout(line)
			}
		}
	}
	return lines
}

func (r *ClaudeCLIRuntime) effectiveCLITimeout(ctx context.Context) time.Duration {
	return effectiveCLITimeoutForConfig(ctx, r.cfg)
}

func effectiveCLITimeoutForConfig(ctx context.Context, cfg *config.Config) time.Duration {
	timeout := time.Duration(0)
	if cfg != nil {
		timeout = cfg.LLM.ClaudeCLI.Timeout
	}
	if timeout <= 0 {
		timeout = 120 * time.Second
	}
	// Global/no-entity agents routinely run deeper research loops; apply a floor
	// to prevent repeated hard-kill timeouts at the process layer.
	if actor, ok := runtimeactors.ActorFromContext(ctx); ok {
		if strings.TrimSpace(actor.EffectiveEntityID()) == "" && timeout < 300*time.Second {
			timeout = 300 * time.Second
		}
	}
	// Explicit env override wins (operator emergency control).
	if raw := strings.TrimSpace(os.Getenv("SWARM_CLAUDE_TIMEOUT_SECONDS")); raw != "" {
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
		dockerBin := strings.TrimSpace(os.Getenv("SWARM_DOCKER_BIN"))
		if dockerBin == "" {
			dockerBin = "docker"
		}
		dockerArgs := []string{"exec", "-i"}
		gatewayURL := runtimeMCPGatewayURLForContainerExecution()
		if gatewayURL != "" {
			dockerArgs = append(dockerArgs, "-e", "SWARM_TOOL_GATEWAY_URL="+gatewayURL)
		}
		if gatewayToken := strings.TrimSpace(os.Getenv("SWARM_TOOL_GATEWAY_TOKEN")); gatewayToken != "" {
			dockerArgs = append(dockerArgs, "-e", "SWARM_TOOL_GATEWAY_TOKEN="+gatewayToken)
		}
		profile, _ := llmselection.ResolveActiveBackend(llmselection.BackendClaudeCLI)
		if oauthToken := llmselection.CredentialValue(profile, os.LookupEnv); oauthToken != "" {
			dockerArgs = append(dockerArgs, "-e", profile.Credential.EnvVar+"="+oauthToken)
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
		return nil, fmt.Errorf("%w: workspace resolver is not configured", ErrClaudeWorkspaceRequired)
	}
	actor, ok := runtimeactors.ActorFromContext(ctx)
	if !ok {
		return nil, fmt.Errorf("%w: actor context is missing", ErrClaudeWorkspaceRequired)
	}
	target, err := r.workspaces.ResolveWorkspace(ctx, actor)
	if err != nil {
		return nil, err
	}
	if target == nil || !target.Enabled() {
		return nil, fmt.Errorf("%w: no container workspace target resolved for agent %s", ErrClaudeWorkspaceRequired, strings.TrimSpace(actor.ID))
	}
	return target, nil
}
