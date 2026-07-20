package llm

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/config"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
)

const claudeCLICompletionAdapter = "claude_cli"

func errClaudeHostWorkspaceUnsupported() error {
	return fmt.Errorf("%w: host workspace backend does not support Claude CLI execution yet", ErrClaudeWorkspaceRequired)
}

func requireClaudeExecutionTarget(target *workspace.Target) (workspace.ExecutionTarget, error) {
	execTarget := target.ExecutionTarget()
	if execTarget.Supports(workspace.ExecutionCapabilityClaudeCLI) {
		return execTarget, nil
	}
	if strings.EqualFold(strings.TrimSpace(execTarget.Backend), workspace.BackendHost) {
		return execTarget, errClaudeHostWorkspaceUnsupported()
	}
	return execTarget, fmt.Errorf("%w: %s", ErrClaudeWorkspaceRequired, execTarget.UnsupportedMessage(workspace.ExecutionCapabilityClaudeCLI))
}

func (r *ClaudeCLIRuntime) runWithPreparedInput(ctx context.Context, args []string, target *workspace.Target, input string, meta MonitorTurnMeta, attempt *runtimeeffects.Handle) (resp *Response, retErr error) {
	if attempt == nil {
		return nil, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "completion_effect_handle_missing", "claude-cli-adapter", "run", nil)
	}
	timeout := r.effectiveCLITimeout(ctx)
	if _, err := requireClaudeExecutionTarget(target); err != nil {
		return nil, returnClaudeAttemptFailure(ctx, attempt, runtimeeffects.StateTerminalFailure, err, "resolve_execution_target", map[string]any{"prelaunch": true})
	}
	release, err := r.admitProviderDispatch(ctx)
	if err != nil {
		return nil, returnClaudeAttemptFailure(ctx, attempt, runtimeeffects.StateTerminalFailure, err, "provider_admission", map[string]any{"prelaunch": true})
	}
	defer release()
	heartbeatCtx, heartbeat, err := startCompletionAttemptHeartbeat(ctx, attempt)
	if err != nil {
		return nil, returnClaudeAttemptFailure(ctx, attempt, runtimeeffects.StateTerminalFailure, err, "heartbeat_attempt", map[string]any{"prelaunch": true})
	}
	defer func() {
		resp, retErr = finishClaudeCompletionAttemptHeartbeat(heartbeat, resp, retErr)
	}()

	runCtx, cancel := context.WithTimeout(heartbeatCtx, timeout)
	defer cancel()

	cmd, err := r.buildCommand(runCtx, args, target)
	if err != nil {
		if IsMissingProviderCredential(err) {
			err = runtimefailures.Wrap(runtimefailures.ClassAuthenticationNeeded, "provider_credential_missing", "claude-cli-adapter", "build_command", map[string]any{"auth_kind": "provider_credential"}, err)
		}
		return nil, returnClaudeAttemptFailure(ctx, attempt, runtimeeffects.StateTerminalFailure, err, "build_command", map[string]any{"prelaunch": true})
	}
	if configuredCLIOutputFormat(r.cfg) == "stream-json" {
		return r.runStreamingPrepared(runCtx, cmd, target, timeout, input, meta, attempt)
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if strings.TrimSpace(input) != "" {
		cmd.Stdin = strings.NewReader(input)
	}
	if err := requireCompletionAttemptHeartbeat(runCtx); err != nil {
		return nil, returnClaudeAttemptFailure(ctx, attempt, runtimeeffects.StateTerminalFailure, err, "heartbeat_attempt", map[string]any{"prelaunch": true})
	}
	if err := attempt.MarkLaunched(runCtx); err != nil {
		return nil, returnClaudeAttemptFailure(ctx, attempt, runtimeeffects.StateTerminalFailure, err, "mark_launched", map[string]any{"prelaunch": true})
	}
	if err := cmd.Start(); err != nil {
		failureErr := runtimefailures.Wrap(runtimefailures.ClassDependencyUnavailable, "claude_cli_process_start_failed", "claude-cli-adapter", "start", map[string]any{"launch_rejected": true}, err)
		return nil, returnClaudeAttemptFailure(ctx, attempt, runtimeeffects.StateTerminalFailure, failureErr, "start", map[string]any{"launch_rejected": true})
	}
	if err := cmd.Wait(); err != nil {
		stderrText := strings.TrimSpace(stderr.String())
		stdoutText := strings.TrimSpace(stdout.String())
		cause := claudeCLIProcessFailure(stderrText, stdoutText, "claude_cli_process_failed", "run", err)
		if errors.Is(runCtx.Err(), context.DeadlineExceeded) {
			cause = runtimefailures.Wrap(runtimefailures.ClassTimeout, "claude_cli_timeout", "claude-cli-adapter", "run", map[string]any{"timeout": timeout.String()}, err)
		}
		return nil, returnClaudeAttemptFailure(ctx, attempt, runtimeeffects.StateOutcomeUncertain, cause, "wait", map[string]any{"timeout": timeout.String(), "stderr": summarizeCLIErrorOutput(stderrText), "stdout": summarizeCLIErrorOutput(stdoutText)})
	}

	raw := bytes.TrimSpace(stdout.Bytes())
	if err := attempt.MarkResponseObserved(runCtx, map[string]any{"response_fingerprint": runtimeeffects.Fingerprint(raw)}); err != nil {
		return nil, returnClaudeAttemptFailure(ctx, attempt, runtimeeffects.StateOutcomeUncertain, err, "mark_response_observed", map[string]any{"response_fingerprint": runtimeeffects.Fingerprint(raw)})
	}
	resp = parseCLIResponse(raw)
	resp.Raw = raw
	return resp, nil
}

func (r *ClaudeCLIRuntime) runStreamingPrepared(ctx context.Context, cmd *exec.Cmd, target *workspace.Target, timeout time.Duration, input string, meta MonitorTurnMeta, attempt *runtimeeffects.Handle) (*Response, error) {
	if attempt == nil {
		return nil, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "completion_effect_handle_missing", "claude-cli-adapter", "run_streaming", nil)
	}
	if err := requireCompletionAttemptHeartbeat(ctx); err != nil {
		return nil, returnClaudeAttemptFailure(ctx, attempt, runtimeeffects.StateTerminalFailure, err, "heartbeat_attempt", map[string]any{"prelaunch": true})
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		failureErr := fmt.Errorf("create claude stdout pipe: %w", err)
		return nil, returnClaudeAttemptFailure(ctx, attempt, runtimeeffects.StateTerminalFailure, failureErr, "create_stdout_pipe", map[string]any{"prelaunch": true})
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		failureErr := fmt.Errorf("create claude stderr pipe: %w", err)
		return nil, returnClaudeAttemptFailure(ctx, attempt, runtimeeffects.StateTerminalFailure, failureErr, "create_stderr_pipe", map[string]any{"prelaunch": true})
	}
	if strings.TrimSpace(input) != "" {
		cmd.Stdin = strings.NewReader(input)
	}
	monitor, monitorErr := r.openMonitorTurn(ctx, meta)
	if monitorErr != nil {
		return nil, returnClaudeAttemptFailure(ctx, attempt, runtimeeffects.StateTerminalFailure, monitorErr, "open_monitor", map[string]any{"prelaunch": true})
	}
	if monitor != nil {
		defer func() { _ = monitor.Close() }()
	}

	if err := attempt.MarkLaunched(ctx); err != nil {
		return nil, returnClaudeAttemptFailure(ctx, attempt, runtimeeffects.StateTerminalFailure, err, "mark_launched", map[string]any{"prelaunch": true})
	}
	if err := cmd.Start(); err != nil {
		failureErr := runtimefailures.Wrap(runtimefailures.ClassDependencyUnavailable, "claude_cli_process_start_failed", "claude-cli-adapter", "start", map[string]any{"launch_rejected": true}, err)
		return nil, returnClaudeAttemptFailure(ctx, attempt, runtimeeffects.StateTerminalFailure, failureErr, "start", map[string]any{"launch_rejected": true})
	}

	stdoutCh := make(chan [][]byte, 1)
	stderrCh := make(chan [][]byte, 1)
	go func() { stdoutCh <- readStreamLines(stdout, monitor, false) }()
	go func() { stderrCh <- readStreamLines(stderr, monitor, true) }()

	stdoutLines := <-stdoutCh
	stderrLines := <-stderrCh
	waitErr := cmd.Wait()
	if waitErr != nil {
		stderrText := strings.TrimSpace(string(joinRawLines(stderrLines)))
		stdoutText := strings.TrimSpace(string(joinRawLines(stdoutLines)))
		if monitor != nil {
			monitor.WriteNotice("turn.end ok=false")
		}
		cause := claudeCLIProcessFailure(stderrText, stdoutText, "claude_cli_process_failed", "run_streaming", waitErr)
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			cause = runtimefailures.Wrap(runtimefailures.ClassTimeout, "claude_cli_timeout", "claude-cli-adapter", "run_streaming", map[string]any{"timeout": timeout.String()}, waitErr)
		}
		return nil, returnClaudeAttemptFailure(ctx, attempt, runtimeeffects.StateOutcomeUncertain, cause, "wait_streaming", map[string]any{"timeout": timeout.String(), "stderr": summarizeCLIErrorOutput(stderrText), "stdout": summarizeCLIErrorOutput(stdoutText)})
	}

	acc := newCLIStreamAccumulator()
	for _, line := range stdoutLines {
		acc.AddLine(line)
	}
	resp := acc.Response()
	if err := attempt.MarkResponseObserved(ctx, map[string]any{"response_fingerprint": runtimeeffects.Fingerprint(resp.Raw)}); err != nil {
		return nil, returnClaudeAttemptFailure(ctx, attempt, runtimeeffects.StateOutcomeUncertain, err, "mark_response_observed", map[string]any{"response_fingerprint": runtimeeffects.Fingerprint(resp.Raw)})
	}
	if monitor != nil {
		monitor.WriteNotice("turn.end ok=true session=%s", strings.TrimSpace(coalesce(resp.SessionID, meta.SessionID)))
	}
	return resp, nil
}

func settleClaudeAttemptFailure(ctx context.Context, attempt *runtimeeffects.Handle, state runtimeeffects.State, original error, operation string, evidence map[string]any) error {
	if original == nil || attempt == nil {
		return original
	}
	settlementCause := original
	if state == runtimeeffects.StateOutcomeUncertain {
		settlementCause = runtimefailures.Wrap(runtimefailures.ClassOutcomeUncertain, "claude_cli_attempt_outcome_unconfirmed", "claude-cli-adapter", operation, evidence, original)
	}
	failure := runtimefailures.FromError(settlementCause, "claude-cli-adapter", operation)
	if settleErr := attempt.Settle(ctx, state, &failure.Failure, evidence); settleErr != nil {
		return errors.Join(original, fmt.Errorf("settle claude attempt after %s: %w", operation, settleErr))
	}
	return original
}

type claudeCompletionAttemptFailure struct {
	state runtimeeffects.State
	err   error
}

func (e *claudeCompletionAttemptFailure) Error() string { return e.err.Error() }
func (e *claudeCompletionAttemptFailure) Unwrap() error { return e.err }

func claudeCompletionFailureState(err error) runtimeeffects.State {
	var attemptFailure *claudeCompletionAttemptFailure
	if errors.As(err, &attemptFailure) {
		return attemptFailure.state
	}
	return runtimeeffects.StateTerminalFailure
}

func returnClaudeAttemptFailure(ctx context.Context, attempt *runtimeeffects.Handle, state runtimeeffects.State, original error, operation string, evidence map[string]any) error {
	if attempt != nil && attempt.Attempt().Authority.Target.Valid() {
		return &claudeCompletionAttemptFailure{state: state, err: original}
	}
	return settleClaudeAttemptFailure(ctx, attempt, state, original, operation, evidence)
}

func finishClaudeCompletionAttemptHeartbeat(heartbeat *completionAttemptHeartbeat, response *Response, prior error) (*Response, error) {
	if heartbeat == nil {
		return response, prior
	}
	heartbeatErr := heartbeat.Stop()
	if heartbeatErr == nil {
		return response, prior
	}
	return nil, &claudeCompletionAttemptFailure{
		state: runtimeeffects.StateOutcomeUncertain,
		err:   errors.Join(prior, completionAttemptHeartbeatLoss(heartbeatErr)),
	}
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
	writer, err := newSessionWatchdogMonitorWriter(ctx, base, r.conversations, r.events, meta)
	if err != nil {
		if base != nil {
			err = errors.Join(err, base.Close())
		}
		return nil, err
	}
	return writer, nil
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
	return timeout
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

func (r *ClaudeCLIRuntime) buildCommand(ctx context.Context, args []string, target *workspace.Target) (*exec.Cmd, error) {
	execTarget, err := requireClaudeExecutionTarget(target)
	if err != nil {
		return nil, err
	}
	if execTarget.Mode == workspace.ExecutionModeDockerContainer {
		dockerBin := configuredWorkspaceDockerBin(r.cfg)
		dockerArgs := []string{"exec", "-i"}
		gatewayURL := r.toolGateway.WorkspaceMCPURL()
		if gatewayURL != "" {
			dockerArgs = append(dockerArgs, "-e", "SWARM_TOOL_GATEWAY_URL="+gatewayURL)
		}
		profile, _ := llmselection.ResolveActiveBackend(llmselection.BackendClaudeCLI)
		credential, err := r.providerCredentials.Resolve(ctx, profile)
		if err != nil {
			return nil, err
		}
		if oauthToken := strings.TrimSpace(credential.Value); oauthToken != "" {
			dockerArgs = append(dockerArgs, "-e", ProviderCredentialKey(profile)+"="+oauthToken)
		}
		if strings.TrimSpace(execTarget.Workdir) != "" {
			dockerArgs = append(dockerArgs, "-w", execTarget.Workdir)
		}
		dockerArgs = append(dockerArgs, execTarget.Container, r.cfg.LLM.ClaudeCLI.Command)
		dockerArgs = append(dockerArgs, args...)
		return exec.CommandContext(ctx, dockerBin, dockerArgs...), nil
	}
	return nil, fmt.Errorf("%w: %s", ErrClaudeWorkspaceRequired, execTarget.UnsupportedMessage(workspace.ExecutionCapabilityClaudeCLI))
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
	if _, err := requireClaudeExecutionTarget(target); err != nil {
		return nil, fmt.Errorf("%w for agent %s", err, strings.TrimSpace(actor.ID))
	}
	return target, nil
}
