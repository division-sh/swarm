package llm

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
)

const cliStartupProbePrompt = "Startup validation probe. Do not call any tools. Reply with the exact text ok."

type cliStartupProbeResult struct {
	resp  *Response
	found bool
	err   error
}

func (r *ClaudeCLIRuntime) ProbeStartupVisibleToolSurface(ctx context.Context, actor models.AgentConfig, systemPrompt string, tools []ToolDefinition) (*Response, error) {
	ctx = models.WithActor(ctx, actor)
	target, err := r.resolveWorkspace(ctx)
	if err != nil {
		return nil, err
	}

	s := &Session{
		ID:           ensurePlatformSessionID(""),
		AgentID:      strings.TrimSpace(actor.ID),
		SystemPrompt: strings.TrimSpace(systemPrompt),
		Tools:        append([]ToolDefinition(nil), tools...),
	}
	surface, ok := managedcapabilities.FromContext(ctx)
	if !ok || surface.Authority.Kind != managedcapabilities.AuthorityStartupProbe {
		return nil, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "startup_probe_capability_surface_missing", "claude-cli-adapter", "startup_probe", nil)
	}
	handle, err := runtimeeffects.BeginStartupProbe(ctx, "claude_cli_startup_probe", jsonBytes(map[string]any{
		"actor_id": actor.ID, "system_prompt": s.SystemPrompt, "tools": s.Tools, "surface_id": surface.ID,
	}), nil)
	if err != nil {
		return nil, err
	}
	childSessionID := strings.TrimSpace(handle.Attempt().AttemptID)
	if childSessionID == "" {
		return nil, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "startup_probe_attempt_identity_missing", "claude-cli-adapter", "startup_probe", nil)
	}

	buildArgs := func(includeSystemPrompt bool) ([]string, string, error) {
		args := []string{
			"-p",
			"--session-id", childSessionID,
			"--output-format", "stream-json",
		}
		args = appendClaudePrintModeArgs(args, r.cfg)
		args = append(args, permissionModeArgs()...)
		if includeSystemPrompt {
			if sys := strings.TrimSpace(s.SystemPrompt); sys != "" {
				args = append(args, "--system-prompt", sys)
			}
		}
		allowed, disallowed, err := claudeToolArgumentsForContext(ctx, actor, s.Tools)
		if err != nil {
			return nil, "", err
		}
		if disallowed = strings.TrimSpace(disallowed); disallowed != "" {
			args = append(args, "--disallowedTools", disallowed)
		}
		if allowed = strings.TrimSpace(allowed); allowed != "" {
			args = append(args, "--allowedTools", allowed)
		}
		mcpConfig, contextToken, enabled, err := r.buildMCPConfigArg(ctx, s)
		if err != nil {
			return nil, "", err
		}
		if enabled {
			args = append(args, "--mcp-config", mcpConfig, "--strict-mcp-config")
		}
		return args, contextToken, nil
	}

	args, contextToken, err := buildArgs(true)
	if contextToken != "" {
		defer r.mcpTurns.UnregisterTurnContext(contextToken)
	}
	if err != nil {
		_ = handle.Fail(ctx, runtimeeffects.StateTerminalFailure, runtimefailures.ClassSchemaInvalid, "startup_probe_prelaunch_rejected", "claude-cli-adapter", "startup_probe", nil, err)
		return nil, err
	}

	resp, err := r.runUntilCLIStartupInit(ctx, args, target, cliStartupProbePrompt, handle)
	if err != nil {
		return nil, err
	}
	observed, err := observeCLIResponse(surface, resp)
	if err != nil {
		_ = handle.Fail(ctx, runtimeeffects.StateTerminalFailure, runtimefailures.ClassSchemaInvalid, "startup_probe_surface_observation_failed", "claude-cli-adapter", "startup_probe", nil, err)
		return nil, err
	}
	resp.CapabilitySurface = &observed
	if err := ValidateCLIProviderCapabilitySurface(observed, resp); err != nil {
		_ = handle.Fail(ctx, runtimeeffects.StateTerminalFailure, runtimefailures.ClassSchemaInvalid, "startup_probe_provider_surface_mismatch", "claude-cli-adapter", "startup_probe", map[string]any{"surface_id": observed.ID, "integrity_hash": observed.IntegrityHash}, err)
		return nil, err
	}
	if err := handle.MarkResponseObserved(ctx, map[string]any{"surface_id": observed.ID, "integrity_hash": observed.IntegrityHash}); err != nil {
		return nil, err
	}
	if err := handle.Succeed(ctx, map[string]any{"surface_id": observed.ID, "integrity_hash": observed.IntegrityHash}); err != nil {
		return nil, err
	}
	return resp, nil
}

func (r *ClaudeCLIRuntime) runUntilCLIStartupInit(ctx context.Context, args []string, target *workspace.Target, input string, handle *runtimeeffects.Handle) (*Response, error) {
	timeout := r.effectiveCLITimeout(ctx)
	if _, err := requireClaudeExecutionTarget(target); err != nil {
		failStartupProbePrelaunch(ctx, handle, err)
		return nil, err
	}
	if strings.TrimSpace(input) == "" {
		err := errors.New("startup probe requires non-empty prompt")
		failStartupProbePrelaunch(ctx, handle, err)
		return nil, err
	}

	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd, err := r.buildCommand(runCtx, args, target)
	if err != nil {
		failStartupProbePrelaunch(ctx, handle, err)
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		failStartupProbePrelaunch(ctx, handle, err)
		return nil, fmt.Errorf("create claude stdout pipe: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		failStartupProbePrelaunch(ctx, handle, err)
		return nil, fmt.Errorf("create claude stderr pipe: %w", err)
	}
	cmd.Stdin = strings.NewReader(input)

	if err := handle.MarkLaunched(ctx); err != nil {
		_ = handle.Fail(ctx, runtimeeffects.StateTerminalFailure, runtimefailures.ClassLifecycleConflict, "startup_probe_launch_mark_failed", "claude-cli-adapter", "startup_probe", nil, err)
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		_ = handle.Fail(ctx, runtimeeffects.StateTerminalFailure, runtimefailures.ClassConnectorFailure, "claude_cli_startup_launch_rejected", "claude-cli-adapter", "startup_probe", nil, err)
		return nil, fmt.Errorf("claude cli run failed: %w", err)
	}

	stdoutCh := make(chan cliStartupProbeResult, 1)
	stderrCh := make(chan [][]byte, 1)
	go func() { stdoutCh <- readCLIStartupInit(stdout) }()
	go func() { stderrCh <- readStreamLines(stderr, nil, true) }()

	result := <-stdoutCh
	if result.found {
		cancel()
	}
	waitErr := cmd.Wait()
	stderrLines := <-stderrCh

	if result.err != nil {
		return nil, result.err
	}
	if result.found {
		if waitErr != nil && !errors.Is(runCtx.Err(), context.Canceled) {
			stderrText := strings.TrimSpace(string(joinRawLines(stderrLines)))
			stdoutText := ""
			if result.resp != nil {
				stdoutText = strings.TrimSpace(string(result.resp.Raw))
			}
			failure := claudeCLIProcessFailure(stderrText, stdoutText, "claude_cli_startup_probe_failed", "startup_probe", waitErr)
			_ = handle.Fail(ctx, runtimeeffects.StateOutcomeUncertain, runtimefailures.ClassOutcomeUncertain, "claude_cli_startup_outcome_uncertain", "claude-cli-adapter", "startup_probe", nil, failure)
			return nil, failure
		}
		return result.resp, nil
	}

	stderrText := strings.TrimSpace(string(joinRawLines(stderrLines)))
	if isClaudeAuthOutput(stderrText) {
		failure := claudeCLIProcessFailure(stderrText, "", "claude_cli_startup_probe_failed", "startup_probe", waitErr)
		_ = handle.Fail(ctx, runtimeeffects.StateOutcomeUncertain, runtimefailures.ClassOutcomeUncertain, "claude_cli_startup_outcome_uncertain", "claude-cli-adapter", "startup_probe", nil, failure)
		return nil, failure
	}
	if waitErr != nil {
		failure := claudeCLIProcessFailure(stderrText, "", "claude_cli_startup_probe_failed", "startup_probe", waitErr)
		_ = handle.Fail(ctx, runtimeeffects.StateOutcomeUncertain, runtimefailures.ClassOutcomeUncertain, "claude_cli_startup_outcome_uncertain", "claude-cli-adapter", "startup_probe", nil, failure)
		return nil, failure
	}
	failure := runtimefailures.New(runtimefailures.ClassConnectorFailure, "claude_cli_startup_surface_missing", "claude-cli-adapter", "startup_probe", nil)
	_ = handle.Fail(ctx, runtimeeffects.StateOutcomeUncertain, runtimefailures.ClassOutcomeUncertain, "claude_cli_startup_outcome_uncertain", "claude-cli-adapter", "startup_probe", nil, failure)
	return nil, failure
}

func failStartupProbePrelaunch(ctx context.Context, handle *runtimeeffects.Handle, err error) {
	if handle == nil || err == nil {
		return
	}
	_ = handle.Fail(ctx, runtimeeffects.StateTerminalFailure, runtimefailures.ClassConnectorFailure, "claude_cli_startup_prelaunch_rejected", "claude-cli-adapter", "startup_probe", nil, err)
}

func readCLIStartupInit(rc io.ReadCloser) cliStartupProbeResult {
	defer rc.Close()

	acc := newCLIStreamAccumulator()
	scanner := bufio.NewScanner(rc)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := append([]byte(nil), scanner.Bytes()...)
		acc.AddLine(line)
		if isCLIStartupInitLine(line) {
			resp := acc.Response()
			resp.Raw = bytes.TrimSpace(acc.raw.Bytes())
			return cliStartupProbeResult{resp: resp, found: true}
		}
	}
	if err := scanner.Err(); err != nil {
		return cliStartupProbeResult{err: err}
	}
	return cliStartupProbeResult{}
}

func isCLIStartupInitLine(line []byte) bool {
	var obj map[string]any
	if err := json.Unmarshal(line, &obj); err != nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(asString(obj["type"])), "system") &&
		strings.EqualFold(strings.TrimSpace(asString(obj["subtype"])), "init")
}
