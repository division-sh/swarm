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
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/google/uuid"
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
		SystemPrompt: augmentCLISystemPrompt(systemPrompt, actor, tools),
		Tools:        append([]ToolDefinition(nil), tools...),
	}

	buildArgs := func(includeSystemPrompt bool) ([]string, string, error) {
		args := []string{
			"-p",
			"--session-id", uuid.NewString(),
			"--output-format", "stream-json",
		}
		args = appendClaudePrintModeArgs(args, r.cfg)
		args = append(args, permissionModeArgs()...)
		if includeSystemPrompt {
			if sys := strings.TrimSpace(s.SystemPrompt); sys != "" {
				args = append(args, "--system-prompt", sys)
			}
		}
		if disallowed := strings.TrimSpace(claudeDisallowedBuiltinToolsArgForActor(actor, s.Tools)); disallowed != "" {
			args = append(args, "--disallowedTools", disallowed)
		}
		if allowed := strings.TrimSpace(claudeAllowedToolsArgForActor(actor, s.Tools)); allowed != "" {
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
		return nil, err
	}

	resp, err := r.runUntilCLIStartupInit(ctx, args, target, cliStartupProbePrompt)
	return resp, err
}

func ObservedCanonicalVisibleToolsForActor(actor models.AgentConfig, tools []ToolDefinition, resp *Response) []string {
	return observedCanonicalVisibleToolsForActor(actor, tools, resp)
}

func PlannedCanonicalVisibleToolsForActor(actor models.AgentConfig, tools []ToolDefinition) []string {
	return plannedCanonicalVisibleToolsForActor(actor, tools)
}

func ObservedProviderNativeVisibleToolsForActor(actor models.AgentConfig, tools []ToolDefinition, resp *Response) []string {
	if resp == nil {
		return nil
	}
	return filterProviderNativeVisibleToolsForActor(actor, tools, resp.VisibleTools)
}

func PlannedProviderNativeVisibleToolsForActor(actor models.AgentConfig, tools []ToolDefinition) []string {
	return providerNativeCanonicalVisibleToolsForActor(actor, tools)
}

func (r *ClaudeCLIRuntime) runUntilCLIStartupInit(ctx context.Context, args []string, target *workspace.Target, input string) (*Response, error) {
	timeout := r.effectiveCLITimeout(ctx)
	if _, err := requireClaudeExecutionTarget(target); err != nil {
		return nil, err
	}
	if strings.TrimSpace(input) == "" {
		return nil, errors.New("startup probe requires non-empty prompt")
	}

	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd, err := r.buildCommand(runCtx, args, target)
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("create claude stdout pipe: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("create claude stderr pipe: %w", err)
	}
	cmd.Stdin = strings.NewReader(input)

	if err := cmd.Start(); err != nil {
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
			return nil, claudeCLIProcessFailure(stderrText, stdoutText, "claude_cli_startup_probe_failed", "startup_probe", waitErr)
		}
		return result.resp, nil
	}

	stderrText := strings.TrimSpace(string(joinRawLines(stderrLines)))
	if isClaudeAuthOutput(stderrText) {
		return nil, claudeCLIProcessFailure(stderrText, "", "claude_cli_startup_probe_failed", "startup_probe", waitErr)
	}
	if waitErr != nil {
		return nil, claudeCLIProcessFailure(stderrText, "", "claude_cli_startup_probe_failed", "startup_probe", waitErr)
	}
	return nil, runtimefailures.New(runtimefailures.ClassConnectorFailure, "claude_cli_startup_surface_missing", "claude-cli-adapter", "startup_probe", nil)
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
