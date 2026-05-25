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

	models "swarm/internal/runtime/core/actors"
	workspace "swarm/internal/runtime/workspace"
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
			"--session-id", sessionToken(s),
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
	if err == nil {
		return resp, nil
	}
	if !isUnsupportedCLIFlagError(err) {
		return nil, err
	}

	args, contextToken, rebuildErr := buildArgs(false)
	if contextToken != "" {
		defer r.mcpTurns.UnregisterTurnContext(contextToken)
	}
	if rebuildErr != nil {
		return nil, rebuildErr
	}
	return r.runUntilCLIStartupInit(ctx, args, target, buildInitialPrompt(s, cliStartupProbePrompt))
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
	if target == nil || !target.Enabled() {
		return nil, fmt.Errorf("%w: claude sessions must run in a container workspace", ErrClaudeWorkspaceRequired)
	}
	if strings.TrimSpace(input) == "" {
		return nil, errors.New("startup probe requires non-empty prompt")
	}

	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := r.buildCommand(runCtx, args, target)
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
				return nil, fmt.Errorf("claude cli startup probe failed: %w", diag)
			}
			if errOut != "" {
				return nil, fmt.Errorf("claude cli startup probe failed: %w, stderr=%s", waitErr, errOut)
			}
		}
		return result.resp, nil
	}

	stderrText := strings.TrimSpace(string(joinRawLines(stderrLines)))
	if isClaudeAuthOutput(stderrText) {
		msg := summarizeCLIErrorOutput(stderrText)
		if msg == "" {
			msg = "not logged in"
		}
		return nil, fmt.Errorf("%w: %s", ErrClaudeAuthRequired, msg)
	}
	if waitErr != nil {
		errOut := summarizeCLIErrorOutput(stderrText)
		if diag := workspaceCLIDiagnosticError(r.cfg, target, coalesce(errOut, stderrText)); diag != nil {
			return nil, fmt.Errorf("claude cli startup probe failed: %w", diag)
		}
		if errOut == "" {
			return nil, fmt.Errorf("claude cli startup probe failed: %w", waitErr)
		}
		return nil, fmt.Errorf("claude cli startup probe failed: %w, stderr=%s", waitErr, errOut)
	}
	return nil, errors.New("claude cli startup probe saw no init surface")
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
