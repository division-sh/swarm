package llm

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/toolgateway"
	"github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/runtime/workspace/worker"
	"github.com/google/uuid"
)

// ObserveClaudeStartup consumes only a compiled plan and a disposable target.
// It never constructs an execution runtime, capability authority or effect row.
func ObserveClaudeStartup(ctx context.Context, cfg *config.Config, credentials ProviderCredentialResolver, actor actors.AgentConfig, prompt string, planned []managedcapabilities.PlannedTool, target *workspace.Target, catalogue toolgateway.HTTPObservation) (*Response, error) {
	if cfg == nil {
		return nil, fmt.Errorf("admitted runtime configuration is required")
	}
	if err := managedcapabilities.ValidatePlannedTools(planned); err != nil {
		return nil, err
	}
	if _, err := requireClaudeExecutionTarget(target); err != nil {
		return nil, err
	}
	var mcpConfig string
	expected := map[string]string{}
	for _, tool := range planned {
		if !tool.Capability.Visible || !tool.Capability.Callable {
			continue
		}
		for _, binding := range tool.Bindings {
			if binding.Kind == managedcapabilities.BindingMCPTool {
				expected[tool.Name] = tool.DefinitionHash
			}
		}
	}
	if len(expected) != 0 {
		if !shouldUseMCPBridge() {
			return nil, workspaceMCPDefinitionMismatch("", "workspace_mcp_transport_required")
		}
		result, err := workspace.RunWorker(ctx, target, configuredWorkspaceDockerBin(cfg), worker.Request{Mode: "probe", Gateway: catalogue})
		if err != nil {
			return nil, err
		}
		if err := compareWorkspaceMCPDefinitions(expected, result.Definitions); err != nil {
			return nil, err
		}
		raw, err := json.Marshal(map[string]any{"mcpServers": map[string]any{"runtime-tools": map[string]any{
			"type": "http", "url": catalogue.URL, "headers": catalogue.Headers,
		}}})
		if err != nil {
			return nil, err
		}
		mcpConfig = string(raw)
	}
	projection := newClaudeInvocationToolProjection(startupPlanBindings(planned, managedcapabilities.BindingProviderBuiltin), append(
		startupPlanBindings(planned, managedcapabilities.BindingProviderBuiltin), startupPlanBindings(planned, managedcapabilities.BindingMCPTool)...))
	ctx = actors.WithActor(ctx, actor)
	r := &ClaudeCLIRuntime{cfg: cfg, providerCredentials: credentials}
	runCtx, cancel := context.WithTimeout(ctx, r.effectiveCLITimeout(ctx))
	defer cancel()
	args := claudeStartupProbeArgs(cfg, uuid.NewString(), prompt, projection, mcpConfig)
	cmd, err := r.buildCommand(runCtx, args, target)
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("create claude stdout pipe: %w", err)
	}
	defer stdout.Close()
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("create claude stderr pipe: %w", err)
	}
	defer stderr.Close()
	cmd.Stdin = strings.NewReader(cliStartupProbePrompt)
	if err := runCtx.Err(); err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("claude cli run failed: %w", err)
	}
	observed := observeCLIStartupProcess(runCtx, cancel, cmd, stdout, stderr)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if observed.err != nil {
		return nil, observed.err
	}
	if err := validateStartupCLIObservation(planned, observed.response); err != nil {
		return nil, err
	}
	return observed.response, nil
}

func startupPlanBindings(planned []managedcapabilities.PlannedTool, kind managedcapabilities.BindingKind) []string {
	var names []string
	for _, tool := range planned {
		if tool.Capability.Visible && tool.Capability.Callable {
			for _, binding := range tool.Bindings {
				if binding.Kind == kind {
					names = append(names, binding.ExactName)
				}
			}
		}
	}
	slices.Sort(names)
	return slices.Compact(names)
}

func validateStartupCLIObservation(planned []managedcapabilities.PlannedTool, response *Response) error {
	expected := startupPlanBindings(planned, managedcapabilities.BindingProviderBuiltin)
	actual, err := exactCLIProviderVisibleTools(response)
	if err != nil {
		return err
	}
	if !slices.Equal(expected, actual) {
		return fmt.Errorf("provider-visible capability mismatch: expected [%s], got [%s]", strings.Join(expected, ", "), strings.Join(actual, ", "))
	}
	expected = startupPlanBindings(planned, managedcapabilities.BindingMCPProvider)
	actual = exactMCPVisibleTools(response)
	if len(expected) != 0 && (response == nil || !mcpRuntimeServerConnected(response.MCPServers)) {
		return workspaceMCPDefinitionMismatch("", "provider_mcp_unavailable")
	}
	if !slices.Equal(expected, actual) {
		return workspaceMCPDefinitionMismatch("", "provider_mcp_visible_mismatch")
	}
	return nil
}
