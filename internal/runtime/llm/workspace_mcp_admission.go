package llm

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/toolidentity"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/toolgateway"
	"github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/runtime/workspace/worker"
)

// ObserveWorkspaceGateway performs only the target-local initialize/list
// observation. Its caller supplies the existing planned surface and authority.
func ObserveWorkspaceGateway(ctx context.Context, cfg *config.Config, turns MCPTurnContextStore, actor models.AgentConfig, tools []ToolDefinition, gateway toolgateway.Binding, resolver workspace.Resolver) (managedcapabilities.Surface, error) {
	surface, ok := managedcapabilities.FromContext(ctx)
	if !ok {
		return managedcapabilities.Surface{}, workspaceMCPDefinitionMismatch("", "planned_surface_missing")
	}
	if len(surface.BindingNames(managedcapabilities.BindingMCPTool)) == 0 {
		return surface, nil
	}
	target, err := workspace.ResolveForCapabilityAdmission(ctx, resolver, actor)
	if err != nil {
		return managedcapabilities.Surface{}, err
	}
	ctx, err = probeWorkspaceMCP(ctx, cfg, turns, &Session{AgentID: actor.ID, Tools: tools}, gateway, target)
	if err != nil {
		return managedcapabilities.Surface{}, err
	}
	observed, _ := managedcapabilities.FromContext(ctx)
	return observed, nil
}

func probeWorkspaceMCP(ctx context.Context, cfg *config.Config, turns MCPTurnContextStore, session *Session, gateway toolgateway.Binding, target *workspace.Target) (context.Context, error) {
	if target == nil {
		return ctx, failures.New(failures.ClassDependencyUnavailable, "workspace_worker_target_missing", "llm-runtime", "observe_mcp", nil)
	}
	endpoint := MCPGatewayWorkspaceEndpoint
	if target.ExecutionTarget().Mode == workspace.ExecutionModeHostLocal {
		endpoint = MCPGatewayHostEndpoint
	}
	binding, enabled, err := BuildMCPHTTPBinding(ctx, cfg, turns, session, gateway, endpoint)
	if err != nil {
		return ctx, err
	}
	if !enabled {
		surface, managed := managedcapabilities.FromContext(ctx)
		required := managed && len(surface.BindingNames(managedcapabilities.BindingMCPTool)) != 0
		if !managed && session != nil {
			required = len(buildConversationForkSandboxTransportSurface(session.Tools).RuntimeToolNames) != 0
		}
		if required {
			return ctx, failures.New(failures.ClassLifecycleConflict, "workspace_mcp_transport_required", "llm-runtime", "observe_mcp", nil)
		}
		return ctx, nil
	}
	defer turns.UnregisterTurnContext(binding.ContextToken)
	if !binding.IsRuntimeOwned() {
		return ctx, failures.New(failures.ClassLifecycleConflict, "workspace_mcp_binding_unowned", "llm-runtime", "observe_mcp", nil)
	}
	dockerBin := ""
	if cfg != nil {
		dockerBin = cfg.Workspace.DockerBin
	}
	result, err := workspace.RunWorker(ctx, target, dockerBin, worker.Request{Mode: "probe", Gateway: toolgateway.HTTPObservation{URL: binding.URL, Headers: binding.Headers}})
	if err != nil {
		if failure, ok := failures.EnvelopeFromError(err); ok && failure.Detail.Code == "workspace_gateway_unreachable" {
			return ctx, fmt.Errorf("workspace gateway observation refused (status=%v, http_status=%v): %w", failure.Detail.Attributes["status"], failure.Detail.Attributes["http_status"], err)
		}
		return ctx, err
	}
	surface, managed := managedcapabilities.FromContext(ctx)
	if managed {
		if err := validatePlannedWorkspaceMCPDefinitions(surface, result.Definitions); err != nil {
			return ctx, err
		}
		observed, ok := turns.ResolveManagedCapabilitySurface(binding.ContextToken)
		if !ok || observed.CanAdvanceFrom(surface) != nil {
			return ctx, workspaceMCPDefinitionMismatch("", "gateway_observation_missing_or_foreign")
		}
		// Consume the gateway's exact list evidence instead of inventing another
		// interpretation of the same binding in the parent process.
		return managedcapabilities.WithContext(ctx, observed), nil
	}
	// Fork chat has its own sandbox context, never a fabricated managed surface.
	if err := validateWorkspaceMCPDefinitions(session.Tools, result.Definitions); err != nil {
		return ctx, err
	}
	return ctx, nil
}

func validatePlannedWorkspaceMCPDefinitions(surface managedcapabilities.Surface, definitions []toolgateway.ListedDefinition) error {
	expected := make(map[string]string)
	for _, tool := range surface.Tools {
		if !tool.Capability.Visible || !tool.Capability.Callable {
			continue
		}
		for _, binding := range tool.Bindings {
			if binding.Kind == managedcapabilities.BindingMCPTool {
				expected[tool.Name] = tool.DefinitionHash
			}
		}
	}
	return compareWorkspaceMCPDefinitions(expected, definitions)
}

func validateWorkspaceMCPDefinitions(tools []ToolDefinition, definitions []toolgateway.ListedDefinition) error {
	expected := make(map[string]string, len(tools))
	for _, tool := range tools {
		expected[toolidentity.CanonicalName(tool.Name)] = ToolDefinitionIdentity(tool)
	}
	return compareWorkspaceMCPDefinitions(expected, definitions)
}

func compareWorkspaceMCPDefinitions(expected map[string]string, definitions []toolgateway.ListedDefinition) error {
	seen := make(map[string]bool, len(definitions))
	for _, definition := range definitions {
		name := toolidentity.CanonicalName(definition.Name)
		var schema map[string]any
		if name == "" || name != definition.Name || seen[name] || canonicaljson.DecodePreservingNumberLexemes(definition.InputSchema, &schema) != nil || schema == nil {
			return workspaceMCPDefinitionMismatch(name, "malformed_or_duplicate_definition")
		}
		seen[name] = true
		hash, exists := expected[name]
		if !exists || ToolDefinitionIdentity(ToolDefinition{Name: name, Description: definition.Description, Schema: schema}) != hash {
			return workspaceMCPDefinitionMismatch(name, "unplanned_or_changed_definition")
		}
	}
	for name := range expected {
		if !seen[name] {
			return workspaceMCPDefinitionMismatch(name, "missing_definition")
		}
	}
	return nil
}

func workspaceMCPDefinitionMismatch(name, reason string) error {
	return failures.New(failures.ClassSchemaInvalid, "managed_capability_mcp_definition_mismatch", "llm-runtime", "observe_mcp", map[string]any{"tool": name, "reason": reason})
}

func (r *ClaudeCLIRuntime) probeWorkspaceMCP(ctx context.Context, session *Session, target *workspace.Target) (context.Context, error) {
	if !shouldUseMCPBridge() && len(session.Tools) != 0 {
		return ctx, fmt.Errorf("managed MCP transport cannot be disabled")
	}
	return probeWorkspaceMCP(ctx, r.cfg, r.mcpTurns, session, r.toolGateway, target)
}
