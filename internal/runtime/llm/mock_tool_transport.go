package llm

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/toolgateway"
	"github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/runtime/workspace/worker"
)

func (r *MockRuntime) executeTransportTool(ctx context.Context, session *Session, call ToolCall, output *ToolOutputAuthority, occurrence string) (_ any, retErr error) {
	if r.workspaces == nil || r.mcpTurns == nil || !r.toolGateway.IsRuntimeOwned() || !shouldUseMCPBridge() {
		return nil, failures.New(failures.ClassLifecycleConflict, "mock_mcp_transport_required", "mock-python-adapter", "execute_tool", nil)
	}
	if output != nil {
		var err error
		ctx, err = withToolOutputCall(ctx, *output, occurrence, call.Name, call.Arguments)
		if err != nil {
			return nil, err
		}
	}
	actor, ok := models.ActorFromContext(ctx)
	if !ok {
		return nil, failures.New(failures.ClassLifecycleConflict, "mock_actor_required", "mock-python-adapter", "execute_tool", nil)
	}
	target, err := r.resolveWorkspace(ctx, actor)
	if err != nil {
		return nil, err
	}
	defer func() { retErr = errors.Join(retErr, releaseMockTarget(ctx, target)) }()
	endpoint := MCPGatewayWorkspaceEndpoint
	if target.ExecutionTarget().Mode == workspace.ExecutionModeHostLocal {
		endpoint = MCPGatewayHostEndpoint
	}
	binding, enabled, err := BuildMCPHTTPBinding(ctx, r.cfg, r.mcpTurns, session, r.toolGateway, endpoint)
	if err != nil {
		return nil, err
	}
	if !enabled {
		return nil, failures.New(failures.ClassLifecycleConflict, "mock_mcp_transport_required", "mock-python-adapter", "execute_tool", nil)
	}
	defer r.mcpTurns.UnregisterTurnContext(binding.ContextToken)
	arguments, err := json.Marshal(call.Arguments)
	if err != nil {
		return nil, err
	}
	result, err := workspace.RunWorker(ctx, target, r.cfg.Workspace.DockerBin, worker.Request{
		Mode: "call", Gateway: toolgateway.HTTPObservation{URL: binding.URL, Headers: binding.Headers},
		Tool: call.Name, Arguments: arguments, Occurrence: occurrence,
	})
	if err != nil {
		return nil, err
	}
	var wire struct {
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
		IsError      bool `json:"isError"`
		RuntimeError *struct {
			Failure *failures.Envelope `json:"failure"`
		} `json:"runtimeError"`
	}
	if json.Unmarshal(result.ToolResult, &wire) != nil || len(wire.Content) != 1 || wire.Content[0].Type != "text" {
		return nil, failures.New(failures.ClassOutcomeUncertain, "workspace_tool_outcome_uncertain", "mock-python-adapter", "execute_tool", map[string]any{"tool": call.Name, "status": "result_invalid"})
	}
	if wire.IsError {
		if wire.RuntimeError != nil && wire.RuntimeError.Failure != nil {
			return nil, failures.FromEnvelope(*wire.RuntimeError.Failure)
		}
		return nil, failures.New(failures.ClassInternalFailure, "mock_mcp_tool_failed", "mock-python-adapter", "execute_tool", map[string]any{"tool": call.Name})
	}
	var value any
	if err := json.Unmarshal([]byte(wire.Content[0].Text), &value); err != nil {
		return nil, failures.New(failures.ClassOutcomeUncertain, "workspace_tool_outcome_uncertain", "mock-python-adapter", "execute_tool", map[string]any{"tool": call.Name, "status": "result_value_invalid"})
	}
	return value, nil
}

func (r *MockRuntime) resolveWorkspace(ctx context.Context, actor models.AgentConfig) (*workspace.Target, error) {
	if authority, ok := runtimeeffects.AuthorityFromContext(ctx); ok && authority.Kind == runtimeeffects.AuthorityConversationForkChat {
		if !authority.Valid() {
			return nil, failures.New(failures.ClassLifecycleConflict, "forkchat_workspace_authority_invalid", "mock-python-adapter", "prepare_target", nil)
		}
		return workspace.ResolveForForkChat(runtimeeffects.WithController(ctx, r.completionController), r.workspaces, actor)
	}
	return r.workspaces.ResolveWorkspace(ctx, actor)
}

func releaseMockTarget(ctx context.Context, target *workspace.Target) error {
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if err := target.Release(cleanup); err != nil {
		return failures.Wrap(failures.ClassLifecycleConflict, "forkchat_workspace_release_failed", "mock-python-adapter", "release_target", nil, err)
	}
	return nil
}
