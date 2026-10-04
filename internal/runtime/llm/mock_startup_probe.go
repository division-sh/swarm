package llm

import (
	"context"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/workspace"
)

func (r *MockRuntime) ProbeStartupVisibleToolSurface(ctx context.Context, actor models.AgentConfig, _ string, tools []ToolDefinition) (*Response, error) {
	ctx = models.WithActor(ctx, actor)
	surface, ok := managedcapabilities.FromContext(ctx)
	if !ok || surface.Authority.Kind != managedcapabilities.AuthorityStartupProbe {
		return nil, failures.New(failures.ClassLifecycleConflict, "startup_probe_capability_surface_missing", "mock-adapter", "startup_probe", nil)
	}
	target, err := workspace.ResolveForCapabilityAdmission(ctx, r.workspaces, actor)
	if err != nil {
		return nil, err
	}
	ctx, err = probeWorkspaceMCP(ctx, r.cfg, r.mcpTurns, &Session{AgentID: actor.ID, Tools: tools}, r.toolGateway, target)
	if err != nil {
		return nil, err
	}
	surface, _ = managedcapabilities.FromContext(ctx)
	surface, err = observeAllBindings(surface, managedcapabilities.BindingMCPProvider, evidenceMCPVisible, managedcapabilities.EvidenceConfirmed, "")
	if err != nil {
		return nil, err
	}
	if err := surface.ValidateEffective(); err != nil {
		return nil, err
	}
	return &Response{CapabilitySurface: &surface}, nil
}
