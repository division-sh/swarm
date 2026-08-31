package cliapp

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/config"
	runtimedestructivereset "github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

var ConfiguredWorkspaceLifecycleForServe = ConfiguredWorkspaceLifecycleForBackend

type ServeWorkspaceLifecycle interface {
	workspace.Lifecycle
	runtimedestructivereset.ManagedContainerInventoryReader
	runtimedestructivereset.ManagedContainerRuntime
}

func configuredWorkspaceLifecycle(cfg *config.Config, projection *sourceartifact.RuntimeProjection, source semanticview.Source, mountSources WorkspaceMountSources) (*workspace.DockerManager, error) {
	manager := workspace.NewDockerManager()
	workspaceCfg, err := dockerWorkspaceConfigFromRuntimeConfig(cfg)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(mountSources.DataSource) != "" {
		return nil, fmt.Errorf("ambient workspace data sources are retired; declare flow_data_access or data_access")
	}
	workspaceCfg.SourceProjection = projection
	manager.SetConfig(workspaceCfg)
	manager.SetSemanticSource(source)
	return manager, nil
}

func ConfiguredWorkspaceLifecycleForBackend(cfg *config.Config, projection *sourceartifact.RuntimeProjection, source semanticview.Source, mountSources WorkspaceMountSources, backend WorkspaceBackendSelection) (ServeWorkspaceLifecycle, error) {
	selected := strings.TrimSpace(backend.Backend)
	if selected == "" {
		return nil, fmt.Errorf("workspace backend decision is required")
	}
	switch selected {
	case WorkspaceBackendNone:
		return nil, nil
	case workspace.BackendDocker:
		return configuredWorkspaceLifecycle(cfg, projection, source, mountSources)
	case workspace.BackendHost:
		return configuredHostWorkspaceLifecycle(cfg, projection, source, mountSources)
	default:
		sourceLabel := strings.TrimSpace(backend.Source)
		if sourceLabel == "" {
			sourceLabel = "workspace backend"
		}
		return nil, fmt.Errorf("workspace backend from %s must be docker or host, got %q", sourceLabel, selected)
	}
}

func configuredHostWorkspaceLifecycle(cfg *config.Config, projection *sourceartifact.RuntimeProjection, source semanticview.Source, mountSources WorkspaceMountSources) (*workspace.HostManager, error) {
	manager := workspace.NewHostManager()
	workspaceCfg, err := hostWorkspaceConfigFromRuntimeConfig(cfg)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(mountSources.DataSource) != "" {
		return nil, fmt.Errorf("ambient workspace data sources are retired; declare flow_data_access or data_access")
	}
	workspaceCfg.SourceProjection = projection
	manager.SetConfig(workspaceCfg)
	manager.SetSemanticSource(source)
	return manager, nil
}
