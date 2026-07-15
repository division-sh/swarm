package cliapp

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/config"
	runtimedestructivereset "github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
)

var ConfiguredWorkspaceLifecycleForServe = ConfiguredWorkspaceLifecycleForBackend

type ServeWorkspaceLifecycle interface {
	workspace.Lifecycle
	workspace.DevEntityContainerCleaner
	runtimedestructivereset.ManagedContainerInventoryReader
	runtimedestructivereset.ManagedContainerRuntime
}

func configuredWorkspaceLifecycle(db *sql.DB, cfg *config.Config, contractsRoot string, source semanticview.Source, mountSources WorkspaceMountSources) (*workspace.DockerManager, error) {
	manager := workspace.NewDockerManager(db)
	workspaceCfg, err := dockerWorkspaceConfigFromRuntimeConfig(cfg)
	if err != nil {
		return nil, err
	}
	if dataSource := strings.TrimSpace(mountSources.DataSource); dataSource != "" {
		if volumesFrom := strings.TrimSpace(workspaceCfg.WorkspaceVolumesFrom); volumesFrom != "" {
			sourceLabel := strings.TrimSpace(mountSources.DataSourceSource)
			if sourceLabel == "" {
				sourceLabel = "explicit data source"
			}
			return nil, fmt.Errorf("workspace data source from %s cannot be combined with workspace.volumes_from=%s", sourceLabel, volumesFrom)
		}
		workspaceCfg.SharedDataSource = dataSource
	}
	if contractsDir := strings.TrimSpace(contractsRoot); contractsDir != "" {
		workspaceCfg.ContractsSource = contractsDir
	}
	manager.SetConfig(workspaceCfg)
	manager.SetSemanticSource(source)
	return manager, nil
}

func ConfiguredWorkspaceLifecycleForBackend(db *sql.DB, cfg *config.Config, contractsRoot string, source semanticview.Source, mountSources WorkspaceMountSources, backend WorkspaceBackendSelection) (ServeWorkspaceLifecycle, error) {
	selected := strings.TrimSpace(backend.Backend)
	if selected == "" {
		return nil, fmt.Errorf("workspace backend decision is required")
	}
	switch selected {
	case WorkspaceBackendNone:
		return nil, nil
	case workspace.BackendDocker:
		return configuredWorkspaceLifecycle(db, cfg, contractsRoot, source, mountSources)
	case workspace.BackendHost:
		return configuredHostWorkspaceLifecycle(db, cfg, contractsRoot, source, mountSources)
	default:
		sourceLabel := strings.TrimSpace(backend.Source)
		if sourceLabel == "" {
			sourceLabel = "workspace backend"
		}
		return nil, fmt.Errorf("workspace backend from %s must be docker or host, got %q", sourceLabel, selected)
	}
}

func configuredHostWorkspaceLifecycle(db *sql.DB, cfg *config.Config, contractsRoot string, source semanticview.Source, mountSources WorkspaceMountSources) (*workspace.HostManager, error) {
	if cfg != nil && cfg.Workspace.VolumesFromConfigured() {
		volumesFrom := strings.TrimSpace(cfg.Workspace.VolumesFrom)
		if volumesFrom == "" {
			return nil, fmt.Errorf("workspace.volumes_from must be non-empty when configured")
		}
		return nil, fmt.Errorf("host workspace backend cannot consume workspace.volumes_from=%s", volumesFrom)
	}
	manager := workspace.NewHostManager(db)
	workspaceCfg, err := hostWorkspaceConfigFromRuntimeConfig(cfg)
	if err != nil {
		return nil, err
	}
	if dataSource := strings.TrimSpace(mountSources.DataSource); dataSource != "" {
		workspaceCfg.SharedDataSource = dataSource
	}
	if contractsDir := strings.TrimSpace(contractsRoot); contractsDir != "" {
		workspaceCfg.ContractsSource = contractsDir
	}
	manager.SetConfig(workspaceCfg)
	manager.SetSemanticSource(source)
	return manager, nil
}
