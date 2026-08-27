package cliapp

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/config"
)

const (
	defaultWorkspaceDataSourceRelativePath = ".swarm/data"
	defaultWorkspaceDataSourceSource       = "project_default"
)

type WorkspaceMountSources struct {
	DataSource       string
	DataSourceSource string
}

type workspaceDataSourceInput struct {
	RepoRoot string

	FlagDataSource string

	ConfigDataSource    string
	ConfigDataSourceSet bool

	VolumesFrom    string
	VolumesFromSet bool

	DefaultDataSource       string
	DefaultDataSourceSource string
	CreateDefaultDataSource bool
}

func resolveWorkspaceMountSourcesFromInput(in workspaceDataSourceInput) (WorkspaceMountSources, error) {
	if strings.TrimSpace(in.FlagDataSource) != "" || in.ConfigDataSourceSet || in.VolumesFromSet {
		return WorkspaceMountSources{}, fmt.Errorf("ambient workspace data sources are retired; use flow_data_access, data_access, or swarm run start --data name=file.jsonl")
	}
	return WorkspaceMountSources{}, nil
}

func runtimeConfigWorkspaceDataSource(cfg *config.Config) (string, bool) {
	if cfg == nil {
		return "", false
	}
	return cfg.Workspace.DataSource, cfg.Workspace.DataSourceConfigured()
}
