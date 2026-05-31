package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"swarm/internal/config"
)

const envWorkspaceDataSource = "SWARM_WORKSPACE_DATA_SOURCE"

type workspaceMountSources struct {
	DataSource       string
	DataSourceSource string
}

type workspaceDataSourceInput struct {
	RepoRoot string

	FlagDataSource string

	ConfigDataSource string

	EnvDataSource    string
	EnvDataSourceSet bool
}

func resolveWorkspaceMountSources(repoRoot string, flagDataSource string, cfg *config.Config) (workspaceMountSources, error) {
	envDataSource, envDataSourceSet := os.LookupEnv(envWorkspaceDataSource)
	return resolveWorkspaceMountSourcesFromInput(workspaceDataSourceInput{
		RepoRoot:         repoRoot,
		FlagDataSource:   flagDataSource,
		ConfigDataSource: runtimeConfigWorkspaceDataSource(cfg),
		EnvDataSource:    envDataSource,
		EnvDataSourceSet: envDataSourceSet,
	})
}

func resolveWorkspaceMountSourcesFromInput(in workspaceDataSourceInput) (workspaceMountSources, error) {
	switch {
	case strings.TrimSpace(in.FlagDataSource) != "":
		path, err := normalizeWorkspaceDataSourcePath(in.RepoRoot, in.FlagDataSource, "--data")
		return workspaceMountSources{DataSource: path, DataSourceSource: "--data"}, err
	case strings.TrimSpace(in.ConfigDataSource) != "":
		path, err := normalizeWorkspaceDataSourcePath(in.RepoRoot, in.ConfigDataSource, "workspace.data_source")
		return workspaceMountSources{DataSource: path, DataSourceSource: "workspace.data_source"}, err
	case in.EnvDataSourceSet && strings.TrimSpace(in.EnvDataSource) != "":
		path, err := normalizeWorkspaceDataSourcePath(in.RepoRoot, in.EnvDataSource, envWorkspaceDataSource)
		return workspaceMountSources{DataSource: path, DataSourceSource: envWorkspaceDataSource}, err
	default:
		return workspaceMountSources{}, nil
	}
}

func runtimeConfigWorkspaceDataSource(cfg *config.Config) string {
	if cfg == nil {
		return ""
	}
	return cfg.Workspace.DataSource
}

func normalizeWorkspaceDataSourcePath(repoRoot string, raw string, source string) (string, error) {
	path := strings.TrimSpace(raw)
	if path == "" {
		return "", fmt.Errorf("workspace data source from %s must be non-empty", source)
	}
	return filepath.Clean(resolvePath(repoRoot, path)), nil
}
