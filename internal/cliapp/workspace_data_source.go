package cliapp

import (
	"fmt"
	"os"
	"path/filepath"
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
	switch {
	case strings.TrimSpace(in.FlagDataSource) != "":
		path, err := normalizeWorkspaceDataSourcePath(in.RepoRoot, in.FlagDataSource, "--data")
		return WorkspaceMountSources{DataSource: path, DataSourceSource: "--data"}, err
	case in.ConfigDataSourceSet:
		path, err := normalizeWorkspaceDataSourcePath(in.RepoRoot, in.ConfigDataSource, "workspace.data_source")
		return WorkspaceMountSources{DataSource: path, DataSourceSource: "workspace.data_source"}, err
	case in.VolumesFromSet && strings.TrimSpace(in.VolumesFrom) != "":
		return WorkspaceMountSources{}, nil
	case strings.TrimSpace(in.DefaultDataSource) != "":
		path, err := normalizeWorkspaceDataSourcePath(in.RepoRoot, in.DefaultDataSource, defaultWorkspaceDataSourceSourceLabel(in.DefaultDataSourceSource))
		if err != nil {
			return WorkspaceMountSources{DataSource: path, DataSourceSource: defaultWorkspaceDataSourceSourceLabel(in.DefaultDataSourceSource)}, err
		}
		if in.CreateDefaultDataSource {
			if err := os.MkdirAll(path, 0o755); err != nil {
				return WorkspaceMountSources{DataSource: path, DataSourceSource: defaultWorkspaceDataSourceSourceLabel(in.DefaultDataSourceSource)}, fmt.Errorf("create default workspace data source %s: %w", path, err)
			}
		}
		return WorkspaceMountSources{DataSource: path, DataSourceSource: defaultWorkspaceDataSourceSourceLabel(in.DefaultDataSourceSource)}, nil
	default:
		return WorkspaceMountSources{}, fmt.Errorf("workspace data source is required: pass --data, set workspace.data_source, or run from a project with a managed %s default", defaultWorkspaceDataSourceRelativePath)
	}
}

func defaultWorkspaceDataSourceSourceLabel(source string) string {
	source = strings.TrimSpace(source)
	if source == "" {
		return defaultWorkspaceDataSourceSource
	}
	return source
}

func runtimeConfigWorkspaceDataSource(cfg *config.Config) (string, bool) {
	if cfg == nil {
		return "", false
	}
	return cfg.Workspace.DataSource, cfg.Workspace.DataSourceConfigured()
}

func normalizeWorkspaceDataSourcePath(RepoRoot string, raw string, source string) (string, error) {
	path := strings.TrimSpace(raw)
	if path == "" {
		return "", fmt.Errorf("workspace data source from %s must be non-empty", source)
	}
	return filepath.Clean(ResolvePath(RepoRoot, path)), nil
}
