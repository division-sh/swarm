package backendselection

import (
	"fmt"
	"path/filepath"
	"strings"
)

type Backend string

const (
	BackendPostgres Backend = "postgres"
	BackendSQLite   Backend = "sqlite"

	ConfigStoreBackendKey = "store.backend"
	ConfigSQLitePathKey   = "store.sqlite.path"

	LegacySQLiteRelativePath = ".swarm/dev.db"
)

type Source string

const (
	SourceFlag            Source = "flag"
	SourceRuntimeConfig   Source = "runtime_config"
	SourceRolloutDefault  Source = "rollout_default"
	SourceProjectDefault  Source = "project_default"
	SourceSwarmDirDefault Source = "swarm_dir_default"
)

type Input struct {
	RepoRoot string

	FlagBackend    string
	FlagBackendSet bool

	ConfigBackend string

	ConfigSQLitePath string

	DefaultSQLitePath       string
	DefaultSQLitePathSource Source
}

type Selection struct {
	Backend          Backend
	BackendSource    Source
	SQLitePath       string
	SQLitePathSource Source
}

func (b Backend) String() string {
	return string(b)
}

func ActiveDefaultBackend() Backend {
	return BackendSQLite
}

func Resolve(in Input) (Selection, error) {
	backend, source, err := resolveBackend(in)
	if err != nil {
		return Selection{}, err
	}
	selection := Selection{
		Backend:       backend,
		BackendSource: source,
	}
	if backend != BackendSQLite {
		return selection, nil
	}
	path, pathSource, err := resolveSQLitePath(in)
	if err != nil {
		return Selection{}, err
	}
	selection.SQLitePath = path
	selection.SQLitePathSource = pathSource
	return selection, nil
}

func resolveBackend(in Input) (Backend, Source, error) {
	switch {
	case in.FlagBackendSet:
		backend, err := parseBackend(in.FlagBackend, SourceFlag)
		return backend, SourceFlag, err
	case strings.TrimSpace(in.ConfigBackend) != "":
		backend, err := parseBackend(in.ConfigBackend, SourceRuntimeConfig)
		return backend, SourceRuntimeConfig, err
	default:
		return ActiveDefaultBackend(), SourceRolloutDefault, nil
	}
}

func parseBackend(raw string, source Source) (Backend, error) {
	value := strings.ToLower(strings.TrimSpace(raw))
	if value == "" {
		return "", fmt.Errorf("store backend from %s must be non-empty", source)
	}
	switch Backend(value) {
	case BackendPostgres:
		return BackendPostgres, nil
	case BackendSQLite:
		return BackendSQLite, nil
	default:
		return "", fmt.Errorf("unsupported store backend %q from %s; supported backends: %s, %s", strings.TrimSpace(raw), source, BackendPostgres, BackendSQLite)
	}
}

func resolveSQLitePath(in Input) (string, Source, error) {
	switch {
	case strings.TrimSpace(in.ConfigSQLitePath) != "":
		path, err := normalizeSQLitePath(in.RepoRoot, in.ConfigSQLitePath, SourceRuntimeConfig)
		return path, SourceRuntimeConfig, err
	default:
		source := in.DefaultSQLitePathSource
		if source == "" {
			source = SourceRolloutDefault
		}
		path, err := normalizeSQLitePath(in.RepoRoot, in.DefaultSQLitePath, source)
		return path, source, err
	}
}

func normalizeSQLitePath(repoRoot, raw string, source Source) (string, error) {
	path := strings.TrimSpace(raw)
	if path == "" {
		return "", fmt.Errorf("sqlite path from %s must be non-empty", source)
	}
	if filepath.IsAbs(path) {
		return filepath.Clean(path), nil
	}
	root := strings.TrimSpace(repoRoot)
	if root == "" {
		return filepath.Clean(path), nil
	}
	return filepath.Clean(filepath.Join(root, path)), nil
}
