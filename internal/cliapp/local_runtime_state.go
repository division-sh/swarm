package cliapp

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/division-sh/swarm/internal/config"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	"github.com/division-sh/swarm/internal/store/devscratch"
)

const (
	localRuntimeStateOwner = "platform-spec.yaml#cli_specification.foundations.local_runtime_state_authority"

	projectSQLiteStoreRelativePath = ".swarm/stores/dev.db"
)

type localRuntimeStateProject struct {
	ProjectRoot          string
	CanonicalProjectRoot string
	ProjectLocal         bool
	Status               string
	Detail               string
}

type LocalRuntimeStateResolution struct {
	Owner          string
	SwarmDir       CLISwarmDirResolution
	Project        localRuntimeStateProject
	StoreSelection storebackend.Selection
	MountSources   WorkspaceMountSources
}

type LocalRuntimeStateOptions struct {
	RepoRoot      string
	ResolvedPaths CLISourcePlatformSpecPaths
	SwarmDir      CLISwarmDirResolution
	Config        *config.Config

	StoreMode    string
	StoreModeSet bool

	DataSource string

	CreateDefaultDataSource bool
	EnforceLegacySQLite     bool
}

type DevScratchResolution struct {
	Coordinate devscratch.Coordinate
	Selection  storebackend.Selection
}

func ResolveLocalRuntimeState(in LocalRuntimeStateOptions) (LocalRuntimeStateResolution, error) {
	if in.Config == nil {
		return LocalRuntimeStateResolution{}, fmt.Errorf("runtime config is required")
	}
	project := resolveLocalRuntimeStateProject(in.RepoRoot, in.ResolvedPaths)
	sqliteDefaultPath, sqliteDefaultSource := localRuntimeSQLiteDefault(in.SwarmDir, project)
	storeSelection, err := resolveRuntimeStoreSelectionWithDefault(in.RepoRoot, in.StoreMode, in.StoreModeSet, in.Config, sqliteDefaultPath, sqliteDefaultSource)
	if err != nil {
		return LocalRuntimeStateResolution{}, err
	}
	if in.EnforceLegacySQLite {
		if err := rejectLegacyProjectSQLiteStore(project, storeSelection); err != nil {
			return LocalRuntimeStateResolution{}, err
		}
	}
	mountSources, err := resolveWorkspaceMountSourcesForLocalState(in.RepoRoot, in.DataSource, in.Config, project, in.CreateDefaultDataSource)
	if err != nil {
		return LocalRuntimeStateResolution{}, err
	}
	return LocalRuntimeStateResolution{
		Owner:          localRuntimeStateOwner,
		SwarmDir:       in.SwarmDir,
		Project:        project,
		StoreSelection: storeSelection,
		MountSources:   mountSources,
	}, nil
}

// ResolveDevScratch projects an already-resolved local state into the only
// supported serve --dev store posture. It performs no filesystem mutation.
func ResolveDevScratch(state LocalRuntimeStateResolution) (DevScratchResolution, error) {
	project := state.Project
	if !project.ProjectLocal || strings.TrimSpace(project.CanonicalProjectRoot) == "" {
		return DevScratchResolution{}, fmt.Errorf("swarm serve --dev scratch storage requires project-local contracts; run from the contracts-owning project root, or use non-dev serve with an explicitly owned store")
	}
	selection := state.StoreSelection
	if selection.Backend != storebackend.BackendSQLite {
		return DevScratchResolution{}, fmt.Errorf("swarm serve --dev supports only its project-local SQLite scratch store; remove the PostgreSQL store selection or use non-dev serve")
	}
	if selection.SQLitePathSource != storebackend.SourceProjectDefault {
		return DevScratchResolution{}, fmt.Errorf("swarm serve --dev cannot use authored or shared SQLite path %q; remove store.sqlite.path and use the project-local scratch store, or use non-dev serve", strings.TrimSpace(selection.SQLitePath))
	}
	coordinate, err := devscratch.Resolve(project.CanonicalProjectRoot)
	if err != nil {
		return DevScratchResolution{}, err
	}
	selection.SQLitePath = coordinate.DatabasePath
	selection.SQLitePathSource = storebackend.SourceProjectScratch
	return DevScratchResolution{Coordinate: coordinate, Selection: selection}, nil
}

func resolveLocalRuntimeStateProject(RepoRoot string, resolvedPaths CLISourcePlatformSpecPaths) localRuntimeStateProject {
	selectedRoot := strings.TrimSpace(resolvedPaths.SourceRoot)
	if selectedRoot == "" {
		return localRuntimeStateProject{Status: "no_project", Detail: "no selected source root"}
	}
	projectRoot := filepath.Clean(selectedRoot)
	canonicalProjectRoot, projectDetail := canonicalizeDoctorTargetPath(projectRoot)
	canonicalProjectRoot = strings.TrimSpace(canonicalProjectRoot)
	if canonicalProjectRoot == "" {
		return localRuntimeStateProject{
			ProjectRoot: projectRoot,
			Status:      "invalid_project",
			Detail:      "selected source root could not be canonicalized",
		}
	}
	canonicalRepoRoot, repoDetail := canonicalizeDoctorTargetPath(RepoRoot)
	projectLocal := localRuntimePathWithin(canonicalProjectRoot, canonicalRepoRoot)
	status := "borrowed_project"
	detail := "selected source root is outside the active repo root; local store uses the Swarm directory and workspace data requires an explicit source"
	if projectLocal {
		status = "project_local"
		detail = "the exact selected source root owns local development state"
	} else if repoDetail != "resolved" {
		detail = "active repo root could not be canonicalized; local store uses the Swarm directory and workspace data requires an explicit source"
	}
	if projectDetail != "resolved" {
		detail = "project root canonicalization detail: " + projectDetail
	}
	return localRuntimeStateProject{
		ProjectRoot:          projectRoot,
		CanonicalProjectRoot: canonicalProjectRoot,
		ProjectLocal:         projectLocal,
		Status:               status,
		Detail:               detail,
	}
}

func localRuntimeSQLiteDefault(swarmDir CLISwarmDirResolution, project localRuntimeStateProject) (string, storebackend.Source) {
	if project.ProjectLocal && strings.TrimSpace(project.CanonicalProjectRoot) != "" {
		return filepath.Join(project.CanonicalProjectRoot, projectSQLiteStoreRelativePath), storebackend.SourceProjectDefault
	}
	if strings.TrimSpace(project.CanonicalProjectRoot) != "" {
		return filepath.Join(swarmDir.Path, "stores", "projects", localRuntimeProjectKey(project.CanonicalProjectRoot), "dev.db"), storebackend.SourceSwarmDirDefault
	}
	return filepath.Join(swarmDir.Path, "stores", "default", "dev.db"), storebackend.SourceSwarmDirDefault
}

func localRuntimeProjectKey(canonicalProjectRoot string) string {
	return sanitizeLocalContextNameComponent(localProjectContextName(canonicalProjectRoot))
}

func localRuntimePathWithin(path, root string) bool {
	path = filepath.Clean(strings.TrimSpace(path))
	root = filepath.Clean(strings.TrimSpace(root))
	if path == "" || root == "" || path == "." || root == "." {
		return false
	}
	if path == root {
		return true
	}
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return false
	}
	return rel != "." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && rel != ".."
}

func resolveRuntimeStoreSelectionWithDefault(repo string, storeMode string, storeModeSet bool, cfg *config.Config, defaultSQLitePath string, defaultSQLiteSource storebackend.Source) (storebackend.Selection, error) {
	if cfg == nil {
		return storebackend.Selection{}, fmt.Errorf("runtime config is required")
	}
	return storebackend.Resolve(storebackend.Input{
		RepoRoot:                repo,
		FlagBackend:             storeMode,
		FlagBackendSet:          storeModeSet,
		ConfigBackend:           cfg.Store.Backend,
		ConfigSQLitePath:        cfg.Store.SQLite.Path,
		DefaultSQLitePath:       defaultSQLitePath,
		DefaultSQLitePathSource: defaultSQLiteSource,
	})
}

func rejectLegacyProjectSQLiteStore(project localRuntimeStateProject, selection storebackend.Selection) error {
	return legacyProjectSQLiteStoreError(project, selection)
}

func legacyProjectSQLiteStoreError(project localRuntimeStateProject, selection storebackend.Selection) error {
	if !project.ProjectLocal || selection.Backend != storebackend.BackendSQLite || selection.SQLitePathSource != storebackend.SourceProjectDefault {
		return nil
	}
	legacyPath := filepath.Join(project.CanonicalProjectRoot, storebackend.LegacySQLiteRelativePath)
	canonicalPath := strings.TrimSpace(selection.SQLitePath)
	if filepath.Clean(legacyPath) == filepath.Clean(canonicalPath) {
		return nil
	}
	if !pathExists(legacyPath) || pathExists(canonicalPath) {
		return nil
	}
	return fmt.Errorf("legacy project SQLite store exists at %s; canonical project SQLite store is %s; move the file to the canonical path or remove the legacy file after confirming the old data is no longer needed", legacyPath, canonicalPath)
}

func resolveWorkspaceMountSourcesForLocalState(RepoRoot string, flagDataSource string, cfg *config.Config, project localRuntimeStateProject, createDefault bool) (WorkspaceMountSources, error) {
	configDataSource, configDataSourceSet := runtimeConfigWorkspaceDataSource(cfg)
	volumesFrom := ""
	volumesFromSet := false
	if cfg != nil {
		volumesFrom = cfg.Workspace.VolumesFrom
		volumesFromSet = cfg.Workspace.VolumesFromConfigured()
	}
	return resolveWorkspaceMountSourcesFromInput(workspaceDataSourceInput{
		RepoRoot: RepoRoot, FlagDataSource: flagDataSource,
		ConfigDataSource: configDataSource, ConfigDataSourceSet: configDataSourceSet,
		VolumesFrom: volumesFrom, VolumesFromSet: volumesFromSet,
	})
}

func pathExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func ResolveRuntimeStoreSelection(repo string, storeMode string, storeModeSet bool, cfg *config.Config) (storebackend.Selection, error) {
	root, err := NewInvocationRoot(repo)
	if err != nil {
		return storebackend.Selection{}, err
	}
	swarmDir, err := resolveCLISwarmDirAt(root, cliSwarmDirOptions{})
	if err != nil {
		return storebackend.Selection{}, err
	}
	defaultPath := filepath.Join(swarmDir.Path, "stores", "default", "dev.db")
	defaultSource := storebackend.SourceSwarmDirDefault
	return resolveRuntimeStoreSelectionWithDefault(repo, storeMode, storeModeSet, cfg, defaultPath, defaultSource)
}
