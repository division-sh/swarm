package cliapp

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/division-sh/swarm/internal/config"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
)

const (
	localRuntimeStateOwner = "platform-spec.yaml#cli_specification.foundations.local_runtime_state_authority"

	projectSQLiteStoreRelativePath = ".swarm/stores/dev.db"
)

type localRuntimeStateProject struct {
	ContractsPath        string
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
	ResolvedPaths CLIContractPlatformSpecPaths
	SwarmDir      CLISwarmDirResolution
	Config        *config.Config

	StoreMode    string
	StoreModeSet bool

	DataSource string

	CreateDefaultDataSource bool
	EnforceLegacySQLite     bool
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

func resolveLocalRuntimeStateProject(RepoRoot string, resolvedPaths CLIContractPlatformSpecPaths) localRuntimeStateProject {
	contractsPath := strings.TrimSpace(resolvedPaths.ContractsPath)
	if contractsPath == "" {
		return localRuntimeStateProject{Status: "no_project", Detail: "no resolved contracts path"}
	}
	projectRoot := inferProjectRootFromContractsPath(contractsPath)
	canonicalProjectRoot, projectDetail := canonicalizeDoctorTargetPath(projectRoot)
	canonicalProjectRoot = strings.TrimSpace(canonicalProjectRoot)
	if canonicalProjectRoot == "" {
		return localRuntimeStateProject{
			ContractsPath: contractsPath,
			ProjectRoot:   projectRoot,
			Status:        "invalid_project",
			Detail:        "project root could not be canonicalized",
		}
	}
	canonicalRepoRoot, repoDetail := canonicalizeDoctorTargetPath(RepoRoot)
	projectLocal := localRuntimePathWithin(canonicalProjectRoot, canonicalRepoRoot)
	status := "borrowed_project"
	detail := "contracts project root is outside the active repo root; local store uses the Swarm directory and workspace data requires an explicit source"
	if projectLocal {
		status = "project_local"
		detail = "resolved contracts project root owns local runtime state"
	} else if repoDetail != "resolved" {
		detail = "active repo root could not be canonicalized; local store uses the Swarm directory and workspace data requires an explicit source"
	}
	if projectDetail != "resolved" {
		detail = "project root canonicalization detail: " + projectDetail
	}
	return localRuntimeStateProject{
		ContractsPath:        contractsPath,
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
	volumesFrom, volumesFromSet, err := runtimeConfigWorkspaceVolumesFrom(cfg)
	if err != nil {
		return WorkspaceMountSources{}, err
	}
	defaultDataSource := ""
	defaultSource := ""
	if project.ProjectLocal && strings.TrimSpace(project.CanonicalProjectRoot) != "" {
		defaultDataSource = filepath.Join(project.CanonicalProjectRoot, defaultWorkspaceDataSourceRelativePath)
		defaultSource = defaultWorkspaceDataSourceSource
	}
	return resolveWorkspaceMountSourcesFromInput(workspaceDataSourceInput{
		RepoRoot:                RepoRoot,
		FlagDataSource:          flagDataSource,
		ConfigDataSource:        configDataSource,
		ConfigDataSourceSet:     configDataSourceSet,
		VolumesFrom:             volumesFrom,
		VolumesFromSet:          volumesFromSet,
		DefaultDataSource:       defaultDataSource,
		DefaultDataSourceSource: defaultSource,
		CreateDefaultDataSource: createDefault,
	})
}

func pathExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func ResolveRuntimeStoreSelection(repo string, storeMode string, storeModeSet bool, cfg *config.Config) (storebackend.Selection, error) {
	swarmDir, err := resolveCLISwarmDir(cliSwarmDirOptions{})
	if err != nil {
		return storebackend.Selection{}, err
	}
	resolvedPaths, err := ResolveCLIContractPlatformSpecPaths(repo, CLIContractPlatformSpecPathOptions{})
	if err != nil {
		return storebackend.Selection{}, err
	}
	project := resolveLocalRuntimeStateProject(repo, resolvedPaths)
	defaultPath, defaultSource := localRuntimeSQLiteDefault(swarmDir, project)
	return resolveRuntimeStoreSelectionWithDefault(repo, storeMode, storeModeSet, cfg, defaultPath, defaultSource)
}

func resolveWorkspaceMountSources(RepoRoot string, flagDataSource string, cfg *config.Config) (WorkspaceMountSources, error) {
	resolvedPaths, err := ResolveCLIContractPlatformSpecPaths(RepoRoot, CLIContractPlatformSpecPathOptions{})
	if err != nil {
		return WorkspaceMountSources{}, err
	}
	project := resolveLocalRuntimeStateProject(RepoRoot, resolvedPaths)
	return resolveWorkspaceMountSourcesForLocalState(RepoRoot, flagDataSource, cfg, project, true)
}
