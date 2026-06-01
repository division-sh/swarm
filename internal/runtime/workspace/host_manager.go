package workspace

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	runtimedestructivereset "swarm/internal/runtime/destructivereset"
	"swarm/internal/runtime/semanticview"

	models "swarm/internal/runtime/core/actors"
)

const EnvHostWorkspaceRoot = "SWARM_WORKSPACE_HOST_ROOT"

type HostConfig struct {
	WorkspaceRoot       string
	SharedDataSource    string
	DataMountPoint      string
	ContractsSource     string
	ContractsMountPoint string
	BundleHash          string
	BundleScope         string
}

type HostManager struct {
	db     *sql.DB
	cfg    HostConfig
	source semanticview.Source
}

func DefaultHostConfig() HostConfig {
	return HostConfig{
		WorkspaceRoot:       EnvOrDefault(EnvHostWorkspaceRoot, defaultHostWorkspaceRoot()),
		SharedDataSource:    "",
		DataMountPoint:      EnvOrDefault("SWARM_WORKSPACE_DATA_MOUNT", "/data"),
		ContractsSource:     EnvOrDefault("SWARM_WORKSPACE_CONTRACTS_SOURCE", ""),
		ContractsMountPoint: EnvOrDefault("SWARM_WORKSPACE_CONTRACTS_MOUNT", "/opt/swarm/contracts"),
	}
}

func NewHostManager(db *sql.DB) *HostManager {
	return &HostManager{
		db:  db,
		cfg: DefaultHostConfig(),
	}
}

func (m *HostManager) SetConfig(cfg HostConfig) {
	if m == nil {
		return
	}
	m.cfg = cfg
}

func (m *HostManager) SetConfigForTest(cfg HostConfig) {
	if m == nil {
		return
	}
	m.cfg = cfg
}

func (m *HostManager) SetSemanticSource(source semanticview.Source) {
	if m == nil {
		return
	}
	m.source = source
}

func (m *HostManager) SetBundleScope(bundleHash string) {
	if m == nil {
		return
	}
	cfg := m.cfg
	cfg.BundleHash = strings.TrimSpace(bundleHash)
	cfg.BundleScope = bundleScopeKey(bundleHash)
	m.cfg = cfg
}

func (m *HostManager) ValidateSource(_ context.Context, source semanticview.Source) error {
	if m == nil {
		return fmt.Errorf("host workspace manager is required")
	}
	if source == nil {
		return fmt.Errorf("workspace semantic source is required")
	}
	m.source = source
	if err := m.validateSharedMounts(); err != nil {
		return err
	}
	classes, err := workspaceClassesForSource(source)
	if err != nil {
		return err
	}
	for agentID, entry := range source.AgentEntries() {
		agentID = strings.TrimSpace(agentID)
		class := strings.TrimSpace(entry.WorkspaceClass)
		if class == "" {
			continue
		}
		scope, ok := classes[class]
		if !ok {
			return fmt.Errorf("workspace validation failed: agent %s references undefined workspace_class %q", agentID, class)
		}
		if !isSupportedWorkspaceScope(scope) {
			return fmt.Errorf("workspace validation failed: workspace_class %q declares unsupported workspace_scope %q", class, scope)
		}
	}
	return nil
}

func (m *HostManager) EnsurePrereqs(context.Context) error {
	if m == nil {
		return fmt.Errorf("host workspace manager is required")
	}
	root, err := m.hostRoot()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return fmt.Errorf("host workspace prerequisite failed: create workspace root %s: %w", root, err)
	}
	return nil
}

func (m *HostManager) EnsureSystemWorkspaces(ctx context.Context) error {
	if err := m.EnsurePrereqs(ctx); err != nil {
		return err
	}
	for _, name := range []string{"scaffold", "system"} {
		if _, err := m.ensureHostWorkspaceDir(name); err != nil {
			return err
		}
	}
	return nil
}

func (m *HostManager) EnsureEntityWorkspace(_ context.Context, entityID string) error {
	if strings.TrimSpace(entityID) == "" {
		return fmt.Errorf("entity_id is required")
	}
	_, err := m.ensureHostWorkspaceDir(filepath.Join("entities", SanitizeSlug(entityID)))
	return err
}

func (m *HostManager) StopEntityWorkspace(context.Context, string) error {
	return nil
}

func (m *HostManager) ResolveWorkspace(_ context.Context, actor models.AgentConfig) (*Target, error) {
	if m == nil {
		return nil, fmt.Errorf("host workspace manager is required")
	}
	class, err := workspaceClassForSource(m.source, actor)
	if err != nil {
		return nil, err
	}
	switch workspaceRouteClass(class) {
	case "scaffold":
		return m.hostTarget("scaffold")
	case "system":
		return m.hostTarget("system")
	}
	scope, scopeKey, err := workspaceScopeForActor(m.source, actor)
	if err != nil {
		return nil, err
	}
	switch scope {
	case "per-flow-instance":
		return m.hostTarget(filepath.Join("flows", SanitizeSlug(scopeKey)))
	default:
		return m.hostTarget(filepath.Join("agents", SanitizeSlug(scopeKey)))
	}
}

func (m *HostManager) CleanupDevEntityContainers(context.Context) (runtimedestructivereset.ContainerResetResult, error) {
	return runtimedestructivereset.ContainerResetResult{
		OperationName: DevEntityCleanupOperationName,
		AppliedAt:     time.Now().UTC(),
	}, nil
}

func (m *HostManager) ManagedResetContainerInventory(context.Context) ([]runtimedestructivereset.ContainerRef, error) {
	return nil, nil
}

func (m *HostManager) InspectManagedContainer(context.Context, string) (runtimedestructivereset.ManagedContainerInspection, error) {
	return runtimedestructivereset.ManagedContainerInspection{}, nil
}

func (m *HostManager) StopManagedContainer(context.Context, string) error {
	return nil
}

func (m *HostManager) RuntimeWorkspaceContainers(context.Context) ([]string, error) {
	return nil, nil
}

func (m *HostManager) SystemWorkspaceContainers() []string {
	return nil
}

func (m *HostManager) KillOrphanProcesses(context.Context) error {
	return nil
}

func (m *HostManager) hostTarget(rel string) (*Target, error) {
	workdir, err := m.ensureHostWorkspaceDir(rel)
	if err != nil {
		return nil, err
	}
	return &Target{
		Workdir: workdir,
		Backend: BackendHost,
	}, nil
}

func (m *HostManager) ensureHostWorkspaceDir(rel string) (string, error) {
	root, err := m.hostRoot()
	if err != nil {
		return "", err
	}
	components := sanitizedHostPathComponents(rel)
	if len(components) == 0 {
		return "", fmt.Errorf("host workspace path is required")
	}
	path := filepath.Join(append([]string{root}, components...)...)
	if err := ensurePathWithinRoot(path, root, "host workspace"); err != nil {
		return "", err
	}
	if err := os.MkdirAll(path, 0o700); err != nil {
		return "", fmt.Errorf("create host workspace %s: %w", path, err)
	}
	return path, nil
}

func (m *HostManager) hostRoot() (string, error) {
	if m == nil {
		return "", fmt.Errorf("host workspace manager is required")
	}
	root, err := cleanAbsPath(m.cfg.WorkspaceRoot, "host workspace root")
	if err != nil {
		return "", err
	}
	if scope := strings.TrimSpace(m.cfg.BundleScope); scope != "" {
		root = filepath.Join(root, SanitizeSlug(scope))
	}
	return root, nil
}

func (m *HostManager) validateSharedMounts() error {
	if err := validateReadableDir(strings.TrimSpace(m.cfg.SharedDataSource), "workspace validation failed: /data source"); err != nil {
		return err
	}
	if err := validateReadableDir(strings.TrimSpace(m.cfg.ContractsSource), "workspace validation failed: /opt/swarm/contracts source"); err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(strings.TrimSpace(m.cfg.ContractsSource), "package.yaml")); err != nil {
		return fmt.Errorf("workspace validation failed: contracts source %s missing package.yaml", strings.TrimSpace(m.cfg.ContractsSource))
	}
	root, err := m.hostRoot()
	if err != nil {
		return err
	}
	for _, source := range []struct {
		name string
		path string
	}{
		{name: "/data source", path: m.cfg.SharedDataSource},
		{name: "/opt/swarm/contracts source", path: m.cfg.ContractsSource},
	} {
		if pathsOverlap(root, source.path) {
			return fmt.Errorf("workspace validation failed: host workspace root %s must not overlap %s %s", root, source.name, filepath.Clean(source.path))
		}
	}
	return nil
}

func sanitizedHostPathComponents(raw string) []string {
	parts := strings.FieldsFunc(raw, func(r rune) bool {
		return r == '/' || r == '\\'
	})
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		slug := SanitizeSlug(part)
		if slug != "" {
			out = append(out, slug)
		}
	}
	return out
}

func defaultHostWorkspaceRoot() string {
	home, err := os.UserHomeDir()
	if err == nil && strings.TrimSpace(home) != "" {
		return filepath.Join(home, ".swarm", "workspaces")
	}
	return filepath.Join(os.TempDir(), "swarm", "workspaces")
}

func cleanAbsPath(path string, label string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", fmt.Errorf("%s is required", label)
	}
	if !filepath.IsAbs(path) {
		abs, err := filepath.Abs(path)
		if err != nil {
			return "", fmt.Errorf("resolve %s %s: %w", label, path, err)
		}
		path = abs
	}
	return filepath.Clean(path), nil
}

func pathsOverlap(a, b string) bool {
	cleanA, errA := cleanAbsPath(a, "path")
	cleanB, errB := cleanAbsPath(b, "path")
	if errA != nil || errB != nil {
		return false
	}
	return pathWithinRoot(cleanA, cleanB) || pathWithinRoot(cleanB, cleanA)
}

func pathWithinRoot(pathValue, root string) bool {
	pathValue = filepath.Clean(strings.TrimSpace(pathValue))
	root = filepath.Clean(strings.TrimSpace(root))
	if pathValue == "" || root == "" {
		return false
	}
	rel, err := filepath.Rel(root, pathValue)
	if err != nil {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator)))
}

func ensurePathWithinRoot(pathValue, root, label string) error {
	if !pathWithinRoot(pathValue, root) {
		return fmt.Errorf("%s path %s escapes root %s", label, filepath.Clean(pathValue), filepath.Clean(root))
	}
	return nil
}
