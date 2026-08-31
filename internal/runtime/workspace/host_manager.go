package workspace

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	runtimedataaccess "github.com/division-sh/swarm/internal/runtime/dataaccess"
	runtimedestructivereset "github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/sourceartifact"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
)

type HostConfig struct {
	WorkspaceRoot      string
	SharedDataSource   string
	DataMountPoint     string
	SourceProjection   *sourceartifact.RuntimeProjection
	SourceMountPoint   string
	BundleHash         string
	BundleScope        string
	SourceProjectionID string
}

type HostManager struct {
	cfg             HostConfig
	source          semanticview.Source
	data            runtimedataaccess.Provider
	ownedProjection *sourceartifact.RuntimeProjection
}

func DefaultHostConfig() HostConfig {
	return HostConfig{
		WorkspaceRoot:    defaultHostWorkspaceRoot(),
		SharedDataSource: "",
		DataMountPoint:   "/data",
		SourceMountPoint: "/opt/swarm/source",
	}
}

func NewHostManager() *HostManager {
	return &HostManager{
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

func (m *HostManager) SetDataProjectionProvider(provider runtimedataaccess.Provider) {
	if m != nil {
		m.data = provider
	}
}

func (m *HostManager) RebindSourceProjection(projection *sourceartifact.RuntimeProjection, source semanticview.Source) (Lifecycle, error) {
	if m == nil {
		return nil, fmt.Errorf("host workspace manager is required")
	}
	if source == nil {
		return nil, fmt.Errorf("workspace semantic source is required")
	}
	if projection == nil {
		return nil, fmt.Errorf("workspace validation failed: runtime source projection is required")
	}
	if _, err := validateSourceProjection(projection, projection.BundleHash()); err != nil {
		return nil, err
	}
	cfg := m.cfg
	cfg.SourceProjection = projection
	cfg.BundleHash = ""
	cfg.BundleScope = ""
	clone := NewHostManager()
	clone.SetConfig(cfg)
	clone.SetSemanticSource(source)
	clone.SetDataProjectionProvider(m.data)
	if err := clone.BindSourceProjection(projection); err != nil {
		return nil, err
	}
	return clone, nil
}

func (m *HostManager) SourceProjectionBundleHash() string {
	if m == nil || m.cfg.SourceProjection == nil {
		return ""
	}
	return m.cfg.SourceProjection.BundleHash()
}

func (m *HostManager) SourceProjectionIdentity() string {
	if m == nil {
		return ""
	}
	return strings.TrimSpace(m.cfg.SourceProjectionID)
}

func (m *HostManager) BindSourceProjection(projection *sourceartifact.RuntimeProjection) error {
	if m == nil {
		return fmt.Errorf("host workspace manager is required")
	}
	if projection == nil {
		return fmt.Errorf("runtime source projection is required")
	}
	if _, err := validateSourceProjection(projection, projection.BundleHash()); err != nil {
		return err
	}
	if strings.TrimSpace(projection.Identity()) == "" {
		return fmt.Errorf("runtime source projection identity is required")
	}
	if m.ownedProjection != nil {
		return fmt.Errorf("host workspace source projection is already bound")
	}
	ownedProjection, err := projection.Retain()
	if err != nil {
		return fmt.Errorf("retain runtime source projection: %w", err)
	}
	cfg := m.cfg
	cfg.SourceProjection = projection
	cfg.BundleHash = strings.TrimSpace(projection.BundleHash())
	cfg.SourceProjectionID = strings.TrimSpace(projection.Identity())
	cfg.BundleScope = projectionScopeKey(cfg.BundleHash, cfg.SourceProjectionID)
	m.cfg = cfg
	m.ownedProjection = ownedProjection
	return nil
}

func (m *HostManager) ReleaseSourceProjection(context.Context) error {
	if m == nil || m.ownedProjection == nil {
		return nil
	}
	projection := m.ownedProjection
	m.ownedProjection = nil
	return projection.Release()
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
	if err := validateAgentWorkspaceClasses(source, classes); err != nil {
		return err
	}
	return nil
}

func (m *HostManager) EnsurePrereqs(context.Context) error {
	if m == nil {
		return fmt.Errorf("host workspace manager is required")
	}
	if err := m.validateSharedMounts(); err != nil {
		return err
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

func (m *HostManager) ResolveWorkspace(ctx context.Context, actor models.AgentConfig) (*Target, error) {
	return m.resolveWorkspace(ctx, actor, true)
}

func (m *HostManager) ResolveWorkspaceForCapabilityAdmission(ctx context.Context, actor models.AgentConfig) (*Target, error) {
	return m.resolveWorkspace(ctx, actor, false)
}

func (m *HostManager) resolveWorkspace(ctx context.Context, actor models.AgentConfig, materializeData bool) (*Target, error) {
	if m == nil {
		return nil, fmt.Errorf("host workspace manager is required")
	}
	class, err := workspaceClassForSource(m.source, actor)
	if err != nil {
		return nil, err
	}
	switch workspaceRouteClass(class) {
	case "scaffold":
		return m.hostTarget("scaffold", "")
	case "system":
		return m.hostTarget("system", "")
	}
	dataRoot := ""
	if materializeData && m.data != nil {
		projection, err := m.data.Materialize(ctx, actor)
		if err != nil {
			return nil, fmt.Errorf("materialize workspace data projection: %w", err)
		}
		if err := projection.Validate(); err != nil {
			return nil, fmt.Errorf("materialize workspace data projection: %w", err)
		}
		dataRoot = projection.Root
	}
	scope, scopeKey, err := workspaceScopeForActor(m.source, actor)
	if err != nil {
		return nil, err
	}
	switch scope {
	case "per-flow-instance":
		return m.hostTarget(filepath.Join("flows", SanitizeSlug(scopeKey)), dataRoot)
	default:
		return m.hostTarget(filepath.Join("agents", SanitizeSlug(scopeKey)), dataRoot)
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

func (m *HostManager) hostTarget(rel, dataRoot string) (*Target, error) {
	workdir, err := m.ensureHostWorkspaceDir(rel)
	if err != nil {
		return nil, err
	}
	return &Target{
		Workdir: workdir,
		Backend: BackendHost,
		Mounts:  m.hostExecutionMounts(workdir, dataRoot),
	}, nil
}

func (m *HostManager) hostExecutionMounts(workdir, dataRoot string) []ExecutionMount {
	dataMount := strings.TrimSpace(m.cfg.DataMountPoint)
	if dataMount == "" {
		dataMount = LogicalDataMount
	}
	sourceMount := strings.TrimSpace(m.cfg.SourceMountPoint)
	if sourceMount == "" {
		sourceMount = LogicalSourceMount
	}
	sourceProjectionPath, _ := validateSourceProjection(m.cfg.SourceProjection, m.cfg.BundleHash)
	out := []ExecutionMount{
		{LogicalPath: LogicalWorkspaceMount, HostPath: strings.TrimSpace(workdir), Access: MountAccessReadWrite},
		{LogicalPath: sourceMount, HostPath: sourceProjectionPath, Access: MountAccessReadOnly},
	}
	if strings.TrimSpace(dataRoot) != "" {
		out = append(out, ExecutionMount{LogicalPath: dataMount, HostPath: strings.TrimSpace(dataRoot), Access: MountAccessReadOnly})
	}
	return out
}

func (m *HostManager) ensureHostWorkspaceDir(rel string) (string, error) {
	if err := m.validateSharedMounts(); err != nil {
		return "", err
	}
	root, err := m.hostRoot()
	if err != nil {
		return "", err
	}
	components := sanitizedHostPathComponents(rel)
	if len(components) == 0 {
		return "", fmt.Errorf("host workspace path is required")
	}
	path := filepath.Join(append([]string{root}, components...)...)
	canonicalPath, err := canonicalPathForOverlap(path, "host workspace")
	if err != nil {
		return "", err
	}
	if err := ensurePathWithinRoot(canonicalPath, root, "host workspace"); err != nil {
		return "", err
	}
	if err := os.MkdirAll(canonicalPath, 0o700); err != nil {
		return "", fmt.Errorf("create host workspace %s: %w", canonicalPath, err)
	}
	return canonicalPath, nil
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
	return canonicalPathForOverlap(root, "host workspace root")
}

func (m *HostManager) validateSharedMounts() error {
	if strings.TrimSpace(m.cfg.SharedDataSource) != "" {
		return fmt.Errorf("workspace.data_source is retired; declare flow_data_access or data_access")
	}
	sourceProjectionPath, err := validateSourceProjection(m.cfg.SourceProjection, m.cfg.BundleHash)
	if err != nil {
		return err
	}
	root, err := m.hostRoot()
	if err != nil {
		return err
	}
	for _, source := range []struct {
		name string
		path string
	}{{name: "/opt/swarm/source projection", path: sourceProjectionPath}} {
		if pathsOverlap(root, source.path) {
			return fmt.Errorf("workspace validation failed: host workspace root %s must not overlap %s %s", root, source.name, source.path)
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

func canonicalReadableDir(path, prefix string) (string, error) {
	canonicalPath, err := canonicalPathForOverlap(path, prefix)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(canonicalPath)
	if err != nil {
		return "", fmt.Errorf("%s %s: %w", prefix, canonicalPath, err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("%s %s is not a directory", prefix, canonicalPath)
	}
	return canonicalPath, nil
}

func canonicalPathForOverlap(path, label string) (string, error) {
	clean, err := cleanAbsPath(path, label)
	if err != nil {
		return "", err
	}
	current := clean
	missing := []string{}
	for {
		info, statErr := os.Lstat(current)
		if statErr == nil {
			if info.Mode()&os.ModeSymlink != 0 {
				resolved, err := filepath.EvalSymlinks(current)
				if err != nil {
					return "", fmt.Errorf("resolve %s symlink %s: %w", label, current, err)
				}
				return filepath.Clean(filepath.Join(append([]string{resolved}, missing...)...)), nil
			}
			resolved, err := filepath.EvalSymlinks(current)
			if err != nil {
				return "", fmt.Errorf("resolve %s %s: %w", label, current, err)
			}
			return filepath.Clean(filepath.Join(append([]string{resolved}, missing...)...)), nil
		}
		if !os.IsNotExist(statErr) {
			return "", fmt.Errorf("inspect %s %s: %w", label, current, statErr)
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", fmt.Errorf("resolve %s %s: no existing ancestor", label, clean)
		}
		missing = append([]string{filepath.Base(current)}, missing...)
		current = parent
	}
}

func pathsOverlap(a, b string) bool {
	cleanA, errA := canonicalPathForOverlap(a, "path")
	cleanB, errB := canonicalPathForOverlap(b, "path")
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
