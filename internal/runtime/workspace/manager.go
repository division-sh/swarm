package workspace

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	runtimecontracts "swarm/internal/runtime/contracts"
	models "swarm/internal/runtime/core/actors"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/runtime/semanticview"
)

type Target struct {
	Container string
	Workdir   string
}

func (t *Target) Enabled() bool {
	return t != nil && strings.TrimSpace(t.Container) != ""
}

type Resolver interface {
	ResolveWorkspace(ctx context.Context, actor models.AgentConfig) (*Target, error)
}

type Lifecycle interface {
	Resolver
	ValidateSource(ctx context.Context, source semanticview.Source) error
	EnsurePrereqs(ctx context.Context) error
	EnsureSystemWorkspaces(ctx context.Context) error
	EnsureEntityWorkspace(ctx context.Context, entityID string) error
	StopEntityWorkspace(ctx context.Context, entityID string) error
}

type OrphanKiller interface {
	KillOrphanProcesses(ctx context.Context) error
}

type DockerConfig struct {
	DockerBin             string
	WorkspaceImage        string
	WorkspaceNetwork      string
	WorkspaceVolumesFrom  string
	SharedDataSource      string
	DataMountPoint        string
	ContractsSource       string
	ContractsMountPoint   string
	ScaffoldContainer     string
	ScaffoldWorkdir       string
	ScaffoldVolume        string
	SystemContainer       string
	SystemWorkdir         string
	SystemEntitiesVolume  string
	SystemNginxVolume     string
	SystemSystemdVolume   string
	EntityContainerPrefix string
	EntityWorkdir         string
}

func DefaultDockerConfig() DockerConfig {
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	return DockerConfig{
		DockerBin:             EnvOrDefault("SWARM_DOCKER_BIN", "docker"),
		WorkspaceImage:        EnvOrDefault("SWARM_WORKSPACE_IMAGE", "swarm-workspace:latest"),
		WorkspaceNetwork:      EnvOrDefault("SWARM_WORKSPACE_NETWORK", "mas_default"),
		WorkspaceVolumesFrom:  EnvOrDefault("SWARM_WORKSPACE_VOLUMES_FROM", ""),
		SharedDataSource:      EnvOrDefault("SWARM_WORKSPACE_DATA_SOURCE", filepath.Join(repoRoot, "data")),
		DataMountPoint:        EnvOrDefault("SWARM_WORKSPACE_DATA_MOUNT", "/data"),
		ContractsSource:       EnvOrDefault("SWARM_WORKSPACE_CONTRACTS_SOURCE", runtimecontracts.DefaultWorkflowContractsDir(repoRoot)),
		ContractsMountPoint:   EnvOrDefault("SWARM_WORKSPACE_CONTRACTS_MOUNT", "/opt/swarm/contracts"),
		ScaffoldContainer:     EnvOrDefault("SWARM_SCAFFOLD_CONTAINER", "swarm-scaffold"),
		ScaffoldWorkdir:       EnvOrDefault("SWARM_SCAFFOLD_WORKDIR", "/opt/swarm/scaffold"),
		ScaffoldVolume:        EnvOrDefault("SWARM_SCAFFOLD_VOLUME", "scaffold"),
		SystemContainer:       EnvOrDefault("SWARM_SYSTEM_CONTAINER", "swarm-system"),
		SystemWorkdir:         EnvOrDefault("SWARM_SYSTEM_WORKDIR", "/opt/swarm"),
		SystemEntitiesVolume:  EnvOrDefault("SWARM_SYSTEM_ENTITIES_VOLUME", "entities"),
		SystemNginxVolume:     EnvOrDefault("SWARM_SYSTEM_NGINX_VOLUME", "nginx"),
		SystemSystemdVolume:   EnvOrDefault("SWARM_SYSTEM_SYSTEMD_VOLUME", "systemd"),
		EntityContainerPrefix: EnvOrDefault("SWARM_ENTITY_CONTAINER_PREFIX", "swarm-"),
		EntityWorkdir:         EnvOrDefault("SWARM_ENTITY_WORKDIR", "/workspace"),
	}
}

type DockerManager struct {
	db          *sql.DB
	cfg         DockerConfig
	source      semanticview.Source
	RunDockerFn func(ctx context.Context, args ...string) (string, error) // test seam
}

const workspaceOrphanKillScript = `if command -v pkill >/dev/null 2>&1; then
  pkill -KILL -f '(^|/)(claude|codex)( |$)' >/dev/null 2>&1 || true
else
  for p in /proc/[0-9]*; do
    cmd=$(tr '\000' ' ' < "$p/cmdline" 2>/dev/null || true)
    case "$cmd" in
      *claude*|*codex*) kill -9 "${p##*/}" >/dev/null 2>&1 || true ;;
    esac
  done
fi`

func NewDockerManager(db *sql.DB) *DockerManager {
	return &DockerManager{
		db:  db,
		cfg: DefaultDockerConfig(),
	}
}

func (m *DockerManager) SetConfigForTest(cfg DockerConfig) {
	if m == nil {
		return
	}
	m.cfg = cfg
}

func (m *DockerManager) SetConfig(cfg DockerConfig) {
	if m == nil {
		return
	}
	m.cfg = cfg
}

func (m *DockerManager) SetSemanticSource(source semanticview.Source) {
	if m == nil {
		return
	}
	m.source = source
}

func (m *DockerManager) SetRunDockerFnForTest(runDockerFn func(ctx context.Context, args ...string) (string, error)) {
	if m == nil {
		return
	}
	m.RunDockerFn = runDockerFn
}

func (m *DockerManager) EnsureSystemWorkspaces(ctx context.Context) error {
	if err := m.EnsureContainerRunning(ctx, m.cfg.ScaffoldContainer, append(m.standardMountArgs(),
		[]string{
			"-v", fmt.Sprintf("%s:%s", m.cfg.ScaffoldVolume, m.cfg.ScaffoldWorkdir),
			"-w", m.cfg.ScaffoldWorkdir,
			m.cfg.WorkspaceImage,
			"sleep", "infinity",
		}...)); err != nil {
		return fmt.Errorf("ensure scaffold workspace: %w", err)
	}

	if err := m.EnsureContainerRunning(ctx, m.cfg.SystemContainer, append(m.standardMountArgs(),
		[]string{
			"--privileged",
			"-v", fmt.Sprintf("%s:/opt/swarm/entities", m.cfg.SystemEntitiesVolume),
			"-v", fmt.Sprintf("%s:/opt/swarm/nginx", m.cfg.SystemNginxVolume),
			"-v", fmt.Sprintf("%s:/etc/systemd/system", m.cfg.SystemSystemdVolume),
			"-w", m.cfg.SystemWorkdir,
			m.cfg.WorkspaceImage,
			"sleep", "infinity",
		}...)); err != nil {
		return fmt.Errorf("ensure system workspace: %w", err)
	}
	return nil
}

func (m *DockerManager) ValidateSource(ctx context.Context, source semanticview.Source) error {
	if m == nil {
		return fmt.Errorf("workspace manager is required")
	}
	if source == nil {
		return fmt.Errorf("workspace semantic source is required")
	}
	m.source = source
	if err := m.validateSharedMounts(ctx); err != nil {
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
	if strings.TrimSpace(m.cfg.WorkspaceImage) == "" {
		return fmt.Errorf("workspace validation failed: workspace image is required")
	}
	return nil
}

func (m *DockerManager) EnsurePrereqs(ctx context.Context) error {
	if m == nil {
		return fmt.Errorf("workspace manager is required")
	}
	if err := m.ensureDockerAvailable(ctx); err != nil {
		return err
	}
	if err := m.ensureWorkspaceNetwork(ctx); err != nil {
		return err
	}
	if err := m.ensureWorkspaceImage(ctx); err != nil {
		return err
	}
	return nil
}

func (m *DockerManager) EnsureEntityWorkspace(ctx context.Context, entityID string) error {
	slug, err := m.LookupEntitySlug(ctx, entityID)
	if err != nil {
		return err
	}
	if slug == "" {
		return fmt.Errorf("entity %s slug is required for workspace container", entityID)
	}
	container := m.EntityContainerName(slug)
	volume := fmt.Sprintf("entities_%s", slug)

	return m.EnsureContainerRunning(ctx, container, append(m.standardMountArgs(),
		[]string{
			"-v", fmt.Sprintf("%s:%s", volume, m.cfg.EntityWorkdir),
			"-w", m.cfg.EntityWorkdir,
			m.cfg.WorkspaceImage,
			"sleep", "infinity",
		}...))
}

func (m *DockerManager) StopEntityWorkspace(ctx context.Context, entityID string) error {
	slug, err := m.LookupEntitySlug(ctx, entityID)
	if err != nil {
		return err
	}
	if slug == "" {
		return nil
	}
	container := m.EntityContainerName(slug)
	if err := m.StopContainer(ctx, container); err != nil {
		return fmt.Errorf("stop entity workspace %s: %w", container, err)
	}
	return nil
}

func (m *DockerManager) KillOrphanProcesses(ctx context.Context) error {
	containers, err := m.RuntimeWorkspaceContainers(ctx)
	if err != nil {
		return err
	}
	errs := make([]error, 0, len(containers))
	for _, container := range containers {
		exists, running, inspectErr := m.InspectContainer(ctx, container)
		if inspectErr != nil {
			errs = append(errs, fmt.Errorf("inspect workspace container %s: %w", container, inspectErr))
			continue
		}
		if !exists || !running {
			continue
		}
		if _, execErr := m.RunDocker(ctx, "exec", container, "sh", "-lc", workspaceOrphanKillScript); execErr != nil {
			errs = append(errs, fmt.Errorf("kill workspace orphans in %s: %w", container, execErr))
		}
	}
	return errors.Join(errs...)
}

func (m *DockerManager) RuntimeWorkspaceContainers(ctx context.Context) ([]string, error) {
	set := map[string]struct{}{}
	for _, name := range []string{
		strings.TrimSpace(m.cfg.ScaffoldContainer),
		strings.TrimSpace(m.cfg.SystemContainer),
	} {
		if name != "" {
			set[name] = struct{}{}
		}
	}

	if m.db != nil {
		rows, err := m.db.QueryContext(ctx, `
			SELECT DISTINCT COALESCE(NULLIF(es.slug, ''), '')
			FROM entity_state es
			JOIN flow_instances fi ON fi.instance_id = es.flow_instance
			WHERE COALESCE(fi.config->>'instance_kind', '') = 'entity'
		`)
		if err != nil {
			return nil, fmt.Errorf("list instance slugs: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var slug string
			if scanErr := rows.Scan(&slug); scanErr != nil {
				return nil, fmt.Errorf("scan instance slug: %w", scanErr)
			}
			slug = SanitizeSlug(slug)
			if slug == "" {
				continue
			}
			set[m.EntityContainerName(slug)] = struct{}{}
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("iterate instance slugs: %w", err)
		}
	}

	out := make([]string, 0, len(set))
	for c := range set {
		out = append(out, c)
	}
	sort.Strings(out)
	return out, nil
}

func (m *DockerManager) ResolveWorkspace(ctx context.Context, actor models.AgentConfig) (*Target, error) {
	class, err := m.workspaceClass(actor)
	if err != nil {
		return nil, err
	}
	switch workspaceRouteClass(class) {
	case "scaffold":
		if err := m.EnsureContainerRunning(ctx, m.cfg.ScaffoldContainer, []string{
			"-v", fmt.Sprintf("%s:%s", m.cfg.ScaffoldVolume, m.cfg.ScaffoldWorkdir),
			"-w", m.cfg.ScaffoldWorkdir,
			m.cfg.WorkspaceImage,
			"sleep", "infinity",
		}); err != nil {
			return nil, err
		}
		return &Target{
			Container: m.cfg.ScaffoldContainer,
			Workdir:   m.cfg.ScaffoldWorkdir,
		}, nil
	case "system":
		if err := m.EnsureContainerRunning(ctx, m.cfg.SystemContainer, []string{
			"--privileged",
			"-v", fmt.Sprintf("%s:/opt/swarm/entities", m.cfg.SystemEntitiesVolume),
			"-v", fmt.Sprintf("%s:/opt/swarm/nginx", m.cfg.SystemNginxVolume),
			"-v", fmt.Sprintf("%s:/etc/systemd/system", m.cfg.SystemSystemdVolume),
			"-w", m.cfg.SystemWorkdir,
			m.cfg.WorkspaceImage,
			"sleep", "infinity",
		}); err != nil {
			return nil, err
		}
		return &Target{
			Container: m.cfg.SystemContainer,
			Workdir:   m.cfg.SystemWorkdir,
		}, nil
	}
	scope, scopeKey, err := m.workspaceScopeForActor(actor)
	if err != nil {
		return nil, err
	}
	container, volume := m.workspaceContainerAndVolume(scope, scopeKey)
	if err := m.EnsureContainerRunning(ctx, container, append(m.standardMountArgs(),
		[]string{
			"-v", fmt.Sprintf("%s:%s", volume, m.cfg.EntityWorkdir),
			"-w", m.cfg.EntityWorkdir,
			m.cfg.WorkspaceImage,
			"sleep", "infinity",
		}...)); err != nil {
		return nil, err
	}
	return &Target{
		Container: container,
		Workdir:   m.cfg.EntityWorkdir,
	}, nil
}

func (m *DockerManager) workspaceClass(actor models.AgentConfig) (string, error) {
	if cfgClass := strings.TrimSpace(actor.WorkspaceClass); cfgClass != "" {
		return cfgClass, nil
	}
	source := m.semanticSource()
	if source == nil {
		return "", nil
	}
	if _, entry, ok := semanticview.ResolveAgentRegistryEntry(source, actor); ok {
		return strings.TrimSpace(entry.WorkspaceClass), nil
	}
	return "", nil
}

func workspaceRouteClass(class string) string {
	switch strings.ToLower(strings.TrimSpace(class)) {
	case "scaffold":
		return "scaffold"
	case "system":
		return "system"
	default:
		return ""
	}
}

func (m *DockerManager) semanticSource() semanticview.Source {
	if m == nil {
		return nil
	}
	return m.source
}

func (m *DockerManager) workspaceScopeForActor(actor models.AgentConfig) (string, string, error) {
	class, err := m.workspaceClass(actor)
	if err != nil {
		return "", "", err
	}
	scope := "per-agent"
	if class != "" {
		source := m.semanticSource()
		if source == nil {
			return "", "", fmt.Errorf("workspace resolution failed: semantic source is required for workspace_class %q", class)
		}
		resolved, ok, err := workspaceClassScope(source, class)
		if err != nil {
			return "", "", err
		}
		if !ok {
			return "", "", fmt.Errorf("workspace resolution failed: workspace_class %q is not defined for agent %s", class, strings.TrimSpace(actor.ID))
		}
		scope = resolved
	}
	switch scope {
	case "per-agent":
		agentID := strings.TrimSpace(actor.ID)
		if agentID == "" {
			return "", "", fmt.Errorf("workspace resolution failed: per-agent workspace requires agent id")
		}
		return scope, agentID, nil
	case "per-flow-instance":
		flowPath := actor.CanonicalFlowPath()
		if flowPath == "" {
			return "", "", fmt.Errorf("workspace resolution failed: per-flow-instance workspace for agent %s requires flow_path", strings.TrimSpace(actor.ID))
		}
		return scope, flowPath, nil
	default:
		return "", "", fmt.Errorf("workspace resolution failed: unsupported workspace scope %q", scope)
	}
}

func (m *DockerManager) workspaceContainerAndVolume(scope, scopeKey string) (string, string) {
	scope = strings.TrimSpace(scope)
	scopeKey = SanitizeSlug(scopeKey)
	switch scope {
	case "per-flow-instance":
		return "swarm-flow-" + scopeKey, "workspaces_flow_" + scopeKey
	default:
		return "swarm-agent-" + scopeKey, "workspaces_agent_" + scopeKey
	}
}

func (m *DockerManager) standardMountArgs() []string {
	if m == nil {
		return nil
	}
	if volumesFrom := strings.TrimSpace(m.cfg.WorkspaceVolumesFrom); volumesFrom != "" {
		return []string{"--volumes-from", volumesFrom + ":ro"}
	}
	args := []string{}
	if source := strings.TrimSpace(m.cfg.SharedDataSource); source != "" {
		args = append(args, "-v", fmt.Sprintf("%s:%s:ro", source, strings.TrimSpace(m.cfg.DataMountPoint)))
	}
	if source := strings.TrimSpace(m.cfg.ContractsSource); source != "" {
		args = append(args, "-v", fmt.Sprintf("%s:%s:ro", source, strings.TrimSpace(m.cfg.ContractsMountPoint)))
	}
	return args
}

func (m *DockerManager) ensureDockerAvailable(ctx context.Context) error {
	if _, err := m.RunDocker(ctx, "version", "--format", "{{.Server.Version}}"); err != nil {
		return fmt.Errorf("workspace prerequisite failed: docker is not available: %w", err)
	}
	return nil
}

func (m *DockerManager) ensureWorkspaceNetwork(ctx context.Context) error {
	network := strings.TrimSpace(m.cfg.WorkspaceNetwork)
	if network == "" {
		return nil
	}
	if _, err := m.RunDocker(ctx, "network", "inspect", network); err == nil {
		return nil
	}
	if _, err := m.RunDocker(ctx, "network", "create", network); err != nil {
		return fmt.Errorf("workspace prerequisite failed: ensure network %s: %w", network, err)
	}
	return nil
}

func (m *DockerManager) ensureWorkspaceImage(ctx context.Context) error {
	image := strings.TrimSpace(m.cfg.WorkspaceImage)
	if image == "" {
		return fmt.Errorf("workspace prerequisite failed: workspace image is required")
	}
	if _, err := m.RunDocker(ctx, "image", "inspect", image); err == nil {
		return nil
	}
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	dockerfile := filepath.Join(repoRoot, "Dockerfile.workspace")
	if _, statErr := os.Stat(dockerfile); statErr != nil {
		return fmt.Errorf("workspace prerequisite failed: inspect image %s and Dockerfile.workspace is unavailable: %w", image, statErr)
	}
	if _, err := m.RunDocker(ctx, "build", "-t", image, "-f", dockerfile, repoRoot); err != nil {
		return fmt.Errorf("workspace prerequisite failed: build image %s: %w", image, err)
	}
	return nil
}

func (m *DockerManager) validateSharedMounts(ctx context.Context) error {
	if volumesFrom := strings.TrimSpace(m.cfg.WorkspaceVolumesFrom); volumesFrom != "" {
		mounts, err := m.inspectContainerMountDestinations(ctx, volumesFrom)
		if err != nil {
			return fmt.Errorf("workspace validation failed: inspect shared mounts from %s: %w", volumesFrom, err)
		}
		for _, required := range []string{strings.TrimSpace(m.cfg.DataMountPoint), strings.TrimSpace(m.cfg.ContractsMountPoint)} {
			if required == "" {
				continue
			}
			if _, ok := mounts[required]; !ok {
				return fmt.Errorf("workspace validation failed: shared mount source %s does not provide %s", volumesFrom, required)
			}
		}
		return nil
	}
	if err := validateReadableDir(strings.TrimSpace(m.cfg.SharedDataSource), "workspace validation failed: /data source"); err != nil {
		return err
	}
	if err := validateReadableDir(strings.TrimSpace(m.cfg.ContractsSource), "workspace validation failed: /opt/swarm/contracts source"); err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(strings.TrimSpace(m.cfg.ContractsSource), "package.yaml")); err != nil {
		return fmt.Errorf("workspace validation failed: contracts source %s missing package.yaml", strings.TrimSpace(m.cfg.ContractsSource))
	}
	return nil
}

func (m *DockerManager) inspectContainerMountDestinations(ctx context.Context, container string) (map[string]struct{}, error) {
	out, err := m.RunDocker(ctx, "inspect", "--format", "{{json .Mounts}}", strings.TrimSpace(container))
	if err != nil {
		return nil, err
	}
	var mounts []struct {
		Destination string `json:"Destination"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &mounts); err != nil {
		return nil, err
	}
	destinations := make(map[string]struct{}, len(mounts))
	for _, mount := range mounts {
		dest := strings.TrimSpace(mount.Destination)
		if dest != "" {
			destinations[dest] = struct{}{}
		}
	}
	return destinations, nil
}

func workspaceClassScope(source semanticview.Source, class string) (string, bool, error) {
	classes, err := workspaceClassesForSource(source)
	if err != nil {
		return "", false, err
	}
	scope, ok := classes[strings.TrimSpace(class)]
	return scope, ok, nil
}

func workspaceClassesForSource(source semanticview.Source) (map[string]string, error) {
	if source == nil {
		return map[string]string{}, nil
	}
	value, ok := semanticview.PolicyValueForFlow(source, "", "workspace_classes")
	if !ok {
		return map[string]string{}, nil
	}
	rawClasses, ok := normalizePolicyMap(value.Value)
	if !ok {
		return nil, fmt.Errorf("workspace_classes must be a mapping")
	}
	out := make(map[string]string, len(rawClasses))
	for className, rawClass := range rawClasses {
		className = strings.TrimSpace(className)
		if className == "" {
			continue
		}
		classMap, ok := normalizePolicyMap(rawClass)
		if !ok {
			return nil, fmt.Errorf("workspace_classes.%s must be a mapping", className)
		}
		scope := strings.TrimSpace(asString(classMap["workspace_scope"]))
		if !isSupportedWorkspaceScope(scope) {
			return nil, fmt.Errorf("workspace_classes.%s.workspace_scope must be per-agent or per-flow-instance", className)
		}
		out[className] = scope
	}
	return out, nil
}

func isSupportedWorkspaceScope(scope string) bool {
	switch strings.TrimSpace(scope) {
	case "per-agent", "per-flow-instance":
		return true
	default:
		return false
	}
}

func normalizePolicyMap(value any) (map[string]any, bool) {
	switch typed := value.(type) {
	case runtimecontracts.PolicyValue:
		return normalizePolicyMap(typed.Value)
	case map[string]any:
		return typed, true
	case map[any]any:
		out := make(map[string]any, len(typed))
		for key, val := range typed {
			out[strings.TrimSpace(asString(key))] = val
		}
		return out, true
	default:
		return nil, false
	}
}

func validateReadableDir(path, prefix string) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return fmt.Errorf("%s is not configured", prefix)
	}
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("%s %s: %w", prefix, path, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("%s %s is not a directory", prefix, path)
	}
	return nil
}

func asString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	default:
		return fmt.Sprint(v)
	}
}

func (m *DockerManager) EnsureContainerRunning(ctx context.Context, name string, createArgs []string) error {
	exists, running, err := m.InspectContainer(ctx, name)
	if err != nil {
		return err
	}
	if !exists {
		args := []string{"create", "--name", name}
		if network := strings.TrimSpace(m.cfg.WorkspaceNetwork); network != "" {
			args = append(args, "--network", network)
		}
		args = append(args, createArgs...)
		if _, err := m.RunDocker(ctx, args...); err != nil {
			return fmt.Errorf("create container %s: %w", name, err)
		}
		running = false
	}
	if running {
		if err := m.EnsureContainerNetwork(ctx, name); err != nil {
			return err
		}
		return nil
	}
	if _, err := m.RunDocker(ctx, "start", name); err != nil {
		// Another process may have started it between inspect/start.
		if !strings.Contains(strings.ToLower(err.Error()), "already running") {
			return fmt.Errorf("start container %s: %w", name, err)
		}
	}
	if err := m.EnsureContainerNetwork(ctx, name); err != nil {
		return err
	}
	return nil
}

func (m *DockerManager) EnsureContainerNetwork(ctx context.Context, name string) error {
	network := strings.TrimSpace(m.cfg.WorkspaceNetwork)
	if network == "" {
		return nil
	}
	if _, err := m.RunDocker(ctx, "network", "connect", network, name); err != nil {
		msg := strings.ToLower(strings.TrimSpace(err.Error()))
		if strings.Contains(msg, "already exists") ||
			strings.Contains(msg, "is already connected") ||
			strings.Contains(msg, "endpoint with name") {
			return nil
		}
		return fmt.Errorf("connect container %s to network %s: %w", name, network, err)
	}
	return nil
}

func (m *DockerManager) StopContainer(ctx context.Context, name string) error {
	exists, running, err := m.InspectContainer(ctx, name)
	if err != nil {
		return err
	}
	if !exists || !running {
		return nil
	}
	if _, err := m.RunDocker(ctx, "stop", name); err != nil {
		return err
	}
	return nil
}

func (m *DockerManager) InspectContainer(ctx context.Context, name string) (bool, bool, error) {
	out, err := m.RunDocker(ctx, "inspect", "--format", "{{.State.Running}}", name)
	if err != nil {
		msg := strings.ToLower(err.Error())
		if strings.Contains(msg, "no such object") {
			return false, false, nil
		}
		return false, false, err
	}
	running := strings.EqualFold(strings.TrimSpace(out), "true")
	return true, running, nil
}

func (m *DockerManager) RunDocker(ctx context.Context, args ...string) (string, error) {
	if m.RunDockerFn != nil {
		return m.RunDockerFn(ctx, args...)
	}
	cmd := exec.CommandContext(ctx, m.cfg.DockerBin, args...)
	raw, err := cmd.CombinedOutput()
	out := strings.TrimSpace(string(raw))
	if err != nil {
		if out == "" {
			return "", fmt.Errorf("%w", err)
		}
		return "", fmt.Errorf("%w: %s", err, out)
	}
	return out, nil
}

func (m *DockerManager) LookupEntitySlug(ctx context.Context, entityID string) (string, error) {
	trimmedID := strings.TrimSpace(entityID)
	if trimmedID == "" {
		return "", errors.New("entity_id is required")
	}
	if m.db == nil {
		return SanitizeSlug(trimmedID), nil
	}
	var slug string
	if err := m.db.QueryRowContext(ctx, `
		SELECT COALESCE(NULLIF(slug, ''), '')
		FROM entity_state
		WHERE entity_id = $1::uuid
	`, trimmedID).Scan(&slug); err != nil {
		return "", fmt.Errorf("lookup instance slug: %w", err)
	}
	slug = SanitizeSlug(slug)
	if slug == "" {
		return SanitizeSlug(trimmedID), nil
	}
	return slug, nil
}

func (m *DockerManager) EntityContainerName(slug string) string {
	return m.cfg.EntityContainerPrefix + SanitizeSlug(slug)
}

func SanitizeSlug(raw string) string {
	raw = strings.ToLower(strings.TrimSpace(raw))
	if raw == "" {
		return ""
	}
	var b strings.Builder
	lastDash := false
	for _, r := range raw {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
			lastDash = false
		case r >= '0' && r <= '9':
			b.WriteRune(r)
			lastDash = false
		case r == '-' || r == '_' || r == ' ' || r == '/':
			if !lastDash {
				b.WriteRune('-')
				lastDash = true
			}
		}
	}
	out := strings.Trim(b.String(), "-")
	if out == "" {
		return ""
	}
	return out
}

func EnvOrDefault(key, fallback string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	return v
}
