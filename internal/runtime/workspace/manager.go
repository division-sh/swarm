package workspace

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"

	runtimecontracts "empireai/internal/runtime/contracts"
	models "empireai/internal/runtime/core/actors"
	runtimepipeline "empireai/internal/runtime/pipeline"
	"empireai/internal/runtime/semanticview"
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
	return DockerConfig{
		DockerBin:             EnvOrDefault("MAS_DOCKER_BIN", "docker"),
		WorkspaceImage:        EnvOrDefault("MAS_WORKSPACE_IMAGE", "mas-workspace:latest"),
		WorkspaceNetwork:      EnvOrDefault("MAS_WORKSPACE_NETWORK", "mas_default"),
		ScaffoldContainer:     EnvOrDefault("MAS_SCAFFOLD_CONTAINER", "mas-scaffold"),
		ScaffoldWorkdir:       EnvOrDefault("MAS_SCAFFOLD_WORKDIR", "/opt/mas/scaffold"),
		ScaffoldVolume:        EnvOrDefault("MAS_SCAFFOLD_VOLUME", "scaffold"),
		SystemContainer:       EnvOrDefault("MAS_SYSTEM_CONTAINER", "mas-system"),
		SystemWorkdir:         EnvOrDefault("MAS_SYSTEM_WORKDIR", "/opt/mas"),
		SystemEntitiesVolume:  EnvOrDefault("MAS_SYSTEM_ENTITIES_VOLUME", "entities"),
		SystemNginxVolume:     EnvOrDefault("MAS_SYSTEM_NGINX_VOLUME", "nginx"),
		SystemSystemdVolume:   EnvOrDefault("MAS_SYSTEM_SYSTEMD_VOLUME", "systemd"),
		EntityContainerPrefix: EnvOrDefault("MAS_ENTITY_CONTAINER_PREFIX", "mas-"),
		EntityWorkdir:         EnvOrDefault("MAS_ENTITY_WORKDIR", "/workspace"),
	}
}

type DockerManager struct {
	db          *sql.DB
	cfg         DockerConfig
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

func (m *DockerManager) SetRunDockerFnForTest(runDockerFn func(ctx context.Context, args ...string) (string, error)) {
	if m == nil {
		return
	}
	m.RunDockerFn = runDockerFn
}

func (m *DockerManager) EnsureSystemWorkspaces(ctx context.Context) error {
	if err := m.EnsureContainerRunning(ctx, m.cfg.ScaffoldContainer, []string{
		"-v", fmt.Sprintf("%s:%s", m.cfg.ScaffoldVolume, m.cfg.ScaffoldWorkdir),
		"-w", m.cfg.ScaffoldWorkdir,
		m.cfg.WorkspaceImage,
		"sleep", "infinity",
	}); err != nil {
		return fmt.Errorf("ensure scaffold workspace: %w", err)
	}

	if err := m.EnsureContainerRunning(ctx, m.cfg.SystemContainer, []string{
		"--privileged",
		"-v", fmt.Sprintf("%s:/opt/mas/entities", m.cfg.SystemEntitiesVolume),
		"-v", fmt.Sprintf("%s:/opt/mas/nginx", m.cfg.SystemNginxVolume),
		"-v", fmt.Sprintf("%s:/etc/systemd/system", m.cfg.SystemSystemdVolume),
		"-w", m.cfg.SystemWorkdir,
		m.cfg.WorkspaceImage,
		"sleep", "infinity",
	}); err != nil {
		return fmt.Errorf("ensure system workspace: %w", err)
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

	return m.EnsureContainerRunning(ctx, container, []string{
		"-v", fmt.Sprintf("%s:%s", volume, m.cfg.EntityWorkdir),
		"-w", m.cfg.EntityWorkdir,
		m.cfg.WorkspaceImage,
		"sleep", "infinity",
	})
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
	switch workspaceRouteClass(WorkspaceClass(actor)) {
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
			"-v", fmt.Sprintf("%s:/opt/mas/entities", m.cfg.SystemEntitiesVolume),
			"-v", fmt.Sprintf("%s:/opt/mas/nginx", m.cfg.SystemNginxVolume),
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

	entityID := actor.EffectiveEntityID()
	if entityID == "" {
		return nil, nil
	}
	if err := m.EnsureEntityWorkspace(ctx, entityID); err != nil {
		return nil, err
	}
	slug, err := m.LookupEntitySlug(ctx, entityID)
	if err != nil {
		return nil, err
	}
	return &Target{
		Container: m.EntityContainerName(slug),
		Workdir:   m.cfg.EntityWorkdir,
	}, nil
}

func WorkspaceClass(actor models.AgentConfig) string {
	if cfgClass := configString(actor.Config, "workspace_class"); cfgClass != "" {
		return cfgClass
	}
	if source := runtimepipeline.DefaultWorkflowSemanticSourceOrNil(); source != nil {
		if entry, ok := workflowAgentRegistryEntry(source, actor.ID, actor.Role); ok {
			return strings.TrimSpace(entry.WorkspaceClass)
		}
	}
	return ""
}

func RoleWorkspaceClass(role string) string {
	role = strings.TrimSpace(role)
	if role == "" {
		return ""
	}
	if source := runtimepipeline.DefaultWorkflowSemanticSourceOrNil(); source != nil {
		if entry, ok := workflowAgentRegistryEntry(source, "", role); ok {
			return strings.TrimSpace(entry.WorkspaceClass)
		}
	}
	return ""
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

func RoleWorkspaceRouteClass(role string) string {
	return workspaceRouteClass(RoleWorkspaceClass(role))
}

func workflowAgentRegistryEntry(source semanticview.Source, agentID, role string) (runtimecontracts.AgentRegistryEntry, bool) {
	entry, ok := semanticview.FindAgentEntry(source, agentID, role)
	return entry, ok
}

func configString(raw []byte, key string) string {
	key = strings.TrimSpace(key)
	if key == "" || len(raw) == 0 || !json.Valid(raw) {
		return ""
	}
	var parsed map[string]any
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return ""
	}
	return strings.TrimSpace(asString(parsed[key]))
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
