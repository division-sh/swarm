package workspace

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"

	"empireai/internal/models"
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
	EnsureVerticalWorkspace(ctx context.Context, verticalID string) error
	StopVerticalWorkspace(ctx context.Context, verticalID string) error
}

type OrphanKiller interface {
	KillOrphanProcesses(ctx context.Context) error
}

type DockerConfig struct {
	DockerBin               string
	WorkspaceImage          string
	WorkspaceNetwork        string
	FactoryContainer        string
	FactoryWorkdir          string
	FactoryVolume           string
	InfraContainer          string
	InfraWorkdir            string
	InfraVerticalsVolume    string
	InfraNginxVolume        string
	InfraSystemdVolume      string
	VerticalContainerPrefix string
	VerticalWorkdir         string
}

func DefaultDockerConfig() DockerConfig {
	return DockerConfig{
		DockerBin:               EnvOrDefault("EMPIREAI_DOCKER_BIN", "docker"),
		WorkspaceImage:          EnvOrDefault("EMPIREAI_WORKSPACE_IMAGE", "empireai-workspace:latest"),
		WorkspaceNetwork:        EnvOrDefault("EMPIREAI_WORKSPACE_NETWORK", "empireai_default"),
		FactoryContainer:        EnvOrDefault("EMPIREAI_FACTORY_CONTAINER", "empireai-factory"),
		FactoryWorkdir:          EnvOrDefault("EMPIREAI_FACTORY_WORKDIR", "/opt/empireai/scaffold"),
		FactoryVolume:           EnvOrDefault("EMPIREAI_FACTORY_VOLUME", "scaffold"),
		InfraContainer:          EnvOrDefault("EMPIREAI_INFRA_CONTAINER", "empireai-infra"),
		InfraWorkdir:            EnvOrDefault("EMPIREAI_INFRA_WORKDIR", "/opt/empireai"),
		InfraVerticalsVolume:    EnvOrDefault("EMPIREAI_INFRA_VERTICALS_VOLUME", "verticals"),
		InfraNginxVolume:        EnvOrDefault("EMPIREAI_INFRA_NGINX_VOLUME", "nginx"),
		InfraSystemdVolume:      EnvOrDefault("EMPIREAI_INFRA_SYSTEMD_VOLUME", "systemd"),
		VerticalContainerPrefix: EnvOrDefault("EMPIREAI_VERTICAL_CONTAINER_PREFIX", "empireai-"),
		VerticalWorkdir:         EnvOrDefault("EMPIREAI_VERTICAL_WORKDIR", "/workspace"),
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
	if err := m.EnsureContainerRunning(ctx, m.cfg.FactoryContainer, []string{
		"-v", fmt.Sprintf("%s:%s", m.cfg.FactoryVolume, m.cfg.FactoryWorkdir),
		"-w", m.cfg.FactoryWorkdir,
		m.cfg.WorkspaceImage,
		"sleep", "infinity",
	}); err != nil {
		return fmt.Errorf("ensure factory workspace: %w", err)
	}

	if err := m.EnsureContainerRunning(ctx, m.cfg.InfraContainer, []string{
		"--privileged",
		"-v", fmt.Sprintf("%s:/opt/empireai/verticals", m.cfg.InfraVerticalsVolume),
		"-v", fmt.Sprintf("%s:/opt/empireai/nginx", m.cfg.InfraNginxVolume),
		"-v", fmt.Sprintf("%s:/etc/systemd/system", m.cfg.InfraSystemdVolume),
		"-w", m.cfg.InfraWorkdir,
		m.cfg.WorkspaceImage,
		"sleep", "infinity",
	}); err != nil {
		return fmt.Errorf("ensure infra workspace: %w", err)
	}
	return nil
}

func (m *DockerManager) EnsureVerticalWorkspace(ctx context.Context, verticalID string) error {
	slug, err := m.LookupVerticalSlug(ctx, verticalID)
	if err != nil {
		return err
	}
	if slug == "" {
		return fmt.Errorf("vertical %s slug is required for workspace container", verticalID)
	}
	container := m.VerticalContainerName(slug)
	volume := fmt.Sprintf("verticals_%s", slug)

	return m.EnsureContainerRunning(ctx, container, []string{
		"-v", fmt.Sprintf("%s:%s", volume, m.cfg.VerticalWorkdir),
		"-w", m.cfg.VerticalWorkdir,
		m.cfg.WorkspaceImage,
		"sleep", "infinity",
	})
}

func (m *DockerManager) StopVerticalWorkspace(ctx context.Context, verticalID string) error {
	slug, err := m.LookupVerticalSlug(ctx, verticalID)
	if err != nil {
		return err
	}
	if slug == "" {
		return nil
	}
	container := m.VerticalContainerName(slug)
	if err := m.StopContainer(ctx, container); err != nil {
		return fmt.Errorf("stop vertical workspace %s: %w", container, err)
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
		strings.TrimSpace(m.cfg.FactoryContainer),
		strings.TrimSpace(m.cfg.InfraContainer),
	} {
		if name != "" {
			set[name] = struct{}{}
		}
	}

	if m.db != nil {
		rows, err := m.db.QueryContext(ctx, `SELECT COALESCE(NULLIF(slug, ''), '') FROM verticals`)
		if err != nil {
			return nil, fmt.Errorf("list vertical slugs: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var slug string
			if scanErr := rows.Scan(&slug); scanErr != nil {
				return nil, fmt.Errorf("scan vertical slug: %w", scanErr)
			}
			slug = SanitizeSlug(slug)
			if slug == "" {
				continue
			}
			set[m.VerticalContainerName(slug)] = struct{}{}
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("iterate vertical slugs: %w", err)
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
	role := strings.TrimSpace(strings.ToLower(actor.Role))
	switch role {
	case "factory-cto":
		if err := m.EnsureContainerRunning(ctx, m.cfg.FactoryContainer, []string{
			"-v", fmt.Sprintf("%s:%s", m.cfg.FactoryVolume, m.cfg.FactoryWorkdir),
			"-w", m.cfg.FactoryWorkdir,
			m.cfg.WorkspaceImage,
			"sleep", "infinity",
		}); err != nil {
			return nil, err
		}
		return &Target{
			Container: m.cfg.FactoryContainer,
			Workdir:   m.cfg.FactoryWorkdir,
		}, nil
	case "holding-devops":
		if err := m.EnsureContainerRunning(ctx, m.cfg.InfraContainer, []string{
			"--privileged",
			"-v", fmt.Sprintf("%s:/opt/empireai/verticals", m.cfg.InfraVerticalsVolume),
			"-v", fmt.Sprintf("%s:/opt/empireai/nginx", m.cfg.InfraNginxVolume),
			"-v", fmt.Sprintf("%s:/etc/systemd/system", m.cfg.InfraSystemdVolume),
			"-w", m.cfg.InfraWorkdir,
			m.cfg.WorkspaceImage,
			"sleep", "infinity",
		}); err != nil {
			return nil, err
		}
		return &Target{
			Container: m.cfg.InfraContainer,
			Workdir:   m.cfg.InfraWorkdir,
		}, nil
	case "empire-coordinator",
		"operations-analyst",
		"scanner-agent",
		"analysis-agent",
		"validation-coordinator",
		"pre-brand-agent",
		"business-research-agent",
		"lightweight-spec-agent",
		"spec-reviewer",
		"market-research-agent",
		"trend-research-agent",
		"spec-auditor",
		"discovery-coordinator":
		// Global agents still need a workspace with the Claude/Codex CLIs available.
		// For now we colocate them in the non-privileged factory workspace.
		if err := m.EnsureContainerRunning(ctx, m.cfg.FactoryContainer, []string{
			"-v", fmt.Sprintf("%s:%s", m.cfg.FactoryVolume, m.cfg.FactoryWorkdir),
			"-w", m.cfg.FactoryWorkdir,
			m.cfg.WorkspaceImage,
			"sleep", "infinity",
		}); err != nil {
			return nil, err
		}
		return &Target{
			Container: m.cfg.FactoryContainer,
			Workdir:   m.cfg.FactoryWorkdir,
		}, nil
	}

	if strings.TrimSpace(actor.VerticalID) == "" {
		return nil, nil
	}
	if err := m.EnsureVerticalWorkspace(ctx, actor.VerticalID); err != nil {
		return nil, err
	}
	slug, err := m.LookupVerticalSlug(ctx, actor.VerticalID)
	if err != nil {
		return nil, err
	}
	return &Target{
		Container: m.VerticalContainerName(slug),
		Workdir:   m.cfg.VerticalWorkdir,
	}, nil
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

func (m *DockerManager) LookupVerticalSlug(ctx context.Context, verticalID string) (string, error) {
	trimmedID := strings.TrimSpace(verticalID)
	if trimmedID == "" {
		return "", errors.New("vertical_id is required")
	}
	if m.db == nil {
		return SanitizeSlug(trimmedID), nil
	}
	var slug string
	if err := m.db.QueryRowContext(ctx, `
		SELECT COALESCE(NULLIF(slug, ''), '')
		FROM verticals
		WHERE id = $1::uuid
	`, trimmedID).Scan(&slug); err != nil {
		return "", fmt.Errorf("lookup vertical slug: %w", err)
	}
	slug = SanitizeSlug(slug)
	if slug == "" {
		return "", fmt.Errorf("vertical %s has no slug", trimmedID)
	}
	return slug, nil
}

func (m *DockerManager) VerticalContainerName(slug string) string {
	return m.cfg.VerticalContainerPrefix + SanitizeSlug(slug)
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
