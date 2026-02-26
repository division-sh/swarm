package runtime

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"empireai/internal/models"
)

type WorkspaceTarget struct {
	Container string
	Workdir   string
}

func (t *WorkspaceTarget) Enabled() bool {
	return t != nil && strings.TrimSpace(t.Container) != ""
}

type WorkspaceResolver interface {
	ResolveWorkspace(ctx context.Context, actor models.AgentConfig) (*WorkspaceTarget, error)
}

type WorkspaceLifecycle interface {
	WorkspaceResolver
	EnsureSystemWorkspaces(ctx context.Context) error
	EnsureVerticalWorkspace(ctx context.Context, verticalID string) error
	StopVerticalWorkspace(ctx context.Context, verticalID string) error
}

type DockerWorkspaceConfig struct {
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

func defaultDockerWorkspaceConfig() DockerWorkspaceConfig {
	return DockerWorkspaceConfig{
		DockerBin:               envOrDefault("EMPIREAI_DOCKER_BIN", "docker"),
		WorkspaceImage:          envOrDefault("EMPIREAI_WORKSPACE_IMAGE", "empireai-workspace:latest"),
		WorkspaceNetwork:        envOrDefault("EMPIREAI_WORKSPACE_NETWORK", "empireai_default"),
		FactoryContainer:        envOrDefault("EMPIREAI_FACTORY_CONTAINER", "empireai-factory"),
		FactoryWorkdir:          envOrDefault("EMPIREAI_FACTORY_WORKDIR", "/opt/empireai/scaffold"),
		FactoryVolume:           envOrDefault("EMPIREAI_FACTORY_VOLUME", "scaffold"),
		InfraContainer:          envOrDefault("EMPIREAI_INFRA_CONTAINER", "empireai-infra"),
		InfraWorkdir:            envOrDefault("EMPIREAI_INFRA_WORKDIR", "/opt/empireai"),
		InfraVerticalsVolume:    envOrDefault("EMPIREAI_INFRA_VERTICALS_VOLUME", "verticals"),
		InfraNginxVolume:        envOrDefault("EMPIREAI_INFRA_NGINX_VOLUME", "nginx"),
		InfraSystemdVolume:      envOrDefault("EMPIREAI_INFRA_SYSTEMD_VOLUME", "systemd"),
		VerticalContainerPrefix: envOrDefault("EMPIREAI_VERTICAL_CONTAINER_PREFIX", "empireai-"),
		VerticalWorkdir:         envOrDefault("EMPIREAI_VERTICAL_WORKDIR", "/workspace"),
	}
}

type DockerWorkspaceManager struct {
	db          *sql.DB
	cfg         DockerWorkspaceConfig
	runDockerFn func(ctx context.Context, args ...string) (string, error) // test seam
}

func NewDockerWorkspaceManager(db *sql.DB) *DockerWorkspaceManager {
	return &DockerWorkspaceManager{
		db:  db,
		cfg: defaultDockerWorkspaceConfig(),
	}
}

func (m *DockerWorkspaceManager) EnsureSystemWorkspaces(ctx context.Context) error {
	if err := m.ensureContainerRunning(ctx, m.cfg.FactoryContainer, []string{
		"-v", fmt.Sprintf("%s:%s", m.cfg.FactoryVolume, m.cfg.FactoryWorkdir),
		"-w", m.cfg.FactoryWorkdir,
		m.cfg.WorkspaceImage,
		"sleep", "infinity",
	}); err != nil {
		return fmt.Errorf("ensure factory workspace: %w", err)
	}

	if err := m.ensureContainerRunning(ctx, m.cfg.InfraContainer, []string{
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

func (m *DockerWorkspaceManager) EnsureVerticalWorkspace(ctx context.Context, verticalID string) error {
	slug, err := m.lookupVerticalSlug(ctx, verticalID)
	if err != nil {
		return err
	}
	if slug == "" {
		return fmt.Errorf("vertical %s slug is required for workspace container", verticalID)
	}
	container := m.verticalContainerName(slug)
	volume := fmt.Sprintf("verticals_%s", slug)

	return m.ensureContainerRunning(ctx, container, []string{
		"-v", fmt.Sprintf("%s:%s", volume, m.cfg.VerticalWorkdir),
		"-w", m.cfg.VerticalWorkdir,
		m.cfg.WorkspaceImage,
		"sleep", "infinity",
	})
}

func (m *DockerWorkspaceManager) StopVerticalWorkspace(ctx context.Context, verticalID string) error {
	slug, err := m.lookupVerticalSlug(ctx, verticalID)
	if err != nil {
		return err
	}
	if slug == "" {
		return nil
	}
	container := m.verticalContainerName(slug)
	if err := m.stopContainer(ctx, container); err != nil {
		return fmt.Errorf("stop vertical workspace %s: %w", container, err)
	}
	return nil
}

func (m *DockerWorkspaceManager) ResolveWorkspace(ctx context.Context, actor models.AgentConfig) (*WorkspaceTarget, error) {
	role := strings.TrimSpace(strings.ToLower(actor.Role))
	switch role {
	case "factory-cto":
		if err := m.ensureContainerRunning(ctx, m.cfg.FactoryContainer, []string{
			"-v", fmt.Sprintf("%s:%s", m.cfg.FactoryVolume, m.cfg.FactoryWorkdir),
			"-w", m.cfg.FactoryWorkdir,
			m.cfg.WorkspaceImage,
			"sleep", "infinity",
		}); err != nil {
			return nil, err
		}
		return &WorkspaceTarget{
			Container: m.cfg.FactoryContainer,
			Workdir:   m.cfg.FactoryWorkdir,
		}, nil
	case "holding-devops":
		if err := m.ensureContainerRunning(ctx, m.cfg.InfraContainer, []string{
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
		return &WorkspaceTarget{
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
		if err := m.ensureContainerRunning(ctx, m.cfg.FactoryContainer, []string{
			"-v", fmt.Sprintf("%s:%s", m.cfg.FactoryVolume, m.cfg.FactoryWorkdir),
			"-w", m.cfg.FactoryWorkdir,
			m.cfg.WorkspaceImage,
			"sleep", "infinity",
		}); err != nil {
			return nil, err
		}
		return &WorkspaceTarget{
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
	slug, err := m.lookupVerticalSlug(ctx, actor.VerticalID)
	if err != nil {
		return nil, err
	}
	return &WorkspaceTarget{
		Container: m.verticalContainerName(slug),
		Workdir:   m.cfg.VerticalWorkdir,
	}, nil
}

func (m *DockerWorkspaceManager) ensureContainerRunning(ctx context.Context, name string, createArgs []string) error {
	exists, running, err := m.inspectContainer(ctx, name)
	if err != nil {
		return err
	}
	if !exists {
		args := []string{"create", "--name", name}
		if network := strings.TrimSpace(m.cfg.WorkspaceNetwork); network != "" {
			args = append(args, "--network", network)
		}
		args = append(args, createArgs...)
		if _, err := m.runDocker(ctx, args...); err != nil {
			return fmt.Errorf("create container %s: %w", name, err)
		}
		running = false
	}
	if running {
		if err := m.ensureContainerNetwork(ctx, name); err != nil {
			return err
		}
		return nil
	}
	if _, err := m.runDocker(ctx, "start", name); err != nil {
		// Another process may have started it between inspect/start.
		if !strings.Contains(strings.ToLower(err.Error()), "already running") {
			return fmt.Errorf("start container %s: %w", name, err)
		}
	}
	if err := m.ensureContainerNetwork(ctx, name); err != nil {
		return err
	}
	return nil
}

func (m *DockerWorkspaceManager) ensureContainerNetwork(ctx context.Context, name string) error {
	network := strings.TrimSpace(m.cfg.WorkspaceNetwork)
	if network == "" {
		return nil
	}
	if _, err := m.runDocker(ctx, "network", "connect", network, name); err != nil {
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

func (m *DockerWorkspaceManager) stopContainer(ctx context.Context, name string) error {
	exists, running, err := m.inspectContainer(ctx, name)
	if err != nil {
		return err
	}
	if !exists || !running {
		return nil
	}
	if _, err := m.runDocker(ctx, "stop", name); err != nil {
		return err
	}
	return nil
}

func (m *DockerWorkspaceManager) inspectContainer(ctx context.Context, name string) (bool, bool, error) {
	out, err := m.runDocker(ctx, "inspect", "--format", "{{.State.Running}}", name)
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

func (m *DockerWorkspaceManager) runDocker(ctx context.Context, args ...string) (string, error) {
	if m.runDockerFn != nil {
		return m.runDockerFn(ctx, args...)
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

func (m *DockerWorkspaceManager) lookupVerticalSlug(ctx context.Context, verticalID string) (string, error) {
	trimmedID := strings.TrimSpace(verticalID)
	if trimmedID == "" {
		return "", errors.New("vertical_id is required")
	}
	if m.db == nil {
		return sanitizeWorkspaceSlug(trimmedID), nil
	}
	var slug string
	if err := m.db.QueryRowContext(ctx, `
		SELECT COALESCE(NULLIF(slug, ''), '')
		FROM verticals
		WHERE id = $1::uuid
	`, trimmedID).Scan(&slug); err != nil {
		return "", fmt.Errorf("lookup vertical slug: %w", err)
	}
	slug = sanitizeWorkspaceSlug(slug)
	if slug == "" {
		return "", fmt.Errorf("vertical %s has no slug", trimmedID)
	}
	return slug, nil
}

func (m *DockerWorkspaceManager) verticalContainerName(slug string) string {
	return m.cfg.VerticalContainerPrefix + sanitizeWorkspaceSlug(slug)
}

func sanitizeWorkspaceSlug(raw string) string {
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

func envOrDefault(key, fallback string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	return v
}
