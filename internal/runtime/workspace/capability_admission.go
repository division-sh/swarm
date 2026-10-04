package workspace

import (
	"context"
	"fmt"
	"strings"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type capabilityInspection struct {
	resolve func(models.AgentConfig) (*Target, error)
}

func NewDockerCapabilityInspection(cfg DockerConfig, source semanticview.Source) (CapabilityAdmissionResolver, error) {
	if source == nil {
		return nil, fmt.Errorf("workspace capability inspection requires semantic source")
	}
	return capabilityInspection{resolve: func(actor models.AgentConfig) (*Target, error) {
		return DockerCapabilityTarget(cfg, source, actor)
	}}, nil
}

func NewHostCapabilityInspection(cfg HostConfig, source semanticview.Source) (CapabilityAdmissionResolver, error) {
	if source == nil {
		return nil, fmt.Errorf("workspace capability inspection requires semantic source")
	}
	return capabilityInspection{resolve: func(actor models.AgentConfig) (*Target, error) {
		return HostCapabilityTarget(cfg, source, actor)
	}}, nil
}

func (r capabilityInspection) ResolveWorkspace(context.Context, models.AgentConfig) (*Target, error) {
	return nil, fmt.Errorf("workspace capability inspection cannot resolve an execution workspace")
}

func (r capabilityInspection) ResolveWorkspaceForCapabilityAdmission(ctx context.Context, actor models.AgentConfig) (*Target, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	target, err := r.resolve(actor)
	if cancelErr := ctx.Err(); cancelErr != nil {
		return nil, cancelErr
	}
	return target, err
}

// DockerCapabilityTarget projects configured capability shape, not an existing
// container or a runtime-owned source/data binding. Boot validates those separately.
func DockerCapabilityTarget(cfg DockerConfig, source semanticview.Source, actor models.AgentConfig) (*Target, error) {
	class, err := workspaceClassForSource(source, actor)
	if err != nil {
		return nil, err
	}
	container := strings.TrimSpace(cfg.SystemContainer)
	workdir := strings.TrimSpace(cfg.SystemWorkdir)
	switch workspaceRouteClass(class) {
	case "scaffold":
		container = strings.TrimSpace(cfg.ScaffoldContainer)
		workdir = strings.TrimSpace(cfg.ScaffoldWorkdir)
	case "system":
	default:
		if _, err := workspaceScopeForCapabilityAdmission(source, actor); err != nil {
			return nil, err
		}
	}
	return &Target{
		Container: container,
		Workdir:   workdir,
		Backend:   BackendDocker,
		Mounts:    dockerExecutionMounts(cfg, false),
	}, nil
}

// HostCapabilityTarget reads the planned root and logical mounts without
// creating directories or fabricating a source projection's physical path.
func HostCapabilityTarget(cfg HostConfig, source semanticview.Source, actor models.AgentConfig) (*Target, error) {
	class, err := workspaceClassForSource(source, actor)
	if err != nil {
		return nil, err
	}
	if workspaceRouteClass(class) == "" {
		if _, err := workspaceScopeForCapabilityAdmission(source, actor); err != nil {
			return nil, err
		}
	}
	root, err := validateHostMountAdmission(cfg)
	if err != nil {
		return nil, err
	}
	sourceMount := strings.TrimSpace(cfg.SourceMountPoint)
	if sourceMount == "" {
		sourceMount = LogicalSourceMount
	}
	return &Target{
		Workdir: root,
		Backend: BackendHost,
		Mounts: []ExecutionMount{
			{LogicalPath: LogicalWorkspaceMount, HostPath: root, Access: MountAccessReadWrite},
			{LogicalPath: sourceMount, Access: MountAccessReadOnly},
		},
	}, nil
}
