package workspace

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/google/uuid"
)

// PrepareClaudeStartupObservation never resolves an execution workspace. All
// writable backing belongs to this disposable check, not a session or actor.
func (m *DockerManager) PrepareClaudeStartupObservation(ctx context.Context, actor actors.AgentConfig, projection *sourceartifact.RuntimeProjection) (_ *Target, retErr error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if m == nil {
		return nil, fmt.Errorf("workspace manager is required")
	}
	bundle, ok := semanticview.Bundle(m.source)
	if !ok || bundle == nil || bundle.SourceArtifact == nil {
		return nil, fmt.Errorf("startup observation requires the admitted semantic source")
	}
	if _, err := validateSourceProjection(projection, bundle.SourceArtifact.BundleHash()); err != nil {
		return nil, fmt.Errorf("startup observation requires the exact source projection")
	}
	base, err := DockerCapabilityTarget(m.cfg, m.source, actor)
	if err != nil {
		return nil, err
	}
	image, err := m.InspectWorkspaceImage(ctx)
	if err != nil {
		return nil, err
	}
	volumes, err := m.runDockerObservation(ctx, "image", "inspect", "--format", "{{json .Config.Volumes}}", image)
	if err != nil {
		return nil, err
	}
	var declared map[string]json.RawMessage
	if err := json.Unmarshal([]byte(volumes), &declared); err != nil || len(declared) != 0 {
		return nil, fmt.Errorf("observed image declares volumes or has an unreadable volume declaration")
	}
	id := uuid.NewString()
	name := "swarm-admission-probe-" + id
	creationAttempted, created, handedOff := false, false, false
	network := strings.TrimSpace(m.cfg.WorkspaceNetwork)
	temporaryNetwork, networkCreated := false, false
	var cleanupOnce sync.Once
	var cleanupErr error
	cleanup := func(ctx context.Context) error {
		cleanupOnce.Do(func() {
			if creationAttempted {
				cleanupErr = m.removeAdmissionProbe(ctx, name, id, created)
			}
			if temporaryNetwork {
				cleanupErr = errors.Join(cleanupErr, m.removeAdmissionProbeNetwork(ctx, network, id, networkCreated))
			}
		})
		return cleanupErr
	}
	defer func() {
		if !handedOff {
			bounded, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer cancel()
			retErr = errors.Join(retErr, cleanup(bounded))
		}
	}()
	if network != "" {
		present, err := m.InspectWorkspaceNetwork(ctx)
		if err != nil {
			return nil, err
		}
		if !present {
			network, temporaryNetwork = name, true
			if _, err := m.runDockerObservation(ctx, "network", "create", "--label", workspaceAdmissionProbeLabel+"="+id, network); err != nil {
				return nil, err
			}
			networkCreated = true
		}
	}
	args := []string{"create", "--name", name, "--label", workspaceAdmissionProbeLabel + "=" + id,
		"--pull=never", "--no-healthcheck", "--log-driver", "none",
		"--cap-drop", "ALL", "--security-opt", "no-new-privileges:true",
		"--tmpfs", ClaudeStateDirectory + ":rw,mode=0700,uid=10001,gid=10001",
		"--tmpfs", "/tmp:rw,mode=1777",
		"--mount", "type=bind,source=" + projection.PrivateRoot() + ",destination=" + m.cfg.SourceMountPoint + ",readonly"}
	if runtime.GOOS == "linux" {
		artifact, err := currentWorkerArtifact()
		if err != nil {
			return nil, err
		}
		args = append(args, "--add-host", "host.docker.internal:host-gateway", "--mount", "type=bind,source="+artifact.path+",destination="+WorkerContainerPath+",readonly")
	}
	if network != "" {
		args = append(args, "--network", network)
	}
	args = append(args, "--workdir", base.Workdir, "--entrypoint", "sleep", image, "infinity")
	creationAttempted = true
	if _, err := m.runDockerObservation(ctx, args...); err != nil {
		return nil, err
	}
	created = true
	if _, err := m.runDockerObservation(ctx, "start", name); err != nil {
		return nil, err
	}
	if _, err := m.runDockerObservation(ctx, "exec", "--user", "0", name, "node", "-e", `const fs=require('fs');fs.mkdirSync(process.argv[1],{recursive:true});fs.chownSync(process.argv[1],10001,10001);`, base.Workdir); err != nil {
		return nil, err
	}
	base.Container = name
	base.ClaudeState = &startupObservationClaudeState{release: cleanup}
	base.release = cleanup
	handedOff = true
	return base, nil
}

type startupObservationClaudeState struct{ release func(context.Context) error }

func (*startupObservationClaudeState) Directory() string { return ClaudeStateDirectory }
func (*startupObservationClaudeState) CheckHead(context.Context, string) error {
	return fmt.Errorf("startup observation has no reusable provider head")
}
func (s *startupObservationClaudeState) Release(ctx context.Context) error { return s.release(ctx) }

func (m *DockerManager) removeAdmissionProbeNetwork(ctx context.Context, name, id string, created bool) error {
	args := []string{"network", "ls", "--no-trunc", "--filter", "label=" + workspaceAdmissionProbeLabel + "=" + id, "--filter", "name=" + name, "--format", "{{json .ID}}"}
	objects, err := m.runDockerObservation(ctx, args...)
	if err != nil {
		return fmt.Errorf("probe network removal unconfirmed: %w", err)
	}
	if strings.TrimSpace(objects) == "" && !created {
		return fmt.Errorf("probe network creation did not settle; removal unconfirmed")
	}
	for _, line := range strings.Split(objects, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var object string
		if json.Unmarshal([]byte(line), &object) != nil || len(object) != 64 || strings.Trim(object, "0123456789abcdef") != "" {
			return fmt.Errorf("probe network removal identity unconfirmed")
		}
		if _, err := m.runDockerObservation(ctx, "network", "rm", object); err != nil {
			return fmt.Errorf("probe network removal unconfirmed: %w", err)
		}
	}
	remaining, err := m.runDockerObservation(ctx, args...)
	if err != nil || strings.TrimSpace(remaining) != "" {
		return errors.Join(fmt.Errorf("probe network removal unconfirmed"), err)
	}
	return nil
}
