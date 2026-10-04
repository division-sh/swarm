package workspace

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"time"

	"github.com/google/uuid"
)

const workspaceAdmissionProbeLabel = "sh.swarm.admission-probe"

func (m *DockerManager) InspectDockerAvailable(ctx context.Context) error {
	if m == nil {
		return fmt.Errorf("workspace manager is required")
	}
	return m.checkDockerAvailable(ctx, m.runDockerObservation)
}

// InspectWorkspaceImage records an existing immutable image, without pulling.
func (m *DockerManager) InspectWorkspaceImage(ctx context.Context) (string, error) {
	if m == nil || strings.TrimSpace(m.cfg.WorkspaceImage) == "" {
		return "", fmt.Errorf("workspace image is required")
	}
	id, err := m.runDockerObservation(ctx, "image", "inspect", "--format", "{{.Id}}", m.cfg.WorkspaceImage)
	if err != nil {
		return "", fmt.Errorf("inspect existing workspace image %q: %w", m.cfg.WorkspaceImage, err)
	}
	if err := validateDockerImageID(id); err != nil {
		return "", err
	}
	return id, nil
}

func validateDockerImageID(id string) error {
	encoded, ok := strings.CutPrefix(id, "sha256:")
	decoded, err := hex.DecodeString(encoded)
	if !ok || err != nil || len(decoded) != 32 || encoded != strings.ToLower(encoded) || id != strings.TrimSpace(id) {
		return fmt.Errorf("Docker image inspection did not return a canonical immutable image ID")
	}
	return nil
}

// InspectWorkspaceNetwork distinguishes known absence from failed observation.
// Boot may create a known-absent network; verification cannot do so.
func (m *DockerManager) InspectWorkspaceNetwork(ctx context.Context) (bool, error) {
	if m == nil {
		return false, fmt.Errorf("workspace manager is required")
	}
	name := strings.TrimSpace(m.cfg.WorkspaceNetwork)
	if name == "" {
		return false, nil
	}
	out, err := m.runDockerObservation(ctx, "network", "ls", "--filter", "name="+name, "--format", "{{json .Name}}")
	if err != nil {
		return false, err
	}
	for _, row := range strings.Split(out, "\n") {
		if strings.TrimSpace(row) == "" {
			continue
		}
		var found string
		if err := json.Unmarshal([]byte(row), &found); err != nil || found == "" {
			return false, fmt.Errorf("Docker network observation returned an invalid name")
		}
		if found == name {
			return true, nil
		}
	}
	return false, nil
}

// ProbeWorkspaceCLICommand observes only --version in a restricted non-workspace
// container. Cleanup is part of the observation, even after caller cancellation.
func (m *DockerManager) ProbeWorkspaceCLICommand(ctx context.Context, imageID, command string) (retErr error) {
	if m == nil {
		return fmt.Errorf("workspace manager is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	ctx, stop := context.WithTimeout(ctx, 15*time.Second)
	defer stop()
	if err := validateDockerImageID(imageID); err != nil {
		return err
	}
	if command == "" || strings.TrimSpace(command) != command {
		return fmt.Errorf("configured CLI command must be nonempty and exact")
	}
	volumesJSON, err := m.runDockerObservation(ctx, "image", "inspect", "--format", "{{json .Config.Volumes}}", imageID)
	if err != nil {
		return fmt.Errorf("inspect immutable probe image mounts: %w", err)
	}
	var volumes map[string]json.RawMessage
	if err := json.Unmarshal([]byte(volumesJSON), &volumes); err != nil {
		return fmt.Errorf("Docker did not return the probe image volume declaration")
	}
	if len(volumes) != 0 {
		return fmt.Errorf("observed image declares volumes; a mount-free CLI version probe is unavailable")
	}
	id := uuid.NewString()
	name := "swarm-admission-probe-" + id
	createSettled := false
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		retErr = errors.Join(retErr, m.removeAdmissionProbe(cleanup, name, id, createSettled))
		if ctx.Err() != nil {
			retErr = errors.Join(ctx.Err(), retErr)
		}
	}()
	if _, err := m.runDockerObservation(ctx, "create", "--name", name,
		"--label", workspaceAdmissionProbeLabel+"="+id,
		"--pull=never", "--read-only", "--network", "none", "--no-healthcheck", "--log-driver", "none",
		"--cap-drop", "ALL", "--security-opt", "no-new-privileges:true",
		"--memory", "256m", "--cpus", "1", "--pids-limit", "64",
		"--entrypoint", command, imageID, "--version"); err != nil {
		return fmt.Errorf("create restricted CLI version probe: %w", err)
	}
	createSettled = true
	if _, err := m.runDockerObservation(ctx, "start", "--attach", name); err != nil {
		return fmt.Errorf("configured CLI %q version probe failed in observed image %s: %w", command, imageID, err)
	}
	stateJSON, err := m.runDockerObservation(ctx, "inspect", "--format", "{{json .State}}", name)
	if err != nil {
		return fmt.Errorf("read CLI probe outcome: %w", err)
	}
	var state struct {
		Status    *string
		Running   *bool
		ExitCode  *int
		OOMKilled *bool
	}
	if err := json.Unmarshal([]byte(stateJSON), &state); err != nil || state.Status == nil || state.Running == nil || state.ExitCode == nil || state.OOMKilled == nil {
		return fmt.Errorf("Docker did not return a complete CLI version probe outcome")
	}
	if *state.Status != "exited" || *state.Running || *state.ExitCode != 0 || *state.OOMKilled {
		return fmt.Errorf("CLI version probe did not settle successfully")
	}
	return nil
}

func (m *DockerManager) admissionProbeContainers(ctx context.Context, name, id string) ([]string, error) {
	out, err := m.runDockerObservation(ctx, "container", "ls", "--all", "--no-trunc",
		"--filter", "name=^/"+name+"$", "--filter", "label="+workspaceAdmissionProbeLabel+"="+id, "--format", "{{json .ID}}")
	if err != nil {
		return nil, err
	}
	var ids []string
	for _, row := range strings.Split(out, "\n") {
		if strings.TrimSpace(row) == "" {
			continue
		}
		var containerID string
		if err := json.Unmarshal([]byte(row), &containerID); err != nil {
			return nil, fmt.Errorf("invalid probe cleanup identity")
		}
		decoded, err := hex.DecodeString(containerID)
		if err != nil || len(decoded) != 32 || containerID != strings.ToLower(containerID) {
			return nil, fmt.Errorf("invalid probe cleanup identity")
		}
		ids = append(ids, containerID)
	}
	if len(ids) > 1 {
		return nil, fmt.Errorf("probe cleanup identity is not unique")
	}
	return ids, nil
}

func (m *DockerManager) removeAdmissionProbe(ctx context.Context, name, id string, createSettled bool) error {
	ids, err := m.admissionProbeContainers(ctx, name, id)
	if err != nil {
		return fmt.Errorf("probe cleanup inspection failed; removal unconfirmed: %w", err)
	}
	// A canceled CLI request can finish in the daemon after this observation.
	// Absence is not cleanup evidence until creation has settled or been found.
	if len(ids) == 0 && !createSettled {
		return fmt.Errorf("probe %q creation did not settle; removal unconfirmed", name)
	}
	for _, containerID := range ids {
		if _, err := m.runDockerObservation(ctx, "rm", "--force", containerID); err != nil {
			return fmt.Errorf("probe cleanup failed; removal unconfirmed: %w", err)
		}
	}
	remaining, err := m.admissionProbeContainers(ctx, name, id)
	if err != nil {
		return fmt.Errorf("probe cleanup readback failed; removal unconfirmed: %w", err)
	}
	if len(remaining) != 0 {
		return fmt.Errorf("probe container remains after cleanup")
	}
	return nil
}

func (m *DockerManager) runDockerObservation(ctx context.Context, args ...string) (out string, retErr error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if m.RunDockerFn != nil {
		return m.RunDockerFn(ctx, args...)
	}
	work := ctx
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return "", context.DeadlineExceeded
		}
		tail := min(time.Second, remaining/2)
		var cancel context.CancelFunc
		work, cancel = context.WithDeadline(ctx, deadline.Add(-tail))
		defer cancel()
	}
	cmd := exec.CommandContext(work, m.cfg.DockerBin, args...)
	configureAdmissionProbeProcess(cmd)
	cmd.WaitDelay = time.Second
	if deadline, ok := ctx.Deadline(); ok {
		cmd.WaitDelay = min(cmd.WaitDelay, max(time.Nanosecond, time.Until(deadline)/2))
	}
	cmd.Cancel = func() error { return killAdmissionProbeProcess(cmd) }
	defer func() {
		if cmd.Process != nil {
			retErr = errors.Join(retErr, killAdmissionProbeProcess(cmd))
		}
		if ctx.Err() != nil {
			retErr = errors.Join(ctx.Err(), retErr)
		} else if work.Err() != nil {
			retErr = errors.Join(work.Err(), retErr)
		}
	}()
	var captured admissionProbeOutput
	cmd.Stderr = io.Discard
	cmd.Stdout = &captured
	if len(args) > 0 && (args[0] == "start" || args[0] == "rm") {
		cmd.Stdout = io.Discard
	}
	err := cmd.Run()
	if captured.exceeded {
		err = errors.Join(err, fmt.Errorf("Docker observation exceeded its output limit"))
	}
	// Untrusted version/daemon output is never appended to a public error.
	return strings.TrimSpace(captured.buffer.String()), err
}

type admissionProbeOutput struct {
	buffer   bytes.Buffer
	exceeded bool
}

func (b *admissionProbeOutput) Write(p []byte) (int, error) {
	const limit = 64 * 1024
	remaining := limit - b.buffer.Len()
	if len(p) > remaining {
		b.exceeded = true
	}
	_, _ = b.buffer.Write(p[:min(len(p), remaining)])
	return len(p), nil
}
