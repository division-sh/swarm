package workspace

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pythonmodule"
	"github.com/division-sh/swarm/internal/runtime/workspace/worker"
)

const WorkerContainerPath = "/opt/swarm/bin/swarm"
const WorkerImageIdentityLabel = "io.division-sh.swarm.workspace-worker"

// WorkerExecutionError retains launch/response facts; a lost response after
// starting the transport must not be settled as a known pre-model refusal.
type WorkerExecutionError struct {
	Started, Observed, ModelStarted bool
	RemoteCleanupUnproven           bool
	Err                             error
}

func (e *WorkerExecutionError) Error() string { return e.Err.Error() }
func (e *WorkerExecutionError) Unwrap() error { return e.Err }

var currentWorkerArtifact = sync.OnceValues(func() (workerArtifact, error) {
	path, err := os.Executable()
	if err != nil {
		return workerArtifact{}, err
	}
	identity, err := worker.ExecutableIdentity()
	return workerArtifact{path: path, identity: identity}, err
})

type workerArtifact struct {
	path     string
	identity worker.Identity
}

func (m *DockerManager) validateContainerWorkerInputs(ctx context.Context, name string) error {
	if runtime.GOOS != "linux" {
		return nil // Image worker identity is checked at target admission on Desktop.
	}
	artifact, err := currentWorkerArtifact()
	if err != nil {
		return err
	}
	raw, err := m.RunDocker(ctx, "inspect", "--format", `{"hosts":{{json .HostConfig.ExtraHosts}},"mounts":{{json .Mounts}}}`, name)
	if err != nil {
		return failures.Wrap(failures.ClassDependencyUnavailable, "workspace_worker_inputs_invalid", "workspace", "adopt_container", nil, err)
	}
	var inputs struct {
		Hosts  []string `json:"hosts"`
		Mounts []struct {
			Type, Source, Destination string
			RW                        bool
		} `json:"mounts"`
	}
	if err := json.Unmarshal([]byte(raw), &inputs); err != nil {
		return failures.New(failures.ClassDependencyUnavailable, "workspace_worker_inputs_invalid", "workspace", "adopt_container", nil)
	}
	mapping, mounted := false, false
	for _, host := range inputs.Hosts {
		if host == "host.docker.internal:host-gateway" {
			mapping = true
		}
	}
	for _, mount := range inputs.Mounts {
		if mount.Destination == WorkerContainerPath {
			mounted = mount.Type == "bind" && mount.Source == artifact.path && !mount.RW
		}
	}
	if !mapping || !mounted {
		return failures.New(failures.ClassDependencyUnavailable, "workspace_worker_inputs_invalid", "workspace", "adopt_container", nil)
	}
	return nil
}

func RunWorker(ctx context.Context, target *Target, dockerBin string, request worker.Request) (worker.Result, error) {
	if err := ctx.Err(); err != nil {
		return worker.Result{}, &WorkerExecutionError{Err: err}
	}
	cmd, expected, err := workerCommand(ctx, target, dockerBin)
	if err != nil {
		return worker.Result{}, &WorkerExecutionError{Err: err}
	}
	request.Expected = expected
	request.Deadline, _ = ctx.Deadline()
	if request.Module != nil {
		module := *request.Module
		module.CompiledCacheRoot = ""
		if target.ExecutionTarget().Mode == ExecutionModeHostLocal {
			module.CompiledCacheRoot, err = pythonmodule.ProcessArtifactCacheRoot()
			if err != nil {
				return worker.Result{}, &WorkerExecutionError{Err: err}
			}
		}
		request.Module = &module
	}
	input, err := json.Marshal(request)
	if err != nil || len(input) > worker.MaxBytes {
		return worker.Result{}, &WorkerExecutionError{Err: failures.New(failures.ClassSchemaInvalid, "workspace_worker_input_invalid", "workspace", "launch_worker", nil)}
	}
	if target.ExecutionTarget().Mode == ExecutionModeDockerContainer {
		return runDockerWorker(ctx, target, dockerBin, cmd, expected, input)
	}
	cmd.Stdin = bytes.NewReader(input)
	stdout := &workerOutputBuffer{limit: worker.MaxBytes}
	stderr := &workerOutputBuffer{limit: 64 << 10}
	cmd.Stdout, cmd.Stderr = stdout, stderr
	if err := cmd.Start(); err != nil {
		return worker.Result{}, &WorkerExecutionError{Err: failures.New(failures.ClassDependencyUnavailable, "workspace_worker_launch_failed", "workspace", "launch_worker", nil)}
	}
	waitErr := cmd.Wait()
	result, decodeErr := worker.DecodeResult(stdout.Bytes())
	if decodeErr == nil && result.Identity == expected {
		if result.Err() != nil {
			return result, &WorkerExecutionError{Started: true, Observed: true, ModelStarted: result.ModelStarted, Err: errors.Join(result.Err(), waitErr, ctx.Err())}
		}
		if waitErr != nil {
			return result, &WorkerExecutionError{Started: true, Observed: true, ModelStarted: result.ModelStarted, Err: waitErr}
		}
		return result, nil
	}
	// The host transport joins the actual child, but a lost launched response
	// still cannot authorize a tool retry. Retain cancellation and exit causes
	// without relaying child diagnostics that may contain private request bytes.
	cause := failures.New(failures.ClassDependencyUnavailable, "workspace_worker_incompatible", "workspace", "admit_worker", nil)
	return worker.Result{}, &WorkerExecutionError{Started: true, Err: failures.Wrap(failures.ClassOutcomeUncertain, "workspace_worker_outcome_uncertain", "workspace", "join_worker", nil, errors.Join(cause, waitErr, ctx.Err()))}
}

func workerCommand(ctx context.Context, target *Target, dockerBin string) (*exec.Cmd, worker.Identity, error) {
	if target == nil {
		return nil, worker.Identity{}, failures.New(failures.ClassDependencyUnavailable, "workspace_worker_target_missing", "workspace", "admit_worker", nil)
	}
	artifact, err := currentWorkerArtifact()
	if err != nil {
		return nil, worker.Identity{}, fmt.Errorf("resolve current workspace worker: %w", err)
	}
	execution := target.ExecutionTarget()
	switch execution.Mode {
	case ExecutionModeHostLocal:
		if artifact.identity.ABI != worker.ABI || artifact.identity.OS != runtime.GOOS || artifact.identity.Arch != runtime.GOARCH || artifact.identity.BinaryDigest == "" {
			return nil, worker.Identity{}, failures.New(failures.ClassDependencyUnavailable, "workspace_worker_incompatible", "workspace", "admit_worker", nil)
		}
		cmd := exec.CommandContext(ctx, artifact.path, worker.Argument)
		cmd.Dir = target.Workdir
		// The native entry owns interpreter/HTTP cancellation. Let it join that
		// work and report possible-commit evidence before escalating to disposal.
		cmd.Cancel = func() error { return cmd.Process.Signal(os.Interrupt) }
		cmd.WaitDelay = 10 * time.Second
		return cmd, artifact.identity, nil
	case ExecutionModeDockerContainer:
		if dockerBin == "" {
			dockerBin = defaultDockerBin
		}
		if artifact.identity.OS != "linux" {
			inspect := exec.CommandContext(ctx, dockerBin, "inspect", "--format", "{{index .Config.Labels \""+WorkerImageIdentityLabel+"\"}}", execution.Container)
			raw, err := inspect.Output()
			var provisioned worker.Identity
			if err != nil || json.Unmarshal(raw, &provisioned) != nil || worker.ValidateLinuxArtifact(artifact.identity, provisioned) != nil {
				return nil, worker.Identity{}, failures.New(failures.ClassDependencyUnavailable, "workspace_worker_linux_artifact_required", "workspace", "admit_worker", nil)
			}
			artifact.identity = provisioned
		}
		if err := worker.ValidateLinuxArtifact(artifact.identity, artifact.identity); err != nil {
			return nil, worker.Identity{}, failures.New(failures.ClassDependencyUnavailable, "workspace_worker_incompatible", "workspace", "admit_worker", nil)
		}
		cmd := exec.CommandContext(ctx, dockerBin, "exec", "-i", "-w", execution.Workdir, execution.Container, WorkerContainerPath, worker.Argument)
		// Killing docker exec only kills the client. Keep its attached wait until
		// the bounded native child returns; its deadline and fuel travel in stdin.
		cmd.Cancel = func() error { return nil }
		return cmd, artifact.identity, nil
	default:
		return nil, worker.Identity{}, failures.New(failures.ClassDependencyUnavailable, "workspace_worker_target_unsupported", "workspace", "admit_worker", nil)
	}
}

func VerifyBuiltWorker(ctx context.Context, dockerBin, image string, expected worker.Identity) error {
	cmd := exec.CommandContext(ctx, dockerBin, "run", "--rm", "-i", "--network", "none", "--entrypoint", WorkerContainerPath, image, worker.Argument)
	cmd.Stdin = strings.NewReader(`{"mode":"identity"}`)
	stdout := &workerOutputBuffer{limit: worker.MaxBytes}
	cmd.Stdout = stdout
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("workspace Linux worker cannot execute in the built image: %w", err)
	}
	result, err := worker.DecodeResult(stdout.Bytes())
	if err != nil || result.Failure != nil || result.Identity != expected {
		return fmt.Errorf("workspace Linux worker bytes/version/ABI/interpreter differ from the admitted image identity")
	}
	return nil
}

type workerOutputBuffer struct {
	bytes.Buffer
	limit int
}

func (b *workerOutputBuffer) Write(p []byte) (int, error) {
	if len(p) > b.limit-b.Len() {
		return 0, errors.New("workspace worker output over budget")
	}
	return b.Buffer.Write(p)
}
