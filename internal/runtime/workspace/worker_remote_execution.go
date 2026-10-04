package workspace

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/workspace/worker"
)

func runDockerWorker(ctx context.Context, target *Target, dockerBin string, cmd *exec.Cmd, expected worker.Identity, input []byte) (worker.Result, error) {
	if dockerBin == "" {
		dockerBin = defaultDockerBin
	}
	// Bind observations to an immutable container, never a name another owner
	// can replace. The invocation coordinate is ephemeral, not a PID ledger.
	inspect := exec.CommandContext(ctx, dockerBin, "inspect", "--format", "{{.Id}}", target.ExecutionTarget().Container)
	rawID, err := inspect.Output()
	containerID := strings.TrimSpace(string(rawID))
	decodedID, decodeErr := hex.DecodeString(containerID)
	if err != nil || decodeErr != nil || len(decodedID) != 32 || strings.ToLower(containerID) != containerID {
		return worker.Result{}, &WorkerExecutionError{Err: failures.Wrap(failures.ClassDependencyUnavailable, "workspace_worker_target_missing", "workspace", "admit_worker", nil, err)}
	}
	var coordinate [16]byte
	if _, err := rand.Read(coordinate[:]); err != nil {
		return worker.Result{}, &WorkerExecutionError{Err: err}
	}
	invocation := hex.EncodeToString(coordinate[:])
	argument, err := worker.InvocationArgument(invocation)
	if err != nil {
		return worker.Result{}, &WorkerExecutionError{Err: err}
	}
	cmd.Args[len(cmd.Args)-3] = containerID
	cmd.Args[len(cmd.Args)-1] = argument
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return worker.Result{}, &WorkerExecutionError{Err: err}
	}
	defer stdin.Close()
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return worker.Result{}, &WorkerExecutionError{Err: err}
	}
	defer stdout.Close()
	cmd.Stderr = &workerOutputBuffer{limit: 64 << 10}
	if err := cmd.Start(); err != nil {
		return worker.Result{}, &WorkerExecutionError{Err: err}
	}
	stop := context.AfterFunc(ctx, func() { _ = stdin.Close() })
	defer stop()
	reader := bufio.NewReader(stdout)
	admitted, protocolErr := acknowledgeDockerWorker(stdin, reader, input, invocation, expected)
	if protocolErr != nil {
		_ = stdin.Close()
	}
	raw, readErr := io.ReadAll(io.LimitReader(reader, worker.MaxBytes+1))
	if len(raw) > worker.MaxBytes {
		readErr = fmt.Errorf("workspace worker output over budget")
		_ = stdin.Close()
		_ = stdout.Close()
	}
	waitErr := cmd.Wait()
	_ = stdin.Close()
	result, resultErr := worker.DecodeResult(raw)
	observed := protocolErr == nil && readErr == nil && resultErr == nil && result.Identity == expected
	var outcomeErr error
	if observed {
		outcomeErr = result.Err()
		if outcomeErr != nil && ctx.Err() != nil {
			// An attachment close caused by the parent's deadline is the same
			// logical cancellation, even if the native deadline callback loses
			// that race. Independent result failures are never rewritten.
			if outcomeErr == context.Canceled {
				outcomeErr = ctx.Err()
			} else {
				outcomeErr = errors.Join(outcomeErr, ctx.Err())
			}
		}
	} else {
		result = worker.Result{}
		outcomeErr = failures.New(failures.ClassDependencyUnavailable, "workspace_worker_launch_failed", "workspace", "launch_worker", nil)
	}
	cleanupErr := observeDockerWorkerGone(dockerBin, containerID, argument, admitted)
	if err := errors.Join(outcomeErr, protocolErr, readErr, waitErr, cleanupErr); err != nil {
		if !observed || cleanupErr != nil {
			// Tool calls share this owner with models and probes. A lost response
			// or unproven cleanup cannot authorize any consumer to retry a call.
			err = failures.Wrap(failures.ClassOutcomeUncertain, "workspace_worker_outcome_uncertain", "workspace", "join_worker", nil, err)
		}
		return result, &WorkerExecutionError{Started: true, Observed: observed, ModelStarted: result.ModelStarted, RemoteCleanupUnproven: cleanupErr != nil, Err: err}
	}
	return result, nil
}

func acknowledgeDockerWorker(stdin io.Writer, reader *bufio.Reader, input []byte, invocation string, expected worker.Identity) (bool, error) {
	if _, err := stdin.Write(append(input, '\n')); err != nil {
		return false, err
	}
	frame, err := worker.ReadFrame(reader)
	if err != nil {
		return false, err
	}
	ready, err := worker.DecodeReady(frame)
	if err != nil {
		return false, err
	}
	if ready.Invocation != invocation || ready.Identity != expected {
		return false, failures.New(failures.ClassDependencyUnavailable, "workspace_worker_incompatible", "workspace", "admit_worker", nil)
	}
	return true, json.NewEncoder(stdin).Encode(worker.Acknowledgement{Invocation: invocation, Execute: true})
}

func observeDockerWorkerGone(dockerBin, containerID, argument string, admitted bool) error {
	unproven := func(err error) error {
		return failures.Wrap(failures.ClassOutcomeUncertain, "workspace_worker_cleanup_unproven", "workspace", "join_worker", nil, err)
	}
	if !admitted {
		// No acknowledgement means no work is authorized, but observing an empty
		// process list before a late launch would not prove process cleanup.
		return unproven(errors.New("remote worker launch was not acknowledged"))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for {
		// Docker requires the PID column to restrict ps output to the container.
		// It is not retained or used as execution authority.
		cmd := exec.CommandContext(ctx, dockerBin, "top", containerID, "-eo", "pid,args")
		raw, err := cmd.Output()
		if err != nil {
			return unproven(err)
		}
		present, err := dockerWorkerPresent(raw, argument)
		if err != nil {
			return unproven(err)
		}
		if !present {
			return nil
		}
		timer := time.NewTimer(20 * time.Millisecond)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return unproven(ctx.Err())
		}
	}
}

func dockerWorkerPresent(raw []byte, argument string) (bool, error) {
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) == 0 || strings.Join(strings.Fields(lines[0]), " ") != "PID COMMAND" {
		return false, errors.New("invalid remote process observation")
	}
	for _, line := range lines[1:] {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return false, errors.New("incomplete remote process observation")
		}
		if pid, err := strconv.ParseUint(fields[0], 10, 64); err != nil || pid == 0 {
			return false, errors.New("invalid remote process observation")
		}
		if len(fields) >= 3 && fields[1] == WorkerContainerPath && fields[2] == argument {
			return true, nil
		}
	}
	return false, nil
}
