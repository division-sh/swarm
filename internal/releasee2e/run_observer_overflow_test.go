package releasee2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestRunStartForegroundObserverOverflowFromReleaseBinary(t *testing.T) {
	const runID = "00000000-0000-4000-8000-000000002156"
	repo := releaseE2ERepoRoot(t)
	releaseRoot := t.TempDir()
	releaseRoot, err := filepath.EvalSymlinks(releaseRoot)
	if err != nil {
		t.Fatalf("resolve release root: %v", err)
	}
	binaryPath := buildReleaseBinary(t, releaseRoot)
	writeReleaseObserverOverflowFixture(t, repo, releaseRoot, 100)

	releaseLock := acquireReleaseMCPPortLock(t)
	defer releaseLock()
	requireDefaultMCPPortAvailable(t)
	apiPort := freeReleaseTCPPort(t)
	home := filepath.Join(releaseRoot, "home")
	fakeBin := filepath.Join(releaseRoot, "fake-bin")
	fakeRoot := filepath.Join(releaseRoot, "fake-state")
	if err := os.MkdirAll(fakeBin, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(fakeRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	testBinary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	dockerScript := fmt.Sprintf("#!/bin/sh\n%s=1 exec %s -- \"$@\"\n", fakeDockerHelperEnv, shellQuote(testBinary))
	writeExecutable(t, filepath.Join(fakeBin, "docker"), dockerScript)
	emitGate := filepath.Join(fakeRoot, "release-mcp-emit")
	env := append(releaseProcessEnv(fakeBin, fakeRoot, home), fakeDockerMCPEmitGateEnv+"="+emitGate)
	verify := runReleaseCommand(t, 30*time.Second, releaseRoot, env, "", binaryPath, "verify")
	if verify.err != nil {
		t.Fatalf("release overflow fixture verification failed: %v\n%s", verify.err, verify.output)
	}
	secrets := runReleaseCommand(t, 30*time.Second, releaseRoot, env, releaseE2EOAuthToken+"\n", binaryPath, "secrets", "set", "CLAUDE_CODE_OAUTH_TOKEN", "--stdin")
	if secrets.err != nil {
		t.Fatalf("release overflow credential setup failed: %v\n%s", secrets.err, secrets.output)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	cmd := exec.CommandContext(
		ctx,
		binaryPath,
		"run", "start",
		"--backend", "claude_cli",
		"--api-port", fmt.Sprint(apiPort),
		"--event", "worker/task.assigned",
		"--payload", filepath.Join(releaseRoot, "payload.json"),
		"--run-id", runID,
	)
	cmd.Dir = releaseRoot
	cmd.Env = env
	sockets, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := unix.SetsockoptInt(sockets[0], unix.SOL_SOCKET, unix.SO_SNDBUF, 1024); err != nil {
		t.Fatal(err)
	}
	if err := unix.SetsockoptInt(sockets[1], unix.SOL_SOCKET, unix.SO_RCVBUF, 1024); err != nil {
		t.Fatal(err)
	}
	stdoutWriter := os.NewFile(uintptr(sockets[0]), "release-overflow-stdout-writer")
	stdoutReader := os.NewFile(uintptr(sockets[1]), "release-overflow-stdout-reader")
	defer stdoutWriter.Close()
	defer stdoutReader.Close()
	cmd.Stdout = stdoutWriter
	stderr := newReleaseSignalBuffer("\"reason_code\":\"queue_overflow\"")
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waitDone := make(chan error, 1)
	go func() { waitDone <- cmd.Wait() }()
	if err := waitForReleasePath(ctx, emitGate+".ready"); err != nil {
		calls, _ := os.ReadFile(filepath.Join(fakeRoot, "calls.jsonl"))
		t.Fatalf("wait for managed Claude emit gate: %v\nstderr:\n%s\ndocker calls:\n%s", err, stderr.String(), calls)
	}
	stdoutPrefix, err := readReleaseSocketUntil(ctx, sockets[1], "agent.requested")
	if err != nil {
		t.Fatalf("wait for attached foreground observer: %v\nstdout:\n%s\nstderr:\n%s", err, stdoutPrefix, stderr.String())
	}
	if err := fillReleaseSocket(sockets[0]); err != nil {
		t.Fatalf("saturate release foreground stdout: %v", err)
	}
	if err := os.WriteFile(emitGate, []byte("release\n"), 0o600); err != nil {
		t.Fatalf("release managed Claude emit gate: %v", err)
	}
	if err := stdoutWriter.Close(); err != nil {
		t.Fatalf("close parent release stdout writer: %v", err)
	}
	type stdoutResult struct {
		output []byte
		err    error
	}
	select {
	case <-stderr.signal:
	case err := <-waitDone:
		select {
		case <-stderr.signal:
			t.Fatalf("release foreground run exited after publishing overflow but before output release: %v\nstderr:\n%s", err, stderr.String())
		default:
			t.Fatalf("release foreground run exited before observer overflow: %v\nstderr:\n%s", err, stderr.String())
		}
	case <-ctx.Done():
		t.Fatalf("release foreground run did not shed its blocked observer: %v\nstderr:\n%s", ctx.Err(), stderr.String())
	}
	stdoutDone := make(chan stdoutResult, 1)
	go func() {
		output, err := io.ReadAll(stdoutReader)
		stdoutDone <- stdoutResult{output: output, err: err}
	}()
	if err := waitForReleaseDockerRecordClass(ctx, fakeRoot, "mcp_emit"); err != nil {
		calls, _ := os.ReadFile(filepath.Join(fakeRoot, "calls.jsonl"))
		t.Fatalf("wait for managed Claude completion: %v\nstderr:\n%s\ndocker calls:\n%s", err, stderr.String(), calls)
	}
	stdoutRead := <-stdoutDone
	if stdoutRead.err != nil {
		t.Fatal(stdoutRead.err)
	}
	stdoutRead.output = append(stdoutPrefix, stdoutRead.output...)
	if err := <-waitDone; err != nil {
		t.Fatalf("release foreground run failed after observer detach: %v\nstdout:\n%s\nstderr:\n%s", err, stdoutRead.output, stderr.String())
	}
	if ctx.Err() != nil {
		t.Fatalf("release foreground run timed out: %v", ctx.Err())
	}
	for _, want := range []string{"run started:", "run terminal:", "status=completed"} {
		if !strings.Contains(string(stdoutRead.output), want) {
			t.Fatalf("release foreground stdout missing %q:\n%s", want, stdoutRead.output)
		}
	}
	var fact struct {
		Type         string `json:"type"`
		Severity     string `json:"severity"`
		ReasonCode   string `json:"reason_code"`
		RunContinues bool   `json:"run_continues"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(stderr.String())), &fact); err != nil {
		t.Fatalf("decode release detach fact: %v\nstderr:\n%s", err, stderr.String())
	}
	if fact.Type != "run_trace_observer_detached" || fact.Severity != "warning" || fact.ReasonCode != "queue_overflow" || !fact.RunContinues {
		t.Fatalf("release detach fact = %#v", fact)
	}
	assertReleaseExternalProcessesExited(t, fakeRoot)
}

func waitForReleasePath(ctx context.Context, path string) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(path); err == nil {
			return nil
		} else if !os.IsNotExist(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func readReleaseSocketUntil(ctx context.Context, socket int, needle string) ([]byte, error) {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	var output bytes.Buffer
	chunk := make([]byte, 4096)
	for {
		n, _, err := unix.Recvfrom(socket, chunk, unix.MSG_DONTWAIT)
		if n > 0 {
			_, _ = output.Write(chunk[:n])
			if strings.Contains(output.String(), needle) {
				return output.Bytes(), nil
			}
		}
		if err != nil && !releaseSocketWouldBlock(err) {
			return output.Bytes(), err
		}
		if n == 0 && err == nil {
			return output.Bytes(), io.EOF
		}
		select {
		case <-ctx.Done():
			return output.Bytes(), ctx.Err()
		case <-ticker.C:
		}
	}
}

func fillReleaseSocket(socket int) error {
	if err := unix.SetNonblock(socket, true); err != nil {
		return fmt.Errorf("set stdout socket nonblocking: %w", err)
	}
	total := 0
	var fillErr error
	for _, size := range []int{1024, 128, 16, 1} {
		payload := bytes.Repeat([]byte{' '}, size)
		for {
			n, err := unix.Write(socket, payload)
			if n > 0 {
				total += n
			}
			if err == nil {
				continue
			}
			if releaseSocketWouldBlock(err) {
				break
			}
			fillErr = err
			break
		}
		if fillErr != nil {
			break
		}
	}
	if err := unix.SetNonblock(socket, false); err != nil {
		return fmt.Errorf("restore blocking stdout socket: %w", err)
	}
	if fillErr != nil {
		return fillErr
	}
	if total == 0 {
		return fmt.Errorf("stdout socket accepted no saturation bytes")
	}
	return nil
}

func releaseSocketWouldBlock(err error) bool {
	return err == unix.EAGAIN || err == unix.EWOULDBLOCK
}

func waitForReleaseDockerRecordClass(ctx context.Context, root, class string) error {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	needle := `"class":"` + class + `"`
	for {
		raw, err := os.ReadFile(filepath.Join(root, "calls.jsonl"))
		if err == nil && strings.Contains(string(raw), needle) {
			return nil
		}
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func writeReleaseObserverOverflowFixture(t *testing.T, repo, root string, eventCount int) {
	t.Helper()
	writeReleaseFile(t, filepath.Join(root, "go.mod"), "module release-observer-overflow\n\ngo 1.23.0\n")
	writeReleaseFile(t, filepath.Join(root, "swarm.yaml"), "runtime:\n  execution_posture: live\n")
	copyReleaseTree(t,
		filepath.Join(repo, "internal", "releasee2e", "testdata", "claude_cli_managed_lifecycle"),
		filepath.Join(root, "contracts"),
	)
	requests := make([]string, eventCount)
	for i := range requests {
		requests[i] = fmt.Sprintf("request-%04d", i)
	}
	payload, err := json.Marshal(map[string]any{"request": requests})
	if err != nil {
		t.Fatal(err)
	}
	writeReleaseFile(t, filepath.Join(root, "payload.json"), string(payload))
	flowRoot := filepath.Join(root, "contracts", "flows", "worker")
	writeReleaseFile(t, filepath.Join(flowRoot, "entities.yaml"), "worker_state:\n  requests: \"[text]\"\n")
	writeReleaseFile(t, filepath.Join(flowRoot, "events.yaml"), "task.assigned:\n  request: \"[text]\"\nagent.requested:\n  request: \"[text]\"\nagent.completed:\n  flow_result: text\ncompletion.item:\n  request: text\n")
	writeReleaseFile(t, filepath.Join(flowRoot, "schema.yaml"), "name: claude-cli-release-worker\nmode: singleton\nstages:\n  pending:\n    initial: true\n  active:\n    timers:\n      - id: complete_after_overflow\n        after: 10s\n        advances_to: done\n  done:\n    terminal: true\npins:\n  inputs:\n    events:\n      - name: task_assigned\n        event: task.assigned\n        source: external\n  outputs:\n    events: []\n")
	writeReleaseFile(t, filepath.Join(flowRoot, "nodes.yaml"), fmt.Sprintf(`intake:
  id: intake
  execution_type: system_node
  subscribes_to: [task.assigned]
  produces: [agent.requested]
  event_handlers:
    task.assigned:
      create_entity: true
      advances_to: active
      data_accumulation:
        writes:
          - source_field: request
            target_field: requests
      emit:
        event: agent.requested

worker-completion:
  id: worker-completion
  execution_type: system_node
  subscribes_to: [agent.completed]
  produces: [completion.item]
  event_handlers:
    agent.completed:
      fan_out:
        items_from: entity.requests
        as: completed_request
        identity: completed_request
        max_items: 100
        emit:
          event: completion.item
          fields:
            request: completed_request

completion-sink:
  id: completion-sink
  execution_type: system_node
  subscribes_to: [completion.item]
  event_handlers:
    completion.item:
      advances_to: active
`))
}

type releaseSignalBuffer struct {
	needle string
	signal chan struct{}
	once   sync.Once
	mu     sync.Mutex
	buf    bytes.Buffer
}

func newReleaseSignalBuffer(needle string) *releaseSignalBuffer {
	return &releaseSignalBuffer{needle: needle, signal: make(chan struct{})}
}

func (b *releaseSignalBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n, err := b.buf.Write(p)
	if strings.Contains(b.buf.String(), b.needle) {
		b.once.Do(func() { close(b.signal) })
	}
	return n, err
}

func (b *releaseSignalBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
