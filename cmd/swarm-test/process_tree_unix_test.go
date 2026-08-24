//go:build aix || darwin || dragonfly || freebsd || hurd || illumos || linux || netbsd || openbsd || solaris

package main

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/testpostgres"
)

const (
	signalFixtureEnv      = "SWARM_TEST_SIGNAL_PROCESS_FIXTURE"
	signalFixturePIDEnv   = "SWARM_TEST_SIGNAL_PROCESS_PID"
	signalFixtureStateDSN = "postgres://swarm:secret@127.0.0.1:1/postgres?sslmode=disable"
)

func TestSwarmTestSignalTerminatesProcessTreeAndPreservesResult(t *testing.T) {
	if os.Getenv(signalFixtureEnv) == "1" {
		pidPath := os.Getenv(signalFixturePIDEnv)
		if err := os.WriteFile(pidPath, []byte(strconv.Itoa(os.Getpid())+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		for {
			time.Sleep(time.Hour)
		}
	}

	runner, repoRoot := buildSignalTestRunner(t)
	for _, test := range []struct {
		name     string
		signal   syscall.Signal
		exitCode int
	}{{"interrupt", syscall.SIGINT, 130}, {"terminate", syscall.SIGTERM, 143}} {
		t.Run(test.name, func(t *testing.T) {
			stateHome := filepath.Join(t.TempDir(), "state")
			pidPath := filepath.Join(t.TempDir(), "child.pid")
			command := exec.Command(runner, "--", "./cmd/swarm-test", "-run", "^TestSwarmTestSignalTerminatesProcessTreeAndPreservesResult$", "-count=1")
			command.Dir = repoRoot
			command.Env = signalTestEnvironment(os.Environ(), stateHome,
				signalFixtureEnv+"=1", signalFixturePIDEnv+"="+pidPath)
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			pid := waitForSignalFixturePID(t, pidPath)
			if err := command.Process.Signal(test.signal); err != nil {
				t.Fatal(err)
			}
			assertSignalExitCode(t, command.Wait(), test.exitCode)
			waitForProcessAbsent(t, pid)
			assertSignalContenderHandoff(t, runner, repoRoot, stateHome)
		})
	}
}

func TestSwarmTestQueuedSignalResultAndTicketCleanup(t *testing.T) {
	runner, repoRoot := buildSignalTestRunner(t)
	for _, test := range []struct {
		name     string
		signal   syscall.Signal
		exitCode int
	}{{"interrupt", syscall.SIGINT, 130}, {"terminate", syscall.SIGTERM, 143}} {
		t.Run(test.name, func(t *testing.T) {
			stateHome := filepath.Join(t.TempDir(), "state")
			stateRoot := filepath.Join(stateHome, "swarm", "test-postgres")
			admission := testpostgres.NewRunAdmission(stateRoot, nil)
			active, err := admission.Acquire(context.Background(), testpostgres.RunCommand{Args: []string{"go", "test", "./blocker"}}, 1)
			if err != nil {
				t.Fatal(err)
			}
			defer active.Complete(false)

			logPath := filepath.Join(t.TempDir(), "queued.log")
			logFile, err := os.Create(logPath)
			if err != nil {
				t.Fatal(err)
			}
			command := exec.Command(runner, "--", "./cmd/swarm-test", "-run", "^TestParseTestArgs$", "-count=1")
			command.Dir = repoRoot
			command.Env = signalTestEnvironment(os.Environ(), stateHome)
			command.Stdout, command.Stderr = logFile, logFile
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			waitForSignalQueueOutput(t, logPath)
			if err := command.Process.Signal(test.signal); err != nil {
				t.Fatal(err)
			}
			assertSignalExitCode(t, command.Wait(), test.exitCode)
			if err := logFile.Close(); err != nil {
				t.Fatal(err)
			}
			if err := active.Complete(false); err != nil {
				t.Fatal(err)
			}
			assertSignalContenderHandoff(t, runner, repoRoot, stateHome)
		})
	}
}

func buildSignalTestRunner(t *testing.T) (string, string) {
	t.Helper()
	_, file, _, _ := runtime.Caller(0)
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	runner := filepath.Join(t.TempDir(), "swarm-test")
	command := exec.Command("go", "build", "-o", runner, "./cmd/swarm-test")
	command.Dir = repoRoot
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build swarm-test: %v\n%s", err, output)
	}
	return runner, repoRoot
}

func signalTestEnvironment(env []string, stateHome string, extra ...string) []string {
	filtered := make([]string, 0, len(env)+len(extra)+2)
	for _, entry := range env {
		key, _, _ := strings.Cut(entry, "=")
		if key == "XDG_STATE_HOME" || key == testpostgres.SourceEnv || key == testpostgres.RunWrapperEnv || key == signalFixtureEnv || key == signalFixturePIDEnv {
			continue
		}
		filtered = append(filtered, entry)
	}
	filtered = append(filtered, "XDG_STATE_HOME="+stateHome, testpostgres.SourceEnv+"="+signalFixtureStateDSN)
	return append(filtered, extra...)
}

func waitForSignalFixturePID(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(path)
		if err == nil {
			pid, parseErr := strconv.Atoi(strings.TrimSpace(string(raw)))
			if parseErr != nil {
				t.Fatal(parseErr)
			}
			return pid
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for signal fixture PID at %s", path)
	return 0
}

func waitForSignalQueueOutput(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		raw, _ := os.ReadFile(path)
		if strings.Contains(string(raw), "Test capacity is busy.") {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for queued output at %s", path)
}

func assertSignalExitCode(t *testing.T, err error, want int) {
	t.Helper()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != want {
		t.Fatalf("process result = %v, want exit %d", err, want)
	}
}

func waitForProcessAbsent(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("test descendant PID %d survived process-tree signal", pid)
}

func assertSignalContenderHandoff(t *testing.T, runner, repoRoot, stateHome string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, runner, "--", "./cmd/swarm-test", "-run", "^TestParseTestArgs$", "-count=1")
	command.Dir = repoRoot
	command.Env = signalTestEnvironment(os.Environ(), stateHome)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("contender handoff: %v\n%s", err, output)
	}
}
