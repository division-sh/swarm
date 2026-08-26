//go:build aix || darwin || dragonfly || freebsd || hurd || illumos || linux || netbsd || openbsd || solaris

package testpostgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestCreatorFenceSurvivesRunnerDeathUntilTerminalHandoff(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 is required for the deterministic Docker process fixture")
	}
	root := t.TempDir()
	dockerState := filepath.Join(root, "docker-state")
	if err := os.MkdirAll(dockerState, 0o700); err != nil {
		t.Fatal(err)
	}
	dockerBin := filepath.Join(root, "docker")
	writeFakeDocker(t, dockerBin, python, dockerState)
	runner := filepath.Join(root, "swarm-test")
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
	build := exec.Command("go", "build", "-o", runner, "./cmd/swarm-test")
	build.Dir = repoRoot
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build runner: %v\n%s", err, out)
	}

	stateHome := filepath.Join(root, "state-home")
	run := exec.Command(runner, "--", "./internal/testpostgres", "-run", "^TestRunCapacityFromEnvironment$", "-count=1")
	run.Dir = repoRoot
	run.Env = append(withoutPostgresConnectionEnv(os.Environ()), "PATH="+root+string(os.PathListSeparator)+os.Getenv("PATH"), "XDG_STATE_HOME="+stateHome)
	if out, err := os.Create(filepath.Join(root, "runner.log")); err != nil {
		t.Fatal(err)
	} else {
		defer out.Close()
		run.Stdout, run.Stderr = out, out
	}
	if err := run.Start(); err != nil {
		t.Fatal(err)
	}
	waitForPath(t, filepath.Join(dockerState, "create-started"), 20*time.Second)

	stateRoot := filepath.Join(stateHome, "swarm", "test-postgres")
	registry := NewServiceRegistry(stateRoot, dockerBin)
	record := waitForSingleServiceState(t, registry, ServiceCreating, 10*time.Second)
	if err := run.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = run.Wait()

	err = registry.Reconcile(context.Background())
	if err == nil || !strings.Contains(err.Error(), "still in flight") {
		t.Fatalf("reconcile during surviving creator = %v", err)
	}
	if got := countLines(t, filepath.Join(dockerState, "create-count")); got != 1 {
		t.Fatalf("docker create count during reconciliation = %d, want 1", got)
	}
	if _, err := registry.record(record.LeaseID); err != nil {
		t.Fatalf("in-flight evidence removed: %v", err)
	}

	if err := os.WriteFile(filepath.Join(dockerState, "release-create"), []byte("release\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	waitForSingleServiceStateWithDiagnostics(t, registry, ServiceCreateSucceeded, 60*time.Second, filepath.Join(root, "runner.log"), dockerState)
	waitForCreatorFenceRelease(t, registry, record.LeaseID, 10*time.Second)
	if err := registry.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.record(record.LeaseID); !os.IsNotExist(err) {
		t.Fatalf("terminal stale service record survives: %v", err)
	}
	if got := countLines(t, filepath.Join(dockerState, "create-count")); got != 1 {
		t.Fatalf("docker create count after terminal reconciliation = %d, want 1", got)
	}
}

func TestServiceCloseTimeoutRetainsDescendantAuthorityForReconciliation(t *testing.T) {
	registry, record := terminalRegistryRecord(t, ServiceReady)
	lease, acquired, err := acquireFileLock(registry.leasePath(record.LeaseID), false)
	if err != nil || !acquired {
		t.Fatalf("acquire service lease: acquired=%v err=%v", acquired, err)
	}
	service := &Service{registry: registry, record: record, lease: lease}
	root := t.TempDir()
	started := filepath.Join(root, "descendant-started")
	release := filepath.Join(root, "descendant-release")
	finished := filepath.Join(root, "descendant-finished")
	script := `
(
  trap '' INT TERM HUP
  touch "$1"
  while [ ! -f "$2" ]; do sleep .01; done
  touch "$3"
) &
`
	child := exec.Command("sh", "-c", script, "service-child", started, release, finished)
	if err := service.InheritLeaseTo(child); err != nil {
		t.Fatal(err)
	}
	if err := child.Run(); err != nil {
		t.Fatal(err)
	}
	waitForPath(t, started, 2*time.Second)

	closeCtx, cancelClose := context.WithTimeout(context.Background(), 80*time.Millisecond)
	err = service.Close(closeCtx)
	cancelClose()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("service close while descendant retained authority = %v, want deadline", err)
	}
	if _, err := registry.record(record.LeaseID); err != nil {
		t.Fatalf("service evidence removed on timeout: %v", err)
	}
	probe, acquired, err := acquireFileLock(registry.leasePath(record.LeaseID), true)
	if err != nil {
		t.Fatal(err)
	}
	if acquired {
		_ = probe.Close()
		t.Fatal("service lease unlocked on timeout while descendant retained authority")
	}
	if containsCall(registry.docker.(*fakeDocker).calls, "rm --force") {
		t.Fatal("service container removal started before descendant quiescence")
	}

	if err := os.WriteFile(release, []byte("release\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	waitForPath(t, finished, 2*time.Second)
	if err := registry.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertServiceAuthorityAbsent(t, registry, record)
}

func TestSwarmTestDockerRunnersQueueBeforeSecondProvision(t *testing.T) {
	if os.Getenv(RunWrapperEnv) == "1" {
		t.Skip("wrapper-of-wrapper Docker proof runs only from the unwrapped semantic-smoke topology")
	}
	docker, err := exec.LookPath("docker")
	if err != nil {
		t.Skip("Docker is required for the concurrent supported-path proof")
	}
	probe := exec.Command(docker, "info")
	if output, err := probe.CombinedOutput(); err != nil {
		t.Skipf("Docker daemon is unavailable: %v (%s)", err, strings.TrimSpace(string(output)))
	}
	root := t.TempDir()
	runner := filepath.Join(root, "swarm-test")
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
	build := exec.Command("go", "build", "-o", runner, "./cmd/swarm-test")
	build.Dir = repoRoot
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build runner: %v\n%s", err, output)
	}
	goBin := filepath.Join(root, "go")
	if err := os.WriteFile(goBin, []byte("#!/bin/sh\nset -eu\ntest \"$1\" = test\nprintf '%s\\n' \"$SWARM_TEST_POSTGRES_DSN\" > \"$SWARM_TEST_PROCESS_PREFIX.dsn\"\ntouch \"$SWARM_TEST_PROCESS_PREFIX.started\"\nwhile [ ! -f \"$SWARM_TEST_PROCESS_PREFIX.release\" ]; do sleep .02; done\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	stateHome := filepath.Join(root, "state-home")
	stateRoot := filepath.Join(stateHome, "swarm", "test-postgres")
	registry := NewServiceRegistry(stateRoot, docker)
	if err := registry.initialize(); err != nil {
		t.Fatal(err)
	}
	prefixes := []string{filepath.Join(root, "first"), filepath.Join(root, "second")}
	commands := make([]*exec.Cmd, 0, len(prefixes))
	done := make([]chan error, 0, len(prefixes))
	finished := make([]bool, len(prefixes))
	logPaths := make([]string, 0, len(prefixes))
	for index, prefix := range prefixes {
		command := exec.Command(runner, "--", "./internal/testpostgres", "-run", "^TestRunCapacityFromEnvironment$", "-count=1")
		command.Env = append(withoutPostgresConnectionEnv(os.Environ()),
			"PATH="+root+string(os.PathListSeparator)+os.Getenv("PATH"),
			"XDG_STATE_HOME="+stateHome,
			"SWARM_TEST_PROCESS_PREFIX="+prefix,
		)
		logPath := filepath.Join(root, fmt.Sprintf("runner-%d.log", index))
		logPaths = append(logPaths, logPath)
		logFile, err := os.Create(logPath)
		if err != nil {
			t.Fatal(err)
		}
		defer logFile.Close()
		command.Stdout, command.Stderr = logFile, logFile
		if err := command.Start(); err != nil {
			t.Fatal(err)
		}
		commands = append(commands, command)
		result := make(chan error, 1)
		done = append(done, result)
		go func() { result <- command.Wait() }()
		if index == 0 {
			waitForRunnerPath(t, prefix+".started", done[index], logPaths, 2*time.Minute)
		} else {
			waitForWaitingRuns(t, NewRunAdmission(stateRoot, nil), 1)
			if _, err := os.Stat(prefix + ".started"); !os.IsNotExist(err) {
				t.Fatalf("second child started before admission: %v", err)
			}
			doc, err := registry.loadRegistry()
			if err != nil || len(doc.Services) != 1 {
				t.Fatalf("services while second queued = %d err=%v, want one", len(doc.Services), err)
			}
		}
	}
	defer func() {
		for _, prefix := range prefixes {
			_ = os.WriteFile(prefix+".release", []byte("release\n"), 0o600)
		}
		for index, command := range commands {
			if !finished[index] {
				select {
				case <-done[index]:
				case <-time.After(5 * time.Second):
					_ = command.Process.Kill()
				}
			}
		}
		_ = registry.Reconcile(context.Background())
	}()
	if err := os.WriteFile(prefixes[0]+".release", []byte("release\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := waitForProcess(t, done[0], time.Minute); err != nil {
		t.Fatalf("first runner: %v", err)
	}
	finished[0] = true
	waitForRunnerPath(t, prefixes[1]+".started", done[1], logPaths, 2*time.Minute)
	doc, err := registry.loadRegistry()
	if err != nil || len(doc.Services) != 1 {
		t.Fatalf("services after FIFO handoff = %d err=%v, want one", len(doc.Services), err)
	}

	if err := os.WriteFile(prefixes[1]+".release", []byte("release\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := waitForProcess(t, done[1], time.Minute); err != nil {
		t.Fatalf("second runner: %v", err)
	}
	finished[1] = true
}

func TestSwarmTestJoinsDescendantAuthorityBeforeSettlement(t *testing.T) {
	docker, err := exec.LookPath("docker")
	dockerAvailable := err == nil
	if dockerAvailable {
		probe := exec.Command(docker, "info")
		if output, probeErr := probe.CombinedOutput(); probeErr != nil {
			t.Logf("Docker daemon is unavailable: %v (%s)", probeErr, strings.TrimSpace(string(output)))
			dockerAvailable = false
		}
	}

	root := t.TempDir()
	runner := filepath.Join(root, "swarm-test")
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
	build := exec.Command("go", "build", "-o", runner, "./cmd/swarm-test")
	build.Dir = repoRoot
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build runner: %v\n%s", err, output)
	}

	goBin := filepath.Join(root, "go")
	fixture := `#!/bin/sh
set -eu
test "$1" = test
"$SWARM_TEST_DESCENDANT_HELPER" -test.run='^TestSwarmTestDescendantProcessFixture$' >/dev/null 2>&1 &
case "$SWARM_TEST_DESCENDANT_MODE" in
  normal_success|normal_failure)
    while [ ! -f "$SWARM_TEST_TOP_RELEASE" ]; do sleep .02; done
    if [ "$SWARM_TEST_DESCENDANT_MODE" = normal_failure ]; then exit 23; fi
    exit 0
    ;;
  signal)
    while :; do sleep 1; done
    ;;
  post_exit_signal)
    exit 0
    ;;
  post_exit_completion)
    exit 0
    ;;
  *)
    echo "invalid descendant fixture mode" >&2
    exit 2
    ;;
esac
`
	if err := os.WriteFile(goBin, []byte(fixture), 0o700); err != nil {
		t.Fatal(err)
	}

	for _, test := range []struct {
		name        string
		explicitDSN bool
	}{{name: "explicit_dsn", explicitDSN: true}, {name: "runner_owned_docker"}} {
		t.Run(test.name, func(t *testing.T) {
			if !test.explicitDSN && os.Getenv(RunWrapperEnv) == "1" {
				t.Skip("wrapper-of-wrapper Docker proof runs only from the unwrapped semantic-smoke topology")
			}
			if !test.explicitDSN && !dockerAvailable {
				t.Skip("Docker is required for the runner-owned service proof")
			}
			for _, completion := range []struct {
				name         string
				mode         string
				signal       syscall.Signal
				postExit     bool
				lateSignal   bool
				terminatedBy syscall.Signal
				exitCode     int
				history      int
			}{
				{name: "normal_success", mode: "normal_success", exitCode: 0, history: 1},
				{name: "normal_failure", mode: "normal_failure", exitCode: 23},
				{name: "interrupt", mode: "signal", signal: syscall.SIGINT, exitCode: 130},
				{name: "terminate", mode: "signal", signal: syscall.SIGTERM, exitCode: 143},
				{name: "post_exit_interrupt", mode: "post_exit_signal", signal: syscall.SIGINT, postExit: true, exitCode: 130},
				{name: "post_exit_terminate", mode: "post_exit_signal", signal: syscall.SIGTERM, postExit: true, exitCode: 143},
				{name: "pre_history_terminate", mode: "post_exit_completion", signal: syscall.SIGTERM, postExit: true, lateSignal: true, terminatedBy: syscall.SIGTERM},
			} {
				t.Run(completion.name, func(t *testing.T) {
					if completion.lateSignal && !test.explicitDSN {
						t.Skip("late history-publication signal proof is backend-independent")
					}
					stateHome := filepath.Join(t.TempDir(), "state-home")
					stateRoot := filepath.Join(stateHome, "swarm", "test-postgres")
					started := filepath.Join(t.TempDir(), "descendant-started")
					release := filepath.Join(t.TempDir(), "descendant-release")
					finished := filepath.Join(t.TempDir(), "descendant-finished")
					topRelease := filepath.Join(t.TempDir(), "top-release")
					signalled := filepath.Join(t.TempDir(), "descendant-signalled")
					logPath := filepath.Join(t.TempDir(), "runner.log")
					logFile, err := os.Create(logPath)
					if err != nil {
						t.Fatal(err)
					}

					command := exec.Command(runner, "--", "./internal/testpostgres", "-run", "^TestRunCapacityFromEnvironment$", "-count=1")
					command.Dir = repoRoot
					command.Env = descendantSettlementEnvironment(os.Environ(), root, stateHome, test.explicitDSN,
						"SWARM_TEST_DESCENDANT_STARTED="+started,
						"SWARM_TEST_DESCENDANT_RELEASE="+release,
						"SWARM_TEST_DESCENDANT_FINISHED="+finished,
						"SWARM_TEST_DESCENDANT_SIGNALLED="+signalled,
						"SWARM_TEST_DESCENDANT_MODE="+completion.mode,
						"SWARM_TEST_DESCENDANT_HELPER="+os.Args[0],
						"SWARM_TEST_TOP_RELEASE="+topRelease,
					)
					command.Stdout, command.Stderr = logFile, logFile
					if err := command.Start(); err != nil {
						_ = logFile.Close()
						t.Fatal(err)
					}
					result := make(chan error, 1)
					go func() { result <- command.Wait() }()
					resultRead := false
					resultReady := false
					var earlyResult error
					defer func() {
						_ = os.WriteFile(topRelease, []byte("release\n"), 0o600)
						_ = os.WriteFile(release, []byte("release\n"), 0o600)
						if !resultRead {
							select {
							case <-result:
							case <-time.After(5 * time.Second):
								_ = command.Process.Kill()
							}
						}
						_ = logFile.Close()
						if !test.explicitDSN {
							cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
							defer cancel()
							_ = NewServiceRegistry(stateRoot, docker).Reconcile(cleanupCtx)
						}
					}()

					waitForRunnerPath(t, started, result, []string{logPath}, 2*time.Minute)
					admission := NewRunAdmission(stateRoot, nil)
					if completion.postExit {
						waitForRunSettlementFence(t, admission, 0, 5*time.Second)
					} else if completion.signal != 0 {
						if err := command.Process.Signal(completion.signal); err != nil {
							t.Fatal(err)
						}
					} else if err := os.WriteFile(topRelease, []byte("release\n"), 0o600); err != nil {
						t.Fatal(err)
					}

					type contenderResult struct {
						lease *RunLease
						err   error
					}
					contenderCtx, cancelContender := context.WithTimeout(context.Background(), 45*time.Second)
					defer cancelContender()
					contender := make(chan contenderResult, 1)
					go func() {
						lease, err := admission.Acquire(contenderCtx, RunCommand{Args: []string{"go", "test", "./contender"}}, 1)
						contender <- contenderResult{lease: lease, err: err}
					}()
					select {
					case got := <-contender:
						if got.lease != nil {
							_ = got.lease.Complete(context.Background(), false)
						}
						t.Fatalf("contender settled while descendant held authority: %v", got.err)
					case <-time.After(200 * time.Millisecond):
					}
					before, err := admission.loadRegistry()
					if err != nil {
						t.Fatal(err)
					}
					if len(before.Active) != 1 || len(before.History) != 0 {
						t.Fatalf("run state before descendant release: active=%d history=%d, want 1/0", len(before.Active), len(before.History))
					}

					if !test.explicitDSN {
						registry := NewServiceRegistry(stateRoot, docker)
						doc, err := registry.loadRegistry()
						if err != nil {
							t.Fatal(err)
						}
						if len(doc.Services) != 1 {
							t.Fatalf("services while descendant held authority = %d, want 1", len(doc.Services))
						}
						for _, record := range doc.Services {
							if record.State != ServiceChildRunning {
								t.Fatalf("service state while descendant held authority = %q, want %q", record.State, ServiceChildRunning)
							}
						}
					}

					select {
					case err := <-result:
						resultRead = true
						raw, _ := os.ReadFile(logPath)
						t.Fatalf("wrapper settled before descendant release: %v\n%s", err, raw)
					default:
					}

					if completion.lateSignal {
						registryLock, acquired, err := acquireFileLock(filepath.Join(stateRoot, "runs-v1.lock"), false)
						if err != nil || !acquired {
							t.Fatalf("hold run registry before history publication: acquired=%v err=%v", acquired, err)
						}
						if err := os.WriteFile(release, []byte("release\n"), 0o600); err != nil {
							_ = registryLock.Close()
							t.Fatal(err)
						}
						waitForPath(t, finished, 5*time.Second)
						waitForRunSettlementFenceRelease(t, admission, 0, 5*time.Second)
						time.Sleep(50 * time.Millisecond)
						if err := command.Process.Signal(completion.signal); err != nil {
							_ = registryLock.Close()
							t.Fatal(err)
						}
						select {
						case earlyResult = <-result:
							resultRead = true
							resultReady = true
						case <-time.After(500 * time.Millisecond):
							// A signal accepted just before the relay froze settles
							// normally after the registry fence is released.
						}
						if err := registryLock.Close(); err != nil {
							t.Fatal(err)
						}
					} else if completion.postExit {
						if err := command.Process.Signal(completion.signal); err != nil {
							t.Fatal(err)
						}
						waitForPath(t, signalled, 5*time.Second)
					} else {
						if err := os.WriteFile(release, []byte("release\n"), 0o600); err != nil {
							t.Fatal(err)
						}
					}
					waitForPath(t, finished, 5*time.Second)
					if resultReady {
						err = earlyResult
					} else {
						err = waitForProcess(t, result, 45*time.Second)
						resultRead = true
					}
					if completion.terminatedBy != 0 {
						var exitErr *exec.ExitError
						if !errors.As(err, &exitErr) {
							t.Fatalf("wrapper result after signal freeze = %v, want signal %v", err, completion.terminatedBy)
						}
						status, ok := exitErr.Sys().(syscall.WaitStatus)
						mappedExit := exitErr.ExitCode() == 128+int(completion.terminatedBy)
						processSignal := ok && status.Signaled() && status.Signal() == completion.terminatedBy
						if !mappedExit && !processSignal {
							t.Fatalf("wrapper status after signal freeze = %v, want signal %v", exitErr.Sys(), completion.terminatedBy)
						}
					} else if completion.exitCode == 0 {
						if err != nil {
							raw, _ := os.ReadFile(logPath)
							t.Fatalf("wrapper result after descendant release = %v, want success\n%s", err, raw)
						}
					} else {
						var exitErr *exec.ExitError
						if !errors.As(err, &exitErr) || exitErr.ExitCode() != completion.exitCode {
							raw, _ := os.ReadFile(logPath)
							t.Fatalf("wrapper result after descendant release = %v, want exit %d\n%s", err, completion.exitCode, raw)
						}
					}

					var got contenderResult
					select {
					case got = <-contender:
					case <-time.After(5 * time.Second):
						t.Fatal("contender did not acquire after descendant release")
					}
					if got.err != nil {
						t.Fatalf("contender after descendant release: %v", got.err)
					}
					if err := got.lease.Complete(context.Background(), false); err != nil {
						t.Fatal(err)
					}
					probe, acquired, err := acquireFileLock(admission.slotPath(0), true)
					if err != nil || !acquired {
						t.Fatalf("settled slot authority: acquired=%v err=%v", acquired, err)
					}
					if err := probe.Close(); err != nil {
						t.Fatal(err)
					}
					tickets, err := os.ReadDir(filepath.Join(stateRoot, "run-tickets"))
					if err != nil {
						t.Fatal(err)
					}
					if len(tickets) != 0 {
						t.Fatalf("run ticket authorities after settlement = %v, want none", tickets)
					}

					runs, err := admission.loadRegistry()
					if err != nil {
						t.Fatal(err)
					}
					if len(runs.Active) != 0 || len(runs.Waiting) != 0 || len(runs.History) != completion.history {
						t.Fatalf("run state after settlement: active=%d waiting=%d history=%d, want 0/0/%d", len(runs.Active), len(runs.Waiting), len(runs.History), completion.history)
					}

					if !test.explicitDSN {
						registry := NewServiceRegistry(stateRoot, docker)
						doc, err := registry.loadRegistry()
						if err != nil {
							t.Fatal(err)
						}
						if len(doc.Services) != 0 {
							t.Fatalf("services after descendant release = %d, want 0", len(doc.Services))
						}
						for _, directory := range []string{"leases", "creators", "handoff"} {
							entries, err := os.ReadDir(filepath.Join(stateRoot, directory))
							if err != nil {
								t.Fatal(err)
							}
							if len(entries) != 0 {
								t.Fatalf("service authorities in %s after settlement = %v, want none", directory, entries)
							}
						}
					}
				})
			}
		})
	}
}

func TestSwarmTestDescendantProcessFixture(t *testing.T) {
	mode := os.Getenv("SWARM_TEST_DESCENDANT_MODE")
	if mode == "" {
		return
	}
	started := os.Getenv("SWARM_TEST_DESCENDANT_STARTED")
	release := os.Getenv("SWARM_TEST_DESCENDANT_RELEASE")
	finished := os.Getenv("SWARM_TEST_DESCENDANT_FINISHED")
	if mode == "post_exit_signal" {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
		defer signal.Stop(signals)
		if err := os.WriteFile(started, []byte("started\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		<-signals
		if err := os.WriteFile(os.Getenv("SWARM_TEST_DESCENDANT_SIGNALLED"), []byte("signalled\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	} else {
		if err := os.WriteFile(started, []byte("started\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		signal.Ignore(os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
		for {
			if _, err := os.Stat(release); err == nil {
				break
			} else if !os.IsNotExist(err) {
				t.Fatal(err)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	if err := os.WriteFile(finished, []byte("finished\n"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func descendantSettlementEnvironment(env []string, binRoot, stateHome string, explicitDSN bool, extra ...string) []string {
	filtered := make([]string, 0, len(env)+len(extra)+4)
	for _, entry := range withoutPostgresConnectionEnv(env) {
		key, _, _ := strings.Cut(entry, "=")
		if key == "PATH" || key == "XDG_STATE_HOME" || key == RunWrapperEnv || key == RunCapacityEnv || strings.HasPrefix(key, "SWARM_TEST_DESCENDANT_") {
			continue
		}
		filtered = append(filtered, entry)
	}
	filtered = append(filtered,
		"PATH="+binRoot+string(os.PathListSeparator)+os.Getenv("PATH"),
		"XDG_STATE_HOME="+stateHome,
		RunCapacityEnv+"=1",
	)
	if explicitDSN {
		filtered = append(filtered, SourceEnv+"=postgres://swarm:secret@127.0.0.1:1/postgres?sslmode=disable")
	}
	return append(filtered, extra...)
}

func TestSwarmTestProcessFixture(t *testing.T) {
	if os.Getenv("SWARM_TEST_PROCESS_FIXTURE_FAIL") == "1" {
		t.Fatal("requested child failure")
	}
	pidPath := os.Getenv("SWARM_TEST_PROCESS_FIXTURE_PID")
	if pidPath == "" {
		return
	}
	if err := os.WriteFile(pidPath, []byte(strconv.Itoa(os.Getpid())+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for {
		time.Sleep(time.Hour)
	}
}

func waitForRunnerPath(t *testing.T, path string, result <-chan error, logs []string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		select {
		case err := <-result:
			t.Fatalf("runner exited before %s: %v\n%s", path, err, readRunnerLogs(logs))
		default:
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s\n%s", path, readRunnerLogs(logs))
}

func readRunnerLogs(paths []string) string {
	var values []string
	for _, path := range paths {
		raw, _ := os.ReadFile(path)
		values = append(values, path+":\n"+string(raw))
	}
	return strings.Join(values, "\n")
}

func waitForRunnerServiceRecord(t *testing.T, registry *ServiceRegistry, dsnPath string, timeout time.Duration) ServiceRecord {
	t.Helper()
	raw, err := os.ReadFile(dsnPath)
	if err != nil {
		t.Fatalf("read runner DSN: %v", err)
	}
	connection, err := ParseConnection(string(raw))
	if err != nil {
		t.Fatalf("parse runner DSN: %v", err)
	}
	wantPort := connection.Parameters().Port
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		doc, loadErr := registry.loadRegistry()
		if loadErr == nil {
			for _, record := range doc.Services {
				if record.State != ServiceChildRunning {
					continue
				}
				portOutput, portErr := registry.dockerOutput(context.Background(), "port", record.ContainerID, "5432/tcp")
				if portErr != nil {
					continue
				}
				port, parseErr := parseDockerPort(portOutput)
				if parseErr == nil && port == wantPort {
					return record
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out resolving runner record for port %d", wantPort)
	return ServiceRecord{}
}

func waitForServiceRecordAbsent(t *testing.T, registry *ServiceRegistry, leaseID string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := registry.record(leaseID); os.IsNotExist(err) {
			for _, path := range []string{
				registry.leasePath(leaseID),
				registry.creatorPath(leaseID),
				filepath.Join(registry.StateRoot, "handoff", leaseID+".cid"),
			} {
				if _, statErr := os.Lstat(path); !os.IsNotExist(statErr) {
					t.Fatalf("retired runner authority remains at %s: %v", path, statErr)
				}
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for runner lease %s to retire", leaseID)
}

func waitForProcess(t *testing.T, result <-chan error, timeout time.Duration) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(timeout):
		t.Fatal("timed out waiting for runner process")
		return nil
	}
}

func waitForCreatorFenceRelease(t *testing.T, registry *ServiceRegistry, leaseID string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		lock, acquired, err := acquireFileLock(registry.creatorPath(leaseID), true)
		if err != nil {
			t.Fatal(err)
		}
		if acquired {
			_ = lock.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for creator fence %s", leaseID)
}

func writeFakeDocker(t *testing.T, path, python, state string) {
	t.Helper()
	script := fmt.Sprintf(`#!%s
import json, os, sys, time
root = %q
args = sys.argv[1:]
def path(name): return os.path.join(root, name)
def load():
    with open(path("inspect.json")) as f: return json.load(f)
with open(path("commands.log"), "a") as f: f.write(" ".join(args) + "\n")
if args[0] == "pull": sys.exit(0)
if args[:3] == ["info", "--format", "{{.ID}}"]:
    print("daemon-id"); sys.exit(0)
if args[:4] == ["image", "inspect", "--format", "{{.Id}}"]:
    print("image-id"); sys.exit(0)
if args[0] == "ps":
    if os.path.exists(path("inspect.json")):
        value = load()[0]
        if args[-1] == "{{.ID}} {{.Names}}": print(value["Id"] + " " + value["Name"].lstrip("/"))
        else: print(value["Id"])
    sys.exit(0)
if args[0] == "create":
    cidfile = args[args.index("--cidfile") + 1]
    with open(cidfile, "w") as f: f.write("container-id\n")
    with open(path("create-count"), "a") as f: f.write("1\n")
    open(path("create-started"), "w").close()
    while not os.path.exists(path("release-create")): time.sleep(.02)
    name = args[args.index("--name") + 1]
    labels = {}
    for i, value in enumerate(args):
        if value == "--label":
            key, label_value = args[i + 1].split("=", 1); labels[key] = label_value
    value = {"Id":"container-id", "Name":"/" + name, "Image":"image-id", "Config":{"Labels":labels}, "State":{"Running":False}}
    with open(path("inspect.json"), "w") as f: json.dump([value], f)
    print("container-id"); sys.exit(0)
if args[0] == "inspect":
    if not os.path.exists(path("inspect.json")):
        print("Error: No such object: " + args[1], file=sys.stderr); sys.exit(1)
    print(json.dumps(load())); sys.exit(0)
if args[0] == "rm":
    os.remove(path("inspect.json")); sys.exit(0)
print("unsupported fake docker command: " + " ".join(args), file=sys.stderr)
sys.exit(2)
`, python, state)
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
}

func waitForPath(t *testing.T, path string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	log, _ := os.ReadFile(filepath.Join(filepath.Dir(filepath.Dir(path)), "runner.log"))
	commands, _ := os.ReadFile(filepath.Join(filepath.Dir(path), "commands.log"))
	t.Fatalf("timed out waiting for %s: runner_log=%s docker_commands=%s", path, log, commands)
}

func waitForSingleServiceState(t *testing.T, registry *ServiceRegistry, state ServiceState, timeout time.Duration) ServiceRecord {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		doc, err := registry.loadRegistry()
		if err == nil && len(doc.Services) == 1 {
			for _, record := range doc.Services {
				if record.State == state {
					return record
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for service state %s", state)
	return ServiceRecord{}
}

func waitForSingleServiceStateWithDiagnostics(t *testing.T, registry *ServiceRegistry, state ServiceState, timeout time.Duration, runnerLog, dockerState string) ServiceRecord {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last registryDocument
	var lastErr error
	for time.Now().Before(deadline) {
		last, lastErr = registry.loadRegistry()
		if lastErr == nil && len(last.Services) == 1 {
			for _, record := range last.Services {
				if record.State == state {
					return record
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	registryJSON, _ := json.Marshal(last)
	log, _ := os.ReadFile(runnerLog)
	commands, _ := os.ReadFile(filepath.Join(dockerState, "commands.log"))
	t.Fatalf("timed out waiting for service state %s: registry=%s load_error=%v runner_log=%s docker_commands=%s", state, registryJSON, lastErr, log, commands)
	return ServiceRecord{}
}

func countLines(t *testing.T, path string) int {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return len(strings.Fields(string(raw)))
}

func TestServiceRegistryProcessFixtureJSONShape(t *testing.T) {
	value := testDockerInspect("id", "lease", "runner")
	if _, err := json.Marshal([]dockerInspect{value}); err != nil {
		t.Fatal(err)
	}
}
