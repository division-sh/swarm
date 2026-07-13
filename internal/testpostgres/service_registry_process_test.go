//go:build aix || darwin || dragonfly || freebsd || hurd || illumos || linux || netbsd || openbsd || solaris

package testpostgres

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
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
	runner := filepath.Join(root, "swarm-test-postgres")
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
	build := exec.Command("go", "build", "-o", runner, "./cmd/swarm-test-postgres")
	build.Dir = repoRoot
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build runner: %v\n%s", err, out)
	}

	stateHome := filepath.Join(root, "state-home")
	run := exec.Command(runner, "--", "true")
	run.Env = append(os.Environ(), "PATH="+root+string(os.PathListSeparator)+os.Getenv("PATH"), "XDG_STATE_HOME="+stateHome)
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

func TestServiceRegistryConcurrentLiveRunnersRemainUntouched(t *testing.T) {
	docker, err := exec.LookPath("docker")
	if err != nil {
		t.Skip("Docker is required for the concurrent supported-path proof")
	}
	probe := exec.Command(docker, "info")
	if output, err := probe.CombinedOutput(); err != nil {
		t.Skipf("Docker daemon is unavailable: %v (%s)", err, strings.TrimSpace(string(output)))
	}
	root := t.TempDir()
	runner := filepath.Join(root, "swarm-test-postgres")
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
	build := exec.Command("go", "build", "-o", runner, "./cmd/swarm-test-postgres")
	build.Dir = repoRoot
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build runner: %v\n%s", err, output)
	}
	blocker := filepath.Join(root, "block-child")
	if err := os.WriteFile(blocker, []byte("#!/bin/sh\nset -eu\ntouch \"$1.started\"\nwhile [ ! -f \"$1.release\" ]; do sleep .02; done\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	registry, err := DefaultServiceRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.initialize(); err != nil {
		t.Fatal(err)
	}
	baseline, err := registry.loadRegistry()
	if err != nil {
		t.Fatal(err)
	}
	baselineAuthority := snapshotAuthorityEntries(t, registry)
	prefixes := []string{filepath.Join(root, "first"), filepath.Join(root, "second")}
	commands := make([]*exec.Cmd, 0, len(prefixes))
	done := make([]chan error, 0, len(prefixes))
	finished := make([]bool, len(prefixes))
	logPaths := make([]string, 0, len(prefixes))
	for index, prefix := range prefixes {
		command := exec.Command(runner, "--", blocker, prefix)
		command.Env = withoutPostgresConnectionEnv(os.Environ())
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
	for index, prefix := range prefixes {
		waitForRunnerPath(t, prefix+".started", done[index], logPaths, 2*time.Minute)
	}
	records := waitForServiceRecords(t, registry, len(baseline.Services)+2, ServiceChildRunning, 2*time.Minute)
	for _, record := range records {
		if _, err := registry.inspectExact(context.Background(), record); err != nil {
			t.Fatalf("live runner %s lacks exact container identity: %v", record.LeaseID, err)
		}
	}

	if err := os.WriteFile(prefixes[0]+".release", []byte("release\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := waitForProcess(t, done[0], time.Minute); err != nil {
		t.Fatalf("first runner: %v", err)
	}
	finished[0] = true
	remaining := waitForServiceRecords(t, registry, len(baseline.Services)+1, ServiceChildRunning, time.Minute)
	for _, record := range remaining {
		if _, err := registry.inspectExact(context.Background(), record); err != nil {
			t.Fatalf("second runner was disturbed by first teardown: %v", err)
		}
	}

	if err := os.WriteFile(prefixes[1]+".release", []byte("release\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := waitForProcess(t, done[1], time.Minute); err != nil {
		t.Fatalf("second runner: %v", err)
	}
	finished[1] = true
	final := waitForServiceRecords(t, registry, len(baseline.Services), ServiceChildRunning, time.Minute)
	for leaseID := range baseline.Services {
		if _, ok := final[leaseID]; !ok {
			t.Fatalf("baseline live runner %s was removed by nested runner teardown", leaseID)
		}
	}
	finalAuthority := snapshotAuthorityEntries(t, registry)
	for dir, want := range baselineAuthority {
		if got := finalAuthority[dir]; got != want {
			t.Fatalf("authority directory %s after teardown = %q, want baseline %q", dir, got, want)
		}
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

func snapshotAuthorityEntries(t *testing.T, registry *ServiceRegistry) map[string]string {
	t.Helper()
	result := make(map[string]string)
	for _, dir := range []string{"leases", "creators", "handoff"} {
		entries, err := os.ReadDir(filepath.Join(registry.StateRoot, dir))
		if err != nil {
			t.Fatal(err)
		}
		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		result[dir] = strings.Join(names, "\n")
	}
	return result
}

func waitForServiceRecords(t *testing.T, registry *ServiceRegistry, count int, state ServiceState, timeout time.Duration) map[string]ServiceRecord {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last registryDocument
	var lastErr error
	for time.Now().Before(deadline) {
		last, lastErr = registry.loadRegistry()
		if lastErr == nil && len(last.Services) == count {
			matching := true
			for _, record := range last.Services {
				if state != "" && record.State != state {
					matching = false
					break
				}
			}
			if matching {
				return last.Services
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	encoded, _ := json.Marshal(last)
	t.Fatalf("timed out waiting for %d services in state %q: registry=%s error=%v", count, state, encoded, lastErr)
	return nil
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
	t.Fatalf("timed out waiting for %s", path)
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
