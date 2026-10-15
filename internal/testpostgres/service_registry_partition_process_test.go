//go:build aix || darwin || dragonfly || freebsd || hurd || illumos || linux || netbsd || openbsd || solaris

package testpostgres

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func proveIndependentRegistryProcesses(t *testing.T) {
	if os.Getenv(RunWrapperEnv) == "1" {
		t.Skip("wrapper-of-wrapper Docker proof runs only from the unwrapped semantic-smoke topology")
	}
	docker, err := exec.LookPath("docker")
	if err != nil {
		t.Skip("Docker is required for the independent-registry supported-path proof")
	}
	if out, err := exec.Command(docker, "info").CombinedOutput(); err != nil {
		t.Skipf("Docker daemon is unavailable: %v %s", err, out)
	}
	root := t.TempDir()
	_, source, _, _ := runtime.Caller(0)
	repo := filepath.Clean(filepath.Join(filepath.Dir(source), "..", ".."))
	runner := filepath.Join(root, "swarm-test")
	build := exec.Command("go", "build", "-o", runner, "./cmd/swarm-test")
	build.Dir = repo
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build runner: %v\n%s", err, out)
	}
	stub := registryFixtureGoHeader() + `test "$1" = test
printf '%s\n' "$SWARM_TEST_POSTGRES_DSN" > "$SWARM_TEST_PROCESS_PREFIX.dsn"
touch "$SWARM_TEST_PROCESS_PREFIX.started"
while [ ! -f "$SWARM_TEST_PROCESS_PREFIX.release" ]; do sleep .02; done
`
	if err := os.WriteFile(filepath.Join(root, "go"), []byte(stub), 0o700); err != nil {
		t.Fatal(err)
	}
	first := startNamespaceProcess(t, repo, root, runner, docker)
	second := startNamespaceProcess(t, repo, root, runner, docker)
	left, right := requireNamespaceProcessRecord(t, first), requireNamespaceProcessRecord(t, second)
	if left.OwnerID == right.OwnerID || left.ContainerID == right.ContainerID {
		t.Fatalf("independent registries shared service identity: first=%+v second=%+v", left, right)
	}
	firstBefore, secondBefore := snapshotServiceFiles(t, first.registry.StateRoot), snapshotServiceFiles(t, second.registry.StateRoot)
	if err := first.registry.Reconcile(context.Background()); err != nil {
		t.Fatal("first registry refused its live neighbor", err)
	}
	assertServiceFilesUnchanged(t, second.registry.StateRoot, secondBefore)
	if err := second.registry.Reconcile(context.Background()); err != nil {
		t.Fatal("second registry refused its live neighbor", err)
	}
	assertServiceFilesUnchanged(t, first.registry.StateRoot, firstBefore)
	first.finish(t)
	if doc, err := first.registry.registrySnapshot(); err != nil || len(doc.Services) != 0 {
		t.Fatalf("first owner did not retire only its service: %+v %v", doc, err)
	}
	assertServiceFilesUnchanged(t, second.registry.StateRoot, secondBefore)
	if got := requireNamespaceProcessRecord(t, second); got.ContainerID != right.ContainerID {
		t.Fatal("first owner retirement changed its neighbor")
	}
	second.finish(t)
	if doc, err := second.registry.registrySnapshot(); err != nil || len(doc.Services) != 0 {
		t.Fatalf("second owner did not retire its service: %+v %v", doc, err)
	}
	t.Log("two supported runner processes used separate registries on one daemon; each retained and retired only its own exact service")
}

type namespaceProcess struct {
	registry *ServiceRegistry
	prefix   string
	log      string
	command  *exec.Cmd
	done     chan error
	finished bool
}

func startNamespaceProcess(t *testing.T, repo, binRoot, runner, docker string) *namespaceProcess {
	t.Helper()
	root := t.TempDir()
	stateHome := filepath.Join(root, "state-home")
	p := &namespaceProcess{
		registry: NewServiceRegistry(filepath.Join(stateHome, "swarm", "test-postgres"), docker),
		prefix:   filepath.Join(root, "child"), log: filepath.Join(root, "runner.log"), done: make(chan error, 1),
	}
	log, err := os.Create(p.log)
	if err != nil {
		t.Fatal(err)
	}
	p.command = exec.Command(runner, "--", "./internal/testpostgres", "-run", "^TestRunCapacityFromEnvironment$", "-count=1")
	p.command.Dir = repo
	p.command.Env = descendantSettlementEnvironment(os.Environ(), binRoot, stateHome, false, "SWARM_TEST_PROCESS_PREFIX="+p.prefix)
	p.command.Stdout, p.command.Stderr = log, log
	if err := p.command.Start(); err != nil {
		_ = log.Close()
		t.Fatal(err)
	}
	go func() { p.done <- p.command.Wait() }()
	t.Cleanup(func() {
		_ = os.WriteFile(p.prefix+".release", []byte("release\n"), 0o600)
		if !p.finished {
			select {
			case <-p.done:
			case <-time.After(5 * time.Second):
				_ = p.command.Process.Kill()
				<-p.done
			}
		}
		_ = log.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := p.registry.Reconcile(ctx); err != nil {
			t.Errorf("exact namespace cleanup: %v", err)
		}
	})
	waitForRunnerPath(t, p.prefix+".started", p.done, []string{p.log}, 2*time.Minute)
	return p
}

func (p *namespaceProcess) finish(t *testing.T) {
	t.Helper()
	if err := os.WriteFile(p.prefix+".release", []byte("release\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := waitForProcess(t, p.done, time.Minute)
	p.finished = true
	if err != nil {
		t.Fatalf("namespace runner: %v\n%s", err, readRunnerLogs([]string{p.log}))
	}
}

func requireNamespaceProcessRecord(t *testing.T, p *namespaceProcess) ServiceRecord {
	t.Helper()
	doc, err := p.registry.registrySnapshot()
	if err != nil || len(doc.Services) != 1 {
		t.Fatalf("expected one exact namespace service: %+v %v", doc, err)
	}
	for _, record := range doc.Services {
		got, err := p.registry.inspectExact(context.Background(), record)
		if err != nil || record.State != ServiceChildRunning || !got.State.Running {
			t.Fatalf("namespace service is not live: record=%+v running=%t err=%v", record, got.State.Running, err)
		}
		assertServiceAuthorityPresent(t, p.registry, record)
		return record
	}
	t.Fatal("missing required namespace service")
	return ServiceRecord{}
}
