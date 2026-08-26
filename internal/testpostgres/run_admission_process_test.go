//go:build aix || darwin || dragonfly || freebsd || hurd || illumos || linux || netbsd || openbsd || solaris

package testpostgres

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestRunSlotSurvivesSupervisorDeathUntilChildExit(t *testing.T) {
	if os.Getenv("SWARM_RUN_LEASE_OWNER_FIXTURE") == "1" {
		runLeaseOwnerFixture()
		return
	}
	root := t.TempDir()
	started := filepath.Join(root, "child-started")
	release := filepath.Join(root, "child-release")
	owner := exec.Command(os.Args[0], "-test.run=^TestRunSlotSurvivesSupervisorDeathUntilChildExit$")
	owner.Env = append(os.Environ(),
		"SWARM_RUN_LEASE_OWNER_FIXTURE=1",
		"SWARM_RUN_LEASE_STATE="+filepath.Join(root, "state"),
		"SWARM_RUN_LEASE_STARTED="+started,
		"SWARM_RUN_LEASE_RELEASE="+release,
	)
	if output, err := owner.CombinedOutput(); err != nil {
		t.Fatalf("lease owner fixture: %v\n%s", err, output)
	}
	waitForRunLeaseFile(t, started, 2*time.Second)

	admission := testRunAdmission(filepath.Join(root, "state"), nil)
	blockedCtx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	if _, err := admission.Acquire(blockedCtx, testRunCommand("blocked"), 1); err == nil {
		t.Fatal("slot released when supervisor died while child survived")
	}
	if err := os.WriteFile(release, []byte("release\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancelAcquire := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelAcquire()
	lease := acquireTestRun(t, admission, ctx, "after-child", 1)
	if err := lease.Complete(context.Background(), false); err != nil {
		t.Fatal(err)
	}
}

func TestRunCompletionDoesNotUnlockSurvivingDescendant(t *testing.T) {
	root := t.TempDir()
	started := filepath.Join(root, "descendant-started")
	release := filepath.Join(root, "descendant-release")
	admission := testRunAdmission(filepath.Join(root, "state"), nil)
	lease := acquireTestRun(t, admission, context.Background(), "owner", 1)
	script := `sh -c 'touch "$1"; while [ ! -f "$2" ]; do sleep .01; done' descendant "$1" "$2" >/dev/null 2>&1 &`
	child := exec.Command("sh", "-c", script, "supervisor-child", started, release)
	if err := lease.InheritTo(child); err != nil {
		t.Fatal(err)
	}
	if err := child.Run(); err != nil {
		t.Fatal(err)
	}
	waitForRunLeaseFile(t, started, 2*time.Second)
	completionCtx, cancelCompletion := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancelCompletion()
	if err := lease.Complete(completionCtx, false); err == nil {
		t.Fatal("completion unlocked a slot still held by a descendant")
	}
	blockedCtx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	if _, err := admission.Acquire(blockedCtx, testRunCommand("blocked"), 1); err == nil {
		t.Fatal("contender acquired while descendant retained the slot")
	}
	if err := os.WriteFile(release, []byte("release\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancelAcquire := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelAcquire()
	after := acquireTestRun(t, admission, ctx, "after-descendant", 1)
	if err := after.Complete(context.Background(), false); err != nil {
		t.Fatal(err)
	}
}

func runLeaseOwnerFixture() {
	state := os.Getenv("SWARM_RUN_LEASE_STATE")
	started := os.Getenv("SWARM_RUN_LEASE_STARTED")
	release := os.Getenv("SWARM_RUN_LEASE_RELEASE")
	admission := testRunAdmission(state, nil)
	lease, err := admission.Acquire(context.Background(), testRunCommand("owner"), 1)
	if err != nil {
		os.Exit(2)
	}
	script := `touch "$1"; while [ ! -f "$2" ]; do sleep .01; done`
	child := exec.Command("sh", "-c", script, "lease-child", started, release)
	if err := lease.InheritTo(child); err != nil {
		os.Exit(3)
	}
	if err := child.Start(); err != nil {
		os.Exit(4)
	}
	waitForPathProcess(started, 2*time.Second)
	os.Exit(0)
}

func waitForPathProcess(path string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(time.Millisecond)
	}
	os.Exit(5)
}

func waitForRunLeaseFile(t *testing.T, path string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", path)
}
