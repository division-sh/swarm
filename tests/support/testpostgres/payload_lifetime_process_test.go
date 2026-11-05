//go:build linux || darwin

package testpostgres

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/tests/support/testutil/testprocess"
)

func TestRunnerPayloadLifetimeAfterOuterDeath(t *testing.T) {
	const fixtureEnv = "SWARM_TEST_RUNNER_PAYLOAD_LIFETIME"
	if root := os.Getenv(fixtureEnv); root != "" {
		writeRegistryGoFixture(t, filepath.Join(root, "go"), registryFixtureGoHeader()+`
test "$1" = test
trap '' INT TERM HUP
printf '%s' "$$" > "$SWARM_TEST_PROCESS_PREFIX.pid"
printf 'write\n' >> "$SWARM_TEST_PROCESS_PREFIX.writes"
touch "$SWARM_TEST_PROCESS_PREFIX.started"
while [ ! -f "$SWARM_TEST_PROCESS_PREFIX.release" ]; do
  printf 'write\n' >> "$SWARM_TEST_PROCESS_PREFIX.writes"
  sleep .01
done
`)
		command := exec.Command(filepath.Join(root, "runner"), "--", "./tests/support/testpostgres", "-run", "^TestRunCapacityFromEnvironment$", "-count=1")
		command.Env = descendantSettlementEnvironment(os.Environ(), root, filepath.Join(root, "state"), true, "SWARM_TEST_PROCESS_PREFIX="+filepath.Join(root, "child"))
		command.Stdout, command.Stderr = os.Stdout, os.Stderr
		if err := testprocess.Start(command); err != nil {
			t.Fatal(err)
		}
		waitForPath(t, filepath.Join(root, "child.started"), 10*time.Second)
		raw, err := os.ReadFile(filepath.Join(root, "child.pid"))
		if err != nil {
			t.Fatal(err)
		}
		pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
		if err != nil {
			t.Fatal(err)
		}
		var identities []testprocess.Lifetime
		for _, pid := range []int{command.Process.Pid, pid} {
			identity, err := testprocess.Observe(pid)
			if err != nil {
				t.Fatal(err)
			}
			identities = append(identities, identity)
		}
		raw, _ = json.Marshal(identities)
		if err := os.WriteFile(filepath.Join(root, "ready"), raw, 0600); err != nil {
			t.Fatal(err)
		}
		for {
			time.Sleep(time.Hour)
		}
	}
	root := t.TempDir()
	_, source, _, _ := runtime.Caller(0)
	build := exec.Command("go", "build", "-o", filepath.Join(root, "runner"), "./cmd/swarm-test")
	build.Dir = filepath.Clean(filepath.Join(filepath.Dir(source), "..", "..", ".."))
	if raw, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build runner: %v\n%s", err, raw)
	}
	parent := exec.Command(os.Args[0], "-test.run=^TestRunnerPayloadLifetimeAfterOuterDeath$")
	parent.Env = append(os.Environ(), fixtureEnv+"="+root)
	log, err := os.Create(filepath.Join(root, "parent.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	parent.Stdout, parent.Stderr = log, log
	if err := testprocess.Start(parent); err != nil {
		t.Fatal(err)
	}
	var identities []testprocess.Lifetime
	t.Cleanup(func() {
		_ = parent.Process.Kill()
		if parent.ProcessState == nil {
			_ = parent.Wait()
		}
		for _, identity := range identities {
			if identity.Alive() {
				process, _ := os.FindProcess(identity.PID)
				_ = process.Kill()
				deadline := time.Now().Add(3 * time.Second)
				for identity.Alive() && time.Now().Before(deadline) {
					time.Sleep(10 * time.Millisecond)
				}
				if identity.Alive() {
					t.Error("proof cleanup did not join owned process termination")
				}
			}
		}
	})
	deadline := time.Now().Add(15 * time.Second)
	for len(identities) != 2 {
		raw, _ := os.ReadFile(filepath.Join(root, "ready"))
		_ = json.Unmarshal(raw, &identities)
		if time.Now().After(deadline) {
			t.Fatal("wrapper and payload did not report readiness")
		}
		time.Sleep(10 * time.Millisecond)
	}
	admission := NewRunAdmission(filepath.Join(root, "state", "swarm", "test-postgres"), nil)
	probe, acquired, err := acquireFileLock(admission.slotPath(0), true)
	if err != nil || acquired {
		if probe != nil {
			_ = probe.Close()
		}
		t.Fatal("live payload did not retain its actual lease", err)
	}
	_ = parent.Process.Kill()
	_ = parent.Wait()
	deadline = time.Now().Add(3 * time.Second)
	for identities[0].Alive() || identities[1].Alive() {
		if time.Now().After(deadline) {
			t.Fatalf("outer death left wrapper=%t payload=%t", identities[0].Alive(), identities[1].Alive())
		}
		time.Sleep(10 * time.Millisecond)
	}
	probe, acquired, err = acquireFileLock(admission.slotPath(0), true)
	// A shell's already-forked sleep has its own short bound. Observe its lease
	// closing within the same deadline, rather than asserting synchronous exit.
	for err == nil && !acquired && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
		probe, acquired, err = acquireFileLock(admission.slotPath(0), true)
	}
	if err != nil || !acquired {
		t.Fatal("terminated payload retained its lease", err)
	}
	_ = probe.Close()
	before, err := os.ReadFile(filepath.Join(root, "child.writes"))
	if err != nil || len(before) == 0 {
		t.Fatal("missing actual writer evidence", err)
	}
	time.Sleep(100 * time.Millisecond)
	after, err := os.ReadFile(filepath.Join(root, "child.writes"))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("payload kept writing after termination", err)
	}
	if err := os.RemoveAll(root); err != nil {
		t.Fatal(err)
	}
}
