//go:build darwin || linux

package main

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/testplanning"
	"github.com/division-sh/swarm/internal/testpostgres"
)

func TestCompletionWorkersBoundedAndJoined(t *testing.T) {
	for _, mode := range []struct {
		name     string
		capacity int
		failure  bool
	}{{"pass", 2, false}, {"failure", 2, true}, {"serial_no_parent_slot", 1, false}, {"three_workers", 3, false}} {
		t.Run(mode.name, func(t *testing.T) {
			failure := mode.failure
			root := t.TempDir()
			worker := filepath.Join(root, "worker")
			script := "#!/bin/sh\nset -eu\nunit=\"$3\"\n" +
				"trap 'touch \"$WORKER_ROOT/joined-$unit\"; exit 130' INT TERM\n" +
				"touch \"$WORKER_ROOT/started-$unit\"\n" +
				"if [ \"$WORKER_FAILURE\" = true ] && [ \"$unit\" = one ]; then\n" +
				"  while [ ! -f \"$WORKER_ROOT/started-two\" ]; do sleep .01; done\n  exit 7\nfi\n" +
				"while [ ! -f \"$WORKER_ROOT/release\" ]; do sleep .01; done\n" +
				"touch \"$WORKER_ROOT/joined-$unit\"\n"
			if err := os.WriteFile(worker, []byte(script), 0700); err != nil {
				t.Fatal(err)
			}
			plan := testplanning.RunPlan{HeadSHA: "head", Digest: "plan", Profile: testplanning.ProfileFull}
			for _, id := range []string{"one", "two", "three", "four"} {
				plan.Units = append(plan.Units, testplanning.ProofUnit{ID: id})
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			env := append(os.Environ(), "WORKER_ROOT="+root, "WORKER_FAILURE="+map[bool]string{false: "false", true: "true"}[failure])
			finished := make(chan int, 1)
			go func() { finished <- executeCompletionWorkers(ctx, plan, root, worker, env, true, mode.capacity) }()
			for _, id := range []string{"one", "two", "three"}[:mode.capacity] {
				for {
					if _, err := os.Stat(filepath.Join(root, "started-"+id)); err == nil {
						break
					}
					if ctx.Err() != nil {
						t.Fatal("worker did not start")
					}
					time.Sleep(5 * time.Millisecond)
				}
			}
			if _, err := os.Stat(filepath.Join(root, "started-"+[]string{"one", "two", "three", "four"}[mode.capacity])); !os.IsNotExist(err) {
				t.Fatal("capacity was exceeded")
			}
			if !failure {
				if err := os.WriteFile(filepath.Join(root, "release"), []byte("release"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			code := <-finished
			if failure && code != 7 || !failure && code != 0 {
				t.Fatalf("worker result=%d", code)
			}
			var receipt completionWorkersReceipt
			raw, err := os.ReadFile(filepath.Join(root, "workers.json"))
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(raw, &receipt); err != nil {
				t.Fatal(err)
			}
			if len(receipt.Workers) != 4 || receipt.Capacity != mode.capacity || receipt.Plan != plan.Digest {
				t.Fatal("unbound aggregate")
			}
			if failure {
				if receipt.Workers[2].Status != "not_started" || receipt.Workers[3].Status != "not_started" {
					t.Fatal("failed plan launched later units")
				}
				if _, err := os.Stat(filepath.Join(root, "joined-two")); err != nil {
					t.Fatal("canceled sibling was not joined")
				}
			} else {
				for _, row := range receipt.Workers {
					if row.Status != "passed" || row.ExitCode != 0 {
						t.Fatal(row)
					}
				}
				if mode.capacity > 1 && (!receipt.Workers[0].StartedAt.Before(receipt.Workers[1].EndedAt) || !receipt.Workers[1].StartedAt.Before(receipt.Workers[0].EndedAt)) {
					t.Fatal("workers did not overlap")
				}
			}
		})
	}
}

func TestCompletionMultiworkerSignals(t *testing.T) {
	if root := os.Getenv("SWARM_COMPLETION_SIGNAL_FIXTURE"); root != "" {
		plan := testplanning.RunPlan{HeadSHA: "head", Digest: "plan", Profile: testplanning.ProfileFull}
		for _, id := range []string{"one", "two", "three", "four"} {
			plan.Units = append(plan.Units, testplanning.ProofUnit{ID: id})
		}
		os.Exit(runCompletionWithSignals(func(ctx context.Context) int {
			return executeCompletionWorkers(ctx, plan, root, filepath.Join(root, "worker"), os.Environ(), true, 2)
		}))
	}
	for _, row := range []struct {
		name   string
		signal syscall.Signal
		exit   int
	}{{"interrupt", syscall.SIGINT, 130}, {"terminate", syscall.SIGTERM, 143}} {
		t.Run(row.name, func(t *testing.T) {
			root := t.TempDir()
			script := "#!/bin/sh\nset -eu\nunit=\"$3\"\ntrap 'touch \"$SWARM_COMPLETION_SIGNAL_FIXTURE/joined-$unit\"; exit 130' INT TERM\ntouch \"$SWARM_COMPLETION_SIGNAL_FIXTURE/started-$unit\"\nwhile :; do sleep .01; done\n"
			if err := os.WriteFile(filepath.Join(root, "worker"), []byte(script), 0700); err != nil {
				t.Fatal(err)
			}
			command := exec.Command(os.Args[0], "-test.run=^TestCompletionMultiworkerSignals$")
			command.Env = append(os.Environ(), "SWARM_COMPLETION_SIGNAL_FIXTURE="+root)
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if command.ProcessState == nil {
					_ = command.Process.Signal(syscall.SIGTERM)
					_ = command.Wait()
				}
			})
			for _, id := range []string{"one", "two"} {
				waitForTestPath(t, filepath.Join(root, "started-"+id), 10*time.Second)
			}
			if err := command.Process.Signal(row.signal); err != nil {
				t.Fatal(err)
			}
			assertSignalExitCode(t, command.Wait(), row.exit)
			for _, id := range []string{"one", "two"} {
				if _, err := os.Stat(filepath.Join(root, "joined-"+id)); err != nil {
					t.Fatalf("worker %s was not joined: %v", id, err)
				}
			}
			var receipt completionWorkersReceipt
			raw, err := os.ReadFile(filepath.Join(root, "workers.json"))
			if err != nil || json.Unmarshal(raw, &receipt) != nil || len(receipt.Workers) != 4 {
				t.Fatalf("missing partial-failure receipt: %s %v", raw, err)
			}
			for i, worker := range receipt.Workers {
				if worker.Status == "passed" || i >= 2 && worker.Status != "not_started" {
					t.Fatal("interrupted plan earned credit or launched new work", worker)
				}
			}
		})
	}
}

func TestCompletionParentDeathRetainsWorkerDescendants(t *testing.T) {
	const fixture = "SWARM_COMPLETION_DEATH_FIXTURE"
	if root := os.Getenv(fixture); root != "" {
		if unit := os.Getenv("SWARM_COMPLETION_DEATH_UNIT"); unit != "" {
			admission := testpostgres.NewRunAdmission(filepath.Join(root, "state"), nil)
			lease, err := admission.Acquire(context.Background(), testpostgres.RunCommand{Args: []string{"fixture", unit}}, 2)
			if err != nil {
				t.Fatal(err)
			}
			child := exec.Command("sh", "-c", `trap '' INT TERM HUP; touch "$1"; while [ ! -f "$2" ]; do sleep .01; done`, "descendant", filepath.Join(root, "started-"+unit), filepath.Join(root, "release"))
			if err := lease.InheritTo(child); err != nil {
				t.Fatal(err)
			}
			if err := child.Start(); err != nil {
				t.Fatal(err)
			}
			if err := publishSignalFixturePID(filepath.Join(root, "worker-"+unit), os.Getpid(), nil); err != nil {
				t.Fatal(err)
			}
			if err := child.Wait(); err != nil {
				t.Fatal(err)
			}
			if err := lease.Complete(context.Background(), false); err != nil {
				t.Fatal(err)
			}
			return
		}
		plan := testplanning.RunPlan{HeadSHA: "head", Digest: "plan", Profile: testplanning.ProfileFull}
		for _, id := range []string{"one", "two", "three"} {
			plan.Units = append(plan.Units, testplanning.ProofUnit{ID: id})
		}
		os.Exit(executeCompletionWorkers(context.Background(), plan, root, filepath.Join(root, "worker"), os.Environ(), true, 2))
	}
	root := t.TempDir()
	script := "#!/bin/sh\nset -eu\nexport SWARM_COMPLETION_DEATH_UNIT=\"$3\"\nexec \"$SWARM_COMPLETION_DEATH_BINARY\" -test.run=^TestCompletionParentDeathRetainsWorkerDescendants$\n"
	if err := os.WriteFile(filepath.Join(root, "worker"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	parent := exec.Command(os.Args[0], "-test.run=^TestCompletionParentDeathRetainsWorkerDescendants$")
	parent.Env = append(os.Environ(), fixture+"="+root, "SWARM_COMPLETION_DEATH_BINARY="+os.Args[0])
	if err := parent.Start(); err != nil {
		t.Fatal(err)
	}
	var workers []int
	t.Cleanup(func() {
		_ = os.WriteFile(filepath.Join(root, "release"), []byte("release\n"), 0600)
		for _, pid := range workers {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
		if parent.ProcessState == nil {
			_ = parent.Process.Kill()
			_ = parent.Wait()
		}
	})
	for _, unit := range []string{"one", "two"} {
		workers = append(workers, waitForSignalFixturePID(t, filepath.Join(root, "worker-"+unit)))
		waitForTestPath(t, filepath.Join(root, "started-"+unit), 10*time.Second)
	}
	if err := parent.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := parent.Wait(); err == nil {
		t.Fatal("killed aggregate passed")
	}
	for _, pid := range workers {
		if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
			t.Fatal(err)
		}
	}
	admission := testpostgres.NewRunAdmission(filepath.Join(root, "state"), nil)
	blocked, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if lease, err := admission.Acquire(blocked, testpostgres.RunCommand{Args: []string{"successor"}}, 2); err == nil {
		_ = lease.Complete(context.Background(), false)
		t.Fatal("parent/worker death released descendant authority")
	}
	if _, err := os.Stat(filepath.Join(root, "workers.json")); !os.IsNotExist(err) {
		t.Fatal("dead aggregate published full proof", err)
	}
	if _, err := os.Stat(filepath.Join(root, "started-three")); !os.IsNotExist(err) {
		t.Fatal("dead parent launched another unit", err)
	}
	if err := os.WriteFile(filepath.Join(root, "release"), []byte("release\n"), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, finish := context.WithTimeout(context.Background(), 5*time.Second)
	defer finish()
	lease, err := admission.Acquire(ctx, testpostgres.RunCommand{Args: []string{"successor-after-join"}}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Complete(ctx, false); err != nil {
		t.Fatal(err)
	}
}

func TestCompletionPreflightAndOrdering(t *testing.T) {
	for _, row := range []struct {
		bytes, inodes uint64
		capacity      int
		good          bool
	}{
		{2 << 30, 10000, 1, true}, {8 << 30, 40000, 4, true},
		{2<<30 - 1, 10000, 1, false}, {2 << 30, 9999, 1, false},
		{8 << 30, 40000, 0, false}, {8 << 30, 39999, 4, false},
	} {
		if err := completionScratchBudget(row.bytes, row.inodes, row.capacity); (err == nil) != row.good {
			t.Fatalf("scratch admission %+v: %v", row, err)
		}
	}
	plan := testplanning.RunPlan{Digest: "unchanged", Units: []testplanning.ProofUnit{{ID: "long", WeightSeconds: 1000}, {ID: "short", WeightSeconds: 1}, {ID: "unknown"}}}
	if got := completionExecutionOrder(plan); !reflect.DeepEqual(got, []int{2, 1, 0}) || plan.Units[0].ID != "long" || plan.Digest != "unchanged" {
		t.Fatal("ordering changed plan identity/membership", got)
	}
	t.Run("missing_tools_before_planning", func(t *testing.T) {
		t.Setenv("PATH", t.TempDir())
		if err := completionHostPreflight(context.Background()); err == nil || !strings.Contains(err.Error(), "requires go") {
			t.Fatal(err)
		}
	})
	t.Run("unwritable_scratch_before_planning", func(t *testing.T) {
		t.Setenv("XDG_STATE_HOME", t.TempDir())
		t.Setenv("SWARM_TEST_RUN_SLOTS", "1")
		t.Setenv("TMPDIR", filepath.Join(t.TempDir(), "absent"))
		if err := completionHostPreflight(context.Background()); err == nil || !strings.Contains(err.Error(), "scratch preflight") {
			t.Fatal(err)
		}
	})
}

func TestCompletionNativePreflightSelection(t *testing.T) {
	for _, name := range []string{"TestManagerCapacityAdmissionNative", "TestServePostgresLossAndRestartFromDurableState", "TestServePostgresRemotePossessionOutlivesLocalLoss", "TestGoldenPostgresStoreCapacityAdmission"} {
		plan := testplanning.RunPlan{Units: []testplanning.ProofUnit{{SelectedRoots: []testplanning.TestRoot{{Package: "fixture", Name: name}}}}}
		if !planNeedsNativePostgres(plan) {
			t.Fatalf("native consumer %s omitted", name)
		}
	}
	plan := testplanning.RunPlan{Units: []testplanning.ProofUnit{{SelectedRoots: []testplanning.TestRoot{{Package: "fixture", Name: "TestServerCapacityAdmission"}}}}}
	if planNeedsNativePostgres(plan) {
		t.Fatal("pure mocked capacity read unnecessarily requires native server")
	}
}
