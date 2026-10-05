//go:build darwin || linux

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/testpostgres"
)

// Extracted from agent-g's fixture repair in befb39402; retain every acquired
// slot until the whole set is free, including when acquisition is interrupted.
func joinCompletionFixtureDescendants(ctx context.Context, root string) (err error) {
	admission := testpostgres.NewRunAdmission(filepath.Join(root, "state"), nil)
	var joined []*testpostgres.RunLease
	defer func() {
		for _, lease := range joined {
			err = errors.Join(err, lease.Complete(ctx, false))
		}
	}()
	for range 2 {
		lease, acquireErr := admission.Acquire(ctx, testpostgres.RunCommand{Args: []string{"go", "test", "./successor-after-join"}}, 2)
		if acquireErr != nil {
			return acquireErr
		}
		joined = append(joined, lease)
	}
	return nil
}

func cleanupCompletionDeathFixture(ctx context.Context, parent *exec.Cmd, root string) error {
	if parent != nil && parent.Process != nil {
		// The private group stops even workers whose PID has not been collected.
		// Synthetic descendants ignore TERM and remain fenced until release/join.
		if err := signalChildProcessTree(parent, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
			return fmt.Errorf("stop fixture workers; evidence retained at %s: %w", root, err)
		}
		if parent.ProcessState == nil {
			if err := parent.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
				return fmt.Errorf("stop fixture parent; evidence retained at %s: %w", root, err)
			}
			var exitErr *exec.ExitError
			if err := parent.Wait(); err != nil && !errors.As(err, &exitErr) {
				return fmt.Errorf("join fixture parent; evidence retained at %s: %w", root, err)
			}
		}
	}
	if err := os.WriteFile(filepath.Join(root, "release"), []byte("release\n"), 0600); err != nil {
		return fmt.Errorf("release fixture descendants; evidence retained at %s: %w", root, err)
	}
	if err := joinCompletionFixtureDescendants(ctx, root); err != nil {
		return fmt.Errorf("join both fixture descendants; evidence retained at %s: %w", root, err)
	}
	return os.RemoveAll(root)
}

func TestCompletionFixtureJoinsEveryDescendantSlot(t *testing.T) {
	for _, released := range []int{0, 1} {
		t.Run(fmt.Sprintf("release_slot_%d_first", released), func(t *testing.T) {
			root := t.TempDir()
			admission := testpostgres.NewRunAdmission(filepath.Join(root, "state"), nil)
			ctx, finish := context.WithTimeout(context.Background(), 5*time.Second)
			defer finish()
			var leases []*testpostgres.RunLease
			for range 2 {
				lease, err := admission.Acquire(ctx, testpostgres.RunCommand{Args: []string{"go", "test", "./held-descendant"}}, 2)
				if err != nil {
					t.Fatal(err)
				}
				leases = append(leases, lease)
				defer func() {
					if err := lease.Complete(ctx, false); err != nil {
						t.Error(err)
					}
				}()
			}
			if err := leases[released].Complete(ctx, false); err != nil {
				t.Fatal(err)
			}
			blocked, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
			err := joinCompletionFixtureDescendants(blocked, root)
			cancel()
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("one released slot certified both descendants: %v", err)
			}
			// Cancellation must release the partial acquisition, not leak the free slot.
			partial, err := admission.Acquire(ctx, testpostgres.RunCommand{Args: []string{"go", "test", "./partial-release"}}, 2)
			if err != nil {
				t.Fatal(err)
			}
			if err := partial.Complete(ctx, false); err != nil {
				t.Fatal(err)
			}
			if err := leases[1-released].Complete(ctx, false); err != nil {
				t.Fatal(err)
			}
			if err := joinCompletionFixtureDescendants(ctx, root); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCompletionFixtureEarlyCleanupPreservesReleaseEvidence(t *testing.T) {
	for _, mode := range []string{"early", "delayed_descendant", "release_failure"} {
		t.Run(mode, func(t *testing.T) {
			root, err := os.MkdirTemp("", "swarm-completion-cleanup-")
			if err != nil {
				t.Fatal(err)
			}
			fixtureRoot := filepath.Join(root, "fixture")
			t.Cleanup(func() {
				if _, err := os.Stat(fixtureRoot); errors.Is(err, os.ErrNotExist) {
					if err := os.RemoveAll(root); err != nil {
						t.Error(err)
					}
					return
				}
				if mode == "release_failure" {
					if err := os.Remove(filepath.Join(fixtureRoot, "release")); err != nil {
						t.Error(err)
						return
					}
				}
				if err := os.WriteFile(filepath.Join(fixtureRoot, "continue-delayed"), []byte("continue\n"), 0600); err != nil {
					t.Error(err)
					return
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := cleanupCompletionDeathFixture(ctx, nil, fixtureRoot); err != nil {
					t.Error(err)
					return
				}
				if err := os.RemoveAll(root); err != nil {
					t.Error(err)
				}
			})
			child := exec.Command(os.Args[0], "-test.run=^TestCompletionParentDeathRetainsWorkerDescendants$", "-test.v")
			child.Env = append(os.Environ(), "SWARM_COMPLETION_DEATH_TEST_ROOT="+fixtureRoot, "SWARM_COMPLETION_DEATH_INTERRUPT="+mode)
			if mode == "delayed_descendant" {
				child.Env = append(child.Env, "SWARM_COMPLETION_DEATH_DELAY_UNIT=two")
			}
			output, err := child.CombinedOutput()
			if err == nil || !strings.Contains(string(output), "fixture interrupted before collecting the second worker PID") {
				t.Fatalf("did not reach actual early-failure cleanup: %s %v", output, err)
			}
			if mode == "early" {
				if _, err := os.Stat(fixtureRoot); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("successful complete cleanup retained its directory: %v", err)
				}
				return
			}
			want := "join both fixture descendants"
			if mode == "release_failure" {
				want = "release fixture descendants"
			}
			if !strings.Contains(string(output), want) {
				t.Fatalf("missing bounded cleanup failure: %s", output)
			}
			if _, err := os.Stat(filepath.Join(fixtureRoot, "release")); err != nil {
				t.Fatalf("cleanup deleted unresolved release evidence: %v", err)
			}
			if _, err := os.Stat(filepath.Join(fixtureRoot, "state")); err != nil {
				t.Fatalf("cleanup deleted unresolved lease evidence: %v", err)
			}
		})
	}
}
