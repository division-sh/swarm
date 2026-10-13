//go:build aix || darwin || dragonfly || freebsd || hurd || illumos || linux || netbsd || openbsd || solaris

package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/testplanning"
	"github.com/division-sh/swarm/internal/testpostgres"
)

func TestPreflightCurrentSourceUsesAllFiniteOwners(t *testing.T) {
	old, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	repo, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(repo); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.Chdir(old); err != nil {
			t.Error(err)
		}
	}()
	plan, err := loadPreflightPlan(context.Background(), strings.Repeat("a", 40), strings.Repeat("b", 40))
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, pkg := range plan.Packages {
		count += len(pkg.Required)
	}
	if count != 105 {
		t.Fatalf("finite native-owner projection changed: %d roots", count)
	}
}

func TestPreflightCommandJoinsCancellationAndRefusesIncompleteExecution(t *testing.T) {
	const modeEnv = "SWARM_TEST_PREFLIGHT_PROCESS_FIXTURE"
	if mode := os.Getenv(modeEnv); mode != "" {
		if strings.HasPrefix(mode, "survivor-") {
			runPreflightSurvivorFixture(t, mode)
			return
		}
		if mode == "fail" {
			t.Fatal("selected guard failure")
		}
		if mode == "hold" {
			if err := publishSignalFixturePID(os.Getenv(signalFixturePIDEnv), os.Getpid(), nil); err != nil {
				t.Fatal(err)
			}
			for {
				time.Sleep(time.Hour)
			}
		}
		return
	}
	repo, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"success", "fail", "hold"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv(modeEnv, mode)
			pidPath := filepath.Join(t.TempDir(), "child.pid")
			t.Setenv(signalFixturePIDEnv, pidPath)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan error, 1)
			go func() {
				_, err := runPreflightProcess(ctx, []string{"go", "test", filepath.Join(repo, "cmd/swarm-test"), "-count=1", "-run", "^TestPreflightCommandJoinsCancellationAndRefusesIncompleteExecution$"}, io.Discard, io.Discard)
				result <- err
			}()
			var pid int
			if mode == "hold" {
				pid = waitForSignalFixturePID(t, pidPath)
				cancel()
			}
			select {
			case err := <-result:
				if (err == nil) != (mode == "success") {
					t.Fatalf("%s process result: %v", mode, err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("preflight did not join cancellation")
			}
			if pid != 0 {
				waitForProcessAbsent(t, pid)
			}
		})
	}
	if run([]string{"preflight", "--run", "^$"}) != 2 {
		t.Fatal("local preflight admitted a filtered selection")
	}
	t.Run("static-child-cancellation", func(t *testing.T) {
		t.Setenv(modeEnv, "hold")
		pidPath := filepath.Join(t.TempDir(), "static.pid")
		t.Setenv(signalFixturePIDEnv, pidPath)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		result := make(chan error, 1)
		go func() {
			_, err := runPreflightProcess(ctx, []string{os.Args[0], "-test.run=^TestPreflightCommandJoinsCancellationAndRefusesIncompleteExecution$"}, io.Discard, io.Discard)
			result <- err
		}()
		pid := waitForSignalFixturePID(t, pidPath)
		cancel()
		select {
		case err := <-result:
			if err == nil {
				t.Fatal("canceled static command became success")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("static cancellation was not joined")
		}
		waitForProcessAbsent(t, pid)
	})
	t.Run("source-mutation-refuses-command", func(t *testing.T) {
		repo, head := completionRepo(t)
		t.Chdir(repo)
		if err := os.WriteFile("new-guard.go", []byte("package proof\n"), 0600); err != nil {
			t.Fatal(err)
		}
		if err := checkPreflightSource(context.Background(), testplanning.PreflightPlan{HeadSHA: head}); err == nil {
			t.Fatal("source mutation reached guard execution")
		}
	})
	t.Run("soft-target-is-not-a-deadline", func(t *testing.T) {
		var output bytes.Buffer
		receipt := preflightReceipt{EndToEndSeconds: 301, Complete: true}
		printPreflightReceipt(&output, receipt, "retained")
		if !strings.Contains(output.String(), "soft target exceeded") || !strings.Contains(output.String(), "complete=true") || !receipt.Complete {
			t.Fatalf("soft target changed execution outcome: %s", output.String())
		}
	})
	provePreflightDoesNotAcquireNativeSlot(t, repo)
	provePreflightSurvivingDescendants(t, repo)
	t.Run("unresolved-lifetime-refuses-completion", func(t *testing.T) {
		reader, writer, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		defer writer.Close()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := joinPreflightDescendants(ctx, reader); !errors.Is(err, context.Canceled) {
			t.Fatalf("unresolved inherited lifetime became success: %v", err)
		}
	})
}

func provePreflightDoesNotAcquireNativeSlot(t *testing.T, repo string) {
	t.Run("occupied-native-slot-is-untouched", func(t *testing.T) {
		home := t.TempDir()
		t.Setenv("XDG_STATE_HOME", home)
		t.Setenv("SWARM_TEST_PREFLIGHT_PROCESS_FIXTURE", "success")
		admission := testpostgres.NewRunAdmission(filepath.Join(home, "swarm", "test-postgres"), io.Discard)
		lease, err := admission.Acquire(context.Background(), testpostgres.RunCommand{Args: []string{"go", "test", "./fixture-owned-heavy-work"}}, 1)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := lease.Complete(context.Background(), false); err != nil {
				t.Error(err)
			}
		})
		before := preflightNativeFixtureFiles(t, home)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := runPreflightProcess(ctx, []string{"go", "test", filepath.Join(repo, "cmd/swarm-test"), "-count=1", "-run", "^TestPreflightCommandJoinsCancellationAndRefusesIncompleteExecution$"}, io.Discard, io.Discard); err != nil {
			t.Fatalf("static guard waited on an occupied native slot: %v", err)
		}
		if after := preflightNativeFixtureFiles(t, home); !reflect.DeepEqual(before, after) {
			t.Fatalf("preflight acquired/mutated native admission or service state: before=%v after=%v", before, after)
		}
	})
}

func preflightNativeFixtureFiles(t *testing.T, home string) map[string]string {
	t.Helper()
	files := map[string]string{}
	if err := filepath.WalkDir(home, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		raw, err := os.ReadFile(path)
		files[path] = string(raw)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return files
}

func runPreflightSurvivorFixture(t *testing.T, mode string) {
	if mode == "survivor-child" {
		signal.Ignore(syscall.SIGTERM, syscall.SIGINT)
		if err := publishSignalFixturePID(os.Getenv(signalFixturePIDEnv), os.Getpid(), nil); err != nil {
			t.Fatal(err)
		}
		for {
			if _, err := os.Stat(os.Getenv("SWARM_TEST_PREFLIGHT_RELEASE")); err == nil {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	child := exec.Command(os.Args[0], "-test.run=^TestPreflightCommandJoinsCancellationAndRefusesIncompleteExecution$")
	child.Env = replaceEnvironment(os.Environ(), "SWARM_TEST_PREFLIGHT_PROCESS_FIXTURE", "survivor-child")
	// Neither redirected stdio nor leader exit may hide the inherited descriptor.
	devnull, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer devnull.Close()
	child.Stdout, child.Stderr = devnull, devnull
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	waitForSignalFixturePID(t, os.Getenv(signalFixturePIDEnv))
	if mode == "survivor-fail" {
		t.Fatal("selected guard failure with a surviving descendant")
	}
}

func provePreflightSurvivingDescendants(t *testing.T, repo string) {
	for _, mode := range []string{"survivor-success", "survivor-fail", "survivor-cancel"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			release := filepath.Join(root, "release")
			pidPath := filepath.Join(root, "survivor.pid")
			t.Setenv("SWARM_TEST_PREFLIGHT_PROCESS_FIXTURE", mode)
			t.Setenv("SWARM_TEST_PREFLIGHT_RELEASE", release)
			t.Setenv(signalFixturePIDEnv, pidPath)
			t.Cleanup(func() {
				if err := os.WriteFile(release, []byte("release\n"), 0600); err != nil {
					t.Error(err)
				}
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan error, 1)
			go func() {
				_, err := runPreflightProcess(ctx, []string{"go", "test", filepath.Join(repo, "cmd/swarm-test"), "-count=1", "-run", "^TestPreflightCommandJoinsCancellationAndRefusesIncompleteExecution$"}, io.Discard, io.Discard)
				result <- err
			}()
			pid := waitForSignalFixturePID(t, pidPath)
			if mode == "survivor-cancel" {
				cancel()
			}
			select {
			case err := <-result:
				t.Fatalf("leader exit/cancellation falsely joined a redirected survivor: %v", err)
			case <-time.After(200 * time.Millisecond):
			}
			if err := os.WriteFile(release, []byte("release\n"), 0600); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-result:
				if (err == nil) != (mode == "survivor-success") {
					t.Fatalf("survivor result: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("released descendant was not joined")
			}
			waitForProcessAbsent(t, pid)
		})
	}
}
