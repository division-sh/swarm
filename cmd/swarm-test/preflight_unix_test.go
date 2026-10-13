//go:build aix || darwin || dragonfly || freebsd || hurd || illumos || linux || netbsd || openbsd || solaris

package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
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
			admission := testpostgres.NewRunAdmission(filepath.Join(t.TempDir(), "state"), io.Discard)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan error, 1)
			go func() {
				result <- runPreflightProcess(ctx, []string{"go", "test", filepath.Join(repo, "cmd/swarm-test"), "-count=1", "-run", "^TestPreflightCommandJoinsCancellationAndRefusesIncompleteExecution$"}, io.Discard, io.Discard, admission, 1)
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
			check, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			lease, err := admission.Acquire(check, testpostgres.RunCommand{Args: []string{"go", "test", "./successor"}}, 1)
			if err != nil {
				t.Fatalf("joined preflight retained its slot: %v", err)
			}
			if err := lease.Complete(check, false); err != nil {
				t.Fatal(err)
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
			result <- runPreflightStaticProcess(ctx, []string{os.Args[0], "-test.run=^TestPreflightCommandJoinsCancellationAndRefusesIncompleteExecution$"}, io.Discard, io.Discard)
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
		receipt := preflightReceipt{Seconds: 301, Complete: true}
		printPreflightReceipt(&output, receipt, "retained")
		if !strings.Contains(output.String(), "soft target exceeded") || !strings.Contains(output.String(), "complete=true") || !receipt.Complete {
			t.Fatalf("soft target changed execution outcome: %s", output.String())
		}
	})
}
