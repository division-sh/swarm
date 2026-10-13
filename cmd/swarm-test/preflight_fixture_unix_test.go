//go:build aix || darwin || dragonfly || freebsd || hurd || illumos || linux || netbsd || openbsd || solaris

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

type preflightFixture struct {
	root, pidPath, releasePath string
	output                     *os.File
	cancel                     context.CancelFunc
	result                     chan error
	joined                     bool
	terminal                   error
	pid                        int
}

func preflightFixtureCommand() []string {
	return []string{os.Args[0], "-test.v", "-test.run=^TestPreflightCommandJoinsCancellationAndRefusesIncompleteExecution$"}
}

func provePreflightTerminalOutcomes(t *testing.T) {
	for _, mode := range []string{"success", "fail", "hold", "early-cleanup"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("PATH", t.TempDir())
			childMode := mode
			if mode == "early-cleanup" {
				childMode = "hold"
			}
			fixture := startPreflightFixture(t, context.Background(), childMode, preflightFixtureCommand())
			fixture.ready(t)
			if mode == "early-cleanup" {
				return
			}
			deadline := time.Now().Add(5 * time.Second)
			if mode == "hold" {
				fixture.cancel()
			} else if err := fixture.release(); err != nil {
				t.Fatal(err)
			}
			err := fixture.wait(time.Until(deadline))
			raw, readErr := os.ReadFile(fixture.output.Name())
			if err := errors.Join(readErr, preflightTerminalOutcome(mode, err, raw)); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func waitForPreflightFixtureRelease(t *testing.T, path string) {
	t.Helper()
	for {
		raw, err := os.ReadFile(path)
		if err == nil && string(raw) == "release\n" {
			return
		}
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func provePreflightEarlyCleanup(t *testing.T, repo string) {
	t.Run("early-cleanup-before-readiness", func(t *testing.T) {
		fixture := startPreflightFixture(t, context.Background(), "startup-hold", preflightFixtureCommand())
		fixture.pid = waitForSignalFixturePID(t, fixture.pidPath+".entered")
		if _, err := os.Stat(fixture.pidPath); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("startup cut already reached readiness: %v", err)
		}
		if err := fixture.cleanup(); err != nil {
			t.Fatal(err)
		}
		if !fixture.joined || !errors.Is(fixture.terminal, context.Canceled) {
			t.Fatalf("early startup cleanup lost real cancellation/join: %v", fixture.terminal)
		}
	})
	args := []string{"go", "test", filepath.Join(repo, "cmd/swarm-test"), "-count=1", "-run", "^TestPreflightCommandJoinsCancellationAndRefusesIncompleteExecution$"}
	t.Run("survivor-early-cleanup", func(t *testing.T) {
		fixture := startPreflightFixture(t, context.Background(), "survivor-success", args)
		fixture.ready(t)
	})
	t.Run("release-write-failure-retains-owner", func(t *testing.T) {
		fixture := startPreflightFixture(t, context.Background(), "survivor-success", args)
		fixture.ready(t)
		original := fixture.releasePath
		fixture.releasePath = filepath.Join(fixture.root, "missing-directory", "release")
		err := fixture.cleanup()
		fixture.releasePath = original
		if !errors.Is(err, os.ErrNotExist) || fixture.joined {
			t.Fatalf("failed release claimed fixture completion: %v / joined=%v", err, fixture.joined)
		}
		if _, err := os.ReadFile(fixture.pidPath); err != nil {
			t.Fatalf("failed release lost retained PID/recovery evidence: %v", err)
		}
		select {
		case err := <-fixture.result:
			fixture.joined, fixture.terminal = true, err
			t.Fatalf("failed release lost the pending native join: %v", err)
		default:
		}
		if err := fixture.cleanup(); err != nil {
			t.Fatal(err)
		}
		if !fixture.joined {
			t.Fatal("release repair did not consume the original join")
		}
	})
	t.Run("spawn-failure-is-not-selected-failure", func(t *testing.T) {
		_, err := runPreflightProcess(context.Background(), []string{filepath.Join(t.TempDir(), "missing-executable")}, os.Stderr, os.Stderr)
		evidence := []byte("fixture ready: mode=fail pid=1\nfixture selected: mode=fail\nselected guard failure\n")
		if preflightTerminalOutcome("fail", err, evidence) == nil {
			t.Fatal("spawn failure was accepted as the selected guard failure")
		}
	})
}

func startPreflightFixture(t *testing.T, parent context.Context, mode string, args []string) *preflightFixture {
	t.Helper()
	root, err := os.MkdirTemp("", "swarm-preflight-fixture-")
	if err != nil {
		t.Fatal(err)
	}
	output, err := os.Create(filepath.Join(root, "child.log"))
	if err != nil {
		t.Fatal(errors.Join(err, os.RemoveAll(root)))
	}
	ctx, cancel := context.WithCancel(parent)
	f := &preflightFixture{root: root, pidPath: filepath.Join(root, "child.pid"), releasePath: filepath.Join(root, "release"), output: output, cancel: cancel, result: make(chan error, 1)}
	t.Setenv("SWARM_TEST_PREFLIGHT_PROCESS_FIXTURE", mode)
	t.Setenv(signalFixturePIDEnv, f.pidPath)
	t.Setenv("SWARM_TEST_PREFLIGHT_RELEASE", f.releasePath)
	// A failed cleanup retains the only release/log evidence for the unresolved work.
	t.Cleanup(func() {
		cleanupErr := f.cleanup()
		raw, readErr := os.ReadFile(output.Name())
		t.Logf("child phase/stdout/stderr evidence (%s):\n%s", root, raw)
		if err := errors.Join(cleanupErr, readErr); err != nil {
			t.Error(err)
			return
		}
		if f.pid == 0 {
			if raw, err := os.ReadFile(f.pidPath); err == nil {
				f.pid, err = strconv.Atoi(strings.TrimSpace(string(raw)))
				if err != nil || f.pid <= 0 {
					t.Fatalf("invalid final child PID: %q / %v", raw, err)
				}
			} else if !errors.Is(err, os.ErrNotExist) {
				t.Fatal(err)
			}
		}
		if f.pid != 0 {
			waitForProcessAbsent(t, f.pid)
		}
		if err := output.Close(); err != nil {
			t.Fatal(err)
		}
		if err := os.RemoveAll(root); err != nil {
			t.Error(err)
		}
	})
	go func() {
		timing, err := runPreflightProcess(ctx, args, output, output)
		fmt.Fprintf(output, "fixture joined: mode=%s execution=%.6fs cleanup=%.6fs error=%v\n", mode, timing.ExecutionSeconds, timing.CleanupSeconds, err)
		f.result <- err
	}()
	return f
}

func (f *preflightFixture) ready(t *testing.T) {
	t.Helper()
	started := time.Now()
	f.pid = waitForSignalFixturePID(t, f.pidPath)
	t.Logf("fixture ready: pid=%d startup=%s", f.pid, time.Since(started))
}

func (f *preflightFixture) release() error {
	return os.WriteFile(f.releasePath, []byte("release\n"), 0600)
}

func (f *preflightFixture) wait(limit time.Duration) error {
	if limit <= 0 {
		return fmt.Errorf("preflight fixture join deadline already expired")
	}
	select {
	case err := <-f.result:
		f.joined, f.terminal = true, err
		return err
	case <-time.After(limit):
		return fmt.Errorf("preflight fixture did not join within %s", limit)
	}
}

func (f *preflightFixture) cleanup() error {
	if !f.joined {
		if err := f.release(); err != nil {
			return fmt.Errorf("release fixture before cleanup: %w", err)
		}
		f.cancel()
		f.wait(authoritySettlementTimeout)
		if !f.joined {
			return fmt.Errorf("fixture cleanup did not join")
		}
	}
	f.cancel()
	return preflightFixtureCleanupError(f.terminal)
}

func preflightFixtureCleanupError(err error) error {
	if multiple, ok := err.(interface{ Unwrap() []error }); ok {
		var unexpected []error
		for _, child := range multiple.Unwrap() {
			unexpected = append(unexpected, preflightFixtureCleanupError(child))
		}
		return errors.Join(unexpected...)
	}
	var exitErr *exec.ExitError
	if err == nil || errors.Is(err, context.Canceled) || errors.As(err, &exitErr) {
		return nil
	}
	return err
}

func preflightTerminalOutcome(mode string, err error, evidence []byte) error {
	if !strings.Contains(string(evidence), "fixture ready: mode="+mode+" ") {
		return fmt.Errorf("fixture did not reach readiness: %v", err)
	}
	switch mode {
	case "success":
		return err
	case "fail":
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) || exitErr.ExitCode() != 1 || !strings.Contains(string(evidence), "fixture selected: mode=fail\n") || !strings.Contains(string(evidence), "selected guard failure") {
			return fmt.Errorf("fixture failed before its selected guard: %v", err)
		}
	case "hold":
		if !errors.Is(err, context.Canceled) {
			return fmt.Errorf("fixture lost cancellation: %v", err)
		}
	default:
		return fmt.Errorf("unknown fixture mode %q", mode)
	}
	return nil
}
