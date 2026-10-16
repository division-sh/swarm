//go:build aix || darwin || dragonfly || freebsd || hurd || illumos || linux || netbsd || openbsd || solaris

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/testpostgres"
)

func precompileRunnerFixture(t *testing.T, warm string) (string, string) {
	t.Helper()
	requirePrecompileSupport(t)
	chdirSwarmRepoRoot(t)
	root := t.TempDir()
	state := filepath.Join(root, "state", "swarm", "test-postgres")
	t.Setenv("XDG_STATE_HOME", filepath.Join(root, "state"))
	t.Setenv("SWARM_TEST_POSTGRES_DSN", signalFixtureStateDSN)
	t.Setenv("GOFLAGS", "")
	t.Setenv("GOENV", "off")
	t.Setenv("PRECOMPILE_PROBE", root)
	t.Setenv("PATH", root+string(os.PathListSeparator)+os.Getenv("PATH"))
	script := fmt.Sprintf(`#!/bin/sh
set -eu
if [ "$1" = env ]; then
  touch "$PRECOMPILE_PROBE/environment"
  exec %q "$@"
fi
if [ "${2-}" = -exec=/bin/true ]; then
  test ! -e "$XDG_STATE_HOME/swarm/test-postgres/runs-v1.json"
  touch "$PRECOMPILE_PROBE/warm"
  %s
  exit 0
fi
test -f "$XDG_STATE_HOME/swarm/test-postgres/runs-v1.json"
cp "$XDG_STATE_HOME/swarm/test-postgres/runs-v1.json" "$PRECOMPILE_PROBE/active.json"
printf 'call\n' >> "$PRECOMPILE_PROBE/calls"
printf '%%s\000' "$@" > "$PRECOMPILE_PROBE/arguments"
printf 'actual-only\n'
`, filepath.Join(runtime.GOROOT(), "bin", "go"), warm)
	if err := os.WriteFile(filepath.Join(root, "go"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	return root, state
}

func capturePrecompileRun(t *testing.T, args []string) (int, string, string) {
	t.Helper()
	log, err := os.Create(filepath.Join(t.TempDir(), "stderr"))
	if err != nil {
		t.Fatal(err)
	}
	var stdout bytes.Buffer
	previous := os.Stderr
	code := func() int {
		os.Stderr = log
		defer func() { os.Stderr = previous }()
		return runTestArgs(args, &stdout, false, "", "", 0)
	}()
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(log.Name())
	if err != nil {
		t.Fatal(err)
	}
	return code, stdout.String(), string(raw)
}

func TestPrecompileFallbackRunsOriginalOnceUnderAdmission(t *testing.T) {
	for _, args := range [][]string{
		{"./fixture", "-exec=custom"}, {"./fixture", "--exec", "custom"},
		{"./fixture", "-test.exec=custom"}, {"./fixture", "-c"}, {"./fixture", "--o=output"},
		{"./fixture", "-args", "-test.run=Selected"}, {"./fixture", "--", "custom"},
		{"./fixture", "-test.coverprofile=coverage"}, {"./fixture", "--test.outputdir=output"},
		{"./fixture", "-artifacts"}, {"./fixture", "-fuzz=Fuzz"}, {"./fixture", "-work"},
		{"./fixture", "-toolexec=custom"}, {"./fixture", "-unknown=custom"},
	} {
		t.Run(strings.Join(args[1:], "_"), func(t *testing.T) {
			root, state := precompileRunnerFixture(t, "exit 99")
			code, stdout, stderr := capturePrecompileRun(t, args)
			if code != 0 || stdout != "actual-only\n" {
				t.Fatalf("original admitted command changed: code=%d stdout=%q stderr=%s", code, stdout, stderr)
			}
			for _, path := range []string{"environment", "warm"} {
				if _, err := os.Stat(filepath.Join(root, path)); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("unsupported form started %s: %v", path, err)
				}
			}
			assertPrecompileActualCommand(t, root, state, args)
		})
	}
	for _, mode := range []string{"shell_flags", "persisted_flags", "foreign_target"} {
		t.Run(mode, func(t *testing.T) {
			root, state := precompileRunnerFixture(t, "exit 99")
			switch mode {
			case "shell_flags":
				t.Setenv("GOFLAGS", "-exec=custom")
			case "persisted_flags":
				path := filepath.Join(root, "goenv")
				if err := os.WriteFile(path, []byte("GOFLAGS=-exec=custom\n"), 0600); err != nil {
					t.Fatal(err)
				}
				t.Setenv("GOENV", path)
			case "foreign_target":
				t.Setenv("GOARCH", "wasm")
				t.Setenv("GOOS", "js")
			}
			args := []string{"./fixture", "-count=1"}
			code, stdout, stderr := capturePrecompileRun(t, args)
			if code != 0 || stdout != "actual-only\n" {
				t.Fatalf("environment fallback changed command: code=%d stdout=%q stderr=%s", code, stdout, stderr)
			}
			if _, err := os.Stat(filepath.Join(root, "warm")); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("environment fallback warmed", err)
			}
			assertPrecompileActualCommand(t, root, state, args)
		})
	}
}

func assertPrecompileActualCommand(t *testing.T, root, state string, args []string) {
	t.Helper()
	calls, err := os.ReadFile(filepath.Join(root, "calls"))
	if err != nil || string(calls) != "call\n" {
		t.Fatalf("actual command did not execute exactly once: %q %v", calls, err)
	}
	raw, err := os.ReadFile(filepath.Join(root, "arguments"))
	want := append([]string{"test"}, args...)
	if err != nil || !reflect.DeepEqual(strings.Split(strings.TrimSuffix(string(raw), "\x00"), "\x00"), want) {
		t.Fatalf("actual argv changed: %q want=%q err=%v", raw, want, err)
	}
	var observed struct {
		Active []struct{ Command []string }
	}
	raw, err = os.ReadFile(filepath.Join(root, "active.json"))
	if err != nil || json.Unmarshal(raw, &observed) != nil || len(observed.Active) != 1 || !reflect.DeepEqual(observed.Active[0].Command, append([]string{"go"}, want...)) {
		t.Fatalf("actual command lacks exact slot authority: %s %v", raw, err)
	}
	var final struct {
		Active, Waiting, History []json.RawMessage
	}
	raw, err = os.ReadFile(filepath.Join(state, "runs-v1.json"))
	if err != nil || json.Unmarshal(raw, &final) != nil || len(final.Active) != 0 || len(final.Waiting) != 0 || len(final.History) != 1 {
		t.Fatalf("actual command did not settle once: %s %v", raw, err)
	}
}

func TestPrecompileFailureAndSuccessOutputHaveNoFalseProofCredit(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint("failure=", fail), func(t *testing.T) {
			warm := "printf '%s\\n' '{\"Action\":\"pass\",\"Package\":\"warm-only\"}'\nprintf 'compiler diagnostic\\n' >&2\n"
			if fail {
				warm += "exit 12\n"
			}
			root, state := precompileRunnerFixture(t, warm)
			args := []string{"./fixture", "-json", "-count=1"}
			code, stdout, stderr := capturePrecompileRun(t, args)
			if fail {
				if code != 1 || stdout != "" || !strings.Contains(stderr, "warm-only") || !strings.Contains(stderr, "compiler diagnostic") || !strings.Contains(stderr, "not test evidence") || !strings.Contains(stderr, "exit status 12") {
					t.Fatalf("failed warm-up earned proof or lost diagnostics: code=%d stdout=%q stderr=%s", code, stdout, stderr)
				}
				if _, err := os.Stat(state); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("failed warm-up published slot/service/history authority", err)
				}
			} else {
				if code != 0 || stdout != "actual-only\n" || strings.Contains(stderr, "warm-only") || strings.Contains(stderr, "compiler diagnostic") {
					t.Fatalf("warm success output escaped: code=%d stdout=%q stderr=%s", code, stdout, stderr)
				}
				assertPrecompileActualCommand(t, root, state, args)
			}
		})
	}
}

func TestPrecompileSignalJoinsCompilerBeforeAnyAdmission(t *testing.T) {
	runner, repo := buildSignalTestRunner(t)
	for _, sig := range []syscall.Signal{syscall.SIGINT, syscall.SIGTERM} {
		t.Run(sig.String(), func(t *testing.T) {
			root, state := precompileRunnerFixture(t, `printf '%s\n' "$$" > "$PRECOMPILE_PROBE/pid.tmp"
mv "$PRECOMPILE_PROBE/pid.tmp" "$PRECOMPILE_PROBE/pid"
while :; do sleep 1; done`)
			command := exec.Command(runner, "--", "./fixture", "-count=1")
			command.Dir = repo
			command.Env = os.Environ()
			log, err := os.Create(filepath.Join(root, "runner.log"))
			if err != nil {
				t.Fatal(err)
			}
			defer log.Close()
			command.Stdout, command.Stderr = log, log
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if command.ProcessState == nil {
					_ = command.Process.Signal(syscall.SIGTERM)
					_ = command.Wait()
				}
			})
			pid := waitForSignalFixturePID(t, filepath.Join(root, "pid"))
			if _, err := os.Stat(state); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("compiler occupied a slot before signal", err)
			}
			if err := command.Process.Signal(sig); err != nil {
				t.Fatal(err)
			}
			assertSignalExitCode(t, command.Wait(), 128+int(sig))
			waitForProcessAbsent(t, pid)
			if _, err := os.Stat(state); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("cancelled compiler published slot/service/history authority", err)
			}
		})
	}
}

func TestPrecompileUnresolvedDescendantRefusesAdmission(t *testing.T) {
	for _, mode := range []string{"survivor-success", "survivor-fail"} {
		t.Run(mode, func(t *testing.T) {
			binary, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			_, state := precompileRunnerFixture(t, fmt.Sprintf("exec %q -test.run=^TestPreflightCommandJoinsCancellationAndRefusesIncompleteExecution$", binary))
			release, pidPath := filepath.Join(t.TempDir(), "release"), filepath.Join(t.TempDir(), "pid")
			t.Setenv("SWARM_TEST_PREFLIGHT_PROCESS_FIXTURE", mode)
			t.Setenv("SWARM_TEST_PREFLIGHT_RELEASE", release)
			t.Setenv(signalFixturePIDEnv, pidPath)
			t.Cleanup(func() {
				if err := os.WriteFile(release, []byte("release\n"), 0600); err != nil {
					t.Error(err)
				}
				if _, err := os.Stat(pidPath); err == nil {
					waitForProcessAbsent(t, waitForSignalFixturePID(t, pidPath))
				}
			})
			old := authoritySettlementTimeout
			authoritySettlementTimeout = 100 * time.Millisecond
			defer func() { authoritySettlementTimeout = old }()
			code, stdout, stderr := capturePrecompileRun(t, []string{"./fixture", "-json", "-count=1"})
			if code != 1 || stdout != "" || !strings.Contains(stderr, "descendant lifetime unresolved") || (mode == "survivor-fail" && !strings.Contains(stderr, "selected guard failure with a surviving descendant")) {
				t.Fatalf("unresolved compiler lifetime earned credit/lost failure: code=%d stdout=%q stderr=%s", code, stdout, stderr)
			}
			if _, err := os.Stat(state); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("unresolved compiler published slot/service/history authority", err)
			}
		})
	}
}

func runPrecompileRealGoControl(t *testing.T, args []string) (string, time.Duration) {
	t.Helper()
	admission, err := testpostgres.DefaultRunAdmission(nil)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := admission.Acquire(context.Background(), testpostgres.RunCommand{Args: append([]string{"go", "test"}, args...)}, 1)
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", append([]string{"test"}, args...)...)
	var output bytes.Buffer
	var diagnostics bytes.Buffer
	command.Stdout, command.Stderr = &output, &diagnostics
	if err := lease.InheritTo(command); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	err = command.Run()
	elapsed := time.Since(started)
	settlement := lease.Complete(context.Background(), err == nil)
	if err != nil || settlement != nil {
		t.Fatalf("no-warm-up control: %v / %v\n%s\n%s", err, settlement, output.String(), diagnostics.String())
	}
	return output.String(), elapsed
}
