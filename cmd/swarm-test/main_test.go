package main

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestParseTestArgs(t *testing.T) {
	if got, err := parseTestArgs(nil); err != nil || len(got) != 1 || got[0] != "./..." {
		t.Fatalf("no-argument selection = %v err=%v", got, err)
	}
	if got, err := parseTestArgs([]string{"--", "./internal/testpostgres", "-count=1"}); err != nil || strings.Join(got, " ") != "./internal/testpostgres -count=1" {
		t.Fatalf("focused selection = %v err=%v", got, err)
	}
	for _, args := range [][]string{{"go", "test", "./..."}, {"--"}} {
		if _, err := parseTestArgs(args); err == nil {
			t.Fatalf("legacy/arbitrary grammar accepted: %v", args)
		}
	}
}

func TestSwarmTestExplicitDSNDoesNotInvokeDocker(t *testing.T) {
	if os.Getenv("SWARM_TEST_EXPLICIT_DSN_CHILD") == "1" {
		return
	}
	chdirSwarmRepoRoot(t)
	stateHome := filepath.Join(t.TempDir(), "state")
	binDir := t.TempDir()
	dockerMarker := filepath.Join(t.TempDir(), "docker-called")
	docker := filepath.Join(binDir, "docker")
	if err := os.WriteFile(docker, []byte("#!/bin/sh\ntouch \"$SWARM_TEST_DOCKER_MARKER\"\nexit 99\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_STATE_HOME", stateHome)
	t.Setenv("SWARM_TEST_DOCKER_MARKER", dockerMarker)
	t.Setenv("SWARM_TEST_EXPLICIT_DSN_CHILD", "1")
	t.Setenv("SWARM_TEST_POSTGRES_DSN", "postgres://swarm:secret@127.0.0.1:1/postgres?sslmode=disable")
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))

	code := run([]string{"--", "./cmd/swarm-test", "-run", "^TestSwarmTestExplicitDSNDoesNotInvokeDocker$", "-count=1"})
	if code != 0 {
		t.Fatalf("run() code = %d", code)
	}
	if _, err := os.Stat(dockerMarker); !os.IsNotExist(err) {
		t.Fatalf("Docker was invoked in explicit-DSN mode: %v", err)
	}
}

func TestSwarmTestPreservesChildFailure(t *testing.T) {
	if os.Getenv("SWARM_TEST_FAILURE_CHILD") == "1" {
		t.Fatal("requested child failure")
	}
	chdirSwarmRepoRoot(t)
	t.Setenv("XDG_STATE_HOME", filepath.Join(t.TempDir(), "state"))
	t.Setenv("SWARM_TEST_FAILURE_CHILD", "1")
	t.Setenv("SWARM_TEST_POSTGRES_DSN", "postgres://swarm:secret@127.0.0.1:1/postgres?sslmode=disable")
	if code := run([]string{"--", "./cmd/swarm-test", "-run", "^TestSwarmTestPreservesChildFailure$", "-count=1"}); code != 1 {
		t.Fatalf("run() code = %d, want child status 1", code)
	}
}

func TestTimingFallbackUsesCanonicalModelForFullSuite(t *testing.T) {
	if duration := timingFallback([]string{"./cmd/swarm-test"}); duration != 0 {
		t.Fatalf("focused fallback = %s, want unknown", duration)
	}
	_, file, _, _ := runtime.Caller(0)
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	old, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(repoRoot); err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(old)
	if duration := timingFallback([]string{"./..."}); duration <= 0 {
		t.Fatalf("full-suite fallback = %s, want canonical model estimate", duration)
	}
}

func chdirSwarmRepoRoot(t *testing.T) {
	t.Helper()
	_, file, _, _ := runtime.Caller(0)
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	old, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(repoRoot); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(old) })
}
