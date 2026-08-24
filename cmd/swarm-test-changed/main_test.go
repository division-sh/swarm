package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

func TestParseNameStatusIncludesDeletesAndRenameSourceAndTarget(t *testing.T) {
	files := parseNameStatus("M\tinternal/a/a.go\nD\tinternal/b/b.go\nR100\told/path.go\tinternal/c/c.go\n")
	want := []string{"internal/a/a.go", "internal/b/b.go", "old/path.go", "internal/c/c.go"}
	var got []string
	for _, file := range files {
		got = append(got, file.Path)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("paths = %#v, want %#v", got, want)
	}
}

func TestParseNameStatusIncludesOnlyCopyTarget(t *testing.T) {
	files := parseNameStatus("C100\told/path.go\tinternal/c/c.go\n")
	want := []string{"internal/c/c.go"}
	var got []string
	for _, file := range files {
		got = append(got, file.Path)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("paths = %#v, want %#v", got, want)
	}
}

func TestShellCommandQuotesGoTestCommand(t *testing.T) {
	got := shellCommand([]string{"go", "test", "-run", "Test Name", "./internal/a"})
	want := "go test -run 'Test Name' ./internal/a"
	if got != want {
		t.Fatalf("shell command = %q, want %q", got, want)
	}
}

func TestFullSuiteFallbackExecutesCanonicalWrapper(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell command probe is Unix-specific")
	}
	repo := t.TempDir()
	writeChangedTestFile(t, filepath.Join(repo, "go.mod"), "module example.test/changed\n\ngo 1.25\n")
	writeChangedTestFile(t, filepath.Join(repo, "main.go"), "package changed\n")
	writeChangedTestFile(t, filepath.Join(repo, "README.md"), "before\n")
	for _, args := range [][]string{{"init"}, {"config", "user.email", "test@example.invalid"}, {"config", "user.name", "Test"}, {"add", "."}, {"commit", "-m", "base"}} {
		cmd := exec.Command("git", args...)
		cmd.Dir = repo
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, output)
		}
	}
	writeChangedTestFile(t, filepath.Join(repo, "README.md"), "after\n")

	realGo, err := exec.LookPath("go")
	if err != nil {
		t.Fatal(err)
	}
	bin := t.TempDir()
	marker := filepath.Join(t.TempDir(), "run-command")
	script := fmt.Sprintf("#!/bin/sh\nif [ \"$1\" = list ]; then exec %q \"$@\"; fi\nprintf '%%s\\n' \"$*\" > %q\n", realGo, marker)
	writeChangedTestFile(t, filepath.Join(bin, "go"), script)
	if err := os.Chmod(filepath.Join(bin, "go"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	var stdout, stderr bytes.Buffer
	if err := run(context.Background(), runConfig{
		base: "HEAD", repo: repo, includeUncommitted: true,
		extraGoTestArgs: []string{"-count=1"}, stdout: &stdout, stderr: &stderr,
	}); err != nil {
		t.Fatalf("run: %v\nstdout=%s\nstderr=%s", err, stdout.String(), stderr.String())
	}
	raw, err := os.ReadFile(marker)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := strings.TrimSpace(string(raw)), "run ./cmd/swarm-test -- -count=1 ./..."; got != want {
		t.Fatalf("executed command = %q, want %q", got, want)
	}
}

func writeChangedTestFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}
