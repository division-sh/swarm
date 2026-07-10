package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestContextListCommandUsesSwarmDirRegistry(t *testing.T) {
	swarmDir := t.TempDir()
	registry := newLocalContextRegistry(swarmDir)
	desc := testLocalContextDescriptor("local", "runtime-a")
	desc.APIServer = "http://127.0.0.1:0/local"
	if err := registry.WriteDescriptor(desc); err != nil {
		t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"--swarm-dir", swarmDir, "context", "list"}, &out, &errOut, rootCommandOptions{})
	if code != 0 {
		t.Fatalf("exit = %d stderr=%s", code, errOut.String())
	}
	if !strings.Contains(out.String(), "local") || !strings.Contains(out.String(), "no_server") {
		t.Fatalf("output = %q, want local no_server row", out.String())
	}
}

func TestContextListCommandSwarmDirFlagBypassesBrokenConfig(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	swarmDir := t.TempDir()
	t.Setenv("SWARM_CONFIG", filepath.Join(t.TempDir(), "missing-config.yaml"))
	var out, errOut bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"--swarm-dir", swarmDir, "context", "list"}, &out, &errOut, rootCommandOptions{})
	if code != 0 {
		t.Fatalf("exit = %d stderr=%s", code, errOut.String())
	}
	if !strings.Contains(out.String(), "No contexts found.") {
		t.Fatalf("output = %q, want empty registry", out.String())
	}
}

func TestContextListCommandSurfacesZeroEntryRegistryFailure(t *testing.T) {
	swarmDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(swarmDir, "contexts"), []byte("not a registry directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"--swarm-dir", swarmDir, "context", "list"}, &out, &errOut, rootCommandOptions{})
	if code != 0 {
		t.Fatalf("exit = %d stderr=%s", code, errOut.String())
	}
	if !strings.Contains(out.String(), "No contexts found.") {
		t.Fatalf("output = %q, want empty-list marker", out.String())
	}
	if !strings.Contains(out.String(), "registry_status: invalid_descriptor") || !strings.Contains(out.String(), "detail:") {
		t.Fatalf("output = %q, want registry failure status and detail", out.String())
	}
}

func TestContextCurrentCommandReportsEmptyRegistry(t *testing.T) {
	var out, errOut bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"--swarm-dir", filepath.Join(t.TempDir(), "state"), "context", "current"}, &out, &errOut, rootCommandOptions{})
	if code != 0 {
		t.Fatalf("exit = %d stderr=%s", code, errOut.String())
	}
	if !strings.Contains(out.String(), "current context: none") {
		t.Fatalf("output = %q, want no current context", out.String())
	}
}
