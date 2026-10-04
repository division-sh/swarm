package cliapp

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRuntimeConfigDeploymentIntentUsesAdmittedLayerFacts(t *testing.T) {
	for _, name := range []string{"defaults", "user global", "project", "local operator", "explicit", "delegated locator"} {
		t.Run(name, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			repo := t.TempDir()
			opts := RuntimeConfigLoadOptions{RepoRoot: repo}
			path := ""
			switch name {
			case "user global":
				path = userGlobalUnifiedConfigPath()
			case "project":
				path = filepath.Join(repo, "swarm.yaml")
			case "local operator":
				path = filepath.Join(repo, ".swarm", "swarm.yaml")
			case "explicit":
				path = filepath.Join(t.TempDir(), "operator.yaml")
				opts.ExplicitPath = path
			case "delegated locator":
				path = filepath.Join(t.TempDir(), "operator.yaml")
				t.Setenv("SWARM_CONFIG", path)
			}
			if path != "" {
				if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
					t.Fatal(err)
				}
				writeRuntimeConfigText(t, path, "serve:\n  api_listen_addr: 127.0.0.1:4444\n")
			}
			loaded, err := LoadRuntimeConfigWithOptions(opts)
			if err != nil {
				t.Fatal(err)
			}
			want := name != "defaults" && name != "user global"
			if loaded.DeploymentConfigured() != want {
				t.Fatalf("deployment intent = %t, layers %+v, want %t", loaded.DeploymentConfigured(), loaded.Layers, want)
			}
		})
	}
}

func TestRuntimeConfigAdmissionRejectsExplicitErrorsBeforeIntentSelection(t *testing.T) {
	for _, name := range []string{"missing", "malformed", "unknown"} {
		t.Run(name, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			path := filepath.Join(t.TempDir(), "operator.yaml")
			if name == "malformed" {
				writeRuntimeConfigText(t, path, "serve: [\n")
			} else if name == "unknown" {
				writeRuntimeConfigText(t, path, "serve:\n  invented: true\n")
			}
			if _, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: t.TempDir(), ExplicitPath: path}); err == nil {
				t.Fatal("explicit deployment failure admitted as default/portable context")
			}
		})
	}
}

func TestRuntimeConfigAdmissionOwnsFrozenServeProjection(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	repo := t.TempDir()
	path := filepath.Join(repo, "operator.yaml")
	tokenPath := filepath.Join(repo, "token")
	if err := os.WriteFile(tokenPath, []byte("operator-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	writeRuntimeConfigText(t, path, "serve:\n  api_listen_addr: 127.0.0.1:4444\n  mcp_listen_addr: 127.0.0.1:5555\n  api_token_file: token\n")
	loaded, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: repo, ExplicitPath: path})
	if err != nil {
		t.Fatal(err)
	}
	// Re-reading this file would fail admission or substitute different listeners.
	writeRuntimeConfigText(t, path, "serve:\n  invented: true\n")
	opts := DefaultServeOptions()
	api, mcp, err := loaded.ResolveServeListeners(opts, false, false)
	if err != nil || api != "127.0.0.1:4444" || mcp != "127.0.0.1:5555" {
		t.Fatalf("frozen listener projection = %q, %q, %v", api, mcp, err)
	}
	opts.APIListenAddr, opts.MCPListenAddr = "127.0.0.1:6666", ""
	api, mcp, err = loaded.ResolveServeListeners(opts, true, true)
	if err != nil || api != "127.0.0.1:6666" || mcp != "" {
		t.Fatalf("explicit flag presence lost = %q, %q, %v", api, mcp, err)
	}
	auth, err := loaded.ResolveServeAPIAuth(mustInvocationRootForTest(repo), DefaultServeOptions())
	if err != nil || !auth.Explicit || len(auth.Tokens) != 1 || auth.Tokens[0] != "operator-token" || !strings.HasSuffix(auth.TokenFile, "/token") {
		t.Fatalf("frozen auth projection failed: explicit=%t, error=%v", auth.Explicit, err)
	}
	if _, _, err := (RuntimeConfigLoadResult{}).ResolveServeListeners(opts, false, false); err == nil {
		t.Fatal("missing frozen config admitted listener defaults")
	}
	if _, err := (RuntimeConfigLoadResult{}).ResolveServeAPIAuth(mustInvocationRootForTest(repo), opts); err == nil {
		t.Fatal("missing frozen config admitted auth defaults")
	}
}
