package cliapp

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/platform"
)

func TestResolveCLISourcePlatformSpecPathsUsesExplicitSourceAndPlatformOwners(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	repo := t.TempDir()
	sourceRoot := filepath.Join(t.TempDir(), "source")
	if err := os.Mkdir(sourceRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	wantSource := mustInvocationRootForTest(sourceRoot).Path()
	t.Setenv("SWARM_CONFIG", writeCLIAPIConfigFile(t, map[string]string{
		"swarm_dir": filepath.Join(t.TempDir(), "state"),
	}))

	got, err := ResolveCLISourcePaths(repo, CLISourcePathOptions{
		SourceRoot: sourceRoot,
	})
	if err != nil {
		t.Fatalf("resolve paths: %v", err)
	}
	if got.SourceRoot != wantSource {
		t.Fatalf("source path = %q, want %q", got.SourceRoot, wantSource)
	}
	t.Run("omitted source is invocation cwd", func(t *testing.T) {
		isolateCLIAPIConfigEnv(t)
		got, err := ResolveCLISourcePaths(repo, CLISourcePathOptions{})
		if err != nil {
			t.Fatalf("resolve omitted source: %v", err)
		}
		if want := mustInvocationRootForTest(repo).Path(); got.SourceRoot != want {
			t.Fatalf("omitted source root = %q, want invocation cwd %q", got.SourceRoot, want)
		}
	})
}

func TestResolveCLISourcePlatformSpecPathsEmbeddedDefaultUsesInvocationRoot(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	outsideRepo := t.TempDir()
	if err := os.WriteFile(filepath.Join(outsideRepo, "platform-spec.yaml"), []byte("not the embedded spec\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	sourceRoot := filepath.Join(RepoRoot(), "tests", "tier8-boot-verification", "test-boot-success")

	chdirForTest(t, outsideRepo)

	got, err := ResolveCLISourcePaths(outsideRepo, CLISourcePathOptions{
		SourceRoot: sourceRoot,
	})
	if err != nil {
		t.Fatalf("resolve paths: %v", err)
	}
	if want := mustInvocationRootForTest(sourceRoot).Path(); got.SourceRoot != want {
		t.Fatalf("contracts path = %q, want %q", got.SourceRoot, want)
	}
	want, err := EmbeddedPlatformSpecPath()
	if err != nil {
		t.Fatalf("embedded platform spec path: %v", err)
	}
	_, bundle, err := NewSwarmWorkflowModule(outsideRepo, got.SourceRoot)
	if err != nil {
		t.Fatalf("load source with embedded spec: %v", err)
	}
	if bundle.Paths.PlatformSpecFile != want {
		t.Fatalf("platform spec path = %q, want embedded %q", bundle.Paths.PlatformSpecFile, want)
	}
	data, err := os.ReadFile(bundle.Paths.PlatformSpecFile)
	if err != nil {
		t.Fatalf("read embedded platform spec materialization: %v", err)
	}
	if !bytes.Contains(data, []byte("cli_specification:")) {
		t.Fatalf("materialized platform spec missing cli_specification")
	}
	if !bytes.Equal(data, platform.PlatformSpecYAML()) {
		t.Fatal("default spec differs from the binary's embedded bytes")
	}
}

func TestResolveCLISourcePlatformSpecPathsRejectsConfigOverrideBeforeSourceLoad(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	path := writeCLIAPIConfigFile(t, map[string]string{"platform_spec_path": "must-not-be-read.yaml"})
	_, err := ResolveCLISourcePaths(t.TempDir(), CLISourcePathOptions{
		ConfigPath: path,
	})
	if err == nil || !strings.Contains(err.Error(), "`paths.platform_spec_path` at "+path+":2:3 is not a config key; the platform spec is embedded in `swarm`.") {
		t.Fatalf("source selection bypassed invalid config admission: %v", err)
	}
}

func TestResolveCLISourcePlatformSpecPathsRejectsMissingInvocationRoot(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	if _, err := ResolveCLISourcePaths("", CLISourcePathOptions{}); err == nil || !strings.Contains(err.Error(), "CLI invocation root is required") {
		t.Fatalf("missing invocation root error = %v", err)
	}
}

func TestCLIContractPathResolutionRejectsUndelegatedContractsDir(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	repo := t.TempDir()
	legacyContracts := filepath.Join(t.TempDir(), "legacy-contracts")

	t.Setenv("SWARM_CONTRACTS_DIR", legacyContracts)

	chdirForTest(t, t.TempDir())
	if _, err := ResolveCLISourcePaths(repo, CLISourcePathOptions{SourceRoot: "."}); err == nil || !strings.Contains(err.Error(), "env/unknown_stale @ SWARM_CONTRACTS_DIR") {
		t.Fatalf("unsupported source path environment: %v", err)
	}
	t.Setenv("SWARM_CONTRACTS_DIR", "")
	got, err := ResolveCLISourcePaths(repo, CLISourcePathOptions{SourceRoot: "."})
	if err != nil {
		t.Fatalf("resolve supported source path: %v", err)
	}
	if want := mustInvocationRootForTest(repo).Path(); got.SourceRoot != want {
		t.Fatalf("source path = %q, want invocation root %q; SWARM_CONTRACTS_DIR must not be a CLI source", got.SourceRoot, want)
	}
}

func TestResolveCLISourcePlatformSpecPathsFailClosedOnUnsupportedConfigKey(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(configPath, []byte("retry: true\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	t.Setenv("SWARM_CONFIG", configPath)

	_, err := ResolveCLISourcePaths(t.TempDir(), CLISourcePathOptions{})
	if err == nil {
		t.Fatal("resolve paths returned nil error")
	}
	if !strings.Contains(err.Error(), `unknown config key "retry"`) {
		t.Fatalf("err = %q", err.Error())
	}
}

func TestRunVerifyCommandConsumesExplicitSourceRoot(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	repo := RepoRoot()
	missingSource := filepath.Join(repo, "tests", "tier8-boot-verification", "test-boot-success", "zzz-not-a-real-dir")

	var out bytes.Buffer
	opts := defaultVerifyCommandOptions()
	opts.sourceRoot = missingSource
	code := runVerifyCommandWithOutput(context.Background(), repo, opts, &out, &out)
	if code == 0 {
		t.Fatalf("verify unexpectedly succeeded: %s", out.String())
	}
	if !strings.Contains(out.String(), missingSource) {
		t.Fatalf("verify did not use explicit source path %q:\n%s", missingSource, out.String())
	}
}
