package cliapp

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestResolveCLISourcePlatformSpecPathsUsesExplicitSourceAndPlatformOwners(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	repo := t.TempDir()
	sourceRoot := filepath.Join(t.TempDir(), "source")
	configPlatform := filepath.Join(t.TempDir(), "config-platform.yaml")
	t.Setenv("SWARM_CONFIG", writeCLIAPIConfigFile(t, map[string]string{
		"platform_spec_path": configPlatform,
	}))

	got, err := ResolveCLISourcePlatformSpecPaths(repo, CLISourcePlatformSpecPathOptions{
		SourceRoot: sourceRoot,
	})
	if err != nil {
		t.Fatalf("resolve paths: %v", err)
	}
	if got.SourceRoot != sourceRoot {
		t.Fatalf("source path = %q, want %q", got.SourceRoot, sourceRoot)
	}
	if got.PlatformSpecPath != configPlatform {
		t.Fatalf("platform spec path = %q, want %q", got.PlatformSpecPath, configPlatform)
	}

	got, err = ResolveCLISourcePlatformSpecPaths(repo, CLISourcePlatformSpecPathOptions{
		SourceRoot:       sourceRoot,
		PlatformSpecPath: "explicit-platform.yaml",
	})
	if err != nil {
		t.Fatalf("resolve explicit platform path: %v", err)
	}
	if want := filepath.Join(repo, "explicit-platform.yaml"); got.PlatformSpecPath != want {
		t.Fatalf("platform spec path = %q, want %q", got.PlatformSpecPath, want)
	}

	t.Run("omitted source is invocation cwd", func(t *testing.T) {
		isolateCLIAPIConfigEnv(t)
		got, err := ResolveCLISourcePlatformSpecPaths(repo, CLISourcePlatformSpecPathOptions{})
		if err != nil {
			t.Fatalf("resolve omitted source: %v", err)
		}
		if got.SourceRoot != repo {
			t.Fatalf("omitted source root = %q, want invocation cwd %q", got.SourceRoot, repo)
		}
	})
}

func TestResolveCLISourcePlatformSpecPathsEmbeddedDefaultUsesInvocationRoot(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	outsideRepo := t.TempDir()
	sourceRoot := filepath.Join(t.TempDir(), "contracts")

	chdirForTest(t, outsideRepo)

	got, err := ResolveCLISourcePlatformSpecPaths(outsideRepo, CLISourcePlatformSpecPathOptions{
		SourceRoot: sourceRoot,
	})
	if err != nil {
		t.Fatalf("resolve paths: %v", err)
	}
	if got.SourceRoot != sourceRoot {
		t.Fatalf("contracts path = %q, want %q", got.SourceRoot, sourceRoot)
	}
	want, err := EmbeddedPlatformSpecPath()
	if err != nil {
		t.Fatalf("embedded platform spec path: %v", err)
	}
	if got.PlatformSpecPath != want {
		t.Fatalf("platform spec path = %q, want %q", got.PlatformSpecPath, want)
	}
	data, err := os.ReadFile(got.PlatformSpecPath)
	if err != nil {
		t.Fatalf("read embedded platform spec materialization: %v", err)
	}
	if !bytes.Contains(data, []byte("cli_specification:")) {
		t.Fatalf("materialized platform spec missing cli_specification")
	}
}

func TestResolveCLISourcePlatformSpecPathsRejectsMissingInvocationRoot(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	if _, err := ResolveCLISourcePlatformSpecPaths("", CLISourcePlatformSpecPathOptions{}); err == nil || !strings.Contains(err.Error(), "CLI invocation root is required") {
		t.Fatalf("missing invocation root error = %v", err)
	}
}

func TestCLIContractPathResolutionIgnoresLegacyContractsDir(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	repo := t.TempDir()
	legacyContracts := filepath.Join(t.TempDir(), "legacy-contracts")

	t.Setenv("SWARM_CONTRACTS_DIR", legacyContracts)

	chdirForTest(t, t.TempDir())
	got, err := ResolveCLISourcePlatformSpecPaths(repo, CLISourcePlatformSpecPathOptions{SourceRoot: "."})
	if err != nil {
		t.Fatalf("resolve paths: %v", err)
	}
	if got.SourceRoot != repo {
		t.Fatalf("source path = %q, want invocation root %q; SWARM_CONTRACTS_DIR must not be a CLI source", got.SourceRoot, repo)
	}
}

func TestResolveCLISourcePlatformSpecPathsFailClosedOnUnsupportedConfigKey(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(configPath, []byte("retry: true\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	t.Setenv("SWARM_CONFIG", configPath)

	_, err := ResolveCLISourcePlatformSpecPaths(t.TempDir(), CLISourcePlatformSpecPathOptions{})
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
