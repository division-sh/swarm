package cliapp

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestResolveCLIContractPlatformSpecPathsPrecedenceAndDiscovery(t *testing.T) {
	repo := t.TempDir()
	discoveredContracts := filepath.Join(repo, "contracts")
	writeWorkflowValidationFixtureFile(t, filepath.Join(discoveredContracts, "package.yaml"), `name: discovered`)
	configContracts := filepath.Join(t.TempDir(), "config-contracts")
	configPlatform := filepath.Join(t.TempDir(), "config-platform.yaml")
	envContracts := filepath.Join(t.TempDir(), "env-contracts")

	t.Run("flags beat env config and discovery", func(t *testing.T) {
		isolateCLIAPIConfigEnv(t)
		t.Setenv("SWARM_CONTRACTS_PATH", envContracts)
		t.Setenv("SWARM_CONFIG", writeCLIAPIConfigFile(t, map[string]string{
			"contracts_path":     configContracts,
			"platform_spec_path": configPlatform,
		}))

		got, err := ResolveCLIContractPlatformSpecPaths(repo, CLIContractPlatformSpecPathOptions{
			ContractsPath:    "flag-contracts",
			PlatformSpecPath: "flag-platform.yaml",
		})
		if err != nil {
			t.Fatalf("resolve paths: %v", err)
		}
		if want := filepath.Join(repo, "flag-contracts"); got.ContractsPath != want {
			t.Fatalf("contracts path = %q, want %q", got.ContractsPath, want)
		}
		if want := filepath.Join(repo, "flag-platform.yaml"); got.PlatformSpecPath != want {
			t.Fatalf("platform spec path = %q, want %q", got.PlatformSpecPath, want)
		}
	})

	t.Run("environment beats config and discovery for contracts only", func(t *testing.T) {
		isolateCLIAPIConfigEnv(t)
		t.Setenv("SWARM_CONTRACTS_PATH", envContracts)
		t.Setenv("SWARM_CONFIG", writeCLIAPIConfigFile(t, map[string]string{
			"contracts_path":     configContracts,
			"platform_spec_path": configPlatform,
		}))

		got, err := ResolveCLIContractPlatformSpecPaths(repo, CLIContractPlatformSpecPathOptions{})
		if err != nil {
			t.Fatalf("resolve paths: %v", err)
		}
		if got.ContractsPath != envContracts {
			t.Fatalf("contracts path = %q, want %q", got.ContractsPath, envContracts)
		}
		if got.PlatformSpecPath != configPlatform {
			t.Fatalf("platform spec path = %q, want %q", got.PlatformSpecPath, configPlatform)
		}
	})

	t.Run("config beats discovery and built in default", func(t *testing.T) {
		isolateCLIAPIConfigEnv(t)
		t.Setenv("SWARM_CONFIG", writeCLIAPIConfigFile(t, map[string]string{
			"contracts_path":     configContracts,
			"platform_spec_path": configPlatform,
		}))

		got, err := ResolveCLIContractPlatformSpecPaths(repo, CLIContractPlatformSpecPathOptions{})
		if err != nil {
			t.Fatalf("resolve paths: %v", err)
		}
		if got.ContractsPath != configContracts {
			t.Fatalf("contracts path = %q, want %q", got.ContractsPath, configContracts)
		}
		if got.PlatformSpecPath != configPlatform {
			t.Fatalf("platform spec path = %q, want %q", got.PlatformSpecPath, configPlatform)
		}
	})

	t.Run("discovers repo contracts and embedded platform spec last", func(t *testing.T) {
		isolateCLIAPIConfigEnv(t)

		got, err := ResolveCLIContractPlatformSpecPaths(repo, CLIContractPlatformSpecPathOptions{})
		if err != nil {
			t.Fatalf("resolve paths: %v", err)
		}
		if got.ContractsPath != discoveredContracts {
			t.Fatalf("contracts path = %q, want %q", got.ContractsPath, discoveredContracts)
		}
		want, err := EmbeddedPlatformSpecPath()
		if err != nil {
			t.Fatalf("embedded platform spec path: %v", err)
		}
		if got.PlatformSpecPath != want {
			t.Fatalf("platform spec path = %q, want %q", got.PlatformSpecPath, want)
		}
	})
}

func TestResolveCLIContractPlatformSpecPathsEmbeddedDefaultDoesNotRequireRepoRoot(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	outsideRepo := t.TempDir()
	contractsRoot := filepath.Join(t.TempDir(), "contracts")
	writeWorkflowValidationFixtureFile(t, filepath.Join(contractsRoot, "package.yaml"), `name: external`)
	chdirForTest(t, outsideRepo)

	got, err := ResolveCLIContractPlatformSpecPaths("", CLIContractPlatformSpecPathOptions{
		ContractsPath: contractsRoot,
	})
	if err != nil {
		t.Fatalf("resolve paths: %v", err)
	}
	if got.ContractsPath != contractsRoot {
		t.Fatalf("contracts path = %q, want %q", got.ContractsPath, contractsRoot)
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

func TestCLIContractPathResolutionIgnoresLegacyContractsDir(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	repo := t.TempDir()
	legacyContracts := filepath.Join(t.TempDir(), "legacy-contracts")
	writeWorkflowValidationFixtureFile(t, filepath.Join(legacyContracts, "package.yaml"), `name: legacy`)
	t.Setenv("SWARM_CONTRACTS_DIR", legacyContracts)

	got, err := ResolveCLIContractPlatformSpecPaths(repo, CLIContractPlatformSpecPathOptions{})
	if err != nil {
		t.Fatalf("resolve paths: %v", err)
	}
	if got.ContractsPath != "" {
		t.Fatalf("contracts path = %q, want empty; SWARM_CONTRACTS_DIR must not be a CLI source", got.ContractsPath)
	}
}

func TestResolveCLIContractPlatformSpecPathsFailClosedOnUnsupportedConfigKey(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(configPath, []byte("retry: true\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	t.Setenv("SWARM_CONFIG", configPath)

	_, err := ResolveCLIContractPlatformSpecPaths(t.TempDir(), CLIContractPlatformSpecPathOptions{})
	if err == nil {
		t.Fatal("resolve paths returned nil error")
	}
	if !strings.Contains(err.Error(), `unknown config key "retry"`) {
		t.Fatalf("err = %q", err.Error())
	}
}

func TestRunVerifyCommandConsumesContractPathResolver(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	repo := RepoRoot()
	configContracts := filepath.Join(t.TempDir(), "config-contracts")
	envContracts := filepath.Join(repo, "tests", "tier8-boot-verification", "test-boot-success", "zzz-not-a-real-dir")
	legacyContracts := filepath.Join(t.TempDir(), "legacy-contracts")
	writeWorkflowValidationFixtureFile(t, filepath.Join(legacyContracts, "package.yaml"), `name: legacy`)
	t.Setenv("SWARM_CONFIG", writeCLIAPIConfigFile(t, map[string]string{
		"contracts_path": configContracts,
	}))
	t.Setenv("SWARM_CONTRACTS_PATH", envContracts)
	t.Setenv("SWARM_CONTRACTS_DIR", legacyContracts)

	var out bytes.Buffer
	code := runVerifyCommandWithOutput(context.Background(), repo, defaultVerifyCommandOptions(), &out, &out)
	if code == 0 {
		t.Fatalf("verify unexpectedly succeeded: %s", out.String())
	}
	if !strings.Contains(out.String(), envContracts) {
		t.Fatalf("verify did not use SWARM_CONTRACTS_PATH path %q:\n%s", envContracts, out.String())
	}
	if strings.Contains(out.String(), configContracts) || strings.Contains(out.String(), legacyContracts) {
		t.Fatalf("verify used lower-priority or legacy path:\n%s", out.String())
	}
}
