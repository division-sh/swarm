package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
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

		got, err := resolveCLIContractPlatformSpecPaths(repo, cliContractPlatformSpecPathOptions{
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

		got, err := resolveCLIContractPlatformSpecPaths(repo, cliContractPlatformSpecPathOptions{})
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

		got, err := resolveCLIContractPlatformSpecPaths(repo, cliContractPlatformSpecPathOptions{})
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

		got, err := resolveCLIContractPlatformSpecPaths(repo, cliContractPlatformSpecPathOptions{})
		if err != nil {
			t.Fatalf("resolve paths: %v", err)
		}
		if got.ContractsPath != discoveredContracts {
			t.Fatalf("contracts path = %q, want %q", got.ContractsPath, discoveredContracts)
		}
		want, err := embeddedPlatformSpecPath()
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

	got, err := resolveCLIContractPlatformSpecPaths("", cliContractPlatformSpecPathOptions{
		ContractsPath: contractsRoot,
	})
	if err != nil {
		t.Fatalf("resolve paths: %v", err)
	}
	if got.ContractsPath != contractsRoot {
		t.Fatalf("contracts path = %q, want %q", got.ContractsPath, contractsRoot)
	}
	want, err := embeddedPlatformSpecPath()
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

	got, err := resolveCLIContractPlatformSpecPaths(repo, cliContractPlatformSpecPathOptions{})
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

	_, err := resolveCLIContractPlatformSpecPaths(t.TempDir(), cliContractPlatformSpecPathOptions{})
	if err == nil {
		t.Fatal("resolve paths returned nil error")
	}
	if !strings.Contains(err.Error(), `unsupported CLI config key "retry"`) {
		t.Fatalf("err = %q", err.Error())
	}
}

func TestRunVerifyCommandConsumesContractPathResolver(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	repo := repoRoot()
	configContracts := filepath.Join(t.TempDir(), "config-contracts")
	envContracts := filepath.Join(t.TempDir(), "env-contracts")
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

func TestRunServeRuntimeConsumesContractPathResolverBeforeBundleLoad(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	repo := t.TempDir()
	configContracts := filepath.Join(t.TempDir(), "config-contracts")
	t.Setenv("SWARM_CONFIG", writeCLIAPIConfigFile(t, map[string]string{
		"contracts_path": configContracts,
	}))
	originalBuildStores := buildStoresForServe
	buildStoresForServe = func(context.Context, storebackend.Selection, *config.Config) (storeBundle, error) {
		return storeBundle{}, nil
	}
	t.Cleanup(func() {
		buildStoresForServe = originalBuildStores
	})

	var out bytes.Buffer
	code := runServeRuntime(context.Background(), repo, serveOptions{
		StoreMode:          "postgres",
		APIListenAddr:      defaultAPIListenAddr,
		MCPListenAddr:      defaultMCPListenAddr,
		ShutdownGrace:      runtime.DefaultShutdownGrace,
		SelfCheck:          true,
		RequireBundleMatch: true,
		Verbose:            true,
		Output:             &out,
	})
	if code == 0 {
		t.Fatalf("serve unexpectedly succeeded: %s", out.String())
	}
	if !strings.Contains(out.String(), "contracts="+configContracts) {
		t.Fatalf("serve boot output did not use config contracts_path %q:\n%s", configContracts, out.String())
	}
	if !strings.Contains(out.String(), "bundle_load") || !strings.Contains(out.String(), "FAILED") {
		t.Fatalf("serve did not reach bundle_load failure proof:\n%s", out.String())
	}
}
