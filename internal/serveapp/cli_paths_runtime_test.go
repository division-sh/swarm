package serveapp

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
)

func TestRunServeRuntimeConsumesContractPathResolverBeforeBundleLoad(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	repo := cliapp.RepoRoot()
	configContracts := filepath.Join(repo, "tests", "tier8-boot-verification", "test-boot-success", "zzz-not-a-real-dir")
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
	code := Run(context.Background(), repo, cliapp.ServeOptions{
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
		t.Fatalf("serve boot output did not use config paths.contracts_path %q:\n%s", configContracts, out.String())
	}
	if !strings.Contains(out.String(), "bundle_load") || !strings.Contains(out.String(), "FAILED") {
		t.Fatalf("serve did not reach bundle_load failure proof:\n%s", out.String())
	}
}
