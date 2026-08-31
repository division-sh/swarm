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

func TestRunServeRuntimeRejectsRetiredConfiguredContractPathBeforeBundleLoad(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	repo := repoRootForTest()
	configContracts := filepath.Join(repo, "tests", "tier8-boot-verification", "test-boot-success", "zzz-not-a-real-dir")
	t.Setenv("SWARM_CONFIG", writeCLIAPIConfigFile(t, map[string]string{
		"contracts_path": configContracts,
	}))
	originalBuildStores := buildStoresForServe
	owner := openSelectedSQLiteOwner(t, filepath.Join(t.TempDir(), "runtime.sqlite"), &config.Config{})
	t.Cleanup(func() { closeUnactivatedSelectedStore(t, owner) })
	buildStoresForServe = func(context.Context, storebackend.Selection, *config.Config) (*selectedStoreOwner, error) {
		return owner, nil
	}
	t.Cleanup(func() {
		buildStoresForServe = originalBuildStores
	})

	var out bytes.Buffer
	code := runFrom(context.Background(), repo, cliapp.ServeOptions{
		StoreMode:     "postgres",
		APIListenAddr: defaultAPIListenAddr,
		MCPListenAddr: defaultMCPListenAddr,
		ShutdownGrace: runtime.DefaultShutdownGrace,
		SelfCheck:     true,
		Verbose:       true,
		Output:        &out,
	})
	if code == 0 {
		t.Fatalf("serve unexpectedly succeeded: %s", out.String())
	}
	for _, want := range []string{
		`config key "paths.contracts_path" is recognized but not yet supported`,
		`RETIRED: authored source roots are positional command inputs; remove paths.contracts_path`,
	} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("serve boot output missing retired path rejection %q:\n%s", want, out.String())
		}
	}
	if strings.Contains(out.String(), "bundle_load") {
		t.Fatalf("serve reached bundle loading after retired config path rejection:\n%s", out.String())
	}
}
