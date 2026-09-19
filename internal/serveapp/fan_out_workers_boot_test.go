package serveapp

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

func TestBuildServeRuntimeRejectsInvalidFanOutWorkersBeforeAcquisition(t *testing.T) {
	for _, count := range []int{0, -1} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			cfg := &config.Config{Runtime: config.RuntimeConfig{FanOutWorkers: &count}}
			built, err := buildServeRuntimeBundleContext(serveRuntimeBundleContextRequest{
				ExecutionPosture: executionposture.Live, Ctx: context.Background(), Config: cfg,
			})
			if err == nil || !strings.Contains(err.Error(), "runtime.fan_out_workers") {
				t.Fatalf("build error = %v, want worker declaration refusal before store/source acquisition", err)
			}
			if built.runtime != nil {
				t.Fatal("invalid declaration constructed a runtime")
			}
		})
	}
}

func TestBuildServeRuntimeMapsFanOutWorkers(t *testing.T) {
	for _, count := range []int{0, 1, 4} {
		name := fmt.Sprint(count)
		if count == 0 {
			name = "absent"
		}
		t.Run(name, func(t *testing.T) {
			repo := repoRootForTest()
			root := canonicalrouting.WriteNovelDerivedScenarioBundle(t)
			loaded, err := loadServeRuntimeBundle(context.Background(), repo, nil, cliapp.CLISourcePlatformSpecPaths{
				SourceRoot: root, PlatformSpecPath: runtimecontracts.DefaultPlatformSpecFile(repo),
			}, cliapp.ServeOptions{}, testPlatformPackBaseGenerations(t))
			if err != nil {
				t.Fatal(err)
			}
			cfg, err := cliapp.DefaultRuntimeConfig()
			if err != nil {
				t.Fatal(err)
			}
			if count != 0 {
				cfg.Runtime.FanOutWorkers = &count
			}
			loaded.sourceArtifactFact = mustServeTestEphemeralSourceArtifactFact(loaded.bootIdentity.BundleHash)
			stores := openSelectedSQLiteOwner(t, filepath.Join(t.TempDir(), "worker-config.sqlite"), cfg)
			t.Cleanup(func() { closeUnactivatedSelectedStore(t, stores) })
			persistence := projectServeRuntimePersistence(stores)
			summary, err := initializeStateStores(context.Background(), persistence.schema, loaded.bundle)
			if err != nil {
				t.Fatal(err)
			}
			built, err := buildServeRuntimeBundleContext(serveRuntimeBundleContextRequest{
				ExecutionPosture: executionposture.Live,
				Ctx:              context.Background(), Stores: persistence, Loaded: loaded,
				StateStoreSummary: summary, Config: cfg,
				WorkspaceBackend: cliapp.WorkspaceBackendSelection{Backend: cliapp.WorkspaceBackendNone, NoWorkspace: true, Source: "test"},
				BootStartedAt:    time.Now().UTC(), ProcessWorkOwner: worklifetime.NewProcess(), RuntimeInstanceID: uuid.NewString(),
				ProviderTriggerCatalog: testProviderTriggerCatalog(t),
				Credentials:            processIngressCredentialStore{}, ProviderCredentials: processIngressCredentialStore{},
				Options: cliapp.ServeOptions{ShutdownGrace: time.Second, TestLLMRuntime: servedNoopLLMRuntime{}},
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := built.runtime.ShutdownWithOptions(runtime.ShutdownOptions{Grace: time.Second}); err != nil {
					t.Error(err)
					return
				}
				if built.workspaces != nil {
					if err := built.workspaces.ReleaseSourceProjection(context.Background()); err != nil {
						t.Error(err)
					}
				}
			})
			workers := built.runtime.Options.FanOutWorkers
			if count == 0 {
				if workers != nil {
					t.Fatal("absent declaration became an explicit backend default")
				}
				return
			}
			if workers == nil || *workers != count || workers == cfg.Runtime.FanOutWorkers {
				t.Fatalf("runtime options workers = %v, want copied %d", workers, count)
			}
			// Construction has no retained serving grant yet. The actual selected
			// backend validates its limits when serving registration is admitted.
		})
	}
}
