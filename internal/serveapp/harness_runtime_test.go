package serveapp

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

func TestServeRejectsHarnessInjectionBeforeRuntime(t *testing.T) {
	repo := repoRootForTest()
	root := canonicalrouting.ExampleRoot(t, canonicalrouting.HarnessInjection)
	loaded, err := loadServeRuntimeBundle(context.Background(), repo, nil, cliapp.CLIContractPlatformSpecPaths{
		ContractsPath: root, PlatformSpecPath: runtimecontracts.DefaultPlatformSpecFile(repo),
	}, cliapp.ServeOptions{}, testPlatformPackBaseGenerations(t))
	if err != nil {
		t.Fatalf("loadServeRuntimeBundle: %v", err)
	}
	cfg, err := cliapp.DefaultRuntimeConfig()
	if err != nil {
		t.Fatalf("DefaultRuntimeConfig: %v", err)
	}
	cfg.Runtime.ExecutionPosture = executionposture.Live
	loaded.bundleSourceFact = mustServeTestEphemeralBundleSourceFact(loaded.bootIdentity.BundleHash)
	contextDef, err := buildServeRuntimeBundleContext(serveRuntimeBundleContextRequest{
		Ctx: context.Background(), Loaded: loaded, StateStoreSummary: "test stores ready",
		WorkspaceBackend: cliapp.WorkspaceBackendSelection{Backend: cliapp.WorkspaceBackendNone, NoWorkspace: true, Source: "test"},
		BootStartedAt:    time.Now().UTC(), Config: cfg,
	})
	if err == nil || !strings.Contains(err.Error(), "production validation rejects test-only input source: harness") {
		t.Fatalf("buildServeRuntimeBundleContext = %#v error=%v, want production rejection", contextDef, err)
	}
	if contextDef.runtime != nil {
		t.Fatal("serve materialized a runtime for harness input")
	}
}

func TestBuildServeRuntimeContextFailureAfterRuntimeConstructionJoinsOccurrence(t *testing.T) {
	repo := repoRootForTest()
	root := canonicalrouting.WriteNovelDerivedScenarioBundle(t)
	loaded, err := loadServeRuntimeBundle(context.Background(), repo, nil, cliapp.CLIContractPlatformSpecPaths{
		ContractsPath: root, PlatformSpecPath: runtimecontracts.DefaultPlatformSpecFile(repo),
	}, cliapp.ServeOptions{}, testPlatformPackBaseGenerations(t))
	if err != nil {
		t.Fatalf("loadServeRuntimeBundle: %v", err)
	}
	cfg, err := cliapp.DefaultRuntimeConfig()
	if err != nil {
		t.Fatalf("DefaultRuntimeConfig: %v", err)
	}
	cfg.Runtime.ExecutionPosture = executionposture.Live
	loaded.bundleSourceFact = mustServeTestEphemeralBundleSourceFact(loaded.bootIdentity.BundleHash)
	stores := openSelectedSQLiteOwner(t, filepath.Join(t.TempDir(), "runtime-context-abort.sqlite"), cfg)
	t.Cleanup(func() { closeUnactivatedSelectedStore(t, stores) })
	persistence := projectServeRuntimePersistence(stores)
	stateStoreSummary, err := initializeStateStores(context.Background(), persistence.schema, loaded.bundle)
	if err != nil {
		t.Fatalf("initialize state stores: %v", err)
	}
	loaded.bundle.PackInventory = nil
	process := worklifetime.NewProcess()

	contextDef, err := buildServeRuntimeBundleContext(serveRuntimeBundleContextRequest{
		Ctx: context.Background(), Stores: persistence, Loaded: loaded,
		StateStoreSummary: stateStoreSummary, Config: cfg,
		WorkspaceBackend: cliapp.WorkspaceBackendSelection{Backend: cliapp.WorkspaceBackendNone, NoWorkspace: true, Source: "test"},
		BootStartedAt:    time.Now().UTC(), ProcessWorkOwner: process, RuntimeInstanceID: uuid.NewString(),
		ProviderTriggerCatalog: testProviderTriggerCatalog(t),
		Credentials:            processIngressCredentialStore{}, ProviderCredentials: processIngressCredentialStore{},
		Options: cliapp.ServeOptions{ShutdownGrace: time.Second, TestLLMRuntime: servedNoopLLMRuntime{}},
	})
	if err == nil || !strings.Contains(err.Error(), "bundle-specific effective pack inventory is required") {
		t.Fatalf("buildServeRuntimeBundleContext = %#v error=%v, want post-construction rejection", contextDef, err)
	}
	if contextDef.runtime != nil {
		t.Fatal("failed runtime context escaped its local owner")
	}
	if active := process.ActiveCount(); active != 0 {
		t.Fatalf("process active work after runtime-context abort = %d, want joined", active)
	}
}
