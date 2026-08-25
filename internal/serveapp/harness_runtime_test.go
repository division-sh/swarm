package serveapp

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestServeRejectsHarnessInjectionBeforeRuntime(t *testing.T) {
	repo := cliapp.RepoRoot()
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
