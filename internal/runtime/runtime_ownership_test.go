package runtime

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/packadmission"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
)

func TestRuntimeRequiresProcessGenerationGrant(t *testing.T) {
	module := loadRuntimeOwnershipWorkflowModule(t)
	cfg := &config.Config{}
	rt, err := NewRuntime(testAuthorActivityContext(context.Background()), RuntimeDeps{
		Config: cfg, Options: RuntimeOptions{
			ExecutionPosture: executionposture.Live,
			SelfCheck:        false, WorkflowModule: module, LLMRuntime: noopLLMRuntime{},
			RuntimeInstanceID:  authorActivityTestRuntimeInstanceID,
			SourceArtifactFact: testSourceArtifactFact(t, runtimeTestBundleHash),
			ProcessWorkOwner:   runtimeTestProcessWorkOwner(t),
		},
	})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	t.Cleanup(func() {
		if shutdownErr := rt.Shutdown(); shutdownErr != nil {
			t.Errorf("Shutdown: %v", shutdownErr)
		}
	})
	if err := rt.Start(testAuthorActivityContext(context.Background())); err == nil {
		t.Fatal("Start unexpectedly admitted a runtime without a process generation grant")
	}
}

func TestRuntimeShutdownRetiresGrantWithoutReleasingProcessCapability(t *testing.T) {
	module := loadRuntimeOwnershipWorkflowModule(t)
	cfg := &config.Config{}
	ctx, fact := runtimeOwnershipFixtureContext(t, module)
	authority, err := runtimeTestProcessAttribution(runtimestartupownership.AcquireRequest{
		OwnerID: "runtime-test-process", BootID: uuid.NewString(), RuntimeInstanceID: authorActivityTestRuntimeInstanceID,
	})
	if err != nil {
		t.Fatalf("new cold authority: %v", err)
	}
	selected := &runtimeTestRetainedSession{authority: authority, agents: map[string]runtimemanager.PersistedAgent{}}
	rt, err := NewRuntime(ctx, completeRuntimeRecoveryTestDeps(t, RuntimeDeps{
		Config:       cfg,
		ManagerStore: selected,
		ManagerPersistenceRoles: runtimemanager.PersistenceRoles{
			LifecycleCensus: selected,
		},
		Options: RuntimeOptions{
			ExecutionPosture: executionposture.Live,
			SelfCheck:        false, WorkflowModule: module, LLMRuntime: noopLLMRuntime{},
			RuntimeInstanceID:  authorActivityTestRuntimeInstanceID,
			SourceArtifactFact: fact, ProcessWorkOwner: runtimeTestProcessWorkOwner(t),
		},
	}))
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	capability, grant, err := newRuntimeTestProcessCapabilityWithSession(t, rt.Manager, module.source, fact, authorActivityTestRuntimeInstanceID, selected, func(session *runtimeTestRetainedSession) runtimestartupownership.RetainedSession {
		return &startupRecoveryFanOutSession{runtimeTestRetainedSession: session, capacity: runtimestartupownership.SQLiteFanOutCapacity()}
	})
	if err != nil {
		t.Fatalf("new process capability: %v", err)
	}
	if err := rt.InstallStartupGrant(grant); err != nil {
		t.Fatalf("InstallStartupGrant: %v", err)
	}
	if err := rt.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := rt.Shutdown(); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	select {
	case <-grant.Done():
	default:
		t.Fatal("runtime shutdown did not retire its generation grant")
	}
	select {
	case <-capability.Done():
		t.Fatal("runtime shutdown released the process capability")
	default:
	}
}

func loadRuntimeOwnershipWorkflowModule(t *testing.T) semanticOnlyWorkflowRuntime {
	t.Helper()
	repoRoot := filepath.Clean(filepath.Join("..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier8-boot-verification", "test-boot-success")
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOptions(
		repoRoot,
		fixtureRoot,
		platformSpec,
		runtimecontracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory},
	)
	if err != nil {
		t.Fatalf("load bundle: %v", err)
	}
	return semanticOnlyWorkflowRuntime{source: semanticview.Wrap(bundle)}
}

func runtimeOwnershipFixtureContext(t *testing.T, module semanticOnlyWorkflowRuntime) (context.Context, runtimecorrelation.SourceArtifactFact) {
	t.Helper()
	bundle, found := semanticview.Bundle(module.SemanticSource())
	if !found || bundle.SourceArtifact == nil {
		t.Fatal("runtime ownership fixture requires its admitted source artifact")
	}
	fact := testSourceArtifactFact(t, bundle.SourceArtifact.BundleHash())
	return testAuthorActivityContextForBundle(context.Background(), fact.BundleHash()), fact
}
