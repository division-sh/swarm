package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

func TestRuntimeStart_PackageBackedFlowOwnedStaticAgentsCarryCanonicalMemoryIdentity(t *testing.T) {
	source := loadPackageBackedRuntimeAgentMemorySource(t)
	assertRuntimeStartCarriesMemoryIdentity(t, source)
}

func TestRuntimeStart_StructurallyNestedProjectAgentsCarryCanonicalMemoryIdentity(t *testing.T) {
	source := loadNestedProjectRuntimeAgentMemorySource(t)
	assertRuntimeStartCarriesMemoryIdentity(t, source)
}

func assertRuntimeStartCarriesMemoryIdentity(t *testing.T, source semanticview.Source) {
	t.Helper()
	rt, err := newScopedTestRuntime(t, testAuthorActivityContext(context.Background()), RuntimeDeps{Config: &config.Config{}, Options: RuntimeOptions{
		SelfCheck:      false,
		LLMRuntime:     noopLLMRuntime{},
		WorkflowModule: semanticOnlyWorkflowRuntime{source: source},
	}})

	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	t.Cleanup(func() {
		_ = rt.Shutdown()
	})

	if err := rt.Start(testAuthorActivityContext(context.Background())); err != nil {
		t.Fatalf("Start: %v", err)
	}

	runID := uuid.NewString()
	if _, err := rt.Manager.ResolveAgentConfig(runID, "backend", "support"); !errors.Is(err, runtimemanager.ErrAgentNotFound) {
		t.Fatalf("pre-run static agent lookup error = %v, want not found", err)
	}
	records, err := runtimemanager.StaticAgentMaterializationRecords(runID, source)
	if err != nil {
		t.Fatalf("materialize static declaration plans: %v", err)
	}
	var identityFound bool
	var route events.DeliveryRoute
	for _, record := range records {
		identity, identityErr := record.Config.ConcreteIdentity()
		if identityErr != nil {
			t.Fatalf("static declaration identity: %v", identityErr)
		}
		if identity.AgentID() == "backend" && identity.FlowInstance() == "support" {
			identityFound = true
			route = events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient("backend"), AgentIdentity: identity}
			break
		}
	}
	if !identityFound {
		t.Fatal("backend@support declaration plan not found")
	}
	event := eventtest.RunCreatingRootIngress(uuid.NewString(), events.EventType("test.static.ready"), "", "", []byte(`{}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
	if err := rt.Manager.FinalizeCommittedAgentReadiness(ctx, event, []events.DeliveryRoute{route}); err != nil {
		t.Fatalf("finalize committed static agent readiness: %v", err)
	}
	cfg, err := rt.Manager.ResolveAgentConfig(runID, "backend", "support")
	if err != nil {
		t.Fatalf("resolve package-backed static flow agent config: %v", err)
	}
	if cfg.FlowPath != "support" {
		t.Fatalf("FlowPath = %q, want support", cfg.FlowPath)
	}
	if cfg.FlowID != "support" {
		t.Fatalf("FlowID = %q, want support", cfg.FlowID)
	}
	if cfg.Memory != agentmemory.Authored(true) {
		t.Fatalf("Memory = %#v, want authored true", cfg.Memory)
	}
}

func loadPackageBackedRuntimeAgentMemorySource(t *testing.T) semanticview.Source {
	t.Helper()
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	root := canonicalrouting.CopyRuntimeAgentMemory(t, canonicalrouting.RuntimeAgentMemoryPackageBacked)

	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return semanticview.Wrap(bundle)
}

func loadNestedProjectRuntimeAgentMemorySource(t *testing.T) semanticview.Source {
	t.Helper()
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	root := canonicalrouting.CopyRuntimeAgentMemory(t, canonicalrouting.RuntimeAgentMemoryNestedProject)

	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return semanticview.Wrap(bundle)
}
