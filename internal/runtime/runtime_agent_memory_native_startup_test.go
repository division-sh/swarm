package runtime_test

import (
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestRuntimeStart_DirectFilesystemFlowAgentsCarryCanonicalMemoryIdentity(t *testing.T) {
	assertNativeRuntimeStartCarriesMemoryIdentity(t, canonicalrouting.RuntimeAgentMemoryDirectFlow, "support")
}

func TestRuntimeStart_StructurallyNestedFlowAgentsCarryCanonicalMemoryIdentity(t *testing.T) {
	assertNativeRuntimeStartCarriesMemoryIdentity(t, canonicalrouting.RuntimeAgentMemoryNestedProject, "support/extras")
}

func assertNativeRuntimeStartCarriesMemoryIdentity(t *testing.T, variant canonicalrouting.RuntimeAgentMemoryVariant, flowPath string) {
	t.Helper()
	root := canonicalrouting.CopyRuntimeAgentMemory(t, variant)
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("load memory-identity source: %v", err)
	}
	source := semanticview.Wrap(bundle)
	artifact, err := sourceartifact.AdmitDirectory(root)
	if err != nil {
		t.Fatalf("admit memory-identity artifact: %v", err)
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected startupRecoveryOrderStore
			if backend == "postgres" {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				selected = storetest.AdmitPostgresRuntimeStore(t, db)
			} else {
				selected = storetest.StartSQLiteRuntimeStore(t)
			}
			sourceFact, err := runtimecorrelation.NewSourceArtifactFact(artifact.BundleHash())
			if err != nil {
				t.Fatalf("construct memory-identity source fact: %v", err)
			}
			runID := uuid.NewString()
			ctx := startupRecoverySourceContext(sourceFact, runID)
			seedStartupRecoverySourceRun(t, ctx, selected, sourceFact, artifact, runID)
			process := worklifetime.NewProcess()
			rt, err := runtimepkg.NewRuntime(ctx, completeExternalRuntimeTestWorkflowDeps(t, selected, runtimepkg.RuntimeDeps{
				Config:     &config.Config{LLM: config.LLMConfig{Backend: "anthropic"}},
				EventStore: selected, EventBusDurable: externalRuntimeTestDurableDependencies(selected),
				EventPayloadAdmissionBinder: selected, AuthorActivityRegistrars: []runtimepkg.AuthorActivityCatalogRegistrar{selected},
				RunLifecycleCandidates: selected, WorkflowPersistence: runtimepipeline.NewWorkflowPersistence(selected),
				ManagerStore: selected, ManagerPersistenceRoles: externalRuntimeTestSelectedManagerRoles(selected),
				DeliveryStore: selected, PipelineObligations: selected.PipelineObligations(),
				Options: runtimepkg.RuntimeOptions{
					SelfCheck: false, WorkflowModule: newRuntimeTestWorkflowModule(t, source), LLMRuntime: startupRecoveryOrderLLM{},
					RuntimeInstanceID: authorActivityTestRuntimeInstanceID, SourceArtifactFact: sourceFact, ProcessWorkOwner: process,
				},
			}))
			if err != nil {
				t.Fatalf("NewRuntime: %v", err)
			}
			capability, _ := installExternalRuntimeTestGeneration(t, ctx, selected, rt)
			t.Cleanup(func() {
				if err := closeExternalRuntimeTestGeneration(rt, process, capability); err != nil {
					t.Errorf("close memory-identity generation: %v", err)
				}
			})
			if err := rt.Start(ctx); err != nil {
				t.Fatalf("Start: %v", err)
			}
			if _, err := rt.Manager.ResolveAgentConfig(runID, "backend", flowPath); !errors.Is(err, runtimemanager.ErrAgentNotFound) {
				t.Fatalf("pre-construction static agent lookup = %v, want not found", err)
			}
			ctx = effects.WithExecutionMode(worklifetime.WithOccurrence(ctx, rt.WorkOccurrence()), effects.ExecutionModeLive)
			if err := rt.Manager.ActivateFlowInstance(ctx, runtimepipeline.FlowInstanceActivationRequest{
				ContractBundle: source, Instance: flowidentity.Stored(source, ".", runID, runID, runID, ""), OccurredAt: time.Now().UTC(),
			}); err != nil {
				t.Fatalf("construct and attach memory-identity tree: %v", err)
			}
			cfg, err := rt.Manager.ResolveAgentConfig(runID, "backend", flowPath)
			if err != nil {
				t.Fatalf("constructed attachment omitted backend: %v", err)
			}
			owner := flowidentity.RunScopedFlowInstance{RunID: runID, Route: flowidentity.StoredRoute(flowPath, flowidentity.LogicalInstanceID(flowPath), flowPath)}
			header, found, err := runtimepipeline.NewWorkflowPersistence(selected).LoadWorkflowInstance(ctx, owner)
			if err != nil || !found {
				t.Fatalf("constructed memory owner missing: found=%v err=%v", found, err)
			}
			construction, err := header.ConstructionIdentity(owner)
			if err != nil || construction.ValidateAgentExecution(source, cfg.Identity, cfg.FlowID, cfg.EntityID) != nil {
				t.Fatalf("memory actor lost exact header construction: %+v %v", construction, err)
			}
			route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient("backend"), AgentIdentity: cfg.Identity}
			event := eventtest.RunCreatingRootIngress(uuid.NewString(), "test.static.ready", "", "", []byte(`{}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())
			if err := rt.Manager.FinalizeCommittedAgentReadiness(ctx, event, []events.DeliveryRoute{route}); err != nil {
				t.Fatalf("finalize committed static readiness: %v", err)
			}
			cfg, err = rt.Manager.ResolveAgentConfig(runID, "backend", flowPath)
			if err != nil {
				t.Fatalf("resolve static memory config: %v", err)
			}
			if cfg.FlowPath != flowPath || cfg.FlowID != flowPath {
				t.Fatalf("flow identity = (%q, %q), want (%q, %q)", cfg.FlowPath, cfg.FlowID, flowPath, flowPath)
			}
			if cfg.Memory != (agentmemory.Plan{Enabled: true}) {
				t.Fatalf("Memory = %#v, want authored true", cfg.Memory)
			}
		})
	}
}
