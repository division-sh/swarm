package runtimepersistence

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
)

// The C4 timer family has no agents or providers. Its post-materialization
// assertions still require a real admitted ordinary runtime and live handoff.
func c4LiveTimerRuntime(t *testing.T, ctx context.Context, selected any, sourceRunID string, loaded runforkexecution.LoadedSelectedContractSource, capability startupownership.ProcessCapability) (*runtimepkg.Runtime, *runtimepkg.RuntimeContextManager) {
	t.Helper()
	value, _ := storeTestWorkFixtures.Load(t)
	work := value.(*storeTestWorkFixture)
	var deps runtimepkg.RuntimeDeps
	switch store := selected.(type) {
	case *PostgresStore:
		workflow := pipeline.NewWorkflowPersistence(store)
		deps = runtimepkg.RuntimeDeps{
			EventStore: store, WorkflowPersistence: workflow, RunBundleAvailability: store,
			EventBusDurable: bus.DurableDependencies{
				Instances: workflow, ConstructionPublications: workflow, EmitFeedback: store,
				ReplyContext: store, RunLifecycle: store, DeliveryLifecycle: store, ActiveAgents: store,
				PreparedEvents: store, TargetFailureRecorder: store, RunOrigins: store, StandingRestarts: store,
			},
			EventPayloadAdmissionBinder: store, AuthorActivityRegistrars: []runtimepkg.AuthorActivityCatalogRegistrar{store},
			RunControlStore: store, RunLifecycleCandidates: store, RuntimeLogStore: store, ManagerStore: store,
			ManagerPersistenceRoles: manager.PersistenceRoles{
				LifecycleState: store, LifecycleEffects: store, LifecycleDiagnostics: store, EffectsRecovery: store,
				DeliveryQuiescence: store, EventExistence: store, DirectiveOperations: store, DirectiveTargets: store, StandingRestarts: store,
			},
			SessionRegistry: store, LiveSessionAcquirer: store, SessionResetter: store, ConversationStore: store,
			EffectsStore: store, CompletionStore: store, CompletionHeartbeatStore: store,
			ManagedCapabilitiesStore: store, DeliveryStore: store, PipelineObligations: store.PipelineObligations(),
			GenericScheduleStore: store, TimerObligationReader: store, DecisionCards: store, ProposedEffects: store,
			DecisionCardHumanTasks: store, DecisionCardDraftExpiry: store, HumanTaskExpiry: store, BudgetSpendStore: store,
		}
	case *SQLiteRuntimeStore:
		workflow := pipeline.NewWorkflowPersistence(store)
		deps = runtimepkg.RuntimeDeps{
			EventStore: store, WorkflowPersistence: workflow, RunBundleAvailability: store,
			EventBusDurable: bus.DurableDependencies{
				Instances: workflow, ConstructionPublications: workflow, EmitFeedback: store,
				ReplyContext: store, RunLifecycle: store, DeliveryLifecycle: store, ActiveAgents: store,
				PreparedEvents: store, TargetFailureRecorder: store, RunOrigins: store, StandingRestarts: store,
			},
			EventPayloadAdmissionBinder: store, AuthorActivityRegistrars: []runtimepkg.AuthorActivityCatalogRegistrar{store},
			RunControlStore: store, RunLifecycleCandidates: store, RuntimeLogStore: store, ManagerStore: store,
			ManagerPersistenceRoles: manager.PersistenceRoles{
				LifecycleState: store, LifecycleEffects: store, LifecycleDiagnostics: store, EffectsRecovery: store,
				DeliveryQuiescence: store, EventExistence: store, DirectiveOperations: store, DirectiveTargets: store, StandingRestarts: store,
			},
			SessionRegistry: store, LiveSessionAcquirer: store, SessionResetter: store, ConversationStore: store,
			EffectsStore: store, CompletionStore: store, CompletionHeartbeatStore: store,
			ManagedCapabilitiesStore: store, DeliveryStore: store, PipelineObligations: store.PipelineObligations(),
			GenericScheduleStore: store, TimerObligationReader: store, DecisionCards: store, ProposedEffects: store,
			DecisionCardHumanTasks: store, DecisionCardDraftExpiry: store, HumanTaskExpiry: store, BudgetSpendStore: store,
		}
	default:
		t.Fatalf("C4 timer runtime requires its native store, got %T", selected)
	}
	plan, found, err := capability.CurrentSourceSet(ctx)
	if err != nil || !found || len(plan.Sources) != 1 || plan.Sources[0].BundleHash != loaded.SourceArtifactFact.BundleHash() || len(plan.Agents) != 0 {
		t.Fatalf("C4 timer runtime lost its exact agent-free source set: %+v found=%t err=%v", plan, found, err)
	}
	coordinates, err := capability.Evidence()
	if err != nil {
		t.Fatal(err)
	}
	ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(coordinates.RuntimeInstanceID, loaded.SourceArtifactFact.BundleHash()))
	paused, err := deps.RunControlStore.PauseRunControlOutcome(ctx, runcontrol.TransitionRequest{
		RunID: sourceRunID, Reason: "C4 retained source", ControlledBy: "native timer fixture", Now: time.Now().UTC(),
	})
	if err != nil || !paused.Acknowledged || paused.State.Status != "paused" {
		t.Fatalf("park exact C4 source before runtime recovery: %+v err=%v", paused, err)
	}
	deps.Config = &config.Config{Runtime: config.RuntimeConfig{RecoveryOnStartup: true}, LLM: config.LLMConfig{Backend: "anthropic"}}
	deps.Options = runtimepkg.RuntimeOptions{ExecutionPosture: executionposture.Live, RuntimeInstanceID: coordinates.RuntimeInstanceID,
		SourceArtifactFact: loaded.SourceArtifactFact, ProcessWorkOwner: work.process, WorkflowModule: loaded.Module}
	rt, err := runtimepkg.NewRuntime(ctx, deps)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := rt.Shutdown(); err != nil {
			t.Error(err)
		}
	})
	grant, err := capability.IssueGenerationGrant(ctx, startupownership.GrantRequest{
		BundleHash: loaded.SourceArtifactFact.BundleHash(), RuntimeInstanceID: coordinates.RuntimeInstanceID,
		RuntimeGeneration: 2, SourceSetRevision: plan.Revision,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.InstallStartupGrant(grant); err != nil {
		t.Fatal(err)
	}
	if err := rt.Start(ctx); err != nil {
		t.Fatal(err)
	}
	contexts, err := runtimepkg.NewRuntimeContextManager(selected.(runforkexecution.SelectedContractForkLifecycle), runtimepkg.BundleContext{
		SourceArtifactFact: loaded.SourceArtifactFact, Source: loaded.Source, Runtime: rt, WorkOwner: rt.WorkOccurrence(),
	})
	if err != nil {
		t.Fatal(err)
	}
	// Installing a context is not a stand-in for admission: take and release a
	// real registered use before it is supplied to the selected handoff owner.
	use, lookup, err := contexts.AcquireBundleHash(ctx, loaded.SourceArtifactFact.BundleHash())
	if err != nil || !lookup.Loaded() || use == nil {
		t.Fatalf("acquire exact ordinary C4 timer runtime: loaded=%t err=%v", lookup.Loaded(), err)
	}
	if err := use.Done(); err != nil {
		t.Fatal(err)
	}
	return rt, contexts
}
