package manager_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deadletters"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

type managerNativeFlowOwner interface {
	managerNativeDeliveryOwner
	agentfixture.Store
	manager.AgentLifecycleOutcomeReader
	manager.AgentLifecycleDiagnosticPersistence
	runtimepkg.RuntimeLogPersistence
	pipeline.WorkflowPersistenceOwner
	pipeline.FlowConstructionPublicationReader
	pipeline.WorkflowEmitFeedbackOwner
	runtimebus.EventStore
	runtimebus.ActiveAgentDescriptorLister
	runtimebus.PreparedPublishEventReader
	runtimebus.RunOriginReader
	replycontext.Store
	runlifecycle.OperationOwner
	deadletters.AcknowledgedRecorder
	decisioncard.Store
	decisioncard.ProposedEffectStore
	decisioncard.HumanTaskStore
	pipeline.DecisionCardDraftExpiry
	pipeline.HumanTaskExpiry
}

type managerNativeFlowModule struct{ semanticview.Source }

func (m managerNativeFlowModule) SemanticSource() semanticview.Source  { return m.Source }
func (managerNativeFlowModule) WorkflowNodes() []pipeline.WorkflowNode { return nil }
func (m managerNativeFlowModule) GuardRegistry() pipeline.GuardRegistry {
	return pipeline.NewContractGuardRegistry(m.Source)
}

func openManagerNativeFlowDelivery(t *testing.T, backend string) *manager.ManagerDeliveryNativeFixture {
	t.Helper()
	fixture := openManagerNativeDelivery(t, backend)
	selected := fixture.Store.(managerNativeFlowOwner)
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, filepath.Join(repo, "tests/tier7-composition/test-agent-emits-to-node"), contracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)
	fact := sourceartifactfixture.RequireArtifact(t, fixture.Context, selected, bundle.SourceArtifact)
	scope, ok := authoractivity.ScopeFromContext(fixture.Context)
	if !ok {
		t.Fatal("native flow fixture lost its process scope")
	}
	runID := uuid.NewString()
	ctx := correlation.WithRuntimeInstanceID(correlation.WithRunID(correlation.WithSourceArtifactFact(fixture.Context, fact), runID), scope.RuntimeInstanceID)
	ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(scope.RuntimeInstanceID, fact.BundleHash()))
	coordinate := agenttopology.SourceCoordinate{BundleHash: fact.BundleHash()}
	plan, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{coordinate}, nil)
	if err != nil {
		t.Fatal(err)
	}
	grant, err := agentfixture.AdmitGeneration(t, ctx, selected, plan, coordinate)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := grant.MarkProbesSettled(ctx, nil); err != nil {
		t.Fatal(err)
	}
	evidence, err := grant.AdmitExecution(ctx)
	if err != nil {
		t.Fatal(err)
	}
	admission, err := managedexecution.New(managedexecution.KindNormalRuntime, evidence.GrantID, evidence.RuntimeGeneration, "", plan.Revision, fact.BundleHash(), nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx = managedexecution.WithAdmission(ctx, admission)
	authority, err := deliverylifecycle.NewExecutionAuthority(fact, admission)
	if err != nil {
		t.Fatal(err)
	}
	if err := selected.ActivateDeliveryAuthority(ctx, authority); err != nil {
		t.Fatal(err)
	}
	fixture.Context, fixture.Authority, fixture.NormalAuthority, fixture.SemanticSource = ctx, authority, authority, source
	storetest.RequireRunningRun(t, ctx, selected, runID, time.Now().UTC())
	process := worklifetime.NewProcess()
	work, err := process.NewRuntime(ctx, worklifetime.RuntimeIdentity{RuntimeInstanceID: evidence.RuntimeInstanceID, BundleHash: fact.BundleHash()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		join, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := work.RetireAndWait(join); err != nil {
			t.Error(err)
			return
		}
		if _, err := process.Join(join); err != nil {
			t.Error(err)
		}
	})
	bus, err := runtimebus.NewEventBusWithOptions(selected, runtimebus.EventBusOptions{
		ContractBundle: source, SourceArtifactFact: fact, RuntimeInstanceID: evidence.RuntimeInstanceID,
		ExecutionPosture: executionposture.Live, ReceiverExecution: eventreceiver.NormalExecution(),
		WorkOwner: work, DeliveryAuthority: authority, PipelineObligations: selected.PipelineObligations(),
		Durable: runtimebus.DurableDependencies{
			Instances: selected, ConstructionPublications: selected, EmitFeedback: selected,
			ReplyContext: selected, RunLifecycle: selected, DeliveryLifecycle: selected,
			ActiveAgents: selected, PreparedEvents: selected, TargetFailureRecorder: selected,
			RunOrigins: selected, StandingRestarts: selected,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	workflow := pipeline.NewPipelineCoordinatorWithOptions(bus, pipeline.PipelineCoordinatorOptions{
		ExecutionPosture: executionposture.Live, ReceiverExecution: eventreceiver.NormalExecution(),
		Module: managerNativeFlowModule{source}, Persistence: pipeline.NewWorkflowPersistence(selected),
		SourceArtifactFact: fact, WorkOwner: work, RunLifecycle: selected, PipelineObligations: selected.PipelineObligations(),
		DeliveryStore: selected, DeadLetters: selected, DecisionCards: selected, ProposedEffects: selected,
		HumanTasks: selected, DecisionCardDraftExpiry: selected, HumanTaskExpiry: selected, DeliveryRuntime: bus,
	})
	if workflow == nil {
		t.Fatal("native flow fixture lacks complete workflow persistence roles")
	}
	topology, err := agenttopology.StaticAdmission(plan.Revision, fact.BundleHash(), agenttopology.LifetimeDurableManaged)
	if err != nil {
		t.Fatal(err)
	}
	owner := flowidentity.Stored(source, ".", runID, runID, runID, "")
	event := func(id string) events.Event {
		return eventtest.ExistingRunRootIngress(id, "task.assigned", "source", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
	}
	fixture.Flow = &manager.ManagerDeliveryNativeFlowFixture{
		Context:  ctx,
		Topology: topology, Plan: plan, Event: event,
		ProjectDiagnostic: runtimepkg.NewRuntimeLogger(selected, executionposture.Live, nil).ProjectLifecycleDiagnostic,
		Options: manager.AgentManagerOptions{
			WorkOwner: work, LifecycleStore: grant, BaseContext: ctx, SemanticSource: source,
			SourceArtifactFact: fact, WorkflowInstances: workflow, DeliveryStore: fixture, LLMBackend: "anthropic",
			PersistenceRoles: manager.PersistenceRoles{LifecycleState: selected, LifecycleCensus: selected, LifecycleOutcome: selected, LifecycleDiagnostics: selected, FlowActivation: bus, FlowTermination: workflow},
		},
		Request: pipeline.FlowInstanceActivationRequest{ContractBundle: source, Instance: owner, TriggerEvent: event(uuid.NewString()), OccurredAt: time.Now().UTC()},
	}
	return fixture
}
