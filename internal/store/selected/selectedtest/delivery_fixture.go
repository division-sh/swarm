package selectedtest

import (
	"context"
	"errors"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil/flowactivationfixture"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
	"path/filepath"
	"testing"
	"time"
)

type SelectedDeliveryFixtureStore interface {
	storetest.DeliveryPublicationFixtureStore
	sourceartifactfixture.Writer
	startupownership.Store
	runforkexecution.SourceArtifactSelectedContractSourceStore
}

type SelectedDeliveryFixture struct {
	Authority         deliverylifecycle.ExecutionAuthority
	NormalAuthority   deliverylifecycle.ExecutionAuthority
	SelectedAdmission managedexecution.Admission
	SelectedRoute     events.DeliveryRoute
	SelectedEvent     events.Event
	SemanticSource    semanticview.Source
	Context           context.Context
}

// OpenSelectedDeliveryExecution reuses the existing native manager-delivery
// claim gate and joins the real selected owner before capability/store release.
func OpenSelectedDeliveryExecution(t *testing.T, ctx context.Context, owner SelectedDeliveryFixtureStore) *SelectedDeliveryFixture {
	t.Helper()
	fixture := &SelectedDeliveryFixture{}
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, filepath.Join(repo, "tests/tier7-composition/test-agent-emits-to-node"), contracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	source := sourceartifactfixture.RequireArtifact(t, ctx, owner, bundle.SourceArtifact)
	ctx = correlation.WithSourceArtifactFact(ctx, source)
	runtimeID := uuid.NewString()
	ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(runtimeID, source.BundleHash()))
	process := worklifetime.NewProcess()
	ctx = worklifetime.WithProcess(ctx, process)
	capability, err := owner.AcquireProcessCapability(ctx, startupownership.AcquireRequest{OwnerID: "manager-selected-delivery-proof", BootID: uuid.NewString(), RuntimeInstanceID: runtimeID})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := capability.Release(context.Background()); err != nil {
			t.Error(err)
		}
	})
	t.Cleanup(func() {
		join, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := process.Join(join); err != nil {
			t.Error(err)
		}
	})
	ready := make(chan runforkexecution.ManagerDeliveryExecutionClaim, 1)
	release := make(chan struct{})
	fork, err := ManagerDeliveryExecution(owner, ready, release)
	if err != nil {
		t.Fatal(err)
	}
	if err := fork.BindSelectedProcess(ctx, process, capability); err != nil {
		t.Fatal(err)
	}
	if _, err := fork.RecoverSelectedForkContexts(ctx, effects.NewRecoveryRequest(time.Now().UTC(), executionposture.Live), runforkexecution.SelectedForkRecoveryEnvironment{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := fork.RetireSelectedContexts(context.Background()); err != nil {
			t.Error(err)
		}
	})
	input := selectedDeliverySourceEvent(t)
	payload, err := runtimepkg.NewRuntimePayloadAdmitter(nil, semanticview.Wrap(bundle), source)(ctx, input, ".")
	if err != nil {
		t.Fatal(err)
	}
	input, err = events.ApplyPayloadAdmission(input, payload)
	if err != nil {
		t.Fatal(err)
	}
	normal, err := deliverylifecycle.NewNormalExecutionAuthority(source, "manager-selected-source-proof", 1)
	if err != nil {
		t.Fatal(err)
	}
	identity := agentidentitytest.RootDeclaredForRun(t, input.RunID(), "test-agent", ".")
	root := selectedDeliverySourceRoot(t, ctx, semanticview.Wrap(bundle), source, input)
	storetest.CommitNativeDeliveryPublication(t, ctx, owner, storetest.AdmitNativeDeliveryEvent(t, input), []events.DeliveryRoute{{Recipient: events.MustAgentDeliveryRecipient(identity.AgentID()), AgentIdentity: identity}}, normal, &root)
	finished := make(chan error, 1)
	loader := runforkexecution.SourceArtifactSelectedContractSourceLoader{RepoRoot: repo, PlatformSpecPath: contracts.DefaultPlatformSpecFile(repo), Store: owner}
	go func() {
		_, err := runforkexecution.ExecuteSelectedContractRunFork(ctx, runforkexecution.SelectedContractExecutionRequest{
			Owner:       fork,
			SourceRunID: input.RunID(), At: input.ID(), ExpectedBundleHash: source.BundleHash(),
			SourceLoader: loader, ContractSelection: runfork.RunForkContractSelection{Mode: "selected_contracts"},
			AgentRuntime: runforkexecution.SelectedContractAgentRuntimeOptions{ProcessCapability: capability, ExecutionPosture: executionposture.Live, Config: &config.Config{LLM: config.LLMConfig{Backend: selection.BackendAnthropic}}},
		})
		finished <- err
	}()
	t.Cleanup(func() {
		close(release)
		select {
		case err := <-finished:
			if !errors.Is(err, runforkexecution.ErrManagerDeliveryClaimGateReleased) || err.Error() != runforkexecution.ErrManagerDeliveryClaimGateReleased.Error() {
				t.Errorf("selected proof did not retire through its exact native failure path: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("selected manager fixture did not join its execution owner")
		}
	})
	var claimed runforkexecution.ManagerDeliveryExecutionClaim
	select {
	case claimed = <-ready:
	case err := <-finished:
		// Leave cleanup one terminal result rather than hanging after a setup failure.
		finished <- err
		t.Fatalf("native selected execution never reached genuine claim: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("native selected execution claim did not arrive")
	}
	admission, err := managedexecution.New(managedexecution.KindSelectedContractFork, claimed.Issued.ExecutionID, claimed.Issued.Generation, claimed.Issued.ForkRunID, claimed.Issued.ActorCensusFingerprint, source.BundleHash(), nil)
	if err != nil {
		t.Fatal(err)
	}
	authority, err := deliverylifecycle.NewExecutionAuthority(source, admission)
	if err != nil {
		t.Fatal(err)
	}
	fixture.Authority, fixture.SelectedAdmission = authority, admission
	if len(claimed.Declarations.Agents) != 1 {
		t.Fatal("selected manager fixture requires its one real declared agent")
	}
	identity, err = claimed.Declarations.Agents[0].Identity.Live(claimed.Issued.ForkRunID)
	if err != nil {
		t.Fatal(err)
	}
	fixture.SelectedRoute = events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(identity.AgentID()), AgentIdentity: identity}
	fixture.SemanticSource = semanticview.Wrap(bundle)
	fixture.NormalAuthority, err = deliverylifecycle.NewNormalExecutionAuthority(source, "manager-delivery-test", 1)
	if err != nil {
		t.Fatal(err)
	}
	fixture.Context = managedexecution.WithAdmission(ctx, admission)
	fixture.SelectedEvent = selectedDeliveryIssuedEvent(t, claimed.Issued.ForkRunID)
	payload, err = runtimepkg.NewRuntimePayloadAdmitter(nil, semanticview.Wrap(bundle), source)(fixture.Context, fixture.SelectedEvent, ".")
	if err != nil {
		t.Fatal(err)
	}
	fixture.SelectedEvent, err = events.ApplyPayloadAdmission(fixture.SelectedEvent, payload)
	if err != nil {
		t.Fatal(err)
	}
	return fixture
}

func selectedDeliveryRootFacts(t *testing.T, runID string) events.EventFacts {
	t.Helper()
	source, err := events.NewRootRoutingSource(runID)
	if err != nil {
		t.Fatal(err)
	}
	return events.EventFacts{
		ID: uuid.NewString(), Type: "task.assigned",
		Producer: events.ProducerClaim{Type: events.EventProducerExternal, ID: "manager-proof"},
		Payload:  []byte(`{}`), RoutingSource: source,
		CreatedAt: time.Now().UTC(), ExecutionMode: executionposture.Live.RootMode(),
	}
}

func selectedDeliverySourceEvent(t *testing.T) events.Event {
	t.Helper()
	run := uuid.NewString()
	event, err := events.NewRunCreatingRootIngressEvent(events.RunCreatingRootIngressEventInput{RunID: run, Facts: selectedDeliveryRootFacts(t, run)})
	if err != nil {
		t.Fatal(err)
	}
	return event
}

func selectedDeliveryIssuedEvent(t *testing.T, run string) events.Event {
	t.Helper()
	event, err := events.NewExistingRunRootIngressEvent(events.ExistingRunRootIngressEventInput{RunID: run, Facts: selectedDeliveryRootFacts(t, run)})
	if err != nil {
		t.Fatal(err)
	}
	return event
}

func selectedDeliverySourceRoot(t *testing.T, ctx context.Context, source semanticview.Source, fact correlation.SourceArtifactFact, event events.Event) bus.FlowInstanceActivationCommand {
	t.Helper()
	flow := semanticview.RootExecutionFlowID(source)
	identity := flowidentity.Stored(source, flow, event.RunID(), event.RunID(), event.RunID(), "")
	constructor, err := pipeline.CompileFlowConstructor(source, flow, "")
	if err != nil {
		t.Fatal(err)
	}
	fields, err := constructor.InitialFields(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	stage, err := constructor.InitialStoredStage()
	if err != nil {
		t.Fatal(err)
	}
	contract, _ := entityruntime.ResolveForFlow(source, flow)
	schema, ok := source.FlowSchemaByID(flow)
	if !ok {
		t.Fatal("selected source lost its root flow schema")
	}
	readiness := pipeline.DynamicFlowRuntimeReadinessPlan{Identity: identity, RunID: event.RunID(), BundleHash: fact.BundleHash(), WorkflowVersion: source.WorkflowVersion(), ExecutionMode: event.ExecutionMode()}
	instance := pipeline.WorkflowInstance{InstanceID: identity.InstanceID, StorageRef: identity.InstancePath, EntityID: identity.EntityID, EntityType: contract.EntityType, WorkflowName: identity.TemplateID, WorkflowVersion: source.WorkflowVersion(), Mode: schema.EffectiveMode(), CurrentState: stage.ID(), StageDefined: !stage.IsStatelessPosture(), Fields: fields, RuntimeReadiness: &readiness, CreatedAt: event.CreatedAt(), EnteredStageAt: event.CreatedAt()}
	ctx = effects.WithExecutionMode(correlation.WithRunID(ctx, event.RunID()), event.ExecutionMode())
	command, err := flowactivationfixture.Command(ctx, instance, pipeline.WorkflowLifecycleMutationPlan{}, event.CreatedAt())
	if err != nil {
		t.Fatal(err)
	}
	return command
}
