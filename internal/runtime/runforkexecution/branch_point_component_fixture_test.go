package runforkexecution

import (
	"context"
	"encoding/json"
	"errors"
	startupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/deliverycontinuation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/gateruntime"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

// #642/6208165755: lifecycle component assembly, not public execution admission.
// All preparation, materialization, attachment and retirement are real owners.
type q1BranchPointAttachment struct {
	prepared     *PreparedSelectedFork
	materialized runfork.RunForkMaterialization
	container    selectedContractForkLocalRuntimeContainer
	closed       bool
}

func q1StageBranchPointComponent(t *testing.T, ctx context.Context, request SelectedContractExecutionRequest) *q1BranchPointAttachment {
	t.Helper()
	prepared, err := request.Owner.Prepare(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := prepared.Close(); err != nil {
			t.Error(err)
		}
	})
	materialized, acknowledged, err := request.Owner.materializePrepared(prepared.operation.PreparationContext(), prepared)
	if err != nil || !acknowledged {
		t.Fatalf("original native child: %+v err=%v", materialized, err)
	}
	return q1AttachBranchPointComponent(t, prepared, materialized)
}

// This exercises selected-phase work only. It neither activates the child nor
// creates the public handoff ACK; those assertions use the live fixture.
func q1ExecuteSelectedPhaseComponent(t *testing.T, ctx context.Context, request SelectedContractExecutionRequest) (out SelectedContractExecutionResult, finalErr error) {
	t.Helper()
	if !request.AgentRuntime.ExecutionPosture.Valid() {
		request.AgentRuntime.ExecutionPosture = executionposture.Live
	}
	ctx, err := selectedFixtureEntryContext(ctx, request.Owner)
	if err != nil {
		return SelectedContractExecutionResult{}, err
	}
	prepared, err := request.Owner.Prepare(ctx, request)
	if err != nil {
		return SelectedContractExecutionResult{}, err
	}
	if err := admitSelectedDeploymentRevisionFrontier(prepared.plan, prepared.frontier); err != nil {
		return SelectedContractExecutionResult{}, errors.Join(err, prepared.Close())
	}
	materialized, acknowledged, err := request.Owner.materializePrepared(prepared.operation.PreparationContext(), prepared)
	if err != nil || !acknowledged {
		return SelectedContractExecutionResult{Materialization: materialized}, errors.Join(err, prepared.Close())
	}
	attachment := q1AttachBranchPointComponent(t, prepared, materialized)
	defer func() { finalErr = errors.Join(finalErr, attachment.closeResult()) }()
	proof := attachment.container.Proof()
	out = SelectedContractExecutionResult{
		Owner: runfork.RunForkSelectedContractExecutionOwner, Materialization: materialized,
		SelectedContractExecutionAdmission: &attachment.container.req.Admission,
		AgentRuntimeMaterialization:        &attachment.container.req.AgentRuntime.Proof,
		ForkLocalRuntimeContainer:          &proof,
	}
	c := attachment.container
	ctx = c.attachment.ctx
	if err := c.attachment.runtime.startExecution(ctx, c.attachment.pipeline, c.diagnostics, false); err != nil {
		return out, err
	}
	continuations, err := deliverycontinuation.NewSelected(c.ports.busDurable.DeliveryLifecycle, c.attachment.deliveryAuthority, prepared.operation.selected, c.attachment.runtime.bus, func(_ context.Context, cause error) { c.attachment.cancel(cause) })
	if err != nil {
		return out, err
	}
	if err := c.attachment.runtime.bus.SetDeliveryContinuationOwner(continuations); err != nil {
		return out, err
	}
	if err := continuations.Start(ctx); err != nil {
		return out, err
	}
	defer func() { finalErr = errors.Join(finalErr, continuations.Retire(context.WithoutCancel(ctx))) }()
	if err := c.attachment.scheduler.ReleaseStartup(ctx); err != nil {
		return out, err
	}
	inputs, err := c.commitSelectedInputs()
	if err != nil {
		return out, err
	}
	defer func() {
		for _, input := range inputs {
			finalErr = errors.Join(finalErr, c.attachment.runtime.bus.AbandonPreparedPublish(context.WithoutCancel(input.ctx), input.prepared))
		}
	}()
	for _, input := range inputs {
		if err := c.dispatchCommittedInput(input); err != nil {
			return out, err
		}
		out.ExecutedEventCount++
		out.ForkEvents = append(out.ForkEvents, SelectedContractExecutionForkEvent{
			SourceEventID: input.sourceEventID, ForkEventID: input.prepared.Event.ID(), EventName: string(input.prepared.Event.Type()),
		})
	}
	if err := continuations.Synchronize(ctx); err != nil {
		return out, err
	}
	wait, cancel := context.WithTimeout(ctx, selectedContractAgentRuntimeDefaultQuiescenceTimeout)
	defer cancel()
	return out, c.attachment.runtime.WaitForQuiescence(wait, c.attachment.runtime.bus)
}

func q1AttachBranchPointComponent(t *testing.T, prepared *PreparedSelectedFork, materialized runfork.RunForkMaterialization) *q1BranchPointAttachment {
	t.Helper()
	p := prepared
	ctx := p.operation.Context()
	request, err := p.MaterializationRequest()
	if err != nil {
		t.Fatal(err)
	}
	observed, err := p.owner.ports.fork.RequireMaterializedRunForkForSelectedContractExecution(ctx, materialized.ForkRunID, request)
	if err != nil || observed.ForkRunID != materialized.ForkRunID || !observed.ForkPoint.SameIdentity(materialized.ForkPoint) {
		t.Fatalf("component lost its original child/cut: %+v err=%v", observed, err)
	}
	agents, err := p.agentRuntime.bindRun(materialized.ForkRunID, materialized.AgentTopologies)
	if err != nil {
		t.Fatal(err)
	}
	admission, err := BuildSelectedContractExecutionAdmission(ctx, SelectedContractExecutionAdmissionRequest{
		ForkRunID: materialized.ForkRunID, SourceRunID: p.plan.SourceRunID,
		SourceArtifactFact: p.loadedSource.SourceArtifactFact, BindingReader: p.owner.ports.fork,
		LoadedSource: p.loadedSource, FrontierAdmission: p.frontier, RouteAdmission: p.routeAdmission,
		RouteTopology: p.routeTopology, ExecutionModel: p.model, DeferredWorkAdmission: p.deferredWorkAdmission,
	})
	if err != nil {
		t.Fatal("native component admission: ", err)
	}
	container, err := buildSelectedContractForkLocalRuntimeContainer(ctx, publishSelectedContractForkEventsRequest{
		OriginalLoopCarriage: p.originalLoopCarriage, Prepared: p, Operation: p.operation, Owner: p.owner,
		Admission: admission, LoadedSource: p.loadedSource, RecipientPlanning: *p.model.RecipientPlanning,
		AgentRuntime: agents, SourceRunID: p.plan.SourceRunID, ForkRunID: materialized.ForkRunID,
		DataPins: materialized.DataPins, ForkEventID: p.plan.ForkPoint.EventID,
		SourceEvents:          selectedContractExecutionFrontierEventIDs(p.frontier.FrontierEvents),
		ExecutionOwner:        runfork.RunForkSelectedContractExecutionOwner,
		DeferredWorkAdmission: p.deferredWorkAdmission, SourcePlan: p.plan,
	})
	if err != nil {
		t.Fatal("native component container: ", err)
	}
	if err := container.PrepareAttachment(p.operation.Context()); err != nil {
		t.Fatal("native component attachment: ", err)
	}
	fixture := &q1BranchPointAttachment{prepared: p, materialized: materialized, container: container}
	t.Cleanup(func() { fixture.close(t) })
	return fixture
}

func (f *q1BranchPointAttachment) activate() (runfork.RunForkActivation, error) {
	p := f.prepared
	return p.owner.ports.fork.ActivateRunForkForSelectedContractExecution(effects.WithAuthority(p.operation.Context(), f.container.authority), runfork.RunForkSelectedContractExecutionActivateRequest{
		InheritedSchedules: p.readiness, ForkOperation: p.forkOperation, DataPins: f.materialized.DataPins,
		ExecutionSource: p.loadedSource.Source, ForkRunID: f.materialized.ForkRunID,
		AllowedSourceEventIDs: selectedContractExecutionFrontierEventIDs(p.frontier.FrontierEvents),
		FrontierAdmission:     p.frontier, RouteTopology: p.routeTopology, RecipientPlanning: *p.model.RecipientPlanning,
	})
}

func (f *q1BranchPointAttachment) serveSelectedControls(t *testing.T) {
	t.Helper()
	if _, err := f.prepared.owner.ports.busDurable.RunLifecycle.TransitionActiveRun(f.container.attachment.ctx, runlifecycle.ActiveTransitionRequest{
		RunID: f.materialized.ForkRunID, State: runlifecycle.StateRunning,
	}); err != nil {
		t.Fatal(err)
	}
	if err := f.prepared.owner.retainPrepared(f.prepared); err != nil {
		t.Fatal(err)
	}
	if err := f.container.Serve(); err != nil {
		t.Fatal(err)
	}
	waitSelectedRuntimeStartup(t, f.prepared.operation.Context(), f.prepared.owner, f.materialized.ForkRunID)
}

func (f *q1BranchPointAttachment) close(t *testing.T) {
	t.Helper()
	if err := f.closeResult(); err != nil {
		t.Error(err)
	}
}

func (f *q1BranchPointAttachment) closeResult() error {
	if f.closed {
		return nil
	}
	f.prepared.bindMu.Lock()
	closing := f.prepared.closed
	f.prepared.bindMu.Unlock()
	if closing {
		err := f.prepared.Close()
		f.closed = err == nil
		return err
	}
	ctx := f.prepared.operation.Context()
	if runtime := f.container.attachment.runtime; runtime != nil {
		if err := runtime.stopExecutionWork(); err != nil {
			return err
		}
	}
	if err := f.container.Quiesce(ctx); err != nil {
		return err
	}
	if err := f.container.Close(ctx); err != nil {
		return err
	}
	if err := f.prepared.Close(); err != nil {
		return err
	}
	f.closed = true
	return nil
}

func q1BranchPointSourceConstructor(t *testing.T, ctx context.Context, owner SelectedContractExecutionOwner, loaded LoadedSelectedContractSource) (*manager.AgentManager, *pipeline.PipelineCoordinator) {
	t.Helper()
	p := owner.ports
	work, found := worklifetime.OccurrenceFromContext(ctx)
	if !found {
		t.Fatal("component constructor requires its original work owner")
	}
	authority, err := deliverylifecycle.NewNormalExecutionAuthority(loaded.SourceArtifactFact, runForkTestRuntimeInstanceID, 1)
	if err != nil {
		t.Fatal(err)
	}
	sourceBus, err := bus.NewEventBusWithOptions(p.events, bus.EventBusOptions{
		ExecutionPosture: executionposture.Live, WorkOwner: work, PipelineObligations: p.pipelineObligations,
		ContractBundle: loaded.Source, SourceArtifactFact: loaded.SourceArtifactFact, RuntimeInstanceID: runForkTestRuntimeInstanceID,
		DeliveryAuthority: authority, ReceiverExecution: eventreceiver.NormalExecution(), Durable: p.busDurable,
	})
	if err != nil {
		t.Fatal(err)
	}
	nodes, err := pipeline.LoadWorkflowNodes(loaded.Source)
	if err != nil {
		t.Fatal(err)
	}
	coordinator := pipeline.NewPipelineCoordinatorWithOptions(sourceBus, pipeline.PipelineCoordinatorOptions{
		Module:      selectedContractWorkflowModule{source: loaded.Source, nodes: nodes, guardRegistry: pipeline.NewContractGuardRegistry(loaded.Source)},
		Persistence: p.workflow, SourceArtifactFact: loaded.SourceArtifactFact, ExecutionPosture: executionposture.Live,
		ReceiverExecution: eventreceiver.NormalExecution(), RunLifecycle: p.busDurable.RunLifecycle, PipelineObligations: p.pipelineObligations,
		DeliveryStore: p.busDurable.DeliveryLifecycle, DeadLetters: p.busDurable.TargetFailureRecorder,
		DecisionCards: p.decisionCards, ProposedEffects: p.proposedEffects, HumanTasks: p.humanTasks,
		DecisionCardDraftExpiry: p.decisionCardDraftExpiry, HumanTaskExpiry: p.humanTaskExpiry, DeliveryRuntime: sourceBus, WorkOwner: work,
	})
	planner := manager.NewAgentManagerWithOptions(sourceBus, nil, manager.AgentManagerOptions{
		ExecutionPosture: executionposture.Live, BaseContext: ctx, SourceArtifactFact: loaded.SourceArtifactFact,
		SemanticSource: loaded.Source, WorkflowInstances: coordinator, WorkOwner: work,
		DeliveryStore: p.busDurable.DeliveryLifecycle, ReceiverExecution: eventreceiver.NormalExecution(),
	}, p.manager)
	t.Cleanup(func() {
		if err := planner.Shutdown(); err != nil {
			t.Error(err)
		}
	})
	return planner, coordinator
}

func q1StateOnlyGateComponentSource(t *testing.T, owner SelectedContractExecutionOwner) (context.Context, SelectedContractExecutionRequest, func()) {
	return q1GateComponentSource(t, owner, true, false, false)
}

func q1GateComponentSource(t *testing.T, owner SelectedContractExecutionOwner, decided, pending, live bool) (context.Context, SelectedContractExecutionRequest, func()) {
	t.Helper()
	selected := owner.ports.fork
	ctx := runForkTestContext(t)
	repo := runForkExecutionRepoRoot(t)
	bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, canonicalrouting.CopyConstructedGateForkControl(t, true), contracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	fact, err := correlation.NewSourceArtifactFact(bundle.SourceArtifact.BundleHash())
	if err != nil {
		t.Fatal(err)
	}
	runID, eventID := uuid.NewString(), uuid.NewString()
	ctx = correlation.WithRunID(correlation.WithSourceArtifactFact(ctx, fact), runID)
	ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(runForkTestRuntimeInstanceID, fact.BundleHash()))
	ctx, err = eventreceiver.NormalExecution().Bind(ctx, executionmode.Live)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	ctx = originalSelectedFixtureContext(t, ctx, any(selected).(startupownership.Store))
	if live {
		artifacts := selected.(interface {
			EnsureSourceArtifact(context.Context, *sourceartifact.AdmittedSourceArtifact) (sourceartifact.EnsureResult, error)
		})
		if _, err := artifacts.EnsureSourceArtifact(ctx, bundle.SourceArtifact); err != nil {
			t.Fatal(err)
		}
		if _, err := selected.(runlifecycle.OperationOwner).CreateRun(ctx, runlifecycle.CreateRequest{
			RunID: runID, Origin: runlifecycle.DeploymentRunOrigin(), Source: fact, StartedAt: at,
			LiveScope: runlifecycle.LiveRunScope{FlowPath: "."},
		}); err != nil {
			t.Fatal(err)
		}
	} else {
		storetest.RequireRun(t, ctx, selected.(storetest.RunFixtureStore), storetest.RunFixture{
			RunID: runID, Origin: storetest.ScenarioSetupOrigin(), Artifact: bundle.SourceArtifact,
			BundleHash: fact.BundleHash(), StartedAt: at,
		})
	}
	loader := SourceArtifactSelectedContractSourceLoader{RepoRoot: repo, Store: selected.(SourceArtifactSelectedContractSourceStore)}
	loaded, err := loader.LoadRunForkSelectedContractSourceForRequest(ctx, SelectedContractSourceLoadRequest{SourceRunID: runID, SourceArtifactFact: fact, Selection: testContractSelection()})
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Cleanup != nil {
		t.Cleanup(func() {
			if err := loaded.Cleanup(); err != nil {
				t.Error(err)
			}
		})
	}
	planner, coordinator := q1BranchPointSourceConstructor(t, ctx, owner, loaded)
	identity := flowidentity.Stored(loaded.Source, ".", runID, runID, runID, "")
	plan, err := planner.PrepareFlowInstanceActivation(ctx, pipeline.FlowInstanceActivationRequest{ContractBundle: loaded.Source, Instance: identity, OccurredAt: at})
	if err != nil {
		t.Fatal(err)
	}
	if result, err := selected.(bus.FlowInstanceActivationCommitOwner).CommitFlowInstanceActivation(ctx, bus.FlowInstanceActivationCommand{Plan: plan}); err != nil || !result.Created || !result.Acknowledged {
		t.Fatalf("original gate construction: %+v err=%v", result, err)
	}
	record, err := plan.PersistenceRecord()
	if err != nil {
		t.Fatal(err)
	}
	state := record.State
	state.Transition, state.ExpectedState, state.ExpectedRevision = pipeline.WorkflowEngineStateTransitionUpdateStateAndCompanion, "pending", 1
	state.CurrentState, state.EntityType, state.Name, state.Fields = "pending", "default", "At R", json.RawMessage(`{"name":"At R"}`)
	if _, err := selected.(pipeline.WorkflowEngineMutationOwner).CommitWorkflowEngineMutation(ctx, pipeline.WorkflowEngineMutationCommand{State: state}); err != nil {
		t.Fatal(err)
	}
	if !decided {
		envelope := events.EventEnvelope{}
		if pending {
			envelope = events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(envelope, runID), runID)
		}
		event := eventtest.ExistingRunRootIngress(eventID, "item.received", "operator", "", []byte(`{}`), 0, runID, envelope, time.Now().UTC())
		if pending {
			storetest.CommitSemanticEventWithRoutes(t, ctx, selected, event, []events.DeliveryRoute{selectedConstructedRootNodeRoute(loaded.Source, runID, "frontier")}, pipelineobligation.ScopeSubscribed)
		} else {
			storetest.CommitSemanticEvent(t, ctx, selected, event)
		}
		storetest.CaptureRunForkSnapshot(t, ctx, selected, runID)
		state.ExpectedRevision = 2
		state.Accumulator = json.RawMessage(`{"source_advanced":{"count":1}}`)
		return ctx, q1GateComponentRequest(owner, loader, runID, eventID, loaded.SourceArtifactFact.BundleHash()), func() {
			if _, err := selected.(pipeline.WorkflowEngineMutationOwner).CommitWorkflowEngineMutation(ctx, pipeline.WorkflowEngineMutationCommand{State: state}); err != nil {
				t.Fatal(err)
			}
		}
	}
	instance, found, err := coordinator.Load(ctx, flowidentity.RunScopedFlowInstance{RunID: runID, Route: identity.Route()})
	if err != nil || !found {
		t.Fatalf("original constructed gate: found=%t err=%v", found, err)
	}
	carrier, err := engine.StateCarrierFromPersisted(instance.Fields, instance.Bookkeeping, instance.Gates, instance.StateBuckets)
	if err != nil {
		t.Fatal(err)
	}
	gates, err := gateruntime.List(carrier.StateBuckets)
	if err != nil || len(gates) != 1 {
		t.Fatalf("original native gate evidence: %+v err=%v", gates, err)
	}
	card, err := owner.ports.decisionCards.GetDecisionCard(ctx, gates[0].CardID)
	if err != nil {
		t.Fatal(err)
	}
	decidedAt := at.Add(time.Second)
	if err := coordinator.CommitDecision(ctx, card, eventID, decidedAt); err != nil {
		t.Fatal(err)
	}
	if _, err := storetest.DecisionCardDomain(selected).ApplyDecisionForTest(ctx, decisioncard.DecideRequest{CardID: card.CardID, Verdict: "approve", Fields: semanticvalue.EmptyObject(), PrincipalID: "operator", ObservedContentHash: card.CardContentHash, DecisionEventID: eventID, Now: decidedAt}); err != nil {
		t.Fatal(err)
	}
	control, err := card.Anchor.ControlRoutingSource()
	if err != nil {
		t.Fatal(err)
	}
	event := eventtest.RuntimeControlWithRoutingSource(eventID, "mailbox.card_decided", "platform", "", []byte(`{"card_id":"`+card.CardID+`"}`), 0, runID, "", events.EnvelopeForEntityID(events.EventEnvelope{}, runID), control, decidedAt)
	storetest.CommitSemanticEvent(t, ctx, selected, event)
	storetest.CaptureRunForkSnapshot(t, ctx, selected, runID)
	return ctx, q1GateComponentRequest(owner, loader, runID, eventID, loaded.SourceArtifactFact.BundleHash()), func() {
		pass, _, outcome, err := coordinator.Intercept(ctx, event)
		if err != nil || pass || !outcome.Committed {
			t.Fatalf("original post-cut gate mutation: pass=%t outcome=%+v err=%v", pass, outcome, err)
		}
	}
}

func q1GateComponentRequest(owner SelectedContractExecutionOwner, loader SourceArtifactSelectedContractSourceLoader, runID, eventID, bundleHash string) SelectedContractExecutionRequest {
	selection := testContractSelection()
	operation := runfork.ForkOperationRequest{OperationID: uuid.NewString(), Actor: "bearer:branch-point-proof", IdempotencyKey: uuid.NewString(), TransportHash: "sha256:branch-point-proof",
		SourceRunID: runID, ForkEventID: eventID, TargetBundleHash: bundleHash, ContractSelection: selection, AllowSourceFreeze: true}
	return SelectedContractExecutionRequest{Owner: owner, SourceRunID: runID, At: eventID, ForkOperation: &operation, AllowSourceFreeze: true,
		SourceLoader: loader, ContractSelection: selection, AgentRuntime: SelectedContractAgentRuntimeOptions{ProcessCapability: owner.ports.contexts.capability, ExecutionPosture: executionposture.Live}}
}

func q1DeploymentComponentSource(t *testing.T, owner SelectedContractExecutionOwner) (context.Context, SelectedContractExecutionRequest, func()) {
	t.Helper()
	selected := owner.ports.fork
	repo, root := runForkExecutionRepoRoot(t), t.TempDir()
	writeSelectedContractFixtureFile(t, root+"/schema.yaml", "name: branch-revision\nstages:\n  pending: {}\n  later: {final: true}\npins:\n  outputs:\n    - records.ready\n")
	writeSelectedContractFixtureFile(t, root+"/events.yaml", "records.ready:\n  body: text\n")
	bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, root, contracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := contracts.BuildDurableDataCatalog(bundle)
	if err != nil {
		t.Fatal(err)
	}
	data := selected.(interface {
		EnsureSourceArtifactWithData(context.Context, *sourceartifact.AdmittedSourceArtifact, durabledata.Catalog) (sourceartifact.EnsureResult, error)
		ExecuteDataSourceOperation(context.Context, durabledata.SourceCommand) (durabledata.SourceOperationResult, error)
	})
	ctx := correlation.WithSourceArtifactFact(runForkTestContext(t), mustComponentSourceFact(t, bundle.SourceArtifact.BundleHash()))
	ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(runForkTestRuntimeInstanceID, bundle.SourceArtifact.BundleHash()))
	ctx, err = eventreceiver.NormalExecution().Bind(ctx, executionmode.Live)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := data.EnsureSourceArtifactWithData(ctx, bundle.SourceArtifact, catalog); err != nil {
		t.Fatal(err)
	}
	bootstrap := uuid.NewString()
	ctx = originalSelectedFixtureContext(t, ctx, any(selected).(startupownership.Store))
	storetest.RequireRun(t, ctx, selected.(storetest.RunFixtureStore), storetest.RunFixture{RunID: bootstrap, Origin: storetest.ScenarioSetupOrigin(), Artifact: bundle.SourceArtifact, BundleHash: catalog.BundleHash})
	loader := SourceArtifactSelectedContractSourceLoader{RepoRoot: repo, Store: selected.(SourceArtifactSelectedContractSourceStore)}
	loaded, err := loader.LoadRunForkSelectedContractSourceForRequest(ctx, SelectedContractSourceLoadRequest{SourceRunID: bootstrap, Selection: testContractSelection()})
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Cleanup != nil {
		t.Cleanup(func() {
			if err := loaded.Cleanup(); err != nil {
				t.Error(err)
			}
		})
	}
	planner, _ := q1BranchPointSourceConstructor(t, ctx, owner, loaded)
	ref, err := durabledata.ParseDeclarationRef(".", "records.ready")
	if err != nil {
		t.Fatal(err)
	}
	imported, err := data.ExecuteDataSourceOperation(ctx, durabledata.SourceCommand{Operation: "import", SourceInvocationID: uuid.NewString(), Actor: "operator", BundleHash: catalog.BundleHash,
		Declaration: ref, ExpectedHead: durabledata.AbsentHead(), InputFormat: "jsonl"})
	if err != nil {
		t.Fatal(err)
	}
	runID, at := uuid.NewString(), time.Now().UTC().Truncate(time.Microsecond)
	ctx = correlation.WithRunID(ctx, runID)
	plan, err := planner.PrepareFlowInstanceActivation(ctx, pipeline.FlowInstanceActivationRequest{ContractBundle: loaded.Source, OccurredAt: at,
		Instance: flowidentity.Stored(loaded.Source, ".", runID, runID, "", "")})
	if err != nil {
		t.Fatal(err)
	}
	result, err := selected.(bus.DeploymentRunCreationCommitOwner).CommitDeploymentRunCreation(ctx, bus.DeploymentRunCreationCommand{
		RunCreation: durabledata.RunCreationCommand{RunID: runID, Actor: "operator", BundleHash: catalog.BundleHash, Data: durabledata.RunCreationDataEnvelope{Pins: []durabledata.ExplicitPin{{Declaration: ref, VersionID: imported.Candidate.VersionID}}}},
		Idempotency: apiidempotency.Request{Method: "run.start", Actor: apiidempotency.BearerActor("operator"), Now: at, TTL: time.Hour}, Root: bus.FlowInstanceActivationCommand{Plan: plan},
	})
	if err != nil || !result.Acknowledged {
		t.Fatalf("original native deployment: %+v err=%v", result, err)
	}
	record, err := plan.PersistenceRecord()
	if err != nil {
		t.Fatal(err)
	}
	state := record.State
	state.Transition, state.ExpectedState, state.ExpectedRevision = pipeline.WorkflowEngineStateTransitionUpdateStateAndCompanion, "pending", 1
	state.Accumulator = json.RawMessage(`{"source_advanced":{"count":1}}`)
	operation := runfork.ForkOperationRequest{OperationID: uuid.NewString(), Actor: "bearer:branch-point-proof", IdempotencyKey: uuid.NewString(), TransportHash: "sha256:branch-point-proof",
		SourceRunID: runID, TargetBundleHash: catalog.BundleHash, ContractSelection: testContractSelection(), AllowSourceFreeze: true}
	latest, err := selected.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: runID})
	if err != nil {
		t.Fatal(err)
	}
	operation, err = operation.BindResolvedPoint(runfork.RunForkPoint{Kind: runfork.RunForkPointDeploymentRevision, Revision: latest.ForkPoint.Revision})
	if err != nil {
		t.Fatal(err)
	}
	request := SelectedContractExecutionRequest{Owner: owner, SourceRunID: runID, ForkOperation: &operation, AllowSourceFreeze: true,
		SourceLoader: loader, ContractSelection: testContractSelection(), AgentRuntime: SelectedContractAgentRuntimeOptions{ProcessCapability: owner.ports.contexts.capability, ExecutionPosture: executionposture.Live}}
	return ctx, request, func() {
		if _, err := selected.(pipeline.WorkflowEngineMutationOwner).CommitWorkflowEngineMutation(ctx, pipeline.WorkflowEngineMutationCommand{State: state}); err != nil {
			t.Fatal(err)
		}
	}
}

func mustComponentSourceFact(t *testing.T, hash string) correlation.SourceArtifactFact {
	t.Helper()
	fact, err := correlation.NewSourceArtifactFact(hash)
	if err != nil {
		t.Fatal(err)
	}
	return fact
}
