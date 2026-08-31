package cataloge2e

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimecore "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimediaglog "github.com/division-sh/swarm/internal/runtime/diaglog"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimeruncontrol "github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	runtimerunforkexecution "github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil/replayconformance"
	runlifecyclefixture "github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
)

const catalogSecondRunID = "99999999-9999-4999-8999-999999999999"

type runScopedCatalogSelectedStore interface {
	runtimeruncontrol.Store
	runtimerunforkexecution.SelectedContractActivationStore
	MaterializeRunForkForSelectedContractExecution(context.Context, runfork.RunForkSelectedContractExecutionMaterializeRequest) (runfork.RunForkMaterialization, error)
	ListFlowInstanceRouteRecords(context.Context, runtimeflowidentity.RunScopedFlowInstance) ([]runtimebus.FlowInstanceRouteRecord, error)
	ListOperatorAgents(context.Context, operatorread.OperatorAgentListOptions) (operatorread.OperatorAgentListResult, error)
	LoadRunLifecycleSnapshot(context.Context, string) (runtimebus.RunLifecycleSnapshot, error)
	ResolveOperatorAgentIdentity(context.Context, string, string, string) (agentidentity.Identity, error)
}

type catalogRunScopedPublicationSignal struct {
	delegate *runtimecore.RuntimeLogger
	event    string
	runs     map[string]chan<- struct{}
}

func (l catalogRunScopedPublicationSignal) Log(
	ctx context.Context,
	level runtimediaglog.Level,
	message, component, action, eventID, eventType, agentID, entityID, sessionID string,
	correlation map[string]string,
	detail any,
	failure *runtimefailures.Envelope,
	durationUS int,
) error {
	if l.delegate != nil {
		if err := l.delegate.Log(ctx, runtimecore.RuntimeLogEntry{
			Level: level, Message: message, Component: component, Action: action,
			EventID: eventID, EventType: eventType, AgentID: agentID, EntityID: entityID, SessionID: sessionID,
			Correlation: correlation, Detail: detail, Failure: runtimefailures.CloneEnvelope(failure), DurationUS: durationUS,
		}); err != nil {
			return err
		}
	}
	if action != "published" || eventType != l.event {
		return nil
	}
	if signal := l.runs[runtimecorrelation.RunIDFromContext(ctx)]; signal != nil {
		select {
		case signal <- struct{}{}:
		default:
		}
	}
	return nil
}

func TestRunScopedTemplateFlowAndAgentExecutionSupportedSurfaceBothStores(t *testing.T) {
	fixtureRoot := catalogRuntimeFixture(t, "catalog.runtime.flow_lifecycle", "test-create-flow-instance").Root
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			h := newRuntimeHarnessForBackend(t, fixtureRoot, backend, true)
			selected := runScopedCatalogStore(t, h)
			flowPath := "worker-flow/worker-001"
			runA, runB := catalogRuntimeRunID, catalogSecondRunID
			completedA := make(chan struct{}, 1)
			completedB := make(chan struct{}, 1)
			h.rt.Bus.SetLoggerHook(catalogRunScopedPublicationSignal{
				delegate: h.rt.Logger,
				event:    flowPath + "/worker.observed",
				runs:     map[string]chan<- struct{}{runA: completedA, runB: completedB},
			})

			catalogRequireSecondRun(t, h, runB)
			seedCatalogRootStateForRun(t, h, runA)
			seedCatalogRootStateForRun(t, h, runB)
			if err := publishCatalogRunScopedSpawn(h, runA, uuid.NewString()); err != nil {
				t.Fatalf("publish run A: %v", err)
			}
			waitForCatalogRunScopedPublication(t, h, completedA, runA)
			assertCatalogRunScopedFlowOwner(t, h, selected, runA, flowPath, "complete", false)
			assertCatalogRunScopedAgent(t, h, selected, runA, flowPath, "delivered")

			startedB := make(chan struct{}, 1)
			releaseB := make(chan struct{})
			h.llm.SetManagedRunBarrier(runB, startedB, releaseB)

			publishB := make(chan error, 1)
			go func() {
				publishB <- publishCatalogRunScopedSpawn(h, runB, uuid.NewString())
			}()
			select {
			case <-startedB:
			case err := <-publishB:
				t.Fatalf("run B publication completed before provider barrier: %v", err)
			case <-time.After(catalogRuntimePublishTimeout):
				t.Fatal("run B did not reach the real managed-provider boundary")
			}

			assertCatalogRunScopedFlowOwner(t, h, selected, runB, flowPath, "idle", true)
			assertCatalogRunScopedAgent(t, h, selected, runB, flowPath, "in_progress")

			controller := runtimeruncontrol.NewController(selected, h.rt.Bus, runtimeruncontrol.Options{})
			stopped, err := controller.Stop(catalogRunContext(h, runA), runtimeruncontrol.TransitionRequest{
				RunID: runA, Reason: "run-scoped identity proof", ControlledBy: "cataloge2e",
			})
			if err != nil {
				t.Fatalf("stop run A: %v", err)
			}
			if stopped.Status != runtimeruncontrol.StatusCancelled {
				t.Fatalf("run A stop status = %q", stopped.Status)
			}
			snapshotA, err := selected.LoadRunLifecycleSnapshot(catalogRunContext(h, runA), runA)
			if err != nil || snapshotA.EndedAt == nil {
				t.Fatalf("run A terminal snapshot = %#v err=%v", snapshotA, err)
			}
			select {
			case err := <-publishB:
				t.Fatalf("run B stopped waiting while run A retired: %v", err)
			default:
			}

			close(releaseB)
			select {
			case err := <-publishB:
				if err != nil {
					t.Fatalf("publish run B after release: %v", err)
				}
			case <-time.After(catalogRuntimePublishTimeout):
				t.Fatal("run B did not complete after provider release")
			}
			waitForCatalogRunScopedPublication(t, h, completedB, runB)

			assertCatalogRunScopedFlowOwner(t, h, selected, runB, flowPath, "complete", false)
			assertCatalogRunScopedAgent(t, h, selected, runB, flowPath, "delivered")
			assertCatalogRunScopedPublicReadback(t, h, selected, runA, runB, flowPath)
		})
	}
}

func TestRunScopedSelectedForkReconstructsFlowAndAgentOnBothStores(t *testing.T) {
	canonicalrouting.Prove(t, canonicalrouting.ArtifactID("tests/tier12-runtime-fork/test-run-scoped-flow-agent-fork"))
	fixtureRoot := catalogRuntimeFixture(t, "catalog.runtime.selected_contract_fork", "test-run-scoped-flow-agent-fork").Root
	repoRoot := repoRootFromCatalogE2E(t)
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			h := newRuntimeHarnessForBackend(t, fixtureRoot, backend, true)
			selected := runScopedCatalogStore(t, h)
			flowPath := "worker-flow/worker-001"
			sourceRunID := catalogRuntimeRunID
			flowEntityID := materializeCatalogSelectedForkSourceFlow(t, h, sourceRunID, flowPath)
			resolvedWorkerRoutes := h.rt.Bus.RouteTable().ResolveForRun(sourceRunID, flowPath+"/worker.ready")
			hasWorkerAgentPlan := false
			for _, route := range resolvedWorkerRoutes {
				if route.AgentPlan.AgentID() == "worker-agent" {
					hasWorkerAgentPlan = true
				}
			}
			if !hasWorkerAgentPlan {
				t.Fatalf("materialized worker route has no declared worker-agent plan: %#v", resolvedWorkerRoutes)
			}

			if _, err := selected.PauseRunControl(catalogRunContext(h, sourceRunID), runtimeruncontrol.TransitionRequest{
				RunID: sourceRunID, Reason: "selected-fork run-scoped identity proof", ControlledBy: "cataloge2e",
			}); err != nil {
				t.Fatalf("pause selected-fork source run: %v", err)
			}
			workerReady := catalogRunScopedWorkerReadyEvent(t, sourceRunID, flowPath, flowEntityID, uuid.NewString())
			workerPlan, err := h.rt.Bus.CheckPublishRecipientPlan(catalogRunContext(h, sourceRunID), workerReady)
			if err != nil {
				t.Fatalf("plan selected-fork source worker event: %v", err)
			}
			hasWorkerDelivery := false
			for _, route := range workerPlan.DeliveryRoutes {
				if route.Recipient.IsAgent() && route.AgentIdentity.AgentID() == "worker-agent" && route.AgentIdentity.RunID == sourceRunID {
					hasWorkerDelivery = true
				}
			}
			if !hasWorkerDelivery {
				t.Fatalf("selected-fork source worker plan has no exact agent delivery: %#v", workerPlan.DeliveryRoutes)
			}
			ctx, cancel := context.WithTimeout(catalogRunContext(h, sourceRunID), catalogRuntimePublishTimeout)
			if err := h.rt.Bus.PublishAndWait(ctx, workerReady); err != nil {
				cancel()
				t.Fatalf("publish paused source flow instance: %v", err)
			}
			cancel()
			sourceEvent := workerReady

			sourceOwner, err := runtimeflowidentity.NewRunScopedFlowInstance(sourceRunID, runtimeflowidentity.RouteForInstancePath(flowPath))
			if err != nil {
				t.Fatal(err)
			}
			sourceBefore, ok, err := h.workflow.Load(catalogRunContext(h, sourceRunID), sourceOwner)
			if err != nil || !ok {
				t.Fatalf("load source flow before fork: ok=%t err=%v", ok, err)
			}
			sourceRoutesBefore, err := selected.ListFlowInstanceRouteRecords(catalogRunContext(h, sourceRunID), sourceOwner)
			if err != nil || len(sourceRoutesBefore) == 0 {
				t.Fatalf("source routes before fork = %#v err=%v", sourceRoutesBefore, err)
			}
			sourceAgent := catalogRunScopedAgentDeliveryIdentity(t, h, sourceRunID, flowPath, "pending")

			loader, selection, selectedSource := selectedContractForkFixtureSelection(t, h.ctx, repoRoot, fixtureRoot)
			selectedBundle, ok := semanticview.Bundle(selectedSource.Source)
			if !ok {
				t.Fatal("selected-contract source has no loader-owned bundle")
			}
			if h.pg != nil {
				storetest.RequireBundleDataCatalog(t, h.ctx, h.pg, selectedBundle)
			} else {
				storetest.RequireBundleDataCatalog(t, h.ctx, h.sqlite, selectedBundle)
			}
			installCatalogSelectedSourceTopology(t, h.ctx, h, selectedSource)
			sourcePlan, err := selected.PlanRunFork(catalogRunContext(h, sourceRunID), runfork.RunForkPlanRequest{
				SourceRunID: sourceRunID,
				At:          sourceEvent.ID(),
			})
			if err != nil {
				t.Fatalf("plan selected-contract source fork: %v", err)
			}
			if len(sourcePlan.PendingWork) == 0 {
				t.Fatal("selected-contract source fork has no pending worker delivery")
			}

			cfg := testRuntimeConfig()
			cfg.LLM.Backend = "anthropic"
			agentRuntime := selectedContractAgentRuntimeOptionsForCatalogHarness(h, cfg)
			executionCtx := worklifetime.WithOccurrence(catalogRunContext(h, sourceRunID), h.rt.WorkOccurrence())
			materialization := materializeCatalogSelectedForkForBootResume(
				t, executionCtx, selected, selectedSource, selection, sourcePlan, agentRuntime, sourceEvent.ID(), flowPath, flowEntityID,
			)
			result, err := runtimerunforkexecution.ActivateSelectedContractRunFork(executionCtx, runtimerunforkexecution.SelectedContractActivationGateRequest{
				ForkRunID: materialization.ForkRunID, ConfirmSourceFreeze: true, Store: selected,
				ExecutionOwner: selectedContractExecutionOwnerForCatalogHarness(t, h), SourceLoader: loader,
				AgentRuntime: agentRuntime,
			})
			if err != nil {
				t.Fatalf("resume materialized selected-contract flow fork: %v; pending=%#v", err, sourcePlan.PendingWork)
			}
			if result.Owner != runfork.RunForkSelectedContractExecutionActivationGateOwner || !result.Activated || result.ExecutedEventCount == 0 {
				t.Fatalf("selected-contract flow fork result = %#v", result)
			}
			forkRunID := materialization.ForkRunID
			assertCatalogRunScopedFlowOwner(t, h, selected, forkRunID, flowPath, "complete", false)
			forkAgent := catalogRunScopedAgentDeliveryIdentity(t, h, forkRunID, flowPath, "delivered")
			if sourceAgent.RunID == forkAgent.RunID || sourceAgent.Name != forkAgent.Name || sourceAgent.Route != forkAgent.Route {
				t.Fatalf("source/fork agent identity = %#v/%#v, want equal declaration+route and distinct run", sourceAgent, forkAgent)
			}

			sourceAfter, ok, err := h.workflow.Load(catalogRunContext(h, sourceRunID), sourceOwner)
			if err != nil || !ok {
				t.Fatalf("load source flow after fork: ok=%t err=%v", ok, err)
			}
			sourceRoutesAfter, err := selected.ListFlowInstanceRouteRecords(catalogRunContext(h, sourceRunID), sourceOwner)
			if err != nil {
				t.Fatalf("source routes after fork: %v", err)
			}
			if sourceBefore.CurrentState != sourceAfter.CurrentState || sourceBefore.Revision != sourceAfter.Revision ||
				!reflect.DeepEqual(sourceBefore.Fields, sourceAfter.Fields) || !reflect.DeepEqual(sourceRoutesBefore, sourceRoutesAfter) {
				t.Fatalf("source flow changed across fork: before=%#v routes=%#v after=%#v routes=%#v", sourceBefore, sourceRoutesBefore, sourceAfter, sourceRoutesAfter)
			}
			_ = catalogRunScopedAgentDeliveryIdentity(t, h, sourceRunID, flowPath, "pending")
		})
	}
}

func materializeCatalogSelectedForkForBootResume(
	t testing.TB,
	ctx context.Context,
	selected runScopedCatalogSelectedStore,
	loaded runtimerunforkexecution.LoadedSelectedContractSource,
	selection runfork.RunForkContractSelection,
	plan runfork.RunForkPlan,
	agentRuntime runtimerunforkexecution.SelectedContractAgentRuntimeOptions,
	sourceEventID, flowPath, entityID string,
) runfork.RunForkMaterialization {
	t.Helper()
	frontier, err := runforkadmission.AdmitContractFrontier(runforkadmission.ContractFrontierRequest{
		Plan: plan, Source: loaded.Source, ContractSelection: selection,
	})
	if err != nil {
		t.Fatalf("admit selected-contract boot-resume frontier: %v", err)
	}
	routeAdmission, err := runforkadmission.AdmitSelectedContractRouteHistory(runforkadmission.SelectedContractRouteHistoryRequest{
		Plan: plan, Source: loaded.Source, ContractSelection: selection, FrontierAdmission: frontier,
	})
	if err != nil {
		t.Fatalf("admit selected-contract boot-resume route history: %v", err)
	}
	topology, err := runtimerunforkexecution.BuildSelectedContractRouteTopology(runtimerunforkexecution.SelectedContractRouteTopologyRequest{
		Admission: frontier, RouteAdmission: routeAdmission,
	})
	if err != nil {
		t.Fatalf("build selected-contract boot-resume route topology: %v", err)
	}
	model, err := runtimerunforkexecution.BuildSelectedContractExecutionModel(runtimerunforkexecution.SelectedContractExecutionModelRequest{
		Admission: frontier, RouteAdmission: routeAdmission, RouteTopology: topology,
	})
	if err != nil || model.RecipientPlanning == nil {
		t.Fatalf("build selected-contract boot-resume execution model: model=%#v err=%v", model, err)
	}
	blueprints, err := runtimemanager.TemplateFlowAgentMaterializationBlueprints(
		loaded.Source, "worker-flow", flowPath, entityID,
	)
	if err != nil || len(blueprints) == 0 {
		t.Fatalf("derive selected-contract boot-resume agent blueprints: count=%d err=%v", len(blueprints), err)
	}
	managerOptions := agentRuntime.AgentManagerOptions
	managerOptions.ExecutionPosture = agentRuntime.ExecutionPosture
	if agentRuntime.Config != nil {
		profile, err := agentRuntime.Config.LLMBackendProfile()
		if err != nil {
			t.Fatalf("resolve selected-contract boot-resume backend profile: %v", err)
		}
		managerOptions.LLMBackend = profile.ID
	}
	var workflowConfig map[string]any
	expectations := make([]runfork.RunForkSelectedContractAgentExpectation, 0, len(blueprints))
	for _, unresolved := range blueprints {
		blueprint, err := runtimemanager.ResolveAgentMaterializationBlueprint(managerOptions, unresolved)
		if err != nil {
			t.Fatalf("resolve selected-contract boot-resume agent blueprint: %v", err)
		}
		plan := blueprint.Identity.Normalize()
		if plan.FlowInstance() != flowPath {
			continue
		}
		candidateConfig := map[string]any{}
		if len(blueprint.Config.Config) > 0 {
			if err := json.Unmarshal(blueprint.Config.Config, &candidateConfig); err != nil {
				t.Fatalf("decode selected-contract boot-resume workflow config: %v", err)
			}
		}
		if workflowConfig == nil {
			workflowConfig = candidateConfig
		} else if !reflect.DeepEqual(workflowConfig, candidateConfig) {
			t.Fatalf("selected-contract boot-resume agent configs disagree: %#v and %#v", workflowConfig, candidateConfig)
		}
		revision, err := runtimemanager.AgentConfigPlanRevision(blueprint.Config, plan)
		if err != nil {
			t.Fatalf("derive selected-contract boot-resume agent revision: %v", err)
		}
		expectations = append(expectations, runfork.RunForkSelectedContractAgentExpectation{
			Plan: plan, ConfigRevision: revision,
		})
	}
	if len(expectations) == 0 || workflowConfig == nil {
		t.Fatal("selected-contract boot-resume workflow has no declaration-owned agent readiness")
	}
	workflowState := runfork.RunForkSelectedContractWorkflowState{
		SourceEventID:   sourceEventID,
		EntityID:        entityID,
		FlowID:          "worker-flow",
		WorkflowVersion: loaded.Source.WorkflowVersion(),
		ExecutionMode:   executionmode.Live,
		Mode:            "template",
		AddressKind:     runfork.RunForkSelectedContractWorkflowStateExact,
		Route: runtimeflowidentity.StoredRoute(
			expectations[0].Plan.Route.ScopeKey,
			expectations[0].Plan.Route.InstanceID,
			expectations[0].Plan.Route.InstancePath,
		),
		Config: workflowConfig,
		Agents: expectations,
	}
	ctx = runtimecorrelation.WithBundleSourceFact(ctx, loaded.BundleSourceFact)
	materialization, err := selected.MaterializeRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID:             plan.SourceRunID,
		At:                      sourceEventID,
		ContractSelection:       selection,
		BundleSourceFact:        loaded.BundleSourceFact,
		EffectiveSourceIdentity: loaded.EffectiveSourceIdentity,
		FrontierAdmission:       frontier,
		RouteTopology:           topology,
		RecipientPlanning:       *model.RecipientPlanning,
		WorkflowStates:          []runfork.RunForkSelectedContractWorkflowState{workflowState},
	})
	if err != nil {
		t.Fatalf("materialize selected-contract boot-resume crash boundary: %v", err)
	}
	if materialization.ForkRunID == "" || len(materialization.AgentTopologies) == 0 {
		t.Fatalf("selected-contract boot-resume materialization lacks exact agent topology: %#v", materialization)
	}
	return materialization
}

func waitForCatalogRunScopedPublication(t testing.TB, h *runtimeHarness, signal <-chan struct{}, runID string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(catalogRuntimePublishTimeout):
		observed, err := catalogRunScopedOperatorEvents(h, runID)
		t.Fatalf("run %s did not publish its terminal worker event: observed=%#v read_error=%v", runID, observed, err)
	}
}

func catalogRunScopedOperatorEvents(h *runtimeHarness, runID string) (map[string]operatorread.OperatorEventFull, error) {
	ctx, cancel := context.WithTimeout(testAuthorActivityContext(context.Background()), catalogRuntimePublishTimeout)
	defer cancel()
	lister, err := h.catalogOperatorEventLister()
	if err != nil {
		return nil, err
	}
	return replayconformance.LoadOperatorEvents(ctx, lister, runID)
}

func runScopedCatalogStore(t testing.TB, h *runtimeHarness) runScopedCatalogSelectedStore {
	t.Helper()
	if h.pg != nil {
		return h.pg
	}
	if h.sqlite != nil {
		return h.sqlite
	}
	t.Fatal("catalog selected store is required")
	return nil
}

func catalogRequireSecondRun(t testing.TB, h *runtimeHarness, runID string) {
	t.Helper()
	source := catalogBundleSourceFact(t, h.bundle)
	fixture := runlifecyclefixture.Fixture{
		Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runID,
		BundleHash: source.BundleHash(), BundleSource: "ephemeral",
	}
	ctx := catalogRunContext(h, runID)
	if h.pg != nil {
		runlifecyclefixture.RequirePostgres(t, ctx, h.db, fixture)
		return
	}
	runlifecyclefixture.RequireSQLite(t, ctx, h.db, fixture)
}

func catalogRunContext(h *runtimeHarness, runID string) context.Context {
	return runtimecorrelation.WithRunID(h.ctx, runID)
}

func seedCatalogRootStateForRun(t testing.TB, h *runtimeHarness, runID string) {
	t.Helper()
	owner, err := runtimeflowidentity.NewRunScopedFlowInstance(runID, runtimeflowidentity.RouteForInstancePath(runID))
	if err != nil {
		t.Fatal(err)
	}
	ctx := worklifetime.WithOccurrence(catalogRunContext(h, runID), h.rt.WorkOccurrence())
	ctx = runtimeeffects.WithExecutionMode(ctx, executionmode.Live)
	_, err = h.workflow.MaterializeInitialEntry(ctx, owner, runtimepipeline.WorkflowInstance{
		InstanceID: runID, StorageRef: runID, EntityID: runtimepipeline.FlowInstanceEntityID(runID),
		WorkflowName: h.bundle.WorkflowName(), WorkflowVersion: h.bundle.WorkflowVersion(),
		CurrentState: h.initialState, EnteredStageAt: h.startedAt, CreatedAt: h.startedAt,
		EntityType: h.requireRootEntityType(),
	}, h.startedAt)
	if err != nil {
		t.Fatalf("materialize root state for run %s: %v", runID, err)
	}
}

func materializeCatalogSelectedForkSourceFlow(t testing.TB, h *runtimeHarness, runID, flowPath string) string {
	t.Helper()
	entityID := eventtest.UUID("run-scoped-selected-fork-worker")
	at := time.Now().UTC()
	ctx := worklifetime.WithOccurrence(catalogRunContext(h, runID), h.rt.WorkOccurrence())
	ctx = runtimeeffects.WithExecutionMode(ctx, executionmode.Live)
	trigger := eventtest.ExistingRunRootIngress(
		uuid.NewString(), "catalog.selected_fork_source_admitted", "cataloge2e", "", nil, 0, runID,
		events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), at,
	)
	if err := h.rt.Manager.ActivateFlowInstance(ctx, runtimepipeline.FlowInstanceActivationRequest{
		ContractBundle: semanticview.Wrap(h.bundle),
		Instance: runtimeflowidentity.Stored(
			semanticview.Wrap(h.bundle), "worker-flow", flowPath, "worker-001", entityID, "",
		),
		Config:       map[string]any{"worker_id": "worker-001"},
		Fields:       map[string]any{"worker_id": "worker-001"},
		TriggerEvent: trigger,
		OccurredAt:   at,
	}); err != nil {
		t.Fatalf("activate selected-fork source flow through committed readiness: %v", err)
	}
	return entityID
}

func catalogRunScopedWorkerReadyEvent(t testing.TB, runID, flowPath, entityID, eventID string) events.Event {
	t.Helper()
	payload, err := json.Marshal(map[string]any{"worker_id": "worker-001"})
	if err != nil {
		t.Fatal(err)
	}
	envelope := events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), flowPath)
	routingSource, err := events.NewConcreteTemplateInstanceRoutingSource(events.RouteIdentity{
		FlowID: "worker-flow", FlowInstance: flowPath, EntityID: entityID,
	})
	if err != nil {
		t.Fatal(err)
	}
	return eventtest.ExistingRunRootIngressWithRoutingSource(
		eventID, events.EventType(flowPath+"/worker.ready"), "cataloge2e", "", payload, 0, runID, envelope, routingSource, time.Now().UTC(),
	)
}

func publishCatalogRunScopedSpawn(h *runtimeHarness, runID, eventID string) error {
	payload, err := json.Marshal(map[string]any{"instance_id": "worker-001", "worker_id": "worker-001"})
	if err != nil {
		return err
	}
	envelope := events.EnvelopeForEntityID(events.EventEnvelope{}, runtimepipeline.FlowInstanceEntityID(runID))
	event := eventtest.ExistingRunRootIngress(
		eventID, "flow.spawn_requested", "cataloge2e", "", payload, 0, runID, envelope, time.Now().UTC(),
	)
	ctx, cancel := context.WithTimeout(catalogRunContext(h, runID), catalogRuntimePublishTimeout)
	defer cancel()
	return h.rt.Bus.PublishAndWait(ctx, event)
}

func assertCatalogRunScopedFlowOwner(
	t testing.TB,
	h *runtimeHarness,
	selected runScopedCatalogSelectedStore,
	runID, flowPath, wantState string,
	wantActiveRoutes bool,
) {
	t.Helper()
	owner, err := runtimeflowidentity.NewRunScopedFlowInstance(runID, runtimeflowidentity.RouteForInstancePath(flowPath))
	if err != nil {
		t.Fatal(err)
	}
	instance, ok, err := h.workflow.Load(catalogRunContext(h, runID), owner)
	if err != nil || !ok {
		t.Fatalf("load %s: ok=%t err=%v", owner.Key(), ok, err)
	}
	if instance.CurrentState != wantState {
		t.Fatalf("%s state = %q, want %q", owner.Key(), instance.CurrentState, wantState)
	}
	routes, err := selected.ListFlowInstanceRouteRecords(catalogRunContext(h, runID), owner)
	if err != nil {
		t.Fatalf("%s route records: %v", owner.Key(), err)
	}
	if wantActiveRoutes && len(routes) == 0 {
		t.Fatalf("%s has no active route records", owner.Key())
	}
	if !wantActiveRoutes && len(routes) != 0 {
		t.Fatalf("%s terminal owner retained %d active route records", owner.Key(), len(routes))
	}
	for _, route := range routes {
		if route.Identity != owner {
			t.Fatalf("route identity = %#v, want %#v", route.Identity, owner)
		}
	}
}

func assertCatalogRunScopedAgent(
	t testing.TB,
	h *runtimeHarness,
	selected runScopedCatalogSelectedStore,
	runID, flowPath string,
	wantDeliveryStatus string,
) {
	t.Helper()
	identity := catalogRunScopedAgentDeliveryIdentity(t, h, runID, flowPath, wantDeliveryStatus)
	if identity.RunID != runID || identity.FlowInstance() != flowPath {
		t.Fatalf("worker-agent identity = %#v, want run=%s flow=%s", identity, runID, flowPath)
	}
	if identity.AgentID() != "worker-agent" {
		t.Fatalf("worker-agent identity = %#v", identity)
	}
	if wantDeliveryStatus != "in_progress" {
		return
	}
	resolved, err := selected.ResolveOperatorAgentIdentity(catalogRunContext(h, runID), runID, "worker-agent", flowPath)
	if err != nil {
		t.Fatalf("resolve active worker-agent for run %s: %v", runID, err)
	}
	if resolved != identity {
		t.Fatalf("active worker-agent resolve = %#v, want persisted delivery identity %#v", resolved, identity)
	}
	result, err := selected.ListOperatorAgents(catalogRunContext(h, runID), operatorread.OperatorAgentListOptions{Flow: flowPath})
	if err != nil {
		t.Fatalf("list worker-agent for run %s: %v", runID, err)
	}
	for _, summary := range result.Agents {
		if summary.Identity != identity {
			continue
		}
		return
	}
	t.Fatalf("worker-agent %s missing from public list", identity.Description())
}

func catalogRunScopedAgentDeliveryIdentity(t testing.TB, h *runtimeHarness, runID, flowPath, wantStatus string) agentidentity.Identity {
	t.Helper()
	observed, err := catalogRunScopedOperatorEvents(h, runID)
	if err != nil {
		t.Fatalf("read worker-agent delivery for run %s: %v", runID, err)
	}
	for _, event := range observed {
		if event.RunID != runID || event.EventName != flowPath+"/worker.ready" {
			continue
		}
		for _, delivery := range event.Deliveries {
			identity := delivery.Route.AgentIdentity.Normalize()
			if identity.RunID != runID || identity.FlowInstance() != flowPath || identity.AgentID() != "worker-agent" {
				continue
			}
			if delivery.Status != wantStatus {
				t.Fatalf("worker-agent %s delivery status = %q, want %q", identity.Description(), delivery.Status, wantStatus)
			}
			return identity
		}
	}
	t.Fatalf("worker-agent for run %s flow %s has no public worker.ready delivery", runID, flowPath)
	return agentidentity.Identity{}
}

func assertCatalogRunScopedPublicReadback(
	t testing.TB,
	h *runtimeHarness,
	selected runScopedCatalogSelectedStore,
	runA, runB, flowPath string,
) {
	t.Helper()
	seen := map[string]bool{}
	for _, runID := range []string{runA, runB} {
		observed, err := catalogRunScopedOperatorEvents(h, runID)
		if err != nil {
			t.Fatalf("list exact run-scoped agent deliveries for %s: %v", runID, err)
		}
		for _, event := range observed {
			if event.EventName != flowPath+"/worker.ready" {
				continue
			}
			for _, delivery := range event.Deliveries {
				identity := delivery.Route.AgentIdentity.Normalize()
				if identity.AgentID() == "worker-agent" && identity.FlowInstance() == flowPath {
					seen[identity.RunID] = true
				}
			}
		}
	}
	if !seen[runA] || !seen[runB] || len(seen) != 2 {
		t.Fatalf("public worker-agent run owners = %v, want exactly %s and %s", seen, runA, runB)
	}
	if _, err := selected.ResolveOperatorAgentIdentity(h.ctx, "", "worker-agent", flowPath); err == nil {
		t.Fatal("runless worker-agent lookup unexpectedly succeeded")
	}
}
