package runforkexecution

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	rootruntime "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deadletters"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/flowactivationfixture"
	"github.com/google/uuid"
)

type publishedJoinSourceDriver struct {
	callback func(context.Context, genericschedule.Wakeup)
	wakeup   genericschedule.Wakeup
}

func (d *publishedJoinSourceDriver) BindGenericScheduleLifecycle(callback func(context.Context, genericschedule.Wakeup)) error {
	d.callback = callback
	return nil
}
func (d *publishedJoinSourceDriver) RegisterGenericScheduleWakeup(_ context.Context, wakeup genericschedule.Wakeup) error {
	d.wakeup = wakeup
	return wakeup.Validate()
}
func (*publishedJoinSourceDriver) RetireGenericScheduleWakeup(genericschedule.Wakeup) error {
	return nil
}
func (*publishedJoinSourceDriver) StopGenericScheduleWakeups(context.Context) error { return nil }

// Hold dispatch after the real generic publication transaction acknowledges.
// This creates the actual already-published, unfinished cut under test.
type publishedJoinSourceDispatch struct{ event events.Event }

func (d *publishedJoinSourceDispatch) DispatchPostCommit(_ context.Context, intents []engine.EmitIntent) error {
	if len(intents) != 1 {
		panic("published join fixture requires exactly one occurrence")
	}
	d.event = intents[0].Event
	return nil
}

const publishedJoinFlowEnv = "SWARM_PUBLISHED_JOIN_TEST_FLOW"

func publishedJoinFixture(t *testing.T) string {
	t.Helper()
	// The existing crash subprocess inherits this test-scoped fixture selection.
	flowID := os.Getenv(publishedJoinFlowEnv)
	root := canonicalrouting.CopyExactJoinEventBusProof(t, flowID)
	path := filepath.Join(root, flowID, "nodes.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	deadline := "        deadline: {after: 1h, from: stage_entry}\n        on_deadline: {advances_to: attention}\n"
	if strings.Count(string(data), deadline) != 1 {
		t.Fatal("published join fixture lost its exact deadline variant")
	}
	data = []byte(strings.ReplaceAll(string(data), deadline, ""))
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return root
}

func seedPublishedJoinSource(t *testing.T, ctx context.Context, selected any, owner SelectedContractExecutionOwner, loaded LoadedSelectedContractSource, runID string) (string, genericschedule.Activation) {
	t.Helper()
	ctx = effects.WithExecutionMode(correlation.WithRunID(correlation.WithSourceArtifactFact(ctx, loaded.SourceArtifactFact), runID), executionmode.Mock)
	scope, err := authoractivity.BundleScopeForTarget(ctx, loaded.SourceArtifactFact.BundleHash())
	if err != nil {
		t.Fatal(err)
	}
	ctx = authoractivity.WithScope(ctx, scope)
	descriptors, err := rootruntime.AuthorActivityEventDescriptors(loaded.Source)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := owner.ports.fork.RegisterAuthorActivityEventCatalog(scope, descriptors)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(catalog.Release)
	at := runlifecycle.CanonicalTimestamp(time.Now().UTC().Add(-time.Minute))
	bundle, ok := semanticview.Bundle(loaded.Source)
	if !ok || bundle.SourceArtifact == nil {
		t.Fatal("published join source lacks its immutable artifact")
	}
	storetest.RequireRun(t, ctx, selected.(storetest.RunFixtureStore), storetest.RunFixture{RunID: runID,
		Origin: storetest.ScenarioSetupOrigin(), Artifact: bundle.SourceArtifact, BundleHash: loaded.SourceArtifactFact.BundleHash(), StartedAt: at})
	event := eventtest.ExistingRunRootIngressWithRoutingSourceAndMode(uuid.NewString(), "item.completed", "source-runtime", "",
		[]byte(`{"member_id":"source","result":{"value":"source"}}`), 0, runID, events.EventEnvelope{}, events.NoRoutingSource(), at, executionmode.Mock)
	identity := flowidentity.Stored(loaded.Source, semanticview.RootExecutionFlowID(loaded.Source), runID, runID, runID, "")
	work, _ := worklifetime.OccurrenceFromContext(ctx)
	authority, err := deliverylifecycle.NewNormalExecutionAuthority(loaded.SourceArtifactFact, runForkTestRuntimeInstanceID, 1)
	if err != nil {
		t.Fatal(err)
	}
	eventBus, err := bus.NewEventBusWithOptions(owner.ports.events, bus.EventBusOptions{
		ExecutionPosture: executionposture.MockOnly, WorkOwner: work, PipelineObligations: owner.ports.pipelineObligations,
		ContractBundle: loaded.Source, SourceArtifactFact: loaded.SourceArtifactFact, RuntimeInstanceID: runForkTestRuntimeInstanceID,
		DeliveryAuthority: authority, ReceiverExecution: eventreceiver.NormalExecution(), Durable: owner.ports.busDurable,
	})
	if err != nil {
		t.Fatal(err)
	}
	coordinator := pipeline.NewPipelineCoordinatorWithOptions(eventBus, pipeline.PipelineCoordinatorOptions{
		Module: selectedContractWorkflowModule{source: loaded.Source}, Persistence: owner.ports.workflow,
		SourceArtifactFact: loaded.SourceArtifactFact, ExecutionPosture: executionposture.MockOnly,
		ReceiverExecution: eventreceiver.NormalExecution(), WorkOwner: work, RunLifecycle: selected.(runlifecycle.OperationOwner),
		PipelineObligations: owner.ports.pipelineObligations, DeliveryStore: owner.ports.busDurable.DeliveryLifecycle,
		DeadLetters: selected.(deadletters.AcknowledgedRecorder), DeliveryRuntime: eventBus,
		DecisionCards: owner.ports.decisionCards, ProposedEffects: owner.ports.proposedEffects, HumanTasks: owner.ports.humanTasks,
		DecisionCardDraftExpiry: owner.ports.decisionCardDraftExpiry, HumanTaskExpiry: owner.ports.humanTaskExpiry,
	})
	if coordinator == nil {
		t.Fatal("published join source lifecycle planner was not admitted")
	}
	if os.Getenv(publishedJoinFlowEnv) == "orders" {
		event = seedPublishedOrdersJoinConstruction(t, ctx, selected, loaded, coordinator, identity, event, at)
	} else {
		command := selectedExecutionSourceFlowCommand(t, ctx, loaded, event, identity)
		instance, lifecycle, err := coordinator.PrepareInitialEntryLifecycle(ctx,
			flowidentity.RunScopedFlowInstance{RunID: runID, Route: identity.Route()}, command.Plan.Instance, at)
		if err != nil {
			t.Fatal(err)
		}
		command, err = flowactivationfixture.Command(ctx, instance, lifecycle, at)
		if err != nil {
			t.Fatal(err)
		}
		commit, err := selected.(bus.FlowInstanceActivationCommitOwner).CommitFlowInstanceActivation(ctx, command)
		if err != nil || !commit.Acknowledged || !commit.Created {
			t.Fatalf("canonical empty-join construction: %+v err=%v", commit, err)
		}
	}
	driver, dispatch := &publishedJoinSourceDriver{}, &publishedJoinSourceDispatch{}
	schedules := selected.(genericschedule.Store)
	logger := rootruntime.NewGenericScheduleRuntimeLogger(rootruntime.NewRuntimeLogger(owner.ports.logs, executionposture.MockOnly, nil))
	clock, err := genericschedule.NewLifecycle(schedules, driver, eventBus, dispatch, logger, executionposture.MockOnly)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := clock.Stop(context.Background()); err != nil {
			t.Error(err)
		}
	})
	if err := clock.ReconcileRunWakeups(ctx, runID); err != nil {
		t.Fatal(err)
	}
	if driver.callback == nil || driver.wakeup.ActivationID() == "" {
		t.Fatal("canonical empty-join construction did not install its completion wakeup")
	}
	driver.callback(ctx, driver.wakeup)
	activation, found, err := schedules.LoadGenericScheduleActivation(ctx, driver.wakeup.ActivationID())
	if err != nil || !found || activation.Status != genericschedule.StatusFired || dispatch.event.ID() != activation.CurrentEventID {
		t.Fatalf("actual accepted source occurrence: %+v found=%v event=%s err=%v", activation, found, dispatch.event.ID(), err)
	}
	marker := eventtest.ExistingRunRootIngressWithRoutingSourceAndMode(uuid.NewString(), event.Type(), "source-runtime", "",
		event.Payload(), 0, runID, events.EventEnvelope{}, events.NoRoutingSource(), at.Add(time.Second), executionmode.Mock)
	if os.Getenv(publishedJoinFlowEnv) == "orders" {
		admission, err := rootruntime.NewRuntimePayloadAdmitter(nil, loaded.Source, loaded.SourceArtifactFact)(ctx, marker, "orders")
		if err != nil {
			t.Fatal(err)
		}
		marker, err = events.ApplyPayloadAdmission(marker, admission)
		if err != nil {
			t.Fatal(err)
		}
	}
	storetest.CommitSemanticEventWithRoutes(t, ctx, selected, marker, nil, pipelineobligation.ScopeSubscribed)
	storetest.CaptureRunForkSnapshot(t, ctx, selected, runID)
	return marker.ID(), activation
}

func seedPublishedOrdersJoinConstruction(t *testing.T, ctx context.Context, selected any, loaded LoadedSelectedContractSource, coordinator *pipeline.PipelineCoordinator, root flowidentity.Instance, event events.Event, at time.Time) events.Event {
	t.Helper()
	work, _ := worklifetime.OccurrenceFromContext(ctx)
	planner := manager.NewAgentManagerWithOptions(nil, nil, manager.AgentManagerOptions{
		BaseContext: ctx, SemanticSource: loaded.Source, SourceArtifactFact: loaded.SourceArtifactFact,
		WorkflowInstances: coordinator, ReceiverExecution: eventreceiver.NormalExecution(),
		ExecutionPosture: executionposture.MockOnly, WorkOwner: work,
	})
	commit := func(plan pipeline.FlowInstanceActivationPlan) pipeline.CommittedFlowInstanceActivation {
		t.Helper()
		command := bus.FlowInstanceActivationCommand{Plan: plan}
		for _, construction := range plan.ConstructionPlans() {
			command.RouteTopology = append(command.RouteTopology, bus.FlowInstanceRouteRecordSet{
				Identity: flowidentity.RunScopedFlowInstance{RunID: construction.Readiness.RunID, Route: construction.Identity.Route()},
			})
		}
		committed, err := selected.(bus.FlowInstanceActivationCommitOwner).CommitFlowInstanceActivation(ctx, command)
		if err != nil || !committed.Acknowledged || !committed.Created {
			t.Fatalf("canonical published-flow construction: %+v err=%v", committed, err)
		}
		if err := committed.Validate(); err != nil {
			t.Fatal(err)
		}
		return committed
	}
	parent, err := planner.PrepareFlowInstanceActivation(ctx, pipeline.FlowInstanceActivationRequest{
		ContractBundle: loaded.Source, Instance: root, OccurredAt: at,
	})
	if err != nil {
		t.Fatal(err)
	}
	committedParent := commit(parent)
	if committedParent.Plan.Identity != root || committedParent.Plan.Readiness.Identity != root {
		t.Fatalf("source parent construction lost its complete identity/readiness: %+v", committedParent.Plan)
	}
	identity, err := flowidentity.KeyedChild(loaded.Source, committedParent.Plan.Identity, "orders", uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	constructors, err := pipeline.CompileFlowConstructors(loaded.Source, "orders")
	if err != nil || len(constructors) != 1 {
		t.Fatalf("orders fixture requires its exact existing constructor pin: count=%d err=%v", len(constructors), err)
	}
	input := constructors[0].Input()
	declaration := semanticview.ResolveFlowEventProof(loaded.Source, "orders", input)
	if !declaration.HasSchema || !declaration.IsAuthored(loaded.Source) || declaration.EventKey() == "item.completed" {
		t.Fatalf("orders creating input lacks its canonical scoped declaration: %+v", declaration)
	}
	event = eventtest.ExistingRunRootIngressWithRoutingSourceAndMode(event.ID(), events.EventType(declaration.EventKey()), "source-runtime", "",
		event.Payload(), 0, root.InstancePath, events.EventEnvelope{}, events.NoRoutingSource(), at, executionmode.Mock)
	admission, err := rootruntime.NewRuntimePayloadAdmitter(nil, loaded.Source, loaded.SourceArtifactFact)(ctx, event, "orders")
	if err != nil {
		t.Fatal(err)
	}
	if admission.Binding().FlowID() != "orders" || admission.Binding().BundleHash() != loaded.SourceArtifactFact.BundleHash() {
		t.Fatal("orders creating input borrowed another declaring flow or source artifact")
	}
	event, err = events.ApplyPayloadAdmission(event, admission)
	if err != nil {
		t.Fatal(err)
	}
	// The creating input is historical evidence, not an owed member delivery.
	storetest.CommitSemanticEventWithRoutes(t, ctx, selected, event, nil, pipelineobligation.ScopeSubscribed)
	plan, err := planner.PrepareFlowInstanceActivation(ctx, pipeline.FlowInstanceActivationRequest{
		ContractBundle: loaded.Source, Instance: identity, ConstructorInput: input, ResolvedKey: identity.InstanceID,
		TriggerEvent: event, OccurredAt: at,
	})
	if err != nil {
		t.Fatal(err)
	}
	committed := commit(plan)
	if committed.Plan.Identity != identity || committed.Plan.Readiness.Identity != identity ||
		committed.Plan.CreatingInput.EventID != event.ID() || committed.Plan.CreatingInput.Input != input {
		t.Fatalf("orders construction lost its exact parent/readiness/creating input: %+v", committed.Plan)
	}
	return event
}

func TestIssue642PublishedJoinCrashRestartBothStores(t *testing.T) {
	for _, fixture := range []struct{ backend, flowID string }{{"sqlite", ""}, {"sqlite", "orders"}, {"postgres", ""}, {"postgres", "orders"}} {
		backend := fixture.backend
		for _, cut := range []string{"retained_join_after_activation", "retained_join_event_committed"} {
			name := backend + "/" + cut
			if fixture.flowID != "" {
				name = backend + "/" + fixture.flowID + "/" + cut
			}
			t.Run(name, func(t *testing.T) {
				t.Setenv(publishedJoinFlowEnv, fixture.flowID)
				var selected startupownership.Store
				var construct func() SelectedContractExecutionOwner
				var dsn string
				if backend == "sqlite" {
					s := storetest.StartSQLiteRuntimeStore(t)
					selected, dsn = s, s.Path()
					construct = func() SelectedContractExecutionOwner { return newSelectedContractSQLiteExecutionOwnerForTest(t, s) }
				} else {
					dsn = testutil.StartPostgresDSN(t)
					s, _ := storetest.StartPostgresRuntimeStoreWithReopen(t, dsn)
					selected = s
					construct = func() SelectedContractExecutionOwner { return newSelectedContractExecutionOwnerForTest(t, s) }
				}
				checkpoint := killSelectedForkAtCheckpoint(t, backend, dsn, cut)
				operations := selected.(interface {
					LoadForkOperation(context.Context, string, string, string) (runfork.ForkOperationRecord, bool, error)
				})
				acknowledged, found, err := operations.LoadForkOperation(t.Context(), "retained-crash", "fixed-cut", "retained-crash-transport")
				if err != nil || !found || acknowledged.Status != runfork.ForkOperationActivated || acknowledged.ForkRunID != checkpoint.ForkRun || acknowledged.Result == nil || acknowledged.Request.ResolvedPoint == nil {
					t.Fatalf("published join lost permanent activation acknowledgment: %+v found=%v err=%v", acknowledged, found, err)
				}
				plan, err := selected.(SelectedContractForkLifecycle).PlanRunFork(t.Context(), runfork.RunForkPlanRequest{
					SourceRunID: checkpoint.SourceRun, ResolvedPoint: acknowledged.Request.ResolvedPoint})
				if err != nil || len(plan.JoinSchedules) != 1 || plan.JoinSchedules[0].Status != genericschedule.StatusFired {
					t.Fatalf("crash lost accepted source occurrence: plan=%+v err=%v", plan, err)
				}
				source := plan.JoinSchedules[0]
				requirePublishedJoinSourceScope(t, source, fixture.flowID)
				before, err := storetest.ReadSelectedForkSourceDomain(t.Context(), selected, checkpoint.SourceRun)
				if err != nil {
					t.Fatal(err)
				}
				ctx := runForkTestContext(t)
				capability := selectedContractTestProcessCapability(t, ctx, selected)
				process, _ := worklifetime.ProcessFromContext(ctx)
				owner := construct()
				if err := owner.BindSelectedProcess(ctx, process, capability); err != nil {
					t.Fatal(err)
				}
				baseline := process.ActiveCount()
				t.Cleanup(func() {
					if err := owner.RetireSelectedContexts(context.Background()); err != nil {
						t.Error(err)
					}
				})
				loader := SourceArtifactSelectedContractSourceLoader{RepoRoot: runForkExecutionRepoRoot(t), Store: selected.(SourceArtifactSelectedContractSourceStore)}
				recovered, err := owner.RecoverSelectedForkContexts(ctx, effects.NewRecoveryRequest(time.Now().UTC(), executionposture.MockOnly), SelectedForkRecoveryEnvironment{
					SourceLoader: loader, AgentRuntime: SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.MockOnly, ProcessCapability: capability}})
				if err != nil || len(recovered) != 1 || recovered[0].RunID != checkpoint.ForkRun || recovered[0].Disposition != runfork.SelectedForkRecoveryResume {
					t.Fatalf("published occurrence successor recovery: %+v err=%v", recovered, err)
				}
				wait, cancel := context.WithTimeout(t.Context(), 15*time.Second)
				defer cancel()
				reader := selected.(interface {
					LoadRunHeader(context.Context, string) (operatorread.RunHeader, error)
				})
				for {
					header, err := reader.LoadRunHeader(wait, checkpoint.ForkRun)
					if err != nil {
						t.Fatal(err)
					}
					if header.Failure != nil {
						t.Fatalf("recovered published occurrence failed: %+v", *header.Failure)
					}
					if header.Status == "completed" && header.EndedAt != nil && process.ActiveCount() == baseline {
						break
					}
					select {
					case <-wait.Done():
						t.Fatalf("recovered published occurrence did not settle: %+v leases=%d baseline=%d", header, process.ActiveCount(), baseline)
					case <-time.After(10 * time.Millisecond):
					}
				}
				child := checkpoint.ForkRun
				event := storetest.LoadCanonicalEventRecord(t, wait, selected, activityidentity.ForkLineageEventID(child, source.CurrentEventID))
				if event.Type() != "platform.join_complete" || !event.CreatedAt().Equal(source.CurrentDueAt) || event.SourceAgent() != genericschedule.OccurrenceProducerID() {
					t.Fatalf("recovery replaced its retained occurrence: %+v", event)
				}
				if count, err := storetest.ReadLifecycleEventCardinality(wait, selected, child, "platform.join_complete"); err != nil || count != 1 {
					t.Fatalf("recovery repeated publication: count=%d err=%v", count, err)
				}
				settlement, err := owner.ports.busDurable.DeliveryLifecycle.SummarizeRun(wait, child)
				if err != nil || settlement.Total != 1 || settlement.Delivered != 1 {
					t.Fatalf("recovery did not settle the exact retained delivery: %+v err=%v", settlement, err)
				}
				if storage := storetest.ObserveWorkflowTimerReplayStorage(t, wait, selected, child, child); storage.Timers != 0 {
					t.Fatalf("recovery rearmed accepted source work: %+v", storage)
				}
				requirePublishedJoinChildReceiver(t, wait, selected, owner, source, child)
				requirePublishedJoinNoChildTimers(t, wait, selected, child)
				retry, found, err := operations.LoadForkOperation(wait, acknowledged.Request.Actor, acknowledged.Request.IdempotencyKey, acknowledged.Request.TransportHash)
				if err != nil || !found || !reflect.DeepEqual(acknowledged, retry) {
					t.Fatalf("successor recovery changed permanent acknowledgment: %+v err=%v", retry, err)
				}
				after, err := storetest.ReadSelectedForkSourceDomain(wait, selected, checkpoint.SourceRun)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("published occurrence recovery changed source business facts: %v", err)
				}
			})
		}
	}
}

func TestIssue642PublishedJoinContinuesExactDeliveryBothStores(t *testing.T) {
	for _, fixture := range []struct{ backend, flowID string }{{"sqlite", ""}, {"sqlite", "orders"}, {"postgres", ""}, {"postgres", "orders"}} {
		backend, name := fixture.backend, fixture.backend
		if fixture.flowID != "" {
			name += "/" + fixture.flowID
		}
		t.Run(name, func(t *testing.T) {
			t.Setenv(publishedJoinFlowEnv, fixture.flowID)
			var selected startupownership.Store
			var owner SelectedContractExecutionOwner
			if backend == "sqlite" {
				s := storetest.StartSQLiteRuntimeStore(t)
				selected, owner = s, selectedContractSQLiteExecutionOwnerForTest(t, s)
			} else {
				_, db, _ := testutil.StartPostgres(t)
				s := storetest.AdmitPostgresRuntimeStore(t, db)
				selected, owner = s, selectedContractExecutionOwnerForTest(t, s)
			}
			ctx := runForkTestContext(t)
			process, _ := worklifetime.ProcessFromContext(ctx)
			baseline := process.ActiveCount()
			t.Cleanup(func() {
				if err := owner.RetireSelectedContexts(context.Background()); err != nil {
					t.Error(err)
				}
			})
			repo := runForkExecutionRepoRoot(t)
			loader := admittedFixtureSelectedContractSourceLoader{RepoRoot: repo, SourceRoot: publishedJoinFixture(t), PlatformSpecPath: filepath.Join(repo, "platform-spec.yaml")}
			loaded, err := loader.LoadRunForkSelectedContractSource(ctx, runfork.RunForkContractSelection{Mode: "selected_contracts"})
			if err != nil {
				t.Fatal(err)
			}
			ctx = correlation.WithSourceArtifactFact(ctx, loaded.SourceArtifactFact)
			scope, err := authoractivity.BundleScopeForTarget(ctx, loaded.SourceArtifactFact.BundleHash())
			if err != nil {
				t.Fatal(err)
			}
			ctx = authoractivity.WithScope(ctx, scope)
			descriptors, err := rootruntime.AuthorActivityEventDescriptors(loaded.Source)
			if err != nil {
				t.Fatal(err)
			}
			catalog, err := owner.ports.fork.RegisterAuthorActivityEventCatalog(scope, descriptors)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(catalog.Release)
			sourceRun := uuid.NewString()
			marker, source := seedPublishedJoinSource(t, ctx, selected, owner, loaded, sourceRun)
			plan, err := owner.ports.fork.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: sourceRun, At: marker})
			if err != nil || len(plan.JoinSchedules) != 1 || plan.JoinSchedules[0].CurrentEventID != source.CurrentEventID {
				t.Fatalf("published fixed cut: %+v err=%v", plan, err)
			}
			requirePublishedJoinSourceScope(t, source, fixture.flowID)
			sourceBefore, err := storetest.ReadSelectedForkSourceDomain(ctx, selected, sourceRun)
			if err != nil {
				t.Fatal(err)
			}
			selection := runforkadmission.SelectedContractSelection(loaded.Source)
			operation := runfork.ForkOperationRequest{OperationID: uuid.NewString(), Actor: "published-join", IdempotencyKey: "fixed-cut",
				TransportHash: "published-join-transport", SourceRunID: sourceRun, ForkEventID: marker,
				TargetBundleHash: loaded.SourceArtifactFact.BundleHash(), AllowSourceFreeze: true, ContractSelection: selection}
			result, err := ExecuteSelectedContractRunFork(ctx, SelectedContractExecutionRequest{SourceRunID: sourceRun, At: marker, AllowSourceFreeze: true,
				Owner: owner, ForkOperation: &operation, SourceLoader: loader, ContractSelection: selection,
				AgentRuntime: SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.MockOnly, ProcessCapability: owner.ports.contexts.capability}})
			if err != nil || !result.Activation.Activated {
				t.Fatalf("selected published-join activation: %+v err=%v", result, err)
			}
			child := result.Materialization.ForkRunID
			operations := selected.(interface {
				LoadForkOperation(context.Context, string, string, string) (runfork.ForkOperationRecord, bool, error)
			})
			acknowledged, found, err := operations.LoadForkOperation(ctx, operation.Actor, operation.IdempotencyKey, operation.TransportHash)
			if err != nil || !found || acknowledged.Status != runfork.ForkOperationActivated || acknowledged.ForkRunID != child || acknowledged.Result == nil {
				t.Fatalf("published join lost permanent activation acknowledgment: %+v found=%v err=%v", acknowledged, found, err)
			}
			wait, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			reader := selected.(interface {
				LoadRunHeader(context.Context, string) (operatorread.RunHeader, error)
			})
			for {
				header, err := reader.LoadRunHeader(wait, child)
				if err != nil {
					t.Fatal(err)
				}
				if header.Status == "completed" && header.EndedAt != nil && header.Failure == nil && process.ActiveCount() == baseline {
					break
				}
				if header.Failure != nil {
					owner.ports.contexts.mu.Lock()
					cleanupErr := owner.ports.contexts.cleanupErr
					owner.ports.contexts.mu.Unlock()
					logs, logErr := selected.(interface {
						ListOperatorRuntimeLogs(context.Context, operatorread.OperatorRuntimeLogListOptions) (operatorread.OperatorRuntimeLogListResult, error)
					}).ListOperatorRuntimeLogs(wait, operatorread.OperatorRuntimeLogListOptions{RunID: child, Limit: 100})
					t.Fatalf("published join continuation failed: %+v cause=%v logs=%+v logErr=%v", *header.Failure, cleanupErr, logs, logErr)
				}
				select {
				case <-wait.Done():
					t.Fatalf("published join child did not settle successfully: header=%+v leases=%d baseline=%d", header, process.ActiveCount(), baseline)
				case <-time.After(10 * time.Millisecond):
				}
			}
			if fixture.flowID == "" {
				childHeader, found, err := owner.ports.workflow.LoadWorkflowInstance(wait,
					flowidentity.RunScopedFlowInstance{RunID: child, Route: flowidentity.StoredRoute(".", child, child)})
				if err != nil || !found || childHeader.CurrentState != "ready" {
					t.Fatalf("exact retained join receiver: %+v found=%v err=%v", childHeader, found, err)
				}
			}
			requirePublishedJoinChildReceiver(t, wait, selected, owner, source, child)
			forkEvent := activityidentity.ForkLineageEventID(child, source.CurrentEventID)
			event := storetest.LoadCanonicalEventRecord(t, wait, selected, forkEvent)
			if event.Type() != "platform.join_complete" || !event.CreatedAt().Equal(source.CurrentDueAt) || event.SourceAgent() != genericschedule.OccurrenceProducerID() {
				t.Fatalf("published occurrence was replaced rather than continued: %+v", event)
			}
			if count, err := storetest.ReadLifecycleEventCardinality(wait, selected, child, "platform.join_complete"); err != nil || count != 1 {
				t.Fatalf("continued %d join occurrences, want exactly one: %v", count, err)
			}
			settlement, err := owner.ports.busDurable.DeliveryLifecycle.SummarizeRun(wait, child)
			if err != nil || settlement.Total != 1 || settlement.Delivered != 1 {
				t.Fatalf("retained join did not settle its exact child delivery: %+v err=%v", settlement, err)
			}
			if storage := storetest.ObserveWorkflowTimerReplayStorage(t, wait, selected, child, child); storage.Timers != 0 {
				t.Fatalf("already-published occurrence acquired a fresh timer: %+v", storage)
			}
			requirePublishedJoinNoChildTimers(t, wait, selected, child)
			original, found, err := selected.(genericschedule.Store).LoadGenericScheduleActivation(correlation.WithRunID(ctx, sourceRun), source.ID)
			if err != nil || !found {
				t.Fatal(err)
			}
			before, _ := source.EvidenceDigest()
			after, err := original.EvidenceDigest()
			if err != nil || before != after {
				t.Fatal("child execution changed original occurrence history")
			}
			sourceAfter, err := storetest.ReadSelectedForkSourceDomain(wait, selected, sourceRun)
			if err != nil || !reflect.DeepEqual(sourceBefore, sourceAfter) {
				t.Fatalf("published occurrence execution changed source business facts: %v", err)
			}
			retry, found, err := operations.LoadForkOperation(wait, operation.Actor, operation.IdempotencyKey, operation.TransportHash)
			if err != nil || !found || !reflect.DeepEqual(acknowledged, retry) {
				t.Fatalf("published occurrence settlement changed permanent acknowledgment: %+v err=%v", retry, err)
			}
		})
	}
}

func requirePublishedJoinSourceScope(t *testing.T, source genericschedule.Activation, flowID string) {
	t.Helper()
	if flowID == "" {
		flowID = "."
	}
	object, ok := source.Command.Payload.Interface().(map[string]any)
	if !ok {
		t.Fatal("source publication lacks an object join handle")
	}
	handle, ref, valid := timeridentity.ParseJoinHandle(object)
	if !valid || handle.Kind() != timeridentity.TimerHandleJoinComplete || ref.FlowPath() != flowID {
		t.Fatalf("published fixture did not construct its requested receiver scope %q: %+v", flowID, ref)
	}
	entry := ref.StageEntry()
	if entry.RunID != source.Command.RunID || entry.EntityID != source.Command.EntityID ||
		flowID == "." && (entry.InstancePath != source.Command.RunID || entry.EntityID != source.Command.RunID) ||
		flowID == "orders" && (entry.InstancePath != source.Command.FlowInstance || !strings.HasPrefix(entry.InstancePath, "orders/") || entry.EntityID == source.Command.RunID) {
		t.Fatalf("published fixture lost its exact constructed receiver: %+v", ref)
	}
}

func requirePublishedJoinChildReceiver(t *testing.T, ctx context.Context, selected any, owner SelectedContractExecutionOwner, source genericschedule.Activation, child string) {
	t.Helper()
	_, original, valid := timeridentity.ParseJoinHandle(source.Command.Payload.Interface().(map[string]any))
	if !valid {
		t.Fatal("source publication lost its exact join handle")
	}
	event := storetest.LoadCanonicalEventRecord(t, ctx, selected, activityidentity.ForkLineageEventID(child, source.CurrentEventID))
	var payload map[string]any
	if err := canonicaljson.DecodePreservingNumberLexemes(event.Payload(), &payload); err != nil {
		t.Fatal(err)
	}
	handle, ref, valid := timeridentity.ParseJoinHandle(payload)
	if !valid || handle.Kind() != timeridentity.TimerHandleJoinComplete || !ref.Declaration().Equal(original.Declaration()) {
		t.Fatal("child publication changed the original declaration or completion handle")
	}
	wantEntity, wantPath, wantInstance := original.StageEntry().EntityID, original.StageEntry().InstancePath, original.StageEntry().InstanceID
	if original.FlowPath() == "." {
		wantEntity, wantPath, wantInstance = child, child, child
	}
	entry, originalEntry := ref.StageEntry(), original.StageEntry()
	origin := originalEntry.OriginRunID
	if origin == "" {
		origin = originalEntry.RunID
	}
	if entry.RunID != child || entry.EntityID != wantEntity || entry.InstancePath != wantPath || entry.InstanceID != wantInstance || entry.OriginRunID != origin ||
		entry.FlowScope != originalEntry.FlowScope || entry.Stage != originalEntry.Stage || entry.Cause != originalEntry.Cause ||
		entry.EventID != originalEntry.EventID || entry.OccurrenceID != originalEntry.OccurrenceID || entry.TransitionID != originalEntry.TransitionID ||
		event.TaskID() != handle.TaskID() || event.TaskID() == source.Command.TaskID || event.ExecutionMode() != source.Command.ExecutionMode {
		t.Fatalf("child publication lost its exact source/receiver frame: %+v", ref)
	}
	lineage, retained := event.SelectedForkLineage()
	if !retained || lineage.SourceRunID() != source.Command.RunID || lineage.SourceEventID() != source.CurrentEventID ||
		lineage.AuthorityStamp() != runfork.RunForkSelectedContractExecutionOwner {
		t.Fatal("child publication lost its exact immediate lineage or selected authority")
	}
	target := events.RouteIdentity{FlowID: ref.FlowPath(), FlowInstance: wantPath, EntityID: wantEntity}
	workflow := flowidentity.RunScopedFlowInstance{RunID: child, Route: flowidentity.StoredRoute(ref.FlowPath(), flowidentity.LogicalInstanceID(wantPath), wantPath)}
	header, found, err := owner.ports.workflow.LoadWorkflowInstance(ctx, workflow)
	if err != nil || !found || header.CurrentState != "ready" || header.WorkflowName != ref.FlowPath() || header.EntityID != wantEntity || header.StorageRef != wantPath {
		t.Fatalf("exact retained join receiver: %+v found=%v err=%v", header, found, err)
	}
	if ref.FlowPath() == "orders" && (header.ParentFlowID != "." || header.ParentFlowInstance != child || header.ParentEntityID != child) {
		t.Fatalf("orders receiver lost its constructed child root parent: %+v", header)
	}
	joins, err := joinruntime.List(header.StateBuckets)
	if err != nil || len(joins) != 1 || !joins[0].JoinRef().Equal(ref) || joins[0].Status != joinruntime.StatusClosed ||
		joins[0].OutcomePending || !joins[0].OutcomeFired || joins[0].TransferredPublication == nil {
		t.Fatalf("receiver did not consume its exact retained completion: %+v err=%v", joins, err)
	}
	publication := joins[0].TransferredPublication
	if publication.EventID != event.ID() || publication.SourceRunID != source.Command.RunID || publication.SourceEventID != source.CurrentEventID ||
		publication.AuthorityStamp != lineage.AuthorityStamp() || publication.ExecutionMode != event.ExecutionMode() {
		t.Fatalf("settled receiver lost its immutable transferred publication: %+v", publication)
	}
	route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(ref.Node()), Target: events.MustExistingEntityTarget(target)}
	id, err := deliverylifecycle.DeliveryID(event.ID(), route)
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := owner.ports.busDurable.DeliveryLifecycle.Snapshot(ctx, id)
	if err != nil || delivery.Status != deliverylifecycle.StatusDelivered || delivery.RunID != child || delivery.EventID != event.ID() ||
		delivery.SubscriberClass != deliverylifecycle.SubscriberNode || delivery.SubscriberID != ref.Node().Key() ||
		delivery.Route.Recipient != route.Recipient || !delivery.Route.Target.ExistingEntity() || !events.SameRouteIdentity(delivery.Route.Target.Route(), target) {
		t.Fatalf("exact child receiver delivery did not settle: %+v err=%v", delivery, err)
	}
}

func requirePublishedJoinNoChildTimers(t *testing.T, ctx context.Context, selected any, child string) {
	t.Helper()
	// Include terminal and generic rows; an ordinary-timer or active-only count
	// cannot exclude a fresh join arm that already fired or was canceled.
	snapshot, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, selected)
	if err != nil {
		t.Fatal(err)
	}
	timers, found := snapshot["timers"]
	if !found {
		t.Fatal("native timer census is absent")
	}
	runColumn := -1
	for i, column := range timers.Columns {
		if column == "run_id" {
			runColumn = i
		}
	}
	if runColumn < 0 {
		t.Fatal("native timer census lacks its run owner")
	}
	for _, row := range timers.Rows {
		var values []any
		if err := json.Unmarshal([]byte(row), &values); err != nil {
			t.Fatal(err)
		}
		if len(values) != len(timers.Columns) {
			t.Fatal("native timer census row contradicts its columns")
		}
		if values[runColumn] == child {
			t.Fatalf("already-published child acquired a physical timer: %s", row)
		}
	}
}
