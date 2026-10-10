package runforkexecution

import (
	"context"
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
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deadletters"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
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

func publishedJoinFixture(t *testing.T) string {
	t.Helper()
	root := canonicalrouting.CopyExactJoinEventBusProof(t, "")
	path := filepath.Join(root, "nodes.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data = []byte(strings.ReplaceAll(string(data), "        deadline: {after: 1h, from: stage_entry}\n        on_deadline: {advances_to: attention}\n", ""))
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
	command := selectedExecutionSourceFlowCommand(t, ctx, loaded, event, identity)
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
	marker := eventtest.ExistingRunRootIngressWithRoutingSourceAndMode(uuid.NewString(), "item.completed", "source-runtime", "",
		event.Payload(), 0, runID, events.EventEnvelope{}, events.NoRoutingSource(), at.Add(time.Second), executionmode.Mock)
	storetest.CommitSemanticEventWithRoutes(t, ctx, selected, marker, nil, pipelineobligation.ScopeSubscribed)
	storetest.CaptureRunForkSnapshot(t, ctx, selected, runID)
	return marker.ID(), activation
}

func TestIssue642PublishedJoinCrashRestartBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, cut := range []string{"retained_join_after_activation", "retained_join_event_committed"} {
			t.Run(backend+"/"+cut, func(t *testing.T) {
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
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
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
			childHeader, found, err := owner.ports.workflow.LoadWorkflowInstance(wait,
				flowidentity.RunScopedFlowInstance{RunID: child, Route: flowidentity.StoredRoute(".", child, child)})
			if err != nil || !found || childHeader.CurrentState != "ready" {
				t.Fatalf("exact retained join receiver: %+v found=%v err=%v", childHeader, found, err)
			}
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
			original, found, err := selected.(genericschedule.Store).LoadGenericScheduleActivation(correlation.WithRunID(ctx, sourceRun), source.ID)
			if err != nil || !found {
				t.Fatal(err)
			}
			before, _ := source.EvidenceDigest()
			after, err := original.EvidenceDigest()
			if err != nil || before != after {
				t.Fatal("child execution changed original occurrence history")
			}
		})
	}
}
