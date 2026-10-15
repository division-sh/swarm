package runtime_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	swarmruntime "github.com/division-sh/swarm/internal/runtime"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	llm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/startuphandoff"
	runtimetimerobligation "github.com/division-sh/swarm/internal/runtime/timerobligation"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil/flowactivationfixture"
	"github.com/google/uuid"
)

type workflowTimerStartupStore interface {
	storetest.RunFixtureStore
	externalRuntimeTestDurableEventStore
	runtimepipeline.WorkflowPersistenceOwner
	swarmruntime.EventPayloadAdmissionBinder
	swarmruntime.AuthorActivityCatalogRegistrar
	runtimerunlifecycle.CandidateOwner
	runtimegenericschedule.Store
	storetest.AgentFixtureStore
	runtimemanager.ManagerPersistence
	runtimemanager.AgentLifecycleCellCensus
	runtimetimerobligation.Reader
	PipelineObligations() runtimepipelineobligation.Store
}

func workflowTimerStartupBackends() []struct {
	name string
	open func(*testing.T) workflowTimerStartupStore
} {
	return []struct {
		name string
		open func(*testing.T) workflowTimerStartupStore
	}{
		{name: "sqlite", open: func(t *testing.T) workflowTimerStartupStore { return storetest.StartSQLiteRuntimeStore(t) }},
		{name: "postgres", open: func(t *testing.T) workflowTimerStartupStore { return storetest.StartPostgresRuntimeStore(t) }},
	}
}

type workflowTimerStartupFlakyManagerStore struct {
	workflowTimerStartupStore

	mu     sync.Mutex
	loads  int
	failAt int
}

type workflowTimerStartupTopologyFailureOwner struct {
	runtimepipeline.WorkflowPersistenceOwner
	readiness runtimepipeline.DynamicFlowRuntimeReadiness
}

func (o workflowTimerStartupTopologyFailureOwner) InspectDynamicFlowRuntimeReadinessForSource(_ context.Context, source runtimecorrelation.SourceArtifactFact) (runtimepipeline.DynamicFlowRuntimeReadinessProjection, error) {
	if !o.readiness.OwningRunSource.Matches(source) {
		return runtimepipeline.DynamicFlowRuntimeReadinessProjection{}, nil
	}
	return runtimepipeline.DynamicFlowRuntimeReadinessProjection{
		CurrentPending: []runtimepipeline.DynamicFlowRuntimeReadiness{o.readiness},
	}, nil
}

func (o workflowTimerStartupTopologyFailureOwner) InspectDynamicFlowRuntimeReadinessForRun(_ context.Context, runID string, source runtimecorrelation.SourceArtifactFact) ([]runtimepipeline.DynamicFlowRuntimeReadiness, error) {
	if strings.TrimSpace(runID) != o.readiness.Plan.RunID || !o.readiness.OwningRunSource.Matches(source) {
		return nil, nil
	}
	return []runtimepipeline.DynamicFlowRuntimeReadiness{o.readiness}, nil
}

func (o workflowTimerStartupTopologyFailureOwner) LoadDynamicFlowRuntimeReadiness(_ context.Context, runID string, route runtimeflowidentity.Route) (runtimepipeline.DynamicFlowRuntimeReadiness, bool, error) {
	if strings.TrimSpace(runID) != o.readiness.Plan.RunID || route.InstancePath != o.readiness.InstancePath {
		return runtimepipeline.DynamicFlowRuntimeReadiness{}, false, nil
	}
	return o.readiness, true, nil
}

func (s *workflowTimerStartupFlakyManagerStore) LoadAgents(ctx context.Context) ([]runtimemanager.PersistedAgent, error) {
	s.mu.Lock()
	s.loads++
	load := s.loads
	s.mu.Unlock()
	if load == s.failAt {
		return nil, errors.New("transient manager hydration failure")
	}
	return s.workflowTimerStartupStore.LoadAgents(ctx)
}

func TestGenericScheduleLifecyclePublishesOneShotAndRecurringThroughWorkflowRuntimeOnBothStores(t *testing.T) {
	for _, backend := range workflowTimerStartupBackends() {
		t.Run(backend.name, func(t *testing.T) {
			selected := backend.open(t)
			workflowPersistence := runtimepipeline.NewWorkflowPersistence(selected)
			runID := uuid.NewString()
			entityID := uuid.NewString()
			bundle, sourceFact := workflowTimerStartupRecoverySource(t, "25ms")
			ctx := runtimecorrelation.WithRunID(workflowTimerStartupContext(sourceFact), runID)
			storetest.RequireRun(t, ctx, selected, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runID, Artifact: bundle.SourceArtifact})

			proof := startuphandoff.New()
			defer proof.Release()
			process := worklifetime.NewProcess()
			source := semanticview.Wrap(bundle)
			rt, err := swarmruntime.NewRuntime(ctx, completeExternalRuntimeTestWorkflowDeps(t, selected, swarmruntime.RuntimeDeps{
				Config: &config.Config{
					Runtime: config.RuntimeConfig{RecoveryOnStartup: true},
					LLM:     config.LLMConfig{Backend: "anthropic"},
				},

				EventStore:                  selected,
				EventBusDurable:             externalRuntimeTestDurableDependencies(selected),
				EventPayloadAdmissionBinder: selected,
				AuthorActivityRegistrars:    []swarmruntime.AuthorActivityCatalogRegistrar{selected},
				RunLifecycleCandidates:      selected,
				GenericScheduleStore:        selected,
				TimerObligationReader:       selected,
				WorkflowPersistence:         workflowPersistence,
				ManagerStore:                selected,
				ManagerPersistenceRoles:     externalRuntimeTestSelectedManagerRoles(selected),
				DeliveryStore:               proof.DeliveryStore(selected),
				PipelineObligations:         proof.RecoveryStore(selected.PipelineObligations()),

				Options: swarmruntime.RuntimeOptions{
					SelfCheck:          false,
					WorkflowModule:     newRuntimeTestWorkflowModule(t, source),
					LLMRuntime:         workflowTimerStartupLLM{},
					RuntimeInstanceID:  authorActivityTestRuntimeInstanceID,
					SourceArtifactFact: sourceFact,
					ProcessWorkOwner:   process,
					TestLifecycleProbe: proof,
					BootProgress: func(event swarmruntime.BootProgressEvent) {
						if event.Step == 16 && event.Status == "ok" {
							proof.MarkContinuationsSettled()
						}
					},
				},
			}))
			if err != nil {
				t.Fatalf("NewRuntime: %v", err)
			}
			capability, _ := installExternalRuntimeTestGeneration(t, ctx, selected, rt)
			t.Cleanup(func() {
				if err := closeExternalRuntimeTestGeneration(rt, process, capability); err != nil {
					t.Errorf("close workflow timer generation: %v", err)
				}
			})
			routingSource, err := events.NewRootRoutingSource(entityID)
			if err != nil {
				t.Fatalf("build generic schedule routing source: %v", err)
			}
			baseCommand := runtimegenericschedule.AdmissionCommand{
				RunID:         runID,
				OwnerID:       "runtime",
				OwnerKind:     runtimegenericschedule.OwnerAgent,
				AgentIdentity: agentidentitytest.RootRuntimeForRun(t, runID, "runtime", "generic-occurrence-proof"),
				EventType:     "generic.tick",
				EntityID:      entityID,
				Payload:       semanticvalue.EmptyObject(),
				RoutingSource: routingSource,
			}
			// Native admission without lifecycle projection leaves this exact due
			// occurrence dormant until Runtime.Start restores it after both handoffs.
			dueCommand := baseCommand
			dueCommand.ScheduleKey, dueCommand.TaskID = "overdue-control", "overdue-control"
			dueCommand.ExecutionMode = executionmode.Live
			dueCommand.Due = runtimegenericschedule.AbsoluteDue(time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond))
			seed, err := selected.AdmitGenericScheduleOutcome(ctx, dueCommand)
			if err != nil || !seed.Acknowledged {
				t.Fatalf("seed native overdue control: %+v err=%v", seed, err)
			}
			original := seed.Result.Activation
			eventID := runtimegenericschedule.OccurrenceEventID(original.ID, original.CurrentDueAt)
			proof.WatchID(eventID)
			startCtx, cancelStart := context.WithCancel(ctx)
			defer cancelStart()
			started := make(chan error, 1)
			go func() { started <- rt.Start(startCtx) }()
			witnessCtx, witnessCancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer witnessCancel()
			for _, barrier := range []*startuphandoff.Barrier{proof.Recovery, proof.Continuation} {
				if err := proof.Wait(witnessCtx, barrier); err != nil {
					t.Fatal(err)
				}
				current, found, err := selected.LoadGenericScheduleActivation(ctx, original.ID)
				if err != nil || !found || current.ID != original.ID || current.ImmutableHash != original.ImmutableHash || !current.CurrentDueAt.Equal(original.CurrentDueAt) || current.Status != runtimegenericschedule.StatusActive || !current.CurrentDueAt.Before(time.Now()) {
					t.Fatalf("held handoff changed overdue control: before=%+v after=%+v found=%t err=%v", original, current, found, err)
				}
				barrier.Release()
			}
			select {
			case err := <-started:
				if err != nil {
					t.Fatalf("Start runtime: %v", err)
				}
			case <-witnessCtx.Done():
				t.Fatal(witnessCtx.Err())
			}
			select {
			case signal := <-proof.Published:
				if signal.EventID != eventID {
					t.Fatalf("overdue control changed occurrence: %+v", signal)
				}
			case <-witnessCtx.Done():
				t.Fatal("overdue control never published after both handoffs")
			}
			if err := rt.Scheduler.Wait(witnessCtx); err != nil {
				t.Fatal(err)
			}
			if err := proof.CheckPublished(eventID); err != nil {
				t.Fatal(err)
			}
			settled, found, err := selected.LoadGenericScheduleActivation(ctx, original.ID)
			if err != nil || !found || settled.Status != runtimegenericschedule.StatusFired || settled.ID != original.ID || !settled.CurrentDueAt.Equal(original.CurrentDueAt) {
				t.Fatalf("overdue control lost exact settlement: %+v found=%t err=%v", settled, found, err)
			}
			if evidence := storetest.ReadSemanticEventFixtureEvidence(t, ctx, selected, runID, eventID); !evidence.RecordFound || evidence.RunEventCount != 1 || evidence.PipelineReceiptCount != 1 {
				t.Fatalf("overdue control lacks exact native publication: %+v", evidence)
			}
			proveOneShot := func(mode executionmode.Mode) {
				t.Helper()
				command := baseCommand
				command.ScheduleKey = "generic-occurrence-proof-" + string(mode)
				command.TaskID = command.ScheduleKey
				command.ExecutionMode = mode
				command.Due = runtimegenericschedule.AbsoluteDue(time.Now().UTC().Add(50 * time.Millisecond).Truncate(time.Microsecond))
				admitted, err := rt.GenericSchedules.Admit(ctx, command)
				if err != nil {
					t.Fatalf("admit %s generic occurrence-shaped schedule: %v", mode, err)
				}

				deadline := time.Now().Add(5 * time.Second)
				for {
					published, err := storetest.ReadGenericSchedulePublicationModes(ctx, selected, command.TaskID)
					if err != nil {
						t.Fatalf("read %s generic scheduled event: %v", mode, err)
					}
					eventCount, minMode, maxMode := published.Count, published.MinMode, published.MaxMode
					activation, found, err := selected.LoadGenericScheduleActivation(ctx, admitted.Activation.ID)
					if err != nil {
						t.Fatalf("load %s generic schedule activation: %v", mode, err)
					}
					if eventCount == 1 && minMode == string(mode) && maxMode == string(mode) &&
						found && activation.Status == runtimegenericschedule.StatusFired && activation.Command.ExecutionMode == mode {
						break
					}
					if time.Now().After(deadline) {
						t.Fatalf("%s generic occurrence-shaped fire did not publish and settle: events=%d modes=%q/%q activation=%#v found=%v", mode, eventCount, minMode, maxMode, activation, found)
					}
					time.Sleep(20 * time.Millisecond)
				}
			}
			proveOneShot(executionmode.Live)
			proveOneShot(executionmode.Mock)

			recurringCommand := baseCommand
			recurringCommand.ScheduleKey = "generic-recurring-proof"
			recurringCommand.TaskID = recurringCommand.ScheduleKey
			recurringCommand.ExecutionMode = executionmode.Mock
			recurringCommand.Due = runtimegenericschedule.EveryDue(40 * time.Millisecond)
			recurring, err := rt.GenericSchedules.Admit(ctx, recurringCommand)
			if err != nil {
				t.Fatalf("admit recurring generic schedule: %v", err)
			}

			countEvents := func() (int, string, string) {
				t.Helper()
				published, err := storetest.ReadGenericScheduleDistinctPublicationModes(ctx, selected, recurringCommand.TaskID)
				if err != nil {
					t.Fatalf("read recurring generic schedule events: %v", err)
				}
				return published.Count, published.MinMode, published.MaxMode
			}

			deadline := time.Now().Add(5 * time.Second)
			for {
				activation, found, err := selected.LoadGenericScheduleActivation(ctx, recurring.Activation.ID)
				if err != nil {
					t.Fatalf("load recurring generic schedule activation: %v", err)
				}
				count, minMode, maxMode := countEvents()
				if count >= 2 && minMode == string(executionmode.Mock) && maxMode == string(executionmode.Mock) && found &&
					activation.Status == runtimegenericschedule.StatusActive && activation.Command.ExecutionMode == executionmode.Mock &&
					activation.CurrentDueAt.After(activation.InitialDueAt) {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("recurring generic schedule did not advance through exact mock occurrences: count=%d modes=%q/%q activation=%#v found=%v", count, minMode, maxMode, activation, found)
				}
				time.Sleep(20 * time.Millisecond)
			}

			cancelled, err := rt.GenericSchedules.Cancel(ctx, runtimegenericschedule.CancelCommand{
				ActivationID: recurring.Activation.ID,
				Cause:        "test_completed",
				CancelledAt:  time.Now().UTC(),
			})
			if err != nil || cancelled.Outcome != runtimegenericschedule.CancelChanged {
				t.Fatalf("cancel recurring generic schedule: result=%#v err=%v", cancelled, err)
			}
			settledCount, _, _ := countEvents()
			time.Sleep(120 * time.Millisecond)
			if after, _, _ := countEvents(); after != settledCount {
				t.Fatalf("recurring generic schedule published after cancellation: before=%d after=%d", settledCount, after)
			}
		})
	}
}

func TestRuntimeStartWithholdsDueSchedulesAndTimersUntilDynamicTopologyCompletesOnBothStores(t *testing.T) {
	for _, backend := range workflowTimerStartupBackends() {
		t.Run(backend.name, func(t *testing.T) {
			selected := backend.open(t)
			workflowRunID := uuid.NewString()
			genericRunID := uuid.NewString()
			genericEntityID := uuid.NewString()
			bundle, sourceFact := workflowTimerStartupRecoverySource(t, "1s")
			ctx := workflowTimerStartupContext(sourceFact)
			workflowCtx := runtimecorrelation.WithRunID(ctx, workflowRunID)
			genericCtx := runtimecorrelation.WithRunID(ctx, genericRunID)
			for _, run := range []struct {
				ctx   context.Context
				runID string
			}{{workflowCtx, workflowRunID}, {genericCtx, genericRunID}} {
				storetest.RequireRun(t, run.ctx, selected, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: run.runID, Artifact: bundle.SourceArtifact})
			}

			source := semanticview.Wrap(bundle)
			module := newRuntimeTestWorkflowModule(t, source)
			newRuntime := func(owner runtimepipeline.WorkflowPersistenceOwner) (*swarmruntime.Runtime, *worklifetime.Process) {
				t.Helper()
				process := worklifetime.NewProcess()
				rt, err := swarmruntime.NewRuntime(ctx, completeExternalRuntimeTestWorkflowDeps(t, selected, swarmruntime.RuntimeDeps{
					Config: &config.Config{
						Runtime: config.RuntimeConfig{RecoveryOnStartup: true},
						LLM:     config.LLMConfig{Backend: "anthropic"},
					},
					EventStore:                  selected,
					EventBusDurable:             externalRuntimeTestDurableDependencies(selected),
					EventPayloadAdmissionBinder: selected,
					AuthorActivityRegistrars:    []swarmruntime.AuthorActivityCatalogRegistrar{selected},
					RunLifecycleCandidates:      selected,
					GenericScheduleStore:        selected,
					TimerObligationReader:       selected,
					WorkflowPersistence:         runtimepipeline.NewWorkflowPersistence(owner),
					ManagerStore:                selected,
					ManagerPersistenceRoles:     externalRuntimeTestSelectedManagerRoles(selected),
					DeliveryStore:               selected,
					PipelineObligations:         selected.PipelineObligations(),
					Options: swarmruntime.RuntimeOptions{
						SelfCheck:          false,
						WorkflowModule:     module,
						LLMRuntime:         workflowTimerStartupLLM{},
						RuntimeInstanceID:  authorActivityTestRuntimeInstanceID,
						SourceArtifactFact: sourceFact,
						ProcessWorkOwner:   process,
					},
				}))
				if err != nil {
					t.Fatalf("NewRuntime: %v", err)
				}
				return rt, process
			}
			closeRuntime := func(label string, rt *swarmruntime.Runtime, process *worklifetime.Process, capability runtimestartupownership.ProcessCapability) {
				t.Helper()
				if err := closeExternalRuntimeTestGeneration(rt, process, capability); err != nil {
					t.Fatalf("close %s runtime: %v", label, err)
				}
			}

			seedRuntime, seedProcess := newRuntime(selected)
			seedCtx := testLiveExecutionContext(worklifetime.WithRuntimeOccurrence(workflowCtx, seedRuntime.WorkOccurrence()))
			occurredAt := time.Now().UTC()
			construction383Ctx := seedCtx
			construction383At := occurredAt
			rootSchema, declared := source.FlowSchemaByID(".")
			if !declared || !rootSchema.Instance.Empty() {
				t.Fatal("timer fixture requires its compiled keyless root declaration")
			}
			construction383Instance, construction383Lifecycle, err := seedRuntime.Pipeline.PrepareInitialEntryLifecycle(construction383Ctx, runtimeflowidentity.RunScopedFlowInstance{RunID: workflowRunID, Route: runtimeflowidentity.StoredRoute(".", workflowRunID, workflowRunID)}, runtimepipeline.WorkflowInstance{
				InstanceID: workflowRunID, StorageRef: workflowRunID,
				WorkflowName: ".", WorkflowVersion: source.WorkflowVersion(), CurrentState: "waiting",
				Mode:       rootSchema.EffectiveMode(),
				Fields:     map[string]any{},
				EntityType: "test_entity",
			}, construction383At)
			if err != nil {
				t.Fatalf("prepare fixture initial lifecycle: %v", err)
			}
			construction383Command, err := flowactivationfixture.Command(construction383Ctx, construction383Instance, construction383Lifecycle, construction383At)
			if err != nil {
				t.Fatalf("prepare fixture activation command: %v", err)
			}
			result, err := any(selected).(runtimebus.FlowInstanceActivationCommitOwner).CommitFlowInstanceActivation(construction383Ctx, construction383Command)
			if err == nil && !result.Acknowledged {
				t.Fatal("fixture activation was not acknowledged")
			}
			if result.Acknowledged && result.Created {
				if finalizeErr := seedRuntime.Pipeline.FinalizeInitialEntryLifecycle(construction383Ctx, result.Lifecycle); finalizeErr != nil {
					t.Fatalf("finalize fixture initial lifecycle: %v", finalizeErr)
				}
			}
			if err != nil || !result.Created {
				t.Fatalf("materialize withheld workflow timer: result=%v err=%v", result, err)
			}
			closeRuntime("seed", seedRuntime, seedProcess, nil)
			dueAt := occurredAt.Add(time.Second)

			routingSource, err := events.NewRootRoutingSource(genericEntityID)
			if err != nil {
				t.Fatal(err)
			}
			genericCommand := runtimegenericschedule.AdmissionCommand{
				RunID: genericRunID, ScheduleKey: "topology-latch-generic", TaskID: "topology-latch-generic",
				OwnerID: "runtime", OwnerKind: runtimegenericschedule.OwnerAgent,
				AgentIdentity: agentidentitytest.RootRuntimeForRun(t, genericRunID, "runtime", "topology-latch-generic"),
				EventType:     "generic.tick", EntityID: genericEntityID, Payload: semanticvalue.EmptyObject(),
				RoutingSource: routingSource, Due: runtimegenericschedule.AbsoluteDue(dueAt), ExecutionMode: executionmode.Mock,
			}
			genericCommit, err := selected.AdmitGenericScheduleOutcome(genericCtx, genericCommand)
			if err != nil || !genericCommit.Acknowledged {
				t.Fatalf("persist overdue generic schedule: commit=%+v err=%v", genericCommit, err)
			}
			genericAdmission := genericCommit.Result
			time.Sleep(time.Until(dueAt) + 50*time.Millisecond)
			countGenericEvents := func() int {
				t.Helper()
				count, err := storetest.CountGenericSchedulePublications(genericCtx, selected, genericCommand.TaskID)
				if err != nil {
					t.Fatalf("count generic schedule events: %v", err)
				}
				return count
			}

			bundleHash := sourceFact.BundleHash()
			failureOwner := workflowTimerStartupTopologyFailureOwner{
				WorkflowPersistenceOwner: selected,
				readiness: runtimepipeline.DynamicFlowRuntimeReadiness{
					InstancePath: "review/inst-1", OwningRunSource: sourceFact,
					RunStatus: "running", InstanceStatus: "active",
					Plan: runtimepipeline.DynamicFlowRuntimeReadinessPlan{
						RunID: workflowRunID, BundleHash: bundleHash,
						WorkflowVersion: source.WorkflowVersion() + "-stale", ExecutionMode: executionmode.Live,
						Identity: runtimeflowidentity.Instance{
							TemplateID: "review", ScopeKey: "review", InstanceID: "inst-1",
							InstancePath: "review/inst-1", EntityID: uuid.NewString(), HasStoredPath: true,
						},
					},
				},
			}
			failedRuntime, failedProcess := newRuntime(failureOwner)
			failedCapability, _ := installExternalRuntimeTestGeneration(t, ctx, selected, failedRuntime)
			err = failedRuntime.Start(ctx)
			if err == nil || !strings.Contains(err.Error(), "workflow version changed") {
				closeRuntime("unexpected successful topology-failure", failedRuntime, failedProcess, failedCapability)
				t.Fatalf("Start error = %v, want dynamic topology completion failure", err)
			}
			closeRuntime("failed topology", failedRuntime, failedProcess, failedCapability)
			time.Sleep(100 * time.Millisecond)
			instance, found, err := failedRuntime.Pipeline.Load(workflowCtx, runtimeflowidentity.RunScopedFlowInstance{
				RunID: workflowRunID,
				Route: runtimeflowidentity.StoredRoute(".", workflowRunID, workflowRunID),
			})
			if err != nil || !found || instance.CurrentState != "waiting" {
				t.Fatalf("workflow timer crossed failed topology latch: found=%v state=%q err=%v", found, instance.CurrentState, err)
			}
			activation, found, err := selected.LoadGenericScheduleActivation(genericCtx, genericAdmission.Activation.ID)
			if err != nil || !found || activation.Status != runtimegenericschedule.StatusActive || countGenericEvents() != 0 {
				t.Fatalf("generic schedule crossed failed topology latch: found=%v activation=%#v events=%d err=%v", found, activation, countGenericEvents(), err)
			}

			for _, interruption := range []string{"publication_abort", "standing_finalization_failure"} {
				interruptedRuntime, interruptedProcess := newRuntime(selected)
				interruptedCapability, _ := installExternalRuntimeTestGeneration(t, ctx, selected, interruptedRuntime)
				release, err := interruptedRuntime.PrepareStart(ctx)
				if err != nil {
					t.Fatalf("prepare %s: %v", interruption, err)
				}
				if interruption == "publication_abort" {
					closeRuntime(interruption, interruptedRuntime, interruptedProcess, interruptedCapability)
					if err := release.Start(nil); err == nil {
						t.Fatal("aborted prepared runtime admitted work")
					}
				} else {
					cause := errors.New("injected standing finalization failure")
					if err := release.Start(func() error { return cause }); !errors.Is(err, cause) {
						t.Fatalf("finalization error lost: %v", err)
					}
					closeRuntime(interruption, interruptedRuntime, interruptedProcess, interruptedCapability)
				}
				if interruptedRuntime.Manager.IsRunning() || countGenericEvents() != 0 {
					t.Fatalf("%s leaked executable work", interruption)
				}
				waiting, found, err := interruptedRuntime.Pipeline.Load(workflowCtx, runtimeflowidentity.RunScopedFlowInstance{RunID: workflowRunID, Route: runtimeflowidentity.StoredRoute(".", workflowRunID, workflowRunID)})
				if err != nil || !found || waiting.CurrentState != "waiting" {
					t.Fatalf("%s released workflow timer: found=%v state=%s err=%v", interruption, found, waiting.CurrentState, err)
				}
			}

			recoveredRuntime, recoveredProcess := newRuntime(selected)
			recoveredCapability, _ := installExternalRuntimeTestGeneration(t, ctx, selected, recoveredRuntime)
			release, err := recoveredRuntime.PrepareStart(ctx)
			if err != nil {
				closeRuntime("failed preparation", recoveredRuntime, recoveredProcess, recoveredCapability)
				t.Fatalf("prepare after completed topology: %v", err)
			}
			if recoveredRuntime.Manager.IsRunning() {
				t.Fatal("preparation admitted manager execution before publication")
			}
			time.Sleep(100 * time.Millisecond)
			if countGenericEvents() != 0 {
				t.Fatal("prepared runtime released an overdue generic schedule")
			}
			beforeRelease, _, err := recoveredRuntime.Pipeline.Load(workflowCtx, runtimeflowidentity.RunScopedFlowInstance{
				RunID: workflowRunID, Route: runtimeflowidentity.StoredRoute(".", workflowRunID, workflowRunID),
			})
			if err != nil || beforeRelease.CurrentState != "waiting" {
				t.Fatalf("prepared runtime released an overdue workflow timer: state=%s err=%v", beforeRelease.CurrentState, err)
			}
			if err := release.Start(nil); err != nil {
				closeRuntime("failed recovery", recoveredRuntime, recoveredProcess, recoveredCapability)
				t.Fatalf("Start after completed topology: %v", err)
			}
			if err := release.Start(nil); err == nil {
				t.Fatal("prepared startup was released twice")
			}
			defer closeRuntime("recovered", recoveredRuntime, recoveredProcess, recoveredCapability)
			deadline := time.Now().Add(8 * time.Second)
			for {
				instance, instanceFound, loadErr := recoveredRuntime.Pipeline.Load(workflowCtx, runtimeflowidentity.RunScopedFlowInstance{
					RunID: workflowRunID,
					Route: runtimeflowidentity.StoredRoute(".", workflowRunID, workflowRunID),
				})
				activation, activationFound, activationErr := selected.LoadGenericScheduleActivation(genericCtx, genericAdmission.Activation.ID)
				if loadErr == nil && instanceFound && instance.CurrentState == "done" && activationErr == nil && activationFound &&
					activation.Status == runtimegenericschedule.StatusFired && countGenericEvents() == 1 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("post-topology producers did not converge: instance=%#v found=%v load_err=%v activation=%#v found=%v activation_err=%v events=%d", instance, instanceFound, loadErr, activation, activationFound, activationErr, countGenericEvents())
				}
				time.Sleep(20 * time.Millisecond)
			}
		})
	}
}

func TestRuntimeStartFailsClosedWhenManagerHydrationWouldWithholdWorkflowTimersOnBothStores(t *testing.T) {
	for _, backend := range workflowTimerStartupBackends() {
		t.Run(backend.name, func(t *testing.T) {
			selected := backend.open(t)
			workflowPersistence := runtimepipeline.NewWorkflowPersistence(selected)
			runID := uuid.NewString()
			bundle, sourceFact := workflowTimerStartupRecoverySource(t, "25ms")
			ctx := runtimecorrelation.WithRunID(workflowTimerStartupContext(sourceFact), runID)
			storetest.RequireRun(t, ctx, selected, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runID, Artifact: bundle.SourceArtifact})

			source := semanticview.Wrap(bundle)
			module := newRuntimeTestWorkflowModule(t, source)
			newRuntime := func(managerStore runtimemanager.ManagerPersistence) (*swarmruntime.Runtime, *worklifetime.Process) {
				process := worklifetime.NewProcess()
				rt, err := swarmruntime.NewRuntime(ctx, completeExternalRuntimeTestWorkflowDeps(t, selected, swarmruntime.RuntimeDeps{
					Config: &config.Config{
						Runtime: config.RuntimeConfig{RecoveryOnStartup: true},
						LLM:     config.LLMConfig{Backend: "anthropic"},
					},

					EventStore:                  selected,
					EventBusDurable:             externalRuntimeTestDurableDependencies(selected),
					EventPayloadAdmissionBinder: selected,
					AuthorActivityRegistrars:    []swarmruntime.AuthorActivityCatalogRegistrar{selected},
					RunLifecycleCandidates:      selected,
					WorkflowPersistence:         workflowPersistence,
					ManagerStore:                managerStore,
					ManagerPersistenceRoles:     externalRuntimeTestSelectedManagerRoles(selected),
					TimerObligationReader:       selected,
					DeliveryStore:               selected,
					PipelineObligations:         selected.PipelineObligations(),

					Options: swarmruntime.RuntimeOptions{
						SelfCheck:          false,
						WorkflowModule:     module,
						LLMRuntime:         workflowTimerStartupLLM{},
						RuntimeInstanceID:  authorActivityTestRuntimeInstanceID,
						SourceArtifactFact: sourceFact,
						ProcessWorkOwner:   process,
					},
				}))
				if err != nil {
					t.Fatalf("NewRuntime: %v", err)
				}
				return rt, process
			}
			shutdown := func(label string, rt *swarmruntime.Runtime, process *worklifetime.Process) {
				t.Helper()
				if err := closeExternalRuntimeTestGeneration(rt, process, nil); err != nil {
					t.Fatalf("close %s runtime: %v", label, err)
				}
			}

			seedRuntime, seedProcess := newRuntime(selected)
			seedCtx := testLiveExecutionContext(worklifetime.WithRuntimeOccurrence(ctx, seedRuntime.WorkOccurrence()))
			construction620Ctx := seedCtx
			construction620At := time.Now().UTC()
			rootSchema, declared := source.FlowSchemaByID(".")
			if !declared || !rootSchema.Instance.Empty() {
				t.Fatal("timer fixture requires its compiled keyless root declaration")
			}
			construction620Instance, construction620Lifecycle, err := seedRuntime.Pipeline.PrepareInitialEntryLifecycle(construction620Ctx, runtimeflowidentity.RunScopedFlowInstance{RunID: runID, Route: runtimeflowidentity.StoredRoute(".", runID, runID)}, runtimepipeline.WorkflowInstance{
				InstanceID:      runID,
				StorageRef:      runID,
				WorkflowName:    ".",
				WorkflowVersion: source.WorkflowVersion(),
				Mode:            rootSchema.EffectiveMode(),
				CurrentState:    "waiting",
				Fields:          map[string]any{},
				EntityType:      "test_entity",
			}, construction620At)
			if err != nil {
				t.Fatalf("prepare fixture initial lifecycle: %v", err)
			}
			construction620Command, err := flowactivationfixture.Command(construction620Ctx, construction620Instance, construction620Lifecycle, construction620At)
			if err != nil {
				t.Fatalf("prepare fixture activation command: %v", err)
			}
			result, err := any(selected).(runtimebus.FlowInstanceActivationCommitOwner).CommitFlowInstanceActivation(construction620Ctx, construction620Command)
			if err == nil && !result.Acknowledged {
				t.Fatal("fixture activation was not acknowledged")
			}
			if result.Acknowledged && result.Created {
				if finalizeErr := seedRuntime.Pipeline.FinalizeInitialEntryLifecycle(construction620Ctx, result.Lifecycle); finalizeErr != nil {
					t.Fatalf("finalize fixture initial lifecycle: %v", finalizeErr)
				}
			}
			if err != nil {
				t.Fatalf("materialize workflow timer before restart: %v", err)
			}
			if !result.Created {
				t.Fatalf("initial materialization result = %v, want created", result)
			}
			shutdown("seed", seedRuntime, seedProcess)

			flakyManagerStore := &workflowTimerStartupFlakyManagerStore{
				workflowTimerStartupStore: selected,
				failAt:                    2,
			}
			restarted, restartedProcess := newRuntime(flakyManagerStore)
			capability, _ := installExternalRuntimeTestGeneration(t, ctx, selected, restarted)
			err = restarted.Start(ctx)
			if err == nil || !strings.Contains(err.Error(), "hydrate static declaration topology") {
				if closeErr := closeExternalRuntimeTestGeneration(restarted, restartedProcess, capability); closeErr != nil {
					t.Fatalf("close unexpected successful restart: %v", closeErr)
				}
				t.Fatalf("Start error = %v, want topology hydration failure before workflow-timer restoration", err)
			}
			if err := closeExternalRuntimeTestGeneration(restarted, restartedProcess, capability); err != nil {
				t.Fatalf("close failed-restart generation: %v", err)
			}

			instance, found, err := restarted.Pipeline.Load(ctx, runtimeflowidentity.RunScopedFlowInstance{
				RunID: runID,
				Route: runtimeflowidentity.StoredRoute(".", runID, runID),
			})
			if err != nil {
				t.Fatalf("load workflow instance after failed restart: %v", err)
			}
			if !found || instance.CurrentState != "waiting" {
				t.Fatalf("workflow instance after failed restart = found:%v state:%q, want durable waiting state", found, instance.CurrentState)
			}
		})
	}
}

func TestRuntimeStartRestoresWorkflowTimersWithoutGenericScheduleStoreOnBothStores(t *testing.T) {
	for _, backend := range workflowTimerStartupBackends() {
		t.Run(backend.name, func(t *testing.T) {
			for _, exit := range []string{"exhausted", "blocked", "error", "cancel_recovery", "cancel_continuation"} {
				t.Run(exit, func(t *testing.T) {
					selected := backend.open(t)
					workflowPersistence := runtimepipeline.NewWorkflowPersistence(selected)
					runID := uuid.NewString()
					bundle, sourceFact := workflowTimerStartupRecoverySource(t, "25ms")
					ctx := runtimecorrelation.WithRunID(workflowTimerStartupContext(sourceFact), runID)
					storetest.RequireRun(t, ctx, selected, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runID, Artifact: bundle.SourceArtifact})

					// Native construction persists the due occurrence without arming the
					// seed runtime. The restarted runtime owns both held startup handoffs.
					proof := startuphandoff.New()
					defer proof.Release()
					recoveryFault := errors.New("startup native recovery observation failed")
					if exit == "error" {
						proof.RecoveryError = recoveryFault
					}
					source := semanticview.Wrap(bundle)
					module := newRuntimeTestWorkflowModule(t, source)
					bootProgress := make([]swarmruntime.BootProgressEvent, 0, swarmruntime.BootProgressTotalSteps)
					newRuntime := func() (*swarmruntime.Runtime, *worklifetime.Process) {
						process := worklifetime.NewProcess()
						rt, err := swarmruntime.NewRuntime(ctx, completeExternalRuntimeTestWorkflowDeps(t, selected, swarmruntime.RuntimeDeps{
							Config: &config.Config{
								Runtime: config.RuntimeConfig{RecoveryOnStartup: true},
								LLM:     config.LLMConfig{Backend: "anthropic"},
							},

							EventStore:                  selected,
							EventBusDurable:             externalRuntimeTestDurableDependencies(selected),
							EventPayloadAdmissionBinder: selected,
							AuthorActivityRegistrars:    []swarmruntime.AuthorActivityCatalogRegistrar{selected},
							RunLifecycleCandidates:      selected,
							WorkflowPersistence:         workflowPersistence,
							ManagerStore:                selected,
							ManagerPersistenceRoles:     externalRuntimeTestSelectedManagerRoles(selected),
							TimerObligationReader:       selected,
							DeliveryStore:               proof.DeliveryStore(selected),
							PipelineObligations:         proof.RecoveryStore(selected.PipelineObligations()),

							Options: swarmruntime.RuntimeOptions{
								SelfCheck:          false,
								WorkflowModule:     module,
								LLMRuntime:         workflowTimerStartupLLM{},
								RuntimeInstanceID:  authorActivityTestRuntimeInstanceID,
								SourceArtifactFact: sourceFact,
								ProcessWorkOwner:   process,
								TestLifecycleProbe: proof,
								BootProgress: func(event swarmruntime.BootProgressEvent) {
									bootProgress = append(bootProgress, event)
									if event.Step == 16 && event.Status == "ok" {
										proof.MarkContinuationsSettled()
									}
								},
							},
						}))
						if err != nil {
							t.Fatalf("NewRuntime: %v", err)
						}
						return rt, process
					}
					shutdown := func(label string, rt *swarmruntime.Runtime, process *worklifetime.Process) {
						t.Helper()
						if err := closeExternalRuntimeTestGeneration(rt, process, nil); err != nil {
							t.Fatalf("close %s runtime: %v", label, err)
						}
					}

					seedRuntime, seedProcess := newRuntime()
					if err := seedRuntime.Scheduler.PrepareStartup(); err != nil {
						t.Fatal(err)
					}
					rootIdentity := runtimeflowidentity.Stored(source, ".", runID, runID, "", "")
					rootOwner := runtimeflowidentity.RunScopedFlowInstance{RunID: runID, Route: rootIdentity.Route()}
					occurredAt := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
					seedCtx := testLiveExecutionContext(worklifetime.WithRuntimeOccurrence(ctx, seedRuntime.WorkOccurrence()))
					construction757Ctx := seedCtx
					construction757At := occurredAt
					rootSchema, declared := source.FlowSchemaByID(".")
					if !declared || !rootSchema.Instance.Empty() {
						t.Fatal("timer fixture requires its compiled keyless root declaration")
					}
					construction757Instance, construction757Lifecycle, err := seedRuntime.Pipeline.PrepareInitialEntryLifecycle(construction757Ctx, rootOwner, runtimepipeline.WorkflowInstance{
						InstanceID:      rootIdentity.InstanceID,
						StorageRef:      rootIdentity.InstancePath,
						EntityID:        rootIdentity.EntityID,
						WorkflowName:    ".",
						WorkflowVersion: source.WorkflowVersion(),
						Mode:            rootSchema.EffectiveMode(),
						CurrentState:    "waiting",
						Fields:          map[string]any{},
						EntityType:      "test_entity",
					}, construction757At)
					if err != nil {
						t.Fatalf("prepare fixture initial lifecycle: %v", err)
					}
					construction757Command, err := flowactivationfixture.Command(construction757Ctx, construction757Instance, construction757Lifecycle, construction757At)
					if err != nil {
						t.Fatalf("prepare fixture activation command: %v", err)
					}
					result, err := any(selected).(runtimebus.FlowInstanceActivationCommitOwner).CommitFlowInstanceActivation(construction757Ctx, construction757Command)
					if err == nil && !result.Acknowledged {
						t.Fatal("fixture activation was not acknowledged")
					}
					if result.Acknowledged && result.Created {
						if finalizeErr := seedRuntime.Pipeline.FinalizeInitialEntryLifecycle(construction757Ctx, result.Lifecycle); finalizeErr != nil {
							t.Fatalf("finalize fixture initial lifecycle: %v", finalizeErr)
						}
					}
					if err != nil {
						t.Fatalf("materialize workflow timer before restart: %v", err)
					}
					if !result.Created {
						t.Fatalf("initial materialization result = %v, want created", result)
					}
					shutdown("seed", seedRuntime, seedProcess)
					timers, err := selected.ListWorkflowTimerActivations(ctx, runID, rootIdentity.EntityID, true)
					if err != nil || len(timers) != 1 {
						t.Fatalf("seed exact dormant timer: timers=%+v err=%v", timers, err)
					}
					original := timers[0]
					if !original.FireAt.Equal(occurredAt.Add(25*time.Millisecond)) || !original.FireAt.Before(time.Now()) {
						t.Fatalf("seed timer is not exact/already due: %+v", original)
					}
					occurrence := timeridentity.WorkflowTimerOccurrenceRef{Activation: original.Ref, DueAt: original.FireAt}
					eventID := timeridentity.WorkflowTimerOccurrenceEventID(occurrence)
					proof.WatchID(eventID)
					if evidence := storetest.ReadSemanticEventFixtureEvidence(t, ctx, selected, runID, eventID); evidence.RecordFound {
						t.Fatalf("seeding runtime consumed overdue occurrence: %+v", evidence)
					}
					pending := eventtest.ExistingRunRootIngress(uuid.NewString(), "generic.tick", "api.v1", "startup-recovery", []byte(`{}`), 0, runID, events.EnvelopeForEntityID(events.EventEnvelope{}, runID), occurredAt.Add(time.Second))
					storetest.CommitSemanticEventWithRoutes(t, ctx, selected, pending, nil, runtimepipelineobligation.ScopeSubscribed)

					restarted, restartedProcess := newRuntime()
					capability, _ := installExternalRuntimeTestGeneration(t, ctx, selected, restarted)
					var held runtimepipelineobligation.ClaimedWork
					if exit == "blocked" {
						held, err = selected.PipelineObligations().ClaimEvent(ctx, pending.ID(), runtimepipelineobligation.PurposeRecovery)
						if err != nil {
							t.Fatalf("hold exact native recovery claim: %v", err)
						}
						defer func() {
							if err := selected.PipelineObligations().Release(context.WithoutCancel(ctx), held.Claim); err != nil {
								t.Error(err)
							}
						}()
					}
					startCtx, cancelStart := context.WithCancel(ctx)
					started := make(chan error, 1)
					joined := false
					defer func() {
						cancelStart()
						proof.Release()
						if !joined {
							select {
							case <-started:
							case <-time.After(20 * time.Second):
								t.Error("startup failed to join after barrier release")
							}
						}
						if err := closeExternalRuntimeTestGeneration(restarted, restartedProcess, capability); err != nil {
							t.Errorf("close restarted generation: %v", err)
						}
					}()
					go func() { started <- restarted.Start(startCtx) }()
					witnessCtx, witnessCancel := context.WithTimeout(t.Context(), 20*time.Second)
					defer witnessCancel()
					if err := proof.Wait(witnessCtx, proof.Recovery); err != nil {
						t.Fatal(err)
					}
					assertDormant := func() {
						t.Helper()
						current, found, err := selected.LoadWorkflowTimerActivation(ctx, original.Ref.ActivationID)
						if err != nil || !found || current.Ref != original.Ref || !current.FireAt.Equal(original.FireAt) || current.Status != original.Status {
							t.Fatalf("held handoff changed exact durable due work: original=%+v current=%+v found=%v err=%v", original, current, found, err)
						}
						if err := proof.CheckWithheld(); err != nil {
							t.Fatal(err)
						}
					}
					assertDormant()
					if exit != "blocked" && proof.Observe().Partial == 0 {
						t.Fatal("native recovery never crossed a partial batch before exhaustion")
					}
					if exit == "cancel_recovery" {
						cancelStart()
					}
					proof.Recovery.Release()
					if exit == "exhausted" || exit == "cancel_continuation" {
						if err := proof.Wait(witnessCtx, proof.Continuation); err != nil {
							t.Fatal(err)
						}
						assertDormant()
						if !proof.Observe().RecoverySettled {
							t.Fatal("continuation entered before native recovery settlement")
						}
						if exit == "cancel_continuation" {
							cancelStart()
						}
						proof.Continuation.Release()
					}
					var startErr error
					select {
					case startErr = <-started:
						joined = true
					case <-witnessCtx.Done():
						t.Fatal(witnessCtx.Err())
					}
					if exit != "exhausted" {
						switch exit {
						case "blocked":
							if startErr == nil || !strings.Contains(startErr.Error(), "pipeline recovery blocked before explicit exhaustion") {
								t.Fatalf("blocked native recovery admitted startup: %v", startErr)
							}
						case "error":
							if !errors.Is(startErr, recoveryFault) {
								t.Fatalf("recovery error lost: %v", startErr)
							}
						default:
							if !errors.Is(startErr, context.Canceled) {
								t.Fatalf("canceled startup lost cause: %v", startErr)
							}
						}
						assertDormant()
						if restarted.WorkOccurrence().ActiveCount() != 0 {
							t.Fatalf("failed startup retained work: %d", restarted.WorkOccurrence().ActiveCount())
						}
						return
					}
					if startErr != nil {
						t.Fatalf("Start restarted runtime: %v", startErr)
					}
					assertBootDetail := func(name, fragment string) {
						t.Helper()
						for _, event := range bootProgress {
							if event.Name == name {
								if !strings.Contains(event.Detail, fragment) {
									t.Fatalf("%s detail = %q, want %q", name, event.Detail, fragment)
								}
								return
							}
						}
						t.Fatalf("boot progress omitted %s", name)
					}
					assertBootDetail("recovery_snapshot_inspection", "timer obligations")
					assertBootDetail("schedule_restoration", "workflow timers restored")
					var managerRecoveryIndex, scheduleRestoreIndex = -1, -1
					for i, event := range bootProgress {
						switch event.Name {
						case "manager_recovery_if_enabled":
							managerRecoveryIndex = i
						case "schedule_restoration":
							scheduleRestoreIndex = i
						}
					}
					if managerRecoveryIndex < 0 || scheduleRestoreIndex < 0 || managerRecoveryIndex >= scheduleRestoreIndex {
						t.Fatalf("boot order manager=%d schedule=%d, want manager recovery before runnable timer restoration", managerRecoveryIndex, scheduleRestoreIndex)
					}

					select {
					case signal := <-proof.Published:
						if signal.EventID != eventID {
							t.Fatalf("late fire changed occurrence: %+v want=%s", signal, eventID)
						}
					case <-witnessCtx.Done():
						t.Fatal("exact already-due timer never published after both handoffs")
					}
					if err := restarted.Scheduler.Wait(witnessCtx); err != nil {
						t.Fatal(err)
					}
					if err := proof.CheckPublished(eventID); err != nil {
						t.Fatal(err)
					}
					instance, found, err := restarted.Pipeline.Load(ctx, rootOwner)
					if err != nil || !found || instance.CurrentState != "done" {
						t.Fatalf("workflow timer was not restored and fired; found=%v state=%q err=%v", found, instance.CurrentState, err)
					}
					settled, found, err := selected.LoadWorkflowTimerActivation(ctx, original.Ref.ActivationID)
					if err != nil || !found || settled.Ref != original.Ref || !settled.FireAt.Equal(original.FireAt) || settled.Status != "fired" {
						t.Fatalf("late fire changed activation/due identity: %+v found=%v err=%v", settled, found, err)
					}
					if evidence := storetest.ReadSemanticEventFixtureEvidence(t, ctx, selected, runID, eventID); !evidence.RecordFound || evidence.PipelineReceiptCount != 1 {
						t.Fatalf("late occurrence lacks its single native pipeline receipt: %+v", evidence)
					}
				})
			}
		})
	}
}

type workflowTimerStartupLLM struct{ llm.NoopRuntime }

func (workflowTimerStartupLLM) ProviderContract() llm.ProviderContract {
	return llm.AnthropicAPIProviderContract()
}

func workflowTimerStartupRecoveryBundleWithDelay(t *testing.T, delay string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	bundle := loadRuntimeTempBundle(t, map[string]string{
		"schema.yaml":   "name: workflow-timer-startup\npins:\n  inputs: [generic.tick]\nstages:\n  waiting:\n    timers:\n      - id: timeout\n        after: " + delay + "\n        advances_to: done\n  done:\n    final: true\n",
		"entities.yaml": "test_entity: {}\n",
		"events.yaml":   "generic.tick:\n",
	})
	return bundle
}

func workflowTimerStartupRecoverySource(t *testing.T, delay string) (*runtimecontracts.WorkflowContractBundle, runtimecorrelation.SourceArtifactFact) {
	t.Helper()
	bundle := workflowTimerStartupRecoveryBundleWithDelay(t, delay)
	if bundle.SourceArtifact == nil {
		t.Fatal("workflow timer startup fixture omitted admitted source artifact")
	}
	fact, err := runtimecorrelation.NewSourceArtifactFact(bundle.SourceArtifact.BundleHash())
	if err != nil {
		t.Fatalf("construct workflow timer source artifact fact: %v", err)
	}
	return bundle, fact
}

func workflowTimerStartupContext(fact runtimecorrelation.SourceArtifactFact) context.Context {
	ctx := runtimecorrelation.WithSourceArtifactFact(context.Background(), fact)
	return runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(
		authorActivityTestRuntimeInstanceID,
		fact.BundleHash(),
	))
}
