package pipeline_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	runtimeeventschema "github.com/division-sh/swarm/internal/runtime/eventschema"
	runtimelifecycleprobe "github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/testutil/flowactivationfixture"
	"github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/google/uuid"
)

func TestWorkflowTimerServedLifecycleConvergesOnBothStores(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{
		{name: "sqlite", open: openSQLiteGateRecoveryStore},
		{name: "postgres", open: openPostgresGateRecoveryStore},
	} {
		t.Run(tc.name, func(t *testing.T) {
			selected := tc.open(t)
			runID := uuid.NewString()
			entityID := uuid.NewString()
			bundle := workflowTimerServedLifecycleBundle(t, false)
			ctx, fact := workflowLifecycleSourceContext(t, selected, bundle, runID)
			source := semanticview.Wrap(bundle)
			bus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{
				ContractBundle: source, SourceArtifactFact: fact, PayloadAdmitter: strictWorkflowTimerPayloadAdmitter,
			}, runtimecontracts.WorkflowStageTimerInternalEvent)
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}

			fireErrors := make(chan error, 4)
			scheduler := runtimepipeline.NewSchedulerWithWorkOwner(pipelineExternalTestWorkOwner(t))
			t.Cleanup(scheduler.Stop)
			coordinator := newGateRecoveryCoordinator(bus, selected, runtimepipeline.PipelineCoordinatorOptions{
				Module:         gateRecoveryModule{source: source},
				Persistence:    selected.persistence,
				TimerScheduler: scheduler,
				WorkOwner:      pipelineExternalTestWorkOwner(t),
			})
			bus.SetInterceptors(coordinator)

			createdAt := time.Now().UTC()
			{
				construction62Ctx := ctx
				construction62At := createdAt
				construction62Instance, construction62Lifecycle, err := coordinator.PrepareInitialEntryLifecycle(construction62Ctx, testRunScopedWorkflowInstanceForRun(runID, runID), runtimepipeline.WorkflowInstance{
					InstanceID: runID, StorageRef: runID, EntityID: entityID, WorkflowName: ".", WorkflowVersion: source.WorkflowVersion(),
					CurrentState: "waiting", StageDefined: true, EnteredStageAt: createdAt, CreatedAt: createdAt,
					Fields:     map[string]any{},
					EntityType: "test_entity",
				}, construction62At)
				if err != nil {
					t.Fatalf("prepare fixture initial lifecycle: %v", err)
				}
				construction62Command, err := flowactivationfixture.Command(construction62Ctx, construction62Instance, construction62Lifecycle, construction62At)
				if err != nil {
					t.Fatalf("prepare fixture activation command: %v", err)
				}
				construction62Committed, err := any(selected.events).(runtimebus.FlowInstanceActivationCommitOwner).CommitFlowInstanceActivation(construction62Ctx, construction62Command)
				if err != nil {
					t.Fatalf("materialize workflow instance: %v", err)
				}
				if err == nil && !construction62Committed.Acknowledged {
					t.Fatal("fixture activation was not acknowledged")
				}
				if construction62Committed.Acknowledged && construction62Committed.Created {
					if finalizeErr := coordinator.FinalizeInitialEntryLifecycle(construction62Ctx, construction62Committed.Lifecycle); finalizeErr != nil {
						t.Fatalf("finalize fixture initial lifecycle: %v", finalizeErr)
					}
				}
			}
			if err := coordinator.ArmInitialEntryTimers(ctx, testRunScopedWorkflowInstanceForRun(runID, runID)); err != nil {
				t.Fatalf("arm initial workflow timers: %v", err)
			}

			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) {
				select {
				case err := <-fireErrors:
					t.Fatalf("workflow timer callback: %v", err)
				default:
				}
				instance, found, err := coordinator.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, runID))
				if err != nil {
					t.Fatalf("load workflow instance: %v", err)
				}
				if found && instance.CurrentState == "done" {
					assertWorkflowTimerServedRows(t, selected, runID, entityID, "fired", 1)
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
			t.Fatal("workflow timer did not fire and advance through the real scheduler/EventBus path")
		})
	}
}

func TestAuthoredWorkflowTimerExecutesCompiledConnectRouteOnBothStores(t *testing.T) {
	canonicalrouting.Prove(t, canonicalrouting.ParentConnect)
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	fixtureRoot := canonicalrouting.CopyParentConnectTimer(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot,
		fixtureRoot,
		runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("load parent-connect timer fixture: %v", err)
	}
	source := semanticview.Wrap(bundle)

	for _, tc := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{
		{name: "sqlite", open: openSQLiteGateRecoveryStore},
		{name: "postgres", open: openPostgresGateRecoveryStore},
	} {
		t.Run(tc.name, func(t *testing.T) {
			selected := tc.open(t)
			runID := uuid.NewString()
			entityID := uuid.NewString()
			ctx, fact := workflowLifecycleSourceContext(t, selected, bundle, runID)
			logger := &exactJoinRuntimeLogger{}
			eventBus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{ContractBundle: source, SourceArtifactFact: fact, Logger: logger})
			if err != nil {
				t.Fatalf("new authored timer EventBus: %v", err)
			}
			consumerNode := externalPipelineSourceNode(t, source, "consumer", "consumer-node")
			deliveries := internalSubscriptionDeliveriesForTest(t, eventBus, consumerNode.Key(), "producer/work.ready")
			if got := eventBus.ResolveSubscribedRecipients("producer/work.ready"); len(got) != 1 || got[0] != consumerNode.Key() {
				t.Fatalf("workflow timer live recipients = %v, want %s", got, consumerNode.Key())
			}
			scheduler := runtimepipeline.NewSchedulerWithWorkOwner(pipelineExternalTestWorkOwner(t))
			t.Cleanup(scheduler.Stop)
			coordinator := newGateRecoveryCoordinator(eventBus, selected, runtimepipeline.PipelineCoordinatorOptions{
				Module:         gateRecoveryModule{source: source},
				Persistence:    selected.persistence,
				TimerScheduler: scheduler,
				WorkOwner:      pipelineExternalTestWorkOwner(t),
			})

			createdAt := time.Now().UTC()
			{
				consumerConstructor, err := runtimepipeline.CompileFlowConstructor(source, "consumer", "")
				if err != nil {
					t.Fatal(err)
				}
				consumerFields, err := consumerConstructor.InitialFields(nil, nil)
				if err != nil {
					t.Fatal(err)
				}
				consumerTopology, found := semanticview.WorkflowStageTopology(source, "consumer")
				if !found {
					t.Fatal("consumer compiled stage topology missing")
				}
				consumerInitial, err := consumerTopology.InitialStoredStage()
				if err != nil {
					t.Fatal(err)
				}
				consumerContract, _ := entityruntime.ResolveForFlow(source, "consumer")
				consumerOwner := testRunScopedWorkflowInstanceForRun(runID, "consumer")
				consumerInstance, consumerLifecycle, err := coordinator.PrepareInitialEntryLifecycle(ctx, consumerOwner, runtimepipeline.WorkflowInstance{
					InstanceID: "consumer", StorageRef: "consumer", WorkflowName: "consumer", WorkflowVersion: source.WorkflowVersion(),
					EntityID: runtimepipeline.FlowInstanceEntityID("consumer"), EntityType: consumerContract.EntityType,
					ParentFlowID: semanticview.RootExecutionFlowID(source), ParentFlowInstance: runID, ParentEntityID: runID,
					CurrentState: consumerInitial.ID(), StageDefined: consumerTopology.StageCount() != 0,
					Fields: consumerFields, CreatedAt: createdAt,
				}, createdAt)
				if err != nil {
					t.Fatalf("prepare consumer constructor lifecycle: %v", err)
				}
				consumerCommand, err := flowactivationfixture.Command(ctx, consumerInstance, consumerLifecycle, createdAt)
				if err != nil {
					t.Fatal(err)
				}
				consumerCommitted, err := eventBus.CommitFlowInstanceActivation(ctx, consumerCommand.Plan)
				if err != nil || !consumerCommitted.Acknowledged || !consumerCommitted.Created {
					t.Fatalf("construct timer consumer: acknowledged=%v created=%v err=%v", consumerCommitted.Acknowledged, consumerCommitted.Created, err)
				}
				if err := coordinator.FinalizeInitialEntryLifecycle(ctx, consumerCommitted.Lifecycle); err != nil {
					t.Fatal(err)
				}
				construction142Ctx := ctx
				construction142At := createdAt
				construction142Instance, construction142Lifecycle, err := coordinator.PrepareInitialEntryLifecycle(construction142Ctx, testRunScopedWorkflowInstanceForRun(runID, "producer"), runtimepipeline.WorkflowInstance{
					InstanceID: "producer", StorageRef: "producer", EntityID: entityID, WorkflowName: "producer", WorkflowVersion: source.WorkflowVersion(),
					CurrentState: "waiting", StageDefined: true, EnteredStageAt: createdAt, CreatedAt: createdAt,
					Fields:     map[string]any{},
					EntityType: "test_entity",
				}, construction142At)
				if err != nil {
					t.Fatalf("prepare fixture initial lifecycle: %v", err)
				}
				construction142Command, err := flowactivationfixture.Command(construction142Ctx, construction142Instance, construction142Lifecycle, construction142At)
				if err != nil {
					t.Fatalf("prepare fixture activation command: %v", err)
				}
				construction142Committed, err := any(selected.events).(runtimebus.FlowInstanceActivationCommitOwner).CommitFlowInstanceActivation(construction142Ctx, construction142Command)
				if err != nil {
					t.Fatalf("materialize producer workflow instance: %v", err)
				}
				if err == nil && !construction142Committed.Acknowledged {
					t.Fatal("fixture activation was not acknowledged")
				}
				if construction142Committed.Acknowledged && construction142Committed.Created {
					if finalizeErr := coordinator.FinalizeInitialEntryLifecycle(construction142Ctx, construction142Committed.Lifecycle); finalizeErr != nil {
						t.Fatalf("finalize fixture initial lifecycle: %v", finalizeErr)
					}
				}
			}
			if err := coordinator.ArmInitialEntryTimers(ctx, testRunScopedWorkflowInstanceForRun(runID, "producer")); err != nil {
				t.Fatalf("arm authored workflow timer: %v", err)
			}

			select {
			case delivery := <-deliveries:
				if delivery.Type() != events.EventType("producer/work.ready") {
					t.Fatalf("delivered timer event type = %q, want producer/work.ready", delivery.Type())
				}
				sourceFact := delivery.Event().RoutingSource()
				if sourceFact.Kind() != events.RoutingSourceFlowOwnedControl || sourceFact.Route().FlowID != "producer" {
					t.Fatalf("delivered timer routing source = %#v, want producer flow-owned control", sourceFact)
				}
				route := delivery.HandoffRoute()
				if route.Recipient.ID() != consumerNode.Key() || route.ConnectClaim.Empty() {
					t.Fatalf("timer delivery route = %#v, want consumer-node with stamped connect claim", route)
				}
				if err := delivery.Complete(); err != nil {
					t.Fatalf("complete authored timer delivery: %v", err)
				}
				waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				if err := eventBus.WaitForQuiescence(waitCtx); err != nil {
					cancel()
					t.Fatalf("wait authored timer delivery quiescence: %v", err)
				}
				cancel()
			case <-time.After(5 * time.Second):
				t.Fatalf("authored workflow timer did not deliver through compiled connect; trace=%v; diagnostics=%s", workflowTimerDeliveryTrace(t, selected, runID), logger)
			}
			waitWorkflowTimerRowStatus(t, selected, runID, entityID, "fired")
		})
	}
}

func TestRecurringWorkflowTimerDoesNotReregisterAfterSynchronousTransitionCancellationOnBothStores(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{
		{name: "sqlite", open: openSQLiteGateRecoveryStore},
		{name: "postgres", open: openPostgresGateRecoveryStore},
	} {
		t.Run(tc.name, func(t *testing.T) {
			selected := tc.open(t)
			runID := uuid.NewString()
			entityID := uuid.NewString()
			bundle := workflowTimerServedLifecycleBundle(t, true)
			bundle.Semantics.Timers[0].Delay = "5s"
			ctx, fact := workflowLifecycleSourceContext(t, selected, bundle, runID)
			source := semanticview.Wrap(bundle)
			bus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{
				ContractBundle: source, SourceArtifactFact: fact, PayloadAdmitter: strictWorkflowTimerPayloadAdmitter,
			}, runtimecontracts.WorkflowStageTimerInternalEvent)
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}

			scheduler := runtimepipeline.NewSchedulerWithWorkOwner(pipelineExternalTestWorkOwner(t))
			t.Cleanup(scheduler.Stop)
			coordinator := newGateRecoveryCoordinator(bus, selected, runtimepipeline.PipelineCoordinatorOptions{
				Module: gateRecoveryModule{source: source}, Persistence: selected.persistence,
				TimerScheduler: scheduler, WorkOwner: pipelineExternalTestWorkOwner(t),
			})
			bus.SetInterceptors(coordinator)

			createdAt := time.Now().UTC().Add(-4900 * time.Millisecond)
			{
				construction217Ctx := ctx
				construction217At := createdAt
				construction217Instance, construction217Lifecycle, err := coordinator.PrepareInitialEntryLifecycle(construction217Ctx, testRunScopedWorkflowInstanceForRun(runID, runID), runtimepipeline.WorkflowInstance{
					InstanceID: runID, StorageRef: runID, EntityID: entityID, WorkflowName: ".", WorkflowVersion: source.WorkflowVersion(),
					CurrentState: "waiting", StageDefined: true, EnteredStageAt: createdAt, CreatedAt: createdAt,
					Fields:     map[string]any{},
					EntityType: "test_entity",
				}, construction217At)
				if err != nil {
					t.Fatalf("prepare fixture initial lifecycle: %v", err)
				}
				construction217Command, err := flowactivationfixture.Command(construction217Ctx, construction217Instance, construction217Lifecycle, construction217At)
				if err != nil {
					t.Fatalf("prepare fixture activation command: %v", err)
				}
				construction217Committed, err := any(selected.events).(runtimebus.FlowInstanceActivationCommitOwner).CommitFlowInstanceActivation(construction217Ctx, construction217Command)
				if err != nil {
					t.Fatalf("materialize workflow instance: %v", err)
				}
				if err == nil && !construction217Committed.Acknowledged {
					t.Fatal("fixture activation was not acknowledged")
				}
				if construction217Committed.Acknowledged && construction217Committed.Created {
					if finalizeErr := coordinator.FinalizeInitialEntryLifecycle(construction217Ctx, construction217Committed.Lifecycle); finalizeErr != nil {
						t.Fatalf("finalize fixture initial lifecycle: %v", finalizeErr)
					}
				}
			}
			if err := coordinator.ArmInitialEntryTimers(ctx, testRunScopedWorkflowInstanceForRun(runID, runID)); err != nil {
				t.Fatalf("arm initial workflow timers: %v", err)
			}

			stateDeadline := time.Now().Add(5 * time.Second)
			for {
				instance, found, err := coordinator.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, runID))
				if err != nil {
					t.Fatalf("load workflow instance: %v", err)
				}
				if found && instance.CurrentState == "done" {
					break
				}
				if time.Now().After(stateDeadline) {
					t.Fatal("recurring workflow timer did not synchronously advance the workflow")
				}
				time.Sleep(10 * time.Millisecond)
			}
			assertWorkflowTimerServedRows(t, selected, runID, entityID, "cancelled", 1)

			settleDeadline := time.Now().Add(2 * time.Second)
			for {
				active, draining := runtimepipeline.WorkflowTimerScheduledCountsForTest(scheduler)
				if active == 0 && draining == 0 {
					break
				}
				if time.Now().After(settleDeadline) {
					t.Fatalf(
						"workflow timer scheduler retained a projection after synchronous cancellation: active=%d draining=%d",
						active,
						draining,
					)
				}
				time.Sleep(10 * time.Millisecond)
			}
			if got := workflowTimerEventCount(t, selected, runID, runtimecontracts.WorkflowStageTimerInternalEvent); got != 1 {
				t.Fatalf("workflow timer events after synchronous cancellation = %d, want 1", got)
			}
		})
	}
}

func TestWorkflowTimerOneShotRestoresBeforeFireAndStaysTerminalAfterRestartOnBothStores(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{
		{name: "sqlite", open: openSQLiteGateRecoveryStore},
		{name: "postgres", open: openPostgresGateRecoveryStore},
	} {
		t.Run(tc.name, func(t *testing.T) {
			selected := tc.open(t)
			runID := uuid.NewString()
			entityID := uuid.NewString()
			bundle := workflowTimerServedLifecycleBundle(t, false)
			ctx, fact := workflowLifecycleSourceContext(t, selected, bundle, runID)
			source := semanticview.Wrap(bundle)
			logger := &exactJoinRuntimeLogger{}
			bus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{
				ContractBundle: source, SourceArtifactFact: fact, PayloadAdmitter: strictWorkflowTimerPayloadAdmitter, Logger: logger,
			}, runtimecontracts.WorkflowStageTimerInternalEvent)
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			module := gateRecoveryModule{source: source}
			coordinator := newGateRecoveryCoordinator(bus, selected, runtimepipeline.PipelineCoordinatorOptions{
				Module: module, Persistence: selected.persistence,
			})
			bus.SetInterceptors(coordinator)

			createdAt := time.Now().UTC()
			{
				construction295Ctx := ctx
				construction295At := createdAt
				construction295Instance, construction295Lifecycle, err := coordinator.PrepareInitialEntryLifecycle(construction295Ctx, testRunScopedWorkflowInstanceForRun(runID, runID), runtimepipeline.WorkflowInstance{
					InstanceID: runID, StorageRef: runID, EntityID: entityID, WorkflowName: ".", WorkflowVersion: source.WorkflowVersion(),
					CurrentState: "waiting", StageDefined: true, EnteredStageAt: createdAt, CreatedAt: createdAt,
					Fields:     map[string]any{},
					EntityType: "test_entity",
				}, construction295At)
				if err != nil {
					t.Fatalf("prepare fixture initial lifecycle: %v", err)
				}
				construction295Command, err := flowactivationfixture.Command(construction295Ctx, construction295Instance, construction295Lifecycle, construction295At)
				if err != nil {
					t.Fatalf("prepare fixture activation command: %v", err)
				}
				construction295Committed, err := any(selected.events).(runtimebus.FlowInstanceActivationCommitOwner).CommitFlowInstanceActivation(construction295Ctx, construction295Command)
				if err != nil {
					t.Fatalf("materialize timer before restart: %v", err)
				}
				if err == nil && !construction295Committed.Acknowledged {
					t.Fatal("fixture activation was not acknowledged")
				}
				if construction295Committed.Acknowledged && construction295Committed.Created {
					if finalizeErr := coordinator.FinalizeInitialEntryLifecycle(construction295Ctx, construction295Committed.Lifecycle); finalizeErr != nil {
						t.Fatalf("finalize fixture initial lifecycle: %v", finalizeErr)
					}
				}
			}
			assertWorkflowTimerServedRows(t, selected, runID, entityID, "active", 1)

			fireErrors := make(chan error, 4)
			scheduler := runtimepipeline.NewSchedulerWithWorkOwner(pipelineExternalTestWorkOwner(t))
			restored := newGateRecoveryCoordinator(bus, selected, runtimepipeline.PipelineCoordinatorOptions{
				Module: module, Persistence: selected.persistence,
				TimerScheduler: scheduler, WorkOwner: pipelineExternalTestWorkOwner(t),
			})
			bus.SetInterceptors(restored)
			if err := restored.RestoreWorkflowTimers(ctx); err != nil {
				scheduler.Stop()
				t.Fatalf("restore active one-shot timer: %v", err)
			}
			deadline := time.Now().Add(5 * time.Second)
			completed := false
			for time.Now().Before(deadline) {
				select {
				case err := <-fireErrors:
					scheduler.Stop()
					t.Fatalf("restored workflow timer callback: %v", err)
				default:
				}
				instance, found, err := restored.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, runID))
				if err != nil {
					scheduler.Stop()
					t.Fatalf("load workflow instance after restored fire: %v", err)
				}
				if found && instance.CurrentState == "done" {
					completed = true
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			scheduler.Stop()
			waitCtx, cancelWait := context.WithTimeout(ctx, 2*time.Second)
			if err := scheduler.Wait(waitCtx); err != nil {
				cancelWait()
				t.Fatalf("wait restored one-shot scheduler: %v", err)
			}
			cancelWait()
			if !completed {
				t.Fatalf("restored one-shot timer did not advance through the real EventBus path; diagnostics=%s", logger)
			}
			assertWorkflowTimerServedRows(t, selected, runID, entityID, "fired", 1)
			if got := workflowTimerEventCount(t, selected, runID, runtimecontracts.WorkflowStageTimerInternalEvent); got != 1 {
				t.Fatalf("one-shot events after pre-fire restart = %d, want 1", got)
			}

			terminalScheduler := runtimepipeline.NewSchedulerWithWorkOwner(pipelineExternalTestWorkOwner(t))
			t.Cleanup(terminalScheduler.Stop)
			terminal := newGateRecoveryCoordinator(bus, selected, runtimepipeline.PipelineCoordinatorOptions{
				Module: module, Persistence: selected.persistence,
				TimerScheduler: terminalScheduler,
			})
			if err := terminal.RestoreWorkflowTimers(ctx); err != nil {
				t.Fatalf("restore after one-shot completion: %v", err)
			}
			select {
			case err := <-fireErrors:
				t.Fatal(err)
			case <-time.After(150 * time.Millisecond):
			}
			if got := workflowTimerEventCount(t, selected, runID, runtimecontracts.WorkflowStageTimerInternalEvent); got != 1 {
				t.Fatalf("one-shot events after terminal restart = %d, want 1", got)
			}
		})
	}
}

func TestRecurringWorkflowTimerFiresRestoresAndCancelsOnBothStores(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{
		{name: "sqlite", open: openSQLiteGateRecoveryStore},
		{name: "postgres", open: openPostgresGateRecoveryStore},
	} {
		t.Run(tc.name, func(t *testing.T) {
			selected := tc.open(t)
			runID := uuid.NewString()
			entityID := runID
			source := workflowTimerRecurringCancellationSource(t)
			bundle, ok := semanticview.Bundle(source)
			if !ok {
				t.Fatal("recurring timer source requires its admitted bundle")
			}
			ctx, fact := workflowLifecycleSourceContext(t, selected, bundle, runID)
			controllerNode := externalPipelineSourceNode(t, source, "", "controller")
			lifecycleProbe := runtimelifecycleprobe.New()
			module := proposedEffectProofModule{
				source: source,
				nodes: []runtimepipeline.WorkflowNode{{
					Node: controllerNode, Subscriptions: []events.EventType{"timer-proof/timer.cancel"},
					ExecutionType: runtimecontracts.SystemNodeExecutionType,
				}},
			}
			bus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{
				ContractBundle: source, SourceArtifactFact: fact, PayloadAdmitter: strictWorkflowTimerPayloadAdmitter, TestLifecycleProbe: lifecycleProbe,
			}, runtimecontracts.WorkflowStageTimerInternalEvent)
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}

			var coordinator *runtimepipeline.PipelineCoordinator
			fireErrors := make(chan error, 8)
			newScheduler := func() *runtimepipeline.Scheduler {
				return runtimepipeline.NewSchedulerWithWorkOwner(pipelineExternalTestWorkOwner(t))
			}
			scheduler := newScheduler()
			coordinator = newGateRecoveryCoordinator(bus, selected, runtimepipeline.PipelineCoordinatorOptions{
				Module: module, Persistence: selected.persistence,
				TimerScheduler: scheduler, WorkOwner: pipelineExternalTestWorkOwner(t),
				TestLifecycleProbe: lifecycleProbe,
			})
			bus.SetInterceptors(coordinator)

			createdAt := time.Now().UTC()
			{
				construction417Ctx := ctx
				construction417At := createdAt
				construction417Instance, construction417Lifecycle, err := coordinator.PrepareInitialEntryLifecycle(construction417Ctx, testRunScopedWorkflowInstanceForRun(runID, runID), runtimepipeline.WorkflowInstance{
					InstanceID: runID, StorageRef: runID, EntityID: entityID, WorkflowName: ".", WorkflowVersion: source.WorkflowVersion(),
					CurrentState: "waiting", StageDefined: true, EnteredStageAt: createdAt, CreatedAt: createdAt,
					Fields:     map[string]any{},
					EntityType: "timer_state",
				}, construction417At)
				if err != nil {
					t.Fatalf("prepare fixture initial lifecycle: %v", err)
				}
				construction417Command, err := flowactivationfixture.Command(construction417Ctx, construction417Instance, construction417Lifecycle, construction417At)
				if err != nil {
					t.Fatalf("prepare fixture activation command: %v", err)
				}
				construction417Committed, err := any(selected.events).(runtimebus.FlowInstanceActivationCommitOwner).CommitFlowInstanceActivation(construction417Ctx, construction417Command)
				if err != nil {
					t.Fatalf("materialize workflow instance: %v", err)
				}
				if err == nil && !construction417Committed.Acknowledged {
					t.Fatal("fixture activation was not acknowledged")
				}
				if construction417Committed.Acknowledged && construction417Committed.Created {
					if finalizeErr := coordinator.FinalizeInitialEntryLifecycle(construction417Ctx, construction417Committed.Lifecycle); finalizeErr != nil {
						t.Fatalf("finalize fixture initial lifecycle: %v", finalizeErr)
					}
				}
			}
			if err := coordinator.ArmInitialEntryTimers(ctx, testRunScopedWorkflowInstanceForRun(runID, runID)); err != nil {
				t.Fatalf("arm initial workflow timers: %v", err)
			}
			armed, found, err := coordinator.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, runID))
			if err != nil || !found {
				t.Fatalf("load workflow instance after timer activation: found=%v err=%v", found, err)
			}
			armedRevision := armed.Revision
			waitWorkflowTimerEventCount(t, selected, fireErrors, runID, runtimecontracts.WorkflowStageTimerInternalEvent, 2)

			scheduler.Stop()
			waitCtx, cancelWait := context.WithTimeout(ctx, 2*time.Second)
			defer cancelWait()
			if err := scheduler.Wait(waitCtx); err != nil {
				t.Fatalf("wait stopped scheduler: %v", err)
			}
			beforeRestart := workflowTimerEventCount(t, selected, runID, runtimecontracts.WorkflowStageTimerInternalEvent)
			scheduler = newScheduler()
			t.Cleanup(scheduler.Stop)
			coordinator = newGateRecoveryCoordinator(bus, selected, runtimepipeline.PipelineCoordinatorOptions{
				Module: module, Persistence: selected.persistence,
				TimerScheduler: scheduler, WorkOwner: pipelineExternalTestWorkOwner(t),
				TestLifecycleProbe: lifecycleProbe,
			})
			bus.SetInterceptors(coordinator)
			if err := coordinator.RestoreWorkflowTimers(ctx); err != nil {
				t.Fatalf("RestoreWorkflowTimers: %v", err)
			}
			waitWorkflowTimerEventCount(t, selected, fireErrors, runID, runtimecontracts.WorkflowStageTimerInternalEvent, beforeRestart+1)
			beforeCancel, found, err := coordinator.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, runID))
			if err != nil || !found {
				t.Fatalf("load workflow instance before cancellation: found=%v err=%v", found, err)
			}
			if beforeCancel.Revision != armedRevision {
				t.Fatalf("non-advancing recurring timers changed workflow revision from %d to %d", armedRevision, beforeCancel.Revision)
			}

			cancelEvent := eventtest.ExistingRunRootIngress(
				uuid.NewString(), "timer.cancel", "operator", "", []byte(`{}`), 0, runID,
				events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), runID), time.Now().UTC(),
			)
			plan, err := bus.CheckPublishRecipientPlan(ctx, cancelEvent)
			if err != nil {
				t.Fatalf("plan timer cancellation transition: %v", err)
			}
			if got := plan.DeliveryRoutes; len(got) != 1 || !got[0].Recipient.IsNode() || got[0].Recipient.ID() != controllerNode.Key() {
				t.Fatalf("timer cancellation delivery routes = %#v, want exact controller node route", got)
			}
			wantTarget := events.RouteIdentity{FlowID: ".", FlowInstance: runID, EntityID: entityID}
			if got := plan.DeliveryRoutes[0].Target.Route(); got != wantTarget {
				t.Fatalf("timer cancellation target owner = %#v, want selected run owner %#v", got, wantTarget)
			}
			if err := bus.Publish(ctx, cancelEvent); err != nil {
				t.Fatalf("publish timer cancellation transition: %v", err)
			}
			probeCtx, cancelProbe := context.WithTimeout(ctx, 2*time.Second)
			defer cancelProbe()
			if _, err := lifecycleProbe.WaitForPostCommitDispatchCompleted(probeCtx, cancelEvent.ID()); err != nil {
				t.Fatalf("wait for timer cancellation post-commit dispatch: %v", err)
			}
			if _, err := lifecycleProbe.WaitForDeliveryStatus(probeCtx, cancelEvent.ID(), "node", controllerNode.Key(), "in_progress"); err != nil {
				t.Fatalf("wait for timer cancellation delivery claim: %v", err)
			}
			if _, err := lifecycleProbe.WaitForHandlerStarted(probeCtx, cancelEvent.ID(), controllerNode.Key()); err != nil {
				t.Fatalf("wait for timer cancellation handler start: %v", err)
			}
			handlerCompletion, err := lifecycleProbe.WaitForHandlerCompleted(probeCtx, cancelEvent.ID(), controllerNode.Key())
			if err != nil {
				t.Fatalf("wait for timer cancellation handler completion: %v", err)
			}
			if handlerCompletion.Status != "completed" {
				query := `SELECT status, COALESCE(failure, '{}') FROM event_deliveries WHERE event_id = ? AND subscriber_type = 'node' AND subscriber_id = ?`
				if selected.postgres {
					query = `SELECT status, COALESCE(failure, '{}'::jsonb) FROM event_deliveries WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = $2`
				}
				var deliveryStatus string
				var failure []byte
				if err := selected.db.QueryRowContext(ctx, query, cancelEvent.ID(), controllerNode.Key()).Scan(&deliveryStatus, &failure); err != nil {
					t.Fatalf("timer cancellation handler status = %q and delivery failure readback failed: %v", handlerCompletion.Status, err)
				}
				t.Fatalf("timer cancellation handler status = %q, delivery status = %q failure = %s; want completed", handlerCompletion.Status, deliveryStatus, failure)
			}
			cancelled := false
			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) {
				instance, found, err := coordinator.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, runID))
				if err != nil {
					t.Fatalf("load workflow instance after cancellation: %v", err)
				}
				if found && instance.CurrentState == "done" {
					assertWorkflowTimerServedRows(t, selected, runID, entityID, "cancelled", 1)
					cancelled = true
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			if !cancelled {
				t.Fatal("workflow timer cancellation event did not advance the workflow to done")
			}
			afterCancel := workflowTimerEventCount(t, selected, runID, runtimecontracts.WorkflowStageTimerInternalEvent)
			time.Sleep(150 * time.Millisecond)
			if got := workflowTimerEventCount(t, selected, runID, runtimecontracts.WorkflowStageTimerInternalEvent); got != afterCancel {
				t.Fatalf("workflow timer events after exact cancellation = %d, want %d", got, afterCancel)
			}
		})
	}
}

func TestWorkflowTimerRealPublishRollbackRetriesPersistedOccurrenceOnBothStores(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{
		{name: "sqlite", open: openSQLiteGateRecoveryStore},
		{name: "postgres", open: openPostgresGateRecoveryStore},
	} {
		t.Run(tc.name, func(t *testing.T) {
			selected := tc.open(t)
			runID := uuid.NewString()
			entityID := uuid.NewString()
			bundle := workflowTimerServedLifecycleBundle(t, false)
			bundle.Semantics.Timers[0].Delay = "200ms"
			ctx, fact := workflowLifecycleSourceContext(t, selected, bundle, runID)
			source := semanticview.Wrap(bundle)
			admitter := newFailOnceWorkflowTimerPayloadAdmitter()
			defer func() {
				select {
				case <-admitter.releaseSecond:
				default:
					close(admitter.releaseSecond)
				}
			}()
			bus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{
				ContractBundle: source, SourceArtifactFact: fact, PayloadAdmitter: admitter.admit,
			}, runtimecontracts.WorkflowStageTimerInternalEvent)
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}

			scheduler := runtimepipeline.NewSchedulerWithWorkOwner(pipelineExternalTestWorkOwner(t))
			t.Cleanup(scheduler.Stop)
			coordinator := newGateRecoveryCoordinator(bus, selected, runtimepipeline.PipelineCoordinatorOptions{
				Module: gateRecoveryModule{source: source}, Persistence: selected.persistence,
				TimerScheduler: scheduler, WorkOwner: pipelineExternalTestWorkOwner(t),
			})
			bus.SetInterceptors(coordinator)
			t.Cleanup(func() {
				stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				_ = coordinator.StopWorkflowTimerLifecycle(stopCtx)
			})

			createdAt := time.Now().UTC()
			{
				construction579Ctx := ctx
				construction579At := createdAt
				construction579Instance, construction579Lifecycle, err := coordinator.PrepareInitialEntryLifecycle(construction579Ctx, testRunScopedWorkflowInstanceForRun(runID, runID), runtimepipeline.WorkflowInstance{
					InstanceID: runID, StorageRef: runID, EntityID: entityID, WorkflowName: ".", WorkflowVersion: source.WorkflowVersion(),
					CurrentState: "waiting", StageDefined: true, EnteredStageAt: createdAt, CreatedAt: createdAt,
					Fields:     map[string]any{},
					EntityType: "test_entity",
				}, construction579At)
				if err != nil {
					t.Fatalf("prepare fixture initial lifecycle: %v", err)
				}
				construction579Command, err := flowactivationfixture.Command(construction579Ctx, construction579Instance, construction579Lifecycle, construction579At)
				if err != nil {
					t.Fatalf("prepare fixture activation command: %v", err)
				}
				construction579Committed, err := any(selected.events).(runtimebus.FlowInstanceActivationCommitOwner).CommitFlowInstanceActivation(construction579Ctx, construction579Command)
				if err != nil {
					t.Fatalf("materialize workflow instance: %v", err)
				}
				if err == nil && !construction579Committed.Acknowledged {
					t.Fatal("fixture activation was not acknowledged")
				}
				if construction579Committed.Acknowledged && construction579Committed.Created {
					if finalizeErr := coordinator.FinalizeInitialEntryLifecycle(construction579Ctx, construction579Committed.Lifecycle); finalizeErr != nil {
						t.Fatalf("finalize fixture initial lifecycle: %v", finalizeErr)
					}
				}
			}
			if err := coordinator.ArmInitialEntryTimers(ctx, testRunScopedWorkflowInstanceForRun(runID, runID)); err != nil {
				t.Fatalf("arm initial workflow timers: %v", err)
			}

			select {
			case <-admitter.secondAttempt:
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for same-process workflow timer retry")
			}

			occurrence, status := workflowTimerPersistedOccurrence(t, selected, runID, entityID)
			if status != "active" {
				t.Fatalf("workflow timer status during retried publication = %q, want active", status)
			}
			if got := workflowTimerEventCount(t, selected, runID, runtimecontracts.WorkflowStageTimerInternalEvent); got != 0 {
				t.Fatalf("persisted events before retried publication commit = %d, want 0", got)
			}
			wantEventID := timeridentity.WorkflowTimerOccurrenceEventID(occurrence)
			close(admitter.releaseSecond)

			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) {
				_, status = workflowTimerPersistedActivation(t, selected, runID, entityID)
				if status == "fired" && workflowTimerEventCount(t, selected, runID, runtimecontracts.WorkflowStageTimerInternalEvent) == 1 {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			if got := workflowTimerPersistedEventID(t, selected, runID); got != wantEventID {
				t.Fatalf("retried workflow timer event id = %q, want %q", got, wantEventID)
			}
			_, status = workflowTimerPersistedActivation(t, selected, runID, entityID)
			if status != "fired" {
				t.Fatalf("workflow timer after retry status = %s, want fired", status)
			}
			if got := admitter.attempts.Load(); got != 2 {
				t.Fatalf("workflow timer publish attempts = %d, want 2", got)
			}
		})
	}
}

func TestWorkflowTimerAcceptedEventReceiptRecoveryIsIdempotentOnBothStores(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{
		{name: "sqlite", open: openSQLiteGateRecoveryStore},
		{name: "postgres", open: openPostgresGateRecoveryStore},
	} {
		t.Run(tc.name, func(t *testing.T) {
			selected := tc.open(t)
			runID := uuid.NewString()
			entityID := uuid.NewString()
			bundle := workflowTimerServedLifecycleBundle(t, false)
			ctx, fact := workflowLifecycleSourceContext(t, selected, bundle, runID)
			source := semanticview.Wrap(bundle)
			failingOwner, failures := failNextWorkflowTimerPipelineDisposition(t, selected.events)
			bus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{
				ContractBundle: source, SourceArtifactFact: fact, PayloadAdmitter: strictWorkflowTimerPayloadAdmitter,
				PipelineObligations: failingOwner,
			}, runtimecontracts.WorkflowStageTimerInternalEvent)
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}

			fireErrors := make(chan error, 4)
			scheduler := runtimepipeline.NewSchedulerWithWorkOwner(pipelineExternalTestWorkOwner(t))
			t.Cleanup(scheduler.Stop)
			coordinator := newGateRecoveryCoordinator(bus, selected, runtimepipeline.PipelineCoordinatorOptions{
				Module: gateRecoveryModule{source: source}, Persistence: selected.persistence,
				TimerScheduler: scheduler, WorkOwner: pipelineExternalTestWorkOwner(t),
			})
			bus.SetInterceptors(coordinator)

			createdAt := time.Now().UTC()
			{
				construction663Ctx := ctx
				construction663At := createdAt
				construction663Instance, construction663Lifecycle, err := coordinator.PrepareInitialEntryLifecycle(construction663Ctx, testRunScopedWorkflowInstanceForRun(runID, runID), runtimepipeline.WorkflowInstance{
					InstanceID: runID, StorageRef: runID, EntityID: entityID, WorkflowName: ".", WorkflowVersion: source.WorkflowVersion(),
					CurrentState: "waiting", StageDefined: true, EnteredStageAt: createdAt, CreatedAt: createdAt,
					Fields:     map[string]any{},
					EntityType: "test_entity",
				}, construction663At)
				if err != nil {
					t.Fatalf("prepare fixture initial lifecycle: %v", err)
				}
				construction663Command, err := flowactivationfixture.Command(construction663Ctx, construction663Instance, construction663Lifecycle, construction663At)
				if err != nil {
					t.Fatalf("prepare fixture activation command: %v", err)
				}
				construction663Committed, err := any(selected.events).(runtimebus.FlowInstanceActivationCommitOwner).CommitFlowInstanceActivation(construction663Ctx, construction663Command)
				if err != nil {
					t.Fatalf("materialize workflow instance: %v", err)
				}
				if err == nil && !construction663Committed.Acknowledged {
					t.Fatal("fixture activation was not acknowledged")
				}
				if construction663Committed.Acknowledged && construction663Committed.Created {
					if finalizeErr := coordinator.FinalizeInitialEntryLifecycle(construction663Ctx, construction663Committed.Lifecycle); finalizeErr != nil {
						t.Fatalf("finalize fixture initial lifecycle: %v", finalizeErr)
					}
				}
			}
			if err := coordinator.ArmInitialEntryTimers(ctx, testRunScopedWorkflowInstanceForRun(runID, runID)); err != nil {
				t.Fatalf("arm initial workflow timers: %v", err)
			}

			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) {
				select {
				case err := <-fireErrors:
					t.Fatalf("workflow timer callback: %v", err)
				default:
				}
				instance, found, err := coordinator.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, runID))
				if err != nil {
					t.Fatalf("load workflow instance: %v", err)
				}
				if found && instance.CurrentState == "done" && failures.Load() == 0 {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			if failures.Load() != 0 {
				t.Fatal("injected pipeline receipt failure was not reached")
			}
			eventID := workflowTimerPersistedEventID(t, selected, runID)
			if got := gateRecoveryPipelineReceiptCount(t, selected, eventID); got != 0 {
				t.Fatalf("pipeline receipts before recovery = %d, want 0", got)
			}

			recovered := 0
			for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline) && recovered == 0; {
				result, err := bus.SweepPipelineObligations(ctx, 10)
				if err != nil {
					t.Fatalf("SweepPipelineObligations: %v", err)
				}
				recovered = result.Settled
				if recovered == 0 {
					time.Sleep(10 * time.Millisecond)
				}
			}
			if recovered != 1 {
				t.Fatalf("SweepPipelineObligations recovered=%d, want 1", recovered)
			}
			instance, found, err := coordinator.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, runID))
			if err != nil || !found {
				t.Fatalf("load recovered workflow instance found=%v err=%v", found, err)
			}
			if instance.CurrentState != "done" || len(instance.TransitionHistory) != 1 || instance.TransitionHistory[0].TriggerEventID != eventID {
				t.Fatalf("recovered workflow lifecycle = state:%s history:%#v, want one exact timer transition", instance.CurrentState, instance.TransitionHistory)
			}
			if got := gateRecoveryPipelineReceiptCount(t, selected, eventID); got != 1 {
				t.Fatalf("pipeline receipts after recovery = %d, want 1", got)
			}
		})
	}
}

func strictWorkflowTimerPayloadAdmitter(_ context.Context, event events.Event, flowID string) (events.PayloadAdmission, error) {
	if event.Type() != runtimecontracts.WorkflowStageTimerInternalEvent {
		return eventtest.PayloadAdmission(event, flowID, string(event.Type()))
	}
	var decoded map[string]any
	if err := json.Unmarshal(event.Payload(), &decoded); err != nil {
		return events.PayloadAdmission{}, err
	}
	if err := runtimeeventschema.ValidatePayloadAgainstSchema(map[string]any{
		"type": "object", "properties": map[string]any{}, "additionalProperties": false,
	}, decoded); err != nil {
		return events.PayloadAdmission{}, err
	}
	return eventtest.PayloadAdmission(event, flowID, string(event.Type()))
}

var errInjectedWorkflowTimerPublishFailure = errors.New("injected workflow timer publish failure")

type failOnceWorkflowTimerPayloadAdmitter struct {
	attempts      atomic.Int32
	secondAttempt chan struct{}
	releaseSecond chan struct{}
}

func newFailOnceWorkflowTimerPayloadAdmitter() *failOnceWorkflowTimerPayloadAdmitter {
	return &failOnceWorkflowTimerPayloadAdmitter{
		secondAttempt: make(chan struct{}),
		releaseSecond: make(chan struct{}),
	}
}

func (v *failOnceWorkflowTimerPayloadAdmitter) admit(ctx context.Context, event events.Event, flowID string) (events.PayloadAdmission, error) {
	admitted, err := strictWorkflowTimerPayloadAdmitter(ctx, event, flowID)
	if err != nil || event.Type() != runtimecontracts.WorkflowStageTimerInternalEvent {
		return admitted, err
	}
	attempt := v.attempts.Add(1)
	if attempt == 1 {
		return events.PayloadAdmission{}, errInjectedWorkflowTimerPublishFailure
	}
	if attempt == 2 {
		close(v.secondAttempt)
		select {
		case <-ctx.Done():
			return events.PayloadAdmission{}, ctx.Err()
		case <-v.releaseSecond:
		}
	}
	return admitted, nil
}

type failOncePipelineDispositionStore struct {
	runtimepipelineobligation.Store
	failures atomic.Int32
}

func (s *failOncePipelineDispositionStore) Settle(
	ctx context.Context,
	claim runtimepipelineobligation.Claim,
	disposition runtimepipelineobligation.Disposition,
) (runtimepipelineobligation.SettlementOutcome, error) {
	if s.failures.CompareAndSwap(1, 0) {
		return runtimepipelineobligation.SettlementOutcome{}, errors.New("injected workflow timer pipeline disposition failure")
	}
	return s.Store.Settle(ctx, claim, disposition)
}

func failNextWorkflowTimerPipelineDisposition(t *testing.T, selected runtimebus.EventStore) (runtimepipelineobligation.Store, *atomic.Int32) {
	t.Helper()
	var owner runtimepipelineobligation.Store
	switch typed := selected.(type) {
	case *store.PostgresStore:
		owner = typed.PipelineObligations()
	case *store.SQLiteRuntimeStore:
		owner = typed.PipelineObligations()
	default:
		t.Fatalf("unsupported selected event store %T", selected)
		return nil, nil
	}
	wrapped := &failOncePipelineDispositionStore{Store: owner}
	wrapped.failures.Store(1)
	return wrapped, &wrapped.failures
}

func workflowTimerPersistedEventID(t *testing.T, selected gateRecoveryStoreCase, runID string) string {
	t.Helper()
	query := `SELECT event_id FROM events WHERE run_id = ? AND event_name = ?`
	if selected.postgres {
		query = `SELECT event_id::text FROM events WHERE run_id = $1::uuid AND event_name = $2`
	}
	var eventID string
	if err := selected.db.QueryRowContext(context.Background(), query, runID, runtimecontracts.WorkflowStageTimerInternalEvent).Scan(&eventID); err != nil {
		t.Fatalf("load persisted workflow timer event: %v", err)
	}
	return eventID
}

func workflowTimerPersistedActivation(t *testing.T, selected gateRecoveryStoreCase, runID, entityID string) (timeridentity.WorkflowTimerActivationRef, string) {
	t.Helper()
	query := `SELECT timer_name, status FROM timers WHERE run_id = ? AND entity_id = ? AND task_type = 'workflow_timer'`
	if selected.postgres {
		query = `SELECT timer_name, status FROM timers WHERE run_id = $1::uuid AND entity_id = $2::uuid AND task_type = 'workflow_timer'`
	}
	var taskID, status string
	if err := selected.db.QueryRowContext(context.Background(), query, runID, entityID).Scan(&taskID, &status); err != nil {
		t.Fatalf("load persisted workflow timer activation: %v", err)
	}
	ref, ok := timeridentity.ParseWorkflowTimerActivationTaskID(taskID)
	if !ok {
		t.Fatalf("persisted workflow timer task id is invalid: %q", taskID)
	}
	return ref, status
}

func workflowTimerPersistedOccurrence(t *testing.T, selected gateRecoveryStoreCase, runID, entityID string) (timeridentity.WorkflowTimerOccurrenceRef, string) {
	t.Helper()
	query := `SELECT timer_name, fire_at, status FROM timers WHERE run_id = ? AND entity_id = ? AND task_type = 'workflow_timer'`
	if selected.postgres {
		query = `SELECT timer_name, fire_at, status FROM timers WHERE run_id = $1::uuid AND entity_id = $2::uuid AND task_type = 'workflow_timer'`
	}
	var taskID, status string
	var dueAtValue any
	if err := selected.db.QueryRowContext(context.Background(), query, runID, entityID).Scan(&taskID, &dueAtValue, &status); err != nil {
		t.Fatalf("load persisted workflow timer occurrence: %v", err)
	}
	dueAt, err := workflowTimerTestTimeValue(dueAtValue)
	if err != nil {
		t.Fatalf("parse persisted workflow timer due time: %v", err)
	}
	ref, ok := timeridentity.ParseWorkflowTimerActivationTaskID(taskID)
	if !ok {
		t.Fatalf("persisted workflow timer task id is invalid: %q", taskID)
	}
	return timeridentity.WorkflowTimerOccurrenceRef{Activation: ref, DueAt: dueAt.UTC()}, status
}

func workflowTimerTestTimeValue(raw any) (time.Time, error) {
	switch value := raw.(type) {
	case time.Time:
		return value.UTC(), nil
	case string:
		return workflowTimerTestParseTime(value)
	case []byte:
		return workflowTimerTestParseTime(string(value))
	default:
		return time.Time{}, fmt.Errorf("unsupported timestamp value %T", raw)
	}
}

func workflowTimerTestParseTime(raw string) (time.Time, error) {
	formats := []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999 -0700 MST",
		"2006-01-02 15:04:05.999999 -0700 MST",
		"2006-01-02 15:04:05 -0700 MST",
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05-07:00",
		"2006-01-02 15:04:05Z07:00",
		"2006-01-02 15:04:05",
	}
	var lastErr error
	for _, format := range formats {
		parsed, err := time.Parse(format, raw)
		if err == nil {
			return parsed.UTC(), nil
		}
		lastErr = err
	}
	return time.Time{}, fmt.Errorf("parse timestamp %q: %w", raw, lastErr)
}

func workflowTimerServedLifecycleBundle(t *testing.T, recurring bool) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	bundle := loadPipelineLifecycleFixtureBundle(t, map[string]string{
		"schema.yaml":   "name: timer-proof\nstages:\n  waiting:\n    initial: true\n    timers:\n      - {id: waiting.timeout, after: 40ms, advances_to: done}\n  done: {terminal: true}\n",
		"entities.yaml": "test_entity: {}\n",
	})
	// Recurrence is a runtime scheduler variant; the admitted timer transition stays exact.
	bundle.Semantics.Timers[0].Recurring = recurring
	return bundle
}

func workflowLifecycleSourceContext(t *testing.T, selected gateRecoveryStoreCase, bundle *runtimecontracts.WorkflowContractBundle, runID string) (context.Context, runtimecorrelation.SourceArtifactFact) {
	t.Helper()
	fact := mustAuthorActivityTestSourceArtifactFactForHash(bundle.SourceArtifact.BundleHash())
	ctx := withLiveGateExecution(runtimecorrelation.WithRunID(testAuthorActivityContextForSource(t, context.Background(), fact), runID))
	fixture := runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runID, Artifact: bundle.SourceArtifact}
	if selected.postgres {
		runlifecyclefixture.RequirePostgres(t, ctx, selected.db, fixture)
	} else {
		runlifecyclefixture.RequireSQLite(t, ctx, selected.db, fixture)
	}
	return ctx, fact
}

func workflowTimerRecurringCancellationSource(t *testing.T) semanticview.Source {
	t.Helper()
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	fixtureRoot := canonicalrouting.CopyRecurringTimerCancellation(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot,
		fixtureRoot,
		runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("load recurring timer cancellation fixture: %v", err)
	}
	timerBundle := workflowTimerServedLifecycleBundle(t, true)
	timerBundle.Semantics.Timers[0].AdvancesTo = ""
	bundle.Semantics.Timers = timerBundle.Semantics.Timers
	return semanticview.Wrap(bundle)
}

func assertWorkflowTimerServedRows(t *testing.T, selected gateRecoveryStoreCase, runID, entityID, status string, want int) {
	t.Helper()
	query := `SELECT COUNT(*) FROM timers WHERE run_id = ? AND entity_id = ? AND task_type = 'workflow_timer' AND status = ?`
	if selected.postgres {
		query = `SELECT COUNT(*) FROM timers WHERE run_id = $1::uuid AND entity_id = $2::uuid AND task_type = 'workflow_timer' AND status = $3`
	}
	var got int
	if err := selected.db.QueryRowContext(context.Background(), query, runID, entityID, status).Scan(&got); err != nil {
		t.Fatalf("count canonical workflow timers: %v", err)
	}
	if got != want {
		t.Fatalf("canonical workflow timers status=%s = %d, want %d", status, got, want)
	}
}

func waitWorkflowTimerRowStatus(t *testing.T, selected gateRecoveryStoreCase, runID, entityID, status string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		query := `SELECT COUNT(*) FROM timers WHERE run_id = ? AND entity_id = ? AND task_type = 'workflow_timer' AND status = ?`
		if selected.postgres {
			query = `SELECT COUNT(*) FROM timers WHERE run_id = $1::uuid AND entity_id = $2::uuid AND task_type = 'workflow_timer' AND status = $3`
		}
		var count int
		if err := selected.db.QueryRowContext(context.Background(), query, runID, entityID, status).Scan(&count); err != nil {
			t.Fatalf("read workflow timer status: %v", err)
		}
		if count == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("workflow timer did not reach status %q", status)
}

func workflowTimerDeliveryTrace(t *testing.T, selected gateRecoveryStoreCase, runID string) []string {
	t.Helper()
	query := `SELECT e.event_name, COALESCE(d.subscriber_id, ''), COALESCE(d.status, '') FROM events e LEFT JOIN event_deliveries d ON d.event_id = e.event_id WHERE e.run_id = ? ORDER BY e.created_at`
	if selected.postgres {
		query = `SELECT e.event_name, COALESCE(d.subscriber_id, ''), COALESCE(d.status, '') FROM events e LEFT JOIN event_deliveries d ON d.event_id = e.event_id WHERE e.run_id = $1::uuid ORDER BY e.created_at`
	}
	rows, err := selected.db.QueryContext(context.Background(), query, runID)
	if err != nil {
		return []string{"readback error: " + err.Error()}
	}
	defer rows.Close()
	var trace []string
	for rows.Next() {
		var eventName, subscriberID, status string
		if err := rows.Scan(&eventName, &subscriberID, &status); err != nil {
			return append(trace, "scan error: "+err.Error())
		}
		trace = append(trace, eventName+" subscriber="+subscriberID+" status="+status)
	}
	var runtimeLog string
	logQuery := `SELECT CAST(payload AS TEXT) FROM events WHERE event_name = 'platform.runtime_log' ORDER BY created_at DESC LIMIT 1`
	if selected.postgres {
		logQuery = `SELECT payload::text FROM events WHERE event_name = 'platform.runtime_log' ORDER BY created_at DESC LIMIT 1`
	}
	if err := selected.db.QueryRowContext(context.Background(), logQuery).Scan(&runtimeLog); err == nil {
		trace = append(trace, "runtime_log="+runtimeLog)
	}
	return trace
}

func waitWorkflowTimerEventCount(t *testing.T, selected gateRecoveryStoreCase, fireErrors <-chan error, runID, eventType string, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-fireErrors:
			t.Fatalf("workflow timer callback: %v", err)
		default:
		}
		if workflowTimerEventCount(t, selected, runID, eventType) >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("workflow timer event count did not reach %d", want)
}

func workflowTimerEventCount(t *testing.T, selected gateRecoveryStoreCase, runID, eventType string) int {
	t.Helper()
	query := `SELECT COUNT(*) FROM events WHERE run_id = ? AND event_name = ?`
	if selected.postgres {
		query = `SELECT COUNT(*) FROM events WHERE run_id = $1::uuid AND event_name = $2`
	}
	var count int
	if err := selected.db.QueryRowContext(context.Background(), query, runID, eventType).Scan(&count); err != nil {
		t.Fatalf("count workflow timer events: %v", err)
	}
	return count
}
