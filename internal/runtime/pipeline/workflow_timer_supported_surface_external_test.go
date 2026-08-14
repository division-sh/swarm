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
	runtimeeventschema "github.com/division-sh/swarm/internal/runtime/eventschema"
	runtimelifecycleprobe "github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store"
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
			insertGateRecoveryRun(t, selected, runID)
			ctx := withLiveGateExecution(runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), runID))
			source := semanticview.Wrap(workflowTimerServedLifecycleBundle(false))
			bus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{
				ContractBundle: source, PayloadValidator: strictWorkflowTimerPayloadValidator,
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
			if _, err := coordinator.MaterializeInitialEntry(ctx, runtimepipeline.WorkflowInstance{
				InstanceID: runID, StorageRef: runID, EntityID: entityID, WorkflowName: "timer-proof", WorkflowVersion: "1",
				CurrentState: "waiting", EnteredStageAt: createdAt, CreatedAt: createdAt,
				Fields: map[string]any{"run_id": runID, "entity_id": entityID, "flow_path": runID, "instance_id": runID},
			}, createdAt); err != nil {
				t.Fatalf("materialize workflow instance: %v", err)
			}
			if err := coordinator.ArmInitialEntryTimers(ctx, testWorkflowInstanceRoute(runID)); err != nil {
				t.Fatalf("arm initial workflow timers: %v", err)
			}

			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) {
				select {
				case err := <-fireErrors:
					t.Fatalf("workflow timer callback: %v", err)
				default:
				}
				instance, found, err := coordinator.Load(ctx, testWorkflowInstanceRoute(runID))
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
	fixtureRoot := canonicalrouting.CopyExample(t, canonicalrouting.ParentConnect)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot,
		fixtureRoot,
		runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("load parent-connect timer fixture: %v", err)
	}
	bundle.Semantics.Timers = []runtimecontracts.WorkflowTimerContract{{
		ID: "waiting.work_ready", FlowID: "producer", Stage: "waiting", StageOwned: true,
		Owner: "runtime", Event: "work.ready", StartOn: "state:waiting", Delay: "40ms",
	}}
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
			insertGateRecoveryRun(t, selected, runID)
			ctx := withLiveGateExecution(runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), runID))
			eventBus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{ContractBundle: source})
			if err != nil {
				t.Fatalf("new authored timer EventBus: %v", err)
			}
			deliveries := internalSubscriptionDeliveriesForTest(t, eventBus, "consumer-node")
			scheduler := runtimepipeline.NewSchedulerWithWorkOwner(pipelineExternalTestWorkOwner(t))
			t.Cleanup(scheduler.Stop)
			coordinator := newGateRecoveryCoordinator(eventBus, selected, runtimepipeline.PipelineCoordinatorOptions{
				Module:         gateRecoveryModule{source: source},
				Persistence:    selected.persistence,
				TimerScheduler: scheduler,
				WorkOwner:      pipelineExternalTestWorkOwner(t),
			})
			eventBus.SetInterceptors(coordinator)

			createdAt := time.Now().UTC()
			if _, err := coordinator.MaterializeInitialEntry(ctx, runtimepipeline.WorkflowInstance{
				InstanceID: "producer", StorageRef: "producer", EntityID: entityID, WorkflowName: "producer", WorkflowVersion: "1.0.0",
				CurrentState: "waiting", EnteredStageAt: createdAt, CreatedAt: createdAt,
				Fields: map[string]any{"run_id": runID, "entity_id": entityID, "flow_path": "producer", "instance_id": "producer"},
			}, createdAt); err != nil {
				t.Fatalf("materialize producer workflow instance: %v", err)
			}
			if err := coordinator.ArmInitialEntryTimers(ctx, testWorkflowInstanceRoute("producer")); err != nil {
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
				if route.Recipient.ID() != "consumer-node" || route.ConnectClaim.Empty() {
					t.Fatalf("timer delivery route = %#v, want consumer-node with stamped connect claim", route)
				}
				if err := delivery.Complete(); err != nil {
					t.Fatalf("complete authored timer delivery: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("authored workflow timer did not deliver through compiled connect")
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
			insertGateRecoveryRun(t, selected, runID)
			ctx := withLiveGateExecution(runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), runID))
			bundle := workflowTimerServedLifecycleBundle(true)
			bundle.Semantics.Timers[0].Delay = "5s"
			source := semanticview.Wrap(bundle)
			bus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{
				ContractBundle: source, PayloadValidator: strictWorkflowTimerPayloadValidator,
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
			if _, err := coordinator.MaterializeInitialEntry(ctx, runtimepipeline.WorkflowInstance{
				InstanceID: runID, StorageRef: runID, EntityID: entityID, WorkflowName: "timer-proof", WorkflowVersion: "1",
				CurrentState: "waiting", EnteredStageAt: createdAt, CreatedAt: createdAt,
				Fields: map[string]any{"run_id": runID, "entity_id": entityID, "flow_path": runID, "instance_id": runID},
			}, createdAt); err != nil {
				t.Fatalf("materialize workflow instance: %v", err)
			}
			if err := coordinator.ArmInitialEntryTimers(ctx, testWorkflowInstanceRoute(runID)); err != nil {
				t.Fatalf("arm initial workflow timers: %v", err)
			}

			stateDeadline := time.Now().Add(5 * time.Second)
			for {
				instance, found, err := coordinator.Load(ctx, testWorkflowInstanceRoute(runID))
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
			insertGateRecoveryRun(t, selected, runID)
			ctx := withLiveGateExecution(runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), runID))
			source := semanticview.Wrap(workflowTimerServedLifecycleBundle(false))
			bus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{
				ContractBundle: source, PayloadValidator: strictWorkflowTimerPayloadValidator,
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
			if _, err := coordinator.MaterializeInitialEntry(ctx, runtimepipeline.WorkflowInstance{
				InstanceID: runID, StorageRef: runID, EntityID: entityID, WorkflowName: "timer-proof", WorkflowVersion: "1",
				CurrentState: "waiting", EnteredStageAt: createdAt, CreatedAt: createdAt,
				Fields: map[string]any{"run_id": runID, "entity_id": entityID, "flow_path": runID, "instance_id": runID},
			}, createdAt); err != nil {
				t.Fatalf("materialize timer before restart: %v", err)
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
				instance, found, err := restored.Load(ctx, testWorkflowInstanceRoute(runID))
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
				t.Fatal("restored one-shot timer did not advance through the real EventBus path")
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
			entityID := uuid.NewString()
			insertGateRecoveryRun(t, selected, runID)
			ctx := withLiveGateExecution(runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), runID))
			source := workflowTimerRecurringCancellationSource(t)
			lifecycleProbe := runtimelifecycleprobe.New()
			module := proposedEffectProofModule{
				source: source,
				workflow: runtimepipeline.NewWorkflowDefinition("timer-proof", []runtimepipeline.WorkflowStage{
					{Name: "waiting"},
					{Name: "done", Terminal: true},
				}, []runtimepipeline.WorkflowTransition{{
					Name: "cancel", From: []runtimepipeline.WorkflowStateID{"waiting"}, To: "done",
					Trigger: "timer.cancel", Node: "controller",
				}}),
				nodes: []runtimepipeline.WorkflowNode{{
					ID: "controller", Subscriptions: []events.EventType{"timer-proof/timer.cancel"},
					ExecutionType: runtimecontracts.SystemNodeExecutionType,
					Policies: map[string]runtimepipeline.WorkflowEventPolicy{
						"timer-proof/timer.cancel": {Consume: true},
					},
				}},
			}
			bus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{
				ContractBundle: source, PayloadValidator: strictWorkflowTimerPayloadValidator, TestLifecycleProbe: lifecycleProbe,
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
			if _, err := coordinator.MaterializeInitialEntry(ctx, runtimepipeline.WorkflowInstance{
				InstanceID: runID, StorageRef: runID, EntityID: entityID, WorkflowName: "timer-proof", WorkflowVersion: "1",
				CurrentState: "waiting", EnteredStageAt: createdAt, CreatedAt: createdAt,
				Fields: map[string]any{"run_id": runID, "entity_id": entityID, "flow_path": runID, "instance_id": runID},
			}, createdAt); err != nil {
				t.Fatalf("materialize workflow instance: %v", err)
			}
			if err := coordinator.ArmInitialEntryTimers(ctx, testWorkflowInstanceRoute(runID)); err != nil {
				t.Fatalf("arm initial workflow timers: %v", err)
			}
			armed, found, err := coordinator.Load(ctx, testWorkflowInstanceRoute(runID))
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
			beforeCancel, found, err := coordinator.Load(ctx, testWorkflowInstanceRoute(runID))
			if err != nil || !found {
				t.Fatalf("load workflow instance before cancellation: found=%v err=%v", found, err)
			}
			if beforeCancel.Revision != armedRevision {
				t.Fatalf("non-advancing recurring timers changed workflow revision from %d to %d", armedRevision, beforeCancel.Revision)
			}

			cancelEvent := eventtest.ExistingRunRootIngress(
				uuid.NewString(), "timer.cancel", "operator", "", []byte(`{}`), 0, runID,
				events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), "timer-proof"), time.Now().UTC(),
			)
			plan, err := bus.CheckPublishRecipientPlan(ctx, cancelEvent)
			if err != nil {
				t.Fatalf("plan timer cancellation transition: %v", err)
			}
			if got := plan.DeliveryRoutes; len(got) != 1 || !got[0].Recipient.IsNode() || got[0].Recipient.ID() != "controller" {
				t.Fatalf("timer cancellation delivery routes = %#v, want exact controller node route", got)
			}
			wantTarget := events.RouteIdentity{FlowID: "timer-proof", FlowInstance: runID, EntityID: entityID}
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
			if _, err := lifecycleProbe.WaitForDeliveryStatus(probeCtx, cancelEvent.ID(), "node", "controller", "in_progress"); err != nil {
				t.Fatalf("wait for timer cancellation delivery claim: %v", err)
			}
			if _, err := lifecycleProbe.WaitForHandlerStarted(probeCtx, cancelEvent.ID(), "controller"); err != nil {
				t.Fatalf("wait for timer cancellation handler start: %v", err)
			}
			handlerCompletion, err := lifecycleProbe.WaitForHandlerCompleted(probeCtx, cancelEvent.ID(), "controller")
			if err != nil {
				t.Fatalf("wait for timer cancellation handler completion: %v", err)
			}
			if handlerCompletion.Status != "completed" {
				query := `SELECT status, COALESCE(failure, '{}') FROM event_deliveries WHERE event_id = ? AND subscriber_type = 'node' AND subscriber_id = 'controller'`
				if selected.postgres {
					query = `SELECT status, COALESCE(failure, '{}'::jsonb) FROM event_deliveries WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'controller'`
				}
				var deliveryStatus string
				var failure []byte
				if err := selected.db.QueryRowContext(ctx, query, cancelEvent.ID()).Scan(&deliveryStatus, &failure); err != nil {
					t.Fatalf("timer cancellation handler status = %q and delivery failure readback failed: %v", handlerCompletion.Status, err)
				}
				t.Fatalf("timer cancellation handler status = %q, delivery status = %q failure = %s; want completed", handlerCompletion.Status, deliveryStatus, failure)
			}
			cancelled := false
			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) {
				instance, found, err := coordinator.Load(ctx, testWorkflowInstanceRoute(runID))
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
			insertGateRecoveryRun(t, selected, runID)
			ctx := withLiveGateExecution(runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), runID))
			bundle := workflowTimerServedLifecycleBundle(false)
			bundle.Semantics.Timers[0].Delay = "200ms"
			source := semanticview.Wrap(bundle)
			validator := newFailOnceWorkflowTimerPayloadValidator()
			defer func() {
				select {
				case <-validator.releaseSecond:
				default:
					close(validator.releaseSecond)
				}
			}()
			bus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{
				ContractBundle: source, PayloadValidator: validator.validate,
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
			if _, err := coordinator.MaterializeInitialEntry(ctx, runtimepipeline.WorkflowInstance{
				InstanceID: runID, StorageRef: runID, EntityID: entityID, WorkflowName: "timer-proof", WorkflowVersion: "1",
				CurrentState: "waiting", EnteredStageAt: createdAt, CreatedAt: createdAt,
				Fields: map[string]any{"run_id": runID, "entity_id": entityID, "flow_path": runID, "instance_id": runID},
			}, createdAt); err != nil {
				t.Fatalf("materialize workflow instance: %v", err)
			}
			if err := coordinator.ArmInitialEntryTimers(ctx, testWorkflowInstanceRoute(runID)); err != nil {
				t.Fatalf("arm initial workflow timers: %v", err)
			}

			select {
			case <-validator.secondAttempt:
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
			close(validator.releaseSecond)

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
			if got := validator.attempts.Load(); got != 2 {
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
			insertGateRecoveryRun(t, selected, runID)
			ctx := withLiveGateExecution(runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), runID))
			source := semanticview.Wrap(workflowTimerServedLifecycleBundle(false))
			failingOwner, failures := failNextWorkflowTimerPipelineDisposition(t, selected.events)
			bus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{
				ContractBundle: source, PayloadValidator: strictWorkflowTimerPayloadValidator,
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
			if _, err := coordinator.MaterializeInitialEntry(ctx, runtimepipeline.WorkflowInstance{
				InstanceID: runID, StorageRef: runID, EntityID: entityID, WorkflowName: "timer-proof", WorkflowVersion: "1",
				CurrentState: "waiting", EnteredStageAt: createdAt, CreatedAt: createdAt,
				Fields: map[string]any{"run_id": runID, "entity_id": entityID, "flow_path": runID, "instance_id": runID},
			}, createdAt); err != nil {
				t.Fatalf("materialize workflow instance: %v", err)
			}
			if err := coordinator.ArmInitialEntryTimers(ctx, testWorkflowInstanceRoute(runID)); err != nil {
				t.Fatalf("arm initial workflow timers: %v", err)
			}

			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) {
				select {
				case err := <-fireErrors:
					t.Fatalf("workflow timer callback: %v", err)
				default:
				}
				instance, found, err := coordinator.Load(ctx, testWorkflowInstanceRoute(runID))
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
			instance, found, err := coordinator.Load(ctx, testWorkflowInstanceRoute(runID))
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

func strictWorkflowTimerPayloadValidator(_ context.Context, eventType string, payload []byte) error {
	if eventType != runtimecontracts.WorkflowStageTimerInternalEvent {
		return nil
	}
	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return err
	}
	return runtimeeventschema.ValidatePayloadAgainstSchema(map[string]any{
		"type": "object", "properties": map[string]any{}, "additionalProperties": false,
	}, decoded)
}

var errInjectedWorkflowTimerPublishFailure = errors.New("injected workflow timer publish failure")

type failOnceWorkflowTimerPayloadValidator struct {
	attempts      atomic.Int32
	secondAttempt chan struct{}
	releaseSecond chan struct{}
}

func newFailOnceWorkflowTimerPayloadValidator() *failOnceWorkflowTimerPayloadValidator {
	return &failOnceWorkflowTimerPayloadValidator{
		secondAttempt: make(chan struct{}),
		releaseSecond: make(chan struct{}),
	}
}

func (v *failOnceWorkflowTimerPayloadValidator) validate(ctx context.Context, eventType string, payload []byte) error {
	if err := strictWorkflowTimerPayloadValidator(ctx, eventType, payload); err != nil || eventType != runtimecontracts.WorkflowStageTimerInternalEvent {
		return err
	}
	attempt := v.attempts.Add(1)
	if attempt == 1 {
		return errInjectedWorkflowTimerPublishFailure
	}
	if attempt == 2 {
		close(v.secondAttempt)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-v.releaseSecond:
		}
	}
	return nil
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

func workflowTimerServedLifecycleBundle(recurring bool) *runtimecontracts.WorkflowContractBundle {
	return &runtimecontracts.WorkflowContractBundle{Semantics: runtimecontracts.WorkflowSemanticView{
		Name: "timer-proof", Version: "1", InitialStage: "waiting", TerminalStages: []string{"done"},
		Timers: []runtimecontracts.WorkflowTimerContract{{
			ID: "waiting.timeout", Stage: "waiting", StageOwned: true, AdvancesTo: "done",
			Owner: "runtime", Event: runtimecontracts.WorkflowStageTimerInternalEvent,
			StartOn: "state:waiting", Delay: "40ms", Recurring: recurring,
		}},
	}}
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
	timerBundle := workflowTimerServedLifecycleBundle(true)
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
