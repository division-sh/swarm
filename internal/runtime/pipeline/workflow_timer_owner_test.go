package pipeline

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

func VerifyNativeWorkflowTimerLifecycleOneShotExactCompletionOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			_, pc, ctx, bus, entityID, activation := nativeWorkflowTimerOwnerActivationForTest(t, tc.name, false, "1h", time.Now().Add(-2*time.Hour), false, executionmode.Live, open)
			store := pc.workflowStore
			occurrence := activation.occurrence()

			outcome, err := fireWorkflowTimerTestWakeup(ctx, pc, activation)
			if err != nil || outcome != WorkflowTimerFireCommitted {
				t.Fatalf("FireWorkflowTimer outcome=%q err=%v, want committed", outcome, err)
			}
			if bus.publishedCount() != 1 {
				t.Fatalf("published events = %d, want 1", bus.publishedCount())
			}
			fired := bus.publishedEvent(0)
			if got, want := fired.ID(), timeridentity.WorkflowTimerOccurrenceEventID(occurrence); got != want {
				t.Fatalf("event id = %q, want %q", got, want)
			}
			persisted := loadWorkflowTimerOwnerActivation(t, store, ctx, activation.Ref.ActivationID)
			if persisted.Status != workflowTimerStatusFired || !persisted.FireAt.Equal(activation.FireAt) {
				t.Fatalf("persisted one-shot = %#v, want fired at original coordinate", persisted)
			}
			authorized, accepted, recognized, err := pc.workflowTimers.AuthorizeAcceptedEvent(ctx, fired)
			if err != nil || !recognized || authorized.Ref != activation.Ref || accepted != occurrence {
				t.Fatalf("AuthorizeAcceptedEvent recognized=%v activation=%#v occurrence=%#v err=%v", recognized, authorized, accepted, err)
			}

			outcome, err = fireWorkflowTimerTestWakeup(ctx, pc, activation)
			if err != nil || outcome != WorkflowTimerFireTerminal {
				t.Fatalf("retry outcome=%q err=%v, want terminal no-op", outcome, err)
			}
			if bus.publishedCount() != 1 {
				t.Fatalf("retry published events = %d, want 1 total", bus.publishedCount())
			}

			wrong := eventtest.RuntimeControl(
				uuid.NewString(), fired.Type(), fired.SourceAgent(), fired.TaskID(), fired.Payload(), 0,
				fired.RunID(), "", events.EventEnvelope{EntityID: entityID, FlowInstance: activation.Route.InstancePath}, fired.CreatedAt(),
			)
			if _, _, recognized, err := pc.workflowTimers.AuthorizeAcceptedEvent(ctx, wrong); err == nil || !recognized {
				t.Fatalf("wrong event id authorization recognized=%v err=%v, want recognized rejection", recognized, err)
			}
		})
	}
}

func VerifyNativeWorkflowTimerLifecyclePreservesMockExecutionModeOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			_, pc, ctx, bus, _, activation := nativeWorkflowTimerOwnerActivationForTest(t, tc.name, false, "1h", time.Now().Add(-2*time.Hour), false, executionmode.Mock, open)
			if activation.ExecutionMode != executionmode.Mock {
				t.Fatalf("persisted activation execution mode = %q, want mock", activation.ExecutionMode)
			}
			mode, err := initialWorkflowTimerExecutionMode(ctx, executionmode.Mock, []WorkflowTimerActivation{activation})
			if err != nil || mode != executionmode.Mock {
				t.Fatalf("reconciliation execution mode = %q err=%v, want persisted mock", mode, err)
			}
			mode, err = initialWorkflowTimerExecutionMode(ctx, "", []WorkflowTimerActivation{activation})
			if err != nil || mode != executionmode.Mock {
				t.Fatalf("static persisted execution mode = %q err=%v, want mock", mode, err)
			}
			conflicting := activation
			conflicting.ExecutionMode = executionmode.Live
			if _, err := initialWorkflowTimerExecutionMode(ctx, executionmode.Mock, []WorkflowTimerActivation{conflicting}); err == nil {
				t.Fatal("readiness mode accepted a conflicting persisted initial timer")
			}

			liveCtx := runtimeeffects.WithExecutionMode(ctx, executionmode.Live)
			outcome, err := fireWorkflowTimerTestWakeup(liveCtx, pc, activation)
			if err != nil || outcome != WorkflowTimerFireCommitted {
				t.Fatalf("FireWorkflowTimer outcome=%q err=%v, want committed", outcome, err)
			}
			if got := bus.publishedEvent(0).ExecutionMode(); got != executionmode.Mock {
				t.Fatalf("fired event execution mode = %q, want mock", got)
			}
		})
	}
}

func VerifyNativeWorkflowTimerLifecyclePreservesSiblingFlowDeclarationsAcrossRestartOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, tc := range workflowJoinStoreCases() {
		for _, order := range []struct {
			name    string
			reverse bool
		}{{name: "a_then_b"}, {name: "b_then_a", reverse: true}} {
			t.Run(tc.name+"/"+order.name, func(t *testing.T) {
				createdAt := canonicalWorkflowTimerTime(time.Now().UTC())
				first := pipelineFlowPathNode(t, "a", "shared")
				second := pipelineFlowPathNode(t, "b", "shared")
				files := map[string]string{
					"schema.yaml":   "name: workflow-timer-sibling-identity\nstages:\n  waiting: {}\n",
					"entities.yaml": "test_entity: {}\n",
				}
				for _, flow := range []string{"a", "b"} {
					files[flow+"/schema.yaml"] = fmt.Sprintf("name: %s\nstages:\n  waiting: {}\n", flow)
					files[flow+"/entities.yaml"] = "test_entity: {}\n"
					files[flow+"/events.yaml"] = fmt.Sprintf("timer.arm:\ntimer.%s:\n", flow)
					files[flow+"/nodes.yaml"] = fmt.Sprintf("shared:\n  execution_type: system_node\n  timers:\n    - {id: same, event: timer.%s, start_on: 'event:%s/timer.arm', delay: 2h}\n  event_handlers:\n    timer.arm: {}\n", flow, flow)
				}
				bundle := loadWorkflowTempBundle(t, files)
				if len(bundle.Semantics.Timers) != 2 {
					t.Fatalf("compiled sibling declarations=%d, want two", len(bundle.Semantics.Timers))
				}
				if order.reverse {
					bundle.Semantics.Timers[0], bundle.Semantics.Timers[1] = bundle.Semantics.Timers[1], bundle.Semantics.Timers[0]
				}
				fixture, pc, ctx := nativePilotPipelineForTest(t, tc.name, bundle, open)
				store := pc.workflowStore
				run := runtimecorrelation.RunIDFromContext(ctx)
				if err := fixture.Construct(ctx, materializedWorkflowInstanceForSource(t, pc.SemanticSource(), ctx, WorkflowInstance{
					InstanceID: run, StorageRef: run, EntityID: run, WorkflowName: ".", WorkflowVersion: bundle.WorkflowVersion(),
					CurrentState: "waiting", CreatedAt: createdAt, EnteredStageAt: createdAt, EntityType: "test_entity",
				})); err != nil {
					t.Fatal(err)
				}
				scheduler := newWorkflowTimerTestScheduler(t, pc.workOwner)
				if err := pc.workflowTimers.bindScheduler(scheduler); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					join, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					if err := pc.StopWorkflowTimerLifecycle(join); err != nil {
						t.Error(err)
					}
				})
				entityIDs := make(map[string]string, 2)
				for _, flowID := range []string{"a", "b"} {
					instance := constructedScenarioInstanceForTest(t, pc.SemanticSource(), ctx, flowID)
					instance.CreatedAt, instance.EnteredStageAt = createdAt, createdAt
					entityID := instance.EntityID
					entityIDs[flowID] = entityID
					route := testWorkflowInstanceRoute(instance.StorageRef)
					if err := fixture.Construct(ctx, instance); err != nil {
						t.Fatal(err)
					}
					inbound := nativeWorkflowJoinEventForTest(ctx, flowID, route.InstancePath, entityID, flowID+"/timer.arm", []byte(`{}`), createdAt)
					dispatchNativeWorkflowJoinEventForTest(t, fixture, pc, ctx, inbound, "shared")
				}
				active := make([]WorkflowTimerActivation, 0, 2)
				for _, flowID := range []string{"a", "b"} {
					active = append(active, listWorkflowTimerOwnerActivations(t, store, ctx, entityIDs[flowID], true)...)
				}
				if len(active) != 2 {
					t.Fatalf("active sibling timers = %#v, want two", active)
				}
				byDeclaration := workflowTimerActivationsByDeclaration(active)
				for _, key := range []string{workflowTimerNodeDeclarationKey(first, "same"), workflowTimerNodeDeclarationKey(second, "same")} {
					if activation, ok := byDeclaration[key]; !ok || activation.Ref.ActivationID == "" {
						t.Fatalf("sibling timer %q = %#v, want exact durable activation", key, activation)
					}
				}
				stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
				if err := pc.StopWorkflowTimerLifecycle(stopCtx); err != nil {
					cancel()
					t.Fatalf("stop predecessor timer lifecycle: %v", err)
				}
				cancel()

				nextFixture := fixture.ReopenExecution()
				restarted := nextFixture.NewCoordinator(PipelineCoordinatorOptions{Module: pc.module, Persistence: nextFixture.Persistence})
				restartedScheduler := newWorkflowTimerTestScheduler(t, restarted.workOwner)
				if err := restarted.workflowTimers.bindScheduler(restartedScheduler); err != nil {
					t.Fatal(err)
				}
				restartedCtx := runtimecorrelation.WithRunID(nextFixture.Context, run)
				t.Cleanup(func() {
					stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					_ = restarted.StopWorkflowTimerLifecycle(stopCtx)
				})
				if err := restarted.RestoreWorkflowTimers(restartedCtx); err != nil {
					t.Fatalf("restore package timers: %v", err)
				}
				if registered, draining := workflowTimerScheduledCounts(restartedScheduler); registered != 2 || draining != 0 {
					t.Fatalf("restored package wakeups active=%d draining=%d, want 2/0", registered, draining)
				}
			})
		}
	}
}

func VerifyNativeWorkflowTimerLifecycleFirstRevisedInitialTimerUsesDynamicReadinessModeOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			bundle := workflowTimerFirstDeclarationRevisionBundle(t, false)
			fixture, pcA, liveCtx := nativePilotPipelineForTest(t, tc.name, bundle, open)
			store := pcA.workflowStore
			createdAt := canonicalWorkflowTimerTime(time.Now().UTC().Add(-2 * time.Second))
			runID := runtimecorrelation.RunIDFromContext(liveCtx)
			route := workflowTimerRootRoute(liveCtx)
			entityID := runID
			mockCtx := runtimeeffects.WithExecutionMode(liveCtx, executionmode.Mock)
			preparedInstance, preparedLifecycle, err := pcA.PrepareInitialEntryLifecycle(mockCtx, testRunScopedWorkflowRoute(mockCtx, route), materializedWorkflowInstanceForSource(t, pcA.SemanticSource(), mockCtx, WorkflowInstance{
				InstanceID: runID, StorageRef: runID, EntityID: entityID,
				WorkflowName: ".", WorkflowVersion: bundle.WorkflowVersion(),
				CurrentState: "waiting", CreatedAt: createdAt,
				EntityType: "test_entity",
			}), createdAt)
			if err != nil {
				t.Fatalf("prepare fixture lifecycle: %v", err)
			}
			committedLifecycle, err := fixture.ConstructInitial(mockCtx, preparedInstance, preparedLifecycle)
			if err != nil {
				t.Fatalf("construct timer-free native lifecycle: %v", err)
			}
			if err := pcA.FinalizeInitialEntryLifecycle(mockCtx, committedLifecycle); err != nil {
				t.Fatalf("finalize fixture lifecycle: %v", err)
			}
			if err := pcA.ArmInitialEntryTimers(mockCtx, testRunScopedWorkflowRoute(mockCtx, route)); err != nil {
				t.Fatalf("arm timer-free source: %v", err)
			}
			if active := listWorkflowTimerOwnerActivations(t, store, liveCtx, entityID, true); len(active) != 0 {
				t.Fatalf("timer-free source active timers = %#v", active)
			}
			if err := pcA.StopWorkflowTimerLifecycle(liveCtx); err != nil {
				t.Fatalf("join timer-free predecessor: %v", err)
			}

			bus := observeNativePipelineDeliveryBusForTest(t, pcA)
			sourceB := semanticview.Wrap(workflowTimerFirstDeclarationRevisionBundle(t, true))
			scheduler := newWorkflowTimerTestScheduler(t, pcA.workOwner)
			// Install the real wakeup while retaining this unit's explicit fire boundary.
			if err := scheduler.PrepareStartup(); err != nil {
				t.Fatal(err)
			}
			// Like the progressed-declaration control, this projects declarations
			// without admitting a replacement source or executable runtime.
			projection := newWorkflowTimerLifecycle(store, sourceB, bus, pcA.workOwner, scheduler, pcA.executionPosture)
			attempt, readiness, closeAttachment := fixture.AdmitAttachment(liveCtx, runID, route.InstancePath)
			t.Cleanup(func() {
				join, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := projection.stop(join); err != nil {
					t.Error(err)
				}
				closeAttachment()
			})
			if readiness.ExecutionMode != executionmode.Mock {
				t.Fatalf("native construction readiness mode=%q, want mock", readiness.ExecutionMode)
			}
			if err := projection.reconcileInitialEntryDeclarations(liveCtx, testRunScopedWorkflowRoute(liveCtx, route), &attempt, &readiness); err != nil {
				t.Fatalf("reconcile first initial timer with live administrative context: %v", err)
			}
			active := listWorkflowTimerOwnerActivations(t, store, liveCtx, entityID, true)
			if len(active) != 1 || active[0].ExecutionMode != executionmode.Mock {
				t.Fatalf("first revised timer = %#v, want one mock activation", active)
			}
			if registered, draining := workflowTimerScheduledCounts(scheduler); registered != 1 || draining != 0 {
				t.Fatalf("first revised timer wakeup active=%d draining=%d, want 1/0", registered, draining)
			}
			wakeup, err := newWorkflowTimerWakeup(active[0])
			if err != nil {
				t.Fatal(err)
			}
			outcome, _, err := projection.fireWakeup(liveCtx, wakeup)
			var failure *runtimefailures.Error
			staleSourceOnly := runtimefailures.OnlyBranches(err, func(cause error) bool {
				return cause.Error() == "accepted workflow timer declaration revision is stale"
			})
			if outcome != WorkflowTimerFireCommitted || !errors.As(err, &failure) || failure.Failure.Class != runtimefailures.ClassInternalFailure || failure.Failure.Detail.Code != "event_interceptor_failed" || failure.Failure.Component != "eventbus" || failure.Failure.Operation != "run_interceptor" || !staleSourceOnly {
				t.Fatalf("prospective fire must commit publication but refuse stale-source admission: outcome=%q err=%v", outcome, err)
			}
			if bus.publishedCount() != 0 {
				t.Fatalf("stale-source event dispatched successfully %d times, want zero", bus.publishedCount())
			}
			persisted, readErr := fixture.PublishedEvent(liveCtx, timeridentity.WorkflowTimerOccurrenceEventID(active[0].occurrence()))
			if readErr != nil || persisted.ExecutionMode() != executionmode.Mock {
				t.Fatalf("committed prospective timer publication mode=%q err=%v, want mock", persisted.ExecutionMode(), readErr)
			}
		})
	}
}

func VerifyNativeWorkflowTimerLifecycleReconcilesInitialDeclarationRevisionOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			bundle := workflowTimerSourceRevisionBundle(t, false)
			fixture, pcA, ctx := nativePilotPipelineForTest(t, tc.name, bundle, open)
			store := pcA.workflowStore
			createdAt := canonicalWorkflowTimerTime(time.Now().UTC())
			entityID := runtimecorrelation.RunIDFromContext(ctx)
			rootRoute := workflowTimerRootRoute(ctx)
			schedulerA := newWorkflowTimerTestScheduler(t, pcA.workOwner)
			pcA.timerScheduler = schedulerA
			if err := pcA.workflowTimers.bindScheduler(schedulerA); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				join, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if err := pcA.StopWorkflowTimerLifecycle(join); err != nil {
					t.Error(err)
				}
			})
			instance := constructedScenarioInstanceForTest(t, pcA.SemanticSource(), ctx, semanticview.RootExecutionFlowID(pcA.SemanticSource()))
			instance.CreatedAt, instance.EnteredStageAt = createdAt, createdAt
			preparedInstance, preparedLifecycle, err := pcA.PrepareInitialEntryLifecycle(ctx, testRunScopedWorkflowRoute(ctx, rootRoute), instance, createdAt)
			if err != nil {
				t.Fatalf("prepare fixture lifecycle: %v", err)
			}
			committedLifecycle, err := fixture.ConstructInitial(ctx, preparedInstance, preparedLifecycle)
			if err != nil {
				t.Fatalf("construct native fixture lifecycle: %v", err)
			}
			if err := pcA.FinalizeInitialEntryLifecycle(ctx, committedLifecycle); err != nil {
				t.Fatalf("finalize fixture lifecycle: %v", err)
			}
			if err := pcA.ArmInitialEntryTimers(ctx, testRunScopedWorkflowRoute(ctx, rootRoute)); err != nil {
				t.Fatalf("arm source A: %v", err)
			}
			sourceARows := listWorkflowTimerOwnerActivations(t, store, ctx, entityID, true)
			if len(sourceARows) != 3 {
				t.Fatalf("source A active timers = %d, want 3: %#v", len(sourceARows), sourceARows)
			}
			if active, draining := workflowTimerScheduledCounts(schedulerA); active != 3 || draining != 0 {
				t.Fatalf("source A process wakeups = %d/%d, want 3/0", active, draining)
			}
			sourceAByDeclaration := workflowTimerActivationsByDeclaration(sourceARows)
			stopCtx, cancelStop := context.WithTimeout(context.Background(), time.Second)
			if err := pcA.workflowTimers.stop(stopCtx); err != nil {
				cancelStop()
				t.Fatalf("stop source A lifecycle: %v", err)
			}
			cancelStop()

			sourceB := semanticview.Wrap(workflowTimerSourceRevisionBundle(t, true))
			// Project prospective declarations without replacing the admitted source.
			schedulerB := newWorkflowTimerTestScheduler(t, pcA.workOwner)
			projection := newWorkflowTimerLifecycle(store, sourceB, pcA.bus, pcA.workOwner, schedulerB, pcA.executionPosture)
			attempt, plan, closeAttachment := fixture.AdmitAttachment(ctx, entityID, rootRoute.InstancePath)
			t.Cleanup(func() {
				join, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := projection.stop(join); err != nil {
					t.Error(err)
				}
				closeAttachment()
			})
			cancelledCtx, cancel := context.WithCancel(ctx)
			cancel()
			if err := projection.reconcileInitialEntryDeclarations(cancelledCtx, testRunScopedWorkflowRoute(cancelledCtx, rootRoute), &attempt, &plan); err == nil {
				t.Fatal("cancelled source revision unexpectedly committed")
			}
			if active := listWorkflowTimerOwnerActivations(t, store, ctx, entityID, true); len(active) != 3 {
				t.Fatalf("cancelled source revision changed active timers: %#v", active)
			}
			if err := projection.reconcileInitialEntryDeclarations(ctx, testRunScopedWorkflowRoute(ctx, rootRoute), &attempt, &plan); err != nil {
				t.Fatalf("reconcile source B: %v", err)
			}
			if err := projection.reconcileInitialEntryDeclarations(ctx, testRunScopedWorkflowRoute(ctx, rootRoute), &attempt, &plan); err != nil {
				t.Fatalf("replay source B reconciliation: %v", err)
			}

			allRows := listWorkflowTimerOwnerActivations(t, store, ctx, entityID, false)
			activeRows := listWorkflowTimerOwnerActivations(t, store, ctx, entityID, true)
			if len(allRows) != 5 || len(activeRows) != 3 {
				t.Fatalf("source B timer rows all/active = %d/%d, want 5/3: %#v", len(allRows), len(activeRows), allRows)
			}
			activeByDeclaration := workflowTimerActivationsByDeclaration(activeRows)
			keepKey := workflowTimerStageDeclarationKey(".", "waiting.keep")
			changedKey := workflowTimerStageDeclarationKey(".", "waiting.changed")
			removedKey := workflowTimerStageDeclarationKey(".", "waiting.removed")
			addedKey := workflowTimerStageDeclarationKey(".", "waiting.added")
			if activeByDeclaration[keepKey].Ref != sourceAByDeclaration[keepKey].Ref {
				t.Fatal("unchanged declaration did not preserve its exact activation")
			}
			if _, found := activeByDeclaration[removedKey]; found {
				t.Fatal("removed declaration remains active")
			}
			if activeByDeclaration[changedKey].Ref == sourceAByDeclaration[changedKey].Ref {
				t.Fatal("changed declaration reused its stale activation identity")
			}
			if activeByDeclaration[addedKey].Ref.ActivationID == "" {
				t.Fatal("added declaration has no active activation")
			}
			if activeByDeclaration[changedKey].EventType != "timer.changed.v2" ||
				!activeByDeclaration[changedKey].FireAt.Equal(createdAt.Add(2*time.Hour)) {
				t.Fatalf("changed declaration facts = %#v", activeByDeclaration[changedKey])
			}
			for _, declaration := range []string{changedKey, removedKey} {
				old := sourceAByDeclaration[declaration]
				persisted := loadWorkflowTimerOwnerActivation(t, store, ctx, old.Ref.ActivationID)
				if persisted.Status != workflowTimerStatusCancelled {
					t.Fatalf("old %s status = %q, want cancelled", declaration, persisted.Status)
				}
			}
			waitForWorkflowTimerCondition(t, time.Second, func() bool {
				active, draining := workflowTimerScheduledCounts(schedulerB)
				return active == 3 && draining == 0
			}, "source B replacement wakeups")

			stale := sourceAByDeclaration[changedKey]
			if err := projection.ReconcileWakeup(ctx, stale.Ref); err != nil {
				t.Fatalf("retire stale source A wakeup: %v", err)
			}
			staleEvent := eventtest.RuntimeControl(
				timeridentity.WorkflowTimerOccurrenceEventID(stale.occurrence()),
				events.EventType(stale.EventType),
				"runtime.workflow_timer",
				stale.occurrence().TaskID(),
				stale.Payload,
				0,
				stale.RunID,
				"",
				events.EventEnvelope{EntityID: stale.EntityID, FlowInstance: stale.Route.InstancePath},
				stale.FireAt,
			)
			if _, _, recognized, err := projection.AuthorizeAcceptedEvent(ctx, staleEvent); err == nil || !recognized {
				t.Fatalf("stale source A event authorization recognized=%v err=%v, want rejection", recognized, err)
			}

			stopCtx, cancelStop = context.WithTimeout(context.Background(), time.Second)
			if err := projection.stop(stopCtx); err != nil {
				cancelStop()
				t.Fatalf("stop source B lifecycle: %v", err)
			}
			cancelStop()
			schedulerRestarted := newWorkflowTimerTestScheduler(t, pcA.workOwner)
			restoredProjection := newWorkflowTimerLifecycle(store, sourceB, pcA.bus, pcA.workOwner, schedulerRestarted, pcA.executionPosture)
			t.Cleanup(func() {
				join, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if err := restoredProjection.stop(join); err != nil {
					t.Error(err)
				}
			})
			if err := restoredProjection.Restore(ctx); err != nil {
				t.Fatalf("restore source B lifecycle: %v", err)
			}
			if active, draining := workflowTimerScheduledCounts(schedulerRestarted); active != 3 || draining != 0 {
				t.Fatalf("restored source B wakeups = %d/%d, want 3/0", active, draining)
			}
		})
	}
}

func VerifyNativeWorkflowTimerLifecycleReconcilesProgressedInitialDeclarationsProspectivelyOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			fixture, pcA, ctx, mutations := nativeLifecycleComponentForTest(t, tc.name, workflowTimerProgressedSourceRevisionBundle(t, false), "waiting", open)
			store := pcA.workflowStore
			entityID := runtimecorrelation.RunIDFromContext(ctx)
			rootRoute := testWorkflowInstanceRoute(entityID)
			if err := applyTestInitialEntryEffect(ctx, pcA, rootRoute, entityID); err != nil {
				t.Fatal(err)
			}
			if err := pcA.ArmInitialEntryTimers(ctx, testRunScopedWorkflowRoute(ctx, rootRoute)); err != nil {
				t.Fatal(err)
			}
			sourceARows := listWorkflowTimerOwnerActivations(t, store, ctx, entityID, true)
			if len(sourceARows) != 3 {
				t.Fatalf("source A active timers = %d, want 3: %#v", len(sourceARows), sourceARows)
			}
			sourceAByDeclaration := workflowTimerActivationsByDeclaration(sourceARows)
			if _, err := executeNativeLifecycleTransitionForTest(t, fixture, mutations, pcA, ctx, "test.workflow_progressed"); err != nil {
				t.Fatalf("native progress transition: %v", err)
			}
			if active := listWorkflowTimerOwnerActivations(t, store, ctx, entityID, true); len(active) != 3 {
				t.Fatalf("progressed active timers = %d, want 3 before source revision: %#v", len(active), active)
			}
			stopCtx, cancelStop := context.WithTimeout(context.Background(), time.Second)
			if err := pcA.workflowTimers.stop(stopCtx); err != nil {
				cancelStop()
				t.Fatalf("stop source A lifecycle: %v", err)
			}
			cancelStop()

			sourceB := semanticview.Wrap(workflowTimerProgressedSourceRevisionBundle(t, true))
			// This is prospective declaration projection, not admission of a
			// replacement source or a new executable runtime over the database.
			schedulerB := newWorkflowTimerTestScheduler(t, pcA.workOwner)
			projection := newWorkflowTimerLifecycle(store, sourceB, pcA.bus, pcA.workOwner, schedulerB, pcA.executionPosture)
			attempt, plan, closeAttachment := fixture.AdmitAttachment(ctx, entityID, rootRoute.InstancePath)
			t.Cleanup(func() {
				join, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := projection.stop(join); err != nil {
					t.Error(err)
				}
				closeAttachment()
			})
			if err := projection.reconcileInitialEntryDeclarations(ctx, testRunScopedWorkflowRoute(ctx, rootRoute), &attempt, &plan); err != nil {
				t.Fatalf("reconcile progressed source B: %v", err)
			}
			if err := projection.reconcileInitialEntryDeclarations(ctx, testRunScopedWorkflowRoute(ctx, rootRoute), &attempt, &plan); err != nil {
				t.Fatalf("replay progressed source B: %v", err)
			}

			allRows := listWorkflowTimerOwnerActivations(t, store, ctx, entityID, false)
			activeRows := listWorkflowTimerOwnerActivations(t, store, ctx, entityID, true)
			if len(allRows) != 3 || len(activeRows) != 1 {
				t.Fatalf("progressed source B rows all/active = %d/%d, want 3/1: %#v", len(allRows), len(activeRows), allRows)
			}
			activeByDeclaration := workflowTimerActivationsByDeclaration(activeRows)
			timerNode := pipelineNode(t, "", "timer-owner")
			keepKey := workflowTimerNodeDeclarationKey(timerNode, "waiting.keep")
			changedKey := workflowTimerNodeDeclarationKey(timerNode, "waiting.changed")
			removedKey := workflowTimerNodeDeclarationKey(timerNode, "waiting.removed")
			addedKey := workflowTimerNodeDeclarationKey(timerNode, "waiting.added")
			if activeByDeclaration[keepKey].Ref != sourceAByDeclaration[keepKey].Ref {
				t.Fatal("unchanged progressed declaration did not preserve its exact activation")
			}
			for _, declaration := range []string{changedKey, removedKey} {
				old := sourceAByDeclaration[declaration]
				persisted := loadWorkflowTimerOwnerActivation(t, store, ctx, old.Ref.ActivationID)
				if persisted.Status != workflowTimerStatusCancelled {
					t.Fatalf("progressed old %s status = %q, want cancelled", declaration, persisted.Status)
				}
			}
			if _, found := activeByDeclaration[addedKey]; found {
				t.Fatal("new initial declaration was retroactively armed after initial entry")
			}
			waitForWorkflowTimerCondition(t, time.Second, func() bool {
				active, draining := workflowTimerScheduledCounts(schedulerB)
				return active == 1 && draining == 0
			}, "progressed source B replacement wakeups")
		})
	}
}

func VerifyNativeWorkflowTimerInitialWakeupProjectionIsCauseScopedOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			bundle := workflowTimerInitialAndEventBundle(t)
			fixture, pc, ctx := nativePilotPipelineForTest(t, tc.name, bundle, open)
			store := pc.workflowStore
			scheduler := newWorkflowTimerTestScheduler(t, pc.workOwner)
			pc.timerScheduler = scheduler
			if err := pc.workflowTimers.bindScheduler(scheduler); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				join, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if err := pc.StopWorkflowTimerLifecycle(join); err != nil {
					t.Error(err)
				}
			})
			entityID := runtimecorrelation.RunIDFromContext(ctx)
			rootRoute := workflowTimerRootRoute(ctx)
			createdAt := canonicalWorkflowTimerTime(time.Now().UTC())
			instance := constructedScenarioInstanceForTest(t, pc.SemanticSource(), ctx, semanticview.RootExecutionFlowID(pc.SemanticSource()))
			instance.CreatedAt, instance.EnteredStageAt = createdAt, createdAt
			preparedInstance, preparedLifecycle, err := pc.PrepareInitialEntryLifecycle(ctx, testRunScopedWorkflowRoute(ctx, rootRoute), instance, createdAt)
			if err != nil {
				t.Fatalf("prepare fixture lifecycle: %v", err)
			}
			committedLifecycle, err := fixture.ConstructInitial(ctx, preparedInstance, preparedLifecycle)
			if err != nil {
				t.Fatalf("construct native fixture lifecycle: %v", err)
			}
			if err := pc.FinalizeInitialEntryLifecycle(ctx, committedLifecycle); err != nil {
				t.Fatalf("finalize fixture lifecycle: %v", err)
			}
			if err := pc.ArmInitialEntryTimers(ctx, testRunScopedWorkflowRoute(ctx, rootRoute)); err != nil {
				t.Fatalf("arm initial timer: %v", err)
			}
			inbound := nativeWorkflowJoinEventForTest(ctx, ".", rootRoute.InstancePath, entityID, "timer.arm", []byte(`{}`), createdAt.Add(time.Minute))
			publishNativeWorkflowJoinEventForTest(t, fixture, pc, ctx, inbound, "timer-owner")
			if err := reconcileWorkflowTimerForTest(runtimecorrelation.WithInboundEvent(ctx, inbound), pc, rootRoute, entityID, "waiting", "waiting", workflowTimerCause{
				Kind: workflowTimerCauseEvent, EventID: inbound.ID(), EventType: string(inbound.Type()),
				OccurredAt: inbound.CreatedAt(), ExecutionMode: executionmode.Live,
			}); err != nil {
				t.Fatalf("reconcile published event through native lifecycle owner: %v", err)
			}
			waitForWorkflowTimerCondition(t, time.Second, func() bool {
				active, draining := workflowTimerScheduledCounts(scheduler)
				return active == 2 && draining == 0
			}, "initial and event workflow timer wakeups")
			rows := listWorkflowTimerOwnerActivations(t, store, ctx, entityID, true)
			if len(rows) != 2 {
				t.Fatalf("active initial/event timers = %d, want 2: %#v", len(rows), rows)
			}
			var eventRef, initialRef timeridentity.WorkflowTimerActivationRef
			for _, row := range rows {
				if row.Ref.Cause == timeridentity.WorkflowTimerActivationCauseEvent {
					eventRef = row.Ref
				}
				if row.Ref.Cause == timeridentity.WorkflowTimerActivationCauseInitial {
					initialRef = row.Ref
				}
			}
			if !eventRef.Valid() || !initialRef.Valid() {
				t.Fatal("initial- or event-caused timer activation is missing")
			}

			if err := pc.RetireInitialEntryTimerWakeups(ctx, testRunScopedWorkflowRoute(ctx, rootRoute)); err != nil {
				t.Fatalf("retire initial wakeups: %v", err)
			}
			waitForWorkflowTimerCondition(t, time.Second, func() bool {
				active, draining := workflowTimerScheduledCounts(scheduler)
				return active == 1 && draining == 0
			}, "event wakeup preserved during initial cleanup")
			if err := pc.workflowTimers.ReconcileWakeup(ctx, eventRef); err != nil {
				t.Fatalf("reconcile preserved event wakeup: %v", err)
			}
			if active := listWorkflowTimerOwnerActivations(t, store, ctx, entityID, true); len(active) != 2 {
				t.Fatalf("initial cleanup changed durable activations: %#v", active)
			}

			if err := pc.ArmInitialEntryTimers(ctx, testRunScopedWorkflowRoute(ctx, rootRoute)); err != nil {
				t.Fatalf("rearm initial wakeup after cleanup: %v", err)
			}
			waitForWorkflowTimerCondition(t, time.Second, func() bool {
				active, draining := workflowTimerScheduledCounts(scheduler)
				return active == 2 && draining == 0
			}, "initial retry and preserved event wakeups")
			if changed, err := fixture.MissingFields(ctx, runtimecorrelation.RunIDFromContext(ctx), entityID); err != nil || changed != 1 {
				t.Fatalf("remove exact instance projection: changed=%d err=%v", changed, err)
			}
			if err := pc.RetireInitialEntryTimerWakeups(ctx, testRunScopedWorkflowRoute(ctx, rootRoute)); err != nil {
				t.Fatalf("retire initial wakeups after instance projection loss: %v", err)
			}
			waitForWorkflowTimerCondition(t, time.Second, func() bool {
				active, draining := workflowTimerScheduledCounts(scheduler)
				return active == 1 && draining == 0
			}, "event wakeup preserved after instance projection loss")
			if err := pc.RetireInitialEntryTimerWakeups(ctx, testRunScopedWorkflowRoute(ctx, rootRoute)); err != nil {
				t.Fatalf("repeat initial wakeup retirement after instance projection loss: %v", err)
			}
			if err := fixture.SetTimerForeignRouteMalformedName(ctx, runtimecorrelation.RunIDFromContext(ctx), eventRef.ActivationID); err != nil {
				t.Fatal(err)
			}
			if err := pc.RetireInitialEntryTimerWakeups(ctx, testRunScopedWorkflowRoute(ctx, rootRoute)); err != nil {
				t.Fatalf("unrelated malformed timer blocked exact retirement: %v", err)
			}
			if err := fixture.SetTimerMalformedName(ctx, runtimecorrelation.RunIDFromContext(ctx), initialRef.ActivationID); err != nil {
				t.Fatal(err)
			}
			if err := pc.RetireInitialEntryTimerWakeups(ctx, testRunScopedWorkflowRoute(ctx, rootRoute)); err == nil {
				t.Fatal("matching malformed timer must fail closed")
			}
		})
	}
}

func VerifyNativeWorkflowTimerLifecycleScopesDeclarationsToOwningFlowOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, tc := range workflowJoinStoreCases() {
		for _, instanceFlow := range []string{"timer-flow-scope-root", "flow-a"} {
			t.Run(tc.name+"/"+instanceFlow, func(t *testing.T) {
				bundle := workflowTimerFlowScopedBundle(t)
				fixture, pc, ctx := nativePilotPipelineForTest(t, tc.name, bundle, open)
				store := pc.workflowStore
				source := pc.SemanticSource()
				scheduler := newWorkflowTimerTestScheduler(t, pc.workOwner)
				if err := pc.workflowTimers.bindScheduler(scheduler); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					join, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					if err := pc.StopWorkflowTimerLifecycle(join); err != nil {
						t.Error(err)
					}
				})
				flowID := semanticview.RootExecutionFlowID(source)
				if instanceFlow == "flow-a" {
					parent := constructedScenarioInstanceForTest(t, source, ctx, flowID)
					if err := fixture.Construct(ctx, parent); err != nil {
						t.Fatal(err)
					}
					flowID = instanceFlow
				}
				instance := constructedScenarioInstanceForTest(t, source, ctx, flowID)
				entityID, instancePath := instance.EntityID, instance.StorageRef
				route := testRunScopedWorkflowInstanceFromContext(ctx, instancePath).Route
				createdAt := canonicalWorkflowTimerTime(time.Now().UTC())
				instance.CreatedAt, instance.EnteredStageAt = createdAt, createdAt
				preparedInstance, preparedLifecycle, err := pc.PrepareInitialEntryLifecycle(ctx, testRunScopedWorkflowInstanceFromContext(ctx, instancePath), instance, createdAt)
				if err != nil {
					t.Fatalf("prepare fixture lifecycle: %v", err)
				}
				committedLifecycle, err := fixture.ConstructInitial(ctx, preparedInstance, preparedLifecycle)
				if err != nil {
					t.Fatalf("construct native initial lifecycle: %v", err)
				}
				if err := pc.FinalizeInitialEntryLifecycle(ctx, committedLifecycle); err != nil {
					t.Fatalf("finalize fixture lifecycle: %v", err)
				}
				rows := listWorkflowTimerOwnerActivations(t, store, ctx, entityID, true)
				if len(rows) != 1 {
					t.Fatalf("%s initial timers = %d, want 1: %#v", instanceFlow, len(rows), rows)
				}
				wantPrefix := "root"
				armEvent := "timer.arm"
				if instanceFlow == "flow-a" {
					wantPrefix = "flow-a/flow_a"
					armEvent = "flow-a/timer.arm"
				}
				wantInitialEvent := wantPrefix + ".initial"
				if rows[0].EventType != wantInitialEvent {
					t.Fatalf("%s initial timer event = %q, want %q", instanceFlow, rows[0].EventType, wantInitialEvent)
				}
				inbound := nativeWorkflowJoinEventForTest(ctx, flowID, route.InstancePath, entityID, armEvent, []byte(`{}`), createdAt.Add(time.Minute))
				dispatchNativeWorkflowJoinEventForTest(t, fixture, pc, ctx, inbound, "timer-owner")
				rows = listWorkflowTimerOwnerActivations(t, store, ctx, entityID, true)
				if len(rows) != 2 {
					t.Fatalf("%s active timers = %d, want 2: %#v", instanceFlow, len(rows), rows)
				}
				eventsByType := map[string]bool{}
				for _, row := range rows {
					eventsByType[row.EventType] = true
					if err := pc.workflowTimers.ReconcileWakeup(ctx, row.Ref); err != nil {
						t.Fatalf("validate %s scoped timer %s: %v", instanceFlow, row.EventType, err)
					}
				}
				for _, want := range []string{wantPrefix + ".initial", wantPrefix + ".event"} {
					if !eventsByType[want] {
						t.Fatalf("%s scoped timer %q is missing: %#v", instanceFlow, want, eventsByType)
					}
				}
				for _, foreign := range []string{"root", "flow-a/flow_a", "flow-b/flow_b"} {
					if foreign == wantPrefix {
						continue
					}
					if eventsByType[foreign+".initial"] || eventsByType[foreign+".event"] {
						t.Fatalf("%s materialized foreign %s timer: %#v", instanceFlow, foreign, eventsByType)
					}
				}
				waitForWorkflowTimerCondition(t, time.Second, func() bool {
					active, draining := workflowTimerScheduledCounts(scheduler)
					return active == 2 && draining == 0
				}, instanceFlow+" scoped wakeup replacement")
			})
		}
	}
}

func workflowTimerActivationsByDeclaration(
	activations []WorkflowTimerActivation,
) map[string]WorkflowTimerActivation {
	byDeclaration := make(map[string]WorkflowTimerActivation, len(activations))
	for _, activation := range activations {
		byDeclaration[activation.Ref.DeclarationKey] = activation
	}
	return byDeclaration
}

func workflowTimerStageDeclarationKey(flowID, id string) string {
	return (runtimecontracts.WorkflowTimerContract{ID: id, FlowID: flowID, StageOwned: true}).SemanticKey()
}

func workflowTimerNodeDeclarationKey(node identity.ExecutableNode, id string) string {
	return (runtimecontracts.WorkflowTimerContract{ID: id, Node: node}).SemanticKey()
}

func VerifyNativeAcceptedWorkflowTimerEventRoutingMatrixOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			_, pc, ctx, bus, entityID, activation := nativeWorkflowTimerOwnerActivationForTest(t, tc.name, false, "1h", time.Now().Add(-2*time.Hour), false, executionmode.Live, open)
			if outcome, err := fireWorkflowTimerTestWakeup(ctx, pc, activation); err != nil || outcome != WorkflowTimerFireCommitted {
				t.Fatalf("fire canonical occurrence outcome=%q err=%v", outcome, err)
			}
			canonical := bus.publishedEvent(0)
			routed := eventtest.TargetRouted(canonical, events.RouteIdentity{
				FlowID: "consumer", FlowInstance: "consumer", EntityID: uuid.NewString(),
			})

			tests := []struct {
				name           string
				event          events.Event
				wantRecognized bool
				wantErr        bool
			}{
				{name: "exact producer and canonical occurrence", event: canonical, wantRecognized: true},
				{name: "exact producer projected to compiled target", event: routed, wantRecognized: true},
				{
					name: "exact producer with malformed occurrence",
					event: eventtest.RuntimeControl(
						uuid.NewString(), canonical.Type(), "runtime.workflow_timer", "malformed",
						canonical.Payload(), 0, canonical.RunID(), "", events.EventEnvelope{
							EntityID: entityID, FlowInstance: activation.Route.InstancePath,
						}, canonical.CreatedAt(),
					),
					wantRecognized: true,
					wantErr:        true,
				},
				{
					name: "occurrence-shaped opaque task from generic scheduler",
					event: eventtest.RuntimeControl(
						canonical.ID(), canonical.Type(), "runtime.scheduler", canonical.TaskID(),
						canonical.Payload(), 0, canonical.RunID(), "", events.EventEnvelope{
							EntityID: entityID, FlowInstance: activation.Route.InstancePath,
						}, canonical.CreatedAt(),
					),
				},
				{
					name: "ordinary task from another producer",
					event: eventtest.RuntimeControl(
						uuid.NewString(), canonical.Type(), "runtime", "ordinary-task",
						canonical.Payload(), 0, canonical.RunID(), "", events.EventEnvelope{
							EntityID: entityID, FlowInstance: activation.Route.InstancePath,
						}, canonical.CreatedAt(),
					),
				},
				{
					name: "exact producer with missing canonical activation",
					event: func() events.Event {
						missing := activation.occurrence()
						missing.Activation.ActivationID = uuid.NewString()
						return eventtest.RuntimeControl(
							timeridentity.WorkflowTimerOccurrenceEventID(missing), canonical.Type(),
							"runtime.workflow_timer", missing.TaskID(), canonical.Payload(), 0,
							canonical.RunID(), "", events.EventEnvelope{
								EntityID: entityID, FlowInstance: activation.Route.InstancePath,
							}, canonical.CreatedAt(),
						)
					}(),
					wantRecognized: true,
					wantErr:        true,
				},
			}
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					_, _, recognized, err := pc.workflowTimers.AuthorizeAcceptedEvent(ctx, test.event)
					if recognized != test.wantRecognized || (err != nil) != test.wantErr {
						t.Fatalf("recognized=%v err=%v, want recognized=%v err=%v", recognized, err, test.wantRecognized, test.wantErr)
					}
				})
			}
		})
	}
}

func TestWorkflowTimerWakeupProjectionCarriesOnlyFamilyOccurrenceAndDueAt(t *testing.T) {
	typ := reflect.TypeOf(WorkflowTimerWakeup{})
	if typ.NumField() != 3 {
		t.Fatalf("WorkflowTimerWakeup fields = %d, want exactly 3", typ.NumField())
	}
	for index, want := range []string{"family", "occurrence", "dueAt"} {
		if got := typ.Field(index).Name; got != want {
			t.Fatalf("WorkflowTimerWakeup field %d = %q, want %q", index, got, want)
		}
	}
}

func VerifyNativeWorkflowTimerActiveProjectionRequiresSchedulerOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			_, pc, ctx, _, _, activation := nativeWorkflowTimerOwnerActivationForTest(t, tc.name, false, "1h", time.Now().Add(-2*time.Hour), false, executionmode.Live, open)

			if err := pc.workflowTimers.ReconcileWakeup(ctx, activation.Ref); !errors.Is(err, errWorkflowTimerSchedulerRequired) {
				t.Fatalf("ReconcileWakeup error = %v, want scheduler-required failure", err)
			}
			if err := pc.RestoreWorkflowTimers(ctx); !errors.Is(err, errWorkflowTimerSchedulerRequired) {
				t.Fatalf("RestoreWorkflowTimers error = %v, want scheduler-required failure", err)
			}
		})
	}
}

func VerifyNativeWorkflowTimerLifecycleExactCauseReplayConvergesAfterTerminalStateOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, tc := range workflowJoinStoreCases() {
		for _, terminal := range []string{workflowTimerStatusFired, workflowTimerStatusCancelled} {
			t.Run(tc.name+"/"+terminal, func(t *testing.T) {
				fixture, pc, ctx, _, entityID, activation := nativeWorkflowTimerOwnerActivationForTest(t, tc.name, false, "1h", time.Now().Add(-2*time.Hour), false, executionmode.Live, open)
				store := pc.workflowStore
				if terminal == workflowTimerStatusFired {
					if outcome, err := fireWorkflowTimerTestWakeup(ctx, pc, activation); err != nil || outcome != WorkflowTimerFireCommitted {
						t.Fatalf("fire activation outcome=%q err=%v", outcome, err)
					}
				} else if err := cancelNativeWorkflowTimerForTest(t, fixture, ctx, pc, activation); err != nil {
					t.Fatalf("cancel activation: %v", err)
				}

				cause := workflowTimerCause{
					Kind: workflowTimerCauseInitial, OccurredAt: activation.CreatedAt, ToState: "waiting", ExecutionMode: executionmode.Live,
				}
				if err := reconcileWorkflowTimerForTest(ctx, pc, workflowTimerRootRoute(ctx), entityID, "", "waiting", cause); err != nil {
					t.Fatalf("replay exact activation cause: %v", err)
				}
				all := listWorkflowTimerOwnerActivations(t, store, ctx, entityID, false)
				if len(all) != 1 || all[0].Ref != activation.Ref || all[0].Status != terminal {
					t.Fatalf("activation history after replay = %#v, want one %s row", all, terminal)
				}
				if active := listWorkflowTimerOwnerActivations(t, store, ctx, entityID, true); len(active) != 0 {
					t.Fatalf("active rows after terminal replay = %#v, want none", active)
				}
			})
		}
	}
}

func VerifyNativeWorkflowTimerLifecycleReactivatesOnlyOnLaterStageEntryOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			files := workflowTimerOwnerSourceFiles()
			files["schema.yaml"] = "name: workflow-timer-owner-test\nstages:\n  waiting:\n    timers:\n      - {id: waiting.timeout, after: 1h, emit: timer.timeout}\n  done: {}\n"
			files["events.yaml"] += "work.noted:\n"
			files["nodes.yaml"] += "    work.noted: {}\n"
			bundle := loadWorkflowTempBundle(t, files)
			fixture, pc, ctx, _, entityID, first := nativeWorkflowTimerSourceActivationForTest(t, tc.name, bundle, time.Now().Add(-2*time.Hour), false, executionmode.Live, open)
			store := pc.workflowStore
			pc.timerScheduler = newWorkflowTimerTestScheduler(t, pc.workOwner)
			if err := pc.workflowTimers.bindScheduler(pc.timerScheduler); err != nil {
				t.Fatal(err)
			}
			if outcome, err := fireWorkflowTimerTestWakeup(ctx, pc, first); err != nil || outcome != WorkflowTimerFireCommitted {
				t.Fatalf("fire first activation outcome=%q err=%v", outcome, err)
			}

			unrelatedAt := canonicalWorkflowTimerTime(first.FireAt.Add(time.Minute))
			unrelatedEvent := nativeWorkflowJoinEventForTest(ctx, ".", workflowTimerRootRoute(ctx).InstancePath, entityID, "work.noted", []byte(`{}`), unrelatedAt)
			dispatchNativeWorkflowJoinEventForTest(t, fixture, pc, ctx, unrelatedEvent, "timer-owner")
			all := listWorkflowTimerOwnerActivations(t, store, ctx, entityID, false)
			if len(all) != 1 {
				t.Fatalf("activations after unrelated same-stage event = %d, want 1", len(all))
			}

			reentryAt := canonicalWorkflowTimerTime(unrelatedAt.Add(time.Minute))
			leaveEvent := nativeWorkflowJoinEventForTest(ctx, ".", workflowTimerRootRoute(ctx).InstancePath, entityID, "work.completed", []byte(`{}`), unrelatedAt.Add(30*time.Second))
			dispatchNativeWorkflowJoinEventForTest(t, fixture, pc, ctx, leaveEvent, "timer-owner")
			reentryEvent := nativeWorkflowJoinEventForTest(ctx, ".", workflowTimerRootRoute(ctx).InstancePath, entityID, "review.reopened", []byte(`{}`), reentryAt)
			dispatchNativeWorkflowJoinEventForTest(t, fixture, pc, ctx, reentryEvent, "timer-owner")
			dispatchNativeWorkflowJoinEventForTest(t, fixture, pc, ctx, reentryEvent, "timer-owner")
			all = listWorkflowTimerOwnerActivations(t, store, ctx, entityID, false)
			if len(all) != 2 {
				t.Fatalf("activations after exact reentry retry = %d, want 2", len(all))
			}
			if all[0].Ref.ActivationID == all[1].Ref.ActivationID {
				t.Fatalf("later stage entry reused activation %s", all[0].Ref.ActivationID)
			}
			active := listWorkflowTimerOwnerActivations(t, store, ctx, entityID, true)
			if len(active) != 1 || active[0].Ref.ActivationID == first.Ref.ActivationID {
				t.Fatalf("active reentry activation = %#v, want one new activation", active)
			}
		})
	}
}

func VerifyNativeWorkflowTimerLifecycleEventOnlyHandlerDoesNotReplayStateEntryOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			createdAt := canonicalWorkflowTimerTime(time.Now().Add(-2 * time.Hour))
			bundle := workflowTimerEventOnlyStateTriggerBundle(t)
			fixture, pc, ctx, bus, entityID, _ := nativeWorkflowTimerSourceActivationForTest(t, tc.name, bundle, createdAt, false, executionmode.Live, open)
			store := pc.workflowStore
			rootRoute := workflowTimerRootRoute(ctx)
			pc.timerScheduler = newWorkflowTimerTestScheduler(t, pc.workOwner)
			if err := pc.workflowTimers.bindScheduler(pc.timerScheduler); err != nil {
				t.Fatal(err)
			}
			active := listWorkflowTimerOwnerActivations(t, store, ctx, entityID, true)
			stateEntryKey := workflowTimerStageDeclarationKey(".", "waiting.state_entry")
			if len(active) != 1 || active[0].Ref.DeclarationKey != stateEntryKey {
				t.Fatalf("initial active timers = %#v, want %s", active, stateEntryKey)
			}
			stateTimer := active[0]
			execute := func(eventType string, eventAt time.Time) {
				t.Helper()
				evt := nativeWorkflowJoinEventForTest(ctx, ".", rootRoute.InstancePath, entityID, eventType, []byte(`{}`), eventAt)
				route := publishNativeWorkflowJoinEventForTest(t, fixture, pc, ctx, evt, "observer")
				node := pipelineNode(t, ".", "observer")
				handler := pc.SemanticSource().ExecutableNodeEventHandlers(node)[eventType]
				result, err := executeNativeClaimedPipelineHandlerForTest(t, pc, withWorkflowNodeDeliveryRoute(ctx, route), node, handler, workflowTriggerContext{
					Event: evt, State: mustCurrentWorkflowState(t, pc, ctx, rootRoute, entityID), HandlerEventKey: eventType,
				})
				if err != nil {
					t.Fatalf("execute %s event-only handler: %v", eventType, err)
				}
				if !result.Handled {
					t.Fatalf("%s event-only handler was not handled", eventType)
				}
			}

			armedAt := canonicalWorkflowTimerTime(time.Now())
			execute("timer.arm", armedAt)
			active = listWorkflowTimerOwnerActivations(t, store, ctx, entityID, true)
			if len(active) != 2 {
				t.Fatalf("timers after event activation = %#v, want active state-entry and event timers", active)
			}

			execute("work.noted", armedAt.Add(time.Minute))

			all := listWorkflowTimerOwnerActivations(t, store, ctx, entityID, false)
			if len(all) != 2 {
				t.Fatalf("timers after event-only handler = %#v, want two original activations", all)
			}
			activationByDeclaration := make(map[string]WorkflowTimerActivation, len(all))
			for _, activation := range all {
				activationByDeclaration[activation.Ref.DeclarationKey] = activation
			}
			persistedStateTimer := activationByDeclaration[stateEntryKey]
			if persistedStateTimer.Ref != stateTimer.Ref || persistedStateTimer.Status != workflowTimerStatusActive {
				t.Fatalf("state-entry timer after event-only handlers = %#v, want original active activation %#v", persistedStateTimer, stateTimer.Ref)
			}
			eventArmedKey := workflowTimerNodeDeclarationKey(pipelineNode(t, ".", "observer"), "waiting.event_armed")
			if got := activationByDeclaration[eventArmedKey].Status; got != workflowTimerStatusActive {
				t.Fatalf("event-armed timer status = %q, want active without state-trigger cancellation", got)
			}

			if outcome, err := fireWorkflowTimerTestWakeup(ctx, pc, stateTimer); err != nil || outcome != WorkflowTimerFireCommitted {
				t.Fatalf("fire preserved state-entry timer outcome=%q err=%v", outcome, err)
			}
			if got := bus.publishedCount(); got != 1 {
				t.Fatalf("published timer events = %d, want 1", got)
			}
			if got, want := bus.publishedEvent(0).ID(), timeridentity.WorkflowTimerOccurrenceEventID(stateTimer.occurrence()); got != want {
				t.Fatalf("preserved timer event id = %q, want %q", got, want)
			}
			if got := loadWorkflowTimerOwnerActivation(t, store, ctx, stateTimer.Ref.ActivationID).Status; got != workflowTimerStatusFired {
				t.Fatalf("state-entry timer status after fire = %q, want fired", got)
			}
			if got := loadWorkflowTimerOwnerActivation(t, store, ctx, activationByDeclaration[eventArmedKey].Ref.ActivationID).Status; got != workflowTimerStatusActive {
				t.Fatalf("event-armed timer status after state timer fire = %q, want active", got)
			}
		})
	}
}

func VerifyNativeWorkflowTimerLifecycleReconcilesOnlyHandledOutcomesOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			fixture, pc, ctx, _ := nativeLifecycleComponentForTest(t, tc.name, workflowTimerHandledOutcomeBundle(t), "waiting", open)
			store := pc.workflowStore
			entityID := runtimecorrelation.RunIDFromContext(ctx)
			rootRoute := workflowTimerRootRoute(ctx)
			createdAt := canonicalWorkflowTimerTime(time.Now())
			eventOffset := time.Duration(0)
			execute := func(eventType string, payload []byte, wantHandled bool) {
				t.Helper()
				eventOffset++
				if len(payload) == 0 {
					payload = []byte(`{}`)
				}
				eventAt := createdAt.Add(eventOffset * time.Minute)
				evt := nativeWorkflowJoinEventForTest(ctx, ".", rootRoute.InstancePath, entityID, eventType, payload, eventAt)
				route := publishNativeWorkflowJoinEventForTest(t, fixture, pc, ctx, evt, "observer")
				node := pipelineNode(t, ".", "observer")
				handler := pc.SemanticSource().ExecutableNodeEventHandlers(node)[eventType]
				result, err := executeNativeClaimedPipelineHandlerForTest(t, pc, withWorkflowNodeDeliveryRoute(ctx, route), node, handler, workflowTriggerContext{
					Event: evt, State: mustCurrentWorkflowState(t, pc, ctx, rootRoute, entityID), HandlerEventKey: eventType,
				})
				if err != nil {
					t.Fatalf("execute %s handler: %v", eventType, err)
				}
				if result.Handled != wantHandled {
					t.Fatalf("%s handled = %v, want %v", eventType, result.Handled, wantHandled)
				}
			}
			declarations := func(id string) []WorkflowTimerActivation {
				t.Helper()
				declarationKey := workflowTimerNodeDeclarationKey(pipelineNode(t, ".", "observer"), id)
				all := listWorkflowTimerOwnerActivations(t, store, ctx, entityID, false)
				matched := make([]WorkflowTimerActivation, 0, 1)
				for _, activation := range all {
					if activation.Ref.DeclarationKey == declarationKey {
						matched = append(matched, activation)
					}
				}
				return matched
			}
			assertOneStatus := func(id, status string) {
				t.Helper()
				matched := declarations(id)
				if len(matched) != 1 || matched[0].Status != status {
					t.Fatalf("timer %s activations = %#v, want one %s", id, matched, status)
				}
			}

			execute("accepted.start", nil, true)
			assertOneStatus("accepted", workflowTimerStatusActive)
			execute("accepted.cancel", nil, true)
			assertOneStatus("accepted", workflowTimerStatusCancelled)

			execute("reject.target", nil, true)
			execute("guard.reject", nil, false)
			if matched := declarations("reject.start"); len(matched) != 0 {
				t.Fatalf("guard reject created timer: %#v", matched)
			}
			assertOneStatus("reject.target", workflowTimerStatusActive)

			execute("discard.target", nil, true)
			execute("guard.discard", nil, false)
			if matched := declarations("discard.start"); len(matched) != 0 {
				t.Fatalf("guard discard created timer: %#v", matched)
			}
			assertOneStatus("discard.target", workflowTimerStatusActive)

			execute("dedup.event", []byte(`{"item_id":"item-1"}`), true)
			assertOneStatus("dedup.start", workflowTimerStatusActive)
			execute("dedup.reset", nil, true)
			assertOneStatus("dedup.start", workflowTimerStatusCancelled)
			execute("dedup.target", nil, true)
			assertOneStatus("dedup.target", workflowTimerStatusActive)
			execute("dedup.event", []byte(`{"item_id":"item-1"}`), false)
			assertOneStatus("dedup.start", workflowTimerStatusCancelled)
			assertOneStatus("dedup.target", workflowTimerStatusActive)
		})
	}
}

func VerifyNativeWorkflowTimerLifecycleEventHandlerFencesLoopGenerationOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			bundle := workflowTimerLoopEventBundle(t)
			fixture, pc, ctx := nativePilotPipelineForTest(t, tc.name, bundle, open)
			store := pc.workflowStore
			runID := runtimecorrelation.RunIDFromContext(ctx)
			entityID := runID
			createdAt := canonicalWorkflowTimerTime(time.Now())
			loopActivation, err := loopruntime.New(
				runID, entityID, ".", "revision", "revision_id", uuid.NewString(), "waiting", 3, createdAt,
			)
			if err != nil {
				t.Fatalf("create loop activation: %v", err)
			}
			carrier := runtimeengine.NewStateCarrier(
				map[string]any{}, nil, map[string]map[string]any{},
			)
			if err := loopruntime.Store(carrier.StateBuckets, loopActivation); err != nil {
				t.Fatalf("store loop activation: %v", err)
			}
			if err := fixture.Construct(ctx, materializedWorkflowInstanceForSource(t, pc.SemanticSource(), ctx, WorkflowInstance{
				InstanceID: runID, StorageRef: runID, EntityID: entityID, WorkflowName: ".",
				WorkflowVersion: bundle.WorkflowVersion(), CurrentState: "waiting", EnteredStageAt: createdAt,
				CreatedAt: createdAt, Fields: carrier.PersistedFields(), Bookkeeping: carrier.PersistedBookkeeping(), Gates: carrier.Gates, StateBuckets: carrier.PersistedStateBuckets(),
				EntityType: "test_entity",
			})); err != nil {
				t.Fatalf("seed workflow instance: %v", err)
			}

			bus := observeNativePipelineDeliveryBusForTest(t, pc)
			pc.workflowTimers.publication = bus
			pc.workflowTimers.dispatcher = bus.EngineDispatcher()
			pc.timerScheduler = newWorkflowTimerTestScheduler(t, pc.workOwner)
			if err := pc.workflowTimers.bindScheduler(pc.timerScheduler); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				join, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := pc.StopWorkflowTimerLifecycle(join); err != nil {
					t.Error(err)
				}
			})
			handlers := pc.SemanticSource().ExecutableNodeEventHandlers(pipelineNode(t, ".", "observer"))
			armHandler := handlers["timer.arm"]
			execute := func(eventID, eventType, revisionID string, handler runtimecontracts.SystemNodeEventHandler) {
				t.Helper()
				payload := []byte(fmt.Sprintf(`{"revision_id":%q}`, revisionID))
				eventAt := createdAt.Add(time.Minute)
				target := events.RouteIdentity{FlowID: ".", FlowInstance: runID, EntityID: entityID}
				evt := eventtest.ExistingRunRootIngressWithRoutingSourceAndMode(
					eventID, events.EventType(eventType), "operator", "",
					payload, 0, runID,
					events.EnvelopeForTargetRoute(events.EventEnvelope{}, target), testWorkflowRoutingSource(".", runID, entityID), eventAt, executionmode.Live,
				)
				route := publishNativeWorkflowJoinEventForTest(t, fixture, pc, ctx, evt, "observer")
				result, err := executeNativeClaimedPipelineHandlerForTest(t, pc, withWorkflowNodeDeliveryRoute(ctx, route), pipelineNode(t, ".", "observer"), handler, workflowTriggerContext{
					Event: evt, State: mustCurrentWorkflowState(t, pc, ctx, testWorkflowInstanceRoute(runID), entityID), HandlerEventKey: eventType,
				})
				if err != nil {
					t.Fatalf("execute %s handler: %v", eventType, err)
				}
				if !result.Handled {
					t.Fatalf("%s handler was not handled", eventType)
				}
			}

			firstEventID := uuid.NewString()
			firstGeneration := loopActivation.Generation()
			execute(firstEventID, "timer.arm", firstGeneration.RevisionID, armHandler)
			execute(firstEventID, "timer.arm", firstGeneration.RevisionID, armHandler)
			active := listWorkflowTimerOwnerActivations(t, store, ctx, entityID, true)
			if len(active) != 1 || !active[0].Ref.Generation.Equal(firstGeneration) {
				t.Fatalf("first-generation exact replay activations = %#v, want one generation %#v", active, firstGeneration)
			}
			firstTimer := active[0]

			repeatHandler := handlers["loop.repeat"]
			execute(uuid.NewString(), "loop.repeat", firstGeneration.RevisionID, repeatHandler)
			persistedInstance, ok, err := store.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, runID))
			if err != nil || !ok {
				t.Fatalf("load repeated workflow instance found=%v err=%v", ok, err)
			}
			persistedCarrier, err := workflowInstanceStateCarrier(persistedInstance)
			if err != nil {
				t.Fatalf("decode repeated loop state: %v", err)
			}
			nextLoop, ok, err := loopruntime.Load(persistedCarrier.StateBuckets, ".", "revision")
			if err != nil || !ok {
				t.Fatalf("load next loop generation found=%v err=%v", ok, err)
			}
			nextGeneration := nextLoop.Generation()
			if nextGeneration.Equal(firstGeneration) {
				t.Fatalf("loop repeat retained generation %#v", nextGeneration)
			}
			if persisted := loadWorkflowTimerOwnerActivation(t, store, ctx, firstTimer.Ref.ActivationID); persisted.Status != workflowTimerStatusCancelled {
				t.Fatalf("superseded timer status = %q, want cancelled", persisted.Status)
			}
			if outcome, err := fireWorkflowTimerTestWakeup(ctx, pc, firstTimer); err != nil || outcome != WorkflowTimerFireTerminal {
				t.Fatalf("stale timer fire outcome=%q err=%v, want terminal no-op", outcome, err)
			}
			if bus.publishedCount() != 0 {
				t.Fatalf("stale timer published events = %d, want 0", bus.publishedCount())
			}

			nextEventID := uuid.NewString()
			execute(nextEventID, "timer.arm", nextGeneration.RevisionID, armHandler)
			execute(nextEventID, "timer.arm", nextGeneration.RevisionID, armHandler)
			active = listWorkflowTimerOwnerActivations(t, store, ctx, entityID, true)
			if len(active) != 1 || !active[0].Ref.Generation.Equal(nextGeneration) || active[0].Ref == firstTimer.Ref {
				t.Fatalf("next-generation exact replay activations = %#v, want one new generation %#v", active, nextGeneration)
			}
			all := listWorkflowTimerOwnerActivations(t, store, ctx, entityID, false)
			if len(all) != 2 {
				t.Fatalf("activation history = %#v, want one cancelled and one active generation", all)
			}
		})
	}
}

func VerifyNativeWorkflowTimerLifecycleInitialAndEventEntrancesDoNotDuplicateOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			files := workflowTimerOwnerSourceFiles()
			files["events.yaml"] += "work.created:\n"
			files["nodes.yaml"] += "    work.created: {}\n  timers:\n    - {id: waiting.timeout, event: timer.timeout, start_on: 'event:work.created', delay: 1h}\n"
			bundle := loadWorkflowTempBundle(t, files)
			fixture, pc, ctx := nativePilotPipelineForTest(t, tc.name, bundle, open)
			store := pc.workflowStore
			entityID := runtimecorrelation.RunIDFromContext(ctx)
			rootRoute := workflowTimerRootRoute(ctx)
			createdAt := canonicalWorkflowTimerTime(time.Now())
			instance := materializedWorkflowInstanceForSource(t, pc.SemanticSource(), ctx, WorkflowInstance{
				InstanceID: entityID, StorageRef: entityID, EntityID: entityID, WorkflowName: ".",
				WorkflowVersion: bundle.WorkflowVersion(), CurrentState: "waiting", EntityType: "test_entity", CreatedAt: createdAt,
			})
			prepared, lifecycle, err := pc.PrepareInitialEntryLifecycle(ctx, testRunScopedWorkflowRoute(ctx, rootRoute), instance, createdAt)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := fixture.ConstructInitial(ctx, prepared, lifecycle); err != nil {
				t.Fatalf("reconcile native initial entrance: %v", err)
			}
			if initial := listWorkflowTimerOwnerActivations(t, store, ctx, entityID, false); len(initial) != 0 {
				t.Fatalf("initial entrance created event-only timers: %#v", initial)
			}
			pc.timerScheduler = newWorkflowTimerTestScheduler(t, pc.workOwner)
			if err := pc.workflowTimers.bindScheduler(pc.timerScheduler); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				join, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if err := pc.StopWorkflowTimerLifecycle(join); err != nil {
					t.Error(err)
				}
			})
			inbound := nativeWorkflowJoinEventForTest(ctx, ".", rootRoute.InstancePath, entityID, "work.created", []byte(`{}`), createdAt)
			eventID := inbound.ID()
			dispatchNativeWorkflowJoinEventForTest(t, fixture, pc, ctx, inbound, "timer-owner")
			activations := listWorkflowTimerOwnerActivations(t, store, ctx, entityID, true)
			if len(activations) != 1 {
				t.Fatalf("active event timer activations = %d, want 1", len(activations))
			}
			want, err := workflowTimerActivationForCause(
				semanticview.Wrap(bundle),
				runtimecorrelation.RunIDFromContext(ctx), entityID, rootRoute, bundle.Semantics.Timers[0],
				activations[0].Ref.Generation,
				// A same-stage accepted event carries no synthetic transition coordinates.
				workflowTimerCause{Kind: workflowTimerCauseEvent, EventID: eventID, EventType: "work.created", OccurredAt: createdAt, ExecutionMode: executionmode.Live},
				time.Hour,
			)
			if err != nil {
				t.Fatalf("derive expected workflow timer activation: %v", err)
			}
			if activations[0].Ref != want.Ref {
				t.Fatalf("event activation ref = %#v, want %#v", activations[0].Ref, want.Ref)
			}
		})
	}
}

func VerifyNativeWorkflowTimerLifecycleRecurringAdvancesPersistedCoordinateOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			fixture, pc, ctx, bus, _, activation := nativeWorkflowTimerOwnerActivationForTest(t, tc.name, true, "1h", time.Now().Add(-2*time.Hour), false, executionmode.Live, open)
			store := pc.workflowStore
			firstOccurrence := activation.occurrence()

			outcome, err := fireWorkflowTimerTestWakeup(ctx, pc, activation)
			if err != nil || outcome != WorkflowTimerFireCommitted {
				t.Fatalf("first recurring fire outcome=%q err=%v", outcome, err)
			}
			next := loadWorkflowTimerOwnerActivation(t, store, ctx, activation.Ref.ActivationID)
			if next.Status != workflowTimerStatusActive {
				t.Fatalf("recurring status = %q, want active", next.Status)
			}
			if want := activation.FireAt.Add(activation.RecurrenceInterval); !next.FireAt.Equal(want) {
				t.Fatalf("next fire_at = %s, want %s", next.FireAt, want)
			}

			outcome, err = fireWorkflowTimerTestWakeup(ctx, pc, activation)
			if err != nil || outcome != WorkflowTimerFireTerminal || bus.publishedCount() != 1 {
				t.Fatalf("same-occurrence retry outcome=%q publishes=%d err=%v", outcome, bus.publishedCount(), err)
			}

			secondOccurrence := next.occurrence()
			outcome, err = fireWorkflowTimerTestWakeup(ctx, pc, next)
			if err != nil || outcome != WorkflowTimerFireCommitted {
				t.Fatalf("second recurring fire outcome=%q err=%v", outcome, err)
			}
			if bus.publishedCount() != 2 {
				t.Fatalf("published recurring events = %d, want 2", bus.publishedCount())
			}
			firstID := timeridentity.WorkflowTimerOccurrenceEventID(firstOccurrence)
			secondID := timeridentity.WorkflowTimerOccurrenceEventID(secondOccurrence)
			if firstID == secondID || bus.publishedEvent(0).ID() != firstID || bus.publishedEvent(1).ID() != secondID {
				t.Fatalf("recurring event ids = (%q, %q), want distinct deterministic (%q, %q)", bus.publishedEvent(0).ID(), bus.publishedEvent(1).ID(), firstID, secondID)
			}

			if err := pc.StopWorkflowTimerLifecycle(ctx); err != nil {
				t.Fatalf("join predecessor timer lifecycle: %v", err)
			}
			nextFixture := fixture.ReopenExecution()
			restarted := nextFixture.NewCoordinator(PipelineCoordinatorOptions{Module: pc.module, Persistence: nextFixture.Persistence})
			restartedScheduler := newWorkflowTimerTestScheduler(t, restarted.workOwner)
			if err := restarted.workflowTimers.bindScheduler(restartedScheduler); err != nil {
				t.Fatal(err)
			}
			restartedCtx := runtimecorrelation.WithRunID(nextFixture.Context, activation.RunID)
			t.Cleanup(func() {
				stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				_ = restarted.StopWorkflowTimerLifecycle(stopCtx)
			})
			if err := restarted.RestoreWorkflowTimers(restartedCtx); err != nil {
				t.Fatalf("RestoreWorkflowTimers: %v", err)
			}
			registered, _ := workflowTimerScheduledCounts(restartedScheduler)
			if registered != 1 {
				t.Fatalf("restored workflow wakeups = %d, want 1", registered)
			}
		})
	}
}

func VerifyNativeWorkflowTimerLifecycleListsScopeWildcardsOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			fixture, pc, ctx, _, entityID, activation := nativeWorkflowTimerOwnerActivationForTest(t, tc.name, false, "1h", time.Now(), false, executionmode.Live, open)
			store := pc.workflowStore
			runID := runtimecorrelation.RunIDFromContext(ctx)
			lookalikeTaskID := "workflowXtimer:v1:generic"
			routing, err := events.NewFlowOwnedControlRoutingSource(events.RouteIdentity{
				FlowID: "generic", FlowInstance: "generic", EntityID: entityID,
			})
			if err != nil {
				t.Fatal(err)
			}
			command := runtimegenericschedule.AdmissionCommand{
				ScheduleKey: lookalikeTaskID, RunID: runID, EntityID: entityID, FlowInstance: "generic",
				OwnerKind: runtimegenericschedule.OwnerSystem, OwnerID: "generic",
				EventType: "timer.task_timeout", Payload: semanticvalue.EmptyObject(), RoutingSource: routing,
				ExecutionMode: executionmode.Live,
				Due:           runtimegenericschedule.AbsoluteDue(activation.FireAt), TaskID: lookalikeTaskID,
			}
			committed, err := fixture.AdmitSchedule(ctx, command)
			if err != nil || !committed.Acknowledged || committed.Result.Outcome != runtimegenericschedule.AdmissionCreated {
				t.Fatalf("admit lookalike generic timer: %+v err=%v", committed, err)
			}

			for _, filter := range []struct {
				name     string
				runID    string
				entityID string
			}{
				{name: "exact", runID: runID, entityID: entityID},
				{name: "run_wildcard", entityID: entityID},
				{name: "entity_wildcard", runID: runID},
				{name: "both_wildcards"},
			} {
				t.Run(filter.name, func(t *testing.T) {
					activations, err := store.listPersistedWorkflowTimerActivations(ctx, filter.runID, filter.entityID, true)
					if err != nil {
						t.Fatalf("list workflow timer activations: %v", err)
					}
					if len(activations) != 1 || activations[0].Ref != activation.Ref {
						t.Fatalf("listed activations = %#v, want exact activation %#v", activations, activation.Ref)
					}
				})
			}
		})
	}
}

func VerifyNativeWorkflowTimerWakeupRejectsForeignDeclarationSourceOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			fixture, pc, ctx, _, _, activation := nativeWorkflowTimerOwnerActivationForTest(t, tc.name, false, "1h", time.Now(), false, executionmode.Live, open)
			scheduler := newWorkflowTimerTestScheduler(t, pc.workflowTimers.workOwner)
			if err := pc.workflowTimers.bindScheduler(scheduler); err != nil {
				t.Fatalf("bind workflow timer scheduler: %v", err)
			}
			before := fixture.Transactions()
			if err := fixture.SetTimerForeignDeclaration(ctx, activation.RunID, activation.Ref.ActivationID); err != nil {
				t.Fatalf("install foreign workflow timer source: %v", err)
			}
			if after := fixture.Transactions(); after.OtherCommits != before.OtherCommits+1 || after.Active != 0 {
				t.Fatalf("foreign timer fault escaped its original coordinator: before=%+v after=%+v", before, after)
			}

			if err := pc.workflowTimers.ReconcileWakeup(ctx, activation.Ref); err == nil || !strings.Contains(err.Error(), "does not match declaration") {
				t.Fatalf("ReconcileWakeup error = %v, want declaration binding rejection", err)
			}
			if active, draining := workflowTimerScheduledCounts(scheduler); active != 0 || draining != 0 {
				t.Fatalf("foreign workflow timer scheduled active=%d draining=%d", active, draining)
			}
		})
	}
}

func VerifyNativeWorkflowTimerLifecycleRollbackAndCancellationOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			fixture, pc, ctx, bus, entityID, activation := nativeWorkflowTimerOwnerActivationForTest(t, tc.name, false, "1h", time.Now().Add(-2*time.Hour), false, executionmode.Live, open)
			store := pc.workflowStore
			publishFailure := errors.New("publish failed")
			bus.prepareFailure = publishFailure

			outcome, err := fireWorkflowTimerTestWakeup(ctx, pc, activation)
			if !errors.Is(err, publishFailure) || outcome != WorkflowTimerFireRetry {
				t.Fatalf("failed fire outcome=%q err=%v, want retry publish failure", outcome, err)
			}
			persisted := loadWorkflowTimerOwnerActivation(t, store, ctx, activation.Ref.ActivationID)
			if persisted.Status != workflowTimerStatusActive || !persisted.FireAt.Equal(activation.FireAt) {
				t.Fatalf("rolled-back activation = %#v, want unchanged active row", persisted)
			}

			bus.prepareFailure = nil
			transitionAt := canonicalWorkflowTimerTime(time.Now())
			inbound := nativeWorkflowJoinEventForTest(ctx, ".", workflowTimerRootRoute(ctx).InstancePath, entityID, "work.completed", []byte(`{}`), transitionAt)
			dispatchNativeWorkflowJoinEventForTest(t, fixture, pc, ctx, inbound, "timer-owner")
			persisted = loadWorkflowTimerOwnerActivation(t, store, ctx, activation.Ref.ActivationID)
			if persisted.Status != workflowTimerStatusCancelled {
				t.Fatalf("cancelled activation status = %q, want cancelled", persisted.Status)
			}

			if err := pc.StopWorkflowTimerLifecycle(ctx); err != nil {
				t.Fatalf("join cancelled predecessor: %v", err)
			}
			nextFixture := fixture.ReopenExecution()
			restarted := nextFixture.NewCoordinator(PipelineCoordinatorOptions{Module: pc.module, Persistence: nextFixture.Persistence})
			restartedScheduler := newWorkflowTimerTestScheduler(t, restarted.workOwner)
			if err := restarted.workflowTimers.bindScheduler(restartedScheduler); err != nil {
				t.Fatal(err)
			}
			restartedCtx := runtimecorrelation.WithRunID(nextFixture.Context, activation.RunID)
			t.Cleanup(func() {
				stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				_ = restarted.StopWorkflowTimerLifecycle(stopCtx)
			})
			if err := restarted.RestoreWorkflowTimers(restartedCtx); err != nil {
				t.Fatalf("restore after cancel: %v", err)
			}
			registered, _ := workflowTimerScheduledCounts(restartedScheduler)
			if registered != 0 {
				t.Fatalf("restored cancelled workflow wakeups = %d, want 0", registered)
			}
		})
	}
}

func VerifyNativeWorkflowTimerLifecycleCommitOrdersConvergeOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	tests := []struct {
		name          string
		steps         []string
		wantStatus    string
		wantPublishes int
	}{
		{name: "cancel_then_fire", steps: []string{"cancel", "fire"}, wantStatus: workflowTimerStatusCancelled},
		{name: "fire_then_cancel", steps: []string{"fire", "cancel"}, wantStatus: workflowTimerStatusFired, wantPublishes: 1},
		{name: "unrelated_then_fire", steps: []string{"unrelated", "fire"}, wantStatus: workflowTimerStatusFired, wantPublishes: 1},
		{name: "fire_then_unrelated", steps: []string{"fire", "unrelated"}, wantStatus: workflowTimerStatusFired, wantPublishes: 1},
		{name: "unrelated_then_cancel", steps: []string{"unrelated", "cancel"}, wantStatus: workflowTimerStatusCancelled},
		{name: "cancel_then_unrelated", steps: []string{"cancel", "unrelated"}, wantStatus: workflowTimerStatusCancelled},
	}
	for _, tc := range workflowJoinStoreCases() {
		for _, test := range tests {
			t.Run(tc.name+"/"+test.name, func(t *testing.T) {
				fixture, pc, ctx, bus, _, activation := nativeWorkflowTimerOwnerActivationForTest(t, tc.name, false, "1h", time.Now().Add(-2*time.Hour), false, executionmode.Live, open)
				store := pc.workflowStore
				unrelatedApplied := false
				for _, step := range test.steps {
					switch step {
					case "fire":
						outcome, err := fireWorkflowTimerTestWakeup(ctx, pc, activation)
						if err != nil {
							t.Fatalf("fire: %v", err)
						}
						if test.wantStatus == workflowTimerStatusCancelled && outcome != WorkflowTimerFireTerminal {
							t.Fatalf("fire after cancel outcome = %q, want terminal", outcome)
						}
						if test.wantStatus == workflowTimerStatusFired && outcome != WorkflowTimerFireCommitted {
							t.Fatalf("fire outcome = %q, want committed", outcome)
						}
					case "cancel":
						if err := cancelNativeWorkflowTimerForTest(t, fixture, ctx, pc, activation); err != nil {
							t.Fatalf("cancel: %v", err)
						}
					case "unrelated":
						if err := store.mutateE(ctx, testRunScopedWorkflowRoute(ctx, workflowTimerRootRoute(ctx)), func(instance *WorkflowInstance) error {
							if instance.Fields == nil {
								instance.Fields = map[string]any{}
							}
							instance.Fields["unrelated_timer_order_proof"] = test.name
							return nil
						}); err != nil {
							t.Fatalf("unrelated workflow mutation: %v", err)
						}
						unrelatedApplied = true
					default:
						t.Fatalf("unknown proof step %q", step)
					}
				}

				persisted := loadWorkflowTimerOwnerActivation(t, store, ctx, activation.Ref.ActivationID)
				if persisted.Status != test.wantStatus || bus.publishedCount() != test.wantPublishes {
					t.Fatalf("converged timer = status:%s publishes:%d, want %s/%d", persisted.Status, bus.publishedCount(), test.wantStatus, test.wantPublishes)
				}
				if unrelatedApplied {
					instance, found, err := store.Load(ctx, testRunScopedWorkflowRoute(ctx, workflowTimerRootRoute(ctx)))
					if err != nil || !found || instance.Fields["unrelated_timer_order_proof"] != test.name {
						t.Fatalf("unrelated mutation found=%v value=%#v err=%v", found, instance.Fields["unrelated_timer_order_proof"], err)
					}
				}
			})
		}
	}
}

func VerifyNativeWorkflowTimerLifecycleRejectsMissingAndMismatchedCallbacksOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			_, pc, ctx, bus, _, activation := nativeWorkflowTimerOwnerActivationForTest(t, tc.name, false, "1h", time.Now().Add(-2*time.Hour), false, executionmode.Live, open)
			store := pc.workflowStore

			missingRef := activation.Ref
			missingRef.ActivationID = uuid.NewString()
			missingOccurrence := timeridentity.WorkflowTimerOccurrenceRef{Activation: missingRef, DueAt: activation.FireAt}
			missing := WorkflowTimerWakeup{family: workflowTimerActivationWakeup, occurrence: missingOccurrence, dueAt: activation.FireAt}
			outcome, err := fireTypedWorkflowTimerTestWakeup(ctx, pc, missing)
			if err != nil || outcome != WorkflowTimerFireTerminal {
				t.Fatalf("missing callback outcome=%q err=%v, want terminal nil", outcome, err)
			}

			mismatchedRef := activation.Ref
			mismatchedRef.DeclarationKey = "different.timer"
			mismatchedOccurrence := timeridentity.WorkflowTimerOccurrenceRef{Activation: mismatchedRef, DueAt: activation.FireAt}
			mismatched := WorkflowTimerWakeup{family: workflowTimerActivationWakeup, occurrence: mismatchedOccurrence, dueAt: activation.FireAt}
			outcome, err = fireTypedWorkflowTimerTestWakeup(ctx, pc, mismatched)
			if err == nil || outcome != WorkflowTimerFireTerminal {
				t.Fatalf("mismatched callback outcome=%q err=%v, want terminal error", outcome, err)
			}
			persisted := loadWorkflowTimerOwnerActivation(t, store, ctx, activation.Ref.ActivationID)
			if persisted.Status != workflowTimerStatusActive || bus.publishedCount() != 0 {
				t.Fatalf("activation after refused callbacks status=%q publishes=%d, want active/0", persisted.Status, bus.publishedCount())
			}

			outcome, err = fireWorkflowTimerTestWakeup(ctx, pc, activation)
			if err != nil || outcome != WorkflowTimerFireCommitted {
				t.Fatalf("canonical callback outcome=%q err=%v, want committed", outcome, err)
			}
			outcome, err = fireWorkflowTimerTestWakeup(ctx, pc, activation)
			if err != nil || outcome != WorkflowTimerFireTerminal {
				t.Fatalf("already-fired callback outcome=%q err=%v, want terminal nil", outcome, err)
			}
		})
	}
}

func VerifyNativeWorkflowTimerLifecycleIsolatesStaleActivationAcrossCancelAndReentryOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			fixture, pc, ctx, bus, entityID, first := nativeWorkflowTimerOwnerActivationForTest(t, tc.name, false, "1h", time.Now().Add(-2*time.Hour), false, executionmode.Live, open)
			store := pc.workflowStore
			cancelAt := canonicalWorkflowTimerTime(first.CreatedAt.Add(time.Minute))
			cancelEvent := nativeWorkflowJoinEventForTest(ctx, ".", workflowTimerRootRoute(ctx).InstancePath, entityID, "work.completed", []byte(`{}`), cancelAt)
			dispatchNativeWorkflowJoinEventForTest(t, fixture, pc, ctx, cancelEvent, "timer-owner")
			reenterAt := canonicalWorkflowTimerTime(cancelAt.Add(time.Minute))
			reenterEvent := nativeWorkflowJoinEventForTest(ctx, ".", workflowTimerRootRoute(ctx).InstancePath, entityID, "work.reopened", []byte(`{}`), reenterAt)
			dispatchNativeWorkflowJoinEventForTest(t, fixture, pc, ctx, reenterEvent, "timer-owner")
			active := listWorkflowTimerOwnerActivations(t, store, ctx, entityID, true)
			if len(active) != 1 || active[0].Ref.ActivationID == first.Ref.ActivationID {
				t.Fatalf("replacement activation = %#v, want one distinct active row", active)
			}
			second := active[0]

			outcome, err := fireWorkflowTimerTestWakeup(ctx, pc, first)
			if err != nil || outcome != WorkflowTimerFireTerminal || bus.publishedCount() != 0 {
				t.Fatalf("stale A callback outcome=%q publishes=%d err=%v, want terminal/0", outcome, bus.publishedCount(), err)
			}
			outcome, err = fireWorkflowTimerTestWakeup(ctx, pc, second)
			if err != nil || outcome != WorkflowTimerFireCommitted || bus.publishedCount() != 1 {
				t.Fatalf("replacement B callback outcome=%q publishes=%d err=%v, want committed/1", outcome, bus.publishedCount(), err)
			}
			if got, want := bus.publishedEvent(0).ID(), timeridentity.WorkflowTimerOccurrenceEventID(second.occurrence()); got != want {
				t.Fatalf("replacement event id = %q, want %q", got, want)
			}
		})
	}
}

func VerifyNativeWorkflowTimerWakeupReconciliationSerializesCancellationOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			fixture, pc, ctx, _, _, activation := nativeWorkflowTimerOwnerActivationForTest(t, tc.name, false, "1h", time.Now(), true, executionmode.Live, open)
			store := pc.workflowStore
			attempt, _, settle := fixture.AdmitAttachment(ctx, activation.RunID, activation.Route.InstancePath)
			defer settle()
			loaded := make(chan struct{})
			release := make(chan struct{})
			defer func() {
				select {
				case <-release:
				default:
					close(release)
				}
			}()
			var loadedOnce sync.Once
			pc.workflowTimers.testAfterWakeupLoad = func() {
				loadedOnce.Do(func() { close(loaded) })
				<-release
			}

			reconcileErr := make(chan error, 1)
			go func() {
				reconcileErr <- pc.workflowTimers.ReconcileWakeup(ctx, activation.Ref)
			}()
			select {
			case <-loaded:
			case <-time.After(time.Second):
				t.Fatal("wakeup reconciliation did not pause after canonical reload")
			}

			cancelErr := make(chan error, 1)
			go func() {
				committed, err := store.timerActivations.CommitWorkflowTimerReconciliation(ctx, WorkflowTimerReconciliationCommand{
					RunID: activation.RunID, Route: activation.Route, EntityID: activation.EntityID,
					ActivationAttempt: &attempt,
					Plan:              WorkflowLifecycleMutationPlan{Timers: []WorkflowTimerMutation{{Kind: WorkflowTimerMutationCancel, Activation: activation}}},
				})
				if err != nil {
					cancelErr <- err
					return
				}
				if len(committed.Cancellations) != 1 || committed.Cancellations[0] != activation.Ref {
					cancelErr <- fmt.Errorf("workflow timer cancellation evidence = %#v", committed.Cancellations)
					return
				}
				cancelErr <- pc.workflowTimers.queueCancellation(ctx, activation)
			}()
			waitForWorkflowTimerPersistedStatus(t, store, ctx, activation.Ref.ActivationID, workflowTimerStatusCancelled)
			close(release)

			if err := <-reconcileErr; err != nil {
				t.Fatalf("stale-snapshot reconciliation: %v", err)
			}
			if err := <-cancelErr; err != nil {
				t.Fatalf("cancel workflow timer: %v", err)
			}
			waitForWorkflowTimerSchedulerEmpty(t, pc.timerScheduler)
		})
	}
}

func VerifyNativeWorkflowTimerReconcileWithRecoveryQueuesAndConvergesOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			_, pc, ctx, _, _, activation := nativeWorkflowTimerOwnerActivationForTest(t, tc.name, false, "1h", time.Now(), true, executionmode.Live, open)
			scheduler := pc.workflowTimers.scheduler
			if err := scheduler.cancelWorkflowTimerWakeup(activation.Ref); err != nil {
				t.Fatal(err)
			}
			waitForWorkflowTimerSchedulerEmpty(t, scheduler)
			var once sync.Once
			pc.workflowTimers.testAfterWakeupLoad = func() {
				once.Do(func() { pc.workflowTimers.scheduler = nil })
			}

			queued, err := pc.workflowTimers.ReconcileWakeupWithRecovery(ctx, activation.Ref)
			pc.workflowTimers.scheduler = scheduler
			if !queued || !errors.Is(err, errWorkflowTimerSchedulerRequired) {
				t.Fatalf("initial reconciliation queued=%v err=%v, want queued scheduler failure", queued, err)
			}
			deadline := time.Now().Add(time.Second)
			for time.Now().Before(deadline) {
				active, draining := workflowTimerScheduledCounts(scheduler)
				if active == 1 && draining == 0 {
					return
				}
				runtime.Gosched()
			}
			active, draining := workflowTimerScheduledCounts(scheduler)
			t.Fatalf("recovered workflow timer scheduler active=%d draining=%d, want 1/0", active, draining)
		})
	}
}

func VerifyNativeWorkflowTimerWakeupReconciliationRetiresTerminalAndMissingRowsOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, tc := range workflowJoinStoreCases() {
		for _, state := range []string{"terminal", "missing"} {
			t.Run(tc.name+"/"+state, func(t *testing.T) {
				fixture, pc, ctx, _, _, activation := nativeWorkflowTimerOwnerActivationForTest(t, tc.name, false, "1h", time.Now(), true, executionmode.Live, open)
				switch state {
				case "terminal":
					attempt, _, release := fixture.AdmitAttachment(ctx, activation.RunID, activation.Route.InstancePath)
					defer release()
					committed, err := pc.workflowStore.timerActivations.CommitWorkflowTimerReconciliation(ctx, WorkflowTimerReconciliationCommand{
						RunID: activation.RunID, Route: activation.Route, EntityID: activation.EntityID, ActivationAttempt: &attempt,
						Plan: WorkflowLifecycleMutationPlan{Timers: []WorkflowTimerMutation{{Kind: WorkflowTimerMutationCancel, Activation: activation}}},
					})
					if err != nil {
						t.Fatalf("terminalize workflow timer: %v", err)
					}
					if len(committed.Cancellations) != 1 || committed.Cancellations[0] != activation.Ref {
						t.Fatalf("workflow timer cancellation evidence = %#v", committed.Cancellations)
					}
				case "missing":
					before := fixture.Transactions()
					if err := fixture.RemoveTimer(ctx, activation.RunID, activation.Ref.ActivationID); err != nil {
						t.Fatalf("delete workflow timer row: %v", err)
					}
					if after := fixture.Transactions(); after.OtherCommits != before.OtherCommits+1 || after.Active != 0 {
						t.Fatalf("missing timer fault escaped its original coordinator: before=%+v after=%+v", before, after)
					}
				default:
					t.Fatalf("unsupported state %q", state)
				}

				if err := pc.workflowTimers.ReconcileWakeup(ctx, activation.Ref); err != nil {
					t.Fatalf("reconcile %s workflow timer: %v", state, err)
				}
				waitForWorkflowTimerSchedulerEmpty(t, pc.timerScheduler)
			})
		}
	}
}

func VerifyNativeWorkflowTimerLifecycleStopFencesRestoreAndRecoveryOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, tc := range workflowJoinStoreCases() {
		for _, operation := range []string{"restore", "recovery"} {
			t.Run(tc.name+"/"+operation, func(t *testing.T) {
				_, pc, ctx, _, _, activation := nativeWorkflowTimerOwnerActivationForTest(t, tc.name, false, "1h", time.Now(), true, executionmode.Live, open)
				if err := pc.workflowTimers.retireWakeup(activation.Ref); err != nil {
					t.Fatalf("retire initial wakeup: %v", err)
				}
				waitForWorkflowTimerSchedulerEmpty(t, pc.timerScheduler)

				loaded := make(chan struct{})
				release := make(chan struct{})
				defer func() {
					select {
					case <-release:
					default:
						close(release)
					}
				}()
				var loadedOnce sync.Once
				pc.workflowTimers.testAfterWakeupLoad = func() {
					loadedOnce.Do(func() { close(loaded) })
					<-release
				}

				operationErr := make(chan error, 1)
				switch operation {
				case "restore":
					go func() { operationErr <- pc.RestoreWorkflowTimers(ctx) }()
				case "recovery":
					if !pc.workflowTimers.startWakeupRecovery(activation.Ref) {
						t.Fatal("start workflow timer recovery")
					}
				default:
					t.Fatalf("unsupported operation %q", operation)
				}
				select {
				case <-loaded:
				case <-time.After(time.Second):
					t.Fatalf("%s did not pause after canonical reload", operation)
				}

				stopStarted := make(chan struct{})
				stopErr := make(chan error, 1)
				go func() {
					close(stopStarted)
					stopCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
					defer cancel()
					stopErr <- pc.StopWorkflowTimerLifecycle(stopCtx)
				}()
				<-stopStarted
				close(release)
				if operation == "restore" {
					if err := <-operationErr; err != nil {
						t.Fatalf("restore workflow timers: %v", err)
					}
				}
				if err := <-stopErr; err != nil {
					t.Fatalf("stop workflow timer lifecycle: %v", err)
				}
				waitForWorkflowTimerSchedulerEmpty(t, pc.timerScheduler)
				pc.workflowTimers.recoveryMu.Lock()
				recovering := len(pc.workflowTimers.recovering)
				pc.workflowTimers.recoveryMu.Unlock()
				if recovering != 0 {
					t.Fatalf("recoveries after lifecycle stop = %d, want 0", recovering)
				}
				if err := pc.workflowTimers.ReconcileWakeup(ctx, activation.Ref); err != nil {
					t.Fatalf("post-stop reconciliation: %v", err)
				}
				waitForWorkflowTimerSchedulerEmpty(t, pc.timerScheduler)
			})
		}
	}
}

func VerifyNativeWorkflowTimerRecoveryCoalescesTypedOccurrencesAndJoinsForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name+"/coalesces", func(t *testing.T) {
			_, pc, ctx, _, _, activation := nativeWorkflowTimerOwnerActivationForTest(t, tc.name, false, "1h", time.Now(), true, executionmode.Live, open)
			if err := pc.workflowTimers.retireWakeup(activation.Ref); err != nil {
				t.Fatalf("retire initial workflow timer wakeup: %v", err)
			}
			waitForWorkflowTimerCondition(t, time.Second, func() bool {
				active, draining := workflowTimerScheduledCounts(pc.workflowTimers.scheduler)
				return active == 0 && draining == 0
			}, "initial typed wakeup cancellation")
			loaded, release := make(chan struct{}), make(chan struct{})
			defer func() {
				select {
				case <-release:
				default:
					close(release)
				}
			}()
			var once sync.Once
			pc.workflowTimers.testAfterWakeupLoad = func() {
				once.Do(func() { close(loaded) })
				<-release
			}
			for attempt := 0; attempt < 3; attempt++ {
				if !pc.workflowTimers.startWakeupRecovery(activation.Ref) {
					t.Fatalf("start coalesced recovery attempt %d", attempt+1)
				}
			}
			select {
			case <-loaded:
			case <-time.After(time.Second):
				t.Fatal("coalesced native recovery did not reach its load cut")
			}
			pc.workflowTimers.recoveryMu.Lock()
			recovering := len(pc.workflowTimers.recovering)
			pc.workflowTimers.recoveryMu.Unlock()
			if recovering != 1 {
				t.Fatalf("coalesced recoveries = %d, want 1", recovering)
			}
			close(release)
			waitForWorkflowTimerCondition(t, 5*time.Second, func() bool {
				active, _ := workflowTimerScheduledCounts(pc.workflowTimers.scheduler)
				return active == 1
			}, "coalesced typed wakeup registration")
			stopCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			if err := pc.StopWorkflowTimerLifecycle(stopCtx); err != nil {
				t.Fatalf("stop coalesced lifecycle: %v", err)
			}
			wakeups, draining := workflowTimerScheduledCounts(pc.workflowTimers.scheduler)
			pc.workflowTimers.recoveryMu.Lock()
			recovering = len(pc.workflowTimers.recovering)
			pc.workflowTimers.recoveryMu.Unlock()
			if wakeups != 0 || draining != 0 || recovering != 0 {
				t.Fatalf("joined lifecycle wakeups=%d draining=%d recovering=%d, want all zero", wakeups, draining, recovering)
			}
		})

		t.Run(tc.name+"/shutdown_cancels_pending", func(t *testing.T) {
			_, pc, ctx, _, _, activation := nativeWorkflowTimerOwnerActivationForTest(t, tc.name, false, "1h", time.Now(), true, executionmode.Live, open)
			if err := pc.workflowTimers.retireWakeup(activation.Ref); err != nil {
				t.Fatalf("retire initial workflow timer wakeup: %v", err)
			}
			if !pc.workflowTimers.startWakeupRecovery(activation.Ref) {
				t.Fatal("start pending recovery")
			}
			stopCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			if err := pc.StopWorkflowTimerLifecycle(stopCtx); err != nil {
				cancel()
				t.Fatalf("stop pending lifecycle: %v", err)
			}
			cancel()
			pc.workflowTimers.recoveryMu.Lock()
			recovering := len(pc.workflowTimers.recovering)
			pc.workflowTimers.recoveryMu.Unlock()
			if recovering != 0 {
				t.Fatalf("recoveries after shutdown = %d, want 0", recovering)
			}
		})
	}
}

func VerifyNativeWorkflowTimerGlobalRestoreDefersStandingUntilRunScopedAdoptionOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			fixture, pc, ctx, _, _, _ := nativeWorkflowTimerOwnerActivationForTest(t, tc.name, false, "1h", time.Now(), false, executionmode.Live, open)
			store := pc.workflowStore
			standingCtx := ctx
			flowPath := "standing-workflow-timer"
			// This component test owns timer storage and restoration, not standing
			// construction. Served tests exercise the native standing mutation owner.
			store.standingServices = workflowTimerStandingReadControl{fact: runtimerunlifecycle.StandingRestartFact{
				ServiceID: runtimeflowidentity.StandingServiceID(flowPath), RunID: runtimecorrelation.RunIDFromContext(ctx),
				Generation: 1, ExactCurrent: true, DeclarationPresent: true, BindingEnabled: true,
				EffectiveState: "active", OperatorOverride: "none", RunState: "running",
			}}
			scheduler := newWorkflowTimerTestScheduler(t, pc.workflowTimers.workOwner)
			if err := pc.workflowTimers.bindScheduler(scheduler); err != nil {
				t.Fatalf("bind workflow timer scheduler: %v", err)
			}

			globalCtx := fixture.Context
			if runtimecorrelation.RunIDFromContext(globalCtx) != "" {
				t.Fatal("global timer restore context unexpectedly carries a run")
			}
			if err := pc.RestoreWorkflowTimers(globalCtx); err != nil {
				t.Fatalf("global restore workflow timers: %v", err)
			}
			if active, draining := workflowTimerScheduledCounts(scheduler); active != 0 || draining != 0 {
				t.Fatalf("global restore scheduled standing wakeups active=%d draining=%d, want 0", active, draining)
			}

			if err := pc.RestoreWorkflowTimers(standingCtx); err != nil {
				t.Fatalf("run-scoped restore workflow timers: %v", err)
			}
			if active, draining := workflowTimerScheduledCounts(scheduler); active != 1 || draining != 0 {
				t.Fatalf("standing adoption scheduled wakeups active=%d draining=%d, want active=1 draining=0", active, draining)
			}
		})
	}
}

type workflowTimerStandingReadControl struct {
	StandingServicePersistence
	fact runtimerunlifecycle.StandingRestartFact
}

func (c workflowTimerStandingReadControl) StandingRunRestartDisposition(_ context.Context, runID string) (runtimerunlifecycle.StandingRestartDisposition, error) {
	if runID != c.fact.RunID {
		return runtimerunlifecycle.ClassifyStandingRestart(runtimerunlifecycle.StandingRestartFact{})
	}
	return runtimerunlifecycle.ClassifyStandingRestart(c.fact)
}

func VerifyNativeWorkflowTimerInitialEntryStaysDormantUntilExplicitArmOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			bundle := workflowTimerOwnerBundleWithDelay(t, false, "1ns")
			fixture, pc, ctx := nativePilotPipelineForTest(t, tc.name, bundle, open)
			store := pc.workflowStore
			bus := observeNativePipelineDeliveryBusForTest(t, pc)
			pc.workflowTimers.publication = bus
			pc.workflowTimers.dispatcher = bus.EngineDispatcher()
			scheduler := newWorkflowTimerTestScheduler(t, pc.workOwner)
			if err := pc.workflowTimers.bindScheduler(scheduler); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				join, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if err := pc.StopWorkflowTimerLifecycle(join); err != nil {
					t.Error(err)
				}
			})
			entityID := runtimecorrelation.RunIDFromContext(ctx)
			rootRoute := workflowTimerRootRoute(ctx)
			createdAt := canonicalWorkflowTimerTime(time.Now().Add(-time.Second))
			instance := materializedWorkflowInstanceForSource(t, pc.SemanticSource(), ctx, WorkflowInstance{
				InstanceID: entityID, StorageRef: entityID, EntityID: entityID, WorkflowName: ".",
				WorkflowVersion: bundle.WorkflowVersion(), CurrentState: "waiting",
				EntityType: "test_entity", CreatedAt: createdAt,
			})
			preparedInstance, preparedLifecycle, err := pc.PrepareInitialEntryLifecycle(ctx, testRunScopedWorkflowRoute(ctx, rootRoute), instance, createdAt)
			if err != nil {
				t.Fatalf("prepare fixture lifecycle: %v", err)
			}
			committedLifecycle, err := fixture.ConstructInitial(ctx, preparedInstance, preparedLifecycle)
			if err != nil {
				t.Fatalf("construct native initial lifecycle: %v", err)
			}
			if err := pc.FinalizeInitialEntryLifecycle(ctx, committedLifecycle); err != nil {
				t.Fatalf("finalize fixture lifecycle: %v", err)
			}
			active := listWorkflowTimerOwnerActivations(t, store, ctx, entityID, true)
			if len(active) != 1 {
				t.Fatalf("durable initial timers = %#v, want one", active)
			}
			if scheduled, draining := workflowTimerScheduledCounts(scheduler); scheduled != 0 || draining != 0 {
				t.Fatalf("pre-arm wakeups active=%d draining=%d, want 0", scheduled, draining)
			}
			if bus.publishedCount() != 0 {
				t.Fatalf("initial timer published before explicit arm: %s", bus.publishedEvent(0).ID())
			}

			if err := pc.ArmInitialEntryTimers(ctx, testRunScopedWorkflowRoute(ctx, rootRoute)); err != nil {
				t.Fatalf("ArmInitialEntryTimers: %v", err)
			}
			waitForWorkflowTimerCondition(t, time.Second, func() bool { return bus.publishedCount() > 0 }, "explicitly armed initial timer")
			event := bus.publishedEvent(0)
			if event.RunID() != runtimecorrelation.RunIDFromContext(ctx) || event.EntityID() != entityID {
				t.Fatalf("published timer scope run=%q entity=%q, want run=%q entity=%q", event.RunID(), event.EntityID(), runtimecorrelation.RunIDFromContext(ctx), entityID)
			}
			waitForWorkflowTimerPersistedStatus(t, store, ctx, active[0].Ref.ActivationID, workflowTimerStatusFired)
			if bus.publishedCount() != 1 || fixture.EventIDCount(ctx, event.ID()) != 1 {
				t.Fatalf("initial timer published more than once: %s", event.ID())
			}
		})
	}
}

func VerifyNativeWorkflowTimerInitialWakeupRetirementJoinsAndRearmsOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			_, pc, ctx, _, _, activation := nativeWorkflowTimerOwnerActivationForTest(t, tc.name, false, "1h", time.Now(), true, executionmode.Live, open)
			store := pc.workflowStore
			if err := pc.ArmInitialEntryTimers(ctx, testRunScopedWorkflowRoute(ctx, workflowTimerRootRoute(ctx))); err != nil {
				t.Fatalf("arm initial workflow timer: %v", err)
			}
			waitForWorkflowTimerCondition(t, time.Second, func() bool {
				active, draining := workflowTimerScheduledCounts(pc.workflowTimers.scheduler)
				return active == 1 && draining == 0
			}, "initial workflow timer wakeup")

			if err := pc.RetireInitialEntryTimerWakeups(ctx, testRunScopedWorkflowRoute(ctx, workflowTimerRootRoute(ctx))); err != nil {
				t.Fatalf("retire initial workflow timer wakeup: %v", err)
			}
			waitForWorkflowTimerSchedulerEmpty(t, pc.workflowTimers.scheduler)
			persisted := loadWorkflowTimerOwnerActivation(t, store, ctx, activation.Ref.ActivationID)
			if persisted.Status != workflowTimerStatusActive {
				t.Fatalf("retired wakeup durable status = %q, want active", persisted.Status)
			}

			if err := pc.ArmInitialEntryTimers(ctx, testRunScopedWorkflowRoute(ctx, workflowTimerRootRoute(ctx))); err != nil {
				t.Fatalf("rearm initial workflow timer: %v", err)
			}
			waitForWorkflowTimerCondition(t, time.Second, func() bool {
				active, draining := workflowTimerScheduledCounts(pc.workflowTimers.scheduler)
				return active == 1 && draining == 0
			}, "rearmed workflow timer wakeup")
		})
	}
}

func waitForWorkflowTimerCondition(t *testing.T, timeout time.Duration, condition func() bool, description string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}

func waitForWorkflowTimerPersistedStatus(
	t *testing.T,
	store *workflowInstanceStore,
	ctx context.Context,
	activationID string,
	want string,
) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		activation, found, err := store.loadPersistedWorkflowTimerActivation(ctx, activationID)
		if err == nil && found && activation.Status == want {
			return
		}
		lastErr = err
		runtime.Gosched()
	}
	t.Fatalf("workflow timer %s did not reach persisted status %s: last error=%v", activationID, want, lastErr)
}

func waitForWorkflowTimerSchedulerEmpty(t *testing.T, scheduler *Scheduler) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		active, draining := workflowTimerScheduledCounts(scheduler)
		if active == 0 && draining == 0 {
			return
		}
		runtime.Gosched()
	}
	active, draining := workflowTimerScheduledCounts(scheduler)
	t.Fatalf("workflow timer scheduler active=%d draining=%d, want empty", active, draining)
}

func VerifyNativeWorkflowTimerLifecycleSchedulerRetryPreservesOccurrenceOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			fixture, pc, ctx, bus, _, activation := nativeWorkflowTimerOwnerActivationForTest(t, tc.name, false, "1ms", time.Now().Add(-time.Hour), false, executionmode.Live, open)
			store := pc.workflowStore
			publishFailure := errors.New("transient publish failure")
			var failures atomic.Int32
			failures.Store(1)
			bus.beforePrepare = func(context.Context) error {
				if failures.CompareAndSwap(1, 0) {
					return publishFailure
				}
				return nil
			}
			if err := pc.workflowTimers.bindScheduler(newWorkflowTimerTestScheduler(t, pc.workOwner)); err != nil {
				t.Fatal(err)
			}
			if err := pc.workflowTimers.ReconcileWakeup(ctx, activation.Ref); err != nil {
				t.Fatalf("register workflow timer wakeup: %v", err)
			}
			waitForWorkflowTimerCondition(t, 5*time.Second, func() bool {
				return bus.publishedCount() == 1
			}, "retrying workflow timer wakeup")
			if bus.publishedCount() != 1 {
				t.Fatalf("published events after retry = %d, want 1", bus.publishedCount())
			}
			wantEventID := timeridentity.WorkflowTimerOccurrenceEventID(activation.occurrence())
			if got := bus.publishedEvent(0).ID(); got != wantEventID {
				t.Fatalf("retried occurrence event id = %q, want %q", got, wantEventID)
			}
			var persisted WorkflowTimerActivation
			waitForWorkflowTimerCondition(t, 5*time.Second, func() bool {
				persisted = loadWorkflowTimerOwnerActivation(t, store, ctx, activation.Ref.ActivationID)
				return persisted.Status == workflowTimerStatusFired
			}, "retrying workflow timer commit")
			if persisted.Status != workflowTimerStatusFired {
				t.Fatalf("retried activation status = %q, want fired", persisted.Status)
			}
			if failures.Load() != 0 || fixture.EventIDCount(ctx, wantEventID) != 1 {
				t.Fatal("retry did not consume the refusal and persist exactly one occurrence")
			}
		})
	}
}

func VerifyNativeWorkflowTimerLifecycleWakeupDeadlineJoinsShutdownOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			fixture, pc, ctx, bus, _, activation := nativeWorkflowTimerOwnerActivationForTest(t, tc.name, false, "1ms", time.Now().Add(-time.Hour), false, executionmode.Live, open)
			store := pc.workflowStore
			publishStarted := make(chan struct{})
			publishSettled := make(chan struct{})
			var publishOnce sync.Once
			bus.beforePrepare = func(callbackCtx context.Context) error {
				publishOnce.Do(func() { close(publishStarted) })
				if _, ok := callbackCtx.Deadline(); !ok {
					return errors.New("workflow timer publication context has no deadline")
				}
				<-callbackCtx.Done()
				close(publishSettled)
				return callbackCtx.Err()
			}
			pc.workflowTimers.wakeupCallbackTimeout = 50 * time.Millisecond
			if err := pc.workflowTimers.bindScheduler(newWorkflowTimerTestScheduler(t, pc.workflowTimers.workOwner)); err != nil {
				t.Fatalf("bind workflow timer scheduler: %v", err)
			}
			if err := pc.workflowTimers.ReconcileWakeup(ctx, activation.Ref); err != nil {
				t.Fatalf("register due workflow timer wakeup: %v", err)
			}
			select {
			case <-publishStarted:
			case <-time.After(time.Second):
				t.Fatal("workflow timer publication did not start")
			}

			stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := pc.StopWorkflowTimerLifecycle(stopCtx); err != nil {
				t.Fatalf("stop workflow timer lifecycle with stalled publication: %v", err)
			}
			select {
			case <-publishSettled:
			default:
				t.Fatal("workflow timer lifecycle stopped before the bounded publication settled")
			}
			waitForWorkflowTimerSchedulerEmpty(t, pc.workflowTimers.scheduler)
			persisted := loadWorkflowTimerOwnerActivation(t, store, ctx, activation.Ref.ActivationID)
			if persisted.Status != workflowTimerStatusActive {
				t.Fatalf("timed-out workflow timer status = %q, want active for durable recovery", persisted.Status)
			}
			if fixture.EventIDCount(ctx, timeridentity.WorkflowTimerOccurrenceEventID(activation.occurrence())) != 0 {
				t.Fatal("timed-out preparation persisted an occurrence publication")
			}
		})
	}
}

func VerifyNativeWorkflowTimerLifecyclePostgresFireDoesNotJoinOuterTestMutationForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, tc := range workflowJoinStoreCases() {
		if tc.name != "postgres" {
			continue
		}
		fixture, pc, ctx, _, _, activation := nativeWorkflowTimerOwnerActivationForTest(t, tc.name, false, "1h", time.Now().Add(-2*time.Hour), false, executionmode.Live, open)
		store := pc.workflowStore
		rollback, err := fixture.HoldUnstampedAdmissionTransaction(ctx, activation.RunID)
		if err != nil {
			t.Fatalf("hold original-coordinator outer mutation: %v", err)
		}
		t.Cleanup(func() {
			if err := rollback(); err != nil {
				t.Error(err)
			}
		})
		if fixture.Transactions().Active != 1 {
			t.Fatal("outer mutation did not reach the original coordinator")
		}
		outcome, err := fireWorkflowTimerTestWakeup(ctx, pc, activation)
		if err != nil || outcome != WorkflowTimerFireCommitted {
			t.Fatalf("FireWorkflowTimer outcome=%q err=%v, want independently committed named operation", outcome, err)
		}
		if fixture.Transactions().Active != 1 {
			t.Fatal("independent fire settled or retained the outer transaction")
		}
		if err := rollback(); err != nil {
			t.Fatalf("Rollback: %v", err)
		}
		if fixture.Transactions().Active != 0 {
			t.Fatal("outer rollback did not join its original transaction")
		}
		persisted := loadWorkflowTimerOwnerActivation(t, store, ctx, activation.Ref.ActivationID)
		if persisted.Status != workflowTimerStatusFired {
			t.Fatalf("named-operation timer status = %q, want fired", persisted.Status)
		}
	}
}

func VerifyNativeWorkflowTimerLifecycleWakeupPublishesItsDurableIntentOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			fixture, pc, ctx, bus, _, activation := nativeWorkflowTimerOwnerActivationForTest(t, tc.name, false, "1h", time.Now().Add(-2*time.Hour), false, executionmode.Live, open)
			store := pc.workflowStore
			wakeup, err := newWorkflowTimerWakeup(activation)
			if err != nil {
				t.Fatalf("new workflow timer wakeup: %v", err)
			}
			pc.workflowTimers.handleWakeup(ctx, wakeup)
			if got := bus.publishedCount(); got != 1 {
				t.Fatalf("durable timer publications = %d, want 1", got)
			}
			published := bus.publishedEvent(0)
			if fixture.EventIDCount(ctx, published.ID()) != 1 {
				t.Fatal("timer handoff has no exact original-store publication")
			}
			persisted := loadWorkflowTimerOwnerActivation(t, store, ctx, activation.Ref.ActivationID)
			if persisted.Status != workflowTimerStatusFired {
				t.Fatalf("durable timer status = %q, want fired", persisted.Status)
			}
		})
	}
}

func workflowTimerRootRoute(ctx context.Context) runtimeflowidentity.Route {
	runID := runtimecorrelation.RunIDFromContext(ctx)
	return runtimeflowidentity.StoredRoute(".", runID, runID)
}

func workflowTimerMaterializedInstance(
	ctx context.Context,
	entityID string,
	instancePath string,
	instance WorkflowInstance,
) WorkflowInstance {
	instancePath = strings.Trim(strings.TrimSpace(instancePath), "/")
	instance.InstanceID = runtimeflowidentity.LogicalInstanceID(instancePath)
	instance.StorageRef = instancePath
	instance.EntityID = strings.TrimSpace(entityID)
	if instancePath == runtimecorrelation.RunIDFromContext(ctx) {
		instance.WorkflowName = "."
	}
	instance.Fields = cloneStringAnyMap(instance.Fields)
	if instance.Fields == nil {
		instance.Fields = map[string]any{}
	}
	return materializedWorkflowInstanceForTest(instance)
}

func newWorkflowTimerTestScheduler(t *testing.T, owner worklifetime.Occurrence) *Scheduler {
	t.Helper()
	scheduler := NewSchedulerWithWorkOwner(owner)
	t.Cleanup(func() {
		scheduler.Stop()
		waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := scheduler.Wait(waitCtx); err != nil {
			t.Errorf("wait for workflow timer test scheduler: %v", err)
		}
	})
	return scheduler
}

func workflowTimerScheduledCounts(scheduler *Scheduler) (active, draining int) {
	if scheduler == nil {
		return 0, 0
	}
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	for _, task := range scheduler.tasks {
		if task.projection.kind == scheduledProjectionWorkflowTimer {
			active++
		}
	}
	for task := range scheduler.draining {
		if task.projection.kind == scheduledProjectionWorkflowTimer {
			draining++
		}
	}
	return active, draining
}

func WorkflowTimerScheduledCountsForTest(scheduler *Scheduler) (active, draining int) {
	return workflowTimerScheduledCounts(scheduler)
}

func loadWorkflowTimerOwnerActivation(t *testing.T, store *workflowInstanceStore, ctx context.Context, activationID string) WorkflowTimerActivation {
	t.Helper()
	activation, found, err := store.loadPersistedWorkflowTimerActivation(ctx, activationID)
	if err != nil || !found {
		t.Fatalf("load workflow timer activation found=%v err=%v", found, err)
	}
	return activation
}

func listWorkflowTimerOwnerActivations(t *testing.T, store *workflowInstanceStore, ctx context.Context, entityID string, activeOnly bool) []WorkflowTimerActivation {
	t.Helper()
	activations, err := store.listPersistedWorkflowTimerActivations(ctx, runtimecorrelation.RunIDFromContext(ctx), entityID, activeOnly)
	if err != nil {
		t.Fatalf("list workflow timer activations: %v", err)
	}
	return activations
}

func workflowTimerOwnerBundle(t *testing.T, recurring bool) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	return workflowTimerOwnerBundleWithDelay(t, recurring, "1h")
}

func workflowTimerOwnerBundleWithDelay(t *testing.T, recurring bool, delay string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	files := workflowTimerOwnerSourceFiles()
	files["schema.yaml"] = fmt.Sprintf("name: workflow-timer-owner-test\nstages:\n  waiting:\n    timers:\n      - {id: waiting.timeout, after: %q, emit: timer.timeout}\n  done: {}\n", delay)
	bundle := loadWorkflowTempBundle(t, files)
	// Recurrence is a scheduler variant, not a different transition declaration.
	bundle.Semantics.Timers[0].Recurring = recurring
	return bundle
}

func workflowTimerOwnerSourceFiles() map[string]string {
	return map[string]string{
		"schema.yaml":   "name: workflow-timer-owner-test\nstages:\n  waiting: {}\n  done: {}\n",
		"entities.yaml": "test_entity: {}\n",
		"events.yaml":   "timer.timeout:\nwork.completed:\nwork.reopened:\nreview.reopened:\ntest.workflow_progressed:\n",
		"nodes.yaml": `timer-owner:
  execution_type: system_node
  event_handlers:
    work.completed: {advances_to: done}
    work.reopened: {advances_to: waiting}
    review.reopened: {advances_to: waiting}
    test.workflow_progressed: {advances_to: done}
`,
	}
}

func workflowTimerSourceRevisionBundle(t *testing.T, revised bool) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	timers := "      - {id: waiting.keep, after: 1h, emit: timer.keep}\n"
	if revised {
		timers += "      - {id: waiting.changed, after: 2h, emit: timer.changed.v2}\n      - {id: waiting.added, after: 30m, emit: timer.added}\n"
	} else {
		timers += "      - {id: waiting.changed, after: 1h, emit: timer.changed.v1}\n      - {id: waiting.removed, after: 1h, emit: timer.removed}\n"
	}
	return loadWorkflowTempBundle(t, map[string]string{
		"schema.yaml":   "name: workflow-timer-source-revision\nstages:\n  waiting:\n    timers:\n" + timers,
		"entities.yaml": "test_entity: {}\n",
		"events.yaml":   "timer.keep:\ntimer.changed.v1:\ntimer.changed.v2:\ntimer.added:\ntimer.removed:\n",
	})
}

func workflowTimerFirstDeclarationRevisionBundle(t *testing.T, revised bool) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	schema := "name: workflow-timer-first-revision\nstages:\n  waiting: {}\n"
	if revised {
		schema = "name: workflow-timer-first-revision\nstages:\n  waiting:\n    timers:\n      - {id: waiting.first, after: 1s, emit: timer.first}\n"
	}
	return loadWorkflowTempBundle(t, map[string]string{
		"schema.yaml":   schema,
		"entities.yaml": "test_entity: {}\n",
		"events.yaml":   "timer.first:\n",
	})
}

func workflowTimerProgressedSourceRevisionBundle(t *testing.T, revised bool) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	files := workflowTimerOwnerSourceFiles()
	files["schema.yaml"] = "name: workflow-timer-progressed-revision\nstages:\n  waiting: {}\n  done: {}\n"
	files["events.yaml"] += "timer.keep:\ntimer.changed.v1:\ntimer.changed.v2:\ntimer.removed:\ntimer.added:\n"
	files["nodes.yaml"] += "  timers:\n    - {id: waiting.keep, event: timer.keep, start_on: 'state:waiting', delay: 1h}\n"
	if revised {
		files["nodes.yaml"] += "    - {id: waiting.changed, event: timer.changed.v2, start_on: 'state:waiting', delay: 2h}\n    - {id: waiting.added, event: timer.added, start_on: 'state:waiting', delay: 30m}\n"
	} else {
		files["nodes.yaml"] += "    - {id: waiting.changed, event: timer.changed.v1, start_on: 'state:waiting', delay: 1h}\n    - {id: waiting.removed, event: timer.removed, start_on: 'state:waiting', delay: 1h}\n"
	}
	return loadWorkflowTempBundle(t, files)
}

func workflowTimerInitialAndEventBundle(t *testing.T) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	return loadWorkflowTempBundle(t, map[string]string{
		"schema.yaml":   "name: workflow-timer-initial-event\nstages:\n  waiting:\n    timers:\n      - {id: waiting.initial, after: 2h, emit: timer.initial}\n",
		"entities.yaml": "test_entity: {}\n",
		"events.yaml":   "timer.arm:\ntimer.initial:\ntimer.event:\n",
		"nodes.yaml":    "timer-owner:\n  execution_type: system_node\n  timers:\n    - {id: waiting.event, event: timer.event, start_on: 'event:timer.arm', delay: 2h}\n  event_handlers:\n    timer.arm: {}\n",
	})
}

func workflowTimerFlowScopedBundle(t *testing.T) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	files := map[string]string{}
	for _, flow := range []string{".", "flow-a", "flow-b"} {
		path, name, prefix := flow+"/", flow, strings.ReplaceAll(flow, "-", "_")
		armEvent := flow + "/timer.arm"
		if flow == "." {
			path, name, prefix = "", "timer-flow-scope-root", "root"
			armEvent = "timer.arm"
		}
		files[path+"schema.yaml"] = fmt.Sprintf("name: %s\nstages:\n  waiting:\n    timers:\n      - {id: initial.local, after: 2h, emit: %s.initial}\n", name, prefix)
		files[path+"entities.yaml"] = "test_entity: {}\n"
		files[path+"events.yaml"] = fmt.Sprintf("timer.arm:\n%s.initial:\n%s.event:\n", prefix, prefix)
		files[path+"nodes.yaml"] = fmt.Sprintf("timer-owner:\n  execution_type: system_node\n  timers:\n    - {id: event.local, event: %s.event, start_on: 'event:%s', delay: 2h}\n  event_handlers:\n    timer.arm: {}\n", prefix, armEvent)
	}
	return loadWorkflowTempBundle(t, files)
}

func workflowTimerEventOnlyStateTriggerBundle(t *testing.T) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	return loadWorkflowTempBundle(t, map[string]string{
		"schema.yaml":   "name: workflow-timer-owner-test\nstages:\n  waiting:\n    timers:\n      - {id: waiting.state_entry, after: 1h, emit: timer.state_entry}\n",
		"entities.yaml": "test_entity: {}\n",
		"events.yaml":   "timer.arm:\nwork.noted:\ntimer.state_entry:\ntimer.event_armed:\n",
		"nodes.yaml":    "observer:\n  execution_type: system_node\n  timers:\n    - {id: waiting.event_armed, event: timer.event_armed, start_on: 'event:timer.arm', cancel_on: 'state:waiting', delay: 1h}\n  event_handlers:\n    timer.arm: {}\n    work.noted: {}\n",
	})
}

func workflowTimerLoopEventBundle(t *testing.T) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	return loadWorkflowTempBundle(t, map[string]string{
		"schema.yaml":   "name: workflow-timer-owner-test\nstages:\n  ready: {}\n  waiting: {}\n  escaped: {}\nloops:\n  revision:\n    revision_field: revision_id\n    max_attempts: 3\n    escape: {advances_to: escaped}\n",
		"entities.yaml": "test_entity: {}\n",
		"events.yaml":   "loop.start:\nloop.repeat:\n  revision_id: text\ntimer.arm:\n  revision_id: text\ntimer.event_armed:\n",
		"nodes.yaml": `observer:
  execution_type: system_node
  timers:
    - {id: waiting.event_armed, event: timer.event_armed, start_on: 'event:timer.arm', delay: 1h}
  event_handlers:
    loop.start:
      loop: {start: revision, from: ready}
      advances_to: waiting
    timer.arm:
      loop: {admit: revision, from: waiting}
    loop.repeat:
      loop: {repeat: revision, from: waiting}
      advances_to: waiting
`,
	})
}

func workflowTimerHandledOutcomeBundle(t *testing.T) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	return loadWorkflowTempBundle(t, map[string]string{
		"schema.yaml":   "name: workflow-timer-owner-test\nstages:\n  waiting: {}\n",
		"entities.yaml": "test_entity: {}\n",
		"events.yaml":   "accepted.start:\naccepted.cancel:\nreject.target:\nguard.reject:\ndiscard.target:\nguard.discard:\ndedup.reset:\ndedup.target:\ndedup.event:\n  item_id: text\ntimer.accepted:\ntimer.reject.start:\ntimer.reject.target:\ntimer.discard.start:\ntimer.discard.target:\ntimer.dedup.start:\ntimer.dedup.target:\n",
		"nodes.yaml": `observer:
  execution_type: system_node
  timers:
    - {id: accepted, event: timer.accepted, start_on: 'event:accepted.start', cancel_on: 'event:accepted.cancel', delay: 1h}
    - {id: reject.start, event: timer.reject.start, start_on: 'event:guard.reject', delay: 1h}
    - {id: reject.target, event: timer.reject.target, start_on: 'event:reject.target', cancel_on: 'event:guard.reject', delay: 1h}
    - {id: discard.start, event: timer.discard.start, start_on: 'event:guard.discard', delay: 1h}
    - {id: discard.target, event: timer.discard.target, start_on: 'event:discard.target', cancel_on: 'event:guard.discard', delay: 1h}
    - {id: dedup.start, event: timer.dedup.start, start_on: 'event:dedup.event', cancel_on: 'event:dedup.reset', delay: 1h}
    - {id: dedup.target, event: timer.dedup.target, start_on: 'event:dedup.target', cancel_on: 'event:dedup.event', delay: 1h}
  event_handlers:
    accepted.start: {}
    accepted.cancel: {}
    reject.target: {}
    guard.reject:
      guard:
        check: false
        on_fail: reject
    discard.target: {}
    guard.discard:
      guard:
        check: false
        on_fail: discard
    dedup.event:
      accumulate: {into: items, from: payload, key: payload.item_id}
    dedup.reset: {}
    dedup.target: {}
`,
	})
}
