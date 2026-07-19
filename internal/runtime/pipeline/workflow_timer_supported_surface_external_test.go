package pipeline_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
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
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
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
			ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
			source := semanticview.Wrap(workflowTimerServedLifecycleBundle(false))
			bus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{
				ContractBundle: source, PayloadValidator: strictWorkflowTimerPayloadValidator,
			}, runtimecontracts.WorkflowStageTimerInternalEvent)
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			scheduleStore, ok := selected.events.(runtimepipeline.SchedulePersistence)
			if !ok {
				t.Fatalf("selected %s store does not implement SchedulePersistence", selected.name)
			}

			fireErrors := make(chan error, 4)
			var coordinator *runtimepipeline.PipelineCoordinator
			scheduler := runtimepipeline.NewScheduler(func(schedule runtimepipeline.Schedule) {
				if coordinator == nil {
					fireErrors <- fmt.Errorf("workflow timer coordinator is unavailable")
					return
				}
				_, err := coordinator.FireWorkflowTimer(ctx, schedule)
				if err != nil {
					fireErrors <- err
				}
			})
			t.Cleanup(scheduler.Stop)
			coordinator = runtimepipeline.NewPipelineCoordinatorWithOptions(bus, selected.db, runtimepipeline.PipelineCoordinatorOptions{
				Module:             gateRecoveryModule{source: source},
				WorkflowStore:      selected.workflowStore,
				TimerScheduler:     scheduler,
				TimerScheduleStore: scheduleStore,
			})
			bus.SetInterceptors(coordinator)

			createdAt := time.Now().UTC()
			if err := selected.workflowStore.Upsert(ctx, runtimepipeline.WorkflowInstance{
				InstanceID: entityID, StorageRef: entityID, WorkflowName: "timer-proof", WorkflowVersion: "1",
				CurrentState: "waiting", EnteredStageAt: createdAt, CreatedAt: createdAt,
				Metadata: map[string]any{"run_id": runID},
			}); err != nil {
				t.Fatalf("seed workflow instance: %v", err)
			}
			if err := coordinator.ArmFlowInstanceInitialStageLifecycle(ctx, entityID); err != nil {
				t.Fatalf("ArmFlowInstanceInitialStageLifecycle: %v", err)
			}

			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) {
				select {
				case err := <-fireErrors:
					t.Fatalf("workflow timer callback: %v", err)
				default:
				}
				instance, found, err := selected.workflowStore.Load(ctx, entityID)
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
			ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
			source := semanticview.Wrap(workflowTimerServedLifecycleBundle(false))
			bus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{
				ContractBundle: source, PayloadValidator: strictWorkflowTimerPayloadValidator,
			}, runtimecontracts.WorkflowStageTimerInternalEvent)
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			scheduleStore, ok := selected.events.(runtimepipeline.SchedulePersistence)
			if !ok {
				t.Fatalf("selected %s store does not implement SchedulePersistence", selected.name)
			}
			module := gateRecoveryModule{source: source}
			coordinator := runtimepipeline.NewPipelineCoordinatorWithOptions(bus, selected.db, runtimepipeline.PipelineCoordinatorOptions{
				Module: module, WorkflowStore: selected.workflowStore, TimerScheduleStore: scheduleStore,
			})
			bus.SetInterceptors(coordinator)

			createdAt := time.Now().UTC()
			if err := selected.workflowStore.Upsert(ctx, runtimepipeline.WorkflowInstance{
				InstanceID: entityID, StorageRef: entityID, WorkflowName: "timer-proof", WorkflowVersion: "1",
				CurrentState: "waiting", EnteredStageAt: createdAt, CreatedAt: createdAt,
				Metadata: map[string]any{"run_id": runID},
			}); err != nil {
				t.Fatalf("seed workflow instance: %v", err)
			}
			if err := coordinator.ArmFlowInstanceInitialStageLifecycle(ctx, entityID); err != nil {
				t.Fatalf("arm timer before restart: %v", err)
			}
			assertWorkflowTimerServedRows(t, selected, runID, entityID, "active", 1)

			fireErrors := make(chan error, 4)
			var restored *runtimepipeline.PipelineCoordinator
			scheduler := runtimepipeline.NewScheduler(func(schedule runtimepipeline.Schedule) {
				_, err := restored.FireWorkflowTimer(ctx, schedule)
				if err != nil {
					fireErrors <- err
				}
			})
			restored = runtimepipeline.NewPipelineCoordinatorWithOptions(bus, selected.db, runtimepipeline.PipelineCoordinatorOptions{
				Module: module, WorkflowStore: selected.workflowStore,
				TimerScheduler: scheduler, TimerScheduleStore: scheduleStore,
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
				instance, found, err := selected.workflowStore.Load(ctx, entityID)
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
			if err := scheduleStore.ReleaseScheduleClaims(ctx); err != nil {
				t.Fatalf("release one-shot claims for second restart: %v", err)
			}

			terminalScheduler := runtimepipeline.NewScheduler(func(schedule runtimepipeline.Schedule) {
				fireErrors <- fmt.Errorf("terminal timer was restored: %s", schedule.TaskID)
			})
			t.Cleanup(terminalScheduler.Stop)
			terminal := runtimepipeline.NewPipelineCoordinatorWithOptions(bus, selected.db, runtimepipeline.PipelineCoordinatorOptions{
				Module: module, WorkflowStore: selected.workflowStore,
				TimerScheduler: terminalScheduler, TimerScheduleStore: scheduleStore,
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
			ctx := withLiveGateExecution(runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID))
			bundle := workflowTimerServedLifecycleBundle(true)
			bundle.Semantics.Timers[0].AdvancesTo = ""
			cancelHandler := runtimecontracts.SystemNodeEventHandler{AdvancesTo: "done"}
			bundle.Nodes = map[string]runtimecontracts.SystemNodeContract{
				"controller": {
					ID: "controller", ExecutionType: runtimecontracts.SystemNodeExecutionType,
					SubscribesTo: []string{"timer.cancel"},
					EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
						"timer.cancel": cancelHandler,
					},
				},
			}
			bundle.Semantics.NodeHandlers = map[string]map[string]runtimecontracts.SystemNodeEventHandler{
				"controller": {"timer.cancel": cancelHandler},
			}
			bundle.Semantics.EventOwners = map[string][]string{"timer.cancel": {"controller"}}
			bundle.Semantics.EffectiveNodes = map[string]runtimecontracts.SystemNodeEffectiveSemantics{
				"controller": {
					ID: "controller", ExecutionType: runtimecontracts.SystemNodeExecutionType,
					RuntimeSubscriptions: []string{"timer.cancel"},
				},
			}
			flow := runtimecontracts.FlowContractView{
				Path: "timer-proof", Paths: runtimecontracts.FlowContractPaths{ID: "timer-proof", Flow: "timer-proof"},
				Nodes: bundle.Nodes, Events: map[string]runtimecontracts.EventCatalogEntry{"timer.cancel": {}},
			}
			bundle.FlowTree = runtimecontracts.FlowTree{
				Root: &flow, ByID: map[string]*runtimecontracts.FlowContractView{"timer-proof": &flow},
			}
			bundle.FlowSchemas = map[string]runtimecontracts.FlowSchemaDocument{"timer-proof": {}}
			source := semanticview.Wrap(bundle)
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
						"timer-proof/timer.cancel": {Consume: true, RequireEntity: true},
					},
				}},
			}
			bus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{
				ContractBundle: source, PayloadValidator: strictWorkflowTimerPayloadValidator,
			}, runtimecontracts.WorkflowStageTimerInternalEvent)
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			scheduleStore, ok := selected.events.(runtimepipeline.SchedulePersistence)
			if !ok {
				t.Fatalf("selected %s store does not implement SchedulePersistence", selected.name)
			}

			var coordinator *runtimepipeline.PipelineCoordinator
			fireErrors := make(chan error, 8)
			newScheduler := func() *runtimepipeline.Scheduler {
				return runtimepipeline.NewScheduler(func(schedule runtimepipeline.Schedule) {
					if coordinator == nil {
						fireErrors <- fmt.Errorf("workflow timer coordinator is unavailable")
						return
					}
					_, err := coordinator.FireWorkflowTimer(ctx, schedule)
					if err != nil {
						fireErrors <- err
					}
				})
			}
			scheduler := newScheduler()
			coordinator = runtimepipeline.NewPipelineCoordinatorWithOptions(bus, selected.db, runtimepipeline.PipelineCoordinatorOptions{
				Module: module, WorkflowStore: selected.workflowStore,
				TimerScheduler: scheduler, TimerScheduleStore: scheduleStore,
			})
			bus.SetInterceptors(coordinator)

			createdAt := time.Now().UTC()
			if err := selected.workflowStore.Upsert(ctx, runtimepipeline.WorkflowInstance{
				InstanceID: entityID, StorageRef: entityID, WorkflowName: "timer-proof", WorkflowVersion: "1",
				CurrentState: "waiting", EnteredStageAt: createdAt, CreatedAt: createdAt,
				Metadata: map[string]any{"run_id": runID},
			}); err != nil {
				t.Fatalf("seed workflow instance: %v", err)
			}
			if err := coordinator.ArmFlowInstanceInitialStageLifecycle(ctx, entityID); err != nil {
				t.Fatalf("ArmFlowInstanceInitialStageLifecycle: %v", err)
			}
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
			coordinator = runtimepipeline.NewPipelineCoordinatorWithOptions(bus, selected.db, runtimepipeline.PipelineCoordinatorOptions{
				Module: module, WorkflowStore: selected.workflowStore,
				TimerScheduler: scheduler, TimerScheduleStore: scheduleStore,
			})
			bus.SetInterceptors(coordinator)
			runCtx, cancelRun := context.WithCancel(ctx)
			t.Cleanup(cancelRun)
			subscribed := make(chan struct{}, 1)
			coordinator.SetTestSubscribeHook(func() {
				select {
				case subscribed <- struct{}{}:
				default:
				}
			})
			go coordinator.Run(runCtx)
			select {
			case <-subscribed:
			case <-time.After(2 * time.Second):
				t.Fatal("workflow coordinator did not subscribe after restart")
			}
			if err := coordinator.RestoreWorkflowTimers(ctx); err != nil {
				t.Fatalf("RestoreWorkflowTimers: %v", err)
			}
			waitWorkflowTimerEventCount(t, selected, fireErrors, runID, runtimecontracts.WorkflowStageTimerInternalEvent, beforeRestart+1)

			cancelEvent := eventtest.RunCreatingRootIngress(
				uuid.NewString(), "timer-proof/timer.cancel", "operator", "", []byte(`{}`), 0, runID, "",
				events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), "timer-proof"), time.Now().UTC(),
			)
			if err := bus.Publish(ctx, cancelEvent); err != nil {
				t.Fatalf("publish timer cancellation transition: %v", err)
			}
			cancelled := false
			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) {
				instance, found, err := selected.workflowStore.Load(ctx, entityID)
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

func TestRecurringWorkflowTimerRegistersNextOccurrenceWhenPostgresReleaseInitiallyFails(t *testing.T) {
	selected := openPostgresGateRecoveryStore(t)
	runID := uuid.NewString()
	entityID := uuid.NewString()
	insertGateRecoveryRun(t, selected, runID)
	ctx := withLiveGateExecution(runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID))
	bundle := workflowTimerServedLifecycleBundle(true)
	bundle.Semantics.Timers[0].AdvancesTo = ""
	source := semanticview.Wrap(bundle)
	bus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{
		ContractBundle: source, PayloadValidator: strictWorkflowTimerPayloadValidator,
	}, runtimecontracts.WorkflowStageTimerInternalEvent)
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	baseScheduleStore, ok := selected.events.(runtimepipeline.SchedulePersistence)
	if !ok {
		t.Fatalf("selected postgres store does not implement SchedulePersistence")
	}
	scheduleStore := &failOnceReleaseSchedulePersistence{
		SchedulePersistence: baseScheduleStore,
		failures:            1,
		releaseAttempts:     make(map[string]int),
	}

	fireErrors := make(chan error, 8)
	var coordinator *runtimepipeline.PipelineCoordinator
	scheduler := runtimepipeline.NewScheduler(func(schedule runtimepipeline.Schedule) {
		if coordinator == nil {
			fireErrors <- fmt.Errorf("workflow timer coordinator is unavailable")
			return
		}
		if _, err := coordinator.FireWorkflowTimer(ctx, schedule); err != nil {
			fireErrors <- err
		}
	})
	coordinator = runtimepipeline.NewPipelineCoordinatorWithOptions(bus, selected.db, runtimepipeline.PipelineCoordinatorOptions{
		Module: gateRecoveryModule{source: source}, WorkflowStore: selected.workflowStore,
		TimerScheduler: scheduler, TimerScheduleStore: scheduleStore,
	})
	bus.SetInterceptors(coordinator)
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = coordinator.StopWorkflowTimerLifecycle(stopCtx)
		scheduler.Stop()
		_ = scheduler.Wait(stopCtx)
	})

	createdAt := time.Now().UTC()
	if err := selected.workflowStore.Upsert(ctx, runtimepipeline.WorkflowInstance{
		InstanceID: entityID, StorageRef: entityID, WorkflowName: "timer-proof", WorkflowVersion: "1",
		CurrentState: "waiting", EnteredStageAt: createdAt, CreatedAt: createdAt,
		Metadata: map[string]any{"run_id": runID},
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}
	if err := coordinator.ArmFlowInstanceInitialStageLifecycle(ctx, entityID); err != nil {
		t.Fatalf("ArmFlowInstanceInitialStageLifecycle: %v", err)
	}
	waitWorkflowTimerEventCount(t, selected, fireErrors, runID, runtimecontracts.WorkflowStageTimerInternalEvent, 2)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if scheduleStore.retriedRelease() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("initial failed claim release was not retried: %#v", scheduleStore.snapshotReleaseAttempts())
}

type failOnceReleaseSchedulePersistence struct {
	runtimepipeline.SchedulePersistence
	mu              sync.Mutex
	failures        int
	releaseAttempts map[string]int
}

func (s *failOnceReleaseSchedulePersistence) ReleaseSchedule(ctx context.Context, schedule runtimepipeline.Schedule) error {
	s.mu.Lock()
	s.releaseAttempts[schedule.TaskID]++
	if s.failures > 0 {
		s.failures--
		s.mu.Unlock()
		return errors.New("transient workflow timer claim release failure")
	}
	s.mu.Unlock()
	return s.SchedulePersistence.ReleaseSchedule(ctx, schedule)
}

func (s *failOnceReleaseSchedulePersistence) retriedRelease() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, attempts := range s.releaseAttempts {
		if attempts >= 2 {
			return true
		}
	}
	return false
}

func (s *failOnceReleaseSchedulePersistence) snapshotReleaseAttempts() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]int, len(s.releaseAttempts))
	for taskID, attempts := range s.releaseAttempts {
		out[taskID] = attempts
	}
	return out
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
			ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
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
			scheduleStore, ok := selected.events.(runtimepipeline.SchedulePersistence)
			if !ok {
				t.Fatalf("selected %s store does not implement SchedulePersistence", selected.name)
			}

			type fireResult struct {
				outcome runtimepipeline.WorkflowTimerFireOutcome
				err     error
			}
			results := make(chan fireResult, 2)
			attemptedSchedules := make(chan runtimepipeline.Schedule, 2)
			var coordinator *runtimepipeline.PipelineCoordinator
			scheduler := runtimepipeline.NewScheduler(func(schedule runtimepipeline.Schedule) {
				attemptedSchedules <- schedule
				outcome, err := coordinator.FireWorkflowTimer(ctx, schedule)
				results <- fireResult{outcome: outcome, err: err}
			})
			t.Cleanup(scheduler.Stop)
			coordinator = runtimepipeline.NewPipelineCoordinatorWithOptions(bus, selected.db, runtimepipeline.PipelineCoordinatorOptions{
				Module: gateRecoveryModule{source: source}, WorkflowStore: selected.workflowStore,
				TimerScheduler: scheduler, TimerScheduleStore: scheduleStore,
			})
			bus.SetInterceptors(coordinator)

			createdAt := time.Now().UTC()
			if err := selected.workflowStore.Upsert(ctx, runtimepipeline.WorkflowInstance{
				InstanceID: entityID, StorageRef: entityID, WorkflowName: "timer-proof", WorkflowVersion: "1",
				CurrentState: "waiting", EnteredStageAt: createdAt, CreatedAt: createdAt,
				Metadata: map[string]any{"run_id": runID},
			}); err != nil {
				t.Fatalf("seed workflow instance: %v", err)
			}
			if err := coordinator.ArmFlowInstanceInitialStageLifecycle(ctx, entityID); err != nil {
				t.Fatalf("ArmFlowInstanceInitialStageLifecycle: %v", err)
			}

			select {
			case result := <-results:
				if result.outcome != runtimepipeline.WorkflowTimerFireRetry ||
					!errors.Is(result.err, runtimebus.ErrPayloadValidation) ||
					!strings.Contains(result.err.Error(), errInjectedWorkflowTimerPublishFailure.Error()) {
					t.Fatalf("first fire outcome=%q err=%v, want retry/injected failure", result.outcome, result.err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for failed workflow timer publication")
			}
			firstSchedule := <-attemptedSchedules
			select {
			case <-validator.secondAttempt:
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for same-process workflow timer retry")
			}

			secondSchedule := <-attemptedSchedules
			ref, status := workflowTimerPersistedActivation(t, selected, runID, entityID)
			if status != "active" {
				t.Fatalf("workflow timer status during retried publication = %q, want active", status)
			}
			if firstSchedule.TaskID != secondSchedule.TaskID || firstSchedule.TimerID != secondSchedule.TimerID || !firstSchedule.At.Equal(secondSchedule.At) {
				t.Fatalf("retried schedule changed occurrence: first=%#v second=%#v", firstSchedule, secondSchedule)
			}
			if got := workflowTimerEventCount(t, selected, runID, runtimecontracts.WorkflowStageTimerInternalEvent); got != 0 {
				t.Fatalf("persisted events before retried publication commit = %d, want 0", got)
			}
			occurrence, ok := timeridentity.ParseWorkflowTimerOccurrenceTaskID(secondSchedule.TaskID)
			if !ok || occurrence.Activation != ref || !occurrence.DueAt.Equal(secondSchedule.At) {
				t.Fatalf("retried schedule occurrence=%#v ok=%v persisted_ref=%#v schedule_at=%s", occurrence, ok, ref, secondSchedule.At)
			}
			wantEventID := timeridentity.WorkflowTimerOccurrenceEventID(occurrence)
			close(validator.releaseSecond)

			select {
			case result := <-results:
				if result.outcome != runtimepipeline.WorkflowTimerFireCommitted || result.err != nil {
					t.Fatalf("retried fire outcome=%q err=%v, want committed", result.outcome, result.err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for committed workflow timer retry")
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
			ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
			source := semanticview.Wrap(workflowTimerServedLifecycleBundle(false))
			failingStore, failures := failNextWorkflowTimerPipelineReceipt(t, selected.events)
			bus, err := newScopedTestEventBus(t, failingStore, runtimebus.EventBusOptions{
				ContractBundle: source, PayloadValidator: strictWorkflowTimerPayloadValidator,
			}, runtimecontracts.WorkflowStageTimerInternalEvent)
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			scheduleStore, ok := selected.events.(runtimepipeline.SchedulePersistence)
			if !ok {
				t.Fatalf("selected %s store does not implement SchedulePersistence", selected.name)
			}

			fireErrors := make(chan error, 4)
			var coordinator *runtimepipeline.PipelineCoordinator
			scheduler := runtimepipeline.NewScheduler(func(schedule runtimepipeline.Schedule) {
				_, err := coordinator.FireWorkflowTimer(ctx, schedule)
				if err != nil {
					fireErrors <- err
				}
			})
			t.Cleanup(scheduler.Stop)
			coordinator = runtimepipeline.NewPipelineCoordinatorWithOptions(bus, selected.db, runtimepipeline.PipelineCoordinatorOptions{
				Module: gateRecoveryModule{source: source}, WorkflowStore: selected.workflowStore,
				TimerScheduler: scheduler, TimerScheduleStore: scheduleStore,
			})
			bus.SetInterceptors(coordinator)

			createdAt := time.Now().UTC()
			if err := selected.workflowStore.Upsert(ctx, runtimepipeline.WorkflowInstance{
				InstanceID: entityID, StorageRef: entityID, WorkflowName: "timer-proof", WorkflowVersion: "1",
				CurrentState: "waiting", EnteredStageAt: createdAt, CreatedAt: createdAt,
				Metadata: map[string]any{"run_id": runID},
			}); err != nil {
				t.Fatalf("seed workflow instance: %v", err)
			}
			if err := coordinator.ArmFlowInstanceInitialStageLifecycle(ctx, entityID); err != nil {
				t.Fatalf("ArmFlowInstanceInitialStageLifecycle: %v", err)
			}

			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) {
				select {
				case err := <-fireErrors:
					t.Fatalf("workflow timer callback: %v", err)
				default:
				}
				instance, found, err := selected.workflowStore.Load(ctx, entityID)
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
				var err error
				recovered, err = bus.SweepUndispatched(ctx, time.Hour, 10)
				if err != nil {
					t.Fatalf("SweepUndispatched: %v", err)
				}
				if recovered == 0 {
					time.Sleep(10 * time.Millisecond)
				}
			}
			if recovered != 1 {
				t.Fatalf("SweepUndispatched recovered=%d, want 1", recovered)
			}
			instance, found, err := selected.workflowStore.Load(ctx, entityID)
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

type failOncePostgresPipelineReceiptStore struct {
	*store.PostgresStore
	failures atomic.Int32
}

func (s *failOncePostgresPipelineReceiptStore) UpsertPipelineReceipt(ctx context.Context, eventID, status string, failure *runtimefailures.Envelope) error {
	if s.failures.CompareAndSwap(1, 0) {
		return errors.New("injected workflow timer pipeline receipt failure")
	}
	return s.PostgresStore.UpsertPipelineReceipt(ctx, eventID, status, failure)
}

type failOnceSQLitePipelineReceiptStore struct {
	*store.SQLiteRuntimeStore
	failures atomic.Int32
}

func (s *failOnceSQLitePipelineReceiptStore) UpsertPipelineReceipt(ctx context.Context, eventID, status string, failure *runtimefailures.Envelope) error {
	if s.failures.CompareAndSwap(1, 0) {
		return errors.New("injected workflow timer pipeline receipt failure")
	}
	return s.SQLiteRuntimeStore.UpsertPipelineReceipt(ctx, eventID, status, failure)
}

func failNextWorkflowTimerPipelineReceipt(t *testing.T, selected runtimebus.EventStore) (runtimebus.EventStore, *atomic.Int32) {
	t.Helper()
	switch typed := selected.(type) {
	case *store.PostgresStore:
		wrapped := &failOncePostgresPipelineReceiptStore{PostgresStore: typed}
		wrapped.failures.Store(1)
		return wrapped, &wrapped.failures
	case *store.SQLiteRuntimeStore:
		wrapped := &failOnceSQLitePipelineReceiptStore{SQLiteRuntimeStore: typed}
		wrapped.failures.Store(1)
		return wrapped, &wrapped.failures
	default:
		t.Fatalf("unsupported selected event store %T", selected)
		return nil, nil
	}
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
	query := `SELECT timer_name, status FROM timers WHERE run_id = ? AND entity_id = ? AND instr(timer_name, ?) = 1`
	if selected.postgres {
		query = `SELECT timer_name, status FROM timers WHERE run_id = $1::uuid AND entity_id = $2::uuid AND strpos(timer_name, $3) = 1`
	}
	var taskID, status string
	if err := selected.db.QueryRowContext(context.Background(), query, runID, entityID, timeridentity.WorkflowTimerActivationTaskPrefix()).Scan(&taskID, &status); err != nil {
		t.Fatalf("load persisted workflow timer activation: %v", err)
	}
	ref, ok := timeridentity.ParseWorkflowTimerActivationTaskID(taskID)
	if !ok {
		t.Fatalf("persisted workflow timer task id is invalid: %q", taskID)
	}
	return ref, status
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

func assertWorkflowTimerServedRows(t *testing.T, selected gateRecoveryStoreCase, runID, entityID, status string, want int) {
	t.Helper()
	query := `SELECT COUNT(*) FROM timers WHERE run_id = ? AND entity_id = ? AND instr(timer_name, ?) = 1 AND status = ?`
	if selected.postgres {
		query = `SELECT COUNT(*) FROM timers WHERE run_id = $1::uuid AND entity_id = $2::uuid AND strpos(timer_name, $3) = 1 AND status = $4`
	}
	var got int
	if err := selected.db.QueryRowContext(context.Background(), query, runID, entityID, timeridentity.WorkflowTimerActivationTaskPrefix(), status).Scan(&got); err != nil {
		t.Fatalf("count canonical workflow timers: %v", err)
	}
	if got != want {
		t.Fatalf("canonical workflow timers status=%s = %d, want %d", status, got, want)
	}
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
