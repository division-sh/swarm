package pipeline

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

func TestWorkflowTimerLifecycleOneShotExactCompletionOnBothStores(t *testing.T) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, ctx := tc.open(t)
			bus := &recordingPipelineBus{}
			pc, entityID, activation := seedWorkflowTimerOwnerActivation(t, store, ctx, bus, false)
			schedule := activation.schedule()
			occurrence := activation.occurrence()

			outcome, err := pc.FireWorkflowTimer(ctx, schedule)
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

			outcome, err = pc.FireWorkflowTimer(ctx, schedule)
			if err != nil || outcome != WorkflowTimerFireTerminal {
				t.Fatalf("retry outcome=%q err=%v, want terminal no-op", outcome, err)
			}
			if bus.publishedCount() != 1 {
				t.Fatalf("retry published events = %d, want 1 total", bus.publishedCount())
			}

			wrong := eventtest.RuntimeControl(
				uuid.NewString(), fired.Type(), fired.SourceAgent(), fired.TaskID(), fired.Payload(), 0,
				fired.RunID(), "", events.EventEnvelope{EntityID: entityID, FlowInstance: activation.FlowInstance}, fired.CreatedAt(),
			)
			if _, _, recognized, err := pc.workflowTimers.AuthorizeAcceptedEvent(ctx, wrong); err == nil || !recognized {
				t.Fatalf("wrong event id authorization recognized=%v err=%v, want recognized rejection", recognized, err)
			}
		})
	}
}

func TestWorkflowTimerLifecycleReactivatesOnlyOnLaterStageEntryOnBothStores(t *testing.T) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, ctx := tc.open(t)
			pc, entityID, first := seedWorkflowTimerOwnerActivation(t, store, ctx, &recordingPipelineBus{}, false)
			if outcome, err := pc.FireWorkflowTimer(ctx, first.schedule()); err != nil || outcome != WorkflowTimerFireCommitted {
				t.Fatalf("fire first activation outcome=%q err=%v", outcome, err)
			}

			unrelatedAt := canonicalWorkflowTimerTime(first.FireAt.Add(time.Minute))
			unrelated := workflowTimerCause{
				Kind: workflowTimerCauseEvent, EventID: uuid.NewString(), EventType: "work.noted", OccurredAt: unrelatedAt,
				FromState: "waiting", ToState: "waiting",
			}
			if err := store.RunPipelineMutation(ctx, func(txctx context.Context) error {
				return pc.workflowTimers.Reconcile(txctx, entityID, "waiting", "waiting", unrelated)
			}); err != nil {
				t.Fatalf("reconcile unrelated same-stage event: %v", err)
			}
			all := listWorkflowTimerOwnerActivations(t, store, ctx, entityID, false)
			if len(all) != 1 {
				t.Fatalf("activations after unrelated same-stage event = %d, want 1", len(all))
			}

			reentryAt := canonicalWorkflowTimerTime(unrelatedAt.Add(time.Minute))
			reentry := workflowTimerCause{
				Kind: workflowTimerCauseTransition, EventID: uuid.NewString(), EventType: "review.reopened", OccurredAt: reentryAt,
				TransitionID: "done_to_waiting", FromState: "done", ToState: "waiting",
			}
			activate := func() error {
				return store.RunPipelineMutation(ctx, func(txctx context.Context) error {
					return pc.workflowTimers.Reconcile(txctx, entityID, "done", "waiting", reentry)
				})
			}
			if err := activate(); err != nil {
				t.Fatalf("reactivate on later stage entry: %v", err)
			}
			if err := activate(); err != nil {
				t.Fatalf("retry later stage entry: %v", err)
			}
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

func TestWorkflowTimerLifecycleInitialAndEventEntrancesDoNotDuplicateOnBothStores(t *testing.T) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, ctx := tc.open(t)
			entityID := uuid.NewString()
			createdAt := canonicalWorkflowTimerTime(time.Date(2026, time.July, 1, 12, 0, 0, 0, time.UTC))
			if err := store.Upsert(ctx, WorkflowInstance{
				InstanceID: entityID, StorageRef: entityID, WorkflowName: "workflow-timer-owner-test",
				WorkflowVersion: "1.0.0", CurrentState: "waiting", EnteredStageAt: createdAt,
				CreatedAt: createdAt, Metadata: map[string]any{"run_id": runtimecorrelation.RunIDFromContext(ctx)},
			}); err != nil {
				t.Fatalf("seed workflow instance: %v", err)
			}
			bundle := workflowTimerOwnerBundle(false)
			bundle.Semantics.Timers[0].Stage = ""
			bundle.Semantics.Timers[0].StageOwned = false
			bundle.Semantics.Timers[0].StartOn = "event:work.created"
			pc := NewPipelineCoordinatorWithOptions(&recordingPipelineBus{}, store.db, PipelineCoordinatorOptions{
				Module: &pipelineFixtureWorkflowModule{source: semanticview.Wrap(bundle)}, WorkflowStore: store,
			})
			eventID := uuid.NewString()
			if err := store.RunPipelineMutation(ctx, func(txctx context.Context) error {
				initial, err := workflowTimerInitialCause(WorkflowInstance{CreatedAt: createdAt}, "waiting")
				if err != nil {
					return err
				}
				if err := pc.workflowTimers.Reconcile(txctx, entityID, "", "waiting", initial); err != nil {
					return err
				}
				return pc.workflowTimers.Reconcile(txctx, entityID, "waiting", "waiting", workflowTimerCause{
					Kind: workflowTimerCauseEvent, EventID: eventID, EventType: "work.created", OccurredAt: createdAt,
					FromState: "waiting", ToState: "waiting",
				})
			}); err != nil {
				t.Fatalf("reconcile initial and event entrances: %v", err)
			}
			activations := listWorkflowTimerOwnerActivations(t, store, ctx, entityID, true)
			if len(activations) != 1 {
				t.Fatalf("active event timer activations = %d, want 1", len(activations))
			}
			want := workflowTimerActivationForCause(
				runtimecorrelation.RunIDFromContext(ctx), entityID, entityID, bundle.Semantics.Timers[0],
				activations[0].Ref.Generation,
				workflowTimerCause{Kind: workflowTimerCauseEvent, EventID: eventID, EventType: "work.created", OccurredAt: createdAt, FromState: "waiting", ToState: "waiting"},
				time.Hour,
			)
			if activations[0].Ref != want.Ref {
				t.Fatalf("event activation ref = %#v, want %#v", activations[0].Ref, want.Ref)
			}
		})
	}
}

func TestWorkflowTimerLifecycleRecurringAdvancesPersistedCoordinateOnBothStores(t *testing.T) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, ctx := tc.open(t)
			bus := &recordingPipelineBus{}
			pc, _, activation := seedWorkflowTimerOwnerActivation(t, store, ctx, bus, true)
			firstSchedule := activation.schedule()
			firstOccurrence := activation.occurrence()

			outcome, err := pc.FireWorkflowTimer(ctx, firstSchedule)
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

			outcome, err = pc.FireWorkflowTimer(ctx, firstSchedule)
			if err != nil || outcome != WorkflowTimerFireTerminal || bus.publishedCount() != 1 {
				t.Fatalf("same-occurrence retry outcome=%q publishes=%d err=%v", outcome, bus.publishedCount(), err)
			}

			secondSchedule := next.schedule()
			secondOccurrence := next.occurrence()
			outcome, err = pc.FireWorkflowTimer(ctx, secondSchedule)
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

			restartedScheduler := NewScheduler()
			t.Cleanup(restartedScheduler.Stop)
			restarted := NewPipelineCoordinatorWithOptions(bus, store.db, PipelineCoordinatorOptions{
				Module:         &pipelineFixtureWorkflowModule{source: semanticview.Wrap(workflowTimerOwnerBundle(true))},
				WorkflowStore:  store,
				TimerScheduler: restartedScheduler,
			})
			if err := restarted.RestoreWorkflowTimers(ctx); err != nil {
				t.Fatalf("RestoreWorkflowTimers: %v", err)
			}
			restartedScheduler.mu.Lock()
			registered := len(restartedScheduler.tasks)
			restartedScheduler.mu.Unlock()
			if registered != 1 {
				t.Fatalf("restored scheduler tasks = %d, want 1", registered)
			}
		})
	}
}

func TestWorkflowTimerLifecycleRollbackAndCancellationOnBothStores(t *testing.T) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, ctx := tc.open(t)
			publishFailure := errors.New("publish failed")
			bus := &recordingPipelineBus{publishErr: publishFailure}
			pc, entityID, activation := seedWorkflowTimerOwnerActivation(t, store, ctx, bus, false)

			outcome, err := pc.FireWorkflowTimer(ctx, activation.schedule())
			if !errors.Is(err, publishFailure) || outcome != WorkflowTimerFireRetry {
				t.Fatalf("failed fire outcome=%q err=%v, want retry publish failure", outcome, err)
			}
			persisted := loadWorkflowTimerOwnerActivation(t, store, ctx, activation.Ref.ActivationID)
			if persisted.Status != workflowTimerStatusActive || !persisted.FireAt.Equal(activation.FireAt) {
				t.Fatalf("rolled-back activation = %#v, want unchanged active row", persisted)
			}

			bus.publishErr = nil
			transitionAt := canonicalWorkflowTimerTime(time.Now())
			err = store.RunPipelineMutation(ctx, func(txctx context.Context) error {
				return pc.workflowTimers.Reconcile(txctx, entityID, "waiting", "done", workflowTimerCause{
					Kind: workflowTimerCauseTransition, EventID: uuid.NewString(), EventType: "work.completed",
					OccurredAt: transitionAt, TransitionID: uuid.NewString(), FromState: "waiting", ToState: "done",
				})
			})
			if err != nil {
				t.Fatalf("cancel timer on transition: %v", err)
			}
			persisted = loadWorkflowTimerOwnerActivation(t, store, ctx, activation.Ref.ActivationID)
			if persisted.Status != workflowTimerStatusCancelled {
				t.Fatalf("cancelled activation status = %q, want cancelled", persisted.Status)
			}

			restartedScheduler := NewScheduler()
			t.Cleanup(restartedScheduler.Stop)
			restarted := NewPipelineCoordinatorWithOptions(bus, store.db, PipelineCoordinatorOptions{
				Module:         &pipelineFixtureWorkflowModule{source: semanticview.Wrap(workflowTimerOwnerBundle(false))},
				WorkflowStore:  store,
				TimerScheduler: restartedScheduler,
			})
			if err := restarted.RestoreWorkflowTimers(ctx); err != nil {
				t.Fatalf("restore after cancel: %v", err)
			}
			restartedScheduler.mu.Lock()
			registered := len(restartedScheduler.tasks)
			restartedScheduler.mu.Unlock()
			if registered != 0 {
				t.Fatalf("restored cancelled scheduler tasks = %d, want 0", registered)
			}
		})
	}
}

func TestWorkflowTimerLifecycleCommitOrdersConvergeOnBothStores(t *testing.T) {
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
				store, ctx := tc.open(t)
				bus := &recordingPipelineBus{}
				pc, entityID, activation := seedWorkflowTimerOwnerActivation(t, store, ctx, bus, false)
				unrelatedApplied := false
				for _, step := range test.steps {
					switch step {
					case "fire":
						outcome, err := pc.FireWorkflowTimer(ctx, activation.schedule())
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
						if err := store.RunPipelineMutation(ctx, func(txctx context.Context) error {
							_, _, err := store.cancelWorkflowTimerActivation(txctx, activation.Ref)
							return err
						}); err != nil {
							t.Fatalf("cancel: %v", err)
						}
					case "unrelated":
						if err := store.MutateE(ctx, entityID, func(instance *WorkflowInstance) error {
							if instance.Metadata == nil {
								instance.Metadata = map[string]any{}
							}
							instance.Metadata["unrelated_timer_order_proof"] = test.name
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
					instance, found, err := store.Load(ctx, entityID)
					if err != nil || !found || instance.Metadata["unrelated_timer_order_proof"] != test.name {
						t.Fatalf("unrelated mutation found=%v value=%#v err=%v", found, instance.Metadata["unrelated_timer_order_proof"], err)
					}
				}
			})
		}
	}
}

func TestWorkflowTimerLifecycleRejectsMissingAndMismatchedCallbacksOnBothStores(t *testing.T) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, ctx := tc.open(t)
			bus := &recordingPipelineBus{}
			pc, _, activation := seedWorkflowTimerOwnerActivation(t, store, ctx, bus, false)

			missingRef := activation.Ref
			missingRef.ActivationID = uuid.NewString()
			missingOccurrence := timeridentity.WorkflowTimerOccurrenceRef{Activation: missingRef, DueAt: activation.FireAt}
			missing := activation.schedule()
			missing.TimerID = missingRef.ActivationID
			missing.TaskID = missingOccurrence.TaskID()
			outcome, err := pc.FireWorkflowTimer(ctx, missing)
			if err != nil || outcome != WorkflowTimerFireTerminal {
				t.Fatalf("missing callback outcome=%q err=%v, want terminal nil", outcome, err)
			}

			mismatchedRef := activation.Ref
			mismatchedRef.Declaration = "different.timer"
			mismatchedOccurrence := timeridentity.WorkflowTimerOccurrenceRef{Activation: mismatchedRef, DueAt: activation.FireAt}
			mismatched := activation.schedule()
			mismatched.TaskID = mismatchedOccurrence.TaskID()
			outcome, err = pc.FireWorkflowTimer(ctx, mismatched)
			if err == nil || outcome != WorkflowTimerFireTerminal {
				t.Fatalf("mismatched callback outcome=%q err=%v, want terminal error", outcome, err)
			}

			wrongTuple := activation.schedule()
			wrongTuple.AgentID = "different-owner"
			outcome, err = pc.FireWorkflowTimer(ctx, wrongTuple)
			if err == nil || outcome != WorkflowTimerFireTerminal {
				t.Fatalf("wrong-tuple callback outcome=%q err=%v, want terminal error", outcome, err)
			}
			persisted := loadWorkflowTimerOwnerActivation(t, store, ctx, activation.Ref.ActivationID)
			if persisted.Status != workflowTimerStatusActive || bus.publishedCount() != 0 {
				t.Fatalf("activation after refused callbacks status=%q publishes=%d, want active/0", persisted.Status, bus.publishedCount())
			}

			outcome, err = pc.FireWorkflowTimer(ctx, activation.schedule())
			if err != nil || outcome != WorkflowTimerFireCommitted {
				t.Fatalf("canonical callback outcome=%q err=%v, want committed", outcome, err)
			}
			outcome, err = pc.FireWorkflowTimer(ctx, activation.schedule())
			if err != nil || outcome != WorkflowTimerFireTerminal {
				t.Fatalf("already-fired callback outcome=%q err=%v, want terminal nil", outcome, err)
			}
		})
	}
}

func TestWorkflowTimerLifecycleIsolatesStaleActivationAcrossCancelAndReentryOnBothStores(t *testing.T) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, ctx := tc.open(t)
			bus := &recordingPipelineBus{}
			pc, entityID, first := seedWorkflowTimerOwnerActivation(t, store, ctx, bus, false)
			cancelAt := canonicalWorkflowTimerTime(time.Now().Add(time.Minute))
			if err := store.RunPipelineMutation(ctx, func(txctx context.Context) error {
				return pc.workflowTimers.Reconcile(txctx, entityID, "waiting", "done", workflowTimerCause{
					Kind: workflowTimerCauseTransition, EventID: uuid.NewString(), EventType: "work.completed",
					OccurredAt: cancelAt, TransitionID: "waiting_to_done", FromState: "waiting", ToState: "done",
				})
			}); err != nil {
				t.Fatalf("cancel first activation: %v", err)
			}
			reenterAt := canonicalWorkflowTimerTime(cancelAt.Add(time.Minute))
			if err := store.RunPipelineMutation(ctx, func(txctx context.Context) error {
				return pc.workflowTimers.Reconcile(txctx, entityID, "done", "waiting", workflowTimerCause{
					Kind: workflowTimerCauseTransition, EventID: uuid.NewString(), EventType: "work.reopened",
					OccurredAt: reenterAt, TransitionID: "done_to_waiting", FromState: "done", ToState: "waiting",
				})
			}); err != nil {
				t.Fatalf("activate replacement timer: %v", err)
			}
			active := listWorkflowTimerOwnerActivations(t, store, ctx, entityID, true)
			if len(active) != 1 || active[0].Ref.ActivationID == first.Ref.ActivationID {
				t.Fatalf("replacement activation = %#v, want one distinct active row", active)
			}
			second := active[0]

			outcome, err := pc.FireWorkflowTimer(ctx, first.schedule())
			if err != nil || outcome != WorkflowTimerFireTerminal || bus.publishedCount() != 0 {
				t.Fatalf("stale A callback outcome=%q publishes=%d err=%v, want terminal/0", outcome, bus.publishedCount(), err)
			}
			outcome, err = pc.FireWorkflowTimer(ctx, second.schedule())
			if err != nil || outcome != WorkflowTimerFireCommitted || bus.publishedCount() != 1 {
				t.Fatalf("replacement B callback outcome=%q publishes=%d err=%v, want committed/1", outcome, bus.publishedCount(), err)
			}
			if got, want := bus.publishedEvent(0).ID(), timeridentity.WorkflowTimerOccurrenceEventID(second.occurrence()); got != want {
				t.Fatalf("replacement event id = %q, want %q", got, want)
			}
		})
	}
}

func TestWorkflowTimerLifecycleActivationRollbackDoesNotRegisterOnBothStores(t *testing.T) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, ctx := tc.open(t)
			entityID := uuid.NewString()
			createdAt := canonicalWorkflowTimerTime(time.Date(2026, time.July, 1, 12, 0, 0, 0, time.UTC))
			if err := store.Upsert(ctx, WorkflowInstance{
				InstanceID: entityID, StorageRef: entityID, WorkflowName: "workflow-timer-owner-test",
				WorkflowVersion: "1.0.0", CurrentState: "waiting", EnteredStageAt: createdAt,
				CreatedAt: createdAt, Metadata: map[string]any{"run_id": runtimecorrelation.RunIDFromContext(ctx)},
			}); err != nil {
				t.Fatalf("seed workflow instance: %v", err)
			}
			scheduler := NewScheduler()
			t.Cleanup(scheduler.Stop)
			pc := NewPipelineCoordinatorWithOptions(&recordingPipelineBus{}, store.db, PipelineCoordinatorOptions{
				Module:         &pipelineFixtureWorkflowModule{source: semanticview.Wrap(workflowTimerOwnerBundle(false))},
				WorkflowStore:  store,
				TimerScheduler: scheduler,
			})
			rollback := errors.New("rollback activation")
			err := store.RunPipelineMutation(ctx, func(txctx context.Context) error {
				if err := pc.workflowTimers.Reconcile(txctx, entityID, "", "waiting", workflowTimerCause{
					Kind: workflowTimerCauseInitial, OccurredAt: createdAt, ToState: "waiting",
				}); err != nil {
					return err
				}
				return rollback
			})
			if !errors.Is(err, rollback) {
				t.Fatalf("activation mutation error = %v, want rollback", err)
			}
			if activations := listWorkflowTimerOwnerActivations(t, store, ctx, entityID, false); len(activations) != 0 {
				t.Fatalf("rolled-back workflow timer activations = %#v, want none", activations)
			}
			scheduler.mu.Lock()
			registered := len(scheduler.tasks)
			scheduler.mu.Unlock()
			if registered != 0 {
				t.Fatalf("scheduler tasks after activation rollback = %d, want 0", registered)
			}
		})
	}
}

func TestWorkflowTimerLifecycleTargetedRegistrationRecoveryOnBothStores(t *testing.T) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name+"/claim_retry", func(t *testing.T) {
			store, ctx := tc.open(t)
			claims := &recordingSchedulePersistence{
				claimFailures: 2,
				claimErr:      errors.New("transient claim failure"),
			}
			scheduler := NewScheduler()
			t.Cleanup(scheduler.Stop)
			_, activation := seedWorkflowTimerOwnerRegisteredActivation(t, store, ctx, claims, scheduler)
			if claims.claimCalls != 3 {
				t.Fatalf("claim attempts = %d, want 3", claims.claimCalls)
			}
			scheduler.mu.Lock()
			registered := len(scheduler.tasks)
			scheduler.mu.Unlock()
			if registered != 1 {
				t.Fatalf("registered tasks after claim recovery = %d, want 1", registered)
			}
			persisted := loadWorkflowTimerOwnerActivation(t, store, ctx, activation.Ref.ActivationID)
			if persisted.Status != workflowTimerStatusActive {
				t.Fatalf("activation after claim recovery = %q, want active", persisted.Status)
			}
		})

		t.Run(tc.name+"/register_retry", func(t *testing.T) {
			store, ctx := tc.open(t)
			claims := &recordingSchedulePersistence{}
			stopped := NewScheduler()
			stopped.Stop()
			pc, activation := seedWorkflowTimerOwnerRegisteredActivation(t, store, ctx, claims, stopped)
			if claims.claimCalls != 3 {
				t.Fatalf("claim attempts against stopped scheduler = %d, want 3", claims.claimCalls)
			}
			persisted := loadWorkflowTimerOwnerActivation(t, store, ctx, activation.Ref.ActivationID)
			if persisted.Status != workflowTimerStatusActive {
				t.Fatalf("activation after register failure = %q, want active", persisted.Status)
			}

			replacement := NewScheduler()
			t.Cleanup(replacement.Stop)
			pc.timerScheduler = replacement
			if err := pc.workflowTimers.EnsureRegistered(ctx, activation.Ref); err != nil {
				t.Fatalf("targeted same-process registration recovery: %v", err)
			}
			replacement.mu.Lock()
			registered := len(replacement.tasks)
			replacement.mu.Unlock()
			if registered != 1 {
				t.Fatalf("registered tasks after targeted recovery = %d, want 1", registered)
			}
		})
	}
}

func TestWorkflowTimerLifecycleSchedulerRetryPreservesOccurrenceOnBothStores(t *testing.T) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, ctx := tc.open(t)
			publishFailure := errors.New("transient publish failure")
			bus := &failOnceWorkflowTimerBus{recordingPipelineBus: &recordingPipelineBus{}, err: publishFailure}
			bus.failures.Store(1)
			pc, _, activation := seedWorkflowTimerOwnerActivationWithDelay(t, store, ctx, bus.recordingPipelineBus, false, "1ms")
			type fireResult struct {
				outcome WorkflowTimerFireOutcome
				err     error
			}
			results := make(chan fireResult, 2)
			scheduler := NewScheduler(func(schedule Schedule) {
				outcome, err := pc.FireWorkflowTimer(ctx, schedule)
				results <- fireResult{outcome: outcome, err: err}
			})
			t.Cleanup(scheduler.Stop)
			pc.bus = bus
			pc.timerScheduler = scheduler
			if err := scheduler.Register(activation.schedule()); err != nil {
				t.Fatalf("register workflow timer: %v", err)
			}

			for index, want := range []WorkflowTimerFireOutcome{WorkflowTimerFireRetry, WorkflowTimerFireCommitted} {
				select {
				case result := <-results:
					if result.outcome != want {
						t.Fatalf("fire %d outcome = %q, want %q (err=%v)", index+1, result.outcome, want, result.err)
					}
					if index == 0 && !errors.Is(result.err, publishFailure) {
						t.Fatalf("first fire error = %v, want transient publish failure", result.err)
					}
					if index == 1 && result.err != nil {
						t.Fatalf("second fire error = %v, want nil", result.err)
					}
				case <-time.After(5 * time.Second):
					t.Fatalf("timed out waiting for fire %d", index+1)
				}
			}
			if bus.publishedCount() != 1 {
				t.Fatalf("published events after retry = %d, want 1", bus.publishedCount())
			}
			wantEventID := timeridentity.WorkflowTimerOccurrenceEventID(activation.occurrence())
			if got := bus.publishedEvent(0).ID(); got != wantEventID {
				t.Fatalf("retried occurrence event id = %q, want %q", got, wantEventID)
			}
			persisted := loadWorkflowTimerOwnerActivation(t, store, ctx, activation.Ref.ActivationID)
			if persisted.Status != workflowTimerStatusFired {
				t.Fatalf("retried activation status = %q, want fired", persisted.Status)
			}
		})
	}
}

type failOnceWorkflowTimerBus struct {
	*recordingPipelineBus
	failures atomic.Int32
	err      error
}

func (b *failOnceWorkflowTimerBus) PublishInMutation(ctx context.Context, evt events.Event) error {
	if b.failures.CompareAndSwap(1, 0) {
		return b.err
	}
	return b.recordingPipelineBus.PublishInMutation(ctx, evt)
}

func TestWorkflowTimerLifecyclePostgresReleasesClaimOnlyAfterOuterCommit(t *testing.T) {
	for _, tc := range workflowJoinStoreCases() {
		if tc.name != "postgres" {
			continue
		}
		store, ctx := tc.open(t)
		bus := &recordingPipelineBus{}
		pc, _, activation := seedWorkflowTimerOwnerActivation(t, store, ctx, bus, false)
		claims := &recordingSchedulePersistence{}
		pc.timerScheduleStore = claims

		tx, err := store.db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("BeginTx: %v", err)
		}
		committed := false
		t.Cleanup(func() {
			if !committed {
				_ = tx.Rollback()
			}
		})
		actions := make([]func(), 0, 1)
		txctx := withPipelinePostCommitActions(WithPipelineSQLTxContext(ctx, tx), &actions)
		txctx, err = runtimeauthoractivity.Begin(txctx, tx, runtimeauthoractivity.DialectPostgres)
		if err != nil {
			t.Fatalf("begin author activity: %v", err)
		}

		outcome, err := pc.FireWorkflowTimer(txctx, activation.schedule())
		if err != nil || outcome != WorkflowTimerFireCommitted {
			t.Fatalf("FireWorkflowTimer outcome=%q err=%v", outcome, err)
		}
		if len(claims.releases) != 0 {
			t.Fatalf("claim releases before outer commit = %d, want 0", len(claims.releases))
		}
		if len(actions) != 1 {
			t.Fatalf("post-commit actions = %d, want 1", len(actions))
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit: %v", err)
		}
		committed = true
		flushPipelinePostCommitActions(actions)
		if len(claims.releases) != 1 {
			t.Fatalf("claim releases after outer commit = %d, want 1", len(claims.releases))
		}
		persisted := loadWorkflowTimerOwnerActivation(t, store, ctx, activation.Ref.ActivationID)
		if persisted.Status != workflowTimerStatusFired {
			t.Fatalf("post-commit timer status = %q, want fired", persisted.Status)
		}
	}
}

func TestWorkflowTimerLifecycleRestoreRejectsObsoleteRowsOnBothStores(t *testing.T) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, ctx := tc.open(t)
			runID := runtimecorrelation.RunIDFromContext(ctx)
			timerID := uuid.NewString()
			entityID := uuid.NewString()
			if store.isSQLite() {
				_, err := store.db.ExecContext(ctx, `
					INSERT INTO timers (
						timer_id, run_id, timer_name, entity_id, flow_instance, fire_event, fire_payload,
						fire_at, recurring, owner_node, task_type, status, created_at
					) VALUES (?, ?, 'obsolete', ?, 'obsolete', 'timer.obsolete', '{}', ?, false,
					          'workflow_instance_store', 'timer', 'active', ?)
				`, timerID, runID, entityID, time.Now().UTC().Add(time.Hour), time.Now().UTC())
				if err != nil {
					t.Fatalf("insert obsolete SQLite timer row: %v", err)
				}
			} else {
				_, err := store.db.ExecContext(ctx, `
					INSERT INTO timers (
						timer_id, run_id, timer_name, entity_id, flow_instance, fire_event, fire_payload,
						fire_at, recurring, owner_node, task_type, status, created_at
					) VALUES ($1::uuid, $2::uuid, 'obsolete', $3::uuid, 'obsolete', 'timer.obsolete', '{}'::jsonb, $4, false,
					          'workflow_instance_store', 'timer', 'active', $5)
				`, timerID, runID, entityID, time.Now().UTC().Add(time.Hour), time.Now().UTC())
				if err != nil {
					t.Fatalf("insert obsolete PostgreSQL timer row: %v", err)
				}
			}
			pc := NewPipelineCoordinatorWithOptions(&recordingPipelineBus{}, store.db, PipelineCoordinatorOptions{
				Module:        &pipelineFixtureWorkflowModule{source: semanticview.Wrap(workflowTimerOwnerBundle(false))},
				WorkflowStore: store,
			})
			err := pc.RestoreWorkflowTimers(ctx)
			if err == nil || !strings.Contains(err.Error(), "recreate the database") {
				t.Fatalf("RestoreWorkflowTimers error = %v, want unsupported-database refusal", err)
			}
		})
	}
}

func seedWorkflowTimerOwnerActivation(
	t *testing.T,
	store *WorkflowInstanceStore,
	ctx context.Context,
	bus *recordingPipelineBus,
	recurring bool,
) (*PipelineCoordinator, string, WorkflowTimerActivation) {
	t.Helper()
	return seedWorkflowTimerOwnerActivationWithDelay(t, store, ctx, bus, recurring, "1h")
}

func seedWorkflowTimerOwnerActivationWithDelay(
	t *testing.T,
	store *WorkflowInstanceStore,
	ctx context.Context,
	bus *recordingPipelineBus,
	recurring bool,
	delay string,
) (*PipelineCoordinator, string, WorkflowTimerActivation) {
	t.Helper()
	entityID := uuid.NewString()
	createdAt := canonicalWorkflowTimerTime(time.Now())
	if err := store.Upsert(ctx, WorkflowInstance{
		InstanceID: entityID, StorageRef: entityID, WorkflowName: "workflow-timer-owner-test",
		WorkflowVersion: "1.0.0", CurrentState: "waiting", EnteredStageAt: createdAt,
		CreatedAt: createdAt, Metadata: map[string]any{"run_id": runtimecorrelation.RunIDFromContext(ctx)},
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}
	pc := NewPipelineCoordinatorWithOptions(bus, store.db, PipelineCoordinatorOptions{
		Module:        &pipelineFixtureWorkflowModule{source: semanticview.Wrap(workflowTimerOwnerBundleWithDelay(recurring, delay))},
		WorkflowStore: store,
	})
	if err := store.RunPipelineMutation(ctx, func(txctx context.Context) error {
		return pc.workflowTimers.Reconcile(txctx, entityID, "", "waiting", workflowTimerCause{
			Kind: workflowTimerCauseInitial, OccurredAt: createdAt, ToState: "waiting",
		})
	}); err != nil {
		t.Fatalf("activate workflow timer: %v", err)
	}
	activations, err := store.listWorkflowTimerActivations(ctx, runtimecorrelation.RunIDFromContext(ctx), entityID, true)
	if err != nil {
		t.Fatalf("list workflow timer activations: %v", err)
	}
	if len(activations) != 1 {
		t.Fatalf("active workflow timers = %d, want 1: %#v", len(activations), activations)
	}
	return pc, entityID, activations[0]
}

func seedWorkflowTimerOwnerRegisteredActivation(
	t *testing.T,
	store *WorkflowInstanceStore,
	ctx context.Context,
	claims SchedulePersistence,
	scheduler *Scheduler,
) (*PipelineCoordinator, WorkflowTimerActivation) {
	t.Helper()
	entityID := uuid.NewString()
	createdAt := canonicalWorkflowTimerTime(time.Now())
	if err := store.Upsert(ctx, WorkflowInstance{
		InstanceID: entityID, StorageRef: entityID, WorkflowName: "workflow-timer-owner-test",
		WorkflowVersion: "1.0.0", CurrentState: "waiting", EnteredStageAt: createdAt,
		CreatedAt: createdAt, Metadata: map[string]any{"run_id": runtimecorrelation.RunIDFromContext(ctx)},
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}
	pc := NewPipelineCoordinatorWithOptions(&recordingPipelineBus{}, store.db, PipelineCoordinatorOptions{
		Module:             &pipelineFixtureWorkflowModule{source: semanticview.Wrap(workflowTimerOwnerBundle(false))},
		WorkflowStore:      store,
		TimerScheduler:     scheduler,
		TimerScheduleStore: claims,
	})
	if err := store.RunPipelineMutation(ctx, func(txctx context.Context) error {
		return pc.workflowTimers.Reconcile(txctx, entityID, "", "waiting", workflowTimerCause{
			Kind: workflowTimerCauseInitial, OccurredAt: createdAt, ToState: "waiting",
		})
	}); err != nil {
		t.Fatalf("activate workflow timer: %v", err)
	}
	activations := listWorkflowTimerOwnerActivations(t, store, ctx, entityID, true)
	if len(activations) != 1 {
		t.Fatalf("active workflow timers = %d, want 1: %#v", len(activations), activations)
	}
	return pc, activations[0]
}

func loadWorkflowTimerOwnerActivation(t *testing.T, store *WorkflowInstanceStore, ctx context.Context, activationID string) WorkflowTimerActivation {
	t.Helper()
	activation, found, err := store.loadWorkflowTimerActivation(ctx, activationID, false)
	if err != nil || !found {
		t.Fatalf("load workflow timer activation found=%v err=%v", found, err)
	}
	return activation
}

func listWorkflowTimerOwnerActivations(t *testing.T, store *WorkflowInstanceStore, ctx context.Context, entityID string, activeOnly bool) []WorkflowTimerActivation {
	t.Helper()
	activations, err := store.listWorkflowTimerActivations(ctx, runtimecorrelation.RunIDFromContext(ctx), entityID, activeOnly)
	if err != nil {
		t.Fatalf("list workflow timer activations: %v", err)
	}
	return activations
}

func workflowTimerOwnerBundle(recurring bool) *runtimecontracts.WorkflowContractBundle {
	return workflowTimerOwnerBundleWithDelay(recurring, "1h")
}

func workflowTimerOwnerBundleWithDelay(recurring bool, delay string) *runtimecontracts.WorkflowContractBundle {
	return &runtimecontracts.WorkflowContractBundle{Semantics: runtimecontracts.WorkflowSemanticView{
		Name: "workflow-timer-owner-test", Version: "1.0.0", InitialStage: "waiting",
		Timers: []runtimecontracts.WorkflowTimerContract{{
			ID: "waiting.timeout", Stage: "waiting", StageOwned: true, Owner: "runtime",
			Event: "timer.timeout", StartOn: "state:waiting", Delay: delay, Recurring: recurring,
		}},
	}}
}
