package pipeline

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

func TestWorkflowTimerLifecycleOneShotExactCompletionOnBothStores(t *testing.T) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, ctx := tc.open(t)
			bus := &recordingPipelineBus{}
			pc, entityID, activation := seedWorkflowTimerOwnerActivation(t, store, ctx, bus, false)
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
				fired.RunID(), "", events.EventEnvelope{EntityID: entityID, FlowInstance: activation.FlowInstance}, fired.CreatedAt(),
			)
			if _, _, recognized, err := pc.workflowTimers.AuthorizeAcceptedEvent(ctx, wrong); err == nil || !recognized {
				t.Fatalf("wrong event id authorization recognized=%v err=%v, want recognized rejection", recognized, err)
			}
		})
	}
}

func TestAcceptedWorkflowTimerEventRoutingMatrixOnBothStores(t *testing.T) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, ctx := tc.open(t)
			bus := &recordingPipelineBus{}
			pc, entityID, activation := seedWorkflowTimerOwnerActivation(t, store, ctx, bus, false)
			if outcome, err := fireWorkflowTimerTestWakeup(ctx, pc, activation); err != nil || outcome != WorkflowTimerFireCommitted {
				t.Fatalf("fire canonical occurrence outcome=%q err=%v", outcome, err)
			}
			canonical := bus.publishedEvent(0)

			tests := []struct {
				name           string
				event          events.Event
				wantRecognized bool
				wantErr        bool
			}{
				{name: "exact producer and canonical occurrence", event: canonical, wantRecognized: true},
				{
					name: "exact producer with malformed occurrence",
					event: eventtest.RuntimeControl(
						uuid.NewString(), canonical.Type(), "runtime.workflow_timer", "malformed",
						canonical.Payload(), 0, canonical.RunID(), "", events.EventEnvelope{
							EntityID: entityID, FlowInstance: activation.FlowInstance,
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
							EntityID: entityID, FlowInstance: activation.FlowInstance,
						}, canonical.CreatedAt(),
					),
				},
				{
					name: "ordinary task from another producer",
					event: eventtest.RuntimeControl(
						uuid.NewString(), canonical.Type(), "runtime", "ordinary-task",
						canonical.Payload(), 0, canonical.RunID(), "", events.EventEnvelope{
							EntityID: entityID, FlowInstance: activation.FlowInstance,
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
								EntityID: entityID, FlowInstance: activation.FlowInstance,
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

func TestWorkflowTimerActiveProjectionRequiresSchedulerOnBothStores(t *testing.T) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, ctx := tc.open(t)
			pc, _, activation := seedWorkflowTimerOwnerActivation(t, store, ctx, &recordingPipelineBus{}, false)

			if err := pc.workflowTimers.ReconcileWakeup(ctx, activation.Ref); !errors.Is(err, errWorkflowTimerSchedulerRequired) {
				t.Fatalf("ReconcileWakeup error = %v, want scheduler-required failure", err)
			}
			if err := pc.RestoreWorkflowTimers(ctx); !errors.Is(err, errWorkflowTimerSchedulerRequired) {
				t.Fatalf("RestoreWorkflowTimers error = %v, want scheduler-required failure", err)
			}
		})
	}
}

type settledPipelineTestObligationOwner struct {
	unavailablePipelineTestObligationOwner
}

func (settledPipelineTestObligationOwner) SummarizeRun(context.Context, string) (runtimepipelineobligation.RunSummary, error) {
	return runtimepipelineobligation.RunSummary{}, nil
}

func TestStandingRestartAbandonUsesTimerObligationSnapshotOnBothStores(t *testing.T) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, ctx := tc.open(t)
			_, _, activation := seedWorkflowTimerOwnerActivation(t, store, ctx, &recordingPipelineBus{}, false)
			store.ConfigureDeliveryLifecycleStore(newPipelineTestDeliveryOwnerForDB(t, store.db))
			store.ConfigurePipelineObligationStore(settledPipelineTestObligationOwner{})
			if store.isSQLite() {
				if _, err := store.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS agent_sessions (run_id TEXT, status TEXT)`); err != nil {
					t.Fatalf("create SQLite standing session proof table: %v", err)
				}
			}

			tx, err := store.db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatalf("begin standing timer proof: %v", err)
			}
			observedAt := canonicalWorkflowTimerTime(activation.FireAt.Add(time.Minute))
			live, err := store.standingRunHasLiveWorkTx(ctx, tx, activation.RunID, observedAt)
			if err != nil || !live {
				t.Fatalf("standing live work before cancellation = %v err=%v, want true", live, err)
			}

			txctx := WithPipelineSQLTxContext(ctx, tx)
			if _, changed, err := store.cancelWorkflowTimerActivation(txctx, activation.Ref); err != nil || !changed {
				t.Fatalf("cancel workflow timer in standing transaction changed=%v err=%v", changed, err)
			}
			live, err = store.standingRunHasLiveWorkTx(ctx, tx, activation.RunID, observedAt)
			if err != nil || live {
				t.Fatalf("standing live work after in-transaction cancellation = %v err=%v, want false", live, err)
			}

			if err := tx.Rollback(); err != nil {
				t.Fatalf("rollback standing timer proof: %v", err)
			}
			verifyTx, err := store.db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatalf("begin standing timer rollback verification: %v", err)
			}
			defer func() { _ = verifyTx.Rollback() }()
			live, err = store.standingRunHasLiveWorkTx(ctx, verifyTx, activation.RunID, observedAt)
			if err != nil || !live {
				t.Fatalf("standing live work after rollback = %v err=%v, want true", live, err)
			}
		})
	}
}

func TestWorkflowTimerLifecycleExactCauseReplayConvergesAfterTerminalStateOnBothStores(t *testing.T) {
	for _, tc := range workflowJoinStoreCases() {
		for _, terminal := range []string{workflowTimerStatusFired, workflowTimerStatusCancelled} {
			t.Run(tc.name+"/"+terminal, func(t *testing.T) {
				store, ctx := tc.open(t)
				pc, entityID, activation := seedWorkflowTimerOwnerActivation(t, store, ctx, &recordingPipelineBus{}, false)
				if terminal == workflowTimerStatusFired {
					if outcome, err := fireWorkflowTimerTestWakeup(ctx, pc, activation); err != nil || outcome != WorkflowTimerFireCommitted {
						t.Fatalf("fire activation outcome=%q err=%v", outcome, err)
					}
				} else if err := store.RunPipelineMutation(ctx, func(txctx context.Context) error {
					cancelled, changed, err := store.cancelWorkflowTimerActivation(txctx, activation.Ref)
					if err != nil || !changed {
						return errors.Join(err, fmt.Errorf("cancel changed=%v", changed))
					}
					return pc.workflowTimers.queueCancellation(txctx, cancelled)
				}); err != nil {
					t.Fatalf("cancel activation: %v", err)
				}

				cause := workflowTimerCause{
					Kind: workflowTimerCauseInitial, OccurredAt: activation.CreatedAt, ToState: "waiting",
				}
				if err := store.RunPipelineMutation(ctx, func(txctx context.Context) error {
					return pc.workflowTimers.Reconcile(txctx, entityID, "", "waiting", cause)
				}); err != nil {
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

func TestWorkflowTimerLifecycleReactivatesOnlyOnLaterStageEntryOnBothStores(t *testing.T) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, ctx := tc.open(t)
			pc, entityID, first := seedWorkflowTimerOwnerActivation(t, store, ctx, &recordingPipelineBus{}, false)
			if outcome, err := fireWorkflowTimerTestWakeup(ctx, pc, first); err != nil || outcome != WorkflowTimerFireCommitted {
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

func TestWorkflowTimerLifecycleEventOnlyHandlerDoesNotReplayStateEntryOnBothStores(t *testing.T) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, ctx := tc.open(t)
			runID := runtimecorrelation.RunIDFromContext(ctx)
			entityID := uuid.NewString()
			createdAt := canonicalWorkflowTimerTime(time.Now().Add(-2 * time.Hour))
			if err := store.Upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
				InstanceID: entityID, StorageRef: entityID, WorkflowName: "workflow-timer-owner-test",
				WorkflowVersion: "1.0.0", CurrentState: "waiting", EnteredStageAt: createdAt,
				CreatedAt: createdAt, Metadata: map[string]any{"run_id": runID},
			})); err != nil {
				t.Fatalf("seed workflow instance: %v", err)
			}
			bundle := workflowTimerEventOnlyStateTriggerBundle()
			bus := &recordingPipelineBus{}
			pc := NewPipelineCoordinatorWithOptions(bus, store.db, PipelineCoordinatorOptions{
				Module: &pipelineFixtureWorkflowModule{source: semanticview.Wrap(bundle)}, WorkflowStore: store,
			})

			if err := store.RunPipelineMutation(ctx, func(txctx context.Context) error {
				return pc.workflowTimers.Reconcile(txctx, entityID, "", "waiting", workflowTimerCause{
					Kind: workflowTimerCauseInitial, OccurredAt: createdAt, ToState: "waiting",
				})
			}); err != nil {
				t.Fatalf("activate state-entry timer: %v", err)
			}
			active := listWorkflowTimerOwnerActivations(t, store, ctx, entityID, true)
			if len(active) != 1 || active[0].Ref.Declaration != "waiting.state_entry" {
				t.Fatalf("initial active timers = %#v, want waiting.state_entry", active)
			}
			stateTimer := active[0]
			execute := func(eventType string, eventAt time.Time) {
				t.Helper()
				eventID := uuid.NewString()
				payload := []byte(`{}`)
				evt := eventtest.RunCreatingRootIngress(
					eventID, events.EventType(eventType), "operator", "", payload, 0,
					runID, "", events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), eventAt,
				)
				persistWorkflowTimerEvent(t, store, ctx, eventID, eventType, runID, entityID, payload, eventAt)
				result, err := pc.executeNodeContractHandler(ctx, "observer", runtimecontracts.SystemNodeEventHandler{}, workflowTriggerContext{
					Event: evt, State: pc.currentWorkflowState(ctx, entityID),
				}, false)
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
				activationByDeclaration[activation.Ref.Declaration] = activation
			}
			persistedStateTimer := activationByDeclaration["waiting.state_entry"]
			if persistedStateTimer.Ref != stateTimer.Ref || persistedStateTimer.Status != workflowTimerStatusActive {
				t.Fatalf("state-entry timer after event-only handlers = %#v, want original active activation %#v", persistedStateTimer, stateTimer.Ref)
			}
			if got := activationByDeclaration["waiting.event_armed"].Status; got != workflowTimerStatusActive {
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
			if got := loadWorkflowTimerOwnerActivation(t, store, ctx, activationByDeclaration["waiting.event_armed"].Ref.ActivationID).Status; got != workflowTimerStatusActive {
				t.Fatalf("event-armed timer status after state timer fire = %q, want active", got)
			}
		})
	}
}

func TestWorkflowTimerLifecycleReconcilesOnlyHandledOutcomesOnBothStores(t *testing.T) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, ctx := tc.open(t)
			entityID := uuid.NewString()
			createdAt := canonicalWorkflowTimerTime(time.Now())
			if err := store.Upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
				InstanceID: entityID, StorageRef: entityID, WorkflowName: "workflow-timer-owner-test",
				WorkflowVersion: "1.0.0", CurrentState: "waiting", EnteredStageAt: createdAt,
				CreatedAt: createdAt, Metadata: map[string]any{"run_id": runtimecorrelation.RunIDFromContext(ctx)},
			})); err != nil {
				t.Fatalf("seed workflow instance: %v", err)
			}
			pc := NewPipelineCoordinatorWithOptions(&recordingPipelineBus{}, store.db, PipelineCoordinatorOptions{
				Module:        &pipelineFixtureWorkflowModule{source: semanticview.Wrap(workflowTimerHandledOutcomeBundle())},
				WorkflowStore: store,
			})
			eventOffset := time.Duration(0)
			execute := func(eventType string, payload []byte, handler runtimecontracts.SystemNodeEventHandler, wantHandled bool) {
				t.Helper()
				eventOffset++
				if len(payload) == 0 {
					payload = []byte(`{}`)
				}
				eventID := uuid.NewString()
				eventAt := createdAt.Add(eventOffset * time.Minute)
				evt := eventtest.RunCreatingRootIngress(
					eventID, events.EventType(eventType), "operator", "", payload, 0,
					runtimecorrelation.RunIDFromContext(ctx), "", events.EnvelopeForEntityID(events.EventEnvelope{}, entityID),
					eventAt,
				)
				persistWorkflowTimerEvent(t, store, ctx, eventID, eventType, runtimecorrelation.RunIDFromContext(ctx), entityID, payload, eventAt)
				result, err := pc.executeNodeContractHandler(ctx, "observer", handler, workflowTriggerContext{
					Event: evt, State: pc.currentWorkflowState(ctx, entityID),
				}, false)
				if err != nil {
					t.Fatalf("execute %s handler: %v", eventType, err)
				}
				if result.Handled != wantHandled {
					t.Fatalf("%s handled = %v, want %v", eventType, result.Handled, wantHandled)
				}
			}
			declarations := func(id string) []WorkflowTimerActivation {
				t.Helper()
				all := listWorkflowTimerOwnerActivations(t, store, ctx, entityID, false)
				matched := make([]WorkflowTimerActivation, 0, 1)
				for _, activation := range all {
					if activation.Ref.Declaration == id {
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

			execute("accepted.start", nil, runtimecontracts.SystemNodeEventHandler{}, true)
			assertOneStatus("accepted", workflowTimerStatusActive)
			execute("accepted.cancel", nil, runtimecontracts.SystemNodeEventHandler{}, true)
			assertOneStatus("accepted", workflowTimerStatusCancelled)

			execute("reject.target", nil, runtimecontracts.SystemNodeEventHandler{}, true)
			execute("guard.reject", nil, runtimecontracts.SystemNodeEventHandler{
				Guard: &runtimecontracts.GuardSpec{Check: "false", OnFail: "reject"},
			}, false)
			if matched := declarations("reject.start"); len(matched) != 0 {
				t.Fatalf("guard reject created timer: %#v", matched)
			}
			assertOneStatus("reject.target", workflowTimerStatusActive)

			execute("discard.target", nil, runtimecontracts.SystemNodeEventHandler{}, true)
			execute("guard.discard", nil, runtimecontracts.SystemNodeEventHandler{
				Guard: &runtimecontracts.GuardSpec{Check: "false", OnFail: "discard"},
			}, false)
			if matched := declarations("discard.start"); len(matched) != 0 {
				t.Fatalf("guard discard created timer: %#v", matched)
			}
			assertOneStatus("discard.target", workflowTimerStatusActive)

			dedupHandler := runtimecontracts.SystemNodeEventHandler{Accumulate: &runtimecontracts.AccumulateSpec{
				Into: "items", From: "payload", DedupBy: "payload.item_id",
			}}
			execute("dedup.event", []byte(`{"item_id":"item-1"}`), dedupHandler, true)
			assertOneStatus("dedup.start", workflowTimerStatusActive)
			execute("dedup.reset", nil, runtimecontracts.SystemNodeEventHandler{}, true)
			assertOneStatus("dedup.start", workflowTimerStatusCancelled)
			execute("dedup.target", nil, runtimecontracts.SystemNodeEventHandler{}, true)
			assertOneStatus("dedup.target", workflowTimerStatusActive)
			execute("dedup.event", []byte(`{"item_id":"item-1"}`), dedupHandler, false)
			assertOneStatus("dedup.start", workflowTimerStatusCancelled)
			assertOneStatus("dedup.target", workflowTimerStatusActive)
		})
	}
}

func TestWorkflowTimerLifecycleEventHandlerFencesLoopGenerationOnBothStores(t *testing.T) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, ctx := tc.open(t)
			runID := runtimecorrelation.RunIDFromContext(ctx)
			entityID := uuid.NewString()
			createdAt := canonicalWorkflowTimerTime(time.Now())
			loopActivation, err := loopruntime.New(
				runID, entityID, "", "revision", "revision_id", uuid.NewString(), "waiting", 3, createdAt,
			)
			if err != nil {
				t.Fatalf("create loop activation: %v", err)
			}
			carrier := runtimeengine.NewStateCarrier(
				map[string]any{"run_id": runID}, nil, map[string]map[string]any{},
			)
			if err := loopruntime.Store(carrier.StateBuckets, loopActivation); err != nil {
				t.Fatalf("store loop activation: %v", err)
			}
			if err := store.Upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
				InstanceID: entityID, StorageRef: entityID, WorkflowName: "workflow-timer-owner-test",
				WorkflowVersion: "1.0.0", CurrentState: "waiting", EnteredStageAt: createdAt,
				CreatedAt: createdAt, Metadata: carrier.PersistedMetadata(), StateBuckets: carrier.PersistedStateBuckets(),
			})); err != nil {
				t.Fatalf("seed workflow instance: %v", err)
			}

			bus := &recordingPipelineBus{}
			pc := NewPipelineCoordinatorWithOptions(bus, store.db, PipelineCoordinatorOptions{
				Module:        &pipelineFixtureWorkflowModule{source: semanticview.Wrap(workflowTimerLoopEventBundle())},
				WorkflowStore: store,
			})
			armHandler := runtimecontracts.SystemNodeEventHandler{
				Loop: &runtimecontracts.LoopOperationSpec{Admit: "revision", From: "waiting"},
			}
			execute := func(eventID, eventType, revisionID string, handler runtimecontracts.SystemNodeEventHandler) {
				t.Helper()
				payload := []byte(fmt.Sprintf(`{"revision_id":%q}`, revisionID))
				eventAt := createdAt.Add(time.Minute)
				evt := eventtest.RunCreatingRootIngress(
					eventID, events.EventType(eventType), "operator", "",
					payload, 0, runID, "",
					events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), eventAt,
				)
				persistWorkflowTimerEvent(t, store, ctx, eventID, eventType, runID, entityID, payload, eventAt)
				result, err := pc.executeNodeContractHandler(ctx, "observer", handler, workflowTriggerContext{
					Event: evt, State: pc.currentWorkflowState(ctx, entityID),
				}, false)
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

			repeatHandler := runtimecontracts.SystemNodeEventHandler{
				Loop:       &runtimecontracts.LoopOperationSpec{Repeat: "revision", From: "waiting"},
				AdvancesTo: "waiting",
			}
			execute(uuid.NewString(), "loop.repeat", firstGeneration.RevisionID, repeatHandler)
			persistedInstance, ok, err := store.Load(ctx, entityID)
			if err != nil || !ok {
				t.Fatalf("load repeated workflow instance found=%v err=%v", ok, err)
			}
			persistedCarrier, err := runtimeengine.StateCarrierFromPersisted(persistedInstance.Metadata, persistedInstance.StateBuckets)
			if err != nil {
				t.Fatalf("decode repeated loop state: %v", err)
			}
			nextLoop, ok, err := loopruntime.Load(persistedCarrier.StateBuckets, "", "revision")
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

func TestWorkflowTimerLifecycleInitialAndEventEntrancesDoNotDuplicateOnBothStores(t *testing.T) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, ctx := tc.open(t)
			entityID := uuid.NewString()
			createdAt := canonicalWorkflowTimerTime(time.Now())
			if err := store.Upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
				InstanceID: entityID, StorageRef: entityID, WorkflowName: "workflow-timer-owner-test",
				WorkflowVersion: "1.0.0", CurrentState: "waiting", EnteredStageAt: createdAt,
				CreatedAt: createdAt, Metadata: map[string]any{"run_id": runtimecorrelation.RunIDFromContext(ctx)},
			})); err != nil {
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
				initial := workflowTimerCause{
					Kind: workflowTimerCauseInitial, EventType: "state:waiting", OccurredAt: createdAt, ToState: "waiting",
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

			restartedOwner := pipelineTestWorkOwner(t)
			restartedScheduler := newWorkflowTimerTestScheduler(t, restartedOwner)
			restarted := NewPipelineCoordinatorWithOptions(bus, store.db, PipelineCoordinatorOptions{
				Module:         &pipelineFixtureWorkflowModule{source: semanticview.Wrap(workflowTimerOwnerBundle(true))},
				WorkflowStore:  store,
				WorkOwner:      restartedOwner,
				TimerScheduler: restartedScheduler,
			})
			t.Cleanup(func() {
				stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				_ = restarted.StopWorkflowTimerLifecycle(stopCtx)
			})
			if err := restarted.RestoreWorkflowTimers(ctx); err != nil {
				t.Fatalf("RestoreWorkflowTimers: %v", err)
			}
			registered, _ := workflowTimerScheduledCounts(restartedScheduler)
			if registered != 1 {
				t.Fatalf("restored workflow wakeups = %d, want 1", registered)
			}
		})
	}
}

func TestWorkflowTimerLifecycleListsScopeWildcardsOnBothStores(t *testing.T) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, ctx := tc.open(t)
			_, entityID, activation := seedWorkflowTimerOwnerActivation(t, store, ctx, &recordingPipelineBus{}, false)
			runID := runtimecorrelation.RunIDFromContext(ctx)
			lookalikeID := uuid.NewString()
			lookalikeTaskID := "workflowXtimer:v1:generic"
			if store.isSQLite() {
				_, err := store.db.ExecContext(ctx, `
					INSERT INTO timers (
						timer_id, run_id, timer_name, entity_id, flow_instance, fire_event,
						fire_payload, fire_at, recurring, owner_agent, task_type, status, created_at
					) VALUES (?, ?, ?, ?, 'generic', 'generic.tick', '{}', ?, false, 'generic', 'timer', 'active', ?)
				`, lookalikeID, runID, lookalikeTaskID, entityID, activation.FireAt, activation.CreatedAt)
				if err != nil {
					t.Fatalf("insert SQLite generic prefix lookalike: %v", err)
				}
			} else {
				_, err := store.db.ExecContext(ctx, `
					INSERT INTO timers (
						timer_id, run_id, timer_name, entity_id, flow_instance, fire_event,
						fire_payload, fire_at, recurring, owner_agent, task_type, status, created_at
					) VALUES (
						$1::uuid, $2::uuid, $3, $4::uuid, 'generic', 'generic.tick',
						'{}'::jsonb, $5, false, 'generic', 'timer', 'active', $6
					)
				`, lookalikeID, runID, lookalikeTaskID, entityID, activation.FireAt, activation.CreatedAt)
				if err != nil {
					t.Fatalf("insert PostgreSQL generic prefix lookalike: %v", err)
				}
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
					activations, err := store.listWorkflowTimerActivations(ctx, filter.runID, filter.entityID, true)
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

func TestWorkflowTimerLifecycleRollbackAndCancellationOnBothStores(t *testing.T) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, ctx := tc.open(t)
			publishFailure := errors.New("publish failed")
			bus := &recordingPipelineBus{publishErr: publishFailure}
			pc, entityID, activation := seedWorkflowTimerOwnerActivation(t, store, ctx, bus, false)

			outcome, err := fireWorkflowTimerTestWakeup(ctx, pc, activation)
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

			restartedOwner := pipelineTestWorkOwner(t)
			restartedScheduler := newWorkflowTimerTestScheduler(t, restartedOwner)
			restarted := NewPipelineCoordinatorWithOptions(bus, store.db, PipelineCoordinatorOptions{
				Module:         &pipelineFixtureWorkflowModule{source: semanticview.Wrap(workflowTimerOwnerBundle(false))},
				WorkflowStore:  store,
				WorkOwner:      restartedOwner,
				TimerScheduler: restartedScheduler,
			})
			t.Cleanup(func() {
				stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				_ = restarted.StopWorkflowTimerLifecycle(stopCtx)
			})
			if err := restarted.RestoreWorkflowTimers(ctx); err != nil {
				t.Fatalf("restore after cancel: %v", err)
			}
			registered, _ := workflowTimerScheduledCounts(restartedScheduler)
			if registered != 0 {
				t.Fatalf("restored cancelled workflow wakeups = %d, want 0", registered)
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
			missing := WorkflowTimerWakeup{family: workflowTimerActivationWakeup, occurrence: missingOccurrence, dueAt: activation.FireAt}
			outcome, err := fireTypedWorkflowTimerTestWakeup(ctx, pc, missing)
			if err != nil || outcome != WorkflowTimerFireTerminal {
				t.Fatalf("missing callback outcome=%q err=%v, want terminal nil", outcome, err)
			}

			mismatchedRef := activation.Ref
			mismatchedRef.Declaration = "different.timer"
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

func TestWorkflowTimerLifecycleIsolatesStaleActivationAcrossCancelAndReentryOnBothStores(t *testing.T) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, ctx := tc.open(t)
			bus := &recordingPipelineBus{}
			pc, entityID, first := seedWorkflowTimerOwnerActivation(t, store, ctx, bus, false)
			cancelAt := canonicalWorkflowTimerTime(first.CreatedAt.Add(time.Minute))
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

func TestWorkflowTimerLifecycleActivationRollbackDoesNotRegisterOnBothStores(t *testing.T) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, ctx := tc.open(t)
			entityID := uuid.NewString()
			createdAt := canonicalWorkflowTimerTime(time.Now())
			if err := store.Upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
				InstanceID: entityID, StorageRef: entityID, WorkflowName: "workflow-timer-owner-test",
				WorkflowVersion: "1.0.0", CurrentState: "waiting", EnteredStageAt: createdAt,
				CreatedAt: createdAt, Metadata: map[string]any{"run_id": runtimecorrelation.RunIDFromContext(ctx)},
			})); err != nil {
				t.Fatalf("seed workflow instance: %v", err)
			}
			owner := pipelineTestWorkOwner(t)
			scheduler := newWorkflowTimerTestScheduler(t, owner)
			pc := NewPipelineCoordinatorWithOptions(&recordingPipelineBus{}, store.db, PipelineCoordinatorOptions{
				Module:         &pipelineFixtureWorkflowModule{source: semanticview.Wrap(workflowTimerOwnerBundle(false))},
				WorkflowStore:  store,
				WorkOwner:      owner,
				TimerScheduler: scheduler,
			})
			t.Cleanup(func() {
				stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				_ = pc.StopWorkflowTimerLifecycle(stopCtx)
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
			registered, _ := workflowTimerScheduledCounts(scheduler)
			if registered != 0 {
				t.Fatalf("workflow wakeups after activation rollback = %d, want 0", registered)
			}
		})
	}
}

func TestWorkflowTimerWakeupReconciliationSerializesCancellationOnBothStores(t *testing.T) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, ctx := tc.open(t)
			pc, _, activation := seedWorkflowTimerOwnerActivationWithDelay(t, store, ctx, &recordingPipelineBus{}, false, "1h")
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
				cancelErr <- store.RunPipelineMutation(ctx, func(txctx context.Context) error {
					cancelled, changed, err := store.cancelWorkflowTimerActivation(txctx, activation.Ref)
					if err != nil {
						return err
					}
					if !changed {
						return errors.New("workflow timer cancellation did not change the active row")
					}
					return pc.workflowTimers.queueCancellation(txctx, cancelled)
				})
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

func TestWorkflowTimerWakeupReconciliationRetiresTerminalAndMissingRowsOnBothStores(t *testing.T) {
	for _, tc := range workflowJoinStoreCases() {
		for _, state := range []string{"terminal", "missing"} {
			t.Run(tc.name+"/"+state, func(t *testing.T) {
				store, ctx := tc.open(t)
				pc, _, activation := seedWorkflowTimerOwnerActivationWithDelay(t, store, ctx, &recordingPipelineBus{}, false, "1h")
				switch state {
				case "terminal":
					if err := store.RunPipelineMutation(ctx, func(txctx context.Context) error {
						_, changed, err := store.cancelWorkflowTimerActivation(txctx, activation.Ref)
						if err == nil && !changed {
							return errors.New("workflow timer cancellation did not change the active row")
						}
						return err
					}); err != nil {
						t.Fatalf("terminalize workflow timer: %v", err)
					}
				case "missing":
					placeholder := "$1"
					if store.isSQLite() {
						placeholder = "?"
					}
					if _, err := store.db.ExecContext(ctx, "DELETE FROM timers WHERE timer_id = "+placeholder, activation.Ref.ActivationID); err != nil {
						t.Fatalf("delete workflow timer row: %v", err)
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

func TestWorkflowTimerLifecycleStopFencesRestoreAndRecoveryOnBothStores(t *testing.T) {
	for _, tc := range workflowJoinStoreCases() {
		for _, operation := range []string{"restore", "recovery"} {
			t.Run(tc.name+"/"+operation, func(t *testing.T) {
				store, ctx := tc.open(t)
				pc, _, activation := seedWorkflowTimerOwnerActivationWithDelay(t, store, ctx, &recordingPipelineBus{}, false, "1h")
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

func TestWorkflowTimerRecoveryCoalescesTypedOccurrencesAndJoins(t *testing.T) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name+"/coalesces", func(t *testing.T) {
			store, ctx := tc.open(t)
			pc, _, activation := seedWorkflowTimerOwnerActivationAt(
				t, store, ctx, &recordingPipelineBus{}, false, "1h", time.Now(), false,
			)
			pc.workflowTimers.workOwner = pipelineTestWorkOwner(t)
			if err := pc.workflowTimers.bindScheduler(newWorkflowTimerTestScheduler(t, pc.workflowTimers.workOwner)); err != nil {
				t.Fatalf("bind workflow timer scheduler: %v", err)
			}
			if err := pc.workflowTimers.retireWakeup(activation.Ref); err != nil {
				t.Fatalf("retire initial workflow timer wakeup: %v", err)
			}
			waitForWorkflowTimerCondition(t, time.Second, func() bool {
				active, draining := workflowTimerScheduledCounts(pc.workflowTimers.scheduler)
				return active == 0 && draining == 0
			}, "initial typed wakeup cancellation")

			for attempt := 0; attempt < 3; attempt++ {
				if !pc.workflowTimers.startWakeupRecovery(activation.Ref) {
					t.Fatalf("start coalesced recovery attempt %d", attempt+1)
				}
			}
			pc.workflowTimers.recoveryMu.Lock()
			recovering := len(pc.workflowTimers.recovering)
			pc.workflowTimers.recoveryMu.Unlock()
			if recovering != 1 {
				t.Fatalf("coalesced recoveries = %d, want 1", recovering)
			}
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
			store, ctx := tc.open(t)
			pc, _, activation := seedWorkflowTimerOwnerActivationAt(
				t, store, ctx, &recordingPipelineBus{}, false, "1h", time.Now(), false,
			)
			pc.workflowTimers.workOwner = pipelineTestWorkOwner(t)
			if err := pc.workflowTimers.bindScheduler(newWorkflowTimerTestScheduler(t, pc.workflowTimers.workOwner)); err != nil {
				t.Fatalf("bind workflow timer scheduler: %v", err)
			}
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

func TestWorkflowTimerGlobalRestoreDefersStandingUntilRunScopedAdoptionOnBothStores(t *testing.T) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, ctx := tc.open(t)
			standingCtx := ctx
			if store.isSQLite() {
				if _, err := store.db.ExecContext(ctx, `
					CREATE TABLE standing_services (
						current_run_id TEXT NOT NULL,
						declaration_present BOOLEAN NOT NULL,
						effective_state TEXT NOT NULL
					)
				`); err != nil {
					t.Fatalf("create standing ownership fixture: %v", err)
				}
				if _, err := store.db.ExecContext(ctx, `
					INSERT INTO standing_services (current_run_id, declaration_present, effective_state)
					VALUES (?, TRUE, 'active')
				`, runtimecorrelation.RunIDFromContext(ctx)); err != nil {
					t.Fatalf("seed standing ownership fixture: %v", err)
				}
			} else {
				packageKey := "root"
				flowID := "standing-workflow-timer"
				sourceFact, ok := runtimecorrelation.BundleSourceFactFromContext(ctx)
				if !ok {
					t.Fatal("standing timer test context missing bundle source fact")
				}
				standing, err := store.ReconcileStandingService(ctx, StandingServiceCandidate{
					ServiceID:  runtimeflowidentity.StandingServiceID(packageKey, flowID),
					PackageKey: packageKey,
					FlowID:     flowID,
					InstanceID: flowID,
					EntityID:   uuid.NewString(),
					Source:     sourceFact,
				})
				if err != nil {
					t.Fatalf("reconcile standing service: %v", err)
				}
				standingCtx = runtimecorrelation.WithRunID(ctx, standing.RunID)
			}
			pc, _, _ := seedWorkflowTimerOwnerActivationAt(
				t, store, standingCtx, &recordingPipelineBus{}, false, "1h", time.Now(), false,
			)
			scheduler := newWorkflowTimerTestScheduler(t, pc.workflowTimers.workOwner)
			if err := pc.workflowTimers.bindScheduler(scheduler); err != nil {
				t.Fatalf("bind workflow timer scheduler: %v", err)
			}

			globalCtx := testAuthorActivityContext(t, context.Background())
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

func TestWorkflowTimerInitialEntryStaysDormantUntilExplicitArmOnBothStores(t *testing.T) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, ctx := tc.open(t)
			owner := pipelineTestWorkOwner(t)
			scheduler := newWorkflowTimerTestScheduler(t, owner)
			published := make(chan events.Event, 1)
			bus := &recordingPipelineBus{
				publishInMutationHook: func(_ context.Context, event events.Event) error {
					published <- event
					return nil
				},
			}
			NewPipelineCoordinatorWithOptions(bus, store.db, PipelineCoordinatorOptions{
				Module: &pipelineFixtureWorkflowModule{
					source: semanticview.Wrap(workflowTimerOwnerBundleWithDelay(false, "1ns")),
				},
				WorkflowStore:  store,
				WorkOwner:      owner,
				TimerScheduler: scheduler,
			})
			entityID := uuid.NewString()
			createdAt := canonicalWorkflowTimerTime(time.Now().Add(-time.Second))
			result, err := store.MaterializeInitialEntry(ctx, WorkflowInstance{
				InstanceID: entityID, StorageRef: entityID, WorkflowName: "workflow-timer-owner-test",
				WorkflowVersion: "1.0.0", CurrentState: "waiting",
				Metadata: map[string]any{"run_id": runtimecorrelation.RunIDFromContext(ctx)},
			}, createdAt)
			if err != nil {
				t.Fatalf("MaterializeInitialEntry: %v", err)
			}
			if result != WorkflowInitialMaterializationCreated {
				t.Fatalf("materialization result = %d, want created", result)
			}
			active := listWorkflowTimerOwnerActivations(t, store, ctx, entityID, true)
			if len(active) != 1 {
				t.Fatalf("durable initial timers = %#v, want one", active)
			}
			if scheduled, draining := workflowTimerScheduledCounts(scheduler); scheduled != 0 || draining != 0 {
				t.Fatalf("pre-arm wakeups active=%d draining=%d, want 0", scheduled, draining)
			}
			select {
			case event := <-published:
				t.Fatalf("initial timer published before explicit arm: %s", event.ID())
			default:
			}

			if err := store.ArmInitialEntryTimers(ctx, entityID); err != nil {
				t.Fatalf("ArmInitialEntryTimers: %v", err)
			}
			var event events.Event
			select {
			case event = <-published:
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for explicitly armed initial timer")
			}
			if event.RunID() != runtimecorrelation.RunIDFromContext(ctx) || event.EntityID() != entityID {
				t.Fatalf("published timer scope run=%q entity=%q, want run=%q entity=%q", event.RunID(), event.EntityID(), runtimecorrelation.RunIDFromContext(ctx), entityID)
			}
			waitForWorkflowTimerPersistedStatus(t, store, ctx, active[0].Ref.ActivationID, workflowTimerStatusFired)
			select {
			case duplicate := <-published:
				t.Fatalf("initial timer published more than once: %s", duplicate.ID())
			default:
			}
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
	store *WorkflowInstanceStore,
	ctx context.Context,
	activationID string,
	want string,
) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		activation, found, err := store.loadWorkflowTimerActivation(ctx, activationID, false)
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

func TestWorkflowTimerLifecycleSchedulerRetryPreservesOccurrenceOnBothStores(t *testing.T) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, ctx := tc.open(t)
			publishFailure := errors.New("transient publish failure")
			bus := &failOnceWorkflowTimerBus{recordingPipelineBus: &recordingPipelineBus{}, err: publishFailure}
			bus.failures.Store(1)
			pc, _, activation := seedWorkflowTimerOwnerActivationWithDelay(t, store, ctx, bus, false, "1ms")
			pc.workflowTimers.workOwner = pipelineTestWorkOwner(t)
			t.Cleanup(func() {
				stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				_ = pc.StopWorkflowTimerLifecycle(stopCtx)
			})
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

func TestWorkflowTimerLifecyclePostgresFireRollsBackWithOuterMutation(t *testing.T) {
	for _, tc := range workflowJoinStoreCases() {
		if tc.name != "postgres" {
			continue
		}
		store, ctx := tc.open(t)
		bus := &recordingPipelineBus{}
		pc, _, activation := seedWorkflowTimerOwnerActivation(t, store, ctx, bus, false)

		tx, err := store.db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("BeginTx: %v", err)
		}
		t.Cleanup(func() { _ = tx.Rollback() })
		actions := make([]OwnerAction, 0, 1)
		rollbackActions := make([]OwnerAction, 0, 1)
		txctx := withPipelinePostCommitActions(WithPipelineSQLTxContext(ctx, tx), &actions)
		txctx = withPipelineRollbackActions(txctx, &rollbackActions)
		txctx, err = runtimeauthoractivity.Begin(txctx, tx, runtimeauthoractivity.DialectPostgres)
		if err != nil {
			t.Fatalf("begin author activity: %v", err)
		}

		outcome, err := fireWorkflowTimerTestWakeup(txctx, pc, activation)
		if err != nil || outcome != WorkflowTimerFireCommitted {
			t.Fatalf("FireWorkflowTimer outcome=%q err=%v", outcome, err)
		}
		if len(actions) != 0 {
			t.Fatalf("post-commit actions = %d, want no generic claim release", len(actions))
		}
		if err := tx.Rollback(); err != nil {
			t.Fatalf("Rollback: %v", err)
		}
		persisted := loadWorkflowTimerOwnerActivation(t, store, ctx, activation.Ref.ActivationID)
		if persisted.Status != workflowTimerStatusActive {
			t.Fatalf("rolled-back timer status = %q, want active", persisted.Status)
		}
	}
}

func seedWorkflowTimerOwnerActivation(
	t *testing.T,
	store *WorkflowInstanceStore,
	ctx context.Context,
	bus Bus,
	recurring bool,
) (*PipelineCoordinator, string, WorkflowTimerActivation) {
	t.Helper()
	return seedWorkflowTimerOwnerActivationAt(t, store, ctx, bus, recurring, "1h", time.Now().Add(-2*time.Hour), false)
}

func seedWorkflowTimerOwnerActivationWithDelay(
	t *testing.T,
	store *WorkflowInstanceStore,
	ctx context.Context,
	bus Bus,
	recurring bool,
	delay string,
) (*PipelineCoordinator, string, WorkflowTimerActivation) {
	t.Helper()
	return seedWorkflowTimerOwnerActivationAt(t, store, ctx, bus, recurring, delay, time.Now(), true)
}

func seedWorkflowTimerOwnerActivationAt(
	t *testing.T,
	store *WorkflowInstanceStore,
	ctx context.Context,
	bus Bus,
	recurring bool,
	delay string,
	created time.Time,
	register bool,
) (*PipelineCoordinator, string, WorkflowTimerActivation) {
	t.Helper()
	entityID := uuid.NewString()
	createdAt := canonicalWorkflowTimerTime(created)
	if err := store.Upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID: entityID, StorageRef: entityID, WorkflowName: "workflow-timer-owner-test",
		WorkflowVersion: "1.0.0", CurrentState: "waiting", EnteredStageAt: createdAt,
		CreatedAt: createdAt, Metadata: map[string]any{"run_id": runtimecorrelation.RunIDFromContext(ctx)},
	})); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}
	owner := pipelineTestWorkOwner(t)
	var scheduler *Scheduler
	if register {
		scheduler = newWorkflowTimerTestScheduler(t, owner)
	}
	pc := NewPipelineCoordinatorWithOptions(bus, store.db, PipelineCoordinatorOptions{
		Module:         &pipelineFixtureWorkflowModule{source: semanticview.Wrap(workflowTimerOwnerBundleWithDelay(recurring, delay))},
		WorkflowStore:  store,
		WorkOwner:      owner,
		TimerScheduler: scheduler,
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

func persistWorkflowTimerEvent(
	t *testing.T,
	store *WorkflowInstanceStore,
	ctx context.Context,
	eventID, eventType, runID, entityID string,
	payload []byte,
	createdAt time.Time,
) {
	t.Helper()
	dialect := runtimeauthoractivity.DialectPostgres
	if store.isSQLite() {
		dialect = runtimeauthoractivity.DialectSQLite
	}
	event := eventtest.RunCreatingRootIngress(
		eventID, events.EventType(eventType), "operator", "", payload, 0, runID, "",
		events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), createdAt.UTC(),
	)
	seedPipelineEventRecordForDialect(t, ctx, store.db, dialect, event)
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

func workflowTimerEventOnlyStateTriggerBundle() *runtimecontracts.WorkflowContractBundle {
	return &runtimecontracts.WorkflowContractBundle{Semantics: runtimecontracts.WorkflowSemanticView{
		Name: "workflow-timer-owner-test", Version: "1.0.0", InitialStage: "waiting",
		Timers: []runtimecontracts.WorkflowTimerContract{
			{
				ID: "waiting.state_entry", Stage: "waiting", StageOwned: true, Owner: "runtime", Event: "timer.state_entry",
				StartOn: "state:waiting", Delay: "1h",
			},
			{
				ID: "waiting.event_armed", Owner: "runtime", Event: "timer.event_armed",
				StartOn: "event:timer.arm", CancelOn: "state:waiting", Delay: "1h",
			},
		},
	}}
}

func workflowTimerLoopEventBundle() *runtimecontracts.WorkflowContractBundle {
	return &runtimecontracts.WorkflowContractBundle{Semantics: runtimecontracts.WorkflowSemanticView{
		Name: "workflow-timer-owner-test", Version: "1.0.0", InitialStage: "waiting",
		Stages: []runtimecontracts.WorkflowStageContract{{ID: "waiting"}, {ID: "escaped"}},
		Loops: []runtimecontracts.WorkflowLoopPlan{{
			ID: "revision", RevisionField: "revision_id", MaxAttempts: runtimecontracts.LoopAttemptLimit{Literal: 3},
			Escape: runtimecontracts.LoopEscapeSpec{AdvancesTo: "escaped"}, EntryStage: "waiting", RegionStages: []string{"waiting"},
		}},
		Timers: []runtimecontracts.WorkflowTimerContract{{
			ID: "waiting.event_armed", Owner: "runtime", Event: "timer.event_armed",
			StartOn: "event:timer.arm", Delay: "1h",
		}},
	}}
}

func workflowTimerHandledOutcomeBundle() *runtimecontracts.WorkflowContractBundle {
	timer := func(id, startOn, cancelOn string) runtimecontracts.WorkflowTimerContract {
		return runtimecontracts.WorkflowTimerContract{
			ID: id, Owner: "runtime", Event: "timer." + id, StartOn: startOn, CancelOn: cancelOn, Delay: "1h",
		}
	}
	return &runtimecontracts.WorkflowContractBundle{Semantics: runtimecontracts.WorkflowSemanticView{
		Name: "workflow-timer-owner-test", Version: "1.0.0", InitialStage: "waiting",
		Timers: []runtimecontracts.WorkflowTimerContract{
			timer("accepted", "event:accepted.start", "event:accepted.cancel"),
			timer("reject.start", "event:guard.reject", ""),
			timer("reject.target", "event:reject.target", "event:guard.reject"),
			timer("discard.start", "event:guard.discard", ""),
			timer("discard.target", "event:discard.target", "event:guard.discard"),
			timer("dedup.start", "event:dedup.event", "event:dedup.reset"),
			timer("dedup.target", "event:dedup.target", "event:dedup.event"),
		},
	}}
}
