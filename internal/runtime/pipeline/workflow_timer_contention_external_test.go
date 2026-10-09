package pipeline_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/testutil/flowactivationfixture"
	"github.com/google/uuid"
)

type timerTransitionContentionOwner struct {
	runtimepipeline.WorkflowPersistenceOwner
	reject                     bool
	cleanupErr                 error
	lastErr                    error
	calls                      int
	acknowledged               int
	commitContext              func(context.Context) context.Context
	interruptedActivationReads int
	activationReadErr          error
}

type timerTransitionOutcomeCapture struct {
	coordinator     *runtimepipeline.PipelineCoordinator
	outcome         runtimepipelineobligation.ExecutionOutcome
	eventID         string
	event           events.Event
	beforeIntercept func(context.Context) context.Context
	entryErr        error
	interceptErr    error
}

func (c *timerTransitionOutcomeCapture) Intercept(ctx context.Context, event events.Event) (bool, []events.Event, runtimepipelineobligation.ExecutionOutcome, error) {
	if c.beforeIntercept != nil {
		ctx = c.beforeIntercept(ctx)
	}
	c.entryErr = ctx.Err()
	pass, emitted, outcome, err := c.coordinator.Intercept(ctx, event)
	c.outcome, c.eventID, c.event, c.interceptErr = outcome, event.ID(), event, err
	return pass, emitted, outcome, err
}

func (o *timerTransitionContentionOwner) LoadWorkflowTimerActivation(ctx context.Context, activationID string) (runtimepipeline.WorkflowTimerActivation, bool, error) {
	activation, found, err := o.WorkflowPersistenceOwner.LoadWorkflowTimerActivation(ctx, activationID)
	if ctx.Err() != nil {
		o.interruptedActivationReads++
		o.activationReadErr = err
	}
	return activation, found, err
}

func (o *timerTransitionContentionOwner) CommitWorkflowEngineMutation(ctx context.Context, command runtimepipeline.WorkflowEngineMutationCommand) (runtimepipeline.CommittedWorkflowEngineMutation, error) {
	o.calls++
	if o.commitContext != nil {
		ctx = o.commitContext(ctx)
	}
	if o.reject {
		command.State.ExpectedRevision++
	}
	result, err := o.WorkflowPersistenceOwner.CommitWorkflowEngineMutation(ctx, command)
	o.lastErr = err
	if result.Committed {
		o.acknowledged++
		err = errors.Join(err, o.cleanupErr)
		if o.cleanupErr != nil {
			// Cleanup metadata is not the selected store's commit acknowledgment.
			result.PostCommit = runtimepipeline.WorkflowEnginePostCommitPlan{}
		}
	}
	return result, err
}

func TestWorkflowTimerPublishedOccurrenceSurvivesTransitionContentionOnBothStores(t *testing.T) {
	verifyWorkflowTimerPublishedOccurrenceRecovery(t, []string{"live_retry", "restart_retry", "committed_cleanup"})
}

func TestIssue2564M09TimerPublishedOccurrenceCanceledTransitionReplayBothStores(t *testing.T) {
	verifyWorkflowTimerPublishedOccurrenceRecovery(t, []string{"canceled_step", "deadline_step", "canceled_dispatch", "deadline_dispatch"})
}

func TestIssue2564M09TimerDispatchEntryCancellationLeavesBusObligationReplayableBothStores(t *testing.T) {
	verifyWorkflowTimerPublishedOccurrenceRecovery(t, []string{"canceled_entry", "deadline_entry"})
}

func TestIssue2564M09TimerReceiverOnlyDeadlineLeavesBusObligationReplayableBothStores(t *testing.T) {
	verifyWorkflowTimerPublishedOccurrenceRecovery(t, []string{"deadline_entry_live_outer"})
}

func TestWorkflowTimerAcknowledgedTransitionSurvivesContextCleanupBothStores(t *testing.T) {
	verifyWorkflowTimerPublishedOccurrenceRecovery(t, []string{"committed_canceled_cleanup", "committed_deadline_cleanup"})
}

func verifyWorkflowTimerPublishedOccurrenceRecovery(t *testing.T, scenarios []string) {
	for _, tc := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{
		{name: "sqlite", open: openSQLiteGateRecoveryStore},
		{name: "postgres", open: openPostgresGateRecoveryStore},
	} {
		for _, scenario := range scenarios {
			t.Run(tc.name+"/"+scenario, func(t *testing.T) {
				selected := tc.open(t)
				runID, entityID := uuid.NewString(), uuid.NewString()
				bundle := workflowTimerServedLifecycleBundle(t, false)
				ctx, fact := workflowLifecycleSourceContext(t, selected, bundle, runID)
				source := semanticview.Wrap(bundle)
				cleanupErr := errors.New("acknowledged timer transition cleanup failed")
				owner := &timerTransitionContentionOwner{WorkflowPersistenceOwner: selected.events.(runtimepipeline.WorkflowPersistenceOwner), reject: scenario == "live_retry" || scenario == "restart_retry"}
				switch scenario {
				case "committed_cleanup":
					owner.cleanupErr = cleanupErr
				case "committed_canceled_cleanup":
					owner.cleanupErr = context.Canceled
				case "committed_deadline_cleanup":
					owner.cleanupErr = context.DeadlineExceeded
				}
				selected.persistence = runtimepipeline.NewWorkflowPersistence(owner)
				newRuntime := func() (*runtimebus.EventBus, *runtimepipeline.PipelineCoordinator, *timerTransitionOutcomeCapture) {
					t.Helper()
					bus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{
						ContractBundle: source, SourceArtifactFact: fact, PayloadAdmitter: strictWorkflowTimerPayloadAdmitter,
					}, runtimecontracts.WorkflowStageTimerInternalEvent)
					if err != nil {
						t.Fatal(err)
					}
					scheduler := runtimepipeline.NewSchedulerWithWorkOwner(pipelineExternalTestWorkOwner(t))
					// The test fires the exact occurrence synchronously, not a second timer.
					if err := scheduler.PrepareStartup(); err != nil {
						t.Fatal(err)
					}
					pc := newGateRecoveryCoordinator(bus, selected, runtimepipeline.PipelineCoordinatorOptions{
						Module: gateRecoveryModule{source: source}, TimerScheduler: scheduler, WorkOwner: pipelineExternalTestWorkOwner(t),
					})
					capture := &timerTransitionOutcomeCapture{coordinator: pc}
					bus.SetInterceptors(capture)
					t.Cleanup(func() {
						join, cancel := context.WithTimeout(context.Background(), 5*time.Second)
						defer cancel()
						if err := pc.StopWorkflowTimerLifecycle(join); err != nil {
							t.Error(err)
						}
						scheduler.Stop()
						if err := scheduler.Wait(join); err != nil {
							t.Error(err)
						}
						if err := bus.WaitForQuiescence(join); err != nil {
							t.Error(err)
						}
					})
					return bus, pc, capture
				}
				bus, pc, capture := newRuntime()
				identity := testRunScopedWorkflowInstanceForRun(runID, runID)
				at := time.Now().UTC().Add(-time.Hour)
				instance, lifecycle, err := pc.PrepareInitialEntryLifecycle(ctx, identity, runtimepipeline.WorkflowInstance{
					InstanceID: runID, StorageRef: runID, EntityID: entityID, WorkflowName: ".", WorkflowVersion: source.WorkflowVersion(),
					CurrentState: "waiting", StageDefined: true, EnteredStageAt: at, CreatedAt: at, EntityType: "test_entity", Fields: map[string]any{},
				}, at)
				if err != nil {
					t.Fatal(err)
				}
				command, err := flowactivationfixture.Command(ctx, instance, lifecycle, at)
				if err != nil {
					t.Fatal(err)
				}
				construction, err := selected.events.(runtimebus.FlowInstanceActivationCommitOwner).CommitFlowInstanceActivation(ctx, command)
				if err != nil || !construction.Acknowledged || !construction.Created {
					t.Fatalf("construct timer instance: %+v, %v", construction, err)
				}
				if err := pc.FinalizeInitialEntryLifecycle(ctx, construction.Lifecycle); err != nil {
					t.Fatal(err)
				}
				activations := selected.events.(runtimepipeline.WorkflowTimerActivationPersistence)
				rows, err := activations.ListWorkflowTimerActivations(ctx, runID, entityID, true)
				if err != nil || len(rows) != 1 {
					t.Fatalf("active timers=%+v error=%v", rows, err)
				}
				fireCtx := ctx
				var interrupted error
				entryCut := scenario == "canceled_entry" || scenario == "deadline_entry" || scenario == "deadline_entry_live_outer"
				switch scenario {
				case "deadline_entry_live_outer":
					interrupted = context.DeadlineExceeded
					capture.beforeIntercept = func(dispatchCtx context.Context) context.Context {
						// A receiver-local deadline may end before the enclosing claim context.
						step, cancel := context.WithDeadline(dispatchCtx, time.Now().Add(-time.Second))
						cancel()
						return step
					}
				case "canceled_entry", "deadline_entry":
					interrupted = context.Canceled
					var cancel context.CancelFunc
					if scenario == "deadline_entry" {
						fireCtx, cancel = context.WithTimeout(ctx, time.Second)
					} else {
						fireCtx, cancel = context.WithCancel(ctx)
					}
					defer cancel()
					capture.beforeIntercept = func(dispatchCtx context.Context) context.Context {
						if scenario == "canceled_entry" {
							cancel()
						} else {
							<-fireCtx.Done()
							// Join parent cancellation propagation without changing its deadline cause.
							cancel()
						}
						select {
						case <-dispatchCtx.Done():
						case <-time.After(5 * time.Second):
							t.Fatal("dispatch context did not end at the bounded entry cut")
						}
						return dispatchCtx
					}
				case "canceled_step":
					interrupted = context.Canceled
					owner.commitContext = func(ctx context.Context) context.Context {
						step, cancel := context.WithCancel(ctx)
						cancel()
						return step
					}
				case "deadline_step":
					interrupted = context.DeadlineExceeded
					owner.commitContext = func(ctx context.Context) context.Context {
						step, cancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
						cancel()
						return step
					}
				case "canceled_dispatch":
					interrupted = context.Canceled
					var cancel context.CancelFunc
					fireCtx, cancel = context.WithCancel(ctx)
					defer cancel()
					owner.commitContext = func(ctx context.Context) context.Context {
						cancel()
						// The receiver bridges cancellation asynchronously; join the
						// mutation context before asserting a pre-commit interruption.
						select {
						case <-ctx.Done():
						case <-time.After(5 * time.Second):
							t.Fatal("mutation context did not end at the bounded cancellation cut")
						}
						return ctx
					}
				case "deadline_dispatch":
					interrupted = context.DeadlineExceeded
					var cancel context.CancelFunc
					fireCtx, cancel = context.WithTimeout(ctx, time.Second)
					defer cancel()
					owner.commitContext = func(ctx context.Context) context.Context {
						<-fireCtx.Done()
						cancel()
						<-ctx.Done()
						return ctx
					}
				}
				outcome, err := runtimepipeline.FireWorkflowTimerOccurrenceForTest(fireCtx, pc, rows[0])
				if entryCut {
					if scenario == "deadline_entry_live_outer" && fireCtx.Err() != nil {
						t.Fatalf("receiver-only deadline also ended the outer context: %v", fireCtx.Err())
					}
					if !errors.Is(capture.entryErr, context.Canceled) && !errors.Is(capture.entryErr, context.DeadlineExceeded) {
						t.Fatalf("coordinator entry context remained live: %v", capture.entryErr)
					}
					interrupted = capture.entryErr
					if owner.calls != 0 || owner.interruptedActivationReads != 1 || !errors.Is(owner.activationReadErr, interrupted) || !errors.Is(capture.interceptErr, interrupted) || !errors.Is(err, interrupted) {
						t.Fatalf("cut did not interrupt authorization before mutation: commits=%d authorization reads=%d read error=%v intercept error=%v dispatch error=%v", owner.calls, owner.interruptedActivationReads, owner.activationReadErr, capture.interceptErr, err)
					}
					if scenario == "deadline_entry" && !errors.Is(fireCtx.Err(), context.DeadlineExceeded) {
						t.Fatalf("dispatch entry deadline did not expire: %v", fireCtx.Err())
					}
				}
				if scenario == "deadline_dispatch" {
					if !errors.Is(fireCtx.Err(), context.DeadlineExceeded) {
						t.Fatalf("dispatch deadline did not expire at the transition cut: %v", fireCtx.Err())
					}
					// The bus's work lifetime converts an expired parent to cancellation.
					if errors.Is(owner.lastErr, context.Canceled) {
						interrupted = context.Canceled
					}
				}
				if outcome != runtimepipeline.WorkflowTimerFireCommitted || (owner.reject && (err != nil || !runtimefailures.IsStateContention(owner.lastErr))) || (owner.cleanupErr != nil && !errors.Is(err, owner.cleanupErr)) || (!entryCut && interrupted != nil && (!errors.Is(owner.lastErr, interrupted) || (err != nil && !errors.Is(err, interrupted) && !errors.Is(err, fireCtx.Err())))) {
					t.Fatalf("publication outcome=%s error=%v mutation_error=%v acknowledged=%d intercept_outcome=%+v",
						outcome, err, owner.lastErr, owner.acknowledged, capture.outcome)
				}
				eventID := workflowTimerPersistedEventID(t, selected, runID)
				if capture.eventID != eventID {
					t.Fatalf("intercepted event=%s, want exact committed event=%s", capture.eventID, eventID)
				}
				fired, found, err := activations.LoadWorkflowTimerActivation(ctx, rows[0].Ref.ActivationID)
				if err != nil || !found || fired.Status != "fired" || fired.FiredAt.IsZero() {
					t.Fatalf("accepted occurrence=%+v found=%t error=%v", fired, found, err)
				}
				if owner.reject || interrupted != nil {
					before, found, err := pc.Load(ctx, identity)
					if err != nil || !found || before.CurrentState != "waiting" || len(before.TransitionHistory) != 0 || owner.acknowledged != 0 {
						t.Fatalf("rejected transition leaked: %+v found=%t error=%v", before, found, err)
					}
					if got := gateRecoveryPipelineReceiptCount(t, selected, eventID); got != 0 {
						t.Fatalf("rejected transition has %d terminal receipts", got)
					}
					retry, ok := capture.outcome.RetryRelease()
					code := "workflow_engine_state_revision_conflict"
					if interrupted != nil {
						code = "workflow_timer_transition_interrupted"
					}
					if entryCut {
						if ok || capture.outcome.Committed || !capture.outcome.ContinueDispatch() {
							t.Fatalf("unauthorized occurrence acquired timer retry/commit authority: %+v", capture.outcome)
						}
						// The bus owner, not timer authorization, preserves the obligation.
						obligations := selected.events.PipelineObligations()
						work, err := obligations.ClaimEvent(ctx, eventID, runtimepipelineobligation.PurposeRecovery)
						if err != nil {
							t.Fatalf("reclaim exact unfinished dispatch-entry obligation: %v", err)
						}
						if work.Claim.EventID() != eventID || work.Event.ID() != eventID || work.Event.TaskID() != capture.event.TaskID() || work.Event.RoutingSource() != capture.event.RoutingSource() || !work.Event.CreatedAt().Equal(capture.event.CreatedAt()) {
							t.Fatalf("bus owner reclaimed a different event/occurrence: %+v", work)
						}
						if err := obligations.Release(ctx, work.Claim); err != nil {
							t.Fatalf("release exact replay proof claim: %v", err)
						}
					} else if !ok || capture.outcome.Committed || retry.Failure() == nil || retry.Failure().Detail.Code != code {
						t.Fatalf("unacknowledged transition lost typed retry: %+v", capture.outcome)
					}
					if interrupted != nil && !entryCut {
						class := runtimefailures.ClassDependencyUnavailable
						if errors.Is(interrupted, context.DeadlineExceeded) {
							class = runtimefailures.ClassTimeout
						}
						if retry.Failure().Class != class {
							t.Fatalf("interrupted transition lost typed cause: %+v", retry.Failure())
						}
					}
					if owner.reject {
						// A still-conflicting live sweep must release, not terminally settle.
						result, err := bus.SweepPipelineObligations(ctx, 10)
						if err != nil || !result.Blocked || result.Settled != 0 || owner.calls != 2 {
							t.Fatalf("retry sweep=%+v calls=%d error=%v", result, owner.calls, err)
						}
					}
					owner.reject = false
					owner.commitContext = nil
					capture.beforeIntercept = nil
					if scenario == "restart_retry" {
						if err := pc.StopWorkflowTimerLifecycle(ctx); err != nil {
							t.Fatal(err)
						}
						bus, pc, capture = newRuntime()
						if err := pc.RestoreWorkflowTimers(ctx); err != nil {
							t.Fatal(err)
						}
						if err := runtimepipeline.NewRecoveryManagerWith(bus).RecoverToExhaustion(ctx); err != nil {
							t.Fatal(err)
						}
					} else if result, err := bus.SweepPipelineObligations(ctx, 10); err != nil || result.Settled != 1 {
						t.Fatalf("live recovery=%+v error=%v", result, err)
					}
				}
				if !capture.outcome.Committed || capture.eventID != eventID {
					t.Fatalf("transition acknowledgment lost: %+v event=%s", capture.outcome, capture.eventID)
				}
				if _, retry := capture.outcome.RetryRelease(); retry {
					t.Fatal("acknowledged transition was released for retry")
				}
				after, found, err := pc.Load(ctx, identity)
				if err != nil || !found || after.CurrentState != "done" || len(after.TransitionHistory) != 1 || after.TransitionHistory[0].TriggerEventID != eventID || owner.acknowledged != 1 {
					t.Fatalf("timer transition not acknowledged exactly once: %+v acknowledgments=%d error=%v", after, owner.acknowledged, err)
				}
				if result, err := bus.SweepPipelineObligations(ctx, 10); err != nil || result.Settled != 0 {
					t.Fatalf("settled occurrence replayed: %+v error=%v", result, err)
				}
				persisted, found, err := activations.LoadWorkflowTimerActivation(ctx, fired.Ref.ActivationID)
				if err != nil || !found || !reflect.DeepEqual(persisted, fired) || workflowTimerEventCount(t, selected, runID, runtimecontracts.WorkflowStageTimerInternalEvent) != 1 || gateRecoveryPipelineReceiptCount(t, selected, eventID) != 1 {
					t.Fatalf("recovery changed exact occurrence/history: before=%+v after=%+v error=%v", fired, persisted, err)
				}
			})
		}
	}
}
