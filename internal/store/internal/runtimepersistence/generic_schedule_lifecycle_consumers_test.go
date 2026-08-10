package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimeruncontrol "github.com/division-sh/swarm/internal/runtime/runcontrol"
	runtimerunquiescence "github.com/division-sh/swarm/internal/runtime/runquiescence"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimetimercancellation "github.com/division-sh/swarm/internal/runtime/timercancellation"
	"github.com/google/uuid"
)

const genericScheduleConsumerTestBundleHash = "bundle-v1:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"

type genericScheduleLifecycleConsumerStore interface {
	runtimegenericschedule.Store
	ApplyActiveRunQuiescence(context.Context, runtimerunquiescence.Request) (runtimerunquiescence.Result, error)
	StopRunControl(context.Context, runtimeruncontrol.TransitionRequest) (runtimeruncontrol.State, error)
}

type recordingGenericScheduleReconciler struct {
	activationIDs []string
}

type transientGenericScheduleReleaseStore struct {
	runtimegenericschedule.Store
	mu          sync.Mutex
	failures    int
	releaseSeen chan error
}

func (s *transientGenericScheduleReleaseStore) ReleaseGenericScheduleWakeup(ctx context.Context, wakeup runtimegenericschedule.Wakeup) error {
	s.mu.Lock()
	if s.failures > 0 {
		s.failures--
		s.mu.Unlock()
		err := errors.New("transient generic schedule claim release failure")
		s.releaseSeen <- err
		return err
	}
	s.mu.Unlock()
	err := s.Store.ReleaseGenericScheduleWakeup(ctx, wakeup)
	s.releaseSeen <- err
	return err
}

func (r *recordingGenericScheduleReconciler) ReconcileWakeupWithRecovery(_ context.Context, activationID string) (bool, error) {
	r.activationIDs = append(r.activationIDs, activationID)
	return false, nil
}

func newGenericScheduleAwareWorkflowTestCoordinator(
	t *testing.T,
	selected workflowTestSelectedStore,
) (*runtimepipeline.PipelineCoordinator, *recordingGenericScheduleReconciler) {
	t.Helper()
	reconciler := &recordingGenericScheduleReconciler{}
	options := completeWorkflowTestCoordinatorOptions(runtimepipeline.NewWorkflowPersistence(selected), selected)
	options.GenericSchedules = reconciler
	coordinator := runtimepipeline.NewPipelineCoordinatorWithOptions(workflowTestBus{}, options)
	if coordinator == nil {
		t.Fatal("construct generic-schedule-aware workflow test coordinator")
	}
	return coordinator, reconciler
}

func assertGenericScheduleReconciled(t *testing.T, recorder *recordingGenericScheduleReconciler, activationID string) {
	t.Helper()
	if len(recorder.activationIDs) != 1 || recorder.activationIDs[0] != activationID {
		t.Fatalf("generic schedule reconciliations = %#v, want [%s]", recorder.activationIDs, activationID)
	}
}

func seedGenericScheduleTimerFamilies(
	t *testing.T,
	selected genericScheduleLifecycleConsumerStore,
	db *sql.DB,
	ctx context.Context,
) (runtimegenericschedule.Activation, workflowTimerDDLProofRow) {
	t.Helper()
	runID := runtimecorrelation.RunIDFromContext(ctx)
	command := testAgentGenericScheduleCommand(
		t, runID, "lifecycle-agent", "lifecycle/instance", uuid.NewString(), "lifecycle-generic",
		runtimegenericschedule.AbsoluteDue(time.Now().UTC().Add(time.Hour)),
	)
	admitted, err := selected.AdmitGenericSchedule(ctx, command)
	if err != nil {
		t.Fatalf("admit generic schedule lifecycle fixture: %v", err)
	}
	workflow := newWorkflowTimerDDLProofRow(runID)
	if err := insertWorkflowTimerDDLProofRow(ctx, db, selected, workflow); err != nil {
		t.Fatalf("insert workflow timer lifecycle fixture: %v", err)
	}
	return admitted.Activation, workflow
}

func authorGenericScheduleConsumerContext(runID string) context.Context {
	return runtimeeffects.WithExecutionMode(
		runtimecorrelation.WithRunID(testAuthorActivityContextForBundle(genericScheduleConsumerTestBundleHash), runID),
		runtimeeffects.ExecutionModeLive,
	)
}

func assertGenericScheduleTimerFamilyCancellation(
	t *testing.T,
	selected genericScheduleLifecycleConsumerStore,
	db *sql.DB,
	ctx context.Context,
	generic runtimegenericschedule.Activation,
	workflow workflowTimerDDLProofRow,
	refs []runtimetimercancellation.Ref,
) {
	t.Helper()
	if len(refs) != 2 {
		t.Fatalf("timer cancellation refs = %#v, want both families", refs)
	}
	byFamily := make(map[runtimetimercancellation.Family]runtimetimercancellation.Ref, len(refs))
	for _, ref := range refs {
		byFamily[ref.Family] = ref
	}
	if got := byFamily[runtimetimercancellation.FamilyGenericSchedule]; got.ActivationID != generic.ID || got.RunID != generic.Command.RunID {
		t.Fatalf("generic cancellation ref = %#v", got)
	}
	if got := byFamily[runtimetimercancellation.FamilyWorkflowTimer]; got.ActivationID != workflow.timerID || got.RunID != workflow.runID {
		t.Fatalf("workflow cancellation ref = %#v", got)
	}
	persisted, found, err := selected.LoadGenericScheduleActivation(ctx, generic.ID)
	if err != nil || !found || persisted.Status != runtimegenericschedule.StatusCancelled {
		t.Fatalf("cancelled generic activation = %#v found=%v err=%v", persisted, found, err)
	}
	query := `SELECT status FROM timers WHERE timer_id = ?`
	if _, ok := selected.(*PostgresStore); ok {
		query = `SELECT status FROM timers WHERE timer_id = $1::uuid`
	}
	var workflowStatus string
	if err := db.QueryRowContext(ctx, query, workflow.timerID).Scan(&workflowStatus); err != nil || workflowStatus != "cancelled" {
		t.Fatalf("workflow timer status = %q err=%v", workflowStatus, err)
	}
}

func TestActiveRunQuiescenceCancelsExactGenericAndWorkflowTimerFamiliesOnBothStores(t *testing.T) {
	for _, tc := range selectedScheduleStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, db, seedCtx := tc.open(t)
			selected := store.(genericScheduleLifecycleConsumerStore)
			runID := runtimecorrelation.RunIDFromContext(seedCtx)
			ctx := authorGenericScheduleConsumerContext(runID)
			generic, workflow := seedGenericScheduleTimerFamilies(t, selected, db, ctx)
			result, err := selected.ApplyActiveRunQuiescence(ctx, runtimerunquiescence.Request{
				OperationName: "test_generic_schedule_family_quiescence",
				RequestedAt:   time.Now().UTC(),
				RunIDs:        []string{runID},
				ReasonCode:    runtimerunquiescence.ServeAbandonReasonCode,
				ControlledBy:  "test",
				DeliveryNote:  "generic schedule family proof",
			})
			if err != nil {
				t.Fatalf("apply active-run quiescence: %v", err)
			}
			if result.TimerCount != 2 {
				t.Fatalf("quiescence timer count = %d, want 2", result.TimerCount)
			}
			assertGenericScheduleTimerFamilyCancellation(t, selected, db, ctx, generic, workflow, result.TimerCancellations)
		})
	}
}

func TestRunControlStopCancelsExactGenericAndWorkflowTimerFamiliesOnBothStores(t *testing.T) {
	for _, tc := range selectedScheduleStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, db, seedCtx := tc.open(t)
			selected := store.(genericScheduleLifecycleConsumerStore)
			runID := runtimecorrelation.RunIDFromContext(seedCtx)
			ctx := authorGenericScheduleConsumerContext(runID)
			generic, workflow := seedGenericScheduleTimerFamilies(t, selected, db, ctx)
			state, err := selected.StopRunControl(ctx, runtimeruncontrol.TransitionRequest{
				RunID: runID, Reason: "test_stop", ControlledBy: "test", Now: time.Now().UTC(),
			})
			if err != nil {
				t.Fatalf("stop run control: %v", err)
			}
			assertGenericScheduleTimerFamilyCancellation(t, selected, db, ctx, generic, workflow, state.TimerCancellations)
		})
	}
}

type runControlTimerWorkflowModule struct {
	workflowTestModule
	source semanticview.Source
}

func (m runControlTimerWorkflowModule) SemanticSource() semanticview.Source { return m.source }

func runControlTimerBundle() *runtimecontracts.WorkflowContractBundle {
	return &runtimecontracts.WorkflowContractBundle{Semantics: runtimecontracts.WorkflowSemanticView{
		Name: "run-stop-timer-proof", Version: "1", InitialStage: "waiting", TerminalStages: []string{"done"},
		Timers: []runtimecontracts.WorkflowTimerContract{{
			ID: "waiting.timeout", Stage: "waiting", StageOwned: true, AdvancesTo: "done",
			Owner: "runtime", Event: runtimecontracts.WorkflowStageTimerInternalEvent,
			StartOn: "state:waiting", Delay: "1h",
		}},
	}}
}

func TestRunControlControllerStopReconcilesBothTimerFamiliesOnBothStores(t *testing.T) {
	for _, tc := range selectedScheduleStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, db, seedCtx := tc.open(t)
			selected := store.(genericScheduleLifecycleConsumerStore)
			runID := runtimecorrelation.RunIDFromContext(seedCtx)
			ctx := authorGenericScheduleConsumerContext(runID)
			entityID := uuid.NewString()

			process := worklifetime.NewProcess()
			workOwner, err := process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{
				RuntimeInstanceID: "run-stop-timer-proof", BundleHash: genericScheduleConsumerTestBundleHash,
			})
			if err != nil {
				t.Fatal(err)
			}
			scheduler := runtimepipeline.NewSchedulerWithWorkOwner(workOwner)
			planner := &terminalSchedulePlannerProbe{}
			dispatcher := &terminalScheduleDispatcherProbe{}
			genericStore := &transientGenericScheduleReleaseStore{
				Store: selected, failures: 1, releaseSeen: make(chan error, 2),
			}
			genericLifecycle, err := runtimegenericschedule.NewLifecycle(genericStore, scheduler, planner, dispatcher, nil)
			if err != nil {
				t.Fatal(err)
			}

			source := semanticview.Wrap(runControlTimerBundle())
			options := completeWorkflowTestCoordinatorOptions(runtimepipeline.NewWorkflowPersistence(selected.(workflowTestSelectedStore)), selected.(workflowTestSelectedStore))
			options.Module = runControlTimerWorkflowModule{source: source}
			options.TimerScheduler = scheduler
			options.GenericSchedules = genericLifecycle
			options.WorkOwner = workOwner
			coordinator := runtimepipeline.NewPipelineCoordinatorWithOptions(workflowTestBus{}, options)
			if coordinator == nil {
				t.Fatal("construct run-stop timer coordinator")
			}
			t.Cleanup(func() {
				stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = coordinator.StopWorkflowTimerLifecycle(stopCtx)
				_ = genericLifecycle.Stop(stopCtx)
				scheduler.Stop()
				_, _ = workOwner.RetireAndWait(stopCtx)
				_, _ = process.Join(stopCtx)
			})

			genericResult, err := genericLifecycle.Admit(ctx, testAgentGenericScheduleCommand(
				t, runID, "run-stop-agent", "run-stop/instance", entityID, "run-stop-generic",
				runtimegenericschedule.AbsoluteDue(time.Now().UTC().Add(time.Hour)),
			))
			if err != nil {
				t.Fatal(err)
			}
			enteredAt := time.Now().UTC()
			if _, err := coordinator.MaterializeInitialEntry(ctx, runtimepipeline.WorkflowInstance{
				InstanceID: runID, StorageRef: runID, WorkflowName: "run-stop-timer-proof", WorkflowVersion: "1",
				CurrentState: "waiting", EnteredStageAt: enteredAt, CreatedAt: enteredAt,
				Metadata: map[string]any{"run_id": runID, "entity_id": entityID, "flow_path": runID, "instance_id": runID},
			}, enteredAt); err != nil {
				t.Fatal(err)
			}
			if err := coordinator.ArmInitialEntryTimers(ctx, runtimeflowidentity.RouteForInstancePath(runID)); err != nil {
				t.Fatal(err)
			}

			controller := runtimeruncontrol.NewController(selected.(runtimeruncontrol.Store), nil, runtimeruncontrol.Options{
				TimerCancellations: coordinator.TimerCancellationReconciler(),
			})
			result, err := controller.Stop(ctx, runtimeruncontrol.TransitionRequest{
				RunID: runID, Reason: "supported_run_stop", ControlledBy: "test", Now: time.Now().UTC(),
			})
			if err != nil || result.Recovery.Disposition != runtimeruncontrol.RecoveryPending {
				t.Fatalf("run stop result=%#v err=%v", result, err)
			}
			for attempt := 0; attempt < 2; attempt++ {
				select {
				case releaseErr := <-genericStore.releaseSeen:
					if attempt == 0 && releaseErr == nil {
						t.Fatal("initial generic schedule release unexpectedly succeeded")
					}
					if attempt == 1 && releaseErr != nil {
						t.Fatalf("queued generic schedule release did not converge: %v", releaseErr)
					}
				case <-time.After(time.Second):
					t.Fatalf("generic schedule release attempt %d was not observed", attempt+1)
				}
			}

			quiesceCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := workOwner.WaitForQuiescence(quiesceCtx); err != nil {
				t.Fatalf("timer wakeups remained live after committed stop: %v", err)
			}
			generic, found, err := selected.LoadGenericScheduleActivation(ctx, genericResult.Activation.ID)
			if err != nil || !found || generic.Status != runtimegenericschedule.StatusCancelled {
				t.Fatalf("generic stop state=%#v found=%v err=%v", generic, found, err)
			}
			query := `SELECT COUNT(*) FROM timers WHERE run_id = ? AND task_type = 'workflow_timer' AND status = 'cancelled'`
			args := []any{runID}
			if _, postgres := selected.(*PostgresStore); postgres {
				query = `SELECT COUNT(*) FROM timers WHERE run_id = $1::uuid AND task_type = 'workflow_timer' AND status = 'cancelled'`
			}
			var cancelledWorkflowTimers int
			if err := db.QueryRowContext(ctx, query, args...).Scan(&cancelledWorkflowTimers); err != nil || cancelledWorkflowTimers != 1 {
				t.Fatalf("cancelled workflow timers=%d err=%v", cancelledWorkflowTimers, err)
			}
			if planner.prepareCalls != 0 || dispatcher.calls != 0 {
				t.Fatalf("stopped long-future timers emitted events: prepare=%d dispatch=%d", planner.prepareCalls, dispatcher.calls)
			}
			if _, postgres := selected.(*PostgresStore); postgres {
				assertAdvisoryLockAvailableOnPool(t, db, "swarm:generic_schedule:"+generic.ID)
			}
		})
	}
}

func assertAdvisoryLockAvailableOnPool(t *testing.T, db *sql.DB, lockKey string) {
	t.Helper()
	var acquired bool
	if err := db.QueryRow(`SELECT pg_try_advisory_lock(hashtext($1))`, lockKey).Scan(&acquired); err != nil {
		t.Fatal(err)
	}
	if !acquired {
		t.Fatalf("PostgreSQL advisory claim %q remained live", lockKey)
	}
	var released bool
	if err := db.QueryRow(`SELECT pg_advisory_unlock(hashtext($1))`, lockKey).Scan(&released); err != nil || !released {
		t.Fatalf("release proof advisory lock=%v err=%v", released, err)
	}
}
