package runtimepersistence

import (
	"context"
	"database/sql"
	"testing"
	"time"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimeruncontrol "github.com/division-sh/swarm/internal/runtime/runcontrol"
	runtimerunquiescence "github.com/division-sh/swarm/internal/runtime/runquiescence"
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

func (r *recordingGenericScheduleReconciler) ReconcileWakeup(_ context.Context, activationID string) error {
	r.activationIDs = append(r.activationIDs, activationID)
	return nil
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
	return runtimecorrelation.WithRunID(testAuthorActivityContextForBundle(genericScheduleConsumerTestBundleHash), runID)
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
