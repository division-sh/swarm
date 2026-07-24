package store

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestSelectedContractWorkflowTimerForkRestoresAndExecutesThroughHandler(t *testing.T) {
	for _, test := range []struct {
		name                  string
		recurring             bool
		sourceAcceptedEarlier bool
		wantStatus            string
	}{
		{name: "one_shot", wantStatus: "fired"},
		{name: "recurring_after_accepted_source_occurrence", recurring: true, sourceAcceptedEarlier: true, wantStatus: "cancelled"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, db, _ := testutil.StartPostgres(t)
			pg := admitTestPostgresStore(t, db)
			ctx := testAuthorActivityContext()
			sourceRunID := uuid.NewString()
			entityID := runtimepipeline.FlowInstanceEntityID("flow-a/1")
			forkPointEventID := uuid.NewString()
			sourceTimerID := uuid.NewString()
			sourceRef := timeridentity.WorkflowTimerActivationRef{
				ActivationID: sourceTimerID, Declaration: "selected-timer",
			}
			forkAt := time.Now().UTC().Truncate(time.Microsecond)
			createdAt := forkAt
			fireAt := forkAt.Add(time.Second)
			var recurrenceInterval any
			var firedAt any
			taskType := "timer"
			if test.recurring {
				createdAt = forkAt.Add(-time.Second)
				recurrenceInterval = "1s"
				taskType = "scheduled_task"
				if test.sourceAcceptedEarlier {
					firedAt = forkAt
				}
			}
			seedSelectedContractExecutionStoreSourceUnpublished(t, db, sourceRunID, entityID, forkPointEventID, forkAt)
			if _, err := db.ExecContext(ctx, `
				INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
				VALUES ('flow-a/1', 'selected-workflow', 'static', '{"workflow_version":"v1"}'::jsonb, 'active', $1)
			`, forkAt); err != nil {
				t.Fatalf("seed selected workflow instance descriptor: %v", err)
			}
			if _, err := db.ExecContext(ctx, `
				INSERT INTO timers (
					timer_id, run_id, timer_name, entity_id, flow_instance, fire_event, fire_payload,
					fire_at, recurring, recurrence_interval, owner_agent, task_type, status, fired_at, created_at
				)
				VALUES (
					$1::uuid, $2::uuid, $3, $4::uuid, 'flow-a/1', $5, '{}'::jsonb,
					$6, $7, $8, 'runtime', $9, 'active', $10, $11
				)
			`, sourceTimerID, sourceRunID, sourceRef.TaskID(), entityID,
				runtimecontracts.WorkflowStageTimerInternalEvent, fireAt, test.recurring, recurrenceInterval, taskType, firedAt, createdAt); err != nil {
				t.Fatalf("seed source workflow timer: %v", err)
			}
			captureRunForkTestRevision(t, db, sourceRunID)

			materialized, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, RunForkSelectedContractExecutionMaterializeRequest{
				SourceRunID: sourceRunID,
				At:          forkPointEventID,
				ContractSelection: RunForkContractSelection{
					Mode: "selected_contracts", ContractsRoot: "/tmp/selected-contracts",
					WorkflowName: "selected-workflow", WorkflowVersion: "v1",
				},
			})
			if err != nil {
				t.Fatalf("MaterializeRunForkForSelectedContractExecution: %v", err)
			}
			seedSelectedContractExecutionForkLineage(t, pg, db, sourceRunID, materialized.ForkRunID, forkPointEventID, entityID, forkAt)
			activated, err := pg.ActivateRunForkForSelectedContractExecution(ctx, RunForkSelectedContractExecutionActivateRequest{
				ForkRunID: materialized.ForkRunID, ConfirmSourceFreeze: true,
				AllowedSourceEventIDs: []string{forkPointEventID},
			})
			if err != nil || !activated.Activated {
				t.Fatalf("ActivateRunForkForSelectedContractExecution activated=%v err=%v result=%#v", activated.Activated, err, activated)
			}

			forkRef, forkCreatedAt, forkFireAt := loadSelectedContractForkTimerCoordinates(t, db, materialized.ForkRunID)
			if forkRef.ActivationID == sourceRef.ActivationID || !forkCreatedAt.Equal(createdAt) || !forkFireAt.Equal(fireAt) {
				t.Fatalf("fork timer ref=%#v created=%s fire=%s, want reminted ref and source lattice %s/%s",
					forkRef, forkCreatedAt, forkFireAt, createdAt, fireAt)
			}

			source := semanticview.Wrap(selectedContractWorkflowTimerBundle(test.recurring))
			lease := registerSelectedContractWorkflowTimerEvent(t, pg, ctx)
			t.Cleanup(lease.Release)
			bus, err := newStoreTestEventBus(t, pg, runtimebus.EventBusOptions{ContractBundle: source})
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			forkCtx := runtimecorrelation.WithRunID(ctx, materialized.ForkRunID)
			forkScope, ok := runtimeauthoractivity.ScopeFromContext(forkCtx)
			if !ok {
				t.Fatal("fork timer proof requires author activity scope")
			}
			workflowStore := runtimepipeline.NewWorkflowInstanceStore(db)
			fireErrors := make(chan error, 4)
			var coordinator *runtimepipeline.PipelineCoordinator
			scheduler := runtimepipeline.NewSchedulerWithWorkOwner(storeTestWorkOwner(t), func(taskCtx context.Context, schedule runtimepipeline.Schedule) {
				if coordinator == nil {
					fireErrors <- fmt.Errorf("workflow timer coordinator is unavailable")
					return
				}
				fireCtx := runtimeauthoractivity.WithScope(taskCtx, forkScope)
				fireCtx = runtimecorrelation.WithRunID(fireCtx, materialized.ForkRunID)
				if _, err := coordinator.FireWorkflowTimer(fireCtx, schedule); err != nil {
					fireErrors <- err
				}
			})
			coordinator = runtimepipeline.NewPipelineCoordinatorWithOptions(bus, db, runtimepipeline.PipelineCoordinatorOptions{
				Module: selectedContractWorkflowTimerModule{source: source}, WorkflowStore: workflowStore,
				TimerScheduler: scheduler, TimerScheduleStore: pg,
			})
			interceptor := &selectedContractWorkflowTimerInterceptor{
				delegate: coordinator, store: workflowStore, results: make(chan selectedContractWorkflowTimerInterceptResult, 2),
			}
			bus.SetInterceptors(interceptor)
			t.Cleanup(func() {
				stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				_ = coordinator.StopWorkflowTimerLifecycle(stopCtx)
				scheduler.Stop()
				_ = scheduler.Wait(stopCtx)
			})
			if err := coordinator.RestoreWorkflowTimers(forkCtx); err != nil {
				t.Fatalf("RestoreWorkflowTimers: %v", err)
			}

			deadline := time.Now().Add(5 * time.Second)
			var interceptResult selectedContractWorkflowTimerInterceptResult
			for time.Now().Before(deadline) {
				select {
				case err := <-fireErrors:
					t.Fatalf("forked workflow timer callback: %v", err)
				default:
				}
				select {
				case interceptResult = <-interceptor.results:
				default:
				}
				instance, found, err := workflowStore.Load(forkCtx, entityID)
				if err != nil {
					t.Fatalf("load fork workflow state: %v", err)
				}
				if found && instance.CurrentState == "done" {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			instance, found, err := workflowStore.Load(forkCtx, entityID)
			if err != nil || !found || instance.CurrentState != "done" {
				outcome, failure := selectedContractForkTimerReceipt(t, db, forkRef, forkFireAt)
				t.Fatalf("fork workflow handler state found=%v state=%q err=%v receipt=%q failure=%s intercept=%#v, want done",
					found, instance.CurrentState, err, outcome, failure, interceptResult)
			}
			assertSelectedContractForkTimerOutcome(t, db, materialized.ForkRunID, forkRef, forkFireAt, test.wantStatus)
		})
	}
}

type selectedContractWorkflowTimerInterceptResult struct {
	EventID       string
	RunID         string
	State         string
	TimerStage    string
	AdvancesTo    string
	StageOwned    bool
	GenerationSet bool
	Found         bool
	Err           error
}

type selectedContractWorkflowTimerInterceptor struct {
	delegate *runtimepipeline.PipelineCoordinator
	store    *runtimepipeline.WorkflowInstanceStore
	results  chan selectedContractWorkflowTimerInterceptResult
}

func (i *selectedContractWorkflowTimerInterceptor) Intercept(ctx context.Context, evt events.Event) (bool, []events.Event, runtimepipelineobligation.ExecutionOutcome, error) {
	pass, out, outcome, err := i.delegate.Intercept(ctx, evt)
	result := selectedContractWorkflowTimerInterceptResult{
		EventID: evt.ID(), RunID: runtimecorrelation.RunIDFromContext(ctx), Err: err,
	}
	if occurrence, ok := timeridentity.ParseWorkflowTimerOccurrenceTaskID(evt.TaskID()); ok {
		result.GenerationSet = occurrence.Activation.Generation.Valid()
		if timer, found := i.delegate.SemanticSource().WorkflowTimerByID(occurrence.Activation.Declaration); found {
			result.TimerStage = timer.Stage
			result.AdvancesTo = timer.AdvancesTo
			result.StageOwned = timer.StageOwned
		}
	}
	if i.store != nil {
		instance, found, loadErr := i.store.Load(ctx, evt.EntityID())
		result.Found = found
		result.State = instance.CurrentState
		if result.Err == nil {
			result.Err = loadErr
		}
	}
	select {
	case i.results <- result:
	default:
	}
	return pass, out, outcome, err
}

func selectedContractForkTimerReceipt(
	t *testing.T,
	db *sql.DB,
	ref timeridentity.WorkflowTimerActivationRef,
	dueAt time.Time,
) (string, string) {
	t.Helper()
	eventID := timeridentity.WorkflowTimerOccurrenceEventID(timeridentity.WorkflowTimerOccurrenceRef{
		Activation: ref, DueAt: dueAt,
	})
	var outcome, failure string
	err := db.QueryRowContext(testAuthorActivityContext(), `
		SELECT outcome, COALESCE(failure::text, '')
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'
	`, eventID).Scan(&outcome, &failure)
	if err == sql.ErrNoRows {
		return "missing", ""
	}
	if err != nil {
		t.Fatalf("load fork workflow timer receipt: %v", err)
	}
	return outcome, failure
}

type selectedContractWorkflowTimerModule struct {
	source semanticview.Source
}

func (m selectedContractWorkflowTimerModule) SemanticSource() semanticview.Source { return m.source }
func (selectedContractWorkflowTimerModule) WorkflowDefinition() *runtimepipeline.WorkflowDefinition {
	return nil
}
func (selectedContractWorkflowTimerModule) WorkflowNodes() []runtimepipeline.WorkflowNode { return nil }
func (selectedContractWorkflowTimerModule) GuardRegistry() runtimepipeline.GuardRegistry  { return nil }
func (selectedContractWorkflowTimerModule) ActionRegistry() runtimepipeline.ActionRegistry {
	return nil
}

func selectedContractWorkflowTimerBundle(recurring bool) *runtimecontracts.WorkflowContractBundle {
	return &runtimecontracts.WorkflowContractBundle{Semantics: runtimecontracts.WorkflowSemanticView{
		Name: "selected-workflow", Version: "v1", InitialStage: "pending", TerminalStages: []string{"done"},
		Timers: []runtimecontracts.WorkflowTimerContract{{
			ID: "selected-timer", Stage: "pending", StageOwned: true, AdvancesTo: "done",
			Owner: "runtime", Event: runtimecontracts.WorkflowStageTimerInternalEvent,
			StartOn: "state:pending", Delay: "1s", Recurring: recurring,
		}},
	}}
}

func registerSelectedContractWorkflowTimerEvent(t *testing.T, pg *PostgresStore, ctx context.Context) *runtimeauthoractivity.EventCatalogLease {
	t.Helper()
	scope, ok := runtimeauthoractivity.ScopeFromContext(ctx)
	if !ok {
		t.Fatal("author activity scope is unavailable")
	}
	lease, err := pg.RegisterAuthorActivityEventCatalog(scope, []runtimeauthoractivity.EventDescriptor{{
		EventType: runtimecontracts.WorkflowStageTimerInternalEvent, Disposition: runtimeauthoractivity.StoryDifferent,
	}})
	if err != nil {
		t.Fatalf("register workflow timer event catalog: %v", err)
	}
	return lease
}

func loadSelectedContractForkTimerCoordinates(t *testing.T, db *sql.DB, runID string) (timeridentity.WorkflowTimerActivationRef, time.Time, time.Time) {
	t.Helper()
	var timerID, timerName string
	var createdAt, fireAt time.Time
	if err := db.QueryRowContext(testAuthorActivityContext(), `
		SELECT timer_id::text, timer_name, created_at, fire_at
		FROM timers
		WHERE run_id = $1::uuid AND source_timer_id IS NOT NULL AND status = 'active'
	`, runID).Scan(&timerID, &timerName, &createdAt, &fireAt); err != nil {
		t.Fatalf("load fork workflow timer: %v", err)
	}
	ref, ok := timeridentity.ParseWorkflowTimerActivationTaskID(timerName)
	if !ok || ref.ActivationID != timerID {
		t.Fatalf("fork workflow timer identity = id:%q task:%q ref:%#v", timerID, timerName, ref)
	}
	return ref, createdAt.UTC(), fireAt.UTC()
}

func assertSelectedContractForkTimerOutcome(
	t *testing.T,
	db *sql.DB,
	runID string,
	ref timeridentity.WorkflowTimerActivationRef,
	dueAt time.Time,
	wantStatus string,
) {
	t.Helper()
	var status string
	var firedAt sql.NullTime
	if err := db.QueryRowContext(testAuthorActivityContext(), `
		SELECT status, fired_at FROM timers WHERE run_id = $1::uuid AND timer_id = $2::uuid
	`, runID, ref.ActivationID).Scan(&status, &firedAt); err != nil {
		t.Fatalf("load fork timer outcome: %v", err)
	}
	if status != wantStatus || !firedAt.Valid {
		t.Fatalf("fork timer outcome status=%q fired_at=%v, want %q/valid", status, firedAt, wantStatus)
	}
	wantEventID := timeridentity.WorkflowTimerOccurrenceEventID(timeridentity.WorkflowTimerOccurrenceRef{
		Activation: ref, DueAt: dueAt,
	})
	var count int
	if err := db.QueryRowContext(testAuthorActivityContext(), `
		SELECT COUNT(*) FROM events
		WHERE run_id = $1::uuid AND event_id = $2::uuid AND event_name = $3
	`, runID, wantEventID, runtimecontracts.WorkflowStageTimerInternalEvent).Scan(&count); err != nil {
		t.Fatalf("count fork workflow timer event: %v", err)
	}
	if count != 1 {
		t.Fatalf("fork workflow timer event count = %d, want 1", count)
	}
}

var _ runtimepipeline.WorkflowModule = selectedContractWorkflowTimerModule{}
