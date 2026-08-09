package runtime_test

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	swarmruntime "github.com/division-sh/swarm/internal/runtime"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	llm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimetimerobligation "github.com/division-sh/swarm/internal/runtime/timerobligation"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	runlifecyclefixture "github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/google/uuid"
)

type workflowTimerStartupStore interface {
	externalRuntimeTestDurableEventStore
	runtimepipeline.WorkflowPersistenceOwner
	swarmruntime.EventPayloadValidationBinder
	swarmruntime.AuthorActivityCatalogRegistrar
	runtimerunlifecycle.CandidateOwner
	runtimegenericschedule.Store
	runtimemanager.AgentLifecyclePersistence
	runtimemanager.ManagerPersistence
	runtimetimerobligation.Reader
	PipelineObligations() runtimepipelineobligation.Store
}

type workflowTimerStartupFlakyManagerStore struct {
	workflowTimerStartupStore

	mu     sync.Mutex
	loads  int
	failAt int
}

func (s *workflowTimerStartupFlakyManagerStore) LoadAgents(ctx context.Context) ([]runtimemanager.PersistedAgent, error) {
	s.mu.Lock()
	s.loads++
	load := s.loads
	s.mu.Unlock()
	if load == s.failAt {
		return nil, errors.New("transient manager hydration failure")
	}
	return s.workflowTimerStartupStore.LoadAgents(ctx)
}

func TestGenericScheduleLifecyclePublishesOneShotAndRecurringThroughWorkflowRuntimeOnBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) (*sql.DB, workflowTimerStartupStore, bool)
	}{
		{
			name: "sqlite",
			open: func(t *testing.T) (*sql.DB, workflowTimerStartupStore, bool) {
				selected := storetest.StartSQLiteRuntimeStore(t)
				return storetest.Database(selected), selected, false
			},
		},
		{
			name: "postgres",
			open: func(t *testing.T) (*sql.DB, workflowTimerStartupStore, bool) {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				selected := storetest.AdmitPostgresRuntimeStore(t, db)
				return db, selected, true
			},
		},
	} {
		t.Run(backend.name, func(t *testing.T) {
			db, selected, postgres := backend.open(t)
			workflowPersistence := runtimepipeline.NewWorkflowPersistence(selected)
			if !postgres {
				workflowPersistence = runtimepipeline.NewWorkflowPersistence(selected)
			}
			runID := uuid.NewString()
			entityID := uuid.NewString()
			ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
			if postgres {
				runlifecyclefixture.RequirePostgres(t, ctx, db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runID, Source: authorActivityTestBundleSourceFact})
			} else {
				runlifecyclefixture.RequireSQLite(t, ctx, db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runID, Source: authorActivityTestBundleSourceFact})
			}

			process := worklifetime.NewProcess()
			source := semanticview.Wrap(workflowTimerStartupRecoveryBundle())
			rt, err := swarmruntime.NewRuntime(ctx, completeExternalRuntimeTestWorkflowDeps(t, selected, swarmruntime.RuntimeDeps{
				Config: &config.Config{
					Runtime: config.RuntimeConfig{RecoveryOnStartup: true},
					LLM:     config.LLMConfig{Backend: "anthropic"},
				},

				EventStore:                   selected,
				EventBusDurable:              externalRuntimeTestDurableDependencies(selected),
				EventPayloadValidationBinder: selected,
				AuthorActivityRegistrars:     []swarmruntime.AuthorActivityCatalogRegistrar{selected},
				RunLifecycleCandidates:       selected,
				GenericScheduleStore:         selected,
				TimerObligationReader:        selected,
				WorkflowPersistence:          workflowPersistence,
				ManagerStore:                 selected,
				ManagerLifecycleStore:        selected,
				ManagerPersistenceRoles:      externalRuntimeTestSelectedManagerRoles(selected),
				DeliveryStore:                selected,
				PipelineObligations:          selected.PipelineObligations(),

				Options: swarmruntime.RuntimeOptions{
					SelfCheck:         false,
					WorkflowModule:    newRuntimeTestWorkflowModule(t, source),
					LLMRuntime:        workflowTimerStartupLLM{},
					RuntimeInstanceID: authorActivityTestRuntimeInstanceID,
					BundleSourceFact:  authorActivityTestBundleSourceFact,
					ProcessWorkOwner:  process,
				},
			}))
			if err != nil {
				t.Fatalf("NewRuntime: %v", err)
			}
			t.Cleanup(func() {
				if err := rt.Shutdown(); err != nil {
					t.Errorf("shutdown runtime: %v", err)
				}
				joinCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if _, err := process.Join(joinCtx); err != nil {
					t.Errorf("join process owner: %v", err)
				}
			})
			if err := rt.Start(ctx); err != nil {
				t.Fatalf("Start runtime: %v", err)
			}

			dueAt := time.Now().UTC().Add(50 * time.Millisecond).Truncate(time.Microsecond)
			routingSource, err := events.NewRootRoutingSource(entityID)
			if err != nil {
				t.Fatalf("build generic schedule routing source: %v", err)
			}
			command := runtimegenericschedule.AdmissionCommand{
				ScheduleKey:   "generic-occurrence-proof",
				RunID:         runID,
				OwnerID:       "runtime",
				OwnerKind:     runtimegenericschedule.OwnerAgent,
				AgentIdentity: agentidentitytest.RootRuntime(t, "runtime", "generic-occurrence-proof"),
				EventType:     "generic.tick",
				EntityID:      entityID,
				TaskID:        "generic-occurrence-proof",
				Payload:       semanticvalue.EmptyObject(),
				RoutingSource: routingSource,
				Due:           runtimegenericschedule.AbsoluteDue(dueAt),
			}
			admitted, err := rt.GenericSchedules.Admit(ctx, command)
			if err != nil {
				t.Fatalf("admit generic occurrence-shaped schedule: %v", err)
			}

			deadline := time.Now().Add(5 * time.Second)
			for {
				var eventCount int
				query := `SELECT COUNT(*) FROM events WHERE task_id = ? AND produced_by = ? AND produced_by_type = 'platform'`
				args := []any{command.TaskID, runtimegenericschedule.OccurrenceProducerID()}
				if postgres {
					query = `SELECT COUNT(*) FROM events WHERE task_id = $1 AND produced_by = $2 AND produced_by_type = 'platform'`
				}
				if err := db.QueryRowContext(ctx, query, args...).Scan(&eventCount); err != nil {
					t.Fatalf("read generic scheduled event: %v", err)
				}
				activation, found, err := selected.LoadGenericScheduleActivation(ctx, admitted.Activation.ID)
				if err != nil {
					t.Fatalf("load generic schedule activation: %v", err)
				}
				if eventCount == 1 && found && activation.Status == runtimegenericschedule.StatusFired {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("generic occurrence-shaped fire did not publish and settle: events=%d activation=%#v found=%v", eventCount, activation, found)
				}
				time.Sleep(20 * time.Millisecond)
			}

			recurringCommand := command
			recurringCommand.ScheduleKey = "generic-recurring-proof"
			recurringCommand.TaskID = recurringCommand.ScheduleKey
			recurringCommand.Due = runtimegenericschedule.EveryDue(40 * time.Millisecond)
			recurring, err := rt.GenericSchedules.Admit(ctx, recurringCommand)
			if err != nil {
				t.Fatalf("admit recurring generic schedule: %v", err)
			}

			countEvents := func() int {
				t.Helper()
				query := `SELECT COUNT(DISTINCT event_id) FROM events WHERE task_id = ? AND produced_by = ? AND produced_by_type = 'platform'`
				args := []any{recurringCommand.TaskID, runtimegenericschedule.OccurrenceProducerID()}
				if postgres {
					query = `SELECT COUNT(DISTINCT event_id) FROM events WHERE task_id = $1 AND produced_by = $2 AND produced_by_type = 'platform'`
				}
				var count int
				if err := db.QueryRowContext(ctx, query, args...).Scan(&count); err != nil {
					t.Fatalf("read recurring generic schedule events: %v", err)
				}
				return count
			}

			deadline = time.Now().Add(5 * time.Second)
			for {
				activation, found, err := selected.LoadGenericScheduleActivation(ctx, recurring.Activation.ID)
				if err != nil {
					t.Fatalf("load recurring generic schedule activation: %v", err)
				}
				if countEvents() >= 2 && found && activation.Status == runtimegenericschedule.StatusActive && activation.CurrentDueAt.After(activation.InitialDueAt) {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("recurring generic schedule did not advance through exact occurrences: activation=%#v found=%v", activation, found)
				}
				time.Sleep(20 * time.Millisecond)
			}

			cancelled, err := rt.GenericSchedules.Cancel(ctx, runtimegenericschedule.CancelCommand{
				ActivationID: recurring.Activation.ID,
				Cause:        "test_completed",
				CancelledAt:  time.Now().UTC(),
			})
			if err != nil || cancelled.Outcome != runtimegenericschedule.CancelChanged {
				t.Fatalf("cancel recurring generic schedule: result=%#v err=%v", cancelled, err)
			}
			settledCount := countEvents()
			time.Sleep(120 * time.Millisecond)
			if after := countEvents(); after != settledCount {
				t.Fatalf("recurring generic schedule published after cancellation: before=%d after=%d", settledCount, after)
			}
		})
	}
}

func TestRuntimeStartFailsClosedWhenManagerHydrationWouldWithholdWorkflowTimersOnBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) (*sql.DB, workflowTimerStartupStore, bool)
	}{
		{
			name: "sqlite",
			open: func(t *testing.T) (*sql.DB, workflowTimerStartupStore, bool) {
				selected := storetest.StartSQLiteRuntimeStore(t)
				return storetest.Database(selected), selected, false
			},
		},
		{
			name: "postgres",
			open: func(t *testing.T) (*sql.DB, workflowTimerStartupStore, bool) {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				selected := storetest.AdmitPostgresRuntimeStore(t, db)
				return db, selected, true
			},
		},
	} {
		t.Run(backend.name, func(t *testing.T) {
			db, selected, postgres := backend.open(t)
			workflowPersistence := runtimepipeline.NewWorkflowPersistence(selected)
			if !postgres {
				workflowPersistence = runtimepipeline.NewWorkflowPersistence(selected)
			}
			runID := uuid.NewString()
			entityID := uuid.NewString()
			ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
			if postgres {
				runlifecyclefixture.RequirePostgres(t, ctx, db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runID, Source: authorActivityTestBundleSourceFact})
			} else {
				runlifecyclefixture.RequireSQLite(t, ctx, db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runID, Source: authorActivityTestBundleSourceFact})
			}

			source := semanticview.Wrap(workflowTimerStartupRecoveryBundle())
			module := newRuntimeTestWorkflowModule(t, source)
			newRuntime := func(managerStore runtimemanager.ManagerPersistence) (*swarmruntime.Runtime, *worklifetime.Process) {
				process := worklifetime.NewProcess()
				rt, err := swarmruntime.NewRuntime(ctx, completeExternalRuntimeTestWorkflowDeps(t, selected, swarmruntime.RuntimeDeps{
					Config: &config.Config{
						Runtime: config.RuntimeConfig{RecoveryOnStartup: true},
						LLM:     config.LLMConfig{Backend: "anthropic"},
					},

					EventStore:                   selected,
					EventBusDurable:              externalRuntimeTestDurableDependencies(selected),
					EventPayloadValidationBinder: selected,
					AuthorActivityRegistrars:     []swarmruntime.AuthorActivityCatalogRegistrar{selected},
					RunLifecycleCandidates:       selected,
					WorkflowPersistence:          workflowPersistence,
					ManagerStore:                 managerStore,
					ManagerLifecycleStore:        selected,
					ManagerPersistenceRoles:      externalRuntimeTestSelectedManagerRoles(selected),
					TimerObligationReader:        selected,
					DeliveryStore:                selected,
					PipelineObligations:          selected.PipelineObligations(),

					Options: swarmruntime.RuntimeOptions{
						SelfCheck:         false,
						WorkflowModule:    module,
						LLMRuntime:        workflowTimerStartupLLM{},
						RuntimeInstanceID: authorActivityTestRuntimeInstanceID,
						BundleSourceFact:  authorActivityTestBundleSourceFact,
						ProcessWorkOwner:  process,
					},
				}))
				if err != nil {
					t.Fatalf("NewRuntime: %v", err)
				}
				return rt, process
			}
			shutdown := func(label string, rt *swarmruntime.Runtime, process *worklifetime.Process) {
				t.Helper()
				if err := rt.Shutdown(); err != nil {
					t.Fatalf("shutdown %s runtime: %v", label, err)
				}
				joinCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if _, err := process.Join(joinCtx); err != nil {
					t.Fatalf("join %s process owner: %v", label, err)
				}
			}

			seedRuntime, seedProcess := newRuntime(selected)
			seedCtx := testLiveExecutionContext(worklifetime.WithRuntimeOccurrence(ctx, seedRuntime.WorkOccurrence()))
			result, err := seedRuntime.Pipeline.MaterializeInitialEntry(seedCtx, runtimepipeline.WorkflowInstance{
				InstanceID:      "workflow-timer-startup",
				StorageRef:      "workflow-timer-startup",
				WorkflowName:    "workflow-timer-startup",
				WorkflowVersion: "1",
				CurrentState:    "waiting",
				Metadata:        map[string]any{"run_id": runID, "entity_id": entityID},
			}, time.Now().UTC())
			if err != nil {
				t.Fatalf("materialize workflow timer before restart: %v", err)
			}
			if result != runtimepipeline.WorkflowInitialMaterializationCreated {
				t.Fatalf("initial materialization result = %v, want created", result)
			}
			shutdown("seed", seedRuntime, seedProcess)

			flakyManagerStore := &workflowTimerStartupFlakyManagerStore{
				workflowTimerStartupStore: selected,
				failAt:                    2,
			}
			restarted, restartedProcess := newRuntime(flakyManagerStore)
			err = restarted.Start(ctx)
			if err == nil || !strings.Contains(err.Error(), "hydrate manager before workflow timer restoration") {
				shutdown("unexpected successful restart", restarted, restartedProcess)
				t.Fatalf("Start error = %v, want workflow-timer hydration gate failure", err)
			}
			shutdown("failed restart", restarted, restartedProcess)

			instance, found, err := restarted.Pipeline.Load(ctx, runtimeflowidentity.RouteForInstancePath("workflow-timer-startup"))
			if err != nil {
				t.Fatalf("load workflow instance after failed restart: %v", err)
			}
			if !found || instance.CurrentState != "waiting" {
				t.Fatalf("workflow instance after failed restart = found:%v state:%q, want durable waiting state", found, instance.CurrentState)
			}
		})
	}
}

func TestRuntimeStartRestoresWorkflowTimersWithoutGenericScheduleStoreOnBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) (*sql.DB, workflowTimerStartupStore, bool)
	}{
		{
			name: "sqlite",
			open: func(t *testing.T) (*sql.DB, workflowTimerStartupStore, bool) {
				selected := storetest.StartSQLiteRuntimeStore(t)
				return storetest.Database(selected), selected, false
			},
		},
		{
			name: "postgres",
			open: func(t *testing.T) (*sql.DB, workflowTimerStartupStore, bool) {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				selected := storetest.AdmitPostgresRuntimeStore(t, db)
				return db, selected, true
			},
		},
	} {
		t.Run(backend.name, func(t *testing.T) {
			db, selected, postgres := backend.open(t)
			workflowPersistence := runtimepipeline.NewWorkflowPersistence(selected)
			if !postgres {
				workflowPersistence = runtimepipeline.NewWorkflowPersistence(selected)
			}
			runID := uuid.NewString()
			entityID := uuid.NewString()
			ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
			if postgres {
				runlifecyclefixture.RequirePostgres(t, ctx, db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runID, Source: authorActivityTestBundleSourceFact})
			} else {
				runlifecyclefixture.RequireSQLite(t, ctx, db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runID, Source: authorActivityTestBundleSourceFact})
			}

			source := semanticview.Wrap(workflowTimerStartupRecoveryBundle())
			module := newRuntimeTestWorkflowModule(t, source)
			bootProgress := make([]swarmruntime.BootProgressEvent, 0, swarmruntime.BootProgressTotalSteps)
			newRuntime := func() (*swarmruntime.Runtime, *worklifetime.Process) {
				process := worklifetime.NewProcess()
				rt, err := swarmruntime.NewRuntime(ctx, completeExternalRuntimeTestWorkflowDeps(t, selected, swarmruntime.RuntimeDeps{
					Config: &config.Config{
						Runtime: config.RuntimeConfig{RecoveryOnStartup: true},
						LLM:     config.LLMConfig{Backend: "anthropic"},
					},

					EventStore:                   selected,
					EventBusDurable:              externalRuntimeTestDurableDependencies(selected),
					EventPayloadValidationBinder: selected,
					AuthorActivityRegistrars:     []swarmruntime.AuthorActivityCatalogRegistrar{selected},
					RunLifecycleCandidates:       selected,
					WorkflowPersistence:          workflowPersistence,
					ManagerStore:                 selected,
					ManagerLifecycleStore:        selected,
					ManagerPersistenceRoles:      externalRuntimeTestSelectedManagerRoles(selected),
					TimerObligationReader:        selected,
					DeliveryStore:                selected,
					PipelineObligations:          selected.PipelineObligations(),

					Options: swarmruntime.RuntimeOptions{
						SelfCheck:         false,
						WorkflowModule:    module,
						LLMRuntime:        workflowTimerStartupLLM{},
						RuntimeInstanceID: authorActivityTestRuntimeInstanceID,
						BundleSourceFact:  authorActivityTestBundleSourceFact,
						ProcessWorkOwner:  process,
						BootProgress: func(event swarmruntime.BootProgressEvent) {
							bootProgress = append(bootProgress, event)
						},
					},
				}))
				if err != nil {
					t.Fatalf("NewRuntime: %v", err)
				}
				return rt, process
			}
			shutdown := func(label string, rt *swarmruntime.Runtime, process *worklifetime.Process) {
				t.Helper()
				if err := rt.Shutdown(); err != nil {
					t.Fatalf("shutdown %s runtime: %v", label, err)
				}
				joinCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if _, err := process.Join(joinCtx); err != nil {
					t.Fatalf("join %s process owner: %v", label, err)
				}
			}

			seedRuntime, seedProcess := newRuntime()
			occurredAt := time.Now().UTC().Add(-time.Second)
			seedCtx := testLiveExecutionContext(worklifetime.WithRuntimeOccurrence(ctx, seedRuntime.WorkOccurrence()))
			result, err := seedRuntime.Pipeline.MaterializeInitialEntry(seedCtx, runtimepipeline.WorkflowInstance{
				InstanceID:      "workflow-timer-startup",
				StorageRef:      "workflow-timer-startup",
				WorkflowName:    "workflow-timer-startup",
				WorkflowVersion: "1",
				CurrentState:    "waiting",
				Metadata:        map[string]any{"run_id": runID, "entity_id": entityID},
			}, occurredAt)
			if err != nil {
				t.Fatalf("materialize workflow timer before restart: %v", err)
			}
			if result != runtimepipeline.WorkflowInitialMaterializationCreated {
				t.Fatalf("initial materialization result = %v, want created", result)
			}
			shutdown("seed", seedRuntime, seedProcess)

			restarted, restartedProcess := newRuntime()
			if err := restarted.Start(ctx); err != nil {
				shutdown("failed restart", restarted, restartedProcess)
				t.Fatalf("Start restarted runtime: %v", err)
			}
			defer shutdown("restarted", restarted, restartedProcess)
			assertBootDetail := func(name, fragment string) {
				t.Helper()
				for _, event := range bootProgress {
					if event.Name == name {
						if !strings.Contains(event.Detail, fragment) {
							t.Fatalf("%s detail = %q, want %q", name, event.Detail, fragment)
						}
						return
					}
				}
				t.Fatalf("boot progress omitted %s", name)
			}
			assertBootDetail("recovery_snapshot_inspection", "timer obligations")
			assertBootDetail("schedule_restoration", "workflow timers restored")
			var managerRecoveryIndex, scheduleRestoreIndex = -1, -1
			for i, event := range bootProgress {
				switch event.Name {
				case "manager_recovery_if_enabled":
					managerRecoveryIndex = i
				case "schedule_restoration":
					scheduleRestoreIndex = i
				}
			}
			if managerRecoveryIndex < 0 || scheduleRestoreIndex < 0 || managerRecoveryIndex >= scheduleRestoreIndex {
				t.Fatalf("boot order manager=%d schedule=%d, want manager recovery before runnable timer restoration", managerRecoveryIndex, scheduleRestoreIndex)
			}

			deadline := time.Now().Add(8 * time.Second)
			for {
				instance, found, err := restarted.Pipeline.Load(ctx, runtimeflowidentity.RouteForInstancePath("workflow-timer-startup"))
				if err != nil {
					t.Fatalf("load restored workflow instance: %v", err)
				}
				if found && instance.CurrentState == "done" {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("workflow timer was not restored and fired; found=%v state=%q", found, instance.CurrentState)
				}
				time.Sleep(20 * time.Millisecond)
			}
		})
	}
}

type workflowTimerStartupLLM struct{}

func (workflowTimerStartupLLM) StartSession(context.Context, string, string, []llm.ToolDefinition) (*llm.Session, error) {
	return &llm.Session{}, nil
}

func (workflowTimerStartupLLM) ContinueSession(context.Context, *llm.Session, llm.Message) (*llm.Response, error) {
	return &llm.Response{}, nil
}

func workflowTimerStartupRecoveryBundle() *runtimecontracts.WorkflowContractBundle {
	bundle := &runtimecontracts.WorkflowContractBundle{Semantics: runtimecontracts.WorkflowSemanticView{
		Name: "workflow-timer-startup", Version: "1", InitialStage: "waiting",
		Stages: []runtimecontracts.WorkflowStageContract{{ID: "waiting"}, {ID: "done"}}, TerminalStages: []string{"done"},
		Timers: []runtimecontracts.WorkflowTimerContract{{
			ID: "waiting.timeout", Stage: "waiting", StageOwned: true, AdvancesTo: "done",
			Owner: "runtime", Event: runtimecontracts.WorkflowStageTimerInternalEvent,
			StartOn: "state:waiting", Delay: "25ms",
		}},
	}, Events: map[string]runtimecontracts.EventCatalogEntry{"generic.tick": {}}}
	bundle.Platform.Platform.Name = "swarm"
	bundle.Platform.Platform.Version = "0.7.0"
	return bundle
}
