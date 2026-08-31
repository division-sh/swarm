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
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	llm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	runtimetimerobligation "github.com/division-sh/swarm/internal/runtime/timerobligation"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	runlifecyclefixture "github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/google/uuid"
)

type workflowTimerStartupStore interface {
	externalRuntimeTestDurableEventStore
	runtimepipeline.WorkflowPersistenceOwner
	swarmruntime.EventPayloadAdmissionBinder
	swarmruntime.AuthorActivityCatalogRegistrar
	runtimerunlifecycle.CandidateOwner
	runtimegenericschedule.Store
	storetest.AgentFixtureStore
	runtimemanager.ManagerPersistence
	runtimemanager.AgentLifecycleCellCensus
	runtimetimerobligation.Reader
	PipelineObligations() runtimepipelineobligation.Store
}

type workflowTimerStartupFlakyManagerStore struct {
	workflowTimerStartupStore

	mu     sync.Mutex
	loads  int
	failAt int
}

type workflowTimerStartupTopologyFailureOwner struct {
	runtimepipeline.WorkflowPersistenceOwner
	readiness runtimepipeline.DynamicFlowRuntimeReadiness
}

func (o workflowTimerStartupTopologyFailureOwner) InspectDynamicFlowRuntimeReadinessForSource(_ context.Context, source runtimecorrelation.BundleSourceFact) (runtimepipeline.DynamicFlowRuntimeReadinessProjection, error) {
	if !o.readiness.OwningRunSource.Matches(source) {
		return runtimepipeline.DynamicFlowRuntimeReadinessProjection{}, nil
	}
	return runtimepipeline.DynamicFlowRuntimeReadinessProjection{
		CurrentPending: []runtimepipeline.DynamicFlowRuntimeReadiness{o.readiness},
	}, nil
}

func (o workflowTimerStartupTopologyFailureOwner) InspectDynamicFlowRuntimeReadinessForRun(_ context.Context, runID string, source runtimecorrelation.BundleSourceFact) ([]runtimepipeline.DynamicFlowRuntimeReadiness, error) {
	if strings.TrimSpace(runID) != o.readiness.Plan.RunID || !o.readiness.OwningRunSource.Matches(source) {
		return nil, nil
	}
	return []runtimepipeline.DynamicFlowRuntimeReadiness{o.readiness}, nil
}

func (o workflowTimerStartupTopologyFailureOwner) LoadDynamicFlowRuntimeReadiness(_ context.Context, runID string, route runtimeflowidentity.Route) (runtimepipeline.DynamicFlowRuntimeReadiness, bool, error) {
	if strings.TrimSpace(runID) != o.readiness.Plan.RunID || route.InstancePath != o.readiness.InstancePath {
		return runtimepipeline.DynamicFlowRuntimeReadiness{}, false, nil
	}
	return o.readiness, true, nil
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
			source := semanticview.Wrap(workflowTimerStartupRecoveryBundle(t))
			rt, err := swarmruntime.NewRuntime(ctx, completeExternalRuntimeTestWorkflowDeps(t, selected, swarmruntime.RuntimeDeps{
				Config: &config.Config{
					Runtime: config.RuntimeConfig{RecoveryOnStartup: true},
					LLM:     config.LLMConfig{Backend: "anthropic"},
				},

				EventStore:                  selected,
				EventBusDurable:             externalRuntimeTestDurableDependencies(selected),
				EventPayloadAdmissionBinder: selected,
				AuthorActivityRegistrars:    []swarmruntime.AuthorActivityCatalogRegistrar{selected},
				RunLifecycleCandidates:      selected,
				GenericScheduleStore:        selected,
				TimerObligationReader:       selected,
				WorkflowPersistence:         workflowPersistence,
				ManagerStore:                selected,
				ManagerPersistenceRoles:     externalRuntimeTestSelectedManagerRoles(selected),
				DeliveryStore:               selected,
				PipelineObligations:         selected.PipelineObligations(),

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
			capability, _ := installExternalRuntimeTestGeneration(t, ctx, selected, rt)
			t.Cleanup(func() {
				if err := closeExternalRuntimeTestGeneration(rt, process, capability); err != nil {
					t.Errorf("close workflow timer generation: %v", err)
				}
			})
			if err := rt.Start(ctx); err != nil {
				t.Fatalf("Start runtime: %v", err)
			}

			routingSource, err := events.NewRootRoutingSource(entityID)
			if err != nil {
				t.Fatalf("build generic schedule routing source: %v", err)
			}
			baseCommand := runtimegenericschedule.AdmissionCommand{
				RunID:         runID,
				OwnerID:       "runtime",
				OwnerKind:     runtimegenericschedule.OwnerAgent,
				AgentIdentity: agentidentitytest.RootRuntime(t, "runtime", "generic-occurrence-proof"),
				EventType:     "generic.tick",
				EntityID:      entityID,
				Payload:       semanticvalue.EmptyObject(),
				RoutingSource: routingSource,
			}
			proveOneShot := func(mode executionmode.Mode) {
				t.Helper()
				command := baseCommand
				command.ScheduleKey = "generic-occurrence-proof-" + string(mode)
				command.TaskID = command.ScheduleKey
				command.ExecutionMode = mode
				command.Due = runtimegenericschedule.AbsoluteDue(time.Now().UTC().Add(50 * time.Millisecond).Truncate(time.Microsecond))
				admitted, err := rt.GenericSchedules.Admit(ctx, command)
				if err != nil {
					t.Fatalf("admit %s generic occurrence-shaped schedule: %v", mode, err)
				}

				deadline := time.Now().Add(5 * time.Second)
				for {
					var eventCount int
					var minMode, maxMode string
					query := `SELECT COUNT(*), COALESCE(MIN(execution_mode), ''), COALESCE(MAX(execution_mode), '') FROM events WHERE task_id = ? AND produced_by = ? AND produced_by_type = 'platform'`
					args := []any{command.TaskID, runtimegenericschedule.OccurrenceProducerID()}
					if postgres {
						query = `SELECT COUNT(*), COALESCE(MIN(execution_mode), ''), COALESCE(MAX(execution_mode), '') FROM events WHERE task_id = $1 AND produced_by = $2 AND produced_by_type = 'platform'`
					}
					if err := db.QueryRowContext(ctx, query, args...).Scan(&eventCount, &minMode, &maxMode); err != nil {
						t.Fatalf("read %s generic scheduled event: %v", mode, err)
					}
					activation, found, err := selected.LoadGenericScheduleActivation(ctx, admitted.Activation.ID)
					if err != nil {
						t.Fatalf("load %s generic schedule activation: %v", mode, err)
					}
					if eventCount == 1 && minMode == string(mode) && maxMode == string(mode) &&
						found && activation.Status == runtimegenericschedule.StatusFired && activation.Command.ExecutionMode == mode {
						break
					}
					if time.Now().After(deadline) {
						t.Fatalf("%s generic occurrence-shaped fire did not publish and settle: events=%d modes=%q/%q activation=%#v found=%v", mode, eventCount, minMode, maxMode, activation, found)
					}
					time.Sleep(20 * time.Millisecond)
				}
			}
			proveOneShot(executionmode.Live)
			proveOneShot(executionmode.Mock)

			recurringCommand := baseCommand
			recurringCommand.ScheduleKey = "generic-recurring-proof"
			recurringCommand.TaskID = recurringCommand.ScheduleKey
			recurringCommand.ExecutionMode = executionmode.Mock
			recurringCommand.Due = runtimegenericschedule.EveryDue(40 * time.Millisecond)
			recurring, err := rt.GenericSchedules.Admit(ctx, recurringCommand)
			if err != nil {
				t.Fatalf("admit recurring generic schedule: %v", err)
			}

			countEvents := func() (int, string, string) {
				t.Helper()
				query := `SELECT COUNT(DISTINCT event_id), COALESCE(MIN(execution_mode), ''), COALESCE(MAX(execution_mode), '') FROM events WHERE task_id = ? AND produced_by = ? AND produced_by_type = 'platform'`
				args := []any{recurringCommand.TaskID, runtimegenericschedule.OccurrenceProducerID()}
				if postgres {
					query = `SELECT COUNT(DISTINCT event_id), COALESCE(MIN(execution_mode), ''), COALESCE(MAX(execution_mode), '') FROM events WHERE task_id = $1 AND produced_by = $2 AND produced_by_type = 'platform'`
				}
				var count int
				var minMode, maxMode string
				if err := db.QueryRowContext(ctx, query, args...).Scan(&count, &minMode, &maxMode); err != nil {
					t.Fatalf("read recurring generic schedule events: %v", err)
				}
				return count, minMode, maxMode
			}

			deadline := time.Now().Add(5 * time.Second)
			for {
				activation, found, err := selected.LoadGenericScheduleActivation(ctx, recurring.Activation.ID)
				if err != nil {
					t.Fatalf("load recurring generic schedule activation: %v", err)
				}
				count, minMode, maxMode := countEvents()
				if count >= 2 && minMode == string(executionmode.Mock) && maxMode == string(executionmode.Mock) && found &&
					activation.Status == runtimegenericschedule.StatusActive && activation.Command.ExecutionMode == executionmode.Mock &&
					activation.CurrentDueAt.After(activation.InitialDueAt) {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("recurring generic schedule did not advance through exact mock occurrences: count=%d modes=%q/%q activation=%#v found=%v", count, minMode, maxMode, activation, found)
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
			settledCount, _, _ := countEvents()
			time.Sleep(120 * time.Millisecond)
			if after, _, _ := countEvents(); after != settledCount {
				t.Fatalf("recurring generic schedule published after cancellation: before=%d after=%d", settledCount, after)
			}
		})
	}
}

func TestRuntimeStartWithholdsDueSchedulesAndTimersUntilDynamicTopologyCompletesOnBothStores(t *testing.T) {
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
				return db, storetest.AdmitPostgresRuntimeStore(t, db), true
			},
		},
	} {
		t.Run(backend.name, func(t *testing.T) {
			db, selected, postgres := backend.open(t)
			workflowRunID := uuid.NewString()
			genericRunID := uuid.NewString()
			workflowEntityID := uuid.NewString()
			genericEntityID := uuid.NewString()
			ctx := testAuthorActivityContext(context.Background())
			workflowCtx := runtimecorrelation.WithRunID(ctx, workflowRunID)
			genericCtx := runtimecorrelation.WithRunID(ctx, genericRunID)
			for _, run := range []struct {
				ctx   context.Context
				runID string
			}{{workflowCtx, workflowRunID}, {genericCtx, genericRunID}} {
				fixture := runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: run.runID, Source: authorActivityTestBundleSourceFact}
				if postgres {
					runlifecyclefixture.RequirePostgres(t, run.ctx, db, fixture)
				} else {
					runlifecyclefixture.RequireSQLite(t, run.ctx, db, fixture)
				}
			}

			bundle := workflowTimerStartupRecoveryBundleWithDelay(t, "1s")
			source := semanticview.Wrap(bundle)
			module := newRuntimeTestWorkflowModule(t, source)
			newRuntime := func(owner runtimepipeline.WorkflowPersistenceOwner) (*swarmruntime.Runtime, *worklifetime.Process) {
				t.Helper()
				process := worklifetime.NewProcess()
				rt, err := swarmruntime.NewRuntime(ctx, completeExternalRuntimeTestWorkflowDeps(t, selected, swarmruntime.RuntimeDeps{
					Config: &config.Config{
						Runtime: config.RuntimeConfig{RecoveryOnStartup: true},
						LLM:     config.LLMConfig{Backend: "anthropic"},
					},
					EventStore:                  selected,
					EventBusDurable:             externalRuntimeTestDurableDependencies(selected),
					EventPayloadAdmissionBinder: selected,
					AuthorActivityRegistrars:    []swarmruntime.AuthorActivityCatalogRegistrar{selected},
					RunLifecycleCandidates:      selected,
					GenericScheduleStore:        selected,
					TimerObligationReader:       selected,
					WorkflowPersistence:         runtimepipeline.NewWorkflowPersistence(owner),
					ManagerStore:                selected,
					ManagerPersistenceRoles:     externalRuntimeTestSelectedManagerRoles(selected),
					DeliveryStore:               selected,
					PipelineObligations:         selected.PipelineObligations(),
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
			closeRuntime := func(label string, rt *swarmruntime.Runtime, process *worklifetime.Process, capability runtimestartupownership.ProcessCapability) {
				t.Helper()
				if err := closeExternalRuntimeTestGeneration(rt, process, capability); err != nil {
					t.Fatalf("close %s runtime: %v", label, err)
				}
			}

			seedRuntime, seedProcess := newRuntime(selected)
			seedCtx := testLiveExecutionContext(worklifetime.WithRuntimeOccurrence(workflowCtx, seedRuntime.WorkOccurrence()))
			occurredAt := time.Now().UTC()
			result, err := seedRuntime.Pipeline.MaterializeInitialEntry(seedCtx, runtimepipeline.WorkflowInstance{
				InstanceID: workflowRunID, StorageRef: workflowRunID,
				WorkflowName: "workflow-timer-startup", WorkflowVersion: "1", CurrentState: "waiting",
				Fields: map[string]any{
					"run_id": workflowRunID, "entity_id": workflowEntityID,
					"flow_path": workflowRunID, "instance_id": workflowRunID,
				},
				EntityType: "test_entity",
			}, occurredAt)
			if err != nil || result != runtimepipeline.WorkflowInitialMaterializationCreated {
				t.Fatalf("materialize withheld workflow timer: result=%v err=%v", result, err)
			}
			closeRuntime("seed", seedRuntime, seedProcess, nil)
			dueAt := occurredAt.Add(time.Second)

			routingSource, err := events.NewRootRoutingSource(genericEntityID)
			if err != nil {
				t.Fatal(err)
			}
			genericCommand := runtimegenericschedule.AdmissionCommand{
				RunID: genericRunID, ScheduleKey: "topology-latch-generic", TaskID: "topology-latch-generic",
				OwnerID: "runtime", OwnerKind: runtimegenericschedule.OwnerAgent,
				AgentIdentity: agentidentitytest.RootRuntime(t, "runtime", "topology-latch-generic"),
				EventType:     "generic.tick", EntityID: genericEntityID, Payload: semanticvalue.EmptyObject(),
				RoutingSource: routingSource, Due: runtimegenericschedule.AbsoluteDue(dueAt), ExecutionMode: executionmode.Mock,
			}
			genericAdmission, err := selected.AdmitGenericSchedule(genericCtx, genericCommand)
			if err != nil {
				t.Fatalf("persist overdue generic schedule: %v", err)
			}
			time.Sleep(time.Until(dueAt) + 50*time.Millisecond)
			countGenericEvents := func() int {
				t.Helper()
				query := `SELECT COUNT(*) FROM events WHERE task_id = ? AND produced_by = ? AND produced_by_type = 'platform'`
				if postgres {
					query = `SELECT COUNT(*) FROM events WHERE task_id = $1 AND produced_by = $2 AND produced_by_type = 'platform'`
				}
				var count int
				if err := db.QueryRowContext(genericCtx, query, genericCommand.TaskID, runtimegenericschedule.OccurrenceProducerID()).Scan(&count); err != nil {
					t.Fatalf("count generic schedule events: %v", err)
				}
				return count
			}

			bundleHash, bundleSource := authorActivityTestBundleSourceFact.StorageValues()
			failureOwner := workflowTimerStartupTopologyFailureOwner{
				WorkflowPersistenceOwner: selected,
				readiness: runtimepipeline.DynamicFlowRuntimeReadiness{
					InstancePath: "review/inst-1", OwningRunSource: authorActivityTestBundleSourceFact,
					RunStatus: "running", InstanceStatus: "active",
					Plan: runtimepipeline.DynamicFlowRuntimeReadinessPlan{
						RunID: workflowRunID, BundleHash: bundleHash, BundleSource: bundleSource,
						WorkflowVersion: source.WorkflowVersion() + "-stale", ExecutionMode: executionmode.Live,
						Identity: runtimeflowidentity.Instance{
							TemplateID: "review", ScopeKey: "review", InstanceID: "inst-1",
							InstancePath: "review/inst-1", EntityID: uuid.NewString(), HasStoredPath: true,
						},
					},
				},
			}
			failedRuntime, failedProcess := newRuntime(failureOwner)
			failedCapability, _ := installExternalRuntimeTestGeneration(t, ctx, selected, failedRuntime)
			err = failedRuntime.Start(ctx)
			if err == nil || !strings.Contains(err.Error(), "workflow version changed") {
				closeRuntime("unexpected successful topology-failure", failedRuntime, failedProcess, failedCapability)
				t.Fatalf("Start error = %v, want dynamic topology completion failure", err)
			}
			closeRuntime("failed topology", failedRuntime, failedProcess, failedCapability)
			time.Sleep(100 * time.Millisecond)
			instance, found, err := failedRuntime.Pipeline.Load(workflowCtx, runtimeflowidentity.RouteForInstancePath(workflowRunID))
			if err != nil || !found || instance.CurrentState != "waiting" {
				t.Fatalf("workflow timer crossed failed topology latch: found=%v state=%q err=%v", found, instance.CurrentState, err)
			}
			activation, found, err := selected.LoadGenericScheduleActivation(genericCtx, genericAdmission.Activation.ID)
			if err != nil || !found || activation.Status != runtimegenericschedule.StatusActive || countGenericEvents() != 0 {
				t.Fatalf("generic schedule crossed failed topology latch: found=%v activation=%#v events=%d err=%v", found, activation, countGenericEvents(), err)
			}

			recoveredRuntime, recoveredProcess := newRuntime(selected)
			recoveredCapability, _ := installExternalRuntimeTestGeneration(t, ctx, selected, recoveredRuntime)
			if err := recoveredRuntime.Start(ctx); err != nil {
				closeRuntime("failed recovery", recoveredRuntime, recoveredProcess, recoveredCapability)
				t.Fatalf("Start after completed topology: %v", err)
			}
			defer closeRuntime("recovered", recoveredRuntime, recoveredProcess, recoveredCapability)
			deadline := time.Now().Add(8 * time.Second)
			for {
				instance, instanceFound, loadErr := recoveredRuntime.Pipeline.Load(workflowCtx, runtimeflowidentity.RouteForInstancePath(workflowRunID))
				activation, activationFound, activationErr := selected.LoadGenericScheduleActivation(genericCtx, genericAdmission.Activation.ID)
				if loadErr == nil && instanceFound && instance.CurrentState == "done" && activationErr == nil && activationFound &&
					activation.Status == runtimegenericschedule.StatusFired && countGenericEvents() == 1 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("post-topology producers did not converge: instance=%#v found=%v load_err=%v activation=%#v found=%v activation_err=%v events=%d", instance, instanceFound, loadErr, activation, activationFound, activationErr, countGenericEvents())
				}
				time.Sleep(20 * time.Millisecond)
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

			source := semanticview.Wrap(workflowTimerStartupRecoveryBundle(t))
			module := newRuntimeTestWorkflowModule(t, source)
			newRuntime := func(managerStore runtimemanager.ManagerPersistence) (*swarmruntime.Runtime, *worklifetime.Process) {
				process := worklifetime.NewProcess()
				rt, err := swarmruntime.NewRuntime(ctx, completeExternalRuntimeTestWorkflowDeps(t, selected, swarmruntime.RuntimeDeps{
					Config: &config.Config{
						Runtime: config.RuntimeConfig{RecoveryOnStartup: true},
						LLM:     config.LLMConfig{Backend: "anthropic"},
					},

					EventStore:                  selected,
					EventBusDurable:             externalRuntimeTestDurableDependencies(selected),
					EventPayloadAdmissionBinder: selected,
					AuthorActivityRegistrars:    []swarmruntime.AuthorActivityCatalogRegistrar{selected},
					RunLifecycleCandidates:      selected,
					WorkflowPersistence:         workflowPersistence,
					ManagerStore:                managerStore,
					ManagerPersistenceRoles:     externalRuntimeTestSelectedManagerRoles(selected),
					TimerObligationReader:       selected,
					DeliveryStore:               selected,
					PipelineObligations:         selected.PipelineObligations(),

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
				if err := closeExternalRuntimeTestGeneration(rt, process, nil); err != nil {
					t.Fatalf("close %s runtime: %v", label, err)
				}
			}

			seedRuntime, seedProcess := newRuntime(selected)
			seedCtx := testLiveExecutionContext(worklifetime.WithRuntimeOccurrence(ctx, seedRuntime.WorkOccurrence()))
			result, err := seedRuntime.Pipeline.MaterializeInitialEntry(seedCtx, runtimepipeline.WorkflowInstance{
				InstanceID:      runID,
				StorageRef:      runID,
				WorkflowName:    "workflow-timer-startup",
				WorkflowVersion: "1",
				CurrentState:    "waiting",
				Fields: map[string]any{
					"run_id":      runID,
					"entity_id":   entityID,
					"flow_path":   runID,
					"instance_id": runID,
				},
				EntityType: "test_entity",
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
			capability, _ := installExternalRuntimeTestGeneration(t, ctx, selected, restarted)
			err = restarted.Start(ctx)
			if err == nil || !strings.Contains(err.Error(), "hydrate static declaration topology") {
				if closeErr := closeExternalRuntimeTestGeneration(restarted, restartedProcess, capability); closeErr != nil {
					t.Fatalf("close unexpected successful restart: %v", closeErr)
				}
				t.Fatalf("Start error = %v, want topology hydration failure before workflow-timer restoration", err)
			}
			if err := closeExternalRuntimeTestGeneration(restarted, restartedProcess, capability); err != nil {
				t.Fatalf("close failed-restart generation: %v", err)
			}

			instance, found, err := restarted.Pipeline.Load(ctx, runtimeflowidentity.RouteForInstancePath(runID))
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

			// This proof owns restoration through Runtime.Start, not the separate
			// overdue-timer versus pipeline-recovery ordering tracked by #2234.
			source := semanticview.Wrap(workflowTimerStartupRecoveryBundleWithDelay(t, "3s"))
			module := newRuntimeTestWorkflowModule(t, source)
			bootProgress := make([]swarmruntime.BootProgressEvent, 0, swarmruntime.BootProgressTotalSteps)
			newRuntime := func() (*swarmruntime.Runtime, *worklifetime.Process) {
				process := worklifetime.NewProcess()
				rt, err := swarmruntime.NewRuntime(ctx, completeExternalRuntimeTestWorkflowDeps(t, selected, swarmruntime.RuntimeDeps{
					Config: &config.Config{
						Runtime: config.RuntimeConfig{RecoveryOnStartup: true},
						LLM:     config.LLMConfig{Backend: "anthropic"},
					},

					EventStore:                  selected,
					EventBusDurable:             externalRuntimeTestDurableDependencies(selected),
					EventPayloadAdmissionBinder: selected,
					AuthorActivityRegistrars:    []swarmruntime.AuthorActivityCatalogRegistrar{selected},
					RunLifecycleCandidates:      selected,
					WorkflowPersistence:         workflowPersistence,
					ManagerStore:                selected,
					ManagerPersistenceRoles:     externalRuntimeTestSelectedManagerRoles(selected),
					TimerObligationReader:       selected,
					DeliveryStore:               selected,
					PipelineObligations:         selected.PipelineObligations(),

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
				if err := closeExternalRuntimeTestGeneration(rt, process, nil); err != nil {
					t.Fatalf("close %s runtime: %v", label, err)
				}
			}

			seedRuntime, seedProcess := newRuntime()
			occurredAt := time.Now().UTC().Add(-time.Second)
			seedCtx := testLiveExecutionContext(worklifetime.WithRuntimeOccurrence(ctx, seedRuntime.WorkOccurrence()))
			result, err := seedRuntime.Pipeline.MaterializeInitialEntry(seedCtx, runtimepipeline.WorkflowInstance{
				InstanceID:      runID,
				StorageRef:      runID,
				WorkflowName:    "workflow-timer-startup",
				WorkflowVersion: "1",
				CurrentState:    "waiting",
				Fields: map[string]any{
					"run_id":      runID,
					"entity_id":   entityID,
					"flow_path":   runID,
					"instance_id": runID,
				},
				EntityType: "test_entity",
			}, occurredAt)
			if err != nil {
				t.Fatalf("materialize workflow timer before restart: %v", err)
			}
			if result != runtimepipeline.WorkflowInitialMaterializationCreated {
				t.Fatalf("initial materialization result = %v, want created", result)
			}
			shutdown("seed", seedRuntime, seedProcess)

			restarted, restartedProcess := newRuntime()
			capability, _ := installExternalRuntimeTestGeneration(t, ctx, selected, restarted)
			if err := restarted.Start(ctx); err != nil {
				if closeErr := closeExternalRuntimeTestGeneration(restarted, restartedProcess, capability); closeErr != nil {
					t.Fatalf("close failed restart: %v", closeErr)
				}
				t.Fatalf("Start restarted runtime: %v", err)
			}
			defer func() {
				if err := closeExternalRuntimeTestGeneration(restarted, restartedProcess, capability); err != nil {
					t.Errorf("close restarted generation: %v", err)
				}
			}()
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
				instance, found, err := restarted.Pipeline.Load(ctx, runtimeflowidentity.RouteForInstancePath(runID))
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

type workflowTimerStartupLLM struct{ llm.NoopRuntime }

func (workflowTimerStartupLLM) ProviderContract() llm.ProviderContract {
	return llm.AnthropicAPIProviderContract()
}

func workflowTimerStartupRecoveryBundle(t *testing.T) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	return workflowTimerStartupRecoveryBundleWithDelay(t, "25ms")
}

func workflowTimerStartupRecoveryBundleWithDelay(t *testing.T, delay string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	bundle := loadRuntimeTempBundle(t, map[string]string{
		"package.yaml":  "name: workflow-timer-startup\nversion: 1\nplatform_version: '>=0.7.0 <0.8.0'\n",
		"schema.yaml":   "name: workflow-timer-startup\ninitial_state: waiting\nstates: [waiting, done]\nterminal_states: [done]\npins:\n  inputs:\n    events:\n      - event: generic.tick\n        source: external\n",
		"entities.yaml": "test_entity: {}\n",
		"events.yaml":   "generic.tick: {}\n",
	})
	bundle.Semantics.Name = "workflow-timer-startup"
	bundle.Semantics.Version = "1"
	bundle.Semantics.InitialStage = "waiting"
	bundle.Semantics.Stages = []runtimecontracts.WorkflowStageContract{{ID: "waiting"}, {ID: "done"}}
	bundle.Semantics.TerminalStages = []string{"done"}
	bundle.Semantics.Timers = []runtimecontracts.WorkflowTimerContract{{
		ID: "waiting.timeout", Stage: "waiting", StageOwned: true, AdvancesTo: "done",
		Owner: "runtime", Event: runtimecontracts.WorkflowStageTimerInternalEvent,
		StartOn: "state:waiting", Delay: delay,
	}}
	bundle.Platform.Platform.Name = "swarm"
	bundle.Platform.Platform.Version = "0.7.0"
	return bundle
}
