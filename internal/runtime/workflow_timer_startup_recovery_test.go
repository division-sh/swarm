package runtime_test

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	swarmruntime "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	llm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type workflowTimerStartupStore interface {
	runtimebus.EventStore
	runtimepipeline.RuntimeMutationRunner
	runtimepipeline.SchedulePersistence
	runtimedelivery.Store
	runtimemanager.ManagerPersistence
	PipelineObligations() runtimepipelineobligation.Store
}

func TestGenericOccurrenceShapedSchedulePublishesThroughWorkflowEnabledRuntimeOnBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) (*sql.DB, workflowTimerStartupStore, *runtimepipeline.WorkflowInstanceStore, bool)
	}{
		{
			name: "sqlite",
			open: func(t *testing.T) (*sql.DB, workflowTimerStartupStore, *runtimepipeline.WorkflowInstanceStore, bool) {
				selected := storetest.StartSQLiteRuntimeStore(t)
				return selected.DB, selected, runtimepipeline.NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(selected.DB, selected), false
			},
		},
		{
			name: "postgres",
			open: func(t *testing.T) (*sql.DB, workflowTimerStartupStore, *runtimepipeline.WorkflowInstanceStore, bool) {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				selected := storetest.AdmitPostgresRuntimeStore(t, db)
				return db, selected, runtimepipeline.NewWorkflowInstanceStore(db), true
			},
		},
	} {
		t.Run(backend.name, func(t *testing.T) {
			db, selected, workflowStore, postgres := backend.open(t)
			runID := uuid.NewString()
			entityID := uuid.NewString()
			ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
			bundleHash, bundleSource := authorActivityTestBundleSourceFact.StorageValues()
			insertRun := `INSERT INTO runs (run_id, status, bundle_hash, bundle_source) VALUES (?, 'running', ?, ?)`
			if postgres {
				insertRun = `INSERT INTO runs (run_id, status, bundle_hash, bundle_source) VALUES ($1::uuid, 'running', $2, $3)`
			}
			if _, err := db.ExecContext(ctx, insertRun, runID, bundleHash, bundleSource); err != nil {
				t.Fatalf("seed active run: %v", err)
			}

			process := worklifetime.NewProcess()
			runtimeDB := db
			if !postgres {
				runtimeDB = nil
			}
			source := semanticview.Wrap(workflowTimerStartupRecoveryBundle())
			rt, err := swarmruntime.NewRuntime(ctx, swarmruntime.RuntimeDeps{
				Config: &config.Config{
					Runtime: config.RuntimeConfig{RecoveryOnStartup: true},
					LLM:     config.LLMConfig{Backend: "anthropic"},
				},
				Stores: swarmruntime.Stores{
					SQLDB:               runtimeDB,
					EventStore:          selected,
					ScheduleStore:       selected,
					PipelineStore:       workflowStore,
					ManagerStore:        selected,
					DeliveryStore:       selected,
					PipelineObligations: selected.PipelineObligations(),
				},
				Options: swarmruntime.RuntimeOptions{
					SelfCheck:         false,
					WorkflowModule:    newRuntimeTestWorkflowModule(t, source),
					LLMRuntime:        workflowTimerStartupLLM{},
					RuntimeInstanceID: authorActivityTestRuntimeInstanceID,
					BundleSourceFact:  authorActivityTestBundleSourceFact,
					ProcessWorkOwner:  process,
				},
			})
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

			occurrence := timeridentity.WorkflowTimerOccurrenceRef{
				Activation: timeridentity.WorkflowTimerActivationRef{
					ActivationID: uuid.NewString(),
					Declaration:  "generic.opaque",
				},
				DueAt: time.Now().UTC().Add(50 * time.Millisecond).Truncate(time.Microsecond),
			}.Normalize()
			schedule := runtimepipeline.Schedule{
				RunID:        runID,
				AgentID:      "runtime",
				EventType:    "generic.tick",
				Mode:         "once",
				At:           occurrence.DueAt,
				EntityID:     entityID,
				FlowInstance: "root",
				TaskID:       occurrence.TaskID(),
				Payload:      []byte(`{}`),
			}
			if err := selected.UpsertSchedule(ctx, schedule); err != nil {
				t.Fatalf("persist generic occurrence-shaped schedule: %v", err)
			}
			if err := rt.Scheduler.Register(ctx, schedule); err != nil {
				t.Fatalf("register generic occurrence-shaped schedule: %v", err)
			}

			deadline := time.Now().Add(5 * time.Second)
			for {
				var eventCount int
				query := `SELECT COUNT(*) FROM events WHERE task_id = ? AND produced_by = 'runtime.scheduler' AND produced_by_type = 'platform'`
				if postgres {
					query = `SELECT COUNT(*) FROM events WHERE task_id = $1 AND produced_by = 'runtime.scheduler' AND produced_by_type = 'platform'`
				}
				if err := db.QueryRowContext(ctx, query, occurrence.TaskID()).Scan(&eventCount); err != nil {
					t.Fatalf("read generic scheduled event: %v", err)
				}
				active, err := selected.LoadActiveSchedules(ctx)
				if err != nil {
					t.Fatalf("load active generic schedules: %v", err)
				}
				if eventCount == 1 && len(active) == 0 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("generic occurrence-shaped fire did not publish and complete: events=%d active=%#v", eventCount, active)
				}
				time.Sleep(20 * time.Millisecond)
			}
		})
	}
}

func TestRuntimeStartRestoresWorkflowTimersWithoutGenericScheduleStoreOnBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) (*sql.DB, workflowTimerStartupStore, *runtimepipeline.WorkflowInstanceStore, bool)
	}{
		{
			name: "sqlite",
			open: func(t *testing.T) (*sql.DB, workflowTimerStartupStore, *runtimepipeline.WorkflowInstanceStore, bool) {
				selected := storetest.StartSQLiteRuntimeStore(t)
				return selected.DB, selected, runtimepipeline.NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(selected.DB, selected), false
			},
		},
		{
			name: "postgres",
			open: func(t *testing.T) (*sql.DB, workflowTimerStartupStore, *runtimepipeline.WorkflowInstanceStore, bool) {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				selected := storetest.AdmitPostgresRuntimeStore(t, db)
				return db, selected, runtimepipeline.NewWorkflowInstanceStore(db), true
			},
		},
	} {
		t.Run(backend.name, func(t *testing.T) {
			db, selected, workflowStore, postgres := backend.open(t)
			runID := uuid.NewString()
			entityID := uuid.NewString()
			ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
			bundleHash, bundleSource := authorActivityTestBundleSourceFact.StorageValues()
			insertRun := `INSERT INTO runs (run_id, status, bundle_hash, bundle_source) VALUES (?, 'running', ?, ?)`
			if postgres {
				insertRun = `INSERT INTO runs (run_id, status, bundle_hash, bundle_source) VALUES ($1::uuid, 'running', $2, $3)`
			}
			if _, err := db.ExecContext(ctx, insertRun, runID, bundleHash, bundleSource); err != nil {
				t.Fatalf("seed active run: %v", err)
			}

			source := semanticview.Wrap(workflowTimerStartupRecoveryBundle())
			module := newRuntimeTestWorkflowModule(t, source)
			bootProgress := make([]swarmruntime.BootProgressEvent, 0, swarmruntime.BootProgressTotalSteps)
			runtimeDB := db
			if !postgres {
				runtimeDB = nil
			}
			newRuntime := func() (*swarmruntime.Runtime, *worklifetime.Process) {
				process := worklifetime.NewProcess()
				rt, err := swarmruntime.NewRuntime(ctx, swarmruntime.RuntimeDeps{
					Config: &config.Config{
						Runtime: config.RuntimeConfig{RecoveryOnStartup: true},
						LLM:     config.LLMConfig{Backend: "anthropic"},
					},
					Stores: swarmruntime.Stores{
						SQLDB:               runtimeDB,
						EventStore:          selected,
						PipelineStore:       workflowStore,
						ManagerStore:        selected,
						DeliveryStore:       selected,
						PipelineObligations: selected.PipelineObligations(),
					},
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
				})
				if err != nil {
					t.Fatalf("NewRuntime: %v", err)
				}
				if rt.Stores.ScheduleStore != nil {
					t.Fatal("workflow-only recovery fixture unexpectedly configured a generic schedule store")
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
			seedCtx := worklifetime.WithRuntimeOccurrence(ctx, seedRuntime.WorkOccurrence())
			result, err := workflowStore.MaterializeInitialEntry(seedCtx, runtimepipeline.WorkflowInstance{
				InstanceID:      entityID,
				StorageRef:      entityID,
				WorkflowName:    "workflow-timer-startup",
				WorkflowVersion: "1",
				CurrentState:    "waiting",
				Metadata:        map[string]any{"run_id": runID},
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
				instance, found, err := workflowStore.Load(ctx, entityID)
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
