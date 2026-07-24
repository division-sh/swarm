package runtime_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	swarmruntime "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	llm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
)

type nodeDeliveryRecoveryStore interface {
	runtimebus.EventStore
	runtimedelivery.Store
	runtimepipeline.RuntimeMutationRunner
	PipelineObligations() runtimepipelineobligation.Store
}

type renewalTrackingDeliveryStore struct {
	runtimedelivery.Store
	renewals atomic.Int64
}

type startupRecoveryOrderStore interface {
	nodeDeliveryRecoveryStore
	runtimemanager.ManagerPersistence
	runtimemanager.AgentLifecyclePersistence
}

type startupRecoveryOrderLLM struct{}

func (startupRecoveryOrderLLM) StartSession(context.Context, string, string, []llm.ToolDefinition) (*llm.Session, error) {
	return &llm.Session{}, nil
}

func (startupRecoveryOrderLLM) ContinueSession(context.Context, *llm.Session, llm.Message) (*llm.Response, error) {
	return &llm.Response{}, nil
}

type startupRecoveryOrderAgent struct {
	id            string
	subscriptions []events.EventType
}

func (a startupRecoveryOrderAgent) ID() string { return a.id }
func (startupRecoveryOrderAgent) Type() string { return "test" }
func (a startupRecoveryOrderAgent) Subscriptions() []events.EventType {
	return append([]events.EventType(nil), a.subscriptions...)
}
func (a startupRecoveryOrderAgent) OnEvent(_ context.Context, event events.Event) ([]events.Event, error) {
	return nil, nil
}

func TestRuntimeStartHydratesPersistedAgentsBeforeRecoveringNodeDeliveriesParity(t *testing.T) {
	for _, backend := range []struct {
		name  string
		setup func(*testing.T) (context.Context, *sql.DB, *sql.DB, startupRecoveryOrderStore, *runtimepipeline.WorkflowInstanceStore)
	}{
		{
			name: "postgres",
			setup: func(t *testing.T) (context.Context, *sql.DB, *sql.DB, startupRecoveryOrderStore, *runtimepipeline.WorkflowInstanceStore) {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				ctx := seedRuntimeTestRun(t, db)
				return ctx, db, db, storetest.AdmitPostgresRuntimeStore(t, db), runtimepipeline.NewWorkflowInstanceStore(db)
			},
		},
		{
			name: "sqlite",
			setup: func(t *testing.T) (context.Context, *sql.DB, *sql.DB, startupRecoveryOrderStore, *runtimepipeline.WorkflowInstanceStore) {
				selected := storetest.StartSQLiteRuntimeStore(t)
				ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), templateInstanceDeliveryRunID)
				if _, err := selected.DB.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES (?, 'running')`, templateInstanceDeliveryRunID); err != nil {
					t.Fatalf("seed SQLite startup-order run: %v", err)
				}
				workflowStore := runtimepipeline.NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(selected.DB, selected)
				return ctx, nil, selected.DB, selected, workflowStore
			},
		},
	} {
		t.Run(backend.name, func(t *testing.T) {
			ctx, runtimeSQLDB, queryDB, selected, workflowStore := backend.setup(t)
			repoRoot := filepath.Clean(filepath.Join("..", ".."))
			fixtureRoot := filepath.Join(repoRoot, "tests", "tier8-boot-verification", "test-boot-success")
			bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
				repoRoot,
				fixtureRoot,
				runtimecontracts.DefaultPlatformSpecFile(repoRoot),
			)
			if err != nil {
				t.Fatalf("load startup-order workflow contract: %v", err)
			}
			source := semanticview.Wrap(bundle)
			module := newRuntimeTestWorkflowModule(t, source)

			const agentID = "startup-order-agent"
			agentConfig := runtimeactors.AgentConfig{
				ID: agentID, Type: "test", Role: "observer", FlowID: "global", Model: "regular",
				ExecutionMode: "live", Subscriptions: []string{"task.completed"}, Config: []byte(`{"system_prompt":"observe completed tasks"}`),
			}

			eventID := eventtest.UUID("startup-order-node-event-" + backend.name)
			event := eventtest.RunCreatingRootIngress(
				eventID, "task.requested", "test", "", []byte(`{}`), 0,
				templateInstanceDeliveryRunID, "", events.EventEnvelope{}, time.Now().UTC(),
			)
			nodeRoute := events.DeliveryRoute{SubscriberType: "node", SubscriberID: "complete-task"}
			storetest.CommitSemanticEventWithRoutes(t, ctx, selected, event, []events.DeliveryRoute{nodeRoute}, runtimepipelineobligation.ScopeSubscribed)

			processOwner := worklifetime.NewProcess()
			t.Cleanup(func() {
				joinCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if _, err := processOwner.Join(joinCtx); err != nil {
					t.Errorf("join startup-order process owner: %v", err)
				}
			})
			hydrated := atomic.Bool{}
			runtime, err := swarmruntime.NewRuntime(ctx, swarmruntime.RuntimeDeps{
				Config: &config.Config{Runtime: config.RuntimeConfig{RecoveryOnStartup: true}, LLM: config.LLMConfig{Backend: "anthropic"}},
				Stores: swarmruntime.Stores{
					SQLDB: runtimeSQLDB, EventStore: selected, PipelineStore: workflowStore,
					ManagerStore: selected, DeliveryStore: selected,
					PipelineObligations: selected.PipelineObligations(),
				},
				Options: swarmruntime.RuntimeOptions{
					SelfCheck: false, WorkflowModule: module, LLMRuntime: startupRecoveryOrderLLM{},
					RuntimeInstanceID: authorActivityTestRuntimeInstanceID, BundleSourceFact: authorActivityTestBundleSourceFact,
					BundleFingerprint: authorActivityTestBundleSourceFact.BundleFingerprint, ProcessWorkOwner: processOwner,
					TestWorkflowNodeHandlerStartHook: func(context.Context, string, events.Event) error {
						if !hydrated.Load() {
							return errors.New("workflow-node recovery started before persisted agent hydration")
						}
						return nil
					},
				},
			})
			if err != nil {
				t.Fatalf("NewRuntime: %v", err)
			}
			t.Cleanup(func() {
				if err := runtime.Shutdown(); err != nil {
					t.Errorf("shutdown startup-order runtime: %v", err)
				}
			})
			if err := runtime.Manager.SpawnAgent(agentConfig); err != nil {
				t.Fatalf("persist startup-order agent: %v", err)
			}
			if err := runtime.Manager.Shutdown(); err != nil {
				t.Fatalf("retire constructed manager before startup-order replacement: %v", err)
			}
			runtime.Manager = runtimemanager.NewAgentManagerWithOptions(runtime.Bus, func(cfg runtimeactors.AgentConfig) (runtimemanager.Agent, error) {
				hydrated.Store(true)
				subscriptions := make([]events.EventType, 0, len(cfg.Subscriptions))
				for _, subscription := range cfg.Subscriptions {
					subscriptions = append(subscriptions, events.EventType(subscription))
				}
				return startupRecoveryOrderAgent{id: cfg.ID, subscriptions: subscriptions}, nil
			}, runtimemanager.AgentManagerOptions{
				BaseContext: ctx, LifecycleStore: selected, DeliveryStore: selected, SemanticSource: source,
				WorkflowInstances: workflowStore, WorkOwner: runtime.WorkOccurrence(),
			}, selected)

			if err := runtime.Start(ctx); err != nil {
				t.Fatalf("Start: %v", err)
			}
			assertRecoveredNodeDelivery(t, ctx, selected, eventID, nodeRoute, 1)
			eventIDQuery := `SELECT event_id::text FROM events WHERE event_name = 'task.completed' ORDER BY created_at DESC LIMIT 1`
			if backend.name == "sqlite" {
				eventIDQuery = `SELECT event_id FROM events WHERE event_name = 'task.completed' ORDER BY created_at DESC LIMIT 1`
			}
			var completedEventID string
			if err := queryDB.QueryRowContext(ctx, eventIDQuery).Scan(&completedEventID); err != nil {
				t.Fatalf("load event emitted by recovered workflow node: %v", err)
			}
			if completedEventID == "" || !hydrated.Load() {
				t.Fatalf("startup order proof = completed event %q hydrated %t, want emitted event after agent hydration", completedEventID, hydrated.Load())
			}
		})
	}
}

func (s *renewalTrackingDeliveryStore) RenewClaim(ctx context.Context, claim runtimedelivery.Claim) (runtimedelivery.Snapshot, error) {
	s.renewals.Add(1)
	return s.Store.RenewClaim(ctx, claim)
}

func TestPipelineCoordinatorRecoverNodeDeliveriesUsesCanonicalSelectedStoreOwner(t *testing.T) {
	for _, backend := range []struct {
		name  string
		setup func(*testing.T) (context.Context, *sql.DB, nodeDeliveryRecoveryStore)
	}{
		{
			name: "postgres",
			setup: func(t *testing.T) (context.Context, *sql.DB, nodeDeliveryRecoveryStore) {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				ctx := seedRuntimeTestRun(t, db)
				return ctx, db, storetest.AdmitPostgresRuntimeStore(t, db)
			},
		},
		{
			name: "sqlite",
			setup: func(t *testing.T) (context.Context, *sql.DB, nodeDeliveryRecoveryStore) {
				selected := storetest.StartSQLiteRuntimeStore(t)
				ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), templateInstanceDeliveryRunID)
				if _, err := selected.DB.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES (?, 'running')`, templateInstanceDeliveryRunID); err != nil {
					t.Fatalf("seed SQLite recovery run: %v", err)
				}
				return ctx, selected.DB, selected
			},
		},
	} {
		t.Run(backend.name, func(t *testing.T) {
			ctx, db, selected := backend.setup(t)
			bundle := loadRuntimeTempBundle(t, artifactActionResultDeliveryFixtureFiles())
			source := semanticview.Wrap(bundle)
			bus, err := newScopedTestEventBus(t, selected, runtimebus.EventBusOptions{ContractBundle: source})
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			workflowStore := runtimepipeline.NewWorkflowInstanceStore(db)
			if backend.name == "sqlite" {
				workflowStore = runtimepipeline.NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(db, selected)
			}
			if err := workflowStore.Upsert(ctx, artifactActionResultWorkflowInstance()); err != nil {
				t.Fatalf("seed workflow instance: %v", err)
			}
			deliveryOwner := &renewalTrackingDeliveryStore{Store: selected}
			pc := runtimepipeline.NewPipelineCoordinatorWithOptions(bus, db, runtimepipeline.PipelineCoordinatorOptions{
				WorkOwner:     runtimeTestEventBusWorkOwner(t, bus),
				Module:        newRuntimeTestWorkflowModule(t, source),
				WorkflowStore: workflowStore,
				DeliveryStore: deliveryOwner,
			})

			eventID := "99999999-9999-4999-8999-999999999981"
			target := events.RouteIdentity{FlowID: "repo-scaffold", FlowInstance: "repo-scaffold/inst-1", EntityID: artifactActionResultEntityID}
			event := eventtest.RunCreatingRootIngress(
				eventID,
				"repo-scaffold/inst-1/repo_scaffold.repo_commit_succeeded",
				"test",
				"",
				[]byte(`{}`),
				0,
				templateInstanceDeliveryRunID,
				"",
				events.EnvelopeForTargetRoute(events.EnvelopeForEntityID(events.EventEnvelope{}, artifactActionResultEntityID), target),
				time.Now().UTC(),
			)
			route := events.DeliveryRoute{SubscriberType: "node", SubscriberID: "repo-scaffold-node", Target: target}
			storetest.CommitSemanticEventWithRoutes(t, ctx, selected, event, []events.DeliveryRoute{route}, runtimepipelineobligation.ScopeSubscribed)

			if err := pc.RecoverNodeDeliveries(ctx); err != nil {
				t.Fatalf("RecoverNodeDeliveries: %v", err)
			}
			assertRecoveredNodeDelivery(t, ctx, selected, eventID, route, 1)
			if got := deliveryOwner.renewals.Load(); got < 2 {
				t.Fatalf("claim renewals = %d, want immediate and final handler renewal", got)
			}
			if err := pc.RecoverNodeDeliveries(ctx); err != nil {
				t.Fatalf("repeat RecoverNodeDeliveries: %v", err)
			}
			assertRecoveredNodeDelivery(t, ctx, selected, eventID, route, 1)
		})
	}
}

func TestPipelineCoordinatorRecoveryContinuesAfterCommittedDeadLetterParity(t *testing.T) {
	for _, backend := range []struct {
		name  string
		setup func(*testing.T) (context.Context, *sql.DB, nodeDeliveryRecoveryStore)
	}{
		{
			name: "postgres",
			setup: func(t *testing.T) (context.Context, *sql.DB, nodeDeliveryRecoveryStore) {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				ctx := seedRuntimeTestRun(t, db)
				return ctx, db, storetest.AdmitPostgresRuntimeStore(t, db)
			},
		},
		{
			name: "sqlite",
			setup: func(t *testing.T) (context.Context, *sql.DB, nodeDeliveryRecoveryStore) {
				selected := storetest.StartSQLiteRuntimeStore(t)
				ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), templateInstanceDeliveryRunID)
				if _, err := selected.DB.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES (?, 'running')`, templateInstanceDeliveryRunID); err != nil {
					t.Fatalf("seed SQLite recovery run: %v", err)
				}
				return ctx, selected.DB, selected
			},
		},
	} {
		t.Run(backend.name, func(t *testing.T) {
			ctx, db, selected := backend.setup(t)
			bundle := loadRuntimeTempBundle(t, artifactActionResultDeliveryFixtureFiles())
			source := semanticview.Wrap(bundle)
			bus, err := newScopedTestEventBus(t, selected, runtimebus.EventBusOptions{ContractBundle: source})
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			workflowStore := runtimepipeline.NewWorkflowInstanceStore(db)
			if backend.name == "sqlite" {
				workflowStore = runtimepipeline.NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(db, selected)
			}
			if err := workflowStore.Upsert(ctx, artifactActionResultWorkflowInstance()); err != nil {
				t.Fatalf("seed healthy workflow instance: %v", err)
			}
			pc := runtimepipeline.NewPipelineCoordinatorWithOptions(bus, db, runtimepipeline.PipelineCoordinatorOptions{
				WorkOwner:     runtimeTestEventBusWorkOwner(t, bus),
				Module:        newRuntimeTestWorkflowModule(t, source),
				WorkflowStore: workflowStore,
				DeliveryStore: selected,
			})

			poisonEntityID := eventtest.UUID("node-recovery-poison-entity")
			poisonTarget := events.RouteIdentity{FlowID: "repo-scaffold", FlowInstance: "repo-scaffold/poison", EntityID: poisonEntityID}
			poisonInstance := artifactActionResultWorkflowInstance()
			poisonInstance.InstanceID = poisonEntityID
			poisonInstance.StorageRef = poisonEntityID
			poisonInstance.Metadata = map[string]any{
				"repo_id": "poison-repo", "namespace": "tenant-alpha", "partition_key": "poison",
				"display_slug": "Poison", "source_record_id": "poison-record", "flow_path": poisonTarget.FlowInstance,
			}
			if err := workflowStore.Upsert(ctx, poisonInstance); err != nil {
				t.Fatalf("seed poison workflow instance: %v", err)
			}
			installNodeRecoveryPoisonMutation(t, ctx, db, backend.name == "postgres", poisonEntityID)
			poison := eventtest.RunCreatingRootIngress(
				eventtest.UUID("node-recovery-poison-event"),
				"repo-scaffold/poison/repo_scaffold.repo_commit_succeeded",
				"test", "", []byte(`{}`), 0, templateInstanceDeliveryRunID, "",
				events.EnvelopeForTargetRoute(events.EnvelopeForEntityID(events.EventEnvelope{}, poisonEntityID), poisonTarget),
				time.Now().UTC().Add(-time.Minute),
			)
			poisonRoute := events.DeliveryRoute{SubscriberType: "node", SubscriberID: "repo-scaffold-node", Target: poisonTarget}
			storetest.CommitSemanticEventWithRoutes(t, ctx, selected, poison, []events.DeliveryRoute{poisonRoute}, runtimepipelineobligation.ScopeSubscribed)

			healthyTarget := events.RouteIdentity{FlowID: "repo-scaffold", FlowInstance: "repo-scaffold/inst-1", EntityID: artifactActionResultEntityID}
			healthy := eventtest.RunCreatingRootIngress(
				eventtest.UUID("node-recovery-healthy-event"),
				"repo-scaffold/inst-1/repo_scaffold.repo_commit_succeeded",
				"test", "", []byte(`{}`), 0, templateInstanceDeliveryRunID, "",
				events.EnvelopeForTargetRoute(events.EnvelopeForEntityID(events.EventEnvelope{}, artifactActionResultEntityID), healthyTarget),
				time.Now().UTC(),
			)
			healthyRoute := events.DeliveryRoute{SubscriberType: "node", SubscriberID: "repo-scaffold-node", Target: healthyTarget}
			storetest.CommitSemanticEventWithRoutes(t, ctx, selected, healthy, []events.DeliveryRoute{healthyRoute}, runtimepipelineobligation.ScopeSubscribed)

			if err := pc.RecoverNodeDeliveries(ctx); err != nil {
				t.Fatalf("RecoverNodeDeliveries after terminal handler failure: %v", err)
			}
			poisonObligation, err := runtimedelivery.NewObligation(poison.ID(), poison.RunID(), poisonRoute)
			if err != nil {
				t.Fatalf("derive poison delivery obligation: %v", err)
			}
			poisonSnapshot, err := selected.Snapshot(ctx, poisonObligation.DeliveryID())
			if err != nil {
				t.Fatalf("load poison delivery snapshot: %v", err)
			}
			if poisonSnapshot.Status != runtimedelivery.StatusDeadLetter || poisonSnapshot.ReasonCode != "handler_terminal_failure" {
				t.Fatalf("poison delivery = status:%s reason:%s, want committed terminal-handler dead letter", poisonSnapshot.Status, poisonSnapshot.ReasonCode)
			}
			assertRecoveredNodeDelivery(t, ctx, selected, healthy.ID(), healthyRoute, 1)
		})
	}
}

func installNodeRecoveryPoisonMutation(t *testing.T, ctx context.Context, db *sql.DB, postgres bool, entityID string) {
	t.Helper()
	statement := fmt.Sprintf(`
		CREATE TRIGGER fail_node_recovery_poison_update
		BEFORE UPDATE ON entity_state
		WHEN OLD.entity_id = '%s'
		BEGIN
			SELECT RAISE(ABORT, 'injected node recovery poison mutation');
		END`, entityID)
	if postgres {
		statement = fmt.Sprintf(`
			CREATE FUNCTION fail_node_recovery_poison_update_fn() RETURNS trigger AS $$
			BEGIN
				IF OLD.entity_id = '%s'::uuid THEN
					RAISE EXCEPTION 'injected node recovery poison mutation';
				END IF;
				RETURN NEW;
			END;
			$$ LANGUAGE plpgsql;
			CREATE TRIGGER fail_node_recovery_poison_update
			BEFORE UPDATE ON entity_state
			FOR EACH ROW EXECUTE FUNCTION fail_node_recovery_poison_update_fn()`, entityID)
	}
	if _, err := db.ExecContext(ctx, statement); err != nil {
		t.Fatalf("install node recovery poison mutation: %v", err)
	}
}

func TestPipelineCoordinatorStandingRecoveryClaimsNewlyEligibleNodeDeliveries(t *testing.T) {
	for _, backend := range []struct {
		name  string
		setup func(*testing.T) (context.Context, *sql.DB, nodeDeliveryRecoveryStore)
	}{
		{
			name: "postgres",
			setup: func(t *testing.T) (context.Context, *sql.DB, nodeDeliveryRecoveryStore) {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				ctx := seedRuntimeTestRun(t, db)
				return ctx, db, storetest.AdmitPostgresRuntimeStore(t, db)
			},
		},
		{
			name: "sqlite",
			setup: func(t *testing.T) (context.Context, *sql.DB, nodeDeliveryRecoveryStore) {
				selected := storetest.StartSQLiteRuntimeStore(t)
				ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), templateInstanceDeliveryRunID)
				if _, err := selected.DB.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES (?, 'running')`, templateInstanceDeliveryRunID); err != nil {
					t.Fatalf("seed SQLite standing recovery run: %v", err)
				}
				return ctx, selected.DB, selected
			},
		},
	} {
		t.Run(backend.name, func(t *testing.T) {
			ctx, db, selected := backend.setup(t)
			bundle := loadRuntimeTempBundle(t, artifactActionResultDeliveryFixtureFiles())
			source := semanticview.Wrap(bundle)
			bus, err := newScopedTestEventBus(t, selected, runtimebus.EventBusOptions{ContractBundle: source})
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			workflowStore := runtimepipeline.NewWorkflowInstanceStore(db)
			if backend.name == "sqlite" {
				workflowStore = runtimepipeline.NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(db, selected)
			}
			if err := workflowStore.Upsert(ctx, artifactActionResultWorkflowInstance()); err != nil {
				t.Fatalf("seed workflow instance: %v", err)
			}
			handlerStarted := make(chan struct{}, 4)
			deliveryOwner := &renewalTrackingDeliveryStore{Store: selected}
			pc := runtimepipeline.NewPipelineCoordinatorWithOptions(bus, db, runtimepipeline.PipelineCoordinatorOptions{
				WorkOwner:     runtimeTestEventBusWorkOwner(t, bus),
				Module:        newRuntimeTestWorkflowModule(t, source),
				WorkflowStore: workflowStore,
				DeliveryStore: deliveryOwner,
				TestWorkflowNodeHandlerStartHook: func(context.Context, string, events.Event) error {
					select {
					case handlerStarted <- struct{}{}:
					default:
					}
					return nil
				},
			})
			pc.SetTestMaintenanceInterval(5 * time.Millisecond)

			eventID := "99999999-9999-4999-8999-999999999982"
			target := events.RouteIdentity{FlowID: "repo-scaffold", FlowInstance: "repo-scaffold/inst-1", EntityID: artifactActionResultEntityID}
			event := eventtest.RunCreatingRootIngress(
				eventID,
				"repo-scaffold/inst-1/repo_scaffold.repo_commit_succeeded",
				"test",
				"",
				[]byte(`{}`),
				0,
				templateInstanceDeliveryRunID,
				"",
				events.EnvelopeForTargetRoute(events.EnvelopeForEntityID(events.EventEnvelope{}, artifactActionResultEntityID), target),
				time.Now().UTC(),
			)
			route := events.DeliveryRoute{SubscriberType: "node", SubscriberID: "repo-scaffold-node", Target: target}
			storetest.CommitSemanticEventWithRoutes(t, ctx, selected, event, []events.DeliveryRoute{route}, runtimepipelineobligation.ScopeSubscribed)
			claimed, err := selected.ClaimNodeDelivery(ctx, event, route)
			if err != nil {
				t.Fatalf("claim node delivery before retry: %v", err)
			}
			failure := runtimefailures.FromError(errors.New("retry later"), "node-recovery-test", "schedule_retry")
			retrying, err := selected.SettleFailure(ctx, claimed.Claim, runtimedelivery.Settlement{
				Disposition: runtimedelivery.FailureRetry,
				Failure:     &failure.Failure,
				RetryBase:   time.Hour,
			})
			if err != nil || retrying.Status != runtimedelivery.StatusFailed {
				t.Fatalf("schedule node retry = %#v, err=%v", retrying, err)
			}

			expiringEventID := "99999999-9999-4999-8999-999999999983"
			expiringEvent := eventtest.RunCreatingRootIngress(
				expiringEventID,
				"repo-scaffold/inst-1/repo_scaffold.repo_commit_succeeded",
				"test",
				"",
				[]byte(`{}`),
				0,
				templateInstanceDeliveryRunID,
				"",
				events.EnvelopeForTargetRoute(events.EnvelopeForEntityID(events.EventEnvelope{}, artifactActionResultEntityID), target),
				time.Now().UTC(),
			)
			storetest.CommitSemanticEventWithRoutes(t, ctx, selected, expiringEvent, []events.DeliveryRoute{route}, runtimepipelineobligation.ScopeSubscribed)
			expiringClaim, err := selected.ClaimNodeDelivery(ctx, expiringEvent, route)
			if err != nil {
				t.Fatalf("claim node delivery before lease expiry: %v", err)
			}
			if err := pc.RecoverNodeDeliveries(ctx); err != nil {
				t.Fatalf("startup recovery before eligibility: %v", err)
			}

			maintenanceCtx, cancelMaintenance := context.WithCancel(ctx)
			maintenanceDone := make(chan struct{})
			go func() {
				defer close(maintenanceDone)
				pc.RunMaintenance(maintenanceCtx)
			}()
			defer func() {
				cancelMaintenance()
				select {
				case <-maintenanceDone:
				case <-time.After(time.Second):
					t.Errorf("standing recovery did not stop after cancellation")
				}
			}()
			makeNodeDeliveryImmediatelyEligible(t, maintenanceCtx, db, backend.name == "postgres", retrying.DeliveryID)
			expireNodeDeliveryClaim(t, maintenanceCtx, db, backend.name == "postgres", expiringClaim.Snapshot.DeliveryID)
			for recovered := 0; recovered < 2; recovered++ {
				select {
				case <-handlerStarted:
				case <-time.After(2 * time.Second):
					t.Fatalf("standing recovery started %d handlers, want retry-eligible and expired-claim handlers", recovered)
				}
			}
			waitForRecoveredNodeDelivery(t, ctx, selected, eventID, route, 2)
			waitForRecoveredNodeDelivery(t, ctx, selected, expiringEventID, route, 1)
			assertExpiredNodeDeliveryAttemptHistory(t, maintenanceCtx, db, backend.name == "postgres", expiringClaim.Snapshot.DeliveryID)
			if got := deliveryOwner.renewals.Load(); got < 4 {
				t.Fatalf("standing recovery claim renewals = %d, want immediate and final renewal for two handlers", got)
			}
		})
	}
}

func expireNodeDeliveryClaim(t *testing.T, ctx context.Context, db *sql.DB, postgres bool, deliveryID string) {
	t.Helper()
	transaction, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin node claim expiry: %v", err)
	}
	defer func() { _ = transaction.Rollback() }()
	startedAt := time.Now().Add(-2 * time.Hour).UTC()
	expiresAt := time.Now().Add(-time.Hour).UTC()
	deliveryQuery := `UPDATE event_deliveries SET created_at = $1, started_at = $1, updated_at = $2 WHERE delivery_id = $3::uuid AND status = 'in_progress'`
	attemptQuery := `UPDATE event_delivery_attempts SET started_at = $1, lease_expires_at = $2 WHERE delivery_id = $3::uuid AND open_marker = TRUE`
	if !postgres {
		deliveryQuery = `UPDATE event_deliveries SET created_at = ?, started_at = ?, updated_at = ? WHERE delivery_id = ? AND status = 'in_progress'`
		attemptQuery = `UPDATE event_delivery_attempts SET started_at = ?, lease_expires_at = ? WHERE delivery_id = ? AND open_marker = TRUE`
	}
	deliveryArgs := []any{startedAt, expiresAt, deliveryID}
	if !postgres {
		deliveryArgs = []any{startedAt, startedAt, expiresAt, deliveryID}
	}
	if result, execErr := transaction.ExecContext(ctx, deliveryQuery, deliveryArgs...); execErr != nil {
		t.Fatalf("expire node delivery claim: %v", execErr)
	} else if rows, rowsErr := result.RowsAffected(); rowsErr != nil || rows != 1 {
		t.Fatalf("expire node delivery claim affected %d rows, err=%v", rows, rowsErr)
	}
	if result, execErr := transaction.ExecContext(ctx, attemptQuery, startedAt, expiresAt, deliveryID); execErr != nil {
		t.Fatalf("expire node delivery attempt: %v", execErr)
	} else if rows, rowsErr := result.RowsAffected(); rowsErr != nil || rows != 1 {
		t.Fatalf("expire node delivery attempt affected %d rows, err=%v", rows, rowsErr)
	}
	if err := transaction.Commit(); err != nil {
		t.Fatalf("commit node claim expiry: %v", err)
	}
}

func assertExpiredNodeDeliveryAttemptHistory(t *testing.T, ctx context.Context, db *sql.DB, postgres bool, deliveryID string) {
	t.Helper()
	query := `SELECT claim_version, outcome FROM event_delivery_attempts WHERE delivery_id = $1::uuid ORDER BY claim_version`
	if !postgres {
		query = `SELECT claim_version, outcome FROM event_delivery_attempts WHERE delivery_id = ? ORDER BY claim_version`
	}
	rows, err := db.QueryContext(ctx, query, deliveryID)
	if err != nil {
		t.Fatalf("load recovered node attempt history: %v", err)
	}
	defer rows.Close()
	var attempts []struct {
		version int64
		outcome string
	}
	for rows.Next() {
		var attempt struct {
			version int64
			outcome string
		}
		if err := rows.Scan(&attempt.version, &attempt.outcome); err != nil {
			t.Fatalf("scan recovered node attempt: %v", err)
		}
		attempts = append(attempts, attempt)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read recovered node attempts: %v", err)
	}
	if len(attempts) != 2 || attempts[0].version != 1 || attempts[0].outcome != "lease_expired" || attempts[1].version != 2 || attempts[1].outcome != "delivered" {
		t.Fatalf("recovered node attempts = %#v, want lease_expired then delivered", attempts)
	}
}

func makeNodeDeliveryImmediatelyEligible(t *testing.T, ctx context.Context, db *sql.DB, postgres bool, deliveryID string) {
	t.Helper()
	query := `UPDATE event_deliveries SET next_eligible_at = $1 WHERE delivery_id = $2::uuid AND status = 'failed'`
	if !postgres {
		query = `UPDATE event_deliveries SET next_eligible_at = ? WHERE delivery_id = ? AND status = 'failed'`
	}
	result, err := db.ExecContext(ctx, query, time.Now().Add(-time.Hour).UTC(), deliveryID)
	if err != nil {
		t.Fatalf("make node delivery immediately eligible: %v", err)
	}
	if rows, rowsErr := result.RowsAffected(); rowsErr != nil || rows != 1 {
		t.Fatalf("make node delivery eligible affected %d rows, err=%v", rows, rowsErr)
	}
}

func waitForRecoveredNodeDelivery(t *testing.T, ctx context.Context, selected runtimedelivery.Store, eventID string, route events.DeliveryRoute, wantOutcomes int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		proof, err := selected.ProveHandoff(ctx, eventID, route)
		if err != nil {
			t.Fatalf("ProveHandoff: %v", err)
		}
		snapshot, err := selected.Snapshot(ctx, proof.DeliveryID())
		if err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
		if snapshot.Status == runtimedelivery.StatusDelivered {
			assertRecoveredNodeDelivery(t, ctx, selected, eventID, route, wantOutcomes)
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("standing recovery status = %q, want delivered", snapshot.Status)
		}
		<-ticker.C
	}
}

func assertRecoveredNodeDelivery(t *testing.T, ctx context.Context, selected runtimedelivery.Store, eventID string, route events.DeliveryRoute, wantOutcomes int) {
	t.Helper()
	proof, err := selected.ProveHandoff(ctx, eventID, route)
	if err != nil {
		t.Fatalf("ProveHandoff: %v", err)
	}
	snapshot, err := selected.Snapshot(ctx, proof.DeliveryID())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if snapshot.Status != runtimedelivery.StatusDelivered {
		t.Fatalf("recovered delivery status = %q, want delivered", snapshot.Status)
	}
	outcomes, err := selected.Outcomes(ctx, snapshot.DeliveryID)
	if err != nil {
		t.Fatalf("Outcomes: %v", err)
	}
	if len(outcomes) != wantOutcomes {
		t.Fatalf("recovered delivery outcomes = %d, want %d: %#v", len(outcomes), wantOutcomes, outcomes)
	}
}
