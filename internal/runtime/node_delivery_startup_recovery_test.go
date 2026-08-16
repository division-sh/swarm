package runtime_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	swarmruntime "github.com/division-sh/swarm/internal/runtime"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedeliverycontinuation "github.com/division-sh/swarm/internal/runtime/deliverycontinuation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	llm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/semanticviewtest"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
)

type nodeDeliveryRecoveryStore interface {
	externalRuntimeTestDurableEventStore
	runtimepipeline.WorkflowPersistenceOwner
	PipelineObligations() runtimepipelineobligation.Store
}

type renewalTrackingDeliveryStore struct {
	runtimedelivery.Store
	renewals atomic.Int64
}

type startupRecoveryActivationProbe struct {
	runtimedelivery.Store
	activations atomic.Int64
}

func (p *startupRecoveryActivationProbe) ActivateDeliveryAuthority(
	ctx context.Context,
	authority runtimedelivery.ExecutionAuthority,
) error {
	p.activations.Add(1)
	return p.Store.ActivateDeliveryAuthority(ctx, authority)
}

type committedCleanupPipelineOwner struct {
	runtimepipelineobligation.Store
	err error
}

func (o committedCleanupPipelineOwner) Settle(
	ctx context.Context,
	claim runtimepipelineobligation.Claim,
	disposition runtimepipelineobligation.Disposition,
) (runtimepipelineobligation.SettlementOutcome, error) {
	outcome, err := o.Store.Settle(ctx, claim, disposition)
	if err == nil && outcome.Committed() {
		return outcome, o.err
	}
	return outcome, err
}

type settlingDeliveryContinuationDispatcher struct {
	store       runtimedelivery.Store
	authority   runtimedelivery.ExecutionAuthority
	coordinator *runtimedeliverycontinuation.Coordinator
	dispatches  atomic.Int64
}

func (d *settlingDeliveryContinuationDispatcher) DispatchDeliveryContinuation(
	ctx context.Context,
	event events.Event,
	route events.DeliveryRoute,
) error {
	result, err := d.store.ClaimDelivery(ctx, d.authority, event, route)
	if err != nil {
		return err
	}
	claimed, ok := result.Acquired()
	if !ok {
		return fmt.Errorf("continuation dispatch disposition = %s", result.Disposition)
	}
	snapshot, err := d.store.SettleSuccess(ctx, claimed.Claim, nil, 0)
	if err != nil {
		return err
	}
	if err := d.coordinator.Release(snapshot.DeliveryID); err != nil {
		return err
	}
	d.dispatches.Add(1)
	return nil
}

type startupRecoveryOrderStore interface {
	nodeDeliveryRecoveryStore
	swarmruntime.EventPayloadValidationBinder
	swarmruntime.AuthorActivityCatalogRegistrar
	runtimerunlifecycle.CandidateOwner
	runtimemanager.ManagerPersistence
	storetest.AgentFixtureStore
}

type startupRecoveryOrderLLM struct{ llm.NoopRuntime }

func (startupRecoveryOrderLLM) ProviderContract() llm.ProviderContract {
	return llm.AnthropicAPIProviderContract()
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
	persistedSource, err := runtimecorrelation.NewPersistedBundleSourceFact(authorActivityTestBundleSourceFact.BundleHash())
	if err != nil {
		t.Fatalf("construct persisted startup bundle source: %v", err)
	}
	persistedBundleHash, persistedBundleSource := persistedSource.StorageValues()
	for _, backend := range []struct {
		name  string
		setup func(*testing.T) (context.Context, *sql.DB, *sql.DB, startupRecoveryOrderStore)
	}{
		{
			name: "postgres",
			setup: func(t *testing.T) (context.Context, *sql.DB, *sql.DB, startupRecoveryOrderStore) {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				selected := storetest.AdmitPostgresRuntimeStore(t, db)
				ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), templateInstanceDeliveryRunID)
				ctx = runtimecorrelation.WithBundleSourceFact(ctx, persistedSource)
				seedStartupRecoveryPersistedBundle(t, ctx, db, "postgres", persistedSource.BundleHash())
				storetest.RequirePostgresRun(t, ctx, db, storetest.RunFixture{
					Origin: storetest.ScenarioSetupOrigin(), RunID: templateInstanceDeliveryRunID,
					BundleHash: persistedBundleHash, BundleSource: persistedBundleSource,
				})
				return ctx, db, db, selected
			},
		},
		{
			name: "sqlite",
			setup: func(t *testing.T) (context.Context, *sql.DB, *sql.DB, startupRecoveryOrderStore) {
				selected := storetest.StartSQLiteRuntimeStore(t)
				ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), templateInstanceDeliveryRunID)
				ctx = runtimecorrelation.WithBundleSourceFact(ctx, persistedSource)
				seedStartupRecoveryPersistedBundle(t, ctx, storetest.Database(selected), "sqlite", persistedSource.BundleHash())
				storetest.RequireSQLiteRun(t, ctx, storetest.Database(selected), storetest.RunFixture{
					Origin: storetest.ScenarioSetupOrigin(), RunID: templateInstanceDeliveryRunID,
					BundleHash: persistedBundleHash, BundleSource: persistedBundleSource,
				})
				return ctx, nil, storetest.Database(selected), selected
			},
		},
	} {
		t.Run(backend.name, func(t *testing.T) {
			ctx, runtimeSQLDB, _, selected := backend.setup(t)
			workflowPersistence := runtimepipeline.NewWorkflowPersistence(selected)
			if runtimeSQLDB == nil {
				workflowPersistence = runtimepipeline.NewWorkflowPersistence(selected)
			}
			bundle := loadEntitylessStartupRecoveryBundle(t)
			const agentID = "startup-order-agent"
			agentConfig := runtimeTestAgentConfig(t, runtimeactors.AgentConfig{
				ID: agentID, Type: "test", Role: "observer", FlowID: "global", Model: "regular",
				ExecutionMode: "live", Subscriptions: []string{"task.completed"},
			})
			bundle.Agents[agentID] = runtimecontracts.AgentRegistryEntry{
				ID: agentID, Type: "test", Role: "observer", Model: "regular",
				ResolvedIntent: agentConfig.Intent, Subscriptions: []string{"task.completed"},
			}
			source := semanticviewtest.WrapRootAgents(bundle)
			module := newRuntimeTestWorkflowModule(t, source)

			eventID := eventtest.UUID("startup-order-node-event-" + backend.name)
			nodeRoute := startupRecoveryNodeRoute(t, "complete-task")
			event := eventtest.ExistingRunRootIngress(
				eventID, "task.requested", "test", "", []byte(`{}`), 0,
				templateInstanceDeliveryRunID, events.EnvelopeForTargetRoute(events.EventEnvelope{}, nodeRoute.Target.Route()), time.Now().UTC(),
			)
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
			runtime, err := swarmruntime.NewRuntime(ctx, completeExternalRuntimeTestWorkflowDeps(t, selected, swarmruntime.RuntimeDeps{
				Config: &config.Config{Runtime: config.RuntimeConfig{RecoveryOnStartup: true}, LLM: config.LLMConfig{Backend: "anthropic"}},

				EventStore: selected, EventBusDurable: externalRuntimeTestDurableDependencies(selected),
				EventPayloadValidationBinder: selected, AuthorActivityRegistrars: []swarmruntime.AuthorActivityCatalogRegistrar{selected},
				RunLifecycleCandidates: selected, WorkflowPersistence: workflowPersistence,
				ManagerStore:            selected,
				ManagerPersistenceRoles: externalRuntimeTestSelectedManagerRoles(selected), DeliveryStore: selected,
				PipelineObligations: selected.PipelineObligations(),

				Options: swarmruntime.RuntimeOptions{
					SelfCheck: false, WorkflowModule: module, LLMRuntime: startupRecoveryOrderLLM{},
					RuntimeInstanceID: authorActivityTestRuntimeInstanceID, BundleSourceFact: persistedSource,
					ProcessWorkOwner: processOwner,
					TestWorkflowNodeHandlerStartHook: func(context.Context, string, events.Event) error {
						if !hydrated.Load() {
							return errors.New("workflow-node recovery started before persisted agent hydration")
						}
						return nil
					},
				},
			}))
			if err != nil {
				t.Fatalf("NewRuntime: %v", err)
			}
			capability, grant := installExternalRuntimeTestGeneration(t, ctx, selected, runtime)
			t.Cleanup(func() {
				if err := runtime.Shutdown(); err != nil {
					t.Errorf("shutdown startup-order runtime: %v", err)
				}
				if err := capability.Release(context.Background()); err != nil {
					t.Errorf("release startup-order process capability: %v", err)
				}
			})
			if err := runtime.Manager.ReconcileStaticTopologyForStartup(ctx, source); err != nil {
				t.Fatalf("persist startup-order declared agent: %v", err)
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
				ExecutionPosture: executionposture.Live,
				BaseContext:      ctx, LifecycleStore: storetest.AgentLifecycleFixture(selected), DeliveryStore: selected, SemanticSource: source,
				PersistenceRoles:  externalRuntimeTestManagerBusRoles(runtime.Bus),
				WorkflowInstances: runtime.Pipeline, WorkOwner: runtime.WorkOccurrence(), ReceiverExecution: eventreceiver.NormalExecution(),
			}, selected)
			installExternalManagerTestGeneration(t, ctx, runtime.Manager, grant)

			if err := runtime.Start(ctx); err != nil {
				t.Fatalf("Start: %v", err)
			}
			waitForRecoveredNodeDelivery(t, ctx, selected, eventID, nodeRoute, 1)
			if !hydrated.Load() {
				t.Fatal("startup order proof delivered the workflow node before persisted agent hydration")
			}
		})
	}
}

func TestRuntimeStartRecoveryDisabledRejectsExecutableDeliveryInventoryParity(t *testing.T) {
	currentSource, err := runtimecorrelation.NewPersistedBundleSourceFact(authorActivityTestBundleSourceFact.BundleHash())
	if err != nil {
		t.Fatalf("construct current startup source: %v", err)
	}
	foreignSource, err := runtimecorrelation.NewPersistedBundleSourceFact("bundle-v1:sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
	if err != nil {
		t.Fatalf("construct foreign startup source: %v", err)
	}
	modes := []struct {
		name                             string
		recoveryOnStartup                bool
		disablePersistentStartupRecovery bool
	}{
		{name: "config_disabled"},
		{name: "internal_persistent_recovery_disabled", recoveryOnStartup: true, disablePersistentStartupRecovery: true},
	}
	cases := []struct {
		name       string
		route      events.DeliveryRoute
		state      string
		foreign    bool
		wantDenied bool
	}{
		{name: "pending_agent", route: startupRecoveryAgentRoute(t, "startup-agent"), state: "pending", wantDenied: true},
		{name: "pending_node", route: startupRecoveryNodeRoute(t, "complete-task"), state: "pending", wantDenied: true},
		{name: "future_failed", route: startupRecoveryAgentRoute(t, "startup-agent"), state: "future_failed", wantDenied: true},
		{name: "busy_in_progress", route: startupRecoveryAgentRoute(t, "startup-agent"), state: "busy", wantDenied: true},
		{name: "reclaimable_in_progress", route: startupRecoveryNodeRoute(t, "complete-task"), state: "reclaimable", wantDenied: true},
		{name: "foreign_bundle_excluded", route: startupRecoveryAgentRoute(t, "foreign-agent"), state: "pending", foreign: true},
		{name: "empty_control"},
	}

	for _, backend := range []string{"sqlite", "postgres"} {
		for _, mode := range modes {
			for _, test := range cases {
				t.Run(backend+"/"+mode.name+"/"+test.name, func(t *testing.T) {
					var (
						db       *sql.DB
						selected startupRecoveryOrderStore
					)
					if backend == "postgres" {
						_, postgresDB, cleanup := testutil.StartPostgres(t)
						t.Cleanup(cleanup)
						db = postgresDB
						selected = storetest.AdmitPostgresRuntimeStore(t, postgresDB)
					} else {
						sqliteStore := storetest.StartSQLiteRuntimeStore(t)
						db = storetest.Database(sqliteStore)
						selected = sqliteStore
					}

					currentRunID := eventtest.UUID("recovery-disabled-current-" + backend + "-" + mode.name + "-" + test.name)
					currentCtx := startupRecoverySourceContext(currentSource, currentRunID)
					seedStartupRecoverySourceRun(t, currentCtx, db, backend, currentSource, currentRunID)

					var (
						deliveryID  string
						deliveryCtx context.Context
						before      runtimedelivery.Snapshot
					)
					if test.state != "" {
						eventSource := currentSource
						eventRunID := currentRunID
						eventCtx := currentCtx
						if test.foreign {
							eventSource = foreignSource
							eventRunID = eventtest.UUID("recovery-disabled-foreign-" + backend + "-" + mode.name)
							eventCtx = startupRecoverySourceContext(eventSource, eventRunID)
							seedStartupRecoverySourceRun(t, eventCtx, db, backend, eventSource, eventRunID)
						}
						deliveryCtx = eventCtx
						event := eventtest.ExistingRunRootIngress(
							eventtest.UUID("recovery-disabled-event-"+backend+"-"+mode.name+"-"+test.name),
							"task.requested", "test", "", []byte(`{}`), 0, eventRunID,
							events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("recovery-disabled-entity-"+backend+"-"+mode.name+"-"+test.name)),
							time.Now().UTC(),
						)
						storetest.CommitSemanticEventWithInitialFacts(
							t, eventCtx, selected, event, []events.DeliveryRoute{test.route},
							runtimepipelineobligation.ScopeSubscribed, storetest.AcknowledgedPipelineDisposition(),
						)
						deliveryID, err = runtimedelivery.DeliveryID(event.ID(), test.route)
						if err != nil {
							t.Fatalf("derive startup delivery id: %v", err)
						}
						switch test.state {
						case "pending":
						case "future_failed":
							claimed, claimErr := storetest.ClaimDelivery(eventCtx, selected, event, test.route)
							if claimErr != nil {
								t.Fatalf("claim future-failed startup delivery: %v", claimErr)
							}
							failure := runtimefailures.FromError(errors.New("retry after startup"), "startup-recovery-test", "schedule_retry")
							if _, settleErr := selected.SettleFailure(eventCtx, claimed.Claim, runtimedelivery.Settlement{
								Disposition: runtimedelivery.FailureRetry,
								Failure:     &failure.Failure,
								RetryBase:   time.Hour,
							}); settleErr != nil {
								t.Fatalf("settle future-failed startup delivery: %v", settleErr)
							}
						case "busy":
							if _, claimErr := storetest.ClaimDelivery(eventCtx, selected, event, test.route); claimErr != nil {
								t.Fatalf("claim busy startup delivery: %v", claimErr)
							}
						case "reclaimable":
							if _, claimErr := storetest.ClaimDelivery(eventCtx, selected, event, test.route); claimErr != nil {
								t.Fatalf("claim reclaimable startup delivery: %v", claimErr)
							}
							expireNodeDeliveryClaim(t, eventCtx, db, backend == "postgres", deliveryID)
						default:
							t.Fatalf("unknown startup delivery state %q", test.state)
						}
						before, err = selected.Snapshot(eventCtx, deliveryID)
						if err != nil {
							t.Fatalf("load prepared startup delivery: %v", err)
						}
					}

					workflowPersistence := runtimepipeline.NewWorkflowPersistence(selected)
					if backend == "sqlite" {
						workflowPersistence = runtimepipeline.NewWorkflowPersistence(selected)
					}
					module := loadStartupRecoveryWorkflowModule(t)
					processOwner := worklifetime.NewProcess()
					activationProbe := &startupRecoveryActivationProbe{Store: selected}
					handlerStarts := atomic.Int64{}
					runtime, runtimeErr := swarmruntime.NewRuntime(currentCtx, completeExternalRuntimeTestWorkflowDeps(t, selected, swarmruntime.RuntimeDeps{
						Config: &config.Config{
							Runtime: config.RuntimeConfig{RecoveryOnStartup: mode.recoveryOnStartup},
							LLM:     config.LLMConfig{Backend: "anthropic"},
						},

						EventStore: selected, EventBusDurable: externalRuntimeTestDurableDependencies(selected),
						EventPayloadValidationBinder: selected, AuthorActivityRegistrars: []swarmruntime.AuthorActivityCatalogRegistrar{selected},
						RunLifecycleCandidates: selected, WorkflowPersistence: workflowPersistence,
						ManagerStore:            selected,
						ManagerPersistenceRoles: externalRuntimeTestSelectedManagerRoles(selected), DeliveryStore: activationProbe,
						PipelineObligations: selected.PipelineObligations(),

						Options: swarmruntime.RuntimeOptions{
							SelfCheck: false, WorkflowModule: module, LLMRuntime: startupRecoveryOrderLLM{},
							RuntimeInstanceID: authorActivityTestRuntimeInstanceID, BundleSourceFact: currentSource,
							ProcessWorkOwner:                 processOwner,
							DisablePersistentStartupRecovery: mode.disablePersistentStartupRecovery,
							TestWorkflowNodeHandlerStartHook: func(context.Context, string, events.Event) error {
								handlerStarts.Add(1)
								return nil
							},
						},
					}))
					if runtimeErr != nil {
						t.Fatalf("NewRuntime: %v", runtimeErr)
					}
					capability, _ := installExternalRuntimeTestGeneration(t, currentCtx, selected, runtime)
					startErr := runtime.Start(currentCtx)
					if test.wantDenied {
						if startErr == nil {
							t.Fatal("Runtime.Start succeeded with recovery disabled and executable delivery work")
						}
						if got := activationProbe.activations.Load(); got != 0 {
							t.Fatalf("delivery authority activations = %d, want denial before activation", got)
						}
					} else {
						if startErr != nil {
							t.Fatalf("Runtime.Start control failed: %v", startErr)
						}
						if got := activationProbe.activations.Load(); got != 1 {
							t.Fatalf("delivery authority activations = %d, want one for admitted startup", got)
						}
					}
					if got := handlerStarts.Load(); got != 0 {
						t.Fatalf("workflow handler starts = %d, want none before recovery admission", got)
					}
					if deliveryID != "" {
						after, snapshotErr := selected.Snapshot(deliveryCtx, deliveryID)
						if snapshotErr != nil {
							t.Fatalf("load delivery after startup decision: %v", snapshotErr)
						}
						if !reflect.DeepEqual(after, before) {
							t.Fatalf("startup decision mutated delivery\nbefore: %#v\nafter:  %#v", before, after)
						}
					}
					if shutdownErr := runtime.Shutdown(); shutdownErr != nil {
						t.Fatalf("shutdown startup-decision runtime: %v", shutdownErr)
					}
					releaseExternalRuntimeTestCapability(t, capability)
					joinCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					if _, joinErr := processOwner.Join(joinCtx); joinErr != nil {
						t.Fatalf("join startup-decision process owner: %v", joinErr)
					}
				})
			}
		}
	}
}

func TestCommittedPipelineHandoffCleanupFailureWakesExactDeliveryOnceParity(t *testing.T) {
	source, err := runtimecorrelation.NewPersistedBundleSourceFact(authorActivityTestBundleSourceFact.BundleHash())
	if err != nil {
		t.Fatalf("construct pipeline handoff source: %v", err)
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var (
				db       *sql.DB
				selected startupRecoveryOrderStore
			)
			if backend == "postgres" {
				_, postgresDB, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				db = postgresDB
				selected = storetest.AdmitPostgresRuntimeStore(t, postgresDB)
			} else {
				sqliteStore := storetest.StartSQLiteRuntimeStore(t)
				db = storetest.Database(sqliteStore)
				selected = sqliteStore
			}
			runID := eventtest.UUID("pipeline-handoff-cleanup-run-" + backend)
			ctx := startupRecoverySourceContext(source, runID)
			seedStartupRecoverySourceRun(t, ctx, db, backend, source, runID)
			route := startupRecoveryNodeRoute(t, "complete-task")
			event := eventtest.ExistingRunRootIngress(
				eventtest.UUID("pipeline-handoff-cleanup-event-"+backend),
				"task.requested", "test", "", []byte(`{}`), 0, runID,
				events.EnvelopeForTargetRoute(events.EventEnvelope{}, route.Target.Route()),
				time.Now().UTC(),
			)
			storetest.CommitSemanticEventWithInitialFacts(
				t, ctx, selected, event, []events.DeliveryRoute{route},
				runtimepipelineobligation.ScopeSubscribed, nil,
			)
			deliveryID, err := runtimedelivery.DeliveryID(event.ID(), route)
			if err != nil {
				t.Fatalf("derive pipeline handoff delivery id: %v", err)
			}
			snapshot, err := selected.Snapshot(ctx, deliveryID)
			if err != nil {
				t.Fatalf("load pipeline handoff delivery: %v", err)
			}

			injectedErr := errors.New("injected committed pipeline cleanup failure")
			pipelineOwner := committedCleanupPipelineOwner{
				Store: selected.PipelineObligations(),
				err:   injectedErr,
			}
			bus, err := newScopedTestEventBus(t, selected, runtimebus.EventBusOptions{
				PipelineObligations: pipelineOwner,
				BundleSourceFact:    source,
				DeliveryAuthority:   snapshot.Authority,
			})
			if err != nil {
				t.Fatalf("construct pipeline handoff event bus: %v", err)
			}
			dispatcher := &settlingDeliveryContinuationDispatcher{
				store: selected, authority: snapshot.Authority,
			}
			coordinator, err := runtimedeliverycontinuation.New(
				selected,
				snapshot.Authority,
				runtimeTestEventBusWorkOwner(t, bus),
				dispatcher,
				func(_ context.Context, reportErr error) { t.Errorf("pipeline handoff continuation: %v", reportErr) },
			)
			if err != nil {
				t.Fatalf("construct pipeline handoff coordinator: %v", err)
			}
			dispatcher.coordinator = coordinator
			if err := bus.SetDeliveryContinuationOwner(coordinator); err != nil {
				t.Fatalf("install pipeline handoff coordinator: %v", err)
			}
			if err := coordinator.Start(ctx); err != nil {
				t.Fatalf("start pipeline handoff coordinator: %v", err)
			}
			t.Cleanup(func() {
				retireCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := coordinator.Retire(retireCtx); err != nil {
					t.Errorf("retire pipeline handoff coordinator: %v", err)
				}
			})
			if got := dispatcher.dispatches.Load(); got != 0 {
				t.Fatalf("hidden pre-settlement delivery dispatches = %d, want 0", got)
			}

			result, sweepErr := bus.SweepPipelineObligations(ctx, 1)
			if !errors.Is(sweepErr, injectedErr) {
				t.Fatalf("pipeline sweep error = %v, want committed cleanup evidence", sweepErr)
			}
			if result.Settled != 0 {
				t.Fatalf("pipeline sweep reported settled = %d despite auxiliary cleanup error", result.Settled)
			}
			deadline := time.Now().Add(2 * time.Second)
			for dispatcher.dispatches.Load() == 0 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if got := dispatcher.dispatches.Load(); got != 1 {
				t.Fatalf("post-commit delivery dispatches = %d, want 1 without unrelated wake", got)
			}
			delivered, err := selected.Snapshot(ctx, deliveryID)
			if err != nil {
				t.Fatalf("load settled pipeline handoff delivery: %v", err)
			}
			if delivered.Status != runtimedelivery.StatusDelivered {
				t.Fatalf("post-commit delivery status = %s, want delivered", delivered.Status)
			}
			coordinator.Signal()
			select {
			case <-time.After(25 * time.Millisecond):
			}
			if got := dispatcher.dispatches.Load(); got != 1 {
				t.Fatalf("post-terminal coordinator wake dispatched %d times, want exactly once", got)
			}
		})
	}
}

func startupRecoveryAgentRoute(t *testing.T, agentID string) events.DeliveryRoute {
	t.Helper()
	name, err := runtimeagentidentity.DeclaredName(agentID, "global")
	if err != nil {
		t.Fatalf("construct startup recovery agent name: %v", err)
	}
	identity, err := runtimeagentidentity.New(name, runtimeagentidentity.RootRoute())
	if err != nil {
		t.Fatalf("construct startup recovery agent identity: %v", err)
	}
	return events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(agentID), AgentIdentity: identity}
}

func startupRecoveryNodeRoute(t testing.TB, nodeID string) events.DeliveryRoute {
	t.Helper()
	return events.DeliveryRoute{
		Recipient: events.MustNodeDeliveryRecipient(identitytest.RootNode(t, nodeID)),
		Target: events.MustEntitylessReceiverTarget(events.RouteIdentity{
			FlowID: "test-boot-success", FlowInstance: templateInstanceDeliveryRunID,
		}),
	}
}

func startupRecoverySourceContext(source runtimecorrelation.BundleSourceFact, runID string) context.Context {
	ctx := runtimecorrelation.WithBundleSourceFact(context.Background(), source)
	ctx = runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(
		authorActivityTestRuntimeInstanceID,
		source.BundleHash(),
	))
	return runtimecorrelation.WithRunID(ctx, runID)
}

func seedStartupRecoverySourceRun(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	backend string,
	source runtimecorrelation.BundleSourceFact,
	runID string,
) {
	t.Helper()
	bundleHash, bundleSource := source.StorageValues()
	seedStartupRecoveryPersistedBundle(t, ctx, db, backend, bundleHash)
	fixture := storetest.RunFixture{
		Origin: storetest.ScenarioSetupOrigin(), RunID: runID,
		BundleHash: bundleHash, BundleSource: bundleSource,
	}
	if backend == "postgres" {
		storetest.RequirePostgresRun(t, ctx, db, fixture)
		return
	}
	storetest.RequireSQLiteRun(t, ctx, db, fixture)
}

func loadStartupRecoveryWorkflowModule(t *testing.T) runtimepipeline.WorkflowModule {
	t.Helper()
	return newRuntimeTestWorkflowModule(t, semanticview.Wrap(loadEntitylessStartupRecoveryBundle(t)))
}

func loadEntitylessStartupRecoveryBundle(t *testing.T) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	repoRoot := filepath.Clean(filepath.Join("..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier8-boot-verification", "test-boot-success")
	files := make(map[string]string)
	for _, name := range []string{"package.yaml", "schema.yaml", "events.yaml", "nodes.yaml", "agents.yaml", "policy.yaml"} {
		body, err := os.ReadFile(filepath.Join(fixtureRoot, name))
		if err != nil {
			t.Fatalf("read startup recovery fixture %s: %v", name, err)
		}
		files[name] = string(body)
	}
	files["nodes.yaml"] = strings.ReplaceAll(files["nodes.yaml"], "\n      advances_to: done", "")
	return loadRuntimeTempBundle(t, files)
}

func seedStartupRecoveryPersistedBundle(t *testing.T, ctx context.Context, db *sql.DB, backend, bundleHash string) {
	t.Helper()
	query := `
		INSERT INTO bundles (bundle_hash, content_yaml, parsed_json)
		VALUES ($1, 'name: startup-recovery-test', '{}'::jsonb)
		ON CONFLICT (bundle_hash) DO NOTHING`
	if backend == "sqlite" {
		query = `
			INSERT INTO bundles (bundle_hash, content_yaml, parsed_json)
			VALUES (?, 'name: startup-recovery-test', '{}')
			ON CONFLICT (bundle_hash) DO NOTHING`
	}
	if _, err := db.ExecContext(ctx, query, bundleHash); err != nil {
		t.Fatalf("seed persisted startup bundle: %v", err)
	}
}

func (s *renewalTrackingDeliveryStore) RenewClaim(ctx context.Context, claim runtimedelivery.Claim) (runtimedelivery.Snapshot, error) {
	s.renewals.Add(1)
	return s.Store.RenewClaim(ctx, claim)
}

func startNodeDeliveryContinuation(
	t *testing.T,
	ctx context.Context,
	eventBus *runtimebus.EventBus,
	selected runtimedelivery.Store,
	workOwner worklifetime.Occurrence,
	eventID string,
	route events.DeliveryRoute,
) *runtimedeliverycontinuation.Coordinator {
	t.Helper()
	deliveryID, err := runtimedelivery.DeliveryID(eventID, route)
	if err != nil {
		t.Fatalf("derive node recovery delivery identity: %v", err)
	}
	snapshot, err := selected.Snapshot(ctx, deliveryID)
	if err != nil {
		t.Fatalf("load node recovery delivery authority: %v", err)
	}
	if err := selected.ActivateDeliveryAuthority(ctx, snapshot.Authority); err != nil {
		t.Fatalf("activate node recovery delivery authority: %v", err)
	}
	if err := eventBus.SetDeliveryAuthority(snapshot.Authority); err != nil {
		t.Fatalf("configure node recovery delivery authority: %v", err)
	}
	coordinator, err := runtimedeliverycontinuation.New(
		selected,
		snapshot.Authority,
		workOwner,
		eventBus,
		func(_ context.Context, reportErr error) {
			t.Errorf("node delivery continuation failed: %v", reportErr)
		},
	)
	if err != nil {
		t.Fatalf("construct node delivery continuation coordinator: %v", err)
	}
	if err := eventBus.SetDeliveryContinuationOwner(coordinator); err != nil {
		t.Fatalf("configure node delivery continuation owner: %v", err)
	}
	if err := coordinator.Start(ctx); err != nil {
		t.Fatalf("start node delivery continuation coordinator: %v", err)
	}
	t.Cleanup(func() {
		retireCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := coordinator.Retire(retireCtx); err != nil {
			t.Errorf("retire node delivery continuation coordinator: %v", err)
		}
	})
	return coordinator
}

func TestDeliveryContinuationCoordinatorRecoversNodeDeliveriesThroughCanonicalSelectedStore(t *testing.T) {
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
				storetest.RequireSQLiteRun(t, ctx, storetest.Database(selected), storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: templateInstanceDeliveryRunID})
				return ctx, storetest.Database(selected), selected
			},
		},
	} {
		t.Run(backend.name, func(t *testing.T) {
			ctx, db, selected := backend.setup(t)
			bundle := loadRuntimeTempBundle(t, artifactActionResultDeliveryFixtureFiles())
			source := semanticview.Wrap(bundle)
			var pc *runtimepipeline.PipelineCoordinator
			bus, err := newScopedTestEventBus(t, selected, runtimebus.EventBusOptions{
				ContractBundle: source,
				InterceptorProvider: func() []runtimebus.EventInterceptor {
					if pc == nil {
						return nil
					}
					return []runtimebus.EventInterceptor{pc}
				},
			})
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			workflowPersistence := runtimepipeline.NewWorkflowPersistence(selected)
			if backend.name == "sqlite" {
				workflowPersistence = runtimepipeline.NewWorkflowPersistence(selected)
			}
			deliveryOwner := &renewalTrackingDeliveryStore{Store: selected}
			workOwner := runtimeTestEventBusWorkOwner(t, bus)
			pc = newExternalRuntimeTestPipelineCoordinator(t, bus, db, selected, runtimepipeline.PipelineCoordinatorOptions{
				WorkOwner:           workOwner,
				Module:              newRuntimeTestWorkflowModule(t, source),
				Persistence:         workflowPersistence,
				RunLifecycle:        selected,
				PipelineObligations: selected.PipelineObligations(),
				DeliveryStore:       deliveryOwner,
				DeliveryRuntime:     bus,
				FlowRoutes:          bus,
			})

			if _, err := pc.MaterializeInitialEntry(testLiveExecutionContext(ctx), artifactActionResultWorkflowInstance(), time.Now().UTC()); err != nil {
				t.Fatalf("seed workflow instance: %v", err)
			}

			eventID := "99999999-9999-4999-8999-999999999981"
			target := events.RouteIdentity{FlowID: "repo-scaffold", FlowInstance: "repo-scaffold/inst-1", EntityID: artifactActionResultEntityID}
			event := eventtest.ExistingRunRootIngressWithRoutingSource(
				eventID,
				"repo-scaffold/inst-1/repo_scaffold.repo_commit_succeeded",
				"test",
				"",
				[]byte(`{}`),
				0,
				templateInstanceDeliveryRunID,
				events.EnvelopeForTargetRoute(events.EnvelopeForEntityID(events.EventEnvelope{}, artifactActionResultEntityID), target),
				eventtest.ConcreteTemplateRoutingSource(target.FlowID, target.FlowInstance, target.EntityID),
				time.Now().UTC(),
			)
			route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(identitytest.FlowNode(t, "repo-scaffold", "repo-scaffold-node")), Target: events.MustExistingEntityTarget(target)}
			storetest.CommitSemanticEventWithInitialFacts(
				t, ctx, selected, event, []events.DeliveryRoute{route},
				runtimepipelineobligation.ScopeSubscribed, storetest.AcknowledgedPipelineDisposition(),
			)

			continuations := startNodeDeliveryContinuation(t, ctx, bus, deliveryOwner, workOwner, eventID, route)
			assertRecoveredNodeDelivery(t, ctx, selected, eventID, route, 1)
			if got := deliveryOwner.renewals.Load(); got < 2 {
				t.Fatalf("claim renewals = %d, want immediate and final handler renewal", got)
			}
			continuations.Signal()
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
				storetest.RequireSQLiteRun(t, ctx, storetest.Database(selected), storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: templateInstanceDeliveryRunID})
				return ctx, storetest.Database(selected), selected
			},
		},
	} {
		t.Run(backend.name, func(t *testing.T) {
			ctx, db, selected := backend.setup(t)
			bundle := loadRuntimeTempBundle(t, artifactActionResultDeliveryFixtureFiles())
			source := semanticview.Wrap(bundle)
			var pc *runtimepipeline.PipelineCoordinator
			bus, err := newScopedTestEventBus(t, selected, runtimebus.EventBusOptions{
				ContractBundle: source,
				InterceptorProvider: func() []runtimebus.EventInterceptor {
					if pc == nil {
						return nil
					}
					return []runtimebus.EventInterceptor{pc}
				},
			})
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			workflowPersistence := runtimepipeline.NewWorkflowPersistence(selected)
			if backend.name == "sqlite" {
				workflowPersistence = runtimepipeline.NewWorkflowPersistence(selected)
			}
			workOwner := runtimeTestEventBusWorkOwner(t, bus)
			pc = newExternalRuntimeTestPipelineCoordinator(t, bus, db, selected, runtimepipeline.PipelineCoordinatorOptions{
				WorkOwner:           workOwner,
				Module:              newRuntimeTestWorkflowModule(t, source),
				Persistence:         workflowPersistence,
				RunLifecycle:        selected,
				PipelineObligations: selected.PipelineObligations(),
				DeliveryStore:       selected,
				DeliveryRuntime:     bus,
				FlowRoutes:          bus,
			})

			if _, err := pc.MaterializeInitialEntry(testLiveExecutionContext(ctx), artifactActionResultWorkflowInstance(), time.Now().UTC()); err != nil {
				t.Fatalf("seed healthy workflow instance: %v", err)
			}

			poisonEntityID := eventtest.UUID("node-recovery-poison-entity")
			poisonTarget := events.RouteIdentity{FlowID: "repo-scaffold", FlowInstance: "repo-scaffold/poison", EntityID: poisonEntityID}
			poisonInstance := artifactActionResultWorkflowInstance()
			poisonInstance.InstanceID = "poison"
			poisonInstance.StorageRef = poisonTarget.FlowInstance
			poisonInstance.EntityID = poisonEntityID
			poisonInstance.Fields = map[string]any{
				"repo_id": "poison-repo", "namespace": "tenant-alpha", "partition_key": "poison",
				"display_slug": "Poison", "source_record_id": "poison-record",
			}
			if _, err := pc.MaterializeInitialEntry(testLiveExecutionContext(ctx), poisonInstance, time.Now().UTC()); err != nil {
				t.Fatalf("seed poison workflow instance: %v", err)
			}
			installNodeRecoveryPoisonMutation(t, ctx, db, backend.name == "postgres", poisonEntityID)
			poison := eventtest.ExistingRunRootIngressWithRoutingSource(
				eventtest.UUID("node-recovery-poison-event"),
				"repo-scaffold/poison/repo_scaffold.repo_commit_succeeded",
				"test", "", []byte(`{}`), 0, templateInstanceDeliveryRunID,
				events.EnvelopeForTargetRoute(events.EnvelopeForEntityID(events.EventEnvelope{}, poisonEntityID), poisonTarget),
				eventtest.ConcreteTemplateRoutingSource(poisonTarget.FlowID, poisonTarget.FlowInstance, poisonTarget.EntityID),
				time.Now().UTC().Add(-time.Minute),
			)
			poisonRoute := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(identitytest.FlowNode(t, "repo-scaffold", "repo-scaffold-node")), Target: events.MustExistingEntityTarget(poisonTarget)}
			storetest.CommitSemanticEventWithInitialFacts(
				t, ctx, selected, poison, []events.DeliveryRoute{poisonRoute},
				runtimepipelineobligation.ScopeSubscribed, storetest.AcknowledgedPipelineDisposition(),
			)

			healthyTarget := events.RouteIdentity{FlowID: "repo-scaffold", FlowInstance: "repo-scaffold/inst-1", EntityID: artifactActionResultEntityID}
			healthy := eventtest.ExistingRunRootIngressWithRoutingSource(
				eventtest.UUID("node-recovery-healthy-event"),
				"repo-scaffold/inst-1/repo_scaffold.repo_commit_succeeded",
				"test", "", []byte(`{}`), 0, templateInstanceDeliveryRunID,
				events.EnvelopeForTargetRoute(events.EnvelopeForEntityID(events.EventEnvelope{}, artifactActionResultEntityID), healthyTarget),
				eventtest.ConcreteTemplateRoutingSource(healthyTarget.FlowID, healthyTarget.FlowInstance, healthyTarget.EntityID),
				time.Now().UTC(),
			)
			healthyRoute := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(identitytest.FlowNode(t, "repo-scaffold", "repo-scaffold-node")), Target: events.MustExistingEntityTarget(healthyTarget)}
			storetest.CommitSemanticEventWithInitialFacts(
				t, ctx, selected, healthy, []events.DeliveryRoute{healthyRoute},
				runtimepipelineobligation.ScopeSubscribed, storetest.AcknowledgedPipelineDisposition(),
			)

			startNodeDeliveryContinuation(t, ctx, bus, selected, workOwner, poison.ID(), poisonRoute)
			poisonDeliveryID, err := runtimedelivery.DeliveryID(poison.ID(), poisonRoute)
			if err != nil {
				t.Fatalf("derive poison delivery identity: %v", err)
			}
			poisonSnapshot, err := selected.Snapshot(ctx, poisonDeliveryID)
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
				storetest.RequireSQLiteRun(t, ctx, storetest.Database(selected), storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: templateInstanceDeliveryRunID})
				return ctx, storetest.Database(selected), selected
			},
		},
	} {
		t.Run(backend.name, func(t *testing.T) {
			ctx, db, selected := backend.setup(t)
			bundle := loadRuntimeTempBundle(t, artifactActionResultDeliveryFixtureFiles())
			source := semanticview.Wrap(bundle)
			var pc *runtimepipeline.PipelineCoordinator
			bus, err := newScopedTestEventBus(t, selected, runtimebus.EventBusOptions{
				ContractBundle: source,
				InterceptorProvider: func() []runtimebus.EventInterceptor {
					if pc == nil {
						return nil
					}
					return []runtimebus.EventInterceptor{pc}
				},
			})
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			workflowPersistence := runtimepipeline.NewWorkflowPersistence(selected)
			if backend.name == "sqlite" {
				workflowPersistence = runtimepipeline.NewWorkflowPersistence(selected)
			}
			handlerStarted := make(chan struct{}, 4)
			deliveryOwner := &renewalTrackingDeliveryStore{Store: selected}
			workOwner := runtimeTestEventBusWorkOwner(t, bus)
			pc = newExternalRuntimeTestPipelineCoordinator(t, bus, db, selected, runtimepipeline.PipelineCoordinatorOptions{
				WorkOwner:           workOwner,
				Module:              newRuntimeTestWorkflowModule(t, source),
				Persistence:         workflowPersistence,
				RunLifecycle:        selected,
				PipelineObligations: selected.PipelineObligations(),
				DeliveryStore:       deliveryOwner,
				DeliveryRuntime:     bus,
				FlowRoutes:          bus,
				TestWorkflowNodeHandlerStartHook: func(context.Context, string, events.Event) error {
					select {
					case handlerStarted <- struct{}{}:
					default:
					}
					return nil
				},
			})

			if _, err := pc.MaterializeInitialEntry(testLiveExecutionContext(ctx), artifactActionResultWorkflowInstance(), time.Now().UTC()); err != nil {
				t.Fatalf("seed workflow instance: %v", err)
			}

			eventID := "99999999-9999-4999-8999-999999999982"
			target := events.RouteIdentity{FlowID: "repo-scaffold", FlowInstance: "repo-scaffold/inst-1", EntityID: artifactActionResultEntityID}
			event := eventtest.ExistingRunRootIngressWithRoutingSource(
				eventID,
				"repo-scaffold/inst-1/repo_scaffold.repo_commit_succeeded",
				"test",
				"",
				[]byte(`{}`),
				0,
				templateInstanceDeliveryRunID,
				events.EnvelopeForTargetRoute(events.EnvelopeForEntityID(events.EventEnvelope{}, artifactActionResultEntityID), target),
				eventtest.ConcreteTemplateRoutingSource(target.FlowID, target.FlowInstance, target.EntityID),
				time.Now().UTC(),
			)
			route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(identitytest.FlowNode(t, "repo-scaffold", "repo-scaffold-node")), Target: events.MustExistingEntityTarget(target)}
			storetest.CommitSemanticEventWithInitialFacts(
				t, ctx, selected, event, []events.DeliveryRoute{route},
				runtimepipelineobligation.ScopeSubscribed, storetest.AcknowledgedPipelineDisposition(),
			)
			claimed, err := storetest.ClaimDelivery(ctx, selected, event, route)
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
			expiringEvent := eventtest.ExistingRunRootIngressWithRoutingSource(
				expiringEventID,
				"repo-scaffold/inst-1/repo_scaffold.repo_commit_succeeded",
				"test",
				"",
				[]byte(`{}`),
				0,
				templateInstanceDeliveryRunID,
				events.EnvelopeForTargetRoute(events.EnvelopeForEntityID(events.EventEnvelope{}, artifactActionResultEntityID), target),
				eventtest.ConcreteTemplateRoutingSource(target.FlowID, target.FlowInstance, target.EntityID),
				time.Now().UTC(),
			)
			storetest.CommitSemanticEventWithInitialFacts(
				t, ctx, selected, expiringEvent, []events.DeliveryRoute{route},
				runtimepipelineobligation.ScopeSubscribed, storetest.AcknowledgedPipelineDisposition(),
			)
			expiringClaim, err := storetest.ClaimDelivery(ctx, selected, expiringEvent, route)
			if err != nil {
				t.Fatalf("claim node delivery before lease expiry: %v", err)
			}
			continuations := startNodeDeliveryContinuation(t, ctx, bus, deliveryOwner, workOwner, eventID, route)
			makeNodeDeliveryImmediatelyEligible(t, ctx, db, backend.name == "postgres", retrying.DeliveryID)
			expireNodeDeliveryClaim(t, ctx, db, backend.name == "postgres", expiringClaim.Snapshot.DeliveryID)
			continuations.Signal()
			for recovered := 0; recovered < 2; recovered++ {
				select {
				case <-handlerStarted:
				case <-time.After(2 * time.Second):
					t.Fatalf("standing recovery started %d handlers, want retry-eligible and expired-claim handlers", recovered)
				}
			}
			waitForRecoveredNodeDelivery(t, ctx, selected, eventID, route, 2)
			waitForRecoveredNodeDelivery(t, ctx, selected, expiringEventID, route, 1)
			assertExpiredNodeDeliveryAttemptHistory(t, ctx, db, backend.name == "postgres", expiringClaim.Snapshot.DeliveryID)
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
			t.Fatalf("standing recovery status = %q failure=%+v, want delivered", snapshot.Status, snapshot.Failure)
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
		t.Fatalf("recovered delivery = %#v failure=%+v, want delivered", snapshot, snapshot.Failure)
	}
	outcomes, err := selected.Outcomes(ctx, snapshot.DeliveryID)
	if err != nil {
		t.Fatalf("Outcomes: %v", err)
	}
	if len(outcomes) != wantOutcomes {
		t.Fatalf("recovered delivery outcomes = %d, want %d: %#v", len(outcomes), wantOutcomes, outcomes)
	}
}
