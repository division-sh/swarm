package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"

	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"

	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"

	agentfixture "github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/division-sh/swarm/internal/testutil"

	"github.com/google/uuid"
	"github.com/lib/pq"
)

type agentFixtureFlowStore interface {
	agentfixture.Store
	runtimemanager.AgentLifecycleOutcomeReader
	runtimedelivery.Store
	LoadDynamicFlowRuntimeReadiness(context.Context, string, runtimeflowidentity.Route) (runtimepipeline.DynamicFlowRuntimeReadiness, bool, error)
	BeginDynamicFlowRuntimeActivation(context.Context, runtimepipeline.DynamicFlowRuntimeActivationRequest) (runtimepipeline.DynamicFlowRuntimeActivationAdmissionResult, error)
	CommitFlowInstanceActivation(context.Context, runtimebus.FlowInstanceActivationCommand) (runtimepipeline.CommittedFlowInstanceActivation, error)
}

type agentFixtureFlowActivationCommitter struct {
	store agentFixtureFlowStore
}

func (o agentFixtureFlowActivationCommitter) CommitFlowInstanceActivation(ctx context.Context, plan runtimepipeline.FlowInstanceActivationPlan) (runtimepipeline.CommittedFlowInstanceActivation, error) {
	return o.store.CommitFlowInstanceActivation(ctx, runtimebus.FlowInstanceActivationCommand{
		Plan: plan,
	})
}

func TestAgentFixtureExactFlowAuthorityParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx, selected := newAgentFixtureAuthorityStore(t, backend)
			runID := uuid.NewString()
			ctx = runtimecorrelation.WithRunID(storeTestWorkContext(t, ctx), runID)
			ctx = runtimeeffects.WithExecutionMode(ctx, runtimeeffects.ExecutionModeLive)
			ctx = storeTestOriginalProcessContext(t, ctx, selected)
			requireRunFixtureForTest(t, ctx, selected, semanticRunFixture{
				Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID,
			})
			bus := &sqliteFlowActivationBus{}
			bundle := sqliteFlowActivationBundle(t)
			workflowStore := configureAgentFixtureFlowLifecycle(t, selected, bus, bundle)
			lifecycle := agentfixture.Lifecycle(t, selected)
			if _, err := lifecycle.(runtimemanager.RunExecutionOwner).InspectRunExecutionOwnership(ctx, runID); err == nil {
				t.Fatal("fixture inspection granted ownership before generation admission")
			}
			coordinate := runtimeagenttopology.SourceCoordinate{BundleHash: authorActivityTestBundleHash}
			plan, err := runtimeagenttopology.NewSourceSetPlan([]runtimeagenttopology.SourceCoordinate{coordinate}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := agentfixture.AdmitGeneration(t, ctx, selected, plan, runtimeagenttopology.SourceCoordinate{}); err == nil {
				t.Fatal("fixture admitted a missing source coordinate")
			}
			capability, err := agentfixture.ProcessCapability(t, ctx, selected)
			if err != nil {
				t.Fatal(err)
			}
			if _, exists, err := capability.CurrentSourceSet(ctx); err != nil || exists {
				t.Fatalf("refused admission changed source plan: exists=%v err=%v", exists, err)
			}
			grant, err := agentfixture.AdmitGeneration(t, ctx, selected, plan, coordinate)
			if err != nil {
				t.Fatalf("admit flow fixture generation: %v", err)
			}
			admitAgentFixtureFlowGrant(t, ctx, grant)
			manager := ownStoreTestAgentManager(t, runtimemanager.NewAgentManagerWithOptions(bus, nil, runtimemanager.AgentManagerOptions{
				ExecutionPosture:   executionposture.Live,
				BaseContext:        ctx,
				SourceArtifactFact: mustStoreTestSourceArtifactFact(authorActivityTestBundleHash),
				SemanticSource:     semanticview.Wrap(bundle),
				WorkflowInstances:  workflowStore,
				LLMBackend:         "anthropic",
				LifecycleStore:     lifecycle,
				DeliveryStore:      selected,
				WorkOwner:          storeTestWorkOwner(t),
				PersistenceRoles: runtimemanager.PersistenceRoles{
					LifecycleState:   selected,
					LifecycleOutcome: selected,
					AgentRoutes:      bus,
					FlowActivation:   agentFixtureFlowActivationCommitter{store: selected},
				},
				ReceiverExecution: eventreceiver.NormalExecution(),
			}, selected))

			activate := func(instanceID string) error {
				return manager.ActivateFlowInstance(ctx, sqliteKeyedFlowActivationRequest(t, ctx, bundle, instanceID))
			}
			activateFixtureStructuralRoot(t, ctx, manager, bundle, time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC))
			if err := activate("sequential-a"); err != nil {
				t.Fatalf("activate first sequential flow: %v", err)
			}
			firstPlan := currentAgentFixtureSourceSet(t, ctx, selected)
			if err := activate("sequential-b"); err != nil {
				t.Fatalf("activate second sequential flow: %v", err)
			}
			secondPlan := currentAgentFixtureSourceSet(t, ctx, selected)
			if firstPlan.Revision != secondPlan.Revision || len(secondPlan.Agents) != 0 {
				t.Fatalf("sequential flow activation changed static source set: first=%#v second=%#v", firstPlan, secondPlan)
			}

			start := make(chan struct{})
			errs := make(chan error, 2)
			var wg sync.WaitGroup
			for _, instanceID := range []string{"concurrent-a", "concurrent-b"} {
				instanceID := instanceID
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					errs <- activate(instanceID)
				}()
			}
			close(start)
			wg.Wait()
			close(errs)
			for err := range errs {
				if err != nil {
					var native *pq.Error
					if errors.As(err, &native) {
						t.Logf("concurrent native flow activation: code=%s detail=%s where=%s", native.Code, native.Detail, native.Where)
					}
					t.Fatalf("concurrent flow activation: %v", err)
				}
			}
			finalPlan := currentAgentFixtureSourceSet(t, ctx, selected)
			if finalPlan.Revision != firstPlan.Revision || len(finalPlan.Agents) != 0 {
				t.Fatalf("concurrent flow activation changed static source set: first=%#v final=%#v", firstPlan, finalPlan)
			}

			provider, ok := lifecycle.(interface {
				ProcessExecutionBinding() (runtimemanager.ProcessExecutionBinding, error)
			})
			if !ok {
				t.Fatal("agent fixture lifecycle does not expose process execution binding")
			}
			binding, err := provider.ProcessExecutionBinding()
			if err != nil {
				t.Fatalf("read retained fixture grant: %v", err)
			}
			if strings.TrimSpace(binding.GenerationGrantID) == "" {
				t.Fatal("retained fixture grant ID is empty")
			}
			agents, err := selected.LoadAgents(ctx)
			if err != nil {
				t.Fatalf("load activated agents: %v", err)
			}
			for _, path := range []string{
				"review/sequential-a", "review/sequential-b", "review/concurrent-a", "review/concurrent-b",
			} {
				assertExactAgentFixtureFlowAuthority(t, ctx, selected, agents, runID, path, binding)
			}
		})
	}
}

func configureAgentFixtureFlowLifecycle(
	t *testing.T,
	selected agentFixtureFlowStore,
	bus *sqliteFlowActivationBus,
	bundle *runtimecontracts.WorkflowContractBundle,
	configure ...func(*runtimepipeline.PipelineCoordinatorOptions),
) *runtimepipeline.PipelineCoordinator {
	t.Helper()
	switch store := selected.(type) {
	case *SQLiteRuntimeStore:
		return configureSQLiteFlowActivationLifecycle(t, store, bus, bundle, configure...)
	case *PostgresStore:
		source := semanticview.Wrap(bundle)
		module := sqliteFlowActivationWorkflowModule{
			source: source,
			guards: runtimepipeline.NewContractGuardRegistry(source),
		}
		options := runtimepipeline.PipelineCoordinatorOptions{
			ExecutionPosture:        executionposture.Live,
			Module:                  module,
			Persistence:             runtimepipeline.NewWorkflowPersistence(store),
			RunLifecycle:            store,
			PipelineObligations:     store.PipelineObligations(),
			DeliveryStore:           store,
			DeadLetters:             store,
			DecisionCards:           store,
			ProposedEffects:         store,
			HumanTasks:              store,
			DecisionCardDraftExpiry: store,
			HumanTaskExpiry:         store,
			DeliveryRuntime:         workflowTestBus{},
			WorkOwner:               storeTestWorkOwner(t),
			ReceiverExecution:       eventreceiver.NormalExecution(),
		}
		for _, apply := range configure {
			if apply != nil {
				apply(&options)
			}
		}
		return runtimepipeline.NewPipelineCoordinatorWithOptions(bus, options)
	default:
		t.Fatalf("unsupported agent fixture flow store %T", selected)
		return nil
	}
}

func currentAgentFixtureSourceSet(t *testing.T, ctx context.Context, selected agentfixture.Store) runtimeagenttopology.SourceSetPlan {
	t.Helper()
	capability, err := agentfixture.ProcessCapability(t, ctx, selected)
	if err != nil {
		t.Fatalf("load fixture process capability: %v", err)
	}
	plan, exists, err := capability.CurrentSourceSet(ctx)
	if err != nil || !exists {
		t.Fatalf("load fixture source set: exists=%v err=%v", exists, err)
	}
	return plan
}

func admitAgentFixtureFlowGrant(t *testing.T, ctx context.Context, grant runtimestartupownership.GenerationGrant) {
	t.Helper()
	if _, err := grant.MarkProbesSettled(ctx, nil); err != nil {
		t.Fatalf("settle flow fixture probes: %v", err)
	}
	if _, err := grant.AdmitExecution(ctx); err != nil {
		t.Fatalf("admit flow fixture execution: %v", err)
	}
}

func assertExactAgentFixtureFlowAuthority(
	t *testing.T,
	ctx context.Context,
	selected agentFixtureFlowStore,
	agents []runtimemanager.PersistedAgent,
	runID string,
	path string,
	binding runtimemanager.ProcessExecutionBinding,
) {
	t.Helper()
	readiness, found, err := selected.LoadDynamicFlowRuntimeReadiness(ctx, runID, runtimeflowidentity.RouteForInstancePath(path))
	if err != nil || !found {
		t.Fatalf("load readiness owner for %s: found=%v err=%v", path, found, err)
	}
	fingerprint, err := readiness.Plan.Hash()
	if err != nil {
		t.Fatalf("fingerprint readiness owner for %s: %v", path, err)
	}
	want, err := runtimeagenttopology.FlowReadinessAdmission(runID, path, fingerprint)
	if err != nil {
		t.Fatalf("build expected readiness authority for %s: %v", path, err)
	}
	var db *sql.DB
	query := `SELECT activation_attempt_id::text, activation_attempt_grant_id::text FROM flow_instance_runtime_readiness WHERE run_id=$1::uuid AND instance_path=$2`
	switch backend := selected.(type) {
	case *SQLiteRuntimeStore:
		db = backend.backend.ConstructionHandle()
		query = `SELECT activation_attempt_id, activation_attempt_grant_id FROM flow_instance_runtime_readiness WHERE run_id=? AND instance_path=?`
	case *PostgresStore:
		db = backend.backend.ConstructionHandle()
	default:
		t.Fatalf("unsupported flow fixture store %T", selected)
	}
	var attemptID, grantID string
	if err := db.QueryRowContext(ctx, query, runID, path).Scan(&attemptID, &grantID); err != nil {
		t.Fatalf("load exact activation attempt for %s: %v", path, err)
	}
	ordinal, err := runtimeflowidentity.ParseActivationAttemptID(attemptID)
	if err != nil || ordinal != readiness.AttemptOrdinal || grantID != binding.GenerationGrantID {
		t.Fatalf("activation attempt for %s = %s/%s, want ordinal %d and grant %s: %v", path, attemptID, grantID, readiness.AttemptOrdinal, binding.GenerationGrantID, err)
	}
	want, err = want.WithFlowActivationAttempt(attemptID)
	if err != nil {
		t.Fatalf("bind expected readiness authority for %s: %v", path, err)
	}
	for _, rec := range agents {
		if rec.Config.Identity.FlowInstance() != path {
			continue
		}
		if !rec.Topology.Equal(want) || !rec.ProcessBinding.Equal(binding) {
			t.Fatalf("activated agent %s topology/binding = %#v / %#v, want %#v / %#v", path, rec.Topology, rec.ProcessBinding, want, binding)
		}
		return
	}
	t.Fatalf("activated agent for flow path %s is missing", path)
}

func newAgentFixtureAuthorityStore(t *testing.T, backend string) (context.Context, agentFixtureFlowStore) {
	t.Helper()
	ctx := testAuthorActivityContext()
	switch backend {
	case "sqlite":
		return ctx, newBootstrappedSQLiteRuntimeStoreForTest(t)
	case "postgres":
		_, db, _ := testutil.StartPostgres(t)
		return ctx, admitTestPostgresStore(t, db)
	default:
		t.Fatalf("unsupported backend %q", backend)
		return nil, nil
	}
}

func agentFixtureStaticRecord(t *testing.T, identity runtimeagentidentity.Identity) runtimemanager.PersistedAgent {
	t.Helper()
	return runtimemanager.PersistedAgent{
		Config: withRuntimePersistenceTestIntent(t, runtimeactors.AgentConfig{
			ID: identity.AgentID(), Identity: identity, Role: "worker", Type: "sonnet", Model: "regular",
			FlowID: "global", FlowPath: identity.FlowInstance(), ExecutionMode: runtimeeffects.ExecutionModeLive,
			Memory: agentmemory.Plan{Enabled: true}, Config: []byte(`{}`),
		}),
		Status: "active", HiredBy: "fixture-authority-proof", StartedAt: time.Now().UTC(),
	}
}
