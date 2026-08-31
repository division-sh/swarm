package runtimepersistence

import (
	"context"
	"database/sql"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimeexecutionmode "github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	agentfixture "github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type agentFixtureFlowStore interface {
	agentfixture.Store
	runtimedelivery.Store
	LoadDynamicFlowRuntimeReadiness(context.Context, string, runtimeflowidentity.Route) (runtimepipeline.DynamicFlowRuntimeReadiness, bool, error)
	CommitFlowInstanceActivation(context.Context, runtimebus.FlowInstanceActivationCommand) (runtimepipeline.CommittedFlowInstanceActivation, error)
}

type agentFixtureFlowActivationCommitter struct {
	store agentFixtureFlowStore
}

func (o agentFixtureFlowActivationCommitter) CommitFlowInstanceActivation(ctx context.Context, plan runtimepipeline.FlowInstanceActivationPlan) (runtimepipeline.CommittedFlowInstanceActivation, error) {
	return o.store.CommitFlowInstanceActivation(ctx, runtimebus.FlowInstanceActivationCommand{
		Plan: plan,
		RouteTopology: []runtimebus.FlowInstanceRouteRecordSet{{
			Identity: plan.Identity.Route(),
		}},
	})
}

func TestAgentFixtureExactFlowAuthorityParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx, selected := newAgentFixtureAuthorityStore(t, backend)
			runID := uuid.NewString()
			ctx = runtimecorrelation.WithRunID(storeTestWorkContext(t, ctx), runID)
			ctx = runtimeeffects.WithExecutionMode(ctx, runtimeeffects.ExecutionModeLive)
			requireRunFixtureForTest(t, ctx, selected, semanticRunFixture{
				Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID,
			})
			bus := &sqliteFlowActivationBus{}
			bundle := sqliteFlowActivationBundle(t)
			workflowStore := configureAgentFixtureFlowLifecycle(t, selected, bus, bundle)
			lifecycle := agentfixture.Lifecycle(t, selected)
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
					AgentRoutes:    bus,
					FlowActivation: agentFixtureFlowActivationCommitter{store: selected},
					RouteInstaller: bus,
					RouteVerifier:  bus,
					RouteRestorer:  bus,
					RouteRetirer:   bus,
				},
				ReceiverExecution: eventreceiver.NormalExecution(),
			}, selected))

			activate := func(instanceID string) error {
				return manager.ActivateFlowInstance(ctx, sqliteFlowActivationRequest(
					bundle, "review", instanceID, "parent-ent", "review/"+instanceID,
				))
			}
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
) *runtimepipeline.PipelineCoordinator {
	t.Helper()
	switch store := selected.(type) {
	case *SQLiteRuntimeStore:
		return configureSQLiteFlowActivationLifecycle(t, store, bus, bundle)
	case *PostgresStore:
		source := semanticview.Wrap(bundle)
		module := sqliteFlowActivationWorkflowModule{
			source:  source,
			guards:  runtimepipeline.NewContractGuardRegistry(source),
			actions: runtimepipeline.NewContractActionRegistry(source),
		}
		return runtimepipeline.NewPipelineCoordinatorWithOptions(bus, runtimepipeline.PipelineCoordinatorOptions{
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
		})
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
	fingerprint, err := canonicaljson.Hash(readiness.Plan)
	if err != nil {
		t.Fatalf("fingerprint readiness owner for %s: %v", path, err)
	}
	want, err := runtimeagenttopology.FlowReadinessAdmission(runID, path, fingerprint)
	if err != nil {
		t.Fatalf("build expected readiness authority for %s: %v", path, err)
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

type agentFixtureAcquireProbe struct {
	agentfixture.Store
	mu           sync.Mutex
	acquireCalls int
}

func (s *agentFixtureAcquireProbe) AcquireProcessCapability(ctx context.Context, req runtimestartupownership.AcquireRequest) (runtimestartupownership.ProcessCapability, error) {
	s.mu.Lock()
	s.acquireCalls++
	s.mu.Unlock()
	return s.Store.AcquireProcessCapability(ctx, req)
}

func (s *agentFixtureAcquireProbe) calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.acquireCalls
}

type agentFixtureAuthoritySnapshot struct {
	Authority runtimestartupownership.Authority
	Plan      runtimeagenttopology.SourceSetPlan
	Binding   runtimemanager.ProcessExecutionBinding
	States    []runtimemanager.AgentLifecycleState
}

type agentFixtureAuthorityRows struct {
	AuthorityFacts      int
	SourceSetHeads      int
	SourceSetOperations int
	GenerationGrants    int
	Agents              int
	LifecycleOperations int
}

func TestAgentFixtureSealedAuthorityRejectionParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx, selected := newAgentFixtureAuthorityStore(t, backend)
			beforeRows := captureAgentFixtureAuthorityRows(t, ctx, selected)
			if beforeRows != (agentFixtureAuthorityRows{}) {
				t.Fatalf("fresh fixture authority store is not empty: %#v", beforeRows)
			}
			identity := testAgentIdentity(t, "invalid-authority", "global")
			validFlow, err := runtimeagenttopology.FlowReadinessAdmission(uuid.NewString(), "review/invalid", "plan-v1")
			if err != nil {
				t.Fatal(err)
			}
			invalid := rejectedAgentFixtureAdmissions(t)
			probe := &agentFixtureAcquireProbe{Store: selected}
			for index, topology := range invalid {
				if _, err := agentfixture.CommitExact(t, ctx, probe, runtimemanager.AgentLifecycleTransition{
					Identity: identity, AgentID: identity.AgentID(), Topology: topology,
				}); err == nil {
					t.Fatalf("invalid exact topology %d was accepted", index)
				}
			}
			if _, err := agentfixture.CommitStatic(t, ctx, probe, runtimemanager.AgentLifecycleTransition{
				Identity: identity, AgentID: identity.AgentID(), Topology: validFlow,
			}); err == nil {
				t.Fatal("synthetic static commit accepted caller topology")
			}
			invalidRecord := agentFixtureStaticRecord(t, identity)
			invalidRecord.Topology = invalid[len(invalid)-1]
			if err := agentfixture.UpsertStatic(t, ctx, probe, invalidRecord); err == nil {
				t.Fatal("synthetic static upsert accepted caller topology")
			}
			if got := probe.calls(); got != 0 {
				t.Fatalf("invalid fixture authority acquired process capability %d times", got)
			}
			states, err := selected.ListDurableAgentLifecycleStates(ctx)
			if err != nil || len(states) != 0 {
				t.Fatalf("invalid fixture authority created lifecycle state: states=%#v err=%v", states, err)
			}
			afterRows := captureAgentFixtureAuthorityRows(t, ctx, selected)
			if afterRows != beforeRows {
				t.Fatalf("invalid fixture authority mutated selected-store rows: before=%#v after=%#v", beforeRows, afterRows)
			}

			proveAgentFixtureMixedAuthorityRejection(t, ctx, selected)
		})
	}
}

func captureAgentFixtureAuthorityRows(t *testing.T, ctx context.Context, selected agentFixtureFlowStore) agentFixtureAuthorityRows {
	t.Helper()
	var db *sql.DB
	switch store := selected.(type) {
	case *SQLiteRuntimeStore:
		db = store.backend.ConstructionHandle()
	case *PostgresStore:
		db = store.backend.ConstructionHandle()
	default:
		t.Fatalf("unsupported agent fixture authority store %T", selected)
	}
	counts := agentFixtureAuthorityRows{}
	for _, query := range []struct {
		table string
		out   *int
	}{
		{table: "runtime_startup_authority_facts", out: &counts.AuthorityFacts},
		{table: "agent_topology_source_set_head", out: &counts.SourceSetHeads},
		{table: "agent_topology_source_set_operations", out: &counts.SourceSetOperations},
		{table: "runtime_generation_grants", out: &counts.GenerationGrants},
		{table: "agents", out: &counts.Agents},
		{table: "agent_lifecycle_operations", out: &counts.LifecycleOperations},
	} {
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+query.table).Scan(query.out); err != nil {
			t.Fatalf("count %s: %v", query.table, err)
		}
	}
	return counts
}

const agentFixtureTestBundleHash = "bundle-v2:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"

func rejectedAgentFixtureAdmissions(t *testing.T) []runtimeagenttopology.Admission {
	t.Helper()
	validEphemeral, err := runtimeagenttopology.NewEphemeralAdmission(uuid.NewString(), "runtime_shard")
	if err != nil {
		t.Fatal(err)
	}
	return []runtimeagenttopology.Admission{
		{},
		{Lifetime: runtimeagenttopology.LifetimeDurableManaged, Authority: runtimeagenttopology.Authority{
			Kind:   runtimeagenttopology.AuthorityKind("future_authority"),
			Static: &runtimeagenttopology.StaticDeclarationPlan{SourceSetRevision: "revision", BundleHash: agentFixtureTestBundleHash},
		}},
		{Lifetime: runtimeagenttopology.LifetimeDurableManaged, Authority: runtimeagenttopology.Authority{
			Kind: runtimeagenttopology.AuthorityStaticDeclarationPlan, Static: &runtimeagenttopology.StaticDeclarationPlan{},
		}},
		{Lifetime: runtimeagenttopology.LifetimeDurableManaged, Authority: runtimeagenttopology.Authority{
			Kind: runtimeagenttopology.AuthorityFlowReadinessPlan, Readiness: &runtimeagenttopology.FlowReadinessPlan{RunID: uuid.NewString(), InstancePath: "review/invalid"},
		}},
		{Lifetime: runtimeagenttopology.LifetimeEphemeral, Authority: runtimeagenttopology.Authority{
			Kind: runtimeagenttopology.AuthorityEphemeralExecution, Ephemeral: &runtimeagenttopology.EphemeralExecution{ExecutionID: uuid.NewString()},
		}},
		validEphemeral,
	}
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
			Memory: agentmemory.Authored(true), Config: []byte(`{}`),
		}),
		Status: "active", HiredBy: "fixture-authority-proof", StartedAt: time.Now().UTC(),
	}
}

func proveAgentFixtureMixedAuthorityRejection(t *testing.T, ctx context.Context, selected agentFixtureFlowStore) {
	t.Helper()
	staticIdentity := testAgentIdentity(t, "static-survivor", "global")
	staticRecord := agentFixtureStaticRecord(t, staticIdentity)
	if err := agentfixture.UpsertStatic(t, ctx, selected, staticRecord); err != nil {
		t.Fatalf("seed static fixture survivor: %v", err)
	}
	flowIdentity := testAgentIdentity(t, "flow-survivor", "review/fixture")
	flowState := seedExactAgentFixtureFlowState(t, ctx, selected, flowIdentity)
	before := captureAgentFixtureAuthority(t, ctx, selected)
	probe := &agentFixtureAcquireProbe{Store: selected}
	for index, topology := range rejectedAgentFixtureAdmissions(t) {
		if _, err := agentfixture.CommitExact(t, ctx, probe, runtimemanager.AgentLifecycleTransition{
			Identity: flowIdentity, AgentID: flowIdentity.AgentID(), Topology: topology,
		}); err == nil {
			t.Fatalf("invalid exact topology %d was accepted against live fixture authority", index)
		}
		after := captureAgentFixtureAuthority(t, ctx, selected)
		if !reflect.DeepEqual(after, before) {
			t.Fatalf("invalid exact topology %d mutated fixture authority:\nbefore=%#v\nafter=%#v", index, before, after)
		}
	}

	requireRejectedUnchanged := func(label string, mutate func() error) {
		t.Helper()
		if err := mutate(); err == nil || !strings.Contains(err.Error(), "retains flow_readiness_plan topology") {
			t.Fatalf("%s error = %v, want live flow-authority rejection", label, err)
		}
		after := captureAgentFixtureAuthority(t, ctx, selected)
		if !reflect.DeepEqual(after, before) {
			t.Fatalf("%s mutated fixture authority:\nbefore=%#v\nafter=%#v", label, before, after)
		}
	}
	requireRejectedUnchanged("static upsert", func() error {
		return agentfixture.UpsertStatic(t, ctx, probe, staticRecord)
	})
	newStaticIdentity := testAgentIdentity(t, "new-static", "global")
	requireRejectedUnchanged("direct static commit", func() error {
		_, err := agentfixture.CommitStatic(t, ctx, probe, runtimemanager.AgentLifecycleTransition{
			OperationID: uuid.NewString(), OperationKind: "spawn", RequestHash: "new-static",
			Identity: newStaticIdentity, AgentID: newStaticIdentity.AgentID(), Trigger: "fixture-proof",
			TargetEpoch: 1, TargetGeneration: 1, TargetPhase: runtimemanager.AgentLifecycleRegistered,
			ConfigRevision: "new-static-revision", RunMode: runtimemanager.AgentRunModeStopped,
			Agent: func() *runtimemanager.PersistedAgent {
				rec := agentFixtureStaticRecord(t, newStaticIdentity)
				return &rec
			}(),
			Now: time.Now().UTC(),
		})
		return err
	})
	requireRejectedUnchanged("target identity pressure", func() error {
		_, err := agentfixture.CommitStatic(t, ctx, probe, runtimemanager.AgentLifecycleTransition{
			OperationID: uuid.NewString(), OperationKind: "reconfigure", RequestHash: "flow-to-static",
			Identity: flowIdentity, AgentID: flowIdentity.AgentID(), Trigger: "fixture-proof",
			ExpectedEpoch: flowState.RuntimeEpoch, ExpectedGeneration: flowState.Generation, ExpectedPhase: flowState.Phase,
			TargetEpoch: flowState.RuntimeEpoch, TargetGeneration: flowState.Generation + 1, TargetPhase: flowState.Phase,
			ConfigRevision: flowState.ConfigRevision, RunMode: flowState.RunMode, Now: time.Now().UTC(),
		})
		return err
	})
	if got := probe.calls(); got != 0 {
		t.Fatalf("mixed-authority rejection acquired a replacement process capability %d times", got)
	}

	terminateLifecycleReadinessOwnerForTest(t, ctx, selected, flowIdentity.FlowInstance())
	if _, err := agentfixture.CommitExact(t, ctx, selected, runtimemanager.AgentLifecycleTransition{
		OperationID: uuid.NewString(), OperationKind: "teardown", RequestHash: "terminate-flow-survivor",
		Identity: flowIdentity, AgentID: flowIdentity.AgentID(), Trigger: "fixture-proof",
		ExpectedEpoch: flowState.RuntimeEpoch, ExpectedGeneration: flowState.Generation, ExpectedPhase: flowState.Phase,
		TargetEpoch: flowState.RuntimeEpoch, TargetGeneration: flowState.Generation + 1,
		TargetPhase: runtimemanager.AgentLifecycleTerminated, ConfigRevision: flowState.ConfigRevision,
		RunMode: runtimemanager.AgentRunModeStopped, Topology: flowState.Topology, Now: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("terminate flow fixture survivor: %v", err)
	}
	postTerminalIdentity := testAgentIdentity(t, "post-terminal-static", "global")
	if err := agentfixture.UpsertStatic(t, ctx, selected, agentFixtureStaticRecord(t, postTerminalIdentity)); err != nil {
		t.Fatalf("terminated flow cell blocked static fixture mutation: %v", err)
	}
	terminated, found, err := selected.LoadAgentLifecycleState(ctx, flowIdentity)
	if err != nil || !found || terminated.Phase != runtimemanager.AgentLifecycleTerminated || !terminated.Topology.Equal(flowState.Topology) {
		t.Fatalf("terminated flow fixture changed during static mutation: state=%#v found=%v err=%v", terminated, found, err)
	}
}

func seedExactAgentFixtureFlowState(
	t *testing.T,
	ctx context.Context,
	selected agentFixtureFlowStore,
	identity runtimeagentidentity.Identity,
) runtimemanager.AgentLifecycleState {
	t.Helper()
	plan := currentAgentFixtureSourceSet(t, ctx, selected)
	if len(plan.Sources) != 1 {
		t.Fatalf("fixture source set sources = %#v, want one", plan.Sources)
	}
	record := agentFixtureStaticRecord(t, identity)
	revision, err := canonicaljson.Hash(record.Config)
	if err != nil {
		t.Fatal(err)
	}
	revision = strings.TrimPrefix(revision, "sha256:")
	runID := uuid.NewString()
	requireRunningRunForTest(t, ctx, selected, runID, time.Now().UTC())
	readinessPlan, err := (runtimepipeline.DynamicFlowRuntimeReadinessPlan{
		Identity: runtimeflowidentity.Instance{
			TemplateID: "review", ScopeKey: "review", InstanceID: "fixture", InstancePath: identity.FlowInstance(),
			EntityID: uuid.NewString(), HasStoredPath: true,
		},
		RunID: runID, BundleHash: plan.Sources[0].BundleHash,
		WorkflowVersion: "1.0.0", ExecutionMode: runtimeexecutionmode.Live,
		Agents: []runtimepipeline.DynamicFlowRuntimeAgentExpectation{{Identity: identity, ConfigRevision: revision}},
	}).Normalized()
	if err != nil {
		t.Fatalf("normalize flow readiness owner: %v", err)
	}
	encoded, err := canonicaljson.Bytes(readinessPlan)
	if err != nil {
		t.Fatal(err)
	}
	seedLifecycleReadinessOwner(t, ctx, selected, runID, identity.FlowInstance(), encoded, time.Now().UTC())
	fingerprint, err := canonicaljson.Hash(readinessPlan)
	if err != nil {
		t.Fatal(err)
	}
	topology, err := runtimeagenttopology.FlowReadinessAdmission(runID, identity.FlowInstance(), fingerprint)
	if err != nil {
		t.Fatal(err)
	}
	record.Topology = topology
	if _, err := agentfixture.CommitExact(t, ctx, selected, runtimemanager.AgentLifecycleTransition{
		OperationID: uuid.NewString(), OperationKind: "spawn", RequestHash: "flow-survivor",
		Identity: identity, AgentID: identity.AgentID(), Trigger: "fixture-proof",
		TargetEpoch: 1, TargetGeneration: 1, TargetPhase: runtimemanager.AgentLifecycleRegistered,
		ConfigRevision: revision, RunMode: runtimemanager.AgentRunModeStopped,
		Agent: &record, Topology: topology, Now: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("seed exact flow fixture survivor: %v", err)
	}
	state, found, err := selected.LoadAgentLifecycleState(ctx, identity)
	if err != nil || !found {
		t.Fatalf("load exact flow fixture survivor: found=%v err=%v", found, err)
	}
	return state
}

func captureAgentFixtureAuthority(t *testing.T, ctx context.Context, selected agentFixtureFlowStore) agentFixtureAuthoritySnapshot {
	t.Helper()
	capability, err := agentfixture.ProcessCapability(t, ctx, selected)
	if err != nil {
		t.Fatalf("load fixture capability: %v", err)
	}
	authority, err := capability.Evidence()
	if err != nil {
		t.Fatalf("load fixture capability evidence: %v", err)
	}
	plan, exists, err := capability.CurrentSourceSet(ctx)
	if err != nil || !exists {
		t.Fatalf("load fixture source set: exists=%v err=%v", exists, err)
	}
	states, err := selected.ListDurableAgentLifecycleStates(ctx)
	if err != nil {
		t.Fatalf("load fixture lifecycle census: %v", err)
	}
	sort.Slice(states, func(i, j int) bool {
		left, _ := states[i].Identity.Fingerprint()
		right, _ := states[j].Identity.Fingerprint()
		return left < right
	})
	binding := runtimemanager.ProcessExecutionBinding{}
	for _, state := range states {
		if state.ProcessBinding.BundleHash == agentFixtureTestBundleHash {
			binding = state.ProcessBinding
			break
		}
	}
	return agentFixtureAuthoritySnapshot{Authority: authority, Plan: plan, Binding: binding, States: states}
}
