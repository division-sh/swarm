package runtimepersistence

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/google/uuid"
)

type receiverConfigActivationFixture struct {
	ctx       context.Context
	db        *sql.DB
	store     agentFixtureFlowStore
	manager   *manager.AgentManager
	workflows *pipeline.PipelineCoordinator
	bus       *sqliteFlowActivationBus
	bundle    *contracts.WorkflowContractBundle
}

func newReceiverConfigActivationFixture(t *testing.T, backend string) receiverConfigActivationFixture {
	t.Helper()
	return newReceiverConfigActivationFixtureWithAgents(t, backend, true)
}

func newReceiverConfigActivationFixtureWithAgents(t *testing.T, backend string, withAgents bool) receiverConfigActivationFixture {
	t.Helper()
	return newReceiverConfigActivationFixtureWithOptions(t, backend, withAgents, false)
}

func newReceiverConfigActivationFixtureWithOptions(t *testing.T, backend string, withAgents, autoEmit bool) receiverConfigActivationFixture {
	t.Helper()
	_, selected := newAgentFixtureAuthorityStore(t, backend)
	actors := sqliteFlowActivationBundle(t)
	files := map[string]string{
		"schema.yaml": "name: receiver-config-atomicity\n",
		"review/schema.yaml": `name: review
mode: template
instance: request_id
instance_variables:
  variables:
    label: {type: string}
    enabled: {type: boolean, default: true}
    nested: {type: json}
stages:
  pending: {initial: true}
pins:
  inputs:
    events: [task.started]
`,
		"review/entities.yaml": "review_item:\n  request_id: string\n",
		"review/events.yaml":   "task.started: {}\n",
	}
	if autoEmit {
		files["events.yaml"] = "request.started: {}\n"
		files["review/schema.yaml"] += "auto_emit_on_create: {event: task.started}\n"
		files["review/events.yaml"] = "task.started:\n  request_id: string\n  label: string\n  enabled: boolean\n  nested: json\n"
	}
	bundle := loadLifecyclePersistenceFixtureForTest(t, files)
	if withAgents {
		bundle.FlowTree.ByID["review"].Agents = actors.FlowTree.ByID["review"].Agents
		bundle.FlowTree.ByID["review"].AgentURIs = actors.FlowTree.ByID["review"].AgentURIs
		bundle.URIRegistry = actors.URIRegistry
	}
	fact := mustStoreTestSourceArtifactFact(bundle.SourceArtifact.BundleHash())
	ctx := correlation.WithRunID(storeTestWorkContext(t, testAuthorActivityContextForBundle(fact.BundleHash())), uuid.NewString())
	requireRunFixtureForTest(t, ctx, selected, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: correlation.RunIDFromContext(ctx), Artifact: bundle.SourceArtifact, BundleHash: fact.BundleHash()})
	bus := &sqliteFlowActivationBus{}
	workflows := configureAgentFixtureFlowLifecycle(t, selected, bus, bundle)
	coordinate := agenttopology.SourceCoordinate{BundleHash: fact.BundleHash()}
	sourceSet, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{coordinate}, nil)
	if err != nil {
		t.Fatal(err)
	}
	grant, err := agentfixture.AdmitGeneration(t, ctx, selected, sourceSet, coordinate)
	if err != nil {
		t.Fatal(err)
	}
	am := ownStoreTestAgentManager(t, manager.NewAgentManagerWithOptions(bus, nil, manager.AgentManagerOptions{
		ExecutionPosture: executionposture.Live, BaseContext: ctx, SourceArtifactFact: fact,
		SemanticSource: semanticview.Wrap(bundle), WorkflowInstances: workflows, LLMBackend: "anthropic",
		DeliveryStore: selected, WorkOwner: storeTestWorkOwner(t),
		PersistenceRoles: manager.PersistenceRoles{
			AgentRoutes: bus, FlowActivation: agentFixtureFlowActivationCommitter{store: selected},
			RouteInstaller: bus, RouteVerifier: bus, RouteRestorer: bus, RouteRetirer: bus,
		}, ReceiverExecution: eventreceiver.NormalExecution(),
	}, selected))
	admission, err := agenttopology.StaticAdmission(sourceSet.Revision, fact.BundleHash(), agenttopology.LifetimeDurableManaged)
	if err != nil {
		t.Fatal(err)
	}
	if err := am.InstallStartupTopology(grant, admission, sourceSet); err != nil {
		t.Fatal(err)
	}
	var db *sql.DB
	switch store := selected.(type) {
	case *SQLiteRuntimeStore:
		db = store.backend.ConstructionHandle()
	case *PostgresStore:
		db = store.backend.ConstructionHandle()
		db.SetMaxOpenConns(12)
	}
	return receiverConfigActivationFixture{ctx: ctx, db: db, store: selected, manager: am, workflows: workflows, bus: bus, bundle: bundle}
}

func (f receiverConfigActivationFixture) request(key, instanceID, label string) pipeline.FlowInstanceActivationRequest {
	req := sqliteFlowActivationRequest(f.bundle, "review", instanceID, "", "review/"+instanceID)
	req.Config = map[string]any{"request_id": key, "label": label, "nested": []any{int64(7), float64(7), nil}}
	return req
}

func TestReceiverConfigActivationRaceAndRollbackBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, scenario := range []string{"same_key", "numeric_kind_only", "numeric_kind_no_agents", "different_keys", "rollback_first"} {
			t.Run(backend+"/"+scenario, func(t *testing.T) {
				// Register the SQLite blocking function before opening connections.
				barrier := newForkContentionBarrier(t, backend, scenario == "rollback_first")
				agentCount := 1
				if scenario == "numeric_kind_no_agents" {
					agentCount = 0
				}
				f := newReceiverConfigActivationFixtureWithAgents(t, backend, agentCount != 0)
				ctx, cancel := context.WithTimeout(f.ctx, 25*time.Second)
				defer cancel()
				requests := []pipeline.FlowInstanceActivationRequest{f.request("business-key", "ti-receiver-one", "first"), f.request("business-key", "ti-receiver-one", "second")}
				if strings.HasPrefix(scenario, "numeric_kind_") {
					requests[1].Config["label"] = "first"
					requests[1].Config["nested"].([]any)[1] = int64(7)
				}
				if scenario == "different_keys" {
					requests[1] = f.request("other-business-key", "ti-receiver-two", "second")
				}
				plans := make([]pipeline.FlowInstanceActivationPlan, 2)
				for i, req := range requests {
					var err error
					plans[i], err = f.manager.PrepareFlowInstanceActivation(ctx, req)
					if err != nil {
						t.Fatal(err)
					}
					if plans[i].Instance.Config["enabled"] != true || len(plans[i].Readiness.Agents) != agentCount {
						t.Fatal("typed preparation omitted defaults or agent plan")
					}
				}
				if agentCount != 0 && plans[0].Readiness.Agents[0].ConfigRevision == plans[1].Readiness.Agents[0].ConfigRevision {
					t.Fatal("different business config produced the same agent plan")
				}
				f.requireCounts(t, 0)
				secondStore := f.store
				observer := f.db
				secondCtx := ctx
				if backend == "sqlite" {
					var sequence int
					var name, path string
					if err := f.db.QueryRow(`PRAGMA database_list`).Scan(&sequence, &name, &path); err != nil {
						t.Fatal(err)
					}
					secondStore = barrier.observeSQLiteStore(t, f.store.(*SQLiteRuntimeStore), path)
					var err error
					observer, err = sql.Open("sqlite", path)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = observer.Close() })
					secondCtx = context.WithValue(ctx, forkContentionContextKey{}, barrier)
				}
				barrier.install(t, f.db, correlation.RunIDFromContext(ctx), "", "writer")
				committed := make([]pipeline.CommittedFlowInstanceActivation, 2)
				done := []chan forkContentionResult{make(chan forkContentionResult, 1), make(chan forkContentionResult, 1)}
				finished := []chan struct{}{make(chan struct{}), make(chan struct{})}
				startedSecond := false
				t.Cleanup(func() {
					cancel()
					barrier.release()
					barrier.resume()
					for i, joined := range finished {
						if i == 1 && !startedSecond {
							continue
						}
						select {
						case <-joined:
						case <-time.After(5 * time.Second):
							t.Error("activation contender did not join")
						}
					}
				})
				invoke := func(i int, ctx context.Context, selected agentFixtureFlowStore) {
					defer close(finished[i])
					var err error
					committed[i], err = (agentFixtureFlowActivationCommitter{store: selected}).CommitFlowInstanceActivation(ctx, plans[i])
					done[i] <- forkContentionResult{err: err}
				}
				go invoke(0, ctx, f.store)
				pid := barrier.awaitWinner(t, ctx, observer, done[0])
				startedSecond = true
				go invoke(1, secondCtx, secondStore)
				barrier.awaitContender(t, ctx, observer, pid, &forkContentionGate{}, done[1])
				barrier.release()
				first := awaitForkContentionResult(t, ctx, done[0])
				if scenario == "rollback_first" {
					if first.err == nil || !strings.Contains(first.err.Error(), "h18_requested_rollback") {
						t.Fatalf("injected rollback: %v", first.err)
					}
					if backend == "sqlite" {
						f.requireCounts(t, 0)
					}
				} else if first.err != nil || !committed[0].Created {
					t.Fatalf("first activation failed: %v", first.err)
				}
				barrier.resume()
				second := awaitForkContentionResult(t, ctx, done[1])
				winner := 0
				if scenario == "same_key" || strings.HasPrefix(scenario, "numeric_kind_") {
					failure, ok := failures.As(second.err)
					if !ok || failure.Failure.Class != failures.ClassConflictingDuplicate || committed[1].Created {
						t.Fatalf("losing preparation accepted: created=%v config=%#v err=%v", committed[1].Created, committed[1].Plan.Instance.Config, second.err)
					}
				} else if second.err != nil || !committed[1].Created {
					t.Fatalf("second activation: %v", second.err)
				}
				if scenario == "rollback_first" {
					winner = 1
				}
				count := 1
				if scenario == "different_keys" {
					count = 2
				}
				f.requireCounts(t, count)
				if agents, err := f.store.LoadAgents(ctx); err != nil || len(agents) != 0 || len(f.bus.routePaths()) != 0 {
					t.Fatalf("pre-commit consumer leaked agent/topology: %#v %v", agents, err)
				}
				f.requireConfig(t, plans[winner])
				// A retry goes through Ensure, not a second creation command: even
				// the losing request must consume the committed config and plan.
				if created, err := f.manager.EnsureFlowInstance(ctx, requests[1-winner]); err != nil || created {
					t.Fatalf("retry did not reuse winner: created=%v err=%v", created, err)
				}
				f.requireConfig(t, plans[winner])
				if scenario == "different_keys" {
					if created, err := f.manager.EnsureFlowInstance(ctx, requests[0]); err != nil || created {
						t.Fatalf("first key reuse: %v %v", created, err)
					}
					f.requireConfig(t, plans[1])
				}
				agents, err := f.store.LoadAgents(ctx)
				if err != nil || len(agents) != count*agentCount {
					t.Fatalf("winner agent count = %d: %v", len(agents), err)
				}
				for _, agent := range agents {
					want := plans[winner]
					if scenario == "different_keys" && agent.Config.Identity.FlowInstance() == plans[1].Identity.InstancePath {
						want = plans[1]
					}
					wire, err := canonicaljson.MarshalPreservingNumberKinds(want.Instance.Config)
					if err != nil {
						t.Fatal(err)
					}
					var loaded any
					if err := canonicaljson.DecodePreservingNumberLexemes(agent.Config.Config, &loaded); err != nil {
						t.Fatal(err)
					}
					actual, err := canonicaljson.MarshalPreservingNumberKinds(loaded)
					if err != nil || string(actual) != string(wire) {
						t.Fatalf("agent consumed losing config: %s want %s: %v", agent.Config.Config, wire, err)
					}
				}
			})
		}
	}
}

func (f receiverConfigActivationFixture) requireCounts(t *testing.T, want int) {
	t.Helper()
	for _, table := range []string{"flow_instances", "entity_state", "workflow_instance_initial_materializations", "flow_instance_runtime_readiness"} {
		var got int
		if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM `+table+` WHERE run_id=$1`, correlation.RunIDFromContext(f.ctx)).Scan(&got); err != nil || got != want {
			t.Fatalf("%s rows=%d want=%d: %v", table, got, want, err)
		}
	}
}

func (f receiverConfigActivationFixture) requireConfig(t *testing.T, want pipeline.FlowInstanceActivationPlan) {
	t.Helper()
	identity := flowidentity.RunScopedFlowInstance{RunID: want.Readiness.RunID, Route: want.Identity.Route()}
	instance, found, err := f.workflows.Load(f.ctx, identity)
	if err != nil || !found {
		t.Fatalf("load committed instance: %v %v", found, err)
	}
	a, err := canonicaljson.MarshalPreservingNumberKinds(instance.Config)
	if err != nil {
		t.Fatal(err)
	}
	b, err := canonicaljson.MarshalPreservingNumberKinds(want.Instance.Config)
	if err != nil || string(a) != string(b) {
		t.Fatalf("committed config=%s want=%s: %v", a, b, err)
	}
	readiness, found, err := f.store.LoadDynamicFlowRuntimeReadiness(f.ctx, identity.RunID, identity.Route)
	if err != nil || !found {
		t.Fatalf("load committed readiness: %v %v", found, err)
	}
	a, err = canonicaljson.MarshalPreservingNumberKinds(readiness.Plan)
	if err != nil {
		t.Fatal(err)
	}
	b, err = canonicaljson.MarshalPreservingNumberKinds(want.Readiness)
	if err != nil || string(a) != string(b) {
		t.Fatalf("committed readiness=%s want=%s: %v", a, b, err)
	}
}
