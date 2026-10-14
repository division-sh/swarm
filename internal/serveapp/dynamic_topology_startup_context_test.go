package serveapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func TestDynamicTopologyStartupPreflightPostgresScopesTwoContextsAndRefusesAtomically(t *testing.T) {
	testDynamicTopologyStartupPreflightTwoContexts(t, "postgres")
}

func TestDynamicTopologyStartupPreflightSQLiteScopesTwoContextsAndRefusesAtomically(t *testing.T) {
	testDynamicTopologyStartupPreflightTwoContexts(t, "sqlite")
}

func testDynamicTopologyStartupPreflightTwoContexts(t *testing.T, backend string) {
	for _, foreign := range []struct {
		name             string
		malformed        bool
		sourceTransition bool
	}{
		{name: "pending"},
		{name: "malformed", malformed: true},
		{name: "source_transition", sourceTransition: true},
	} {
		t.Run(foreign.name, func(t *testing.T) {
			cfg := &config.Config{}
			var db *sql.DB
			var selected *selectedStoreOwner
			if backend == "sqlite" {
				selected = openSelectedSQLiteOwner(t, filepath.Join(t.TempDir(), "dynamic-topology.db"), cfg)
				db = selectedStoreDatabaseForTest(t, selected)
				if _, err := initializeServePlatformStateStores(context.Background(), selected.Schema(), filepath.Join(repoRootForTest(), defaultPlatformSpecPath)); err != nil {
					t.Fatalf("admit SQLite selected store: %v", err)
				}
			} else {
				dsn, postgresDB, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				db = postgresDB
				selected = openSelectedPostgresOwner(t, dsn, db, cfg)
			}
			t.Cleanup(func() { closeUnactivatedSelectedStore(t, selected) })

			loaded := []serveRuntimeBundle{
				loadServeDynamicTopologySource(t, selected, "context-a"),
				loadServeDynamicTopologySource(t, selected, "context-b"),
			}
			facts := make([]runtimecorrelation.SourceArtifactFact, len(loaded))
			for index, source := range loaded {
				facts[index] = sourceartifactfixture.RequireArtifact(t, context.Background(),
					selected.RuntimeDeps().EventStore.(sourceartifactfixture.Writer), source.bundle.SourceArtifact)
			}
			paths := []string{"worker/context-a", "worker/context-b"}
			runIDs := []string{uuid.NewString(), uuid.NewString()}

			process := worklifetime.NewProcess()
			runtimeInstanceID := uuid.NewString()
			providerRegistry := testProviderTriggerCatalog(t)
			runtimes := make([]*runtimepkg.Runtime, 0, len(facts))
			t.Cleanup(func() {
				for _, rt := range runtimes {
					if err := rt.Shutdown(); err != nil {
						t.Errorf("shutdown preflight runtime: %v", err)
					}
				}
				process.Retire()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if _, err := process.Join(ctx); err != nil {
					t.Errorf("join preflight process owner: %v", err)
				}
			})
			for index, fact := range facts {
				rt, err := runtimepkg.NewRuntime(context.Background(), runtimeDepsForServeTest(t, selected, cfg, runtimepkg.RuntimeOptions{
					SelfCheck:                        false,
					WorkflowModule:                   loaded[index].module,
					LLMRuntime:                       servedNoopLLMRuntime{},
					DisablePersistentStartupRecovery: true,
					ProviderTriggerCatalog:           providerRegistry,
					ProcessWorkOwner:                 process,
					SourceArtifactFact:               fact,
					RuntimeInstanceID:                runtimeInstanceID,
				}))
				if err != nil {
					t.Fatalf("construct runtime context %s: %v", fact.BundleHash(), err)
				}
				runtimes = append(runtimes, rt)
			}
			for index := range facts {
				captured := index
				if foreign.sourceTransition && index == 1 {
					captured = 0
				}
				seedServeDynamicTopologyReadiness(t, selected, runtimes[captured], loaded[captured].source,
					facts[captured], runIDs[index], paths[index])
				if captured != index {
					ctx := serveDynamicTopologySetupContext(facts[index], runIDs[index], runtimeInstanceID)
					disposition, err := storetest.ReviseRunSource(ctx,
						selected.RuntimeDeps().EventBusDurable.RunLifecycle.(storetest.RunFixtureStore),
						runlifecycle.SourceRevisionRequest{RunID: runIDs[index], Source: facts[index]})
					if err != nil || disposition != runlifecycle.MutationApplied {
						t.Fatalf("revise captured source to current context: disposition=%s err=%v", disposition, err)
					}
					// The structural parent has no changed declaration or pending work.
					// Reconcile its desired source without rewriting its original receipt.
					parent, present, err := runtimes[index].Pipeline.LoadDynamicFlowRuntimeReadiness(ctx, runIDs[index],
						runtimeflowidentity.RouteForInstancePath(runIDs[index]))
					if err != nil || !present {
						t.Fatalf("read captured structural parent: present=%v err=%v", present, err)
					}
					expected := parent.Plan
					expected.BundleHash = facts[index].BundleHash()
					expected.WorkflowVersion = loaded[index].source.WorkflowVersion()
					results, err := runtimes[index].Pipeline.ReconcileDynamicFlowRuntimeReadinessPlans(ctx,
						[]runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliation{{Observed: parent, Expected: expected}}, time.Now().UTC())
					if err != nil || len(results) != 1 || !results[0].Changed {
						t.Fatalf("reconcile structural parent to current source: results=%+v err=%v", results, err)
					}
				}
			}
			if foreign.malformed {
				query := `UPDATE flow_instance_runtime_readiness SET plan = '{}'::jsonb WHERE run_id = $1::uuid AND instance_path = $2`
				if backend == "sqlite" {
					query = `UPDATE flow_instance_runtime_readiness SET plan = '{}' WHERE run_id = $1 AND instance_path = $2`
				}
				if _, err := db.Exec(query, runIDs[1], paths[1]); err != nil {
					t.Fatalf("corrupt foreign readiness: %v", err)
				}
			}
			capability, err := selected.StartupOwnership().AcquireProcessCapability(context.Background(), runtimestartupownership.AcquireRequest{
				OwnerID: "dynamic-topology-preflight-test", BootID: uuid.NewString(), RuntimeInstanceID: runtimeInstanceID,
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := capability.Release(context.Background()); err != nil {
					t.Errorf("release preflight process possession: %v", err)
				}
			})
			var coordinates []runtimeagenttopology.SourceCoordinate
			var desired []runtimeagenttopology.DesiredAgent
			for index, rt := range runtimes {
				coordinate := runtimeagenttopology.SourceCoordinate{BundleHash: facts[index].BundleHash()}
				agents, err := rt.Manager.CompileStaticTopologyDesiredAgents(loaded[index].source, coordinate)
				if err != nil {
					t.Fatal(err)
				}
				coordinates = append(coordinates, coordinate)
				desired = append(desired, agents...)
			}
			plan, err := runtimeagenttopology.NewSourceSetPlan(coordinates, desired)
			if err != nil {
				t.Fatal(err)
			}
			if err := installServeSourceSet(context.Background(), capability, plan); err != nil {
				t.Fatal(err)
			}
			for index, rt := range runtimes {
				grant := installSelectedStoreTestGeneration(t, capability, rt, plan, 1)
				completed := []string{runIDs[index]}
				if index == 0 {
					completed = append(completed, paths[index])
				}
				for _, path := range completed {
					evidence, err := grant.Evidence()
					if err != nil {
						t.Fatal(err)
					}
					// Physical completed-row projection, not attachment execution proof.
					// It still binds the real installed generation, never a fake grant.
					query := `UPDATE flow_instance_runtime_readiness SET activation_attempt_grant_id=$1::uuid, activation_attempt_state='accepted', phase='ready' WHERE run_id=$2::uuid AND instance_path=$3`
					if backend == "sqlite" {
						query = `UPDATE flow_instance_runtime_readiness SET activation_attempt_grant_id=$1, activation_attempt_state='accepted', phase='ready' WHERE run_id=$2 AND instance_path=$3`
					}
					if _, err := db.Exec(query, evidence.GrantID, runIDs[index], path); err != nil {
						t.Fatalf("seed exact completed preflight projection: %v", err)
					}
				}
			}
			local, err := runtimes[0].Manager.InspectDynamicFlowRuntimeReadinessForSource(context.Background(), facts[0])
			if err != nil || len(local.CurrentCompleted) != 2 ||
				local.CurrentCompleted[0].InstancePath != runIDs[0] || local.CurrentCompleted[1].InstancePath != paths[0] ||
				len(local.CurrentPending) != 0 || len(local.SourceTransitionRequired) != 0 {
				t.Fatalf("first context projection = %#v err=%v", local, err)
			}
			if foreign.sourceTransition {
				transition, inspectErr := runtimes[1].Manager.InspectDynamicFlowRuntimeReadinessForSource(context.Background(), facts[1])
				if inspectErr != nil || len(transition.SourceTransitionRequired) != 1 || transition.SourceTransitionRequired[0].InstancePath != paths[1] ||
					transition.SourceTransitionRequired[0].Plan.BundleHash != facts[0].BundleHash() ||
					transition.SourceTransitionRequired[0].Plan.WorkflowVersion != loaded[0].source.WorkflowVersion() ||
					!transition.SourceTransitionRequired[0].OwningRunSource.Matches(facts[1]) ||
					len(transition.CurrentCompleted) != 1 || transition.CurrentCompleted[0].InstancePath != runIDs[1] ||
					transition.CurrentCompleted[0].Plan.BundleHash != facts[1].BundleHash() ||
					transition.CurrentCompleted[0].Plan.WorkflowVersion != loaded[1].source.WorkflowVersion() || len(transition.CurrentPending) != 0 {
					t.Fatalf("foreign transition projection = %#v err=%v", transition, inspectErr)
				}
			} else if !foreign.malformed {
				pending, inspectErr := runtimes[1].Manager.InspectDynamicFlowRuntimeReadinessForSource(context.Background(), facts[1])
				if inspectErr != nil || len(pending.CurrentPending) != 1 || pending.CurrentPending[0].InstancePath != paths[1] ||
					len(pending.CurrentCompleted) != 1 || pending.CurrentCompleted[0].InstancePath != runIDs[1] || len(pending.SourceTransitionRequired) != 0 {
					t.Fatalf("foreign pending projection = %#v err=%v", pending, inspectErr)
				}
			}
			before := snapshotServeDynamicTopologyReadiness(t, db)
			contexts := []serveRuntimeBundleContext{
				{runtime: runtimes[0], sourceArtifactFact: facts[0]},
				{runtime: runtimes[1], sourceArtifactFact: facts[1]},
			}
			_, err = prepareServeRuntimeContexts(context.Background(), contexts, nil)
			if err == nil {
				t.Fatal("two-context startup accepted foreign incomplete topology")
			}
			if foreign.malformed {
				if !strings.Contains(err.Error(), "decode") && !strings.Contains(err.Error(), "readiness") {
					t.Fatalf("malformed foreign context error = %v", err)
				}
			} else if foreign.sourceTransition {
				if !strings.Contains(err.Error(), "predecessor_pending_transitions=1") || !strings.Contains(err.Error(), "requires recovery") {
					t.Fatalf("source-transition foreign context error = %v", err)
				}
			} else if !strings.Contains(err.Error(), "requires recovery") {
				t.Fatalf("pending foreign context error = %v", err)
			}
			after := snapshotServeDynamicTopologyReadiness(t, db)
			if !reflect.DeepEqual(after, before) {
				t.Fatalf("all-context preflight mutated selected state:\nbefore=%v\nafter=%v", before, after)
			}
			for index, rt := range runtimes {
				if configs := rt.Manager.ListAgentConfigs(); len(configs) != 0 {
					t.Fatalf("context %d acquired process agents before all-context admission: %#v", index, configs)
				}
			}
		})
	}
}

func loadServeDynamicTopologySource(t *testing.T, selected *selectedStoreOwner, label string) serveRuntimeBundle {
	t.Helper()
	root := t.TempDir()
	for path, contents := range canonicalrouting.DynamicTopologyStartupDocuments(t, label) {
		writeWorkflowValidationFixtureFile(t, filepath.Join(root, path), contents)
	}
	loaded, err := loadServeRuntimeBundle(context.Background(), repoRootForTest(), selected.SourceArtifactStore(),
		cliapp.CLISourcePlatformSpecPaths{SourceRoot: root, PlatformSpecPath: filepath.Join(repoRootForTest(), defaultPlatformSpecPath)},
		cliapp.ServeOptions{}, testPlatformPackBaseGenerations(t))
	if err != nil {
		t.Fatalf("compile exact startup source %s: %v", label, err)
	}
	t.Cleanup(func() {
		if err := loaded.cleanup(); err != nil {
			t.Errorf("release startup source %s: %v", label, err)
		}
	})
	if _, err := initializeStateStores(context.Background(), selected.Schema(), loaded.bundle); err != nil {
		t.Fatalf("initialize exact startup source %s: %v", label, err)
	}
	return loaded
}

func serveDynamicTopologySetupContext(fact runtimecorrelation.SourceArtifactFact, runID, runtimeID string) context.Context {
	ctx := runtimecorrelation.WithSourceArtifactFact(context.Background(), fact)
	ctx = runtimecorrelation.WithRunID(ctx, runID)
	ctx = runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(runtimeID, fact.BundleHash()))
	return effects.WithExecutionMode(ctx, executionmode.Live)
}

func seedServeDynamicTopologyReadiness(t *testing.T, selected *selectedStoreOwner, rt *runtimepkg.Runtime,
	source semanticview.Source, fact runtimecorrelation.SourceArtifactFact, runID, instancePath string,
) {
	t.Helper()
	bundle, found := semanticview.Bundle(source)
	if !found || bundle.SourceArtifact == nil || bundle.SourceArtifact.BundleHash() != fact.BundleHash() {
		t.Fatal("startup construction requires its exact compiled and stored artifact")
	}
	ctx := worklifetime.WithOccurrence(serveDynamicTopologySetupContext(fact, runID, rt.Options.RuntimeInstanceID), rt.WorkOccurrence())
	storetest.RequireRun(t, ctx, selected.RuntimeDeps().EventBusDurable.RunLifecycle.(storetest.RunFixtureStore),
		storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runID, Artifact: bundle.SourceArtifact})
	parent := runtimeflowidentity.Stored(source, semanticview.RootExecutionFlowID(source), runID, runID, runID, "")
	rootPlan, err := rt.Manager.PrepareFlowInstanceActivation(ctx, runtimepipeline.FlowInstanceActivationRequest{
		ContractBundle: source, Instance: parent, OccurredAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("prepare structural parent: %v", err)
	}
	rootCommit, err := rt.Bus.CommitFlowInstanceActivation(ctx, rootPlan)
	if err != nil || !rootCommit.Acknowledged || !rootCommit.Created {
		t.Fatalf("construct structural parent: result=%+v err=%v", rootCommit, err)
	}
	parts := strings.SplitN(instancePath, "/", 2)
	if len(parts) != 2 {
		t.Fatalf("invalid readiness instance path %q", instancePath)
	}
	child, err := runtimeflowidentity.KeyedChild(source, parent, parts[0], parts[1])
	if err != nil || child.InstancePath != instancePath {
		t.Fatalf("derive exact keyed worker: identity=%+v err=%v", child, err)
	}
	trigger := eventtest.ExistingRunRootIngress(uuid.NewString(), "worker.requested", "startup-preflight", "",
		json.RawMessage(fmt.Sprintf(`{"worker_id":%q}`, parts[1])), 0, runID, events.EventEnvelope{}, time.Now().UTC())
	storetest.CommitSemanticEvent(t, ctx, selected.RuntimeDeps().EventStore, trigger)
	plan, err := rt.Manager.PrepareFlowInstanceActivation(ctx, runtimepipeline.FlowInstanceActivationRequest{
		ContractBundle: source, Instance: child, TriggerEvent: trigger, OccurredAt: trigger.CreatedAt(),
		ConstructorInput: "worker.requested", ResolvedKey: parts[1],
	})
	if err != nil {
		t.Fatalf("prepare exact worker construction: %v", err)
	}
	committed, err := rt.Bus.CommitFlowInstanceActivation(ctx, plan)
	if err != nil || !committed.Acknowledged || !committed.Created {
		t.Fatalf("construct exact worker: result=%+v err=%v", committed, err)
	}
	owner := runtimeflowidentity.RunScopedFlowInstance{RunID: runID, Route: child.Route()}
	header, found, err := rt.Pipeline.LoadConstructedFlowInstance(ctx, owner, identity.NormalizeEntityID(child.EntityID))
	if err != nil || !found || header.ParentFlowInstance != runID || header.ParentEntityID != parent.EntityID ||
		header.ParentFlowID != parent.TemplateID || header.InstanceKey != parts[1] || header.WorkflowVersion != source.WorkflowVersion() ||
		header.InstanceID != child.InstanceID || header.StorageRef != child.InstancePath || header.WorkflowName != child.TemplateID ||
		header.InstanceKind != plan.Instance.InstanceKind || !reflect.DeepEqual(header.Fields, plan.Instance.Fields) {
		t.Fatalf("captured worker header: found=%v header=%+v err=%v", found, header, err)
	}
	receipt, err := storetest.ReadReceiverConstructionPublication(ctx, selected.RuntimeDeps().DeliveryStore, owner, child.EntityID)
	if err != nil || receipt.Identity != child || receipt.InstanceKey != parts[1] || receipt.BundleHash != fact.BundleHash() ||
		receipt.WorkflowVersion != source.WorkflowVersion() || receipt.CreatingInput != plan.CreatingInput {
		t.Fatalf("captured worker receipt: evidence=%+v err=%v", receipt, err)
	}
}

func snapshotServeDynamicTopologyReadiness(t *testing.T, db *sql.DB) []string {
	t.Helper()
	rows, err := db.Query(`
		SELECT CAST(readiness.run_id AS TEXT), readiness.instance_path, CAST(readiness.plan AS TEXT),
		       readiness.phase, CAST(readiness.updated_at AS TEXT),
		       run.bundle_hash, run.status
		FROM flow_instance_runtime_readiness AS readiness
		JOIN runs AS run ON run.run_id = readiness.run_id
		ORDER BY readiness.run_id, readiness.instance_path
	`)
	if err != nil {
		t.Fatalf("snapshot readiness: %v", err)
	}
	defer rows.Close()
	var snapshot []string
	for rows.Next() {
		var runID, path, plan, readyAt, updatedAt, bundleHash, runStatus string
		if err := rows.Scan(&runID, &path, &plan, &readyAt, &updatedAt, &bundleHash, &runStatus); err != nil {
			t.Fatalf("scan readiness snapshot: %v", err)
		}
		snapshot = append(snapshot, fmt.Sprintf("%s|%s|%s|%s|%s|%s|%s", runID, path, plan, readyAt, updatedAt, bundleHash, runStatus))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read readiness snapshot: %v", err)
	}
	return snapshot
}
