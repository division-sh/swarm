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

	"github.com/division-sh/swarm/internal/config"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil"
	runlifecyclefixture "github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
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

			bundle := loadWorkflowValidationFixtureBundle(t, "tests/tier11-flow-composition/test-dynamic-flow-instance")
			if _, err := initializeStateStores(context.Background(), selected.Schema(), bundle); err != nil {
				t.Fatalf("initialize workflow state stores: %v", err)
			}
			source := semanticview.Wrap(bundle)
			artifacts := []*sourceartifact.AdmittedSourceArtifact{
				sourceartifactfixture.New("agents.yaml", []byte("agents: {}\n# dynamic-context-a\n")),
				sourceartifactfixture.New("agents.yaml", []byte("agents: {}\n# dynamic-context-b\n")),
			}
			facts := []runtimecorrelation.SourceArtifactFact{
				sourceartifactfixture.FactFor(artifacts[0]),
				sourceartifactfixture.FactFor(artifacts[1]),
			}
			paths := []string{"worker/context-a", "worker/context-b"}
			runIDs := []string{uuid.NewString(), uuid.NewString()}
			for index, fact := range facts {
				planSource := fact
				if foreign.sourceTransition && index == 1 {
					planSource = facts[0]
				}
				seedServeDynamicTopologyReadiness(t, db, backend, fact, planSource, artifacts[index], runIDs[index], paths[index], index == 0)
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

			process := worklifetime.NewProcess()
			runtimeInstanceID := uuid.NewString()
			providerRegistry := testProviderTriggerCatalog(t)
			runtimes := make([]*runtimepkg.Runtime, 0, len(facts))
			for _, fact := range facts {
				rt, err := runtimepkg.NewRuntime(context.Background(), runtimeDepsForServeTest(t, selected, cfg, runtimepkg.RuntimeOptions{
					SelfCheck:                        false,
					WorkflowModule:                   stubWorkflowModule{source: source},
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
				agents, err := rt.Manager.CompileStaticTopologyDesiredAgents(source, coordinate)
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
			for _, rt := range runtimes {
				installSelectedStoreTestGeneration(t, capability, rt, plan, 1)
			}
			t.Cleanup(func() {
				for _, rt := range runtimes {
					_ = rt.Shutdown()
				}
				process.Retire()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if _, err := process.Join(ctx); err != nil {
					t.Errorf("join preflight process owner: %v", err)
				}
			})

			local, err := runtimes[0].Manager.InspectDynamicFlowRuntimeReadinessForSource(context.Background(), facts[0])
			if err != nil || len(local.CurrentCompleted) != 1 || local.CurrentCompleted[0].InstancePath != paths[0] ||
				len(local.CurrentPending) != 0 || len(local.SourceTransitionRequired) != 0 {
				t.Fatalf("first context projection = %#v err=%v", local, err)
			}
			if foreign.sourceTransition {
				transition, inspectErr := runtimes[1].Manager.InspectDynamicFlowRuntimeReadinessForSource(context.Background(), facts[1])
				if inspectErr != nil || len(transition.SourceTransitionRequired) != 1 || transition.SourceTransitionRequired[0].InstancePath != paths[1] ||
					len(transition.CurrentCompleted) != 0 || len(transition.CurrentPending) != 0 {
					t.Fatalf("foreign transition projection = %#v err=%v", transition, inspectErr)
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

func seedServeDynamicTopologyReadiness(
	t *testing.T,
	db *sql.DB,
	backend string,
	source runtimecorrelation.SourceArtifactFact,
	planSource runtimecorrelation.SourceArtifactFact,
	artifact *sourceartifact.AdmittedSourceArtifact,
	runID string,
	instancePath string,
	complete bool,
) {
	t.Helper()
	runtimeInstanceID := "11111111-1111-1111-1111-111111111111"
	ctx := runtimecorrelation.WithSourceArtifactFact(context.Background(), source)
	ctx = runtimecorrelation.WithRunID(ctx, runID)
	ctx = runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(runtimeInstanceID, source.BundleHash()))
	fixture := runlifecyclefixture.Fixture{
		Origin:   runlifecyclefixture.ScenarioSetupOrigin(),
		RunID:    runID,
		Artifact: artifact,
	}
	if backend == "sqlite" {
		runlifecyclefixture.RequireSQLite(t, ctx, db, fixture)
	} else {
		runlifecyclefixture.RequirePostgres(t, ctx, db, fixture)
	}
	parts := strings.SplitN(instancePath, "/", 2)
	if len(parts) != 2 {
		t.Fatalf("invalid readiness instance path %q", instancePath)
	}
	entityID := uuid.NewString()
	plan, err := (runtimepipeline.DynamicFlowRuntimeReadinessPlan{
		Identity: runtimeflowidentity.Instance{
			TemplateID: parts[0], ScopeKey: parts[0], InstanceID: parts[1], InstancePath: instancePath,
			EntityID: entityID, HasStoredPath: true,
		},
		RunID: runID, BundleHash: planSource.BundleHash(),
		WorkflowVersion: "1.0.0", ExecutionMode: executionmode.Live,
	}).Normalized()
	if err != nil {
		t.Fatalf("normalize readiness plan: %v", err)
	}
	raw, err := json.Marshal(plan)
	if err != nil {
		t.Fatalf("marshal readiness plan: %v", err)
	}
	flowInsert := `
		INSERT INTO flow_instances (run_id, instance_path, flow_template, mode, config, status, created_at)
		VALUES ($1::uuid, $2, $3, 'template', '{}'::jsonb, 'active', NOW())
	`
	entityInsert := `
		INSERT INTO entity_state (entity_id, run_id, flow_instance, entity_type, current_state, fields, created_at, updated_at)
		VALUES ($1::uuid, $2::uuid, $3, 'worker', 'idle', '{}'::jsonb, NOW(), NOW())
	`
	readinessInsert := `
		INSERT INTO flow_instance_runtime_readiness (
			run_id, instance_path, plan, topology_ready_at, created_at, updated_at
		) VALUES ($1::uuid, $2, $3::jsonb, $4, NOW(), NOW())
	`
	if backend == "sqlite" {
		flowInsert = `
			INSERT INTO flow_instances (run_id, instance_path, flow_template, mode, config, status, created_at)
			VALUES ($1, $2, $3, 'template', '{}', 'active', CURRENT_TIMESTAMP)
		`
		entityInsert = `
			INSERT INTO entity_state (entity_id, run_id, flow_instance, entity_type, current_state, fields, created_at, updated_at)
			VALUES ($1, $2, $3, 'worker', 'idle', '{}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
		`
		readinessInsert = `
			INSERT INTO flow_instance_runtime_readiness (
				run_id, instance_path, plan, topology_ready_at, created_at, updated_at
			) VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
		`
	}
	if _, err := db.Exec(flowInsert, runID, instancePath, parts[0]); err != nil {
		t.Fatalf("seed flow instance %s: %v", instancePath, err)
	}
	if _, err := db.Exec(entityInsert, entityID, runID, instancePath); err != nil {
		t.Fatalf("seed entity state %s: %v", instancePath, err)
	}
	if _, err := db.Exec(readinessInsert, runID, instancePath, raw, nullableServeReadinessTime(complete)); err != nil {
		t.Fatalf("seed readiness %s: %v", instancePath, err)
	}
}

func nullableServeReadinessTime(complete bool) any {
	if !complete {
		return nil
	}
	return time.Now().UTC()
}

func snapshotServeDynamicTopologyReadiness(t *testing.T, db *sql.DB) []string {
	t.Helper()
	rows, err := db.Query(`
		SELECT CAST(readiness.run_id AS TEXT), readiness.instance_path, CAST(readiness.plan AS TEXT),
		       COALESCE(CAST(readiness.topology_ready_at AS TEXT), ''), CAST(readiness.updated_at AS TEXT),
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
