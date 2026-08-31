package serveapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/testutil"
	runlifecyclefixture "github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/google/uuid"
)

func TestDynamicTopologyStartupPreflightPostgresScopesTwoContextsAndRefusesAtomically(t *testing.T) {
	for _, foreign := range []struct {
		name      string
		malformed bool
	}{
		{name: "pending"},
		{name: "malformed", malformed: true},
	} {
		t.Run(foreign.name, func(t *testing.T) {
			dsn, db, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)
			cfg := &config.Config{}
			selected := openSelectedPostgresOwner(t, dsn, db, cfg)
			t.Cleanup(func() { closeUnactivatedSelectedStore(t, selected) })

			bundle := loadWorkflowValidationFixtureBundle(t, "tests/tier11-flow-composition/test-dynamic-flow-instance")
			if _, err := initializeStateStores(context.Background(), selected.Schema(), bundle); err != nil {
				t.Fatalf("initialize workflow state stores: %v", err)
			}
			source := semanticview.Wrap(bundle)
			facts := []runtimecorrelation.BundleSourceFact{
				mustServeTestEphemeralBundleSourceFact(runtimeContextTestHash("a")),
				mustServeTestEphemeralBundleSourceFact(runtimeContextTestHash("b")),
			}
			paths := []string{"worker/context-a", "worker/context-b"}
			runIDs := []string{uuid.NewString(), uuid.NewString()}
			for index, fact := range facts {
				seedServeDynamicTopologyReadiness(t, db, fact, runIDs[index], paths[index], index == 0)
			}
			if foreign.malformed {
				if _, err := db.Exec(`UPDATE flow_instance_runtime_readiness SET plan = '{}'::jsonb WHERE run_id = $1::uuid AND instance_path = $2`, runIDs[1], paths[1]); err != nil {
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
					BundleSourceFact:                 fact,
					RuntimeInstanceID:                runtimeInstanceID,
				}))
				if err != nil {
					t.Fatalf("construct runtime context %s: %v", fact.BundleHash(), err)
				}
				runtimes = append(runtimes, rt)
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
			before := snapshotServeDynamicTopologyReadiness(t, db)
			contexts := []serveRuntimeBundleContext{
				{runtime: runtimes[0], bundleSourceFact: facts[0]},
				{runtime: runtimes[1], bundleSourceFact: facts[1]},
			}
			err = startServeRuntimeContexts(context.Background(), contexts, nil)
			if err == nil {
				t.Fatal("two-context startup accepted foreign incomplete topology")
			}
			if foreign.malformed {
				if !strings.Contains(err.Error(), "decode") && !strings.Contains(err.Error(), "readiness") {
					t.Fatalf("malformed foreign context error = %v", err)
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
	source runtimecorrelation.BundleSourceFact,
	runID string,
	instancePath string,
	complete bool,
) {
	t.Helper()
	runtimeInstanceID := "11111111-1111-1111-1111-111111111111"
	ctx := runtimecorrelation.WithBundleSourceFact(context.Background(), source)
	ctx = runtimecorrelation.WithRunID(ctx, runID)
	ctx = runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(runtimeInstanceID, source.BundleHash()))
	bundleHash, bundleSource := source.StorageValues()
	runlifecyclefixture.RequirePostgres(t, ctx, db, runlifecyclefixture.Fixture{
		Origin:       runlifecyclefixture.ScenarioSetupOrigin(),
		RunID:        runID,
		BundleHash:   bundleHash,
		BundleSource: bundleSource,
	})
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
		RunID: runID, BundleHash: bundleHash, BundleSource: bundleSource,
		WorkflowVersion: "1.0.0", ExecutionMode: executionmode.Live,
	}).Normalized()
	if err != nil {
		t.Fatalf("normalize readiness plan: %v", err)
	}
	raw, err := json.Marshal(plan)
	if err != nil {
		t.Fatalf("marshal readiness plan: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO flow_instances (run_id, instance_path, flow_template, mode, config, status, created_at)
		VALUES ($1::uuid, $2, $3, 'template', '{}'::jsonb, 'active', NOW())
	`, runID, instancePath, parts[0]); err != nil {
		t.Fatalf("seed flow instance %s: %v", instancePath, err)
	}
	if _, err := db.Exec(`
		INSERT INTO entity_state (entity_id, run_id, flow_instance, entity_type, current_state, fields, created_at, updated_at)
		VALUES ($1::uuid, $2::uuid, $3, 'worker', 'idle', '{}'::jsonb, NOW(), NOW())
	`, entityID, runID, instancePath); err != nil {
		t.Fatalf("seed entity state %s: %v", instancePath, err)
	}
	if _, err := db.Exec(`
		INSERT INTO flow_instance_runtime_readiness (
			run_id, instance_path, plan, topology_ready_at, created_at, updated_at
		) VALUES ($1::uuid, $2, $3::jsonb, $4, NOW(), NOW())
	`, runID, instancePath, raw, nullableServeReadinessTime(complete)); err != nil {
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
		SELECT readiness.run_id::text, readiness.instance_path, readiness.plan::text,
		       COALESCE(readiness.topology_ready_at::text, ''), readiness.updated_at::text,
		       run.bundle_hash, run.bundle_source, run.status
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
		var runID, path, plan, readyAt, updatedAt, bundleHash, bundleSource, runStatus string
		if err := rows.Scan(&runID, &path, &plan, &readyAt, &updatedAt, &bundleHash, &bundleSource, &runStatus); err != nil {
			t.Fatalf("scan readiness snapshot: %v", err)
		}
		snapshot = append(snapshot, fmt.Sprintf("%s|%s|%s|%s|%s|%s|%s|%s", runID, path, plan, readyAt, updatedAt, bundleHash, bundleSource, runStatus))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read readiness snapshot: %v", err)
	}
	return snapshot
}
