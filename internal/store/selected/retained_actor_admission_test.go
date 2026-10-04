package selected

import (
	"context"
	"database/sql"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/backendselection"
	"github.com/division-sh/swarm/internal/store/construction"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func TestRetainedActorAdmissionRealSnapshotBothStores(t *testing.T) {
	root := filepath.Join("testdata", "retained_actor_admission")
	bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(selectedStoreRepoRoot(t), root, filepath.Join(selectedStoreRepoRoot(t), "platform-spec.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)
	plans, err := store.GeneratePlatformTableDDLs(bundle.Platform)
	if err != nil {
		t.Fatal(err)
	}
	schema := store.SchemaBootstrapRequest{PlatformPlans: plans, Origin: store.RuntimeStoreOrigin{
		SwarmVersion: "retained-actor-inspection", PlatformVersion: bundle.Platform.Platform.Version, CreatedAt: time.Now().UTC(),
	}}
	for _, backend := range []backendselection.Backend{backendselection.BackendSQLite, backendselection.BackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			request := AuthorityRequest{Selection: backendselection.Selection{Backend: backend}}
			var owner interface {
				store.SchemaBootstrapper
				agentfixture.Store
				Close() error
			}
			var db *sql.DB
			if backend == backendselection.BackendSQLite {
				request.Selection.SQLitePath = filepath.Join(t.TempDir(), "store.db")
				selected, handle, err := construction.OpenSQLiteRuntimeWithOwnershipBinding(request.Selection.SQLitePath)
				if err != nil {
					t.Fatal(err)
				}
				owner, db = selected, handle
			} else {
				request.PostgresDSN, _, _ = testutil.StartEmptyPostgres(t)
				selected, handle, err := construction.OpenPostgres(request.PostgresDSN)
				if err != nil {
					t.Fatal(err)
				}
				owner, db = selected, handle
			}
			t.Cleanup(func() {
				if err := owner.Close(); err != nil {
					t.Error(err)
				}
			})
			if err := owner.BootstrapSchema(ctx, schema); err != nil {
				t.Fatal(err)
			}
			fact := sourceartifactfixture.RequireArtifact(t, ctx, owner, bundle.SourceArtifact)
			runtimeID := uuid.NewString()
			ctx = correlation.WithSourceArtifactFact(ctx, fact)
			ctx = correlation.WithRuntimeInstanceID(ctx, runtimeID)
			ctx = effects.WithExecutionMode(ctx, effects.ExecutionModeLive)
			ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(runtimeID, fact.BundleHash()))
			oldOptions := manager.AgentManagerOptions{LLMBackend: llmselection.BackendOpenAIResponses, ExecutionPosture: executionposture.Live, RequireModelResolution: true}
			blueprints, err := manager.ResolveStaticTopologyBlueprints(oldOptions, source)
			if err != nil || len(blueprints) == 0 {
				t.Fatalf("loaded source blueprint: %d %v", len(blueprints), err)
			}
			for _, blueprint := range blueprints {
				actor, err := blueprint.Materialize(uuid.NewString())
				if err != nil {
					t.Fatal(err)
				}
				if err := agentfixture.UpsertStaticForSource(t, ctx, owner, actor, fact); err != nil {
					t.Fatal(err)
				}
			}
			capability, err := agentfixture.ProcessCapability(t, ctx, owner)
			if err != nil {
				t.Fatal(err)
			}
			if err := capability.Release(ctx); err != nil {
				t.Fatal(err)
			}
			before, err := owner.LoadAgents(ctx)
			if err != nil {
				t.Fatal(err)
			}
			var authorityBefore int
			if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM runtime_startup_authority_facts").Scan(&authorityBefore); err != nil {
				t.Fatal(err)
			}
			inspection, err := OpenAdmissionInspection(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := inspection.Close(); err != nil {
					t.Error(err)
				}
			})
			options := oldOptions
			options.LLMBackend = llmselection.BackendAnthropic
			_, err = inspection.Inspect(ctx, schema, func(snapshot *AdmissionSnapshot) error {
				actors, err := manager.InspectRetainedActors(ctx, snapshot, source, fact, options)
				if err != nil {
					return err
				}
				if actors.Observed != len(blueprints) || actors.Reconciliations != len(blueprints) || len(actors.Configurations) != len(blueprints) {
					t.Fatalf("actual persisted admission projection: %+v", actors)
				}
				for _, cfg := range actors.Configurations {
					if cfg.ResolvedLLMBackend != options.LLMBackend {
						t.Fatalf("retained descriptor borrowed predecessor selection: %+v", cfg)
					}
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			after, err := owner.LoadAgents(ctx)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("inspection reconciled or hydrated persisted actors: %v", err)
			}
			var authorityAfter int
			if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM runtime_startup_authority_facts").Scan(&authorityAfter); err != nil || authorityAfter != authorityBefore {
				t.Fatalf("inspection acquired process authority: %d -> %d %v", authorityBefore, authorityAfter, err)
			}
			if possession, err := inspection.ProbePossession(ctx); err != nil || !possession.Available {
				t.Fatalf("actor inspection retained possession: %+v %v", possession, err)
			}
		})
	}
}
