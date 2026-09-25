package serveapp

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/budgetspend"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

type stageRecoverySelectedStore interface {
	budgetspend.Store
	sourceArtifactReader
	sourceartifactfixture.Writer
	CreateEntity(context.Context, runtimetools.EntityCreateRecord) (runtimetools.EntityCreateResult, error)
}

type countedStageRecoveryStore struct {
	budgetspend.Store
	entityQueries map[string]int
}

func (s *countedStageRecoveryStore) SumSpendUSD(ctx context.Context, query budgetspend.SpendQuery) (float64, error) {
	if query.EntityID != "" {
		s.entityQueries[query.EntityID]++
	}
	return s.Store.SumSpendUSD(ctx, query)
}

type corruptStageRecoveryReader struct {
	sourceArtifactReader
	hash string
}

type missingStageRecoveryReader struct {
	sourceArtifactReader
	hash string
}

func (r missingStageRecoveryReader) GetSourceArtifact(ctx context.Context, hash string) (sourceartifact.Persisted, error) {
	if hash == r.hash {
		return sourceartifact.Persisted{}, sourceartifact.ErrNotFound
	}
	return r.sourceArtifactReader.GetSourceArtifact(ctx, hash)
}

func (r corruptStageRecoveryReader) GetSourceArtifact(ctx context.Context, hash string) (sourceartifact.Persisted, error) {
	if hash == r.hash {
		return sourceartifact.Persisted{BundleHash: hash, SourceBlob: []byte("not a source artifact"), MemberCount: 1, TotalBytes: 21}, nil
	}
	return r.sourceArtifactReader.GetSourceArtifact(ctx, hash)
}

func TestBudgetRecoveryLoadsExactRetainedStagesAndStatelessPostureBothStores(t *testing.T) {
	repo := runtimepipeline.WorkflowRepoRoot()
	spec := runtimecontracts.DefaultPlatformSpecFile(repo)
	packBases := testPlatformPackBaseGenerations(t)
	artifacts := []*sourceartifact.AdmittedSourceArtifact{
		stageRecoveryArtifact(t, "ready:\n    initial: true\n  Ready:\n    terminal: true"),
		stageRecoveryArtifact(t, "open:\n    initial: true\n  ready:\n    terminal: true"),
		stageRecoveryArtifact(t, "[]"),
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected stageRecoverySelectedStore
			var db *sql.DB
			if backend == "sqlite" {
				s := storetest.StartSQLiteRuntimeStore(t)
				selected, db = s, storetest.DatabaseForTest(s)
			} else {
				_, opened, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				selected, db = storetest.AdmitPostgresRuntimeStore(t, opened), opened
			}
			ctx := context.Background()
			sharedEntity, terminalEntity := uuid.NewString(), uuid.NewString()
			runs := make([]string, len(artifacts))
			for i, artifact := range artifacts {
				runs[i] = uuid.NewString()
				sourceartifactfixture.RequireArtifact(t, runStatusAuthorActivityContext(sourceartifactfixture.FactFor(artifact)), selected, artifact)
				entitySource, err := loadBudgetRecoveryStageSource(ctx, selected, repo, spec, packBases, artifact.BundleHash())
				if err != nil {
					t.Fatalf("load retained entity source %d: %v", i, err)
				}
				fixture := storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runs[i], Artifact: artifact, StartedAt: time.Now().UTC()}
				if backend == "sqlite" {
					storetest.RequireSQLiteRun(t, ctx, db, fixture)
				} else {
					storetest.RequirePostgresRun(t, ctx, db, fixture)
				}
				seedStageRecoveryFlowInstance(t, ctx, db, backend, runs[i])
				initial := "ready"
				entityID := sharedEntity
				if i == 1 {
					entityID = terminalEntity
				} else if i == 2 {
					initial = "pending"
				}
				createCtx := runtimecorrelation.WithRunID(runStatusAuthorActivityContext(sourceartifactfixture.FactFor(artifact)), runs[i])
				if _, err := selected.CreateEntity(createCtx, runtimetools.EntityCreateRecord{
					RunID: runs[i], EntityID: entityID, FlowInstance: "child", EntityType: "item", CurrentState: initial,
					Source: entitySource, CreatedAt: time.Now().UTC(), Writer: runtimetools.EntityMutationWriter{Type: "agent", ID: "stage-recovery-proof", HandlerStep: "create_entity"},
				}); err != nil {
					t.Fatalf("create retained entity %d: %v", i, err)
				}
			}
			current, err := loadBudgetRecoveryStageSource(ctx, selected, repo, spec, packBases, artifacts[0].BundleHash())
			if err != nil {
				t.Fatalf("load current source: %v", err)
			}
			stateless, err := loadBudgetRecoveryStageSource(ctx, selected, repo, spec, packBases, artifacts[2].BundleHash())
			if err != nil {
				t.Fatalf("load stateless source: %v", err)
			}
			terminalSource, err := loadBudgetRecoveryStageSource(ctx, selected, repo, spec, packBases, artifacts[1].BundleHash())
			if err != nil {
				t.Fatalf("load terminal source: %v", err)
			}
			terminalGraph, ok := semanticview.WorkflowStageTopology(terminalSource, "child")
			if !ok {
				t.Fatal("retained terminal source has no child graph")
			}
			terminalStage, err := terminalGraph.ResolveStage("ready")
			if err != nil || !terminalStage.IsTerminal() {
				t.Fatalf("retained ready terminal stage = %#v, %v", terminalStage, err)
			}
			if got := stateless.FlowInitialStage("child"); got != "" {
				t.Fatalf("author-facing stateless initial stage = %q, want absent", got)
			}
			targets, err := selected.ListBudgetProjectionTargets(ctx)
			if err != nil {
				t.Fatalf("list selected budget targets: %v", err)
			}
			for i, runID := range runs {
				found := false
				wantStage := "ready"
				if i == 2 {
					wantStage = "pending"
				}
				for _, target := range targets {
					if target.RunID != runID {
						continue
					}
					found = true
					if target.BundleHash != artifacts[i].BundleHash() || target.FlowTemplate != "child" || target.FlowInstance != "child" || target.Stage != wantStage {
						t.Fatalf("run %d target evidence = %#v", i, target)
					}
				}
				if !found {
					t.Fatalf("run %d has no budget target in %#v", i, targets)
				}
			}
			queries := &countedStageRecoveryStore{Store: selected, entityQueries: make(map[string]int)}
			loads := make(map[string]int)
			tracker := runtimepkg.NewBudgetTracker(queries, &runtimebus.EventBus{}, &config.Config{Extensions: map[string]any{
				"budget": map[string]any{"per_entity_monthly_cap": 10},
			}}, nil, nil, current, executionposture.Live, runtimepkg.BudgetRecoveryStageSources{
				CurrentBundleHash: artifacts[0].BundleHash(),
				Load: func(ctx context.Context, hash string) (semanticview.Source, error) {
					loads[hash]++
					return loadBudgetRecoveryStageSource(ctx, selected, repo, spec, packBases, hash)
				},
			})
			for pass := 1; pass <= 2; pass++ {
				if err := tracker.ProjectRecoveryBudgetState(ctx); err != nil {
					t.Fatalf("recovery pass %d: %v", pass, err)
				}
				if got := tracker.CurrentState(string(budgetspend.ScopeEntity), sharedEntity); got != "ok" {
					t.Fatalf("shared nonterminal/stateless state = %q, want ok", got)
				}
				if queries.entityQueries[terminalEntity] != 0 || queries.entityQueries[sharedEntity] != pass || loads[artifacts[1].BundleHash()] != pass || loads[artifacts[2].BundleHash()] != pass {
					t.Fatalf("recovery pass %d: entity queries=%#v loads=%#v", pass, queries.entityQueries, loads)
				}
			}
			for _, tc := range []struct {
				name   string
				reader sourceArtifactReader
			}{
				{"missing", missingStageRecoveryReader{sourceArtifactReader: selected, hash: artifacts[2].BundleHash()}},
				{"corrupt", corruptStageRecoveryReader{sourceArtifactReader: selected, hash: artifacts[2].BundleHash()}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					badTracker := runtimepkg.NewBudgetTracker(selected, &runtimebus.EventBus{}, &config.Config{Extensions: map[string]any{
						"budget": map[string]any{"per_entity_monthly_cap": 10},
					}}, nil, nil, current, executionposture.Live, runtimepkg.BudgetRecoveryStageSources{
						CurrentBundleHash: artifacts[0].BundleHash(),
						Load: func(ctx context.Context, hash string) (semanticview.Source, error) {
							return loadBudgetRecoveryStageSource(ctx, tc.reader, repo, spec, packBases, hash)
						},
					})
					if err := badTracker.ProjectRecoveryBudgetState(ctx); err == nil {
						t.Fatal("recovery accepted unavailable selected source")
					}
				})
			}
			query := "UPDATE entity_state SET current_state = 'unknown' WHERE run_id = ? AND entity_id = ?"
			if backend == "postgres" {
				query = "UPDATE entity_state SET current_state = 'unknown' WHERE run_id = $1::uuid AND entity_id = $2::uuid"
			}
			if _, err := db.ExecContext(ctx, query, runs[0], sharedEntity); err != nil {
				t.Fatalf("inject undeclared staged storage state: %v", err)
			}
			if err := tracker.ProjectRecoveryBudgetState(ctx); err == nil || !strings.Contains(err.Error(), "undeclared stage") {
				t.Fatalf("staged unknown recovery = %v, want declared-stage refusal", err)
			}
		})
	}
}

func stageRecoveryArtifact(t *testing.T, stages string) *sourceartifact.AdmittedSourceArtifact {
	t.Helper()
	root := t.TempDir()
	childSchema := "name: child\nmode: static\nstages:\n  " + stages + "\n"
	if stages == "[]" {
		childSchema = "name: child\nmode: static\nstages: []\n"
	}
	for path, contents := range map[string]string{
		"schema.yaml":         "name: stage-recovery\n",
		"child/schema.yaml":   childSchema,
		"child/entities.yaml": "item: {}\n",
		"policy.yaml":         "budget_warning_percent: 50\nbudget_throttle_percent: 75\nbudget_emergency_percent: 90\n",
	} {
		absolute := filepath.Join(root, path)
		if err := os.MkdirAll(filepath.Dir(absolute), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(absolute, []byte(contents), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	artifact, err := sourceartifact.AdmitDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	return artifact
}

func seedStageRecoveryFlowInstance(t *testing.T, ctx context.Context, db *sql.DB, backend, runID string) {
	t.Helper()
	query := "INSERT INTO flow_instances (run_id, instance_path, flow_template, mode, config, status) VALUES (?, 'child', 'child', 'static', '{}', 'active')"
	if backend == "postgres" {
		query = "INSERT INTO flow_instances (run_id, instance_path, flow_template, mode, config, status) VALUES ($1::uuid, 'child', 'child', 'static', '{}'::jsonb, 'active')"
	}
	if _, err := db.ExecContext(ctx, query, runID); err != nil {
		t.Fatalf("seed selected child flow instance: %v", err)
	}
}
