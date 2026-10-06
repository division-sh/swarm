package runtimepersistence_test

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

type retainedStageSourceStore interface {
	runforkexecution.SourceArtifactSelectedContractSourceStore
	storetest.RunFixtureStore
	Close() error
}

func TestStageCatalogRetainedStoreReloadAndSelectedForkSourceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected retainedStageSourceStore
			var reopen func() retainedStageSourceStore
			if backend == "sqlite" {
				owner, open := storetest.StartSQLiteRuntimeStoreWithReopen(t, context.Background())
				selected, reopen = owner, func() retainedStageSourceStore { return open() }
			} else {
				owner, open := storetest.StartPostgresRuntimeStoreWithReopen(t)
				selected, reopen = owner, func() retainedStageSourceStore { return open() }
			}
			root := t.TempDir()
			path := filepath.Join(root, "schema.yaml")
			body := []byte("name: ordered-retained\nstages:\n  registered: {}\n  cooling: {}\n  Done: {final: true}\n")
			if err := os.WriteFile(path, body, 0600); err != nil {
				t.Fatal(err)
			}
			repo := canonicalrouting.RepoRoot(t)
			bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, root, contracts.DefaultPlatformSpecFile(repo))
			if err != nil {
				t.Fatal(err)
			}
			ctx := testAuthorActivityContext()
			runID := uuid.NewString()
			storetest.RequireRun(t, ctx, selected, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runID, Artifact: bundle.SourceArtifact, StartedAt: time.Now().UTC()})
			// A sorted live directory remains graph-valid but changes entry. Neither
			// selected-store reopen nor fork selection may reconstruct from it.
			if err := os.WriteFile(path, []byte("name: ordered-retained\nstages: {cooling: {}, Done: {final: true}, registered: {}}\n"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := selected.Close(); err != nil {
				t.Fatal(err)
			}
			reloaded := reopen()
			persisted, err := reloaded.GetSourceArtifact(ctx, bundle.SourceArtifact.BundleHash())
			if err != nil {
				t.Fatal(err)
			}
			artifact, err := persisted.Decode()
			if err != nil {
				t.Fatal(err)
			}
			logical, err := sourceartifact.DecodeLogical(artifact.LogicalBlob())
			if err != nil {
				t.Fatal(err)
			}
			loaded, err := contracts.LoadWorkflowContractBundleFromArtifact(repo, logical, contracts.DefaultPlatformSpecFile(repo), contracts.WorkflowContractLoadOptions{})
			if err != nil {
				t.Fatal(err)
			}
			assertEntry := func(source semanticview.Source) {
				t.Helper()
				graph, ok := semanticview.WorkflowStageTopology(source, ".")
				if !ok || !graph.ValidStageCatalog() || graph.InitialStage != "registered" || !reflect.DeepEqual(graph.StageIDs(), []string{"registered", "cooling", "Done"}) || !reflect.DeepEqual(graph.FinalStageIDs(), []string{"Done"}) {
					t.Fatalf("retained read changed reviewed entry: %+v", graph)
				}
				entry, err := graph.InitialStoredStage()
				if err != nil || entry.ID() != "registered" || entry.IsFinal() || entry.IsStatelessPosture() {
					t.Fatalf("retained read changed typed entry: %+v: %v", entry, err)
				}
			}
			assertEntry(semanticview.Wrap(loaded))
			loader := runforkexecution.SourceArtifactSelectedContractSourceLoader{RepoRoot: repo, PlatformSpecPath: contracts.DefaultPlatformSpecFile(repo), Store: reloaded}
			forkSource, err := loader.LoadRunForkSelectedContractSourceForRequest(ctx, runforkexecution.SelectedContractSourceLoadRequest{SourceRunID: runID, Selection: runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeSelectedContracts}})
			if err != nil {
				t.Fatal(err)
			}
			if forkSource.Cleanup != nil {
				t.Cleanup(func() {
					if err := forkSource.Cleanup(); err != nil {
						t.Error(err)
					}
				})
			}
			assertEntry(forkSource.Source)
		})
	}
}
