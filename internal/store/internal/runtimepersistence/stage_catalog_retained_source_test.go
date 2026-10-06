package runtimepersistence

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
	"github.com/google/uuid"
)

func assertRetainedStageCatalogsForFork(t *testing.T, original, retained semanticview.Source) {
	t.Helper()
	bundle, ok := semanticview.Bundle(original)
	if !ok {
		t.Fatal("source fixture lost its admitted bundle")
	}
	for flow, expected := range bundle.Semantics.StageTopologies {
		actual, ok := semanticview.WorkflowStageTopology(retained, flow)
		if !ok || !actual.ValidStageCatalog() || !reflect.DeepEqual(expected.StageIDs(), actual.StageIDs()) || !reflect.DeepEqual(expected.FinalStageIDs(), actual.FinalStageIDs()) {
			t.Fatalf("retained fork changed ordered stage/final facts for %s: expected=%+v actual=%+v", flow, expected, actual)
		}
		before, err := expected.InitialStoredStage()
		if err != nil {
			t.Fatal(err)
		}
		after, err := actual.InitialStoredStage()
		if err != nil || before.ID() != after.ID() || before.IsFinal() != after.IsFinal() || before.IsStatelessPosture() != after.IsStatelessPosture() {
			t.Fatalf("retained fork changed entry for %s: %v", flow, err)
		}
	}
}

func TestStageCatalogRetainedStoreReloadAndSelectedForkSourceBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
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
			selected := fixture.store.(interface {
				selectedSourceArtifactStore
				runforkexecution.SourceArtifactSelectedContractSourceStore
			})
			if _, err := selected.EnsureSourceArtifact(ctx, bundle.SourceArtifact); err != nil {
				t.Fatal(err)
			}
			runID := uuid.NewString()
			requireRunFixtureForTest(t, ctx, fixture.store, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID, BundleHash: bundle.SourceArtifact.BundleHash(), Artifact: bundle.SourceArtifact, StartedAt: time.Now().UTC()})
			// Change the live directory to a sorted, still graph-valid source. The
			// retained selected source must not reconstruct entry from it.
			if err := os.WriteFile(path, []byte("name: ordered-retained\nstages: {cooling: {}, Done: {final: true}, registered: {}}\n"), 0600); err != nil {
				t.Fatal(err)
			}
			var reloaded interface {
				selectedSourceArtifactStore
				runforkexecution.SourceArtifactSelectedContractSourceStore
			}
			if backend.name == "sqlite" {
				var sequence int
				var name, databasePath string
				if err := fixture.db.QueryRow(`PRAGMA database_list`).Scan(&sequence, &name, &databasePath); err != nil {
					t.Fatal(err)
				}
				reloaded = newBootstrappedSQLiteRuntimeStoreForPath(t, databasePath)
			} else {
				reloaded = newTestPostgresStore(t, fixture.db)
			}
			persisted, err := reloaded.GetSourceArtifact(context.Background(), bundle.SourceArtifact.BundleHash())
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
			graph, ok := loaded.WorkflowStageTopology(".")
			if !ok || graph.InitialStage != "registered" || !reflect.DeepEqual(graph.StageIDs(), []string{"registered", "cooling", "Done"}) || !reflect.DeepEqual(graph.FinalStageIDs(), []string{"Done"}) {
				t.Fatalf("retained read changed reviewed entry: %+v", graph)
			}
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
			assertRetainedStageCatalogsForFork(t, semanticview.Wrap(bundle), forkSource.Source)
		})
	}
}
