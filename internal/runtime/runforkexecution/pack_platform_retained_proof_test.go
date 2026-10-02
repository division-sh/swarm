package runforkexecution

import (
	"bytes"
	"database/sql"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestPackPlatformRetainedSourceExecutesSelectedForkBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var db *sql.DB
			var selected startupownership.Store
			var owner SelectedContractExecutionOwner
			if backend == "sqlite" {
				s := storetest.StartSQLiteRuntimeStore(t)
				selected, db, owner = s, storetest.Database(s), selectedContractSQLiteExecutionOwnerForTest(t, s)
			} else {
				_, db, _ = testutil.StartPostgres(t)
				s := storetest.AdmitPostgresRuntimeStore(t, db)
				selected, owner = s, selectedContractExecutionOwnerForTest(t, s)
			}
			ctx := runForkTestContext(t)
			repo, root := runForkExecutionRepoRoot(t), t.TempDir()
			if err := os.CopyFS(root, os.DirFS(filepath.Join(repo, "tests/tier1-primitives/test-emits-multiple"))); err != nil {
				t.Fatal(err)
			}
			base, err := packartifact.LoadEmbeddedPlatformPackInventory("0.7.0")
			if err != nil {
				t.Fatal(err)
			}
			for _, id := range []string{"provider.telegram", "provider.telegram.connector", "provider.telegram.hitl_channel"} {
				if changed, err := packartifact.ImportEmbeddedPack(root, id, base); err != nil || !changed {
					t.Fatalf("import %s: %t %v", id, changed, err)
				}
			}
			fixture := admittedFixtureSelectedContractSourceLoader{RepoRoot: repo, SourceRoot: root, PlatformSpecPath: contracts.DefaultPlatformSpecFile(repo)}
			loaded, err := fixture.LoadRunForkSelectedContractSource(ctx, runfork.RunForkContractSelection{Mode: "selected_contracts"})
			if err != nil {
				t.Fatal(err)
			}
			before, ok := semanticview.Bundle(loaded.Source)
			if !ok {
				t.Fatal("typed bundle missing")
			}
			beforeProjection, err := packadmission.FromBundle(before)
			if err != nil {
				t.Fatal(err)
			}
			sourceRun, eventID := uuid.NewString(), uuid.NewString()
			seedSelectedOperationSource(t, ctx, backend, db, selected, loaded, sourceRun, eventID, uuid.NewString())
			if err := os.RemoveAll(root); err != nil {
				t.Fatal(err)
			}
			loader := SourceArtifactSelectedContractSourceLoader{RepoRoot: repo, PlatformSpecPath: contracts.DefaultPlatformSpecFile(repo), Store: selected.(SourceArtifactSelectedContractSourceStore)}
			selection := runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeBundleHash, BundleHash: loaded.SourceArtifactFact.BundleHash()}
			for i := 0; i < 2; i++ {
				retained, err := loader.LoadRunForkSelectedContractSourceForRequest(ctx, SelectedContractSourceLoadRequest{SourceRunID: sourceRun, BundleHash: selection.BundleHash, Selection: selection})
				if err != nil {
					t.Fatal(err)
				}
				after, ok := semanticview.Bundle(retained.Source)
				if !ok {
					t.Fatal("retained typed bundle missing")
				}
				projection, err := packadmission.FromBundle(after)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(before.SourceArtifact.LogicalBlob(), after.SourceArtifact.LogicalBlob()) || !reflect.DeepEqual(before.EffectiveProvenance().Entries(), after.EffectiveProvenance().Entries()) || !reflect.DeepEqual(beforeProjection.ChannelPlans, projection.ChannelPlans) || before.PackInventory.Digest() != after.PackInventory.Digest() {
					t.Fatal("retained reload changed source/provenance/plan/hash")
				}
				for _, entry := range before.PackInventory.Entries() {
					other, ok := after.PackInventory.Lookup(entry.ID())
					if !ok || !bytes.Equal(entry.ManifestBody(), other.ManifestBody()) || !bytes.Equal(entry.EnvelopeBody(), other.EnvelopeBody()) {
						t.Fatalf("retained pack bytes changed: %s", entry.ID())
					}
				}
				cleanupLoadedSelectedContractSource(retained)
			}
			result, err := ExecuteSelectedContractRunFork(ctx, SelectedContractExecutionRequest{
				SourceRunID: sourceRun, At: eventID, AllowSourceFreeze: true, Owner: owner, SourceLoader: loader,
				ContractSelection: runforkadmission.SelectedContractSelection(loaded.Source),
				AgentRuntime:      SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.MockOnly, ProcessCapability: selectedContractTestProcessCapability(t, ctx, selected)},
			})
			if err != nil {
				t.Fatalf("retained selected fork execution: %v", err)
			}
			var outputs int
			if err := db.QueryRowContext(ctx, `SELECT count(*) FROM events WHERE run_id=$1 AND event_name='item.processed'`, result.Materialization.ForkRunID).Scan(&outputs); err != nil || outputs != 1 {
				t.Fatalf("selected executor settlement=%d: %v", outputs, err)
			}
		})
	}
}
