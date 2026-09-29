package contracts

import (
	"bytes"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/sourceartifact"
)

func TestNodeAdmissionDiskCatalogAndRetainedSourceParity(t *testing.T) {
	repo := repoRootForContractsTest(t)
	for _, path := range []string{"fan-in/stream", "fan-in/barrier", "notify-all-children", "template-create-minted-key", "template-reply"} {
		t.Run(path, func(t *testing.T) {
			root := filepath.Join(repo, "examples/routing", path)
			disk, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
			if err != nil {
				t.Fatal(err)
			}
			persisted, err := sourceartifact.PersistedFromArtifact(disk.SourceArtifact, time.Unix(1, 0))
			if err != nil {
				t.Fatal(err)
			}
			catalog, err := persisted.Decode()
			if err != nil {
				t.Fatal(err)
			}
			retained, err := sourceartifact.DecodeLogical(catalog.LogicalBlob())
			if err != nil {
				t.Fatal(err)
			}
			for name, artifact := range map[string]*sourceartifact.AdmittedSourceArtifact{"catalog": catalog, "retained": retained} {
				loaded, err := LoadWorkflowContractBundleFromArtifact(repo, artifact, DefaultPlatformSpecFile(repo), WorkflowContractLoadOptions{})
				if err != nil {
					t.Fatalf("%s: %v", name, err)
				}
				if len(loaded.ScopedNodeRecords()) == 0 {
					t.Fatal("parity fixture must reach node admission")
				}
				if !bytes.Equal(disk.SourceArtifact.LogicalBlob(), artifact.LogicalBlob()) || disk.SourceArtifact.BundleHash() != artifact.BundleHash() {
					t.Fatalf("%s changed exact source bytes/hash", name)
				}
				for _, comparison := range []struct {
					label       string
					left, right any
				}{
					{"nodes", disk.ScopedNodeRecords(), loaded.ScopedNodeRecords()},
					{"joins", disk.WorkflowJoins(), loaded.WorkflowJoins()},
					{"timers", disk.WorkflowTimers(), loaded.WorkflowTimers()},
					{"fan-out", disk.FanOutPlans(), loaded.FanOutPlans()},
					{"activities", disk.ActivitySites(), loaded.ActivitySites()},
					{"connects", disk.CompositionConnects(), loaded.CompositionConnects()},
					{"provenance", disk.EffectiveProvenance().Entries(), loaded.EffectiveProvenance().Entries()},
				} {
					if !reflect.DeepEqual(comparison.left, comparison.right) {
						t.Errorf("%s changed %s", name, comparison.label)
					}
				}
			}
		})
	}
}
