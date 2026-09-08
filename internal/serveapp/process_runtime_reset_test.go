package serveapp

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

func TestResetInventoryExcludesForeignAndSuccessorProjections(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "schema.yaml"), []byte("name: reset-inventory\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	artifact, err := sourceartifact.AdmitDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	projection, err := sourceartifact.MaterializeRuntimeProjection(artifact)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = projection.Release() })
	owned := destructivereset.ContainerRef{Name: "owned", BundleHash: projection.BundleHash(), SourceProjection: projection.Identity()}
	successor, foreign := owned, owned
	successor.Name, successor.SourceProjection = "successor", "other-projection"
	foreign.Name, foreign.BundleHash = "foreign", "other-bundle"
	s := &processLifecycleSupervisor{resetContexts: []serveRuntimeBundleContext{{
		loaded:             serveRuntimeBundle{sourceProjection: projection},
		sourceArtifactFact: mustServeTestEphemeralSourceArtifactFact(projection.BundleHash()),
		workspaces:         serveRuntimeWorkspaceStub{managedContainers: []destructivereset.ContainerRef{owned, successor, foreign, owned}},
	}}}
	got, err := s.ManagedResetContainerInventory(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != owned {
		t.Fatalf("reset inventory = %#v, want only current exact projection", got)
	}
}
