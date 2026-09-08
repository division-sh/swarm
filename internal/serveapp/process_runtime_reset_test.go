package serveapp

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/google/uuid"
)

func TestResetConvergedOperationRetryDoesNotWithdrawSuccessor(t *testing.T) {
	id := uuid.NewString()
	ready := &atomic.Bool{}
	ready.Store(true)
	releases := 0
	s := &processLifecycleSupervisor{
		resetOperationID: id, resetConverged: true, ready: ready,
		resetContexts: []serveRuntimeBundleContext{{
			loaded: serveRuntimeBundle{cleanup: func() error { releases++; return nil }},
		}},
	}
	// A converged operation must not need construction inputs or a manager.
	// Touching withdrawal/reconstruction would fail, not silently pass this test.
	for i := 0; i < 2; i++ {
		reset, err := s.BeginDestructiveReset(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		err = reset.Complete(context.Background(), true)
		reset.Release()
		if err != nil || !ready.Load() || s.resetting || releases != 0 {
			t.Fatalf("retry touched successor: err=%v ready=%v resetting=%v releases=%d", err, ready.Load(), s.resetting, releases)
		}
	}
}

func TestResetMissingContainerInspectorCannotProveAbsence(t *testing.T) {
	s := newProcessLifecycleSupervisor(nil, nil)
	if _, err := s.InspectManagedContainer(context.Background(), "saved-object-id"); err == nil {
		t.Fatal("missing inspection owner was accepted as evidence that the container is absent")
	}
}

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
	intent, err := projection.CleanupIntent()
	if err != nil {
		t.Fatal(err)
	}
	var stopped []string
	s.resetContexts = nil
	s.resetStartup = true
	s.resetRecoveredProjections = []destructivereset.SourceProjection{{Cleanup: intent, ManagedContainers: true}}
	s.resetContainerRuntime = serveRuntimeWorkspaceStub{
		managedContainers: []destructivereset.ContainerRef{owned, successor, foreign}, stoppedContainers: &stopped,
	}
	got, err = s.ManagedResetContainerInventory(context.Background())
	if err != nil || len(got) != 1 || got[0] != owned {
		t.Fatalf("recovered inventory expanded source scope: %#v, %v", got, err)
	}
	for _, unowned := range []destructivereset.ContainerRef{successor, foreign} {
		if err := s.StopManagedContainer(context.Background(), unowned); err == nil {
			t.Fatal("recovery stopped an unowned projection")
		}
	}
	if len(stopped) != 0 {
		t.Fatal("unowned stop reached the container runtime")
	}
	if err := s.StopManagedContainer(context.Background(), owned); err != nil {
		t.Fatal(err)
	}
	if len(stopped) != 1 || stopped[0] != owned.Name {
		t.Fatalf("recovery lost exact predecessor target: %v", stopped)
	}
	s.resetRecoveredProjections[0].ManagedContainers = false
	if got, err = s.ManagedResetContainerInventory(context.Background()); err != nil || len(got) != 0 {
		t.Fatalf("host-only scope inventoried Docker: %#v, %v", got, err)
	}
	if err := s.StopManagedContainer(context.Background(), owned); err == nil {
		t.Fatal("host-only scope authorized Docker stop")
	}
}
