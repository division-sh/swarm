package sourceartifact

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestRuntimeProjectionReleaseRetriesFailedDeletionWithoutReopening(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("requires filesystem permission enforcement")
	}
	parent := t.TempDir()
	root := filepath.Join(parent, "projection")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	p := &RuntimeProjection{state: &runtimeProjectionState{root: root, refs: 1}}
	defer func() { _ = os.Chmod(parent, 0o700) }()
	if err := os.Chmod(parent, 0o500); err != nil {
		t.Fatal(err)
	}
	if err := p.Release(); err == nil {
		t.Fatal("release concealed failed filesystem deletion")
	}
	if p.PrivateRoot() != "" {
		t.Fatal("failed release reopened the projection")
	}
	if _, err := p.Retain(); err == nil {
		t.Fatal("failed release permitted a new handle")
	}
	if err := os.Chmod(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := p.Release(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Fatalf("retry forgot the undeleted projection: %v", err)
	}
}

func TestRuntimeProjectionHandleReadsAndReleaseAreSynchronized(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "schema.yaml"), []byte("name: admitted\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	artifact, err := AdmitDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	projection, err := MaterializeRuntimeProjection(artifact)
	if err != nil {
		t.Fatal(err)
	}
	defer projection.Release()
	retained, err := projection.Retain()
	if err != nil {
		t.Fatal(err)
	}
	defer retained.Release()
	projectionRoot := retained.PrivateRoot()
	started := make(chan struct{}, 8)
	var readers sync.WaitGroup
	for range 8 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			started <- struct{}{}
			for range 100 {
				_ = projection.Identity()
				_ = projection.BundleHash()
				_ = projection.PrivateRoot()
				if handle, err := projection.Retain(); err == nil {
					if err := handle.Release(); err != nil {
						t.Error(err)
					}
				}
			}
		}()
	}
	<-started
	if err := projection.Release(); err != nil {
		t.Fatal(err)
	}
	readers.Wait()
	if projection.Identity() != "" || projection.BundleHash() != "" || projection.PrivateRoot() != "" {
		t.Fatal("released handle still exposes its projection")
	}
	if retained.BundleHash() != artifact.BundleHash() || retained.PrivateRoot() != projectionRoot {
		t.Fatal("peer handle release invalidated the retained owner")
	}
	if err := retained.Release(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(projectionRoot); !os.IsNotExist(err) {
		t.Fatalf("final release did not remove the tree: %v", err)
	}
}

func TestRuntimeProjectionOwnsExactGenerationAndLifetime(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "schema.yaml"), []byte("name: admitted\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "prompts"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "prompts", "worker.md"), []byte("worker\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	artifact, err := AdmitDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	projection, err := MaterializeRuntimeProjection(artifact)
	if err != nil {
		t.Fatal(err)
	}
	retained, err := projection.Retain()
	if err != nil {
		t.Fatal(err)
	}
	projectionRoot := projection.PrivateRoot()
	projectionIdentity := projection.Identity()
	if projection.BundleHash() != artifact.BundleHash() || projectionIdentity == "" || retained.Identity() != projectionIdentity || projectionRoot == "" {
		t.Fatalf("projection identity = %q %q %q", projection.BundleHash(), projectionIdentity, projectionRoot)
	}
	if err := ValidateRuntimeProjectionIdentity(projectionIdentity); err != nil {
		t.Fatalf("projection identity %q is invalid: %v", projectionIdentity, err)
	}
	peer, err := MaterializeRuntimeProjection(artifact)
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Release()
	if peer.Identity() == projectionIdentity || peer.PrivateRoot() == projectionRoot {
		t.Fatalf("separate same-hash projection reused process identity or root: first=%q/%q peer=%q/%q", projectionIdentity, projectionRoot, peer.Identity(), peer.PrivateRoot())
	}
	if err := os.WriteFile(filepath.Join(root, "schema.yaml"), []byte("name: mutated\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(projectionRoot, "schema.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "name: admitted\n" {
		t.Fatalf("projected bytes = %q", got)
	}
	for path, want := range map[string]os.FileMode{
		projectionRoot:                                        0o555,
		filepath.Join(projectionRoot, "prompts"):              0o555,
		filepath.Join(projectionRoot, "schema.yaml"):          0o444,
		filepath.Join(projectionRoot, "prompts", "worker.md"): 0o444,
	} {
		info, err := os.Stat(path)
		if err != nil || info.Mode().Perm() != want {
			t.Fatalf("projection mode for %s = %v, %v; want %#o", path, info, err, want)
		}
	}
	if err := projection.Release(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(projectionRoot); err != nil {
		t.Fatalf("retained generation disappeared early: %v", err)
	}
	if err := retained.Release(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(projectionRoot); !os.IsNotExist(err) {
		t.Fatalf("released projection still exists: %v", err)
	}
}

func TestRuntimeProjectionIdentityValidationRejectsNonCanonicalForms(t *testing.T) {
	for _, invalid := range []string{
		"runtime-projection-v1:deadbeef",
		"runtime-projection-v1:" + strings.Repeat("A", 32),
		"projection:" + strings.Repeat("a", 32),
	} {
		if err := ValidateRuntimeProjectionIdentity(invalid); err == nil {
			t.Fatalf("ValidateRuntimeProjectionIdentity(%q) error = nil", invalid)
		}
	}
}
