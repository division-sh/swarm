package sourceartifactfixture

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/sourceartifact"
)

type RootedWriter interface {
	EnsureRootedSource(context.Context, sourceartifact.RootedSource) (sourceartifact.RootedEnsureResult, error)
}

// CapturedRoot retains an existing filesystem admission for in-memory compiler
// controls. Decoded copies establish byte parity, never rooted authority.
func CapturedRoot(t testing.TB, captured *sourceartifact.AdmittedSourceArtifact, copies ...*sourceartifact.AdmittedSourceArtifact) sourceartifact.RootedSource {
	t.Helper()
	rooted, err := captured.RootedSource()
	if err != nil {
		t.Fatalf("captured filesystem root is required: %v", err)
	}
	for _, copy := range copies {
		if copy == nil || copy.BundleHash() != captured.BundleHash() || !bytes.Equal(copy.LogicalBlob(), captured.LogicalBlob()) {
			t.Fatal("decoded compiler copy differs from its captured source")
		}
	}
	return rooted
}

// Rooted admits an exact fixture copy through the real filesystem owner.
// The returned path is only for fixture mutation/removal, never reconstruction.
func Rooted(t testing.TB, directory string, artifact *sourceartifact.AdmittedSourceArtifact) (sourceartifact.RootedSource, string) {
	t.Helper()
	if artifact == nil {
		t.Fatal("rooted source fixture requires admitted member bytes")
	}
	if err := sourceartifact.ValidateFlowSegment(directory); err != nil {
		t.Fatalf("rooted source fixture directory: %v", err)
	}
	root := filepath.Join(t.TempDir(), directory)
	for _, entry := range artifact.Entries() {
		file := filepath.Join(root, filepath.FromSlash(entry.Label()))
		if err := os.MkdirAll(filepath.Dir(file), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(file, entry.Bytes(), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	admitted, err := sourceartifact.AdmitDirectory(root)
	if err != nil {
		t.Fatalf("admit rooted source fixture: %v", err)
	}
	if admitted.BundleHash() != artifact.BundleHash() || !bytes.Equal(admitted.LogicalBlob(), artifact.LogicalBlob()) {
		t.Fatal("rooted fixture admission changed the exact member snapshot")
	}
	rooted, err := admitted.RootedSource()
	if err != nil {
		t.Fatal(err)
	}
	return rooted, root
}
