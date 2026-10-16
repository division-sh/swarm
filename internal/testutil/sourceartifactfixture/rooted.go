package sourceartifactfixture

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/sourceartifact"
)

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
