package sourceartifact

import (
	"os"
	"path/filepath"
	"testing"
)

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
	if projection.BundleHash() != artifact.BundleHash() || projectionRoot == "" {
		t.Fatalf("projection identity = %q %q", projection.BundleHash(), projectionRoot)
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
