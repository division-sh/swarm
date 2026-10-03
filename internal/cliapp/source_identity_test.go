package cliapp

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/sourceartifact"
)

func Test2376LocalIdentitySelectorsUseAdmittedSource(t *testing.T) {
	root, err := NewInvocationRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root.Path(), "selected")
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "schema.yaml"), []byte("description: selected\n"), 0600); err != nil {
		t.Fatal(err)
	}
	want, err := sourceartifact.AdmitDirectory(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, operand := range []string{"selected", path} {
		got, err := admitCLIIdentitySource(root, operand)
		if err != nil || got != want.BundleHash() {
			t.Fatalf("%q: %v %#v", operand, err, got)
		}
	}
	if err := os.WriteFile(filepath.Join(path, "manifest.yaml"), []byte("name: Reception\nversion: 1.0.0\nplatform_version: '>=99.0.0'\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := admitCLIIdentitySource(root, "selected"); err == nil || !strings.Contains(err.Error(), "platform_version") {
		t.Fatalf("identity selector admitted incompatible root: %v", err)
	}
	if _, err := admitCLIIdentitySource(root, " "); err == nil {
		t.Fatal("explicit empty source guessed cwd")
	}
}

func identitySource2376(t *testing.T) (string, *sourceartifact.AdmittedSourceArtifact) {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "schema.yaml"), []byte("description: identity selector fixture\n"), 0600); err != nil {
		t.Fatal(err)
	}
	artifact, err := sourceartifact.AdmitDirectory(dir)
	if err != nil {
		t.Fatal(err)
	}
	return dir, artifact
}
