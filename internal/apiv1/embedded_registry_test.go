package apiv1

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/platform"
)

func TestHandlerUsesEmbeddedRegistryWithoutPathSelection(t *testing.T) {
	t.Chdir(t.TempDir())
	if err := os.WriteFile("platform-spec.yaml", []byte("not the embedded spec\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	handler, err := NewHandler(Options{})
	if err != nil {
		t.Fatalf("construct handler outside the repository: %v", err)
	}
	path, err := platform.MaterializePlatformSpecFile()
	if err != nil {
		t.Fatal(err)
	}
	if !filepath.IsAbs(path) {
		t.Fatal("embedded registry path was relative to caller cwd")
	}
	expected, err := LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(handler.registry, expected) {
		t.Fatal("handler registry differs from the binary's embedded specification")
	}
}
