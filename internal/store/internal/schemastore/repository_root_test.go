package schemastore

import (
	"path/filepath"
	"runtime"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/yamlsource"
)

func repoRootForRuntimeWriterGuard(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve schema store test path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..", ".."))
}

func loadPlatformSpecDocumentForStoreTest(t testing.TB, path string) runtimecontracts.PlatformSpecDocument {
	t.Helper()
	source, err := yamlsource.LoadFile(path)
	if err != nil {
		t.Fatalf("read platform spec: %v", err)
	}
	var spec runtimecontracts.PlatformSpecDocument
	if err := source.Decode(&spec); err != nil {
		t.Fatalf("unmarshal platform spec: %v", err)
	}
	return spec
}
