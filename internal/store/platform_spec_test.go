package store

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/yamlsource"
)

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
