package runtime_test

import (
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/division-sh/swarm/internal/providertriggers"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
)

func testProviderTriggerRegistry(t *testing.T) *providertriggers.Registry {
	t.Helper()
	root := filepath.Join("..", "..", "packs", "provider-triggers")
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("read provider trigger pack root: %v", err)
	}
	dirs := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			dirs = append(dirs, filepath.Join(root, entry.Name()))
		}
	}
	sort.Strings(dirs)
	registry, _, err := providertriggers.NewRegistryFromPackDirs("0.7.0", dirs, nil)
	if err != nil {
		t.Fatalf("load provider trigger registry: %v", err)
	}
	return registry
}

func newTestInboundGateway(t *testing.T, bus *runtimebus.EventBus, logger *runtimepkg.RuntimeLogger, shutdownAdmissionClosed func() bool, stores ...runtimepkg.InboundPersistence) *runtimepkg.InboundGateway {
	t.Helper()
	return runtimepkg.NewInboundGatewayWithProviderRegistry(bus, logger, shutdownAdmissionClosed, testProviderTriggerRegistry(t), stores...)
}
