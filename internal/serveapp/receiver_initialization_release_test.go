package serveapp

import (
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

// Unlike the in-process served proofs, this exercises the actual CLI verify
// and serve commands, host workspace, and HTTP boundary in a separate process.
func TestReleaseReceiverInitializationBothStores(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "swarm")
	build := exec.Command("go", "build", "-race", "-o", binary, "./cmd/swarm")
	build.Dir = repoRootForTest()
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build race-instrumented release executable: %v\n%s", err, output)
	}
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			t.Run("connected_typed_creation", func(t *testing.T) {
				root := canonicalrouting.CopyServedReceiverInitialization(t)
				rt := startLifecycleReleaseProcess(t, binary, backend, root)
				requireTypedReceiverInitializationCases(t, rt)
			})
			t.Run("direct_provider_schema", func(t *testing.T) {
				root := canonicalrouting.CopyProviderReceiverInitialization(t)
				rt := startLifecycleReleaseProcess(t, binary, backend, root)
				requireReceiverInitializationPublicProviderIngressCases(t, rt)
			})
		})
	}
}
