package serveapp

import (
	"testing"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
)

type selectedForkRuntimeProofOptions = runforkexecution.SelectedContractAgentRuntimeOptions

// Direct owner probes use the same deployed inputs as the public fork handler.
func captureServedForkRuntimeOptions(t *testing.T) *selectedForkRuntimeProofOptions {
	t.Helper()
	var options selectedForkRuntimeProofOptions
	previous := buildSelectedAPICapabilities
	buildSelectedAPICapabilities = func(owner *selectedStoreOwner, req selectedAPICapabilityRequest) (selectedAPICapabilities, error) {
		caps, err := previous(owner, req)
		if err == nil {
			if executor, ok := caps.RunFork.(apiv1.SelectedContractRunForkExecutor); ok {
				options = executor.AgentRuntime
			}
		}
		return caps, err
	}
	t.Cleanup(func() { buildSelectedAPICapabilities = previous })
	return &options
}
