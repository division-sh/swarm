package manager

import (
	"os"
	"strings"
	"testing"
)

func TestAgentManagerExecutionProjectionHasNoLegacyExecutionAuthority(t *testing.T) {
	for _, path := range []string{"agent_manager.go", "runtime.go", "receipts.go", "flow_activation.go"} {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		source := string(raw)
		for _, forbidden := range []string{
			"am.agents", "am.agentCfg", "am.agentUpAt",
			"am.bus.Subscribe(", "am.bus.Unsubscribe(",
			"effectContext(", "startAgentLoopTransition", "activateAgentLoopTransition",
		} {
			if strings.Contains(source, forbidden) {
				t.Fatalf("%s contains retired execution authority %q", path, forbidden)
			}
		}
	}
}
