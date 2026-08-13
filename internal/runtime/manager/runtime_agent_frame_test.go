package manager

import "testing"

func TestResolveAgentFrameConfigRejectsNoncanonicalFlowInstanceBeforeLookup(t *testing.T) {
	manager := &AgentManager{}
	for _, flowInstance := range []string{"/review/one/", " review/one ", "review/one/"} {
		if _, err := manager.ResolveAgentFrameConfig("reviewer", flowInstance, false); err == nil {
			t.Fatalf("resolver accepted noncanonical flow-instance alias %q", flowInstance)
		}
	}
}
