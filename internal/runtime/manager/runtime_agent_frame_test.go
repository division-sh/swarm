package manager

import (
	"errors"
	"testing"
)

func TestResolveAgentFrameConfigRejectsNoncanonicalFlowInstanceBeforeLookup(t *testing.T) {
	manager := &AgentManager{}
	for _, flowInstance := range []string{"/review/one/", " review/one ", "review/one/"} {
		if _, err := manager.ResolveAgentFrameConfig("reviewer", flowInstance, false); err == nil {
			t.Fatalf("resolver accepted noncanonical flow-instance alias %q", flowInstance)
		}
	}
}

func TestResolveAgentFrameConfigRejectsNoncanonicalAgentIDBeforeLookup(t *testing.T) {
	manager := &AgentManager{}
	for _, agentID := range []string{" reviewer", "reviewer ", " reviewer "} {
		_, err := manager.ResolveAgentFrameConfig(agentID, "", true)
		if err == nil {
			t.Fatalf("resolver accepted noncanonical agent id %q", agentID)
		}
		if errors.Is(err, ErrAgentNotFound) {
			t.Fatalf("resolver looked up noncanonical agent id %q: %v", agentID, err)
		}
	}
}
