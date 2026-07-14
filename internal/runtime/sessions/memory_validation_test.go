package sessions

import (
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
)

func TestValidateAgentMemoryConfig(t *testing.T) {
	if err := agentmemory.ValidateFlowOwnership(agentmemory.PlatformDefault(), ""); err != nil {
		t.Fatalf("root stateless config: %v", err)
	}
	if err := agentmemory.ValidateFlowOwnership(agentmemory.Authored(true), ""); err == nil || !strings.Contains(err.Error(), "flow-instance owner") {
		t.Fatalf("root remembered error = %v", err)
	}
	if err := agentmemory.ValidateFlowOwnership(agentmemory.Authored(true), "support/chat-a"); err != nil {
		t.Fatalf("flow memory config: %v", err)
	}
}

func TestLeaseHeartbeatIntervalBounds(t *testing.T) {
	if got := LeaseHeartbeatInterval(time.Now().Add(time.Second)); got != minLeaseHeartbeatInterval {
		t.Fatalf("short interval = %v", got)
	}
	if got := LeaseHeartbeatInterval(time.Now().Add(10 * time.Minute)); got != maxLeaseHeartbeatInterval {
		t.Fatalf("long interval = %v", got)
	}
}
