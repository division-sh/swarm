package tools

import (
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
)

func TestNativeToolAdmissionPreservesTurnBoundWithoutEmitCapability(t *testing.T) {
	timeout := timeridentity.TurnTimeout{After: 10 * time.Minute, Emit: "work.aborted"}
	entry := runtimecontracts.AgentRegistryEntry{TurnTimeout: &timeout}
	cfg := nativeToolAgentConfig("worker", "worker", entry)
	if cfg.TurnTimeout == nil || *cfg.TurnTimeout != timeout || cfg.TurnTimeout == entry.TurnTimeout || len(cfg.EmitEvents) != 0 {
		t.Fatalf("native-tool projection changed bound or emission authority: %+v", cfg)
	}
}
