package manager

import (
	"context"
	"errors"
	"strings"
	"testing"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
)

func TestAgentManagerSpawnAgentConsumesNativeToolAdmissionValidator(t *testing.T) {
	t.Parallel()

	am := NewAgentManagerWithOptions(nil, nil, AgentManagerOptions{
		NativeToolAdmissionValidator: func(context.Context, models.AgentConfig) error {
			return errors.New("native tool denied")
		},
	})

	err := am.SpawnAgent(models.AgentConfig{
		ExecutionMode: "live",
		ID:            "worker-1",
		Role:          "worker",
		NativeTools:   models.NativeToolConfig{FileIO: true},
	})
	if err == nil || !strings.Contains(err.Error(), "native tool admission failed: native tool denied") {
		t.Fatalf("SpawnAgent error = %v, want native tool admission failure", err)
	}
	if _, ok := am.GetAgentConfig("worker-1"); ok {
		t.Fatal("agent was registered despite native tool admission failure")
	}
}

func TestAgentManagerReconfigureConsumesNativeToolAdmissionValidator(t *testing.T) {
	t.Parallel()

	am := NewAgentManagerWithOptions(nil, nil, AgentManagerOptions{
		NativeToolAdmissionValidator: func(_ context.Context, cfg models.AgentConfig) error {
			if cfg.NativeTools.Any() {
				return errors.New("native tool denied")
			}
			return nil
		},
	})
	if err := am.SpawnAgent(models.AgentConfig{ExecutionMode: "live", ID: "worker-1", Role: "worker"}); err != nil {
		t.Fatalf("SpawnAgent setup: %v", err)
	}

	err := am.ReconfigureAgent("worker-1", models.AgentConfig{
		ExecutionMode: "live",
		NativeTools:   models.NativeToolConfig{FileIO: true},
	})
	if err == nil || !strings.Contains(err.Error(), "native tool admission failed: native tool denied") {
		t.Fatalf("ReconfigureAgent error = %v, want native tool admission failure", err)
	}
	cfg, ok := am.GetAgentConfig("worker-1")
	if !ok {
		t.Fatal("setup agent missing after failed reconfigure")
	}
	if cfg.NativeTools.Any() {
		t.Fatalf("agent native tools changed after denied reconfigure: %#v", cfg.NativeTools)
	}
}
