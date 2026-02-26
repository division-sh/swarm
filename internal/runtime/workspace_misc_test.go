package runtime

import (
	"os"
	"testing"
)

func TestWorkspace_DefaultDockerWorkspaceConfig_EnvOverrides(t *testing.T) {
	old := os.Getenv("EMPIREAI_VERTICAL_CONTAINER_PREFIX")
	t.Cleanup(func() { _ = os.Setenv("EMPIREAI_VERTICAL_CONTAINER_PREFIX", old) })
	_ = os.Setenv("EMPIREAI_VERTICAL_CONTAINER_PREFIX", "x-")

	cfg := defaultDockerWorkspaceConfig()
	if cfg.VerticalContainerPrefix != "x-" {
		t.Fatalf("expected env override for prefix, got %q", cfg.VerticalContainerPrefix)
	}

	m := NewDockerWorkspaceManager(nil)
	m.cfg = cfg
	if got := m.verticalContainerName(" Pet Grooming_2026 "); got != "x-pet-grooming-2026" {
		t.Fatalf("unexpected container name: %s", got)
	}

	if envOrDefault("EMPIREAI_VERTICAL_CONTAINER_PREFIX", "fallback") != "x-" {
		t.Fatal("expected envOrDefault to return env value")
	}
	_ = os.Setenv("EMPIREAI_VERTICAL_CONTAINER_PREFIX", "")
	if envOrDefault("EMPIREAI_VERTICAL_CONTAINER_PREFIX", "fallback") != "fallback" {
		t.Fatal("expected envOrDefault to return fallback when empty")
	}
}

