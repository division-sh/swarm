package runtime

import (
	"context"
	"errors"
	"testing"
)

func TestDockerWorkspaceManager_EnsureSystemAndStopVerticalWorkspace(t *testing.T) {
	m := NewDockerWorkspaceManager(nil)
	m.cfg.FactoryContainer = "factoryC"
	m.cfg.InfraContainer = "infraC"
	m.cfg.WorkspaceImage = "img"
	m.cfg.FactoryWorkdir = "/f"
	m.cfg.InfraWorkdir = "/i"
	m.cfg.VerticalContainerPrefix = "v-"

	state := map[string]bool{}
	m.runDockerFn = func(_ context.Context, args ...string) (string, error) {
		switch args[0] {
		case "inspect":
			name := args[len(args)-1]
			running, ok := state[name]
			if !ok {
				return "", errors.New("no such object")
			}
			if running {
				return "true", nil
			}
			return "false", nil
		case "create":
			for i := 0; i+1 < len(args); i++ {
				if args[i] == "--name" {
					state[args[i+1]] = false
					break
				}
			}
			return "created", nil
		case "start":
			state[args[1]] = true
			return "started", nil
		case "stop":
			state[args[1]] = false
			return "stopped", nil
		default:
			return "ok", nil
		}
	}

	if err := m.EnsureSystemWorkspaces(context.Background()); err != nil {
		t.Fatalf("EnsureSystemWorkspaces: %v", err)
	}
	if !state["factoryC"] || !state["infraC"] {
		t.Fatalf("expected system containers running: %+v", state)
	}

	// StopVerticalWorkspace (db nil -> slug from verticalID).
	if err := m.EnsureVerticalWorkspace(context.Background(), " Pet Grooming_2026 "); err != nil {
		t.Fatalf("EnsureVerticalWorkspace: %v", err)
	}
	if !state["v-pet-grooming-2026"] {
		t.Fatal("expected vertical container running")
	}
	if err := m.StopVerticalWorkspace(context.Background(), " Pet Grooming_2026 "); err != nil {
		t.Fatalf("StopVerticalWorkspace: %v", err)
	}
	if state["v-pet-grooming-2026"] {
		t.Fatal("expected vertical container stopped")
	}
}

