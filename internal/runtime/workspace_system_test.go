package runtime

import (
	"context"
	"errors"
	"strings"
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

func TestDockerWorkspaceManager_KillOrphanProcesses_ExecutesInRunningSystemContainers(t *testing.T) {
	m := NewDockerWorkspaceManager(nil)
	m.cfg.FactoryContainer = "factoryC"
	m.cfg.InfraContainer = "infraC"

	state := map[string]bool{
		"factoryC": true,
		"infraC":   false,
	}
	execTargets := make([]string, 0, 2)

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
		case "exec":
			if len(args) < 5 {
				t.Fatalf("unexpected exec args: %v", args)
			}
			if args[2] != "sh" || args[3] != "-lc" {
				t.Fatalf("unexpected exec command shape: %v", args)
			}
			if !strings.Contains(args[4], "pkill") {
				t.Fatalf("expected kill script, got %q", args[4])
			}
			execTargets = append(execTargets, args[1])
			return "", nil
		default:
			return "", errors.New("unexpected docker cmd: " + strings.Join(args, " "))
		}
	}

	if err := m.KillOrphanProcesses(context.Background()); err != nil {
		t.Fatalf("KillOrphanProcesses: %v", err)
	}
	if len(execTargets) != 1 || execTargets[0] != "factoryC" {
		t.Fatalf("expected only running factory container kill exec, got %v", execTargets)
	}
}

func TestDockerWorkspaceManager_KillOrphanProcesses_PropagatesExecFailure(t *testing.T) {
	m := NewDockerWorkspaceManager(nil)
	m.cfg.FactoryContainer = "factoryC"
	m.cfg.InfraContainer = ""

	m.runDockerFn = func(_ context.Context, args ...string) (string, error) {
		switch args[0] {
		case "inspect":
			return "true", nil
		case "exec":
			return "", errors.New("boom")
		default:
			return "", nil
		}
	}

	err := m.KillOrphanProcesses(context.Background())
	if err == nil || !strings.Contains(err.Error(), "kill workspace orphans in factoryC") {
		t.Fatalf("expected exec failure propagated, got %v", err)
	}
}
