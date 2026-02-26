package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"

	"empireai/internal/models"
)

func TestDockerWorkspaceManager_EnsureContainerRunning_CreateStartStopInspect(t *testing.T) {
	m := NewDockerWorkspaceManager(nil)
	state := map[string]bool{} // name -> running
	var calls [][]string

	m.runDockerFn = func(_ context.Context, args ...string) (string, error) {
		calls = append(calls, append([]string(nil), args...))
		if len(args) == 0 {
			return "", errors.New("missing args")
		}
		switch args[0] {
		case "inspect":
			name := args[len(args)-1]
			running, ok := state[name]
			if !ok {
				return "", errors.New("Error: No such object: " + name)
			}
			if running {
				return "true", nil
			}
			return "false", nil
		case "create":
			// create --name <name> ...
			for i := 0; i+1 < len(args); i++ {
				if args[i] == "--name" {
					state[args[i+1]] = false
					break
				}
			}
			return "created", nil
		case "start":
			name := args[1]
			state[name] = true
			return "started", nil
		case "stop":
			name := args[1]
			state[name] = false
			return "stopped", nil
		case "network":
			return "connected", nil
		default:
			return "", errors.New("unexpected docker cmd: " + strings.Join(args, " "))
		}
	}

	// Not exists -> create then start.
	if err := m.ensureContainerRunning(context.Background(), "c1", []string{"img", "sleep", "infinity"}); err != nil {
		t.Fatalf("ensure c1: %v", err)
	}
	if !state["c1"] {
		t.Fatal("expected c1 running")
	}

	// Exists+running -> no start.
	before := len(calls)
	if err := m.ensureContainerRunning(context.Background(), "c1", []string{"img"}); err != nil {
		t.Fatalf("ensure c1 again: %v", err)
	}
	if len(calls) != before+2 { // inspect + network connect
		t.Fatalf("expected inspect + network connect calls, got %d new calls", len(calls)-before)
	}

	// Stop running -> calls stop.
	if err := m.stopContainer(context.Background(), "c1"); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if state["c1"] {
		t.Fatal("expected c1 stopped")
	}

	// Stop already stopped/no-such -> no error.
	if err := m.stopContainer(context.Background(), "missing"); err != nil {
		t.Fatalf("stop missing: %v", err)
	}
}

func TestDockerWorkspaceManager_ResolveWorkspace_RolesAndVertical(t *testing.T) {
	m := NewDockerWorkspaceManager(nil)
	m.cfg.FactoryContainer = "factoryC"
	m.cfg.FactoryWorkdir = "/factory"
	m.cfg.InfraContainer = "infraC"
	m.cfg.InfraWorkdir = "/infra"
	m.cfg.WorkspaceImage = "img"
	m.cfg.VerticalContainerPrefix = "v-"
	m.cfg.VerticalWorkdir = "/v"

	state := map[string]bool{}
	m.runDockerFn = func(_ context.Context, args ...string) (string, error) {
		switch args[0] {
		case "inspect":
			name := args[len(args)-1]
			if _, ok := state[name]; !ok {
				return "", errors.New("no such object")
			}
			if state[name] {
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
		case "network":
			return "connected", nil
		default:
			return "ok", nil
		}
	}

	// factory-cto
	tgt, err := m.ResolveWorkspace(context.Background(), models.AgentConfig{Role: "factory-cto"})
	if err != nil {
		t.Fatalf("resolve factory: %v", err)
	}
	if tgt.Container != "factoryC" || tgt.Workdir != "/factory" {
		t.Fatalf("unexpected factory target: %+v", tgt)
	}

	// holding-devops
	tgt, err = m.ResolveWorkspace(context.Background(), models.AgentConfig{Role: "holding-devops"})
	if err != nil {
		t.Fatalf("resolve infra: %v", err)
	}
	if tgt.Container != "infraC" || tgt.Workdir != "/infra" {
		t.Fatalf("unexpected infra target: %+v", tgt)
	}

	// vertical actor (db nil => slug sanitized from verticalID)
	tgt, err = m.ResolveWorkspace(context.Background(), models.AgentConfig{Role: "pm-agent", VerticalID: " Pet Grooming_2026 "})
	if err != nil {
		t.Fatalf("resolve vertical: %v", err)
	}
	if tgt.Container != "v-pet-grooming-2026" || tgt.Workdir != "/v" {
		t.Fatalf("unexpected vertical target: %+v", tgt)
	}
}
