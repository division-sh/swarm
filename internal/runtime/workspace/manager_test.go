package workspace

import (
	"context"
	"empireai/internal/models"
	"errors"
	"os"
	"strings"
	"testing"
)

func TestSanitizeWorkspaceSlug(t *testing.T) {
	got := SanitizeSlug(" Pet Grooming_2026 ")
	if got != "pet-grooming-2026" {
		t.Fatalf("unexpected slug: %s", got)
	}
}

func TestWorkspace_DefaultDockerWorkspaceConfig_EnvOverrides(t *testing.T) {
	old := os.Getenv("EMPIREAI_VERTICAL_CONTAINER_PREFIX")
	t.Cleanup(func() { _ = os.Setenv("EMPIREAI_VERTICAL_CONTAINER_PREFIX", old) })
	_ = os.Setenv("EMPIREAI_VERTICAL_CONTAINER_PREFIX", "x-")

	cfg := DefaultDockerConfig()
	if cfg.VerticalContainerPrefix != "x-" {
		t.Fatalf("expected env override for prefix, got %q", cfg.VerticalContainerPrefix)
	}

	m := NewDockerManager(nil)
	m.cfg = cfg
	if got := m.VerticalContainerName(" Pet Grooming_2026 "); got != "x-pet-grooming-2026" {
		t.Fatalf("unexpected container name: %s", got)
	}

	if EnvOrDefault("EMPIREAI_VERTICAL_CONTAINER_PREFIX", "fallback") != "x-" {
		t.Fatal("expected EnvOrDefault to return env value")
	}
	_ = os.Setenv("EMPIREAI_VERTICAL_CONTAINER_PREFIX", "")
	if EnvOrDefault("EMPIREAI_VERTICAL_CONTAINER_PREFIX", "fallback") != "fallback" {
		t.Fatal("expected EnvOrDefault to return fallback when empty")
	}
}

func TestDockerManager_EnsureSystemAndStopVerticalWorkspace(t *testing.T) {
	m := NewDockerManager(nil)
	m.cfg.FactoryContainer = "factoryC"
	m.cfg.InfraContainer = "infraC"
	m.cfg.WorkspaceImage = "img"
	m.cfg.FactoryWorkdir = "/f"
	m.cfg.InfraWorkdir = "/i"
	m.cfg.VerticalContainerPrefix = "v-"

	state := map[string]bool{}
	m.RunDockerFn = func(_ context.Context, args ...string) (string, error) {
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

func TestDockerManager_KillOrphanProcesses_ExecutesInRunningSystemContainers(t *testing.T) {
	m := NewDockerManager(nil)
	m.cfg.FactoryContainer = "factoryC"
	m.cfg.InfraContainer = "infraC"

	state := map[string]bool{
		"factoryC": true,
		"infraC":   false,
	}
	execTargets := make([]string, 0, 2)

	m.RunDockerFn = func(_ context.Context, args ...string) (string, error) {
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

func TestDockerManager_KillOrphanProcesses_PropagatesExecFailure(t *testing.T) {
	m := NewDockerManager(nil)
	m.cfg.FactoryContainer = "factoryC"
	m.cfg.InfraContainer = ""

	m.RunDockerFn = func(_ context.Context, args ...string) (string, error) {
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
func TestDockerManager_EnsureContainerRunning_CreateStartStopInspect(t *testing.T) {
	m := NewDockerManager(nil)
	state := map[string]bool{} // name -> running
	var calls [][]string

	m.RunDockerFn = func(_ context.Context, args ...string) (string, error) {
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
	if err := m.EnsureContainerRunning(context.Background(), "c1", []string{"img", "sleep", "infinity"}); err != nil {
		t.Fatalf("ensure c1: %v", err)
	}
	if !state["c1"] {
		t.Fatal("expected c1 running")
	}

	// Exists+running -> no start.
	before := len(calls)
	if err := m.EnsureContainerRunning(context.Background(), "c1", []string{"img"}); err != nil {
		t.Fatalf("ensure c1 again: %v", err)
	}
	if len(calls) != before+2 { // inspect + network connect
		t.Fatalf("expected inspect + network connect calls, got %d new calls", len(calls)-before)
	}

	// Stop running -> calls stop.
	if err := m.StopContainer(context.Background(), "c1"); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if state["c1"] {
		t.Fatal("expected c1 stopped")
	}

	// Stop already stopped/no-such -> no error.
	if err := m.StopContainer(context.Background(), "missing"); err != nil {
		t.Fatalf("stop missing: %v", err)
	}
}

func TestDockerManager_ResolveWorkspace_RolesAndVertical(t *testing.T) {
	m := NewDockerManager(nil)
	m.cfg.FactoryContainer = "factoryC"
	m.cfg.FactoryWorkdir = "/factory"
	m.cfg.InfraContainer = "infraC"
	m.cfg.InfraWorkdir = "/infra"
	m.cfg.WorkspaceImage = "img"
	m.cfg.VerticalContainerPrefix = "v-"
	m.cfg.VerticalWorkdir = "/v"

	state := map[string]bool{}
	m.RunDockerFn = func(_ context.Context, args ...string) (string, error) {
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
