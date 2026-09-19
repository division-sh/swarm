package cliapp

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestCommandConfigFanOutWorkers(t *testing.T) {
	for _, value := range []string{"", "1", "4", "64", "0", "-1", "null", "4.0", "'4'"} {
		t.Run(value, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			root := t.TempDir()
			if value != "" {
				writeRuntimeConfigText(t, filepath.Join(root, "swarm.yaml"), "runtime:\n  fan_out_workers: "+value+"\n")
			}
			loaded, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: root})
			if value == "0" || value == "-1" || value == "null" || value == "4.0" || value == "'4'" {
				if err == nil || !strings.Contains(err.Error(), "runtime.fan_out_workers") {
					t.Fatalf("load = %v, want worker declaration refusal", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			workers := loaded.Config.Runtime.FanOutWorkers
			if value == "" {
				if workers != nil {
					t.Fatal("omitted workers did not retain backend-default selection")
				}
				return
			}
			want := map[string]int{"1": 1, "4": 4, "64": 64}[value]
			if workers == nil || *workers != want {
				t.Fatalf("workers = %v, want %d", workers, want)
			}
		})
	}
}

func TestCommandConfigFanOutWorkersLayerOverride(t *testing.T) {
	for _, override := range []string{"", "1", "0", "null"} {
		t.Run(override, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			root := t.TempDir()
			writeRuntimeConfigText(t, filepath.Join(root, "swarm.yaml"), "runtime:\n  fan_out_workers: 4\n")
			source := "runtime:\n  recovery_on_startup: false\n"
			if override != "" {
				source += "  fan_out_workers: " + override + "\n"
			}
			path := filepath.Join(t.TempDir(), "operator.yaml")
			writeRuntimeConfigText(t, path, source)
			loaded, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: root, ExplicitPath: path})
			if override == "0" || override == "null" {
				if err == nil || !strings.Contains(err.Error(), "runtime.fan_out_workers") {
					t.Fatalf("load = %v, want explicit invalid override refusal", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			want := 4
			if override == "1" {
				want = 1
			}
			if workers := loaded.Config.Runtime.FanOutWorkers; workers == nil || *workers != want {
				t.Fatalf("workers = %v, want %d", workers, want)
			}
			if loaded.Config.Runtime.RecoveryOnStartup {
				t.Fatal("worker config changed existing explicit-false semantics")
			}
		})
	}
}
