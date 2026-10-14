package cliapp

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCommandConfigRejectsSelectedExecutionSelectors(t *testing.T) {
	for _, selector := range []struct{ key, source string }{
		{"runtime.execution_posture", "runtime:\n  execution_posture: mock_only\n"},
		{"llm.backend", "llm:\n  backend: mock\n"},
	} {
		for _, selection := range []string{"user", "project", "deployment", "locator", "explicit"} {
			t.Run(selector.key+"/"+selection, func(t *testing.T) {
				isolateCLIAPIConfigEnv(t)
				root := t.TempDir()
				paths := map[string]string{
					"user":       ignoredUserGlobalConfigPath(t),
					"project":    filepath.Join(root, "swarm.yaml"),
					"deployment": filepath.Join(root, ".swarm", "swarm.yaml"),
					"locator":    filepath.Join(t.TempDir(), "locator.yaml"),
					"explicit":   filepath.Join(t.TempDir(), "explicit.yaml"),
				}
				for _, path := range paths {
					if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(path, []byte("llm:\n  backend: claude_cli\nruntime:\n  recovery_on_startup: false\n"), 0o600); err != nil {
						t.Fatal(err)
					}
				}
				if err := os.WriteFile(paths[selection], []byte(selector.source), 0o600); err != nil {
					t.Fatal(err)
				}
				opts := unifiedConfigLoadOptions{RepoRoot: root}
				if selection == "explicit" || selection == "user" || selection == "project" {
					opts.ExplicitPath = paths["explicit"]
				} else if selection == "locator" {
					t.Setenv("SWARM_CONFIG", paths["locator"])
				}
				loaded, err := loadUnifiedConfigForTest(t, opts)
				if selection == "user" || selection == "project" {
					if err != nil || loaded.Config.LLM.Backend != "claude_cli" || loaded.Config.Runtime.RecoveryOnStartup {
						t.Fatalf("ignored selector affected explicit source: %v", err)
					}
					return
				}
				if err == nil || !strings.Contains(err.Error(), selector.key) || !strings.Contains(err.Error(), paths[selection]) || !strings.Contains(err.Error(), "unsupported") && !strings.Contains(err.Error(), "unknown config key") {
					t.Fatalf("selected %s selector = %v, want originating file/key refusal", selection, err)
				}
			})
		}
	}
}

func TestCommandConfigRecoveryDefaultsAndExplicitFalse(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	root := t.TempDir()
	loaded, err := loadUnifiedConfigForTest(t, unifiedConfigLoadOptions{RepoRoot: root})
	if err != nil {
		t.Fatal(err)
	}
	if !loaded.Config.Runtime.RecoveryOnStartup {
		t.Fatal("retained recovery did not default true")
	}
	writeSingleDeploymentConfig(t, filepath.Join(root, ".swarm", "swarm.yaml"), "runtime:\n  recovery_on_startup: false\n")
	loaded, err = loadUnifiedConfigForTest(t, unifiedConfigLoadOptions{RepoRoot: root})
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Config.Runtime.RecoveryOnStartup {
		t.Fatal("selected explicit false lost")
	}
}
