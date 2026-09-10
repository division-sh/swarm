package cliapp

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCommandConfigRejectsMaskedExecutionSelectors(t *testing.T) {
	for _, selector := range []struct{ key, source string }{
		{"runtime.execution_posture", "runtime:\n  execution_posture: mock_only\n"},
		{"llm.backend", "llm:\n  backend: mock\n"},
	} {
		for _, layer := range []string{"user", "project", "local", "explicit"} {
			t.Run(selector.key+"/"+layer, func(t *testing.T) {
				isolateCLIAPIConfigEnv(t)
				root := t.TempDir()
				paths := map[string]string{
					"user":     userGlobalUnifiedConfigPath(),
					"project":  filepath.Join(root, "swarm.yaml"),
					"local":    filepath.Join(root, ".swarm", "swarm.yaml"),
					"explicit": filepath.Join(t.TempDir(), "explicit.yaml"),
				}
				for _, path := range paths {
					if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(path, []byte("llm:\n  backend: claude_cli\nruntime:\n  recovery_on_startup: false\n"), 0o600); err != nil {
						t.Fatal(err)
					}
				}
				if err := os.WriteFile(paths[layer], []byte(selector.source), 0o600); err != nil {
					t.Fatal(err)
				}
				_, err := loadUnifiedConfigForTest(t, unifiedConfigLoadOptions{RepoRoot: root, ExplicitPath: paths["explicit"]})
				if err == nil || !strings.Contains(err.Error(), selector.key) || !strings.Contains(err.Error(), paths[layer]) || !strings.Contains(err.Error(), "retired") {
					t.Fatalf("masked %s selector = %v, want originating file/key refusal", layer, err)
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
	writeRuntimeConfigText(t, filepath.Join(root, "swarm.yaml"), "runtime:\n  recovery_on_startup: false\n")
	loaded, err = loadUnifiedConfigForTest(t, unifiedConfigLoadOptions{RepoRoot: root})
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Config.Runtime.RecoveryOnStartup {
		t.Fatal("explicit false lost during merge")
	}
}
