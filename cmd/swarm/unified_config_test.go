package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestUnifiedConfigExplicitPathBeatsSWARMCONFIGLocator(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	envPath := filepath.Join(t.TempDir(), "env.yaml")
	writeRuntimeConfigText(t, envPath, "connection:\n  api_server: http://127.0.0.1:1111\n")
	explicitPath := filepath.Join(t.TempDir(), "explicit.yaml")
	writeRuntimeConfigText(t, explicitPath, "connection:\n  api_server: http://127.0.0.1:2222\n")
	t.Setenv("SWARM_CONFIG", envPath)

	got, err := loadUnifiedConfig(unifiedConfigLoadOptions{ExplicitPath: explicitPath})
	if err != nil {
		t.Fatalf("loadUnifiedConfig: %v", err)
	}
	if got.CLI.APIServer != "http://127.0.0.1:2222" || got.Source != string(unifiedLayerExplicit) {
		t.Fatalf("api_server/source = %q/%q, want explicit config", got.CLI.APIServer, got.Source)
	}
}

func TestUnifiedConfigLayerOrderAndExplicitEmptyOverride(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	repo := t.TempDir()
	userPath := userGlobalUnifiedConfigPath()
	if err := os.MkdirAll(filepath.Dir(userPath), 0o755); err != nil {
		t.Fatalf("mkdir user config: %v", err)
	}
	writeRuntimeConfigText(t, userPath, strings.Join([]string{
		"serve:",
		"  api_listen_addr: 127.0.0.1:1111",
		"paths:",
		"  contracts_path: user-contracts",
	}, "\n")+"\n")
	writeRuntimeConfigText(t, filepath.Join(repo, "swarm.yaml"), strings.Join([]string{
		"serve:",
		"  api_listen_addr: 127.0.0.1:2222",
		"paths:",
		"  contracts_path: project-contracts",
	}, "\n")+"\n")
	localDir := filepath.Join(repo, ".swarm")
	if err := os.MkdirAll(localDir, 0o755); err != nil {
		t.Fatalf("mkdir local config: %v", err)
	}
	writeRuntimeConfigText(t, filepath.Join(localDir, "swarm.yaml"), strings.Join([]string{
		"serve:",
		"  api_listen_addr: 127.0.0.1:3333",
		"paths:",
		"  contracts_path: \"\"",
	}, "\n")+"\n")

	got, err := loadUnifiedConfig(unifiedConfigLoadOptions{RepoRoot: repo})
	if err != nil {
		t.Fatalf("loadUnifiedConfig: %v", err)
	}
	if got.CLI.ServeAPIListenAddr != "127.0.0.1:3333" {
		t.Fatalf("serve api listen addr = %q, want local-operator override", got.CLI.ServeAPIListenAddr)
	}
	if got.CLI.ContractsPath != "" {
		t.Fatalf("contracts_path = %q, want explicit empty local override", got.CLI.ContractsPath)
	}

	explicitPath := filepath.Join(t.TempDir(), "explicit.yaml")
	writeRuntimeConfigText(t, explicitPath, "serve:\n  api_listen_addr: 127.0.0.1:4444\n")
	got, err = loadUnifiedConfig(unifiedConfigLoadOptions{RepoRoot: repo, ExplicitPath: explicitPath})
	if err != nil {
		t.Fatalf("load explicit unified config: %v", err)
	}
	if got.CLI.ServeAPIListenAddr != "127.0.0.1:4444" || got.Source != string(unifiedLayerExplicit) {
		t.Fatalf("explicit serve api/source = %q/%q", got.CLI.ServeAPIListenAddr, got.Source)
	}
}

func TestUnifiedConfigRejectsLegacyFlatShapeAndSplitUnsupported(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	for _, tt := range []struct {
		name string
		body string
		want string
	}{
		{name: "old flat", body: "api_server: http://127.0.0.1:8081\n", want: "old flat config key \"api_server\""},
		{name: "split unsupported", body: "runtime:\n  max_concurrent_agents: 4\n", want: "recognized but not yet supported"},
		{name: "claude cli retries split unsupported", body: "llm:\n  claude_cli:\n    retries: 2\n", want: "llm.claude_cli.retries"},
		{name: "claude cli no session persistence split unsupported", body: "llm:\n  claude_cli:\n    no_session_persistence: true\n", want: "llm.claude_cli.no_session_persistence"},
		{name: "claude cli tmux split unsupported", body: "llm:\n  claude_cli:\n    use_tmux: true\n", want: "llm.claude_cli.use_tmux"},
		{name: "unknown budget key", body: "budget:\n  not_a_real_key: 1\n", want: "unknown config key \"budget.not_a_real_key\""},
		{name: "unknown human task budget typo", body: "budget:\n  human_tasks:\n    max_tasks_per_wek: 1\n", want: "unknown config key \"budget.human_tasks.max_tasks_per_wek\""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "swarm.yaml")
			writeRuntimeConfigText(t, path, tt.body)
			_, err := loadUnifiedConfig(unifiedConfigLoadOptions{ExplicitPath: path})
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("loadUnifiedConfig error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestUnifiedConfigRejectsProjectTrustAndPathEscapes(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	t.Run("connection key in project config", func(t *testing.T) {
		repo := t.TempDir()
		writeRuntimeConfigText(t, filepath.Join(repo, "swarm.yaml"), "connection:\n  api_server: http://127.0.0.1:8081\n")
		_, err := loadUnifiedConfig(unifiedConfigLoadOptions{RepoRoot: repo})
		if err == nil || !strings.Contains(err.Error(), "not allowed in project_config") {
			t.Fatalf("loadUnifiedConfig error = %v, want project trust rejection", err)
		}
	})

	t.Run("project-contained symlink escape", func(t *testing.T) {
		repo := t.TempDir()
		outside := t.TempDir()
		if err := os.Symlink(outside, filepath.Join(repo, "contracts-link")); err != nil {
			t.Skipf("symlink unavailable: %v", err)
		}
		writeRuntimeConfigText(t, filepath.Join(repo, "swarm.yaml"), "paths:\n  contracts_path: contracts-link\n")
		_, err := loadUnifiedConfig(unifiedConfigLoadOptions{RepoRoot: repo})
		if err == nil || !strings.Contains(err.Error(), "path escapes project root") {
			t.Fatalf("loadUnifiedConfig error = %v, want project path containment rejection", err)
		}
	})
}

func TestUnifiedConfigRejectsExecutableAdjacentConfigYAML(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	exeDir := t.TempDir()
	writeRuntimeConfigText(t, filepath.Join(exeDir, "config.yaml"), "runtime:\n  recovery_on_startup: true\n")
	originalExecutablePath := runtimeConfigExecutablePath
	runtimeConfigExecutablePath = func() (string, error) {
		return filepath.Join(exeDir, "swarm"), nil
	}
	t.Cleanup(func() { runtimeConfigExecutablePath = originalExecutablePath })

	_, err := loadUnifiedConfig(unifiedConfigLoadOptions{RepoRoot: t.TempDir()})
	if err == nil || !strings.Contains(err.Error(), "executable-adjacent runtime config") || !strings.Contains(err.Error(), "no longer a config source") {
		t.Fatalf("loadUnifiedConfig error = %v, want executable-adjacent legacy diagnostic", err)
	}
}
