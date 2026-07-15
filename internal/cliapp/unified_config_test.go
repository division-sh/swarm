package cliapp

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
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
	if got.CLI.Connection.APIServer != "http://127.0.0.1:2222" || got.Source != string(unifiedLayerExplicit) {
		t.Fatalf("connection.api_server/source = %q/%q, want explicit config", got.CLI.Connection.APIServer, got.Source)
	}
}

func TestGeneratedUnifiedConfigExampleMatchesCommittedFile(t *testing.T) {
	got, err := os.ReadFile(filepath.Join(RepoRoot(), "swarm.example.yaml"))
	if err != nil {
		t.Fatalf("read swarm.example.yaml: %v", err)
	}
	want := generatedUnifiedConfigExample()
	if string(got) != want {
		t.Fatalf("swarm.example.yaml drifted from generated config example metadata:\n%s", firstStringDiff(string(got), want))
	}
}

func firstStringDiff(got, want string) string {
	max := len(got)
	if len(want) < max {
		max = len(want)
	}
	for i := 0; i < max; i++ {
		if got[i] != want[i] {
			return fmt.Sprintf("first diff at byte %d\ngot:  %q\nwant: %q", i, got[i:unifiedConfigTestMin(i+120, len(got))], want[i:unifiedConfigTestMin(i+120, len(want))])
		}
	}
	return fmt.Sprintf("length differs: got %d want %d", len(got), len(want))
}

func unifiedConfigTestMin(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func TestGeneratedUnifiedConfigExampleValidationSampleLoads(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	path := filepath.Join(t.TempDir(), "swarm.yaml")
	writeRuntimeConfigText(t, path, generatedUnifiedConfigValidationSample())
	if _, err := loadUnifiedConfig(unifiedConfigLoadOptions{ExplicitPath: path}); err != nil {
		t.Fatalf("generated validation sample failed unified parser: %v\n%s", err, generatedUnifiedConfigValidationSample())
	}
}

func TestGeneratedUnifiedConfigExampleAdvertisedDoctorCommandWorks(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"doctor", "--target", "--json"}, &stdout, &stderr, defaultRootCommandOptions())
	if code != 0 {
		t.Fatalf("doctor --target --json code=%d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if !strings.Contains(stdout.String(), `"mode": "target"`) {
		t.Fatalf("doctor --target --json missing target mode:\n%s", stdout.String())
	}
	text := generatedUnifiedConfigExample()
	if !strings.Contains(text, "swarm doctor --target") {
		t.Fatalf("generated example missing advertised doctor command:\n%s", text)
	}
	if strings.Contains(text, "swarm doctor config") {
		t.Fatalf("generated example advertises nonexistent doctor command:\n%s", text)
	}
}

func TestGeneratedUnifiedConfigExampleHeaderFitsPublicContract(t *testing.T) {
	text := generatedUnifiedConfigExample()
	header, _, ok := strings.Cut(text, "\n\n")
	if !ok {
		t.Fatalf("generated example missing blank line after header:\n%s", text)
	}
	lines := strings.Split(strings.TrimRight(header, "\n"), "\n")
	if len(lines) > 4 {
		t.Fatalf("generated example header has %d lines, want <= 4:\n%s", len(lines), header)
	}
	for _, line := range lines {
		if !strings.HasPrefix(line, "# ") {
			t.Fatalf("generated example header line is not a comment: %q\n%s", line, header)
		}
	}
}

func TestGeneratedUnifiedConfigExampleMetadataCoversSupportedRules(t *testing.T) {
	entries := map[string]unifiedConfigExampleEntry{}
	for _, entry := range unifiedConfigExampleEntries() {
		if entry.Path == "" || entry.Value == "" || entry.Description == "" {
			t.Fatalf("incomplete example metadata: %#v", entry)
		}
		if _, exists := entries[entry.Path]; exists {
			t.Fatalf("duplicate example metadata for %q", entry.Path)
		}
		entries[entry.Path] = entry
		if entry.RequiresRuleLookup {
			rule, ok := unifiedConfigRule(strings.Split(entry.Path, "."))
			if !ok {
				t.Fatalf("example metadata path %q is not accepted by unified config rules", entry.Path)
			}
			if rule.Split != "" || rule.OldShape != "" || rule.InlineSecret {
				t.Fatalf("example metadata path %q is not a supported configurable leaf: %#v", entry.Path, rule)
			}
		}
	}
	for path, rule := range unifiedConfigRules() {
		if !rule.supportedExampleLeaf() {
			continue
		}
		if _, ok := entries[path]; !ok {
			t.Fatalf("supported unified config key %q missing generated example metadata", path)
		}
	}
}

func TestGeneratedUnifiedConfigExampleOmitsSplitUnsupportedAndPlaintextSecrets(t *testing.T) {
	text := generatedUnifiedConfigExample()
	for _, forbidden := range []string{
		"runtime.max_concurrent_agents",
		"runtime.event_poll_interval",
		"llm.runtime_mode",
		"llm.claude_api.default_model",
		"llm.claude_api.haiku_model",
		"llm.claude_cli.retries",
		"llm.claude_cli.no_session_persistence",
		"llm.claude_cli.use_tmux",
		"llm.openai_compatible.default_model",
		"llm.openai_compatible.low_cost_model",
		"claude-3-5-sonnet",
		".swarm/dev.db",
		"sharding",
		"\n#   password:",
	} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("generated example exposes unsupported or plaintext-secret key %q:\n%s", forbidden, text)
		}
	}
	for _, want := range []string{"password_secret_key", "password_file", "password_env", "never store plaintext secrets"} {
		if !strings.Contains(text, want) {
			t.Fatalf("generated example missing secret-reference guidance %q:\n%s", want, text)
		}
	}
}

func TestGeneratedUnifiedConfigExampleUsesPublicVocabulary(t *testing.T) {
	text := generatedUnifiedConfigExample()
	lower := strings.ToLower(text)
	for _, forbidden := range []string{
		"unified",
		"presence",
		"authority",
		"precedence",
		"drain",
	} {
		if strings.Contains(lower, forbidden) {
			t.Fatalf("generated example exposes internal vocabulary %q:\n%s", forbidden, text)
		}
	}
	if regexp.MustCompile(`#\d+`).MatchString(text) {
		t.Fatalf("generated example exposes issue-number implementation context:\n%s", text)
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
	if got.CLI.Serve.APIListenAddr != "127.0.0.1:3333" {
		t.Fatalf("serve api listen addr = %q, want local-operator override", got.CLI.Serve.APIListenAddr)
	}
	if got.CLI.Paths.ContractsPath != "" {
		t.Fatalf("contracts_path = %q, want explicit empty local override", got.CLI.Paths.ContractsPath)
	}

	explicitPath := filepath.Join(t.TempDir(), "explicit.yaml")
	writeRuntimeConfigText(t, explicitPath, "serve:\n  api_listen_addr: 127.0.0.1:4444\n")
	got, err = loadUnifiedConfig(unifiedConfigLoadOptions{RepoRoot: repo, ExplicitPath: explicitPath})
	if err != nil {
		t.Fatalf("load explicit unified config: %v", err)
	}
	if got.CLI.Serve.APIListenAddr != "127.0.0.1:4444" || got.Source != string(unifiedLayerExplicit) {
		t.Fatalf("explicit serve api/source = %q/%q", got.CLI.Serve.APIListenAddr, got.Source)
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
		{name: "sharding split unsupported", body: "sharding:\n  enabled: true\n", want: "config key \"sharding\" is recognized but not yet supported"},
		{name: "sharding dotted split unsupported", body: "sharding.foo: true\n", want: "config key \"sharding.foo\" is recognized but not yet supported"},
		{name: "sharding typo unknown", body: "shardingtypo:\n  enabled: true\n", want: "unknown config key \"shardingtypo\""},
		{name: "unknown budget key", body: "budget:\n  not_a_real_key: 1\n", want: "unknown config key \"budget.not_a_real_key\""},
		{name: "unknown human task budget typo", body: "budget:\n  human_tasks:\n    max_tasks_per_wek: 1\n", want: "unknown config key \"budget.human_tasks.max_tasks_per_wek\""},
		{name: "unknown provider limit profile policy typo", body: "llm:\n  provider_limits:\n    anthropic:\n      max_concurency: 2\n", want: "unknown config key \"llm.provider_limits.anthropic.max_concurency\""},
		{name: "unknown provider limit model policy typo", body: "llm:\n  provider_limits:\n    anthropic:\n      models:\n        sonnet:\n          rate_limt: 10/m\n", want: "unknown config key \"llm.provider_limits.anthropic.models.sonnet.rate_limt\""},
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

func TestUnifiedConfigAllowsProviderLimitsDynamicProfilesAndModels(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	path := filepath.Join(t.TempDir(), "swarm.yaml")
	writeRuntimeConfigText(t, path, strings.Join([]string{
		"llm:",
		"  provider_limits:",
		"    anthropic:",
		"      max_concurrency: 2",
		"      max_concurrency_max_wait: 1s",
		"      models:",
		"        gpt-4.1:",
		"          rate_limit: 30/m",
		"          rate_limit_max_wait: 1s",
	}, "\n")+"\n")

	got, err := loadUnifiedConfig(unifiedConfigLoadOptions{ExplicitPath: path})
	if err != nil {
		t.Fatalf("loadUnifiedConfig: %v", err)
	}
	if _, ok := got.Config.LLM.ProviderLimits["anthropic"].Models["gpt-4.1"]; !ok {
		t.Fatalf("provider limit model key gpt-4.1 missing: %#v", got.Config.LLM.ProviderLimits["anthropic"].Models)
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
