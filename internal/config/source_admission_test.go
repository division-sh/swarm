package config

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"gopkg.in/yaml.v3"
)

func TestConfigCurrentVocabularySuggestionsFollowSourcePolicy(t *testing.T) {
	for _, row := range []struct {
		source              string
		target              func() any
		excluded, supported []string
	}{
		{"runtime: {max_concurrent_agent: 2}", func() any { return &Config{} }, []string{"max_concurrent_agents", "event_poll_interval"}, []string{"fan_out_workers", "recovery_on_startup"}},
		{"shardingtypo: {}", func() any { return &Config{} }, []string{"sharding"}, []string{"runtime", "workspace", "budget"}},
		{"<<: {max_concurrent_agent: 2}", func() any { return &RuntimeConfig{} }, []string{"max_concurrent_agents", "event_poll_interval"}, []string{"fan_out_workers", "recovery_on_startup"}},
		{"llm: {claude_cli: {retrie: 2}}", func() any { return &Config{} }, []string{"retries", "no_session_persistence", "use_tmux"}, []string{"command", "timeout"}},
		{"database: {<<: {passwor: secret}}", func() any { return &Config{} }, []string{"password"}, []string{"password_env", "password_file"}},
	} {
		t.Run(row.source, func(t *testing.T) {
			err := yaml.Unmarshal([]byte(row.source), row.target())
			diagnostic, ok := runtimecontracts.AsLoaderDiagnostic(err)
			if !ok {
				t.Fatalf("missing typed diagnostic: %v", err)
			}
			for _, field := range row.excluded {
				if slices.Contains(diagnostic.ValidOptions, field) {
					t.Fatalf("unsupported field advertised: %v", err)
				}
			}
			for _, field := range row.supported {
				if !slices.Contains(diagnostic.ValidOptions, field) {
					t.Fatalf("supported field missing: %v", err)
				}
			}
			if diagnostic.Location.Line == 0 || diagnostic.Location.Column == 0 {
				t.Fatalf("missing source location: %v", err)
			}
		})
	}
}

func TestCurrentConfigVocabularyRejectsUnknownAndMergedKeys(t *testing.T) {
	for _, row := range []struct{ source, key string }{
		{"totally_unknown: true", "totally_unknown"},
		{"runtime: {execution_posture: null}", "execution_posture"},
		{"runtime: {<<: {totally_unknown: true}}", "totally_unknown"},
		{"workspace: {data_source: /data}", "data_source"},
		{"workspace: {<<: {data_source: /data}}", "data_source"},
		{"workspace: {<<: [{volumes_from: container}]}", "volumes_from"},
		{"workspace: &workspace {volumes_from: ''}", "volumes_from"},
		{"workspace: {image: alpine, imag: another}", "imag"},
		{"database: {<<: {totally_unknown: true}}", "totally_unknown"},
		{"llm: {session: {unknown: false}}", "unknown"},
		{"llm: {provider_limits: {anthropic: {models: {fast: {unknown: 1}}}}}", "unknown"},
		{"connection: {api_serve: url}", "api_serve"},
	} {
		t.Run(row.source, func(t *testing.T) {
			for _, known := range []bool{false, true} {
				decoder := yaml.NewDecoder(strings.NewReader(row.source))
				decoder.KnownFields(known)
				var cfg Config
				err := decoder.Decode(&cfg)
				diagnostic, ok := runtimecontracts.AsLoaderDiagnostic(err)
				if !ok || !strings.Contains(diagnostic.Problem, row.key) || len(diagnostic.ValidOptions) == 0 || diagnostic.Location.Line == 0 || diagnostic.Location.Column == 0 {
					t.Fatalf("KnownFields(%v): error=%v diagnostic=%#v", known, err, diagnostic)
				}
				if !strings.Contains(err.Error(), "Valid fields:") || strings.Contains(err.Error(), "RETIRED") {
					t.Fatalf("human teaching: %v", err)
				}
			}
		})
	}
}

func TestConfigCustomReadersAreStrictWithoutOuterDecoder(t *testing.T) {
	for _, target := range []any{&RuntimeConfig{}, &WorkspaceConfig{}} {
		for _, source := range []string{"unknown: null", "<<: {unknown: true}"} {
			if err := yaml.Unmarshal([]byte(source), target); err == nil || !strings.Contains(err.Error(), "unknown") || !strings.Contains(err.Error(), "Valid fields:") {
				t.Fatalf("%T %q: %v", target, source, err)
			}
		}
	}
}

func TestConfigSourcePreservesSupportedAliasesPresenceAndOpenMaps(t *testing.T) {
	var cfg Config
	source := "runtime: {<<: {fan_out_workers: 2, recovery_on_startup: false}}\nworkspace: {<<: {image: '', allow_exec_on_host: false}, backend: host}\nllm: {models: {business_model: {provider: model}}, provider_limits: {provider: {models: {model: {max_concurrency: 2}}}}}\nconnection: {api_server: localhost}\nbudget: {human_tasks: {categories_enabled: [support]}}\n"
	if err := yaml.Unmarshal([]byte(source), &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Runtime.FanOutWorkers == nil || *cfg.Runtime.FanOutWorkers != 2 || cfg.Runtime.RecoveryOnStartup || !cfg.Workspace.ImageConfigured() || !cfg.Workspace.AllowExecOnHostConfigured() || cfg.Workspace.AllowExecOnHost {
		t.Fatalf("lost values/presence: %#v %#v", cfg.Runtime, cfg.Workspace)
	}
	var cli CLISourceConfig
	if err := yaml.Unmarshal([]byte(source), &cli); err != nil || cli.Connection.APIServer != "localhost" {
		t.Fatalf("CLI source=%#v error=%v", cli, err)
	}
	for _, source := range []string{"runtime: {<<: {fan_out_workers: 1}, fan_out_workers: 2}", "workspace: {image: a, image: b}"} {
		if err := yaml.Unmarshal([]byte(source), &cfg); err == nil || !strings.Contains(err.Error(), "duplicate") {
			t.Fatalf("duplicate %q admitted: %v", source, err)
		}
	}
	for _, source := range []string{"", "runtime: null", "workspace: {}", "workspace: null"} {
		if err := yaml.Unmarshal([]byte(source), &cfg); err != nil {
			t.Fatalf("supported presence %q: %v", source, err)
		}
	}
}

func TestConfigLoadUnknownKeyDiagnosticNamesSourceVocabularyAndSuggestion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "swarm.yaml")
	if err := os.WriteFile(path, []byte("runtime:\n  recovery_on_startupp: true\n"), 0600); err != nil {
		t.Fatal(err)
	}
	_, err := Load(path)
	diagnostic, ok := runtimecontracts.AsLoaderDiagnostic(err)
	if !ok || diagnostic.Location.File != path || diagnostic.Location.Line != 2 || !strings.Contains(diagnostic.Remediation, "recovery_on_startup") {
		t.Fatalf("error=%v diagnostic=%#v", err, diagnostic)
	}
	if !strings.Contains(err.Error(), "Valid fields:") || !strings.Contains(err.Error(), path) {
		t.Fatal(err)
	}
}
