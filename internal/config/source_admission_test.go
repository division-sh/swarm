package config

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/yamlsource"
	"gopkg.in/yaml.v3"
)

func decodeConfigSourceTest(source string, target *Config) error {
	var document yaml.Node
	if err := yaml.Unmarshal([]byte(source), &document); err != nil {
		return err
	}
	return DecodeSource(yamlsource.ValueFromNode(&document), target)
}

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
		{"paths: {unrecognized: file}", func() any { return &Config{} }, []string{"agent_config_map_file", "verification_gates_file", "tooling_lock_file", "platform_spec_path"}, []string{"swarm_dir", "monitor_dir"}},
	} {
		t.Run(row.source, func(t *testing.T) {
			target := row.target()
			var err error
			if cfg, ok := target.(*Config); ok {
				err = decodeConfigSourceTest(row.source, cfg)
			} else {
				err = yaml.Unmarshal([]byte(row.source), target)
			}
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

func TestConfigSourceRejectsExcludedDeclaredFieldsBeforeProjection(t *testing.T) {
	for _, row := range []struct{ path, supported string }{
		{"sharding", "runtime"},
		{"runtime.max_concurrent_agents", "fan_out_workers"},
		{"runtime.event_poll_interval", "recovery_on_startup"},
		{"database.password", "password_env"},
		{"llm.claude_cli.retries", "timeout"},
		{"llm.claude_cli.no_session_persistence", "command"},
		{"llm.claude_cli.use_tmux", "output_format"},
		{"paths.agent_config_map_file", "swarm_dir"},
		{"paths.verification_gates_file", "monitor_dir"},
		{"paths.tooling_lock_file", "swarm_dir"},
		{"paths.platform_spec_path", "monitor_dir"},
	} {
		parts := strings.Split(row.path, ".")
		key := parts[len(parts)-1]
		for _, literal := range []string{"null", "''", "{}", "[]", "1", "false"} {
			for _, merged := range []bool{false, true} {
				source := key + ": " + literal
				if merged {
					source = "<<: {" + source + "}"
				}
				for i := len(parts) - 2; i >= 0; i-- {
					source = parts[i] + ": {" + source + "}"
				}
				t.Run(fmt.Sprintf("%s/%s/merged=%t", row.path, literal, merged), func(t *testing.T) {
					assertDiagnostic := func(err error, file string) {
						t.Helper()
						diagnostic, ok := runtimecontracts.AsLoaderDiagnostic(err)
						if !ok || !strings.Contains(diagnostic.Problem, key) || slices.Contains(diagnostic.ValidOptions, key) || !slices.Contains(diagnostic.ValidOptions, row.supported) || diagnostic.Location.File != file || diagnostic.Location.Line == 0 || diagnostic.Location.Column == 0 {
							t.Fatalf("excluded field admitted or misdiagnosed: error=%v diagnostic=%#v", err, diagnostic)
						}
						if row.path == "paths.platform_spec_path" {
							want := fmt.Sprintf("`paths.platform_spec_path` at <config>:%d:%d is not a config key; the platform spec is embedded in `swarm`.", diagnostic.Location.Line, diagnostic.Location.Column)
							if err.Error() != want || diagnostic.Remediation != "" {
								t.Fatalf("embedded-spec refusal differs from ruling6197217312: %v", err)
							}
						} else if !strings.Contains(err.Error(), "Valid fields:") || strings.Contains(err.Error(), "RETIRED") {
							t.Fatalf("not current-vocabulary rejection: %v", err)
						}
					}
					before := Config{Runtime: RuntimeConfig{RecoveryOnStartup: true}, LLM: LLMConfig{Backend: "anthropic"}}
					got := before
					assertDiagnostic(decodeConfigSourceTest(source, &got), "")
					if !reflect.DeepEqual(got, before) {
						t.Fatal("excluded source changed caller-owned defaults")
					}
					if parts[0] == "runtime" {
						var runtime RuntimeConfig
						assertDiagnostic(yaml.Unmarshal([]byte(strings.TrimSuffix(strings.TrimPrefix(source, "runtime: {"), "}")), &runtime), "")
					}
				})
			}
		}
	}
}

func TestConfigPlatformSpecOverrideRefusalTeachesEmbeddedDefault(t *testing.T) {
	for _, source := range []string{
		"paths:\n  platform_spec_path: elsewhere.yaml\n",
		"paths:\n  platform_spec_path: null\n",
		"paths:\n  platform_spec_path: ''\n",
		"paths:\n  <<: {platform_spec_path: elsewhere.yaml}\n",
	} {
		t.Run(source, func(t *testing.T) {
			before := Config{Runtime: RuntimeConfig{RecoveryOnStartup: true}}
			got := before
			err := decodeConfigSourceTest(source, &got)
			diagnostic, ok := runtimecontracts.AsLoaderDiagnostic(err)
			if !ok || diagnostic.Remediation != "" || diagnostic.Location.Line != 2 || diagnostic.Location.Column == 0 || slices.Contains(diagnostic.ValidOptions, "platform_spec_path") {
				t.Fatalf("missing exact typed retirement teaching/location: error=%v diagnostic=%#v", err, diagnostic)
			}
			want := fmt.Sprintf("`paths.platform_spec_path` at <config>:2:%d is not a config key; the platform spec is embedded in `swarm`.", diagnostic.Location.Column)
			if err.Error() != want {
				t.Fatalf("refusal = %q, want %q", err.Error(), want)
			}
			if !reflect.DeepEqual(got, before) {
				t.Fatal("refused config mutated the projection target")
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
				var document yaml.Node
				err := decoder.Decode(&document)
				if err == nil {
					err = DecodeSource(yamlsource.ValueFromNode(&document), &Config{})
				}
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
	source := "runtime: {<<: {fan_out_workers: 2, recovery_on_startup: false}}\nworkspace: {<<: {image: '', allow_exec_on_host: false}, backend: host}\nllm: {models: {business_model: {provider: model}}, provider_limits: {provider: {models: {model: {max_concurrency: 2}}}}}\nconnection: {api_server: localhost}\nbudget: {human_tasks: {categories_enabled: [support]}}\npaths: {swarm_dir: state, monitor_dir: monitor}\n"
	if err := decodeConfigSourceTest(source, &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Runtime.FanOutWorkers == nil || *cfg.Runtime.FanOutWorkers != 2 || cfg.Runtime.RecoveryOnStartup || !cfg.Workspace.ImageConfigured() || !cfg.Workspace.AllowExecOnHostConfigured() || cfg.Workspace.AllowExecOnHost {
		t.Fatalf("lost values/presence: %#v %#v", cfg.Runtime, cfg.Workspace)
	}
	var cli CLISourceConfig
	if err := yaml.Unmarshal([]byte(source), &cli); err != nil || cli.Connection.APIServer != "localhost" || cli.Paths.SwarmDir != "state" || cli.Paths.MonitorDir != "monitor" {
		t.Fatalf("CLI source=%#v error=%v", cli, err)
	}
	for _, source := range []string{"runtime: {<<: {fan_out_workers: 1}, fan_out_workers: 2}", "workspace: {image: a, image: b}"} {
		if err := decodeConfigSourceTest(source, &cfg); err == nil || !strings.Contains(err.Error(), "duplicate") {
			t.Fatalf("duplicate %q admitted: %v", source, err)
		}
	}
	// USER #1801/6170905244: omitted/empty containers remain valid; null refuses.
	for _, source := range []string{"", "runtime: {}", "workspace: {}"} {
		if err := decodeConfigSourceTest(source, &cfg); err != nil {
			t.Fatalf("supported presence %q: %v", source, err)
		}
	}
}

func TestConfigDecodeUnknownKeyDiagnosticNamesSourceVocabularyAndSuggestion(t *testing.T) {
	var cfg Config
	err := decodeConfigSourceTest("runtime:\n  recovery_on_startupp: true\n", &cfg)
	diagnostic, ok := runtimecontracts.AsLoaderDiagnostic(err)
	if !ok || diagnostic.Location.File != "" || diagnostic.Location.Line != 2 || !strings.Contains(diagnostic.Remediation, "recovery_on_startup") {
		t.Fatalf("error=%v diagnostic=%#v", err, diagnostic)
	}
	if !strings.Contains(err.Error(), "Valid fields:") {
		t.Fatal(err)
	}
}

func TestConfigDecodeSourceRejectsBeforeProjection(t *testing.T) {
	if _, exists := reflect.TypeFor[*Config]().MethodByName("UnmarshalYAML"); exists {
		t.Fatal("extra root callback restored instead of explicit admission")
	}
	for _, source := range []string{
		"totally_unknown: true",
		"<<: {totally_unknown: true}",
		"database: {<<: {totally_unknown: true}}",
		"llm: {provider_limits: {provider: {models: {model: {unknown: 1}}}}}",
		"runtime: {recovery_on_startup: true, recovery_on_startup: false}",
	} {
		t.Run(source, func(t *testing.T) {
			before := Config{Runtime: RuntimeConfig{RecoveryOnStartup: true}, LLM: LLMConfig{Backend: "anthropic"}}
			got := before
			if err := decodeConfigSourceTest(source, &got); err == nil {
				t.Fatal("invalid source projected")
			}
			if !reflect.DeepEqual(got, before) {
				t.Fatal("refused source changed caller-owned defaults")
			}
		})
	}
	if err := DecodeSource(yamlsource.MissingDocument("").Root(), nil); err == nil {
		t.Fatal("nil projection target accepted")
	}
}
