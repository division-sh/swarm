package config

import (
	"reflect"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

// USER #1801/6170905244: omission keeps defaults; a null container is not a reset.
func TestSingleDeploymentConfigRejectsNullContainersBeforeProjection(t *testing.T) {
	for _, path := range []string{
		"", "runtime", "database", "store", "store.sqlite", "workspace", "llm",
		"llm.models", "llm.models.regular", "llm.session", "llm.claude_cli",
		"llm.openai_compatible", "llm.openai_responses", "llm.provider_limits",
		"llm.provider_limits.anthropic", "llm.provider_limits.anthropic.models",
		"llm.provider_limits.anthropic.models.model", "platform", "platform.packs",
		"platform.packs.platform_dirs", "channels", "channels.bindings",
		"channels.bindings.hitl", "budget", "budget.human_tasks",
		"budget.human_tasks.categories_enabled", "connection", "serve", "paths",
	} {
		for _, literal := range []string{"null", "~"} {
			for _, merged := range []bool{false, true} {
				name := path + "/" + literal
				if merged {
					name += "/merged"
				}
				t.Run(name, func(t *testing.T) {
					source := literal
					if path != "" {
						parts := strings.Split(path, ".")
						source = parts[len(parts)-1] + ": " + source
						if merged {
							source = "<<: {" + source + "}"
						}
						for i := len(parts) - 2; i >= 0; i-- {
							source = parts[i] + ": {" + source + "}"
						}
					}
					before := Config{Runtime: RuntimeConfig{RecoveryOnStartup: true}, LLM: LLMConfig{Backend: "anthropic"}}
					got := before
					err := decodeConfigSourceTest(source, &got)
					diagnostic, ok := runtimecontracts.AsLoaderDiagnostic(err)
					wantPath := "config"
					if path != "" {
						wantPath += "." + path
					}
					if !ok || diagnostic.Code != "config.source_shape" || diagnostic.Location.YAMLPath != wantPath || diagnostic.Location.Line == 0 || diagnostic.Location.Column == 0 || !strings.Contains(diagnostic.Remediation, "omit") {
						t.Fatalf("null container did not produce located typed refusal: %v (%#v)", err, diagnostic)
					}
					if !reflect.DeepEqual(got, before) {
						t.Fatal("refused container changed caller-owned defaults")
					}
				})
			}
		}
	}
}

func TestSingleDeploymentConfigPreservesOmittedAndExplicitLeafValues(t *testing.T) {
	for _, source := range []string{"", "{}", "runtime: {}\nllm: {session: {}}\n"} {
		t.Run("omitted/"+source, func(t *testing.T) {
			before := Config{Runtime: RuntimeConfig{RecoveryOnStartup: true}, LLM: LLMConfig{Backend: "anthropic", Session: LLMSessionConfig{RotateAfterTurns: 40}}}
			got := before
			if err := decodeConfigSourceTest(source, &got); err != nil || !reflect.DeepEqual(got, before) {
				t.Fatalf("omitted values changed defaults: got=%#v error=%v", got, err)
			}
		})
	}
	for _, source := range []string{
		"runtime: {recovery_on_startup: false}\nllm: {backend: '', session: {rotate_after_turns: 0}}\nplatform: {packs: {platform_dirs: []}}\n",
		"runtime: {<<: {recovery_on_startup: false}}\nllm: {<<: {backend: '', session: {rotate_after_turns: 0}}}\nplatform: {packs: {platform_dirs: []}}\n",
	} {
		t.Run("explicit/"+source, func(t *testing.T) {
			got := Config{Runtime: RuntimeConfig{RecoveryOnStartup: true}, LLM: LLMConfig{Backend: "anthropic", Session: LLMSessionConfig{RotateAfterTurns: 40}}, Platform: PlatformConfig{Packs: PlatformPacksConfig{PlatformDirs: []string{"old"}}}}
			if err := decodeConfigSourceTest(source, &got); err != nil {
				t.Fatal(err)
			}
			if got.Runtime.RecoveryOnStartup || got.LLM.Backend != "" || got.LLM.Session.RotateAfterTurns != 0 || len(got.Platform.Packs.PlatformDirs) != 0 {
				t.Fatalf("explicit false/empty/zero/sequence lost: %#v", got)
			}
		})
	}
}

func TestSingleDeploymentConfigRejectsWrongContainerShapeBeforeProjection(t *testing.T) {
	for _, row := range []struct{ source, path, expected string }{
		{"[]", "config", "mapping"},
		{"runtime: false", "config.runtime", "mapping"},
		{"workspace: []", "config.workspace", "mapping"},
		{"connection: ''", "config.connection", "mapping"},
		{"llm: {provider_limits: []}", "config.llm.provider_limits", "mapping"},
		{"llm: {models: {regular: []}}", "config.llm.models.regular", "mapping"},
		{"platform: {packs: {platform_dirs: {}}}", "config.platform.packs.platform_dirs", "sequence"},
		{"budget: {human_tasks: {categories_enabled: false}}", "config.budget.human_tasks.categories_enabled", "sequence"},
	} {
		t.Run(row.path+"/"+row.source, func(t *testing.T) {
			before := Config{Runtime: RuntimeConfig{RecoveryOnStartup: true}, LLM: LLMConfig{Backend: "anthropic"}}
			got := before
			err := decodeConfigSourceTest(row.source, &got)
			diagnostic, ok := runtimecontracts.AsLoaderDiagnostic(err)
			if !ok || diagnostic.Code != "config.source_shape" || diagnostic.Location.YAMLPath != row.path || diagnostic.Location.Line == 0 || diagnostic.Location.Column == 0 || !strings.Contains(diagnostic.Problem, "must be a YAML "+row.expected) || !reflect.DeepEqual(got, before) {
				t.Fatalf("wrong-shaped container admitted or misdiagnosed: error=%v diagnostic=%#v", err, diagnostic)
			}
		})
	}
}
