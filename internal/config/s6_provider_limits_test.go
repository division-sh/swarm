package config

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/yamlsource"
	"gopkg.in/yaml.v3"
)

func TestS6ProviderLimitsRejectRetiredFieldsBeforeProjection(t *testing.T) {
	for _, key := range []string{"rate_limit", "rate_limit_max_wait", "max_concurrency_max_wait", "models"} {
		for _, literal := range []string{"null", "''", "{}", "[]", "0", "false", "'1/s'"} {
			for _, merged := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/merged_%t", key, literal, merged), func(t *testing.T) {
					policy := fmt.Sprintf("{%s: %s, max_concurrency: 2}", key, literal)
					if merged {
						policy = fmt.Sprintf("{<<: &retired {%s: %s}, max_concurrency: 2}", key, literal)
					}
					source := "llm: {provider_limits: {anthropic: " + policy + "}}\n"
					target := Config{LLM: LLMConfig{Backend: "mock", ProviderLimits: map[string]LLMProviderLimitPolicy{"mock": {MaxConcurrency: 3}}}}
					want := Config{LLM: LLMConfig{Backend: "mock", ProviderLimits: map[string]LLMProviderLimitPolicy{"mock": {MaxConcurrency: 3}}}}
					err := decodeConfigSourceTest(source, &target)
					assertS6RetiredProviderField(t, err, key)
					if !reflect.DeepEqual(target, want) {
						t.Fatalf("rejected source changed projection: %#v", target)
					}
					path := filepath.Join(t.TempDir(), "swarm.yaml")
					if err := os.WriteFile(path, []byte(source), 0600); err != nil {
						t.Fatal(err)
					}
					_, err = LoadWithOptions(path, LoadOptions{BackendOverride: "mock"})
					assertS6RetiredProviderField(t, err, key)
					var direct LLMProviderLimitPolicy
					assertS6RetiredProviderField(t, yaml.Unmarshal([]byte(policy), &direct), key)
				})
			}
		}
	}
}

func assertS6RetiredProviderField(t *testing.T, err error, key string) {
	t.Helper()
	diagnostic, ok := runtimecontracts.AsLoaderDiagnostic(err)
	if !ok || !strings.Contains(diagnostic.Problem, key) || !reflect.DeepEqual(diagnostic.ValidOptions, []string{"max_concurrency"}) {
		t.Fatalf("error = %v, want typed %s refusal with only max_concurrency admitted", err, key)
	}
}

func TestS6ProviderLimitsAdmitOneProfileInteger(t *testing.T) {
	for _, profile := range []string{"anthropic", "openai_compatible", "openai_responses", "claude_cli", "mock"} {
		t.Run(profile, func(t *testing.T) {
			source := fmt.Sprintf("llm: {provider_limits: {%s: {max_concurrency: 2}}}\n", profile)
			var target Config
			if err := decodeConfigSourceTest(source, &target); err != nil {
				t.Fatal(err)
			}
			if err := target.validateLLMProviderLimits(); err != nil {
				t.Fatalf("one profile integer rejected: %v", err)
			}
			path := filepath.Join(t.TempDir(), "swarm.yaml")
			fileSource := fmt.Sprintf("llm:\n  backend: anthropic\n  session: {lock_ttl: 1s, rotate_after_turns: 1, rotate_on_parse_failures: 1}\n  provider_limits: {%s: {max_concurrency: 2}}\n", profile)
			if err := os.WriteFile(path, []byte(fileSource), 0600); err != nil {
				t.Fatal(err)
			}
			loaded, err := Load(path)
			if err != nil || loaded.LLM.ProviderLimits[profile].MaxConcurrency != 2 {
				t.Fatalf("on-disk profile integer: cfg=%#v err=%v", loaded, err)
			}
			var direct LLMProviderLimitPolicy
			if err := yaml.Unmarshal([]byte("{max_concurrency: 2}"), &direct); err != nil || direct.MaxConcurrency != 2 {
				t.Fatalf("direct policy = %#v, err = %v", direct, err)
			}
		})
	}
}

func TestS6ProviderLimitsRejectDuplicateCapsAndPreserveAliases(t *testing.T) {
	for _, policy := range []string{"{max_concurrency: 2, max_concurrency: 3}", "{<<: {max_concurrency: 2}, max_concurrency: 3}"} {
		var cfg Config
		if err := decodeConfigSourceTest("llm: {provider_limits: {mock: "+policy+"}}", &cfg); err == nil || !strings.Contains(err.Error(), "duplicate") {
			t.Fatalf("duplicate cap admitted: %v", err)
		}
	}
	var cfg Config
	if err := decodeConfigSourceTest("llm: {provider_limits: {anthropic: &policy {max_concurrency: 2}, mock: *policy}}", &cfg); err != nil {
		t.Fatal(err)
	}
	if err := cfg.validateLLMProviderLimits(); err != nil || cfg.LLM.ProviderLimits["mock"].MaxConcurrency != 2 || cfg.LLM.ProviderLimits["anthropic"].MaxConcurrency != 2 {
		t.Fatalf("aliased profile integer: cfg=%#v err=%v", cfg.LLM.ProviderLimits, err)
	}
}

func TestS6ProviderLimitsRefuseNonpositiveOrNonintegerSource(t *testing.T) {
	for _, literal := range []string{"0", "-1", "2.5", "2.0", "'2'", "true", "null", "{}", "[]"} {
		t.Run(literal, func(t *testing.T) {
			source := "llm: {provider_limits: {anthropic: {max_concurrency: " + literal + "}}}\n"
			var node yaml.Node
			if err := yaml.Unmarshal([]byte(source), &node); err != nil {
				t.Fatal(err)
			}
			target := Config{LLM: LLMConfig{Backend: "mock"}}
			err := DecodeSource(yamlsource.ValueFromNode(&node), &target)
			if err == nil || !strings.Contains(err.Error(), "max_concurrency must be a positive integer") {
				t.Fatalf("error = %v, want positive-integer refusal", err)
			}
			if target.LLM.Backend != "mock" || target.LLM.ProviderLimits != nil {
				t.Fatalf("invalid integer changed projection: %#v", target)
			}
			var direct LLMProviderLimitPolicy
			if err := yaml.Unmarshal([]byte("{max_concurrency: "+literal+"}"), &direct); err == nil {
				t.Fatal("direct policy admitted invalid integer")
			}
		})
	}
}

func TestS6ProviderLimitsPreserveGlobalModelsAndNoRoleCap(t *testing.T) {
	var cfg Config
	if err := decodeConfigSourceTest("llm: {models: {business: {provider: model}}, provider_limits: {mock: {max_concurrency: 2}}}", &cfg); err != nil {
		t.Fatal(err)
	}
	if len(cfg.LLM.Models) != 1 || cfg.LLM.ProviderLimits["mock"].MaxConcurrency != 2 {
		t.Fatalf("profile integer changed model aliases: %#v", cfg.LLM)
	}
	err := decodeConfigSourceTest("runtime: {max_concurrent_agents: 2}", &cfg)
	diagnostic, ok := runtimecontracts.AsLoaderDiagnostic(err)
	if !ok || !strings.Contains(diagnostic.Problem, "max_concurrent_agents") {
		t.Fatalf("role cap admitted: %v", err)
	}
}
