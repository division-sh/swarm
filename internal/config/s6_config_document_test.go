package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestS6DirectConfigDocumentAdmission(t *testing.T) {
	const valid = "llm:\n  backend: anthropic\n  session: {lock_ttl: 10s, rotate_after_turns: 40, rotate_on_parse_failures: 3}\n  provider_limits: {anthropic: {max_concurrency: 2}}\n"
	for _, test := range []struct {
		name, source, override, backend, refusal string
	}{
		{name: "single", source: valid, backend: "anthropic"},
		{name: "override", source: valid, override: "openai_responses", backend: "openai_responses"},
		{name: "empty", refusal: "has no document"},
		{name: "comment_only", source: "# no document\n", refusal: "has no document"},
		{name: "multiple", source: valid + "---\nllm: {backend: mock}\n", refusal: "has multiple documents"},
		{name: "multiple_with_override", source: valid + "---\n{}\n", override: "openai_responses", refusal: "has multiple documents"},
		{name: "malformed", source: valid + "budget: [\n", refusal: "parse config"},
		{name: "unknown", source: valid + "unknown_s6: true\n", refusal: "unknown_s6"},
		{name: "unknown_with_override", source: valid + "unknown_s6: true\n", override: "openai_responses", refusal: "unknown_s6"},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "swarm.yaml")
			if err := os.WriteFile(path, []byte(test.source), 0600); err != nil {
				t.Fatal(err)
			}
			cfg, err := LoadWithOptions(path, LoadOptions{BackendOverride: test.override})
			if test.refusal != "" {
				if cfg != nil || err == nil || !strings.Contains(err.Error(), test.refusal) {
					t.Fatalf("direct source admission: cfg=%+v err=%v, want %q", cfg, err, test.refusal)
				}
				if strings.HasPrefix(test.name, "unknown") {
					diagnostic, ok := runtimecontracts.AsLoaderDiagnostic(err)
					if !ok || diagnostic.Location.File != path || diagnostic.Location.Line == 0 {
						t.Fatalf("unknown source lost typed/file diagnostic: %v", err)
					}
				}
				return
			}
			if err != nil || cfg == nil || cfg.LLM.Backend != test.backend || !cfg.Runtime.RecoveryOnStartup || cfg.LLM.ProviderLimits["anthropic"].MaxConcurrency != 2 {
				t.Fatalf("single document changed values/defaults/override: cfg=%+v err=%v", cfg, err)
			}
		})
	}
}
