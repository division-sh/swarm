package runtime

import (
	"testing"
	"time"

	"empireai/internal/config"
)

func TestRuntimeFactory_Build_APIAndCLI(t *testing.T) {
	base := &config.Config{
		LLM: config.LLMConfig{
			Session: config.LLMSessionConfig{
				LockTTL:               2 * time.Second,
				RotateAfterTurns:      10,
				RotateOnParseFailures: 2,
			},
			ClaudeAPI: config.ClaudeAPIConfig{
				DefaultModel: "m",
				MaxRetries:   1,
				RetryBackoff: 1 * time.Millisecond,
			},
			ClaudeCLI: config.ClaudeCLIConfig{
				Command:      "true",
				Timeout:      1 * time.Second,
				OutputFormat: "json",
				Retries:      1,
			},
		},
	}

	cfgAPI := *base
	cfgAPI.LLM.RuntimeMode = "api"
	r1, err := (RuntimeFactory{Cfg: &cfgAPI}).Build()
	if err != nil {
		t.Fatalf("build api: %v", err)
	}
	if _, ok := r1.(*AnthropicAPIRuntime); !ok {
		t.Fatalf("expected *AnthropicAPIRuntime, got %T", r1)
	}

	cfgCLI := *base
	cfgCLI.LLM.RuntimeMode = "cli_test"
	r2, err := (RuntimeFactory{Cfg: &cfgCLI}).Build()
	if err != nil {
		t.Fatalf("build cli: %v", err)
	}
	if _, ok := r2.(*ClaudeCLIRuntime); !ok {
		t.Fatalf("expected *ClaudeCLIRuntime, got %T", r2)
	}

	if defaultLockOwner() == "" {
		t.Fatal("expected non-empty lock owner")
	}

	// Back-compat helper.
	if NewSessionRegistry(1*time.Second) == nil {
		t.Fatal("expected session registry")
	}
}

