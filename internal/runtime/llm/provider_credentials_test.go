package llm

import (
	"context"
	"testing"

	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
)

func TestProviderCredentialResolver_StoreWinsOverEnvForActiveProfiles(t *testing.T) {
	for backend, key := range map[string]string{
		llmselection.BackendAnthropic:        "ANTHROPIC_API_KEY",
		llmselection.BackendClaudeCLI:        "CLAUDE_CODE_OAUTH_TOKEN",
		llmselection.BackendOpenAICompatible: "OPENAI_COMPATIBLE_API_KEY",
		llmselection.BackendOpenAIResponses:  "OPENAI_API_KEY",
	} {
		t.Run(backend, func(t *testing.T) {
			t.Setenv(key, "env-"+key)
			profile, err := llmselection.ResolveActiveBackend(backend)
			if err != nil {
				t.Fatalf("ResolveActiveBackend: %v", err)
			}
			resolver := testProviderCredentialResolver(t, key, "stored-"+key)
			credential, err := resolver.Resolve(context.Background(), profile)
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			if credential.Value != "stored-"+key {
				t.Fatalf("credential value = %q, want stored secret", credential.Value)
			}
			if credential.Source != runtimecredentials.SourceFile {
				t.Fatalf("credential source = %q, want file", credential.Source)
			}
			if !credential.EnvPresent || !credential.EnvShadowed {
				t.Fatalf("credential env diagnostics = present:%v shadowed:%v, want true/true", credential.EnvPresent, credential.EnvShadowed)
			}
		})
	}
}

func TestProviderCredentialResolver_EnvOnlyFailsClosed(t *testing.T) {
	t.Setenv("ANTHROPIC_API_KEY", "env-only")
	profile, err := llmselection.ResolveActiveBackend(llmselection.BackendAnthropic)
	if err != nil {
		t.Fatalf("ResolveActiveBackend: %v", err)
	}
	resolver := testProviderCredentialResolver(t, "ANTHROPIC_API_KEY", "")
	_, err = resolver.Resolve(context.Background(), profile)
	if err == nil {
		t.Fatal("Resolve error = nil, want missing provider credential")
	}
	failure, ok := runtimefailures.As(err)
	if !ok || failure.Failure.Class != runtimefailures.ClassAuthenticationNeeded || failure.Failure.Detail.Code != "provider_credential_missing" {
		t.Fatalf("Resolve failure = %#v, want authentication required", failure)
	}
}

func TestProviderCredentialResolver_ActiveCredentiallessProfileNeedsNoSyntheticKey(t *testing.T) {
	profile, err := llmselection.ResolveActiveBackend(llmselection.BackendMock)
	if err != nil {
		t.Fatalf("ResolveActiveBackend: %v", err)
	}
	resolver := NewProviderCredentialResolver(nil)
	if credential, resolveErr := resolver.Resolve(context.Background(), profile); resolveErr != nil || credential.Key != "" || credential.Value != "" {
		t.Fatalf("Resolve credential = %#v err=%v, want empty credential", credential, resolveErr)
	}
	if credential, inspectErr := resolver.Inspect(context.Background(), profile); inspectErr != nil || credential.Key != "" || credential.Value != "" {
		t.Fatalf("Inspect credential = %#v err=%v, want empty credential", credential, inspectErr)
	}
}
