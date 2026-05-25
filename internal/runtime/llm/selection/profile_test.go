package selection

import (
	"strings"
	"testing"
)

func TestResolveActiveBackendProfiles(t *testing.T) {
	tests := []struct {
		raw       string
		wantID    string
		provider  string
		transport string
	}{
		{raw: "", wantID: BackendAPI, provider: ProviderAnthropic, transport: TransportAPI},
		{raw: BackendAPI, wantID: BackendAPI, provider: ProviderAnthropic, transport: TransportAPI},
		{raw: BackendCLITest, wantID: BackendCLITest, provider: ProviderClaude, transport: TransportCLI},
	}
	for _, tt := range tests {
		t.Run(tt.raw, func(t *testing.T) {
			profile, err := ResolveActiveBackend(tt.raw)
			if err != nil {
				t.Fatalf("ResolveActiveBackend: %v", err)
			}
			if profile.ID != tt.wantID || profile.Provider != tt.provider || profile.Transport != tt.transport {
				t.Fatalf("profile = %#v, want id=%s provider=%s transport=%s", profile, tt.wantID, tt.provider, tt.transport)
			}
		})
	}
}

func TestResolveActiveBackendRejectsReservedAndUnknown(t *testing.T) {
	for _, raw := range []string{BackendMock, BackendLocal, "openai"} {
		t.Run(raw, func(t *testing.T) {
			if _, err := ResolveActiveBackend(raw); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestResolvePersistedBackendAllowsReservedProfiles(t *testing.T) {
	for _, raw := range []string{BackendAPI, BackendCLITest, BackendMock, BackendLocal} {
		t.Run(raw, func(t *testing.T) {
			profile, err := ResolvePersistedBackend(raw)
			if err != nil {
				t.Fatalf("ResolvePersistedBackend: %v", err)
			}
			if profile.ID != raw {
				t.Fatalf("profile id = %q, want %q", profile.ID, raw)
			}
		})
	}
}

func TestRejectRetiredSelectors(t *testing.T) {
	if err := RejectRetiredConfigRuntimeMode("api"); err == nil || !strings.Contains(err.Error(), ConfigBackendField) {
		t.Fatalf("RejectRetiredConfigRuntimeMode error = %v, want backend guidance", err)
	}
	if err := RejectRetiredEnvRuntimeMode(func(key string) (string, bool) {
		return "api", key == RetiredEnvRuntimeMode
	}); err == nil || !strings.Contains(err.Error(), EnvBackend) {
		t.Fatalf("RejectRetiredEnvRuntimeMode error = %v, want backend env guidance", err)
	}
}

func TestCredentialAuthority(t *testing.T) {
	profile, err := ResolveActiveBackend(BackendAPI)
	if err != nil {
		t.Fatalf("ResolveActiveBackend: %v", err)
	}
	lookup := func(key string) (string, bool) {
		if key == profile.Credential.EnvVar {
			return " token ", true
		}
		return "", false
	}
	if got := CredentialValue(profile, lookup); got != "token" {
		t.Fatalf("CredentialValue = %q, want token", got)
	}
	if err := RequireCredential(profile, lookup); err != nil {
		t.Fatalf("RequireCredential: %v", err)
	}
	if err := RequireCredential(profile, func(string) (string, bool) { return "", false }); err == nil {
		t.Fatal("expected missing credential error")
	}
}

func TestResolveModelNameUsesModelTier(t *testing.T) {
	profile, err := ResolveActiveBackend(BackendAPI)
	if err != nil {
		t.Fatalf("ResolveActiveBackend: %v", err)
	}
	req := ModelResolution{
		ModelTier: "haiku",
		Models:    ModelMap{Default: "claude-sonnet", Haiku: "claude-haiku"},
	}
	if got, err := ResolveModelName(profile, req); err != nil || got != "claude-haiku" {
		t.Fatalf("ResolveModelName = %q, %v; want claude-haiku", got, err)
	}
	req.ModelTier = "sonnet"
	if got, err := ResolveModelName(profile, req); err != nil || got != "claude-sonnet" {
		t.Fatalf("ResolveModelName = %q, %v; want claude-sonnet", got, err)
	}
	req.ForceLowCost = true
	if got, err := ResolveModelName(profile, req); err != nil || got != "claude-haiku" {
		t.Fatalf("ResolveModelName forced low cost = %q, %v; want claude-haiku", got, err)
	}
}

func TestResolveCLIModelNameUsesModelTier(t *testing.T) {
	profile, err := ResolveActiveBackend(BackendCLITest)
	if err != nil {
		t.Fatalf("ResolveActiveBackend: %v", err)
	}
	if got, err := ResolveModelName(profile, ModelResolution{ModelTier: "sonnet"}); err != nil || got != "sonnet" {
		t.Fatalf("ResolveModelName = %q, %v; want sonnet", got, err)
	}
	if got, err := ResolveModelName(profile, ModelResolution{}); err != nil || got != "generic" {
		t.Fatalf("ResolveModelName empty = %q, %v; want generic", got, err)
	}
}
