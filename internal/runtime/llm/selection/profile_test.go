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
		{raw: "", wantID: BackendAnthropic, provider: ProviderAnthropic, transport: TransportAPI},
		{raw: BackendAnthropic, wantID: BackendAnthropic, provider: ProviderAnthropic, transport: TransportAPI},
		{raw: BackendClaudeCLI, wantID: BackendClaudeCLI, provider: ProviderClaude, transport: TransportCLI},
		{raw: BackendOpenAICompatible, wantID: BackendOpenAICompatible, provider: ProviderOpenAICompatible, transport: TransportAPI},
		{raw: BackendOpenAIResponses, wantID: BackendOpenAIResponses, provider: ProviderOpenAI, transport: TransportAPI},
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
	for _, raw := range []string{BackendMock, BackendLocal, LegacyBackendAPI, LegacyBackendCLITest, "openai"} {
		t.Run(raw, func(t *testing.T) {
			if _, err := ResolveActiveBackend(raw); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestResolvePersistedBackendAllowsReservedProfiles(t *testing.T) {
	for _, raw := range []string{BackendAnthropic, BackendClaudeCLI, BackendOpenAICompatible, BackendOpenAIResponses, BackendMock, BackendLocal} {
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

func TestResolvePersistedBackendAcceptsActivatedOpenAIResponsesProfile(t *testing.T) {
	profile, err := ResolvePersistedBackend(BackendOpenAIResponses)
	if err != nil {
		t.Fatalf("ResolvePersistedBackend(openai_responses): %v", err)
	}
	if profile.Provider != ProviderOpenAI || profile.RuntimeMode != ProviderContractRuntimeModeOpenAIResponses {
		t.Fatalf("openai_responses profile = %#v", profile)
	}
}

func TestMigratePersistedBackendBackfillsLegacyProfiles(t *testing.T) {
	tests := []struct {
		raw     string
		want    string
		changed bool
	}{
		{raw: LegacyBackendAPI, want: BackendAnthropic, changed: true},
		{raw: LegacyBackendCLITest, want: BackendClaudeCLI, changed: true},
		{raw: BackendOpenAICompatible, want: BackendOpenAICompatible},
		{raw: BackendOpenAIResponses, want: BackendOpenAIResponses},
		{raw: BackendMock, want: BackendMock},
	}
	for _, tt := range tests {
		profile, changed, err := MigratePersistedBackend(tt.raw)
		if err != nil {
			t.Fatalf("MigratePersistedBackend(%q): %v", tt.raw, err)
		}
		if profile.ID != tt.want || changed != tt.changed {
			t.Fatalf("MigratePersistedBackend(%q) = id=%q changed=%v, want id=%q changed=%v", tt.raw, profile.ID, changed, tt.want, tt.changed)
		}
	}
}

func TestRejectRetiredSelectors(t *testing.T) {
	if err := RejectRetiredConfigRuntimeMode("api"); err == nil || !strings.Contains(err.Error(), ConfigBackendField) {
		t.Fatalf("RejectRetiredConfigRuntimeMode error = %v, want backend guidance", err)
	}
	if err := RejectRetiredEnvBackend(func(key string) (string, bool) {
		return "api", key == EnvBackend
	}); err == nil || !strings.Contains(err.Error(), "--backend") {
		t.Fatalf("RejectRetiredEnvBackend error = %v, want flag/config guidance", err)
	}
	if err := RejectRetiredEnvRuntimeMode(func(key string) (string, bool) {
		return "api", key == RetiredEnvRuntimeMode
	}); err == nil || !strings.Contains(err.Error(), "--backend") {
		t.Fatalf("RejectRetiredEnvRuntimeMode error = %v, want flag/config guidance", err)
	}
	if err := RejectRetiredOpenAICompatibleBaseURLEnv(func(key string) (string, bool) {
		return "https://example.test/v1", key == OpenAICompatibleBaseURLEnv
	}); err == nil || !strings.Contains(err.Error(), OpenAICompatibleBaseURLConfigField) {
		t.Fatalf("RejectRetiredOpenAICompatibleBaseURLEnv error = %v, want config guidance", err)
	}
	if err := RejectRetiredModelEnv(func(key string) (string, bool) {
		return "claude-test", key == ClaudeDefaultModelEnv
	}); err == nil || !strings.Contains(err.Error(), "llm.models") {
		t.Fatalf("RejectRetiredModelEnv error = %v, want llm.models guidance", err)
	}
}

func TestResolveModelNameUsesModel(t *testing.T) {
	profile, err := ResolveActiveBackend(BackendAnthropic)
	if err != nil {
		t.Fatalf("ResolveActiveBackend: %v", err)
	}
	req := ModelResolution{
		Model: ModelAliasCheap,
		Models: ModelAliases{
			ModelAliasCheap:   {BackendAnthropic: "claude-haiku"},
			ModelAliasRegular: {BackendAnthropic: "claude-sonnet"},
		},
	}
	if got, err := ResolveModelName(profile, req); err != nil || got != "claude-haiku" {
		t.Fatalf("ResolveModelName = %q, %v; want claude-haiku", got, err)
	}
	req.Model = ModelAliasRegular
	if got, err := ResolveModelName(profile, req); err != nil || got != "claude-sonnet" {
		t.Fatalf("ResolveModelName = %q, %v; want claude-sonnet", got, err)
	}
}

func TestResolveOpenAICompatibleProfileAuthority(t *testing.T) {
	profile, err := ResolveActiveBackend(BackendOpenAICompatible)
	if err != nil {
		t.Fatalf("ResolveActiveBackend: %v", err)
	}
	if profile.Credential.EnvVar != OpenAICompatibleCredentialEnv {
		t.Fatalf("credential env = %q, want %q", profile.Credential.EnvVar, OpenAICompatibleCredentialEnv)
	}
	if _, err := ResolveBaseURL(profile, ""); err == nil || !strings.Contains(err.Error(), OpenAICompatibleBaseURLConfigField) {
		t.Fatalf("ResolveBaseURL missing error = %v, want %s", err, OpenAICompatibleBaseURLConfigField)
	}
	if _, err := ResolveBaseURL(profile, "localhost:11434/v1"); err == nil || !strings.Contains(err.Error(), "http(s)") {
		t.Fatalf("ResolveBaseURL invalid error = %v, want http(s)", err)
	}
	if got, err := ResolveBaseURL(profile, " https://example.test/v1/ "); err != nil || got != "https://example.test/v1" {
		t.Fatalf("ResolveBaseURL = %q, %v; want normalized base url", got, err)
	}
	req := ModelResolution{
		Model: ModelAliasCheap,
		Models: ModelAliases{
			ModelAliasCheap:   {BackendOpenAICompatible: "gpt-mini"},
			ModelAliasRegular: {BackendOpenAICompatible: "gpt-main"},
		},
	}
	if got, err := ResolveModelName(profile, req); err != nil || got != "gpt-mini" {
		t.Fatalf("ResolveModelName low cost = %q, %v; want gpt-mini", got, err)
	}
	req.Model = ModelAliasRegular
	if got, err := ResolveModelName(profile, req); err != nil || got != "gpt-main" {
		t.Fatalf("ResolveModelName default = %q, %v; want gpt-main", got, err)
	}
	if _, err := ResolveModelName(profile, ModelResolution{}); err == nil || !strings.Contains(err.Error(), "model is required") {
		t.Fatalf("ResolveModelName missing error = %v, want model required", err)
	}
}

func TestResolveOpenAIResponsesProfileAuthority(t *testing.T) {
	profile, err := ResolveActiveBackend(BackendOpenAIResponses)
	if err != nil {
		t.Fatalf("ResolveActiveBackend: %v", err)
	}
	if profile.Provider != ProviderOpenAI || profile.RuntimeMode != ProviderContractRuntimeModeOpenAIResponses {
		t.Fatalf("profile = %#v, want provider openai runtime openai_responses", profile)
	}
	if profile.Credential.EnvVar != OpenAIResponsesCredentialEnv {
		t.Fatalf("credential env = %q, want %q", profile.Credential.EnvVar, OpenAIResponsesCredentialEnv)
	}
	if got, err := ResolveBaseURL(profile, ""); err != nil || got != OpenAIResponsesDefaultBaseURL {
		t.Fatalf("ResolveBaseURL default = %q, %v; want %s", got, err, OpenAIResponsesDefaultBaseURL)
	}
	if got, err := ResolveBaseURL(profile, " https://proxy.test/v1/ "); err != nil || got != "https://proxy.test/v1" {
		t.Fatalf("ResolveBaseURL override = %q, %v; want normalized base url", got, err)
	}
	for alias, want := range map[string]string{
		ModelAliasCheap:    "gpt-5.4-nano",
		ModelAliasRegular:  "gpt-5.4",
		ModelAliasFrontier: "gpt-5.5",
	} {
		got, err := ResolveModelName(profile, ModelResolution{Model: alias})
		if err != nil || got != want {
			t.Fatalf("ResolveModelName(%s) = %q, %v; want %s", alias, got, err, want)
		}
	}
	got, err := ResolveModelName(profile, ModelResolution{
		Model: ModelAliasRegular,
		Models: ModelAliases{
			ModelAliasRegular: {BackendOpenAIResponses: "gpt-test"},
		},
	})
	if err != nil || got != "gpt-test" {
		t.Fatalf("ResolveModelName override = %q, %v; want gpt-test", got, err)
	}
}

func TestResolveCLIModelNameUsesModel(t *testing.T) {
	profile, err := ResolveActiveBackend(BackendClaudeCLI)
	if err != nil {
		t.Fatalf("ResolveActiveBackend: %v", err)
	}
	if got, err := ResolveModelName(profile, ModelResolution{Model: ModelAliasRegular}); err != nil || got != "sonnet" {
		t.Fatalf("ResolveModelName = %q, %v; want sonnet", got, err)
	}
	if _, err := ResolveModelName(profile, ModelResolution{}); err == nil || !strings.Contains(err.Error(), "model is required") {
		t.Fatalf("ResolveModelName empty error = %v, want required model", err)
	}
}

func TestMigrateLegacyModelTier(t *testing.T) {
	tests := []struct {
		raw     string
		want    string
		changed bool
		wantErr bool
	}{
		{raw: "", changed: false},
		{raw: "haiku", want: ModelAliasCheap, changed: true},
		{raw: "low_cost", want: ModelAliasCheap, changed: true},
		{raw: "sonnet", want: ModelAliasRegular, changed: true},
		{raw: "general", want: ModelAliasRegular, changed: true},
		{raw: "generic", want: ModelAliasRegular, changed: true},
		{raw: "opus", changed: true, wantErr: true},
	}
	for _, tt := range tests {
		got, changed, err := MigrateLegacyModelTier(tt.raw)
		if tt.wantErr {
			if err == nil {
				t.Fatalf("MigrateLegacyModelTier(%q) err = nil, want error", tt.raw)
			}
			continue
		}
		if err != nil || got != tt.want || changed != tt.changed {
			t.Fatalf("MigrateLegacyModelTier(%q) = %q, %v, %v; want %q, %v, nil", tt.raw, got, changed, err, tt.want, tt.changed)
		}
	}
}
