package selection

import (
	"fmt"
	"strings"
)

const (
	BackendAnthropic        = "anthropic"
	BackendClaudeCLI        = "claude_cli"
	BackendOpenAICompatible = "openai_compatible"
	BackendMock             = "mock"
	BackendLocal            = "local"

	LegacyBackendAPI     = "api"
	LegacyBackendCLITest = "cli_test"

	ProviderAnthropic        = "anthropic"
	ProviderClaude           = "claude"
	ProviderOpenAICompatible = "openai_compatible"
	ProviderMock             = "mock"
	ProviderLocal            = "local"

	TransportAPI   = "api"
	TransportCLI   = "cli"
	TransportMock  = "mock"
	TransportLocal = "local"

	ProviderContractRuntimeModeAPI       = "api"
	ProviderContractRuntimeModeClaudeCLI = "cli_test"

	DefaultBackend = BackendAnthropic

	EnvBackend            = "SWARM_LLM_BACKEND"
	RetiredEnvRuntimeMode = "SWARM_LLM_RUNTIME_MODE"

	ConfigBackendField            = "llm.backend"
	RetiredConfigRuntimeModeField = "llm.runtime_mode"

	OpenAICompatibleCredentialEnv      = "OPENAI_COMPATIBLE_API_KEY"
	OpenAICompatibleBaseURLEnv         = "SWARM_OPENAI_COMPATIBLE_BASE_URL"
	OpenAICompatibleDefaultModelEnv    = "SWARM_OPENAI_COMPATIBLE_DEFAULT_MODEL"
	OpenAICompatibleLowCostModelEnv    = "SWARM_OPENAI_COMPATIBLE_LOW_COST_MODEL"
	OpenAICompatibleBaseURLConfigField = "llm.openai_compatible.base_url"
	OpenAICompatibleDefaultModelConfig = "llm.openai_compatible.default_model"
	OpenAICompatibleLowCostModelConfig = "llm.openai_compatible.low_cost_model"
)

type CredentialSource struct {
	EnvVar   string
	Required bool
	Purpose  string
}

type BaseURLSource struct {
	ConfigKey string
	EnvVar    string
	Required  bool
	Purpose   string
}

type ModelMap struct {
	Default string
	LowCost string
}

type Profile struct {
	ID           string
	Provider     string
	Transport    string
	RuntimeMode  string
	Credential   CredentialSource
	BaseURL      BaseURLSource
	Active       bool
	ReservedNote string
}

type ModelResolution struct {
	ModelTier    string
	Models       ModelMap
	ForceLowCost bool
}

type EnvLookup func(string) (string, bool)

var profiles = map[string]Profile{
	BackendAnthropic: {
		ID:          BackendAnthropic,
		Provider:    ProviderAnthropic,
		Transport:   TransportAPI,
		RuntimeMode: ProviderContractRuntimeModeAPI,
		Credential: CredentialSource{
			EnvVar:   "ANTHROPIC_API_KEY",
			Required: true,
			Purpose:  "anthropic api runtime",
		},
		Active: true,
	},
	BackendClaudeCLI: {
		ID:          BackendClaudeCLI,
		Provider:    ProviderClaude,
		Transport:   TransportCLI,
		RuntimeMode: ProviderContractRuntimeModeClaudeCLI,
		Credential: CredentialSource{
			EnvVar:   "CLAUDE_CODE_OAUTH_TOKEN",
			Required: true,
			Purpose:  "claude cli runtime",
		},
		Active: true,
	},
	BackendOpenAICompatible: {
		ID:          BackendOpenAICompatible,
		Provider:    ProviderOpenAICompatible,
		Transport:   TransportAPI,
		RuntimeMode: BackendOpenAICompatible,
		Credential: CredentialSource{
			EnvVar:   OpenAICompatibleCredentialEnv,
			Required: true,
			Purpose:  "openai-compatible http runtime",
		},
		BaseURL: BaseURLSource{
			ConfigKey: OpenAICompatibleBaseURLConfigField,
			Required:  true,
			Purpose:   "openai-compatible chat completions endpoint base url",
		},
		Active: true,
	},
	BackendMock: {
		ID:           BackendMock,
		Provider:     ProviderMock,
		Transport:    TransportMock,
		RuntimeMode:  BackendMock,
		ReservedNote: "mock fixture replay backend is a persisted value only; no active runtime provider is shipped",
	},
	BackendLocal: {
		ID:           BackendLocal,
		Provider:     ProviderLocal,
		Transport:    TransportLocal,
		RuntimeMode:  BackendLocal,
		ReservedNote: "local model backend is a persisted value only; no active runtime provider is shipped",
	},
}

func NormalizeBackendID(raw string) string {
	return strings.TrimSpace(raw)
}

func DefaultBackendID() string {
	return DefaultBackend
}

func ResolveActiveBackend(raw string) (Profile, error) {
	profile, err := ResolvePersistedBackend(raw)
	if err != nil {
		return Profile{}, err
	}
	if !profile.Active {
		return Profile{}, fmt.Errorf("%s %q is reserved: %s", ConfigBackendField, profile.ID, profile.ReservedNote)
	}
	return profile, nil
}

func ResolvePersistedBackend(raw string) (Profile, error) {
	id := NormalizeBackendID(raw)
	if id == "" {
		id = DefaultBackend
	}
	profile, ok := profiles[id]
	if !ok {
		return Profile{}, fmt.Errorf("unsupported llm backend profile %q", id)
	}
	return profile, nil
}

func MigratePersistedBackend(raw string) (Profile, bool, error) {
	id := NormalizeBackendID(raw)
	if id == "" {
		id = DefaultBackend
	}
	switch id {
	case LegacyBackendAPI:
		profile, err := ResolvePersistedBackend(BackendAnthropic)
		return profile, true, err
	case LegacyBackendCLITest:
		profile, err := ResolvePersistedBackend(BackendClaudeCLI)
		return profile, true, err
	default:
		profile, err := ResolvePersistedBackend(id)
		return profile, false, err
	}
}

func RejectRetiredConfigRuntimeMode(raw string) error {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	return fmt.Errorf("%s is retired; use %s", RetiredConfigRuntimeModeField, ConfigBackendField)
}

func RejectRetiredEnvBackend(lookup EnvLookup) error {
	if lookup == nil {
		return nil
	}
	if _, ok := lookup(EnvBackend); ok {
		return fmt.Errorf("%s is retired and must not select the LLM backend; use --backend or %s", EnvBackend, ConfigBackendField)
	}
	return nil
}

func RejectRetiredEnvRuntimeMode(lookup EnvLookup) error {
	if lookup == nil {
		return nil
	}
	if _, ok := lookup(RetiredEnvRuntimeMode); ok {
		return fmt.Errorf("%s is retired; use --backend or %s", RetiredEnvRuntimeMode, ConfigBackendField)
	}
	return nil
}

func RejectRetiredOpenAICompatibleBaseURLEnv(lookup EnvLookup) error {
	if lookup == nil {
		return nil
	}
	if _, ok := lookup(OpenAICompatibleBaseURLEnv); ok {
		return fmt.Errorf("%s is retired; use %s", OpenAICompatibleBaseURLEnv, OpenAICompatibleBaseURLConfigField)
	}
	return nil
}

func CredentialValue(profile Profile, lookup EnvLookup) string {
	if lookup == nil || strings.TrimSpace(profile.Credential.EnvVar) == "" {
		return ""
	}
	value, _ := lookup(profile.Credential.EnvVar)
	return strings.TrimSpace(value)
}

func ResolveBaseURL(profile Profile, raw string) (string, error) {
	baseURL := strings.TrimSpace(raw)
	if baseURL == "" {
		if profile.BaseURL.Required {
			field := strings.TrimSpace(profile.BaseURL.ConfigKey)
			if field == "" {
				field = "llm backend base_url"
			}
			env := strings.TrimSpace(profile.BaseURL.EnvVar)
			if env != "" {
				return "", fmt.Errorf("%s is required for backend %q; set %s or %s", field, profile.ID, field, env)
			}
			return "", fmt.Errorf("%s is required for backend %q", field, profile.ID)
		}
		return "", nil
	}
	if !strings.HasPrefix(baseURL, "http://") && !strings.HasPrefix(baseURL, "https://") {
		return "", fmt.Errorf("%s must be an http(s) URL for backend %q", profile.BaseURL.ConfigKey, profile.ID)
	}
	return strings.TrimRight(baseURL, "/"), nil
}

func RequireCredential(profile Profile, lookup EnvLookup) error {
	if !profile.Credential.Required {
		return nil
	}
	if CredentialValue(profile, lookup) == "" {
		return fmt.Errorf("%s is required for %s", profile.Credential.EnvVar, profile.Credential.Purpose)
	}
	return nil
}

func ResolveModelName(profile Profile, req ModelResolution) (string, error) {
	tier := NormalizeModelTier(req.ModelTier)
	switch profile.ID {
	case BackendAnthropic:
		model := strings.TrimSpace(req.Models.Default)
		if req.ForceLowCost || tier == "haiku" {
			if lowCost := strings.TrimSpace(req.Models.LowCost); lowCost != "" {
				model = lowCost
			}
		}
		if model == "" {
			return "", fmt.Errorf("llm.claude_api.default_model is required for backend %q", profile.ID)
		}
		return model, nil
	case BackendClaudeCLI:
		return tier, nil
	case BackendOpenAICompatible:
		model := strings.TrimSpace(req.Models.Default)
		if req.ForceLowCost || tier == "low_cost" || tier == "haiku" {
			if lowCost := strings.TrimSpace(req.Models.LowCost); lowCost != "" {
				model = lowCost
			}
		}
		if model == "" {
			return "", fmt.Errorf("%s is required for backend %q", OpenAICompatibleDefaultModelConfig, profile.ID)
		}
		return model, nil
	default:
		return "", fmt.Errorf("llm backend profile %q does not support model resolution", profile.ID)
	}
}

func NormalizeModelTier(raw string) string {
	tier := strings.TrimSpace(raw)
	if tier == "" {
		return "generic"
	}
	return tier
}
