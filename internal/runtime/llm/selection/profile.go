package selection

import (
	"fmt"
	"strings"
)

const (
	BackendAPI     = "api"
	BackendCLITest = "cli_test"
	BackendMock    = "mock"
	BackendLocal   = "local"

	ProviderAnthropic = "anthropic"
	ProviderClaude    = "claude"
	ProviderMock      = "mock"
	ProviderLocal     = "local"

	TransportAPI   = "api"
	TransportCLI   = "cli"
	TransportMock  = "mock"
	TransportLocal = "local"

	DefaultBackend = BackendAPI

	EnvBackend            = "SWARM_LLM_BACKEND"
	RetiredEnvRuntimeMode = "SWARM_LLM_RUNTIME_MODE"

	ConfigBackendField            = "llm.backend"
	RetiredConfigRuntimeModeField = "llm.runtime_mode"
)

type CredentialSource struct {
	EnvVar   string
	Required bool
	Purpose  string
}

type ModelMap struct {
	Default string
	Haiku   string
}

type Profile struct {
	ID           string
	Provider     string
	Transport    string
	RuntimeMode  string
	Credential   CredentialSource
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
	BackendAPI: {
		ID:          BackendAPI,
		Provider:    ProviderAnthropic,
		Transport:   TransportAPI,
		RuntimeMode: BackendAPI,
		Credential: CredentialSource{
			EnvVar:   "ANTHROPIC_API_KEY",
			Required: true,
			Purpose:  "anthropic api runtime",
		},
		Active: true,
	},
	BackendCLITest: {
		ID:          BackendCLITest,
		Provider:    ProviderClaude,
		Transport:   TransportCLI,
		RuntimeMode: BackendCLITest,
		Credential: CredentialSource{
			EnvVar:   "CLAUDE_CODE_OAUTH_TOKEN",
			Required: true,
			Purpose:  "claude cli runtime",
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

func RejectRetiredConfigRuntimeMode(raw string) error {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	return fmt.Errorf("%s is retired; use %s", RetiredConfigRuntimeModeField, ConfigBackendField)
}

func RejectRetiredEnvRuntimeMode(lookup EnvLookup) error {
	if lookup == nil {
		return nil
	}
	if _, ok := lookup(RetiredEnvRuntimeMode); ok {
		return fmt.Errorf("%s is retired; use %s", RetiredEnvRuntimeMode, EnvBackend)
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
	case BackendAPI:
		model := strings.TrimSpace(req.Models.Default)
		if req.ForceLowCost || tier == "haiku" {
			if haiku := strings.TrimSpace(req.Models.Haiku); haiku != "" {
				model = haiku
			}
		}
		if model == "" {
			return "", fmt.Errorf("llm.claude_api.default_model is required for backend %q", profile.ID)
		}
		return model, nil
	case BackendCLITest:
		return tier, nil
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
