package selection

import (
	"fmt"
	"strings"
)

const (
	BackendAnthropic        = "anthropic"
	BackendClaudeCLI        = "claude_cli"
	BackendOpenAICompatible = "openai_compatible"
	BackendOpenAIResponses  = "openai_responses"
	BackendMock             = "mock"
	BackendLocal            = "local"

	LegacyBackendAPI     = "api"
	LegacyBackendCLITest = "cli_test"

	ProviderAnthropic        = "anthropic"
	ProviderClaude           = "claude"
	ProviderOpenAICompatible = "openai_compatible"
	ProviderOpenAI           = "openai"
	ProviderMock             = "mock"
	ProviderLocal            = "local"

	TransportAPI   = "api"
	TransportCLI   = "cli"
	TransportMock  = "mock"
	TransportLocal = "local"

	ProviderContractRuntimeModeAPI             = "api"
	ProviderContractRuntimeModeClaudeCLI       = "cli_test"
	ProviderContractRuntimeModeOpenAIResponses = BackendOpenAIResponses

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

	OpenAIResponsesCredentialEnv      = "OPENAI_API_KEY"
	OpenAIResponsesBaseURLConfigField = "llm.openai_responses.base_url"
	OpenAIResponsesDefaultBaseURL     = "https://api.openai.com/v1"
)

type CredentialSource struct {
	EnvVar   string
	Required bool
	Purpose  string
}

type BaseURLSource struct {
	ConfigKey      string
	EnvVar         string
	Required       bool
	BuiltInDefault string
	Purpose        string
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

type ModelAliases map[string]map[string]string

type ModelResolution struct {
	Model  string
	Models ModelAliases
}

type ResolvedModel struct {
	ModelAlias    string
	ConcreteModel string
	Backend       string
	Provider      string
	Transport     string
	RuntimeMode   string
}

type EnvLookup func(string) (string, bool)

const (
	ModelAliasCheap    = "cheap"
	ModelAliasRegular  = "regular"
	ModelAliasFrontier = "frontier"

	ClaudeDefaultModelEnv = "SWARM_CLAUDE_DEFAULT_MODEL"
	ClaudeHaikuModelEnv   = "SWARM_CLAUDE_HAIKU_MODEL"

	ClaudeDefaultModelConfig = "llm.claude_api.default_model"
	ClaudeHaikuModelConfig   = "llm.claude_api.haiku_model"
)

var builtInModelAliases = ModelAliases{
	ModelAliasCheap: {
		BackendAnthropic:        "claude-3-5-haiku",
		BackendClaudeCLI:        "haiku",
		BackendOpenAICompatible: "gpt-compatible-mini",
		BackendOpenAIResponses:  "gpt-5.4-nano",
	},
	ModelAliasRegular: {
		BackendAnthropic:        "claude-3-5-sonnet",
		BackendClaudeCLI:        "sonnet",
		BackendOpenAICompatible: "gpt-compatible",
		BackendOpenAIResponses:  "gpt-5.4",
	},
	ModelAliasFrontier: {
		BackendAnthropic:        "claude-3-opus",
		BackendClaudeCLI:        "opus",
		BackendOpenAICompatible: "gpt-compatible-frontier",
		BackendOpenAIResponses:  "gpt-5.5",
	},
}

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
	BackendOpenAIResponses: {
		ID:          BackendOpenAIResponses,
		Provider:    ProviderOpenAI,
		Transport:   TransportAPI,
		RuntimeMode: ProviderContractRuntimeModeOpenAIResponses,
		Credential: CredentialSource{
			EnvVar:   OpenAIResponsesCredentialEnv,
			Required: true,
			Purpose:  "native OpenAI Responses runtime",
		},
		BaseURL: BaseURLSource{
			ConfigKey:      OpenAIResponsesBaseURLConfigField,
			Required:       false,
			BuiltInDefault: OpenAIResponsesDefaultBaseURL,
			Purpose:        "native OpenAI Responses endpoint base url",
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

func RejectRetiredModelEnv(lookup EnvLookup) error {
	if lookup == nil {
		return nil
	}
	for _, env := range []string{
		ClaudeDefaultModelEnv,
		ClaudeHaikuModelEnv,
		OpenAICompatibleDefaultModelEnv,
		OpenAICompatibleLowCostModelEnv,
	} {
		if _, ok := lookup(env); ok {
			return fmt.Errorf("%s is retired for model selection; use %s", env, "llm.models")
		}
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
		if strings.TrimSpace(profile.BaseURL.BuiltInDefault) != "" {
			return strings.TrimRight(strings.TrimSpace(profile.BaseURL.BuiltInDefault), "/"), nil
		}
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

func BuiltInModelAliases() ModelAliases {
	return cloneModelAliases(builtInModelAliases)
}

func EffectiveModelAliases(overrides ModelAliases) ModelAliases {
	models := BuiltInModelAliases()
	for alias, targets := range overrides {
		alias = NormalizeModelAlias(alias)
		if alias == "" {
			continue
		}
		if models[alias] == nil {
			models[alias] = map[string]string{}
		}
		for backend, model := range targets {
			backend = strings.TrimSpace(backend)
			model = strings.TrimSpace(model)
			if backend == "" || model == "" {
				continue
			}
			models[alias][backend] = model
		}
	}
	return models
}

func ValidateModelAliases(models ModelAliases) error {
	for alias, targets := range models {
		if _, err := RequireModelAlias(alias); err != nil {
			return err
		}
		if len(targets) == 0 {
			return fmt.Errorf("%s alias %q must declare at least one backend target", "llm.models", strings.TrimSpace(alias))
		}
		for backend, model := range targets {
			profile, err := ResolvePersistedBackend(backend)
			if err != nil {
				return fmt.Errorf("%s alias %q backend %q: %w", "llm.models", strings.TrimSpace(alias), strings.TrimSpace(backend), err)
			}
			if !profile.Active {
				return fmt.Errorf("%s alias %q targets reserved backend %q", "llm.models", strings.TrimSpace(alias), profile.ID)
			}
			if strings.TrimSpace(model) == "" {
				return fmt.Errorf("%s alias %q backend %q model is required", "llm.models", strings.TrimSpace(alias), profile.ID)
			}
		}
	}
	return nil
}

func ResolveModel(profile Profile, req ModelResolution) (ResolvedModel, error) {
	if !profile.Active {
		return ResolvedModel{}, fmt.Errorf("llm backend profile %q is not active", profile.ID)
	}
	alias, err := RequireModelAlias(req.Model)
	if err != nil {
		return ResolvedModel{}, err
	}
	models := EffectiveModelAliases(req.Models)
	if err := ValidateModelAliases(models); err != nil {
		return ResolvedModel{}, err
	}
	targets := models[alias]
	if len(targets) == 0 {
		return ResolvedModel{}, fmt.Errorf("%s alias %q is not configured", "llm.models", alias)
	}
	concrete := strings.TrimSpace(targets[profile.ID])
	if concrete == "" {
		return ResolvedModel{}, fmt.Errorf("%s alias %q does not resolve for backend %q", "llm.models", alias, profile.ID)
	}
	return ResolvedModel{
		ModelAlias:    alias,
		ConcreteModel: concrete,
		Backend:       profile.ID,
		Provider:      profile.Provider,
		Transport:     profile.Transport,
		RuntimeMode:   profile.RuntimeMode,
	}, nil
}

func ResolveModelName(profile Profile, req ModelResolution) (string, error) {
	resolved, err := ResolveModel(profile, req)
	if err != nil {
		return "", err
	}
	return resolved.ConcreteModel, nil
}

func RequireModelAlias(raw string) (string, error) {
	alias := NormalizeModelAlias(raw)
	if alias == "" {
		return "", fmt.Errorf("model is required; use one of %s or a configured llm.models alias", strings.Join(BuiltInModelAliasNames(), ", "))
	}
	if !wellFormedModelAlias(alias) {
		return "", fmt.Errorf("model alias %q is invalid; use letters, digits, '.', '_', ':', or '-'", alias)
	}
	return alias, nil
}

func NormalizeModelAlias(raw string) string {
	return strings.TrimSpace(raw)
}

func BuiltInModelAliasNames() []string {
	return []string{ModelAliasCheap, ModelAliasRegular, ModelAliasFrontier}
}

func MigrateLegacyModelTier(raw string) (string, bool, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "":
		return "", false, nil
	case "haiku", "low_cost":
		return ModelAliasCheap, true, nil
	case "sonnet", "general", "generic":
		return ModelAliasRegular, true, nil
	default:
		return "", true, fmt.Errorf("legacy model_tier %q cannot be migrated; use model alias cheap, regular, or frontier", strings.TrimSpace(raw))
	}
}

func cloneModelAliases(in ModelAliases) ModelAliases {
	out := make(ModelAliases, len(in))
	for alias, targets := range in {
		copied := make(map[string]string, len(targets))
		for backend, model := range targets {
			copied[backend] = model
		}
		out[alias] = copied
	}
	return out
}

func wellFormedModelAlias(alias string) bool {
	if alias == "" {
		return false
	}
	for _, r := range alias {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '.', r == '_', r == ':', r == '-':
		default:
			return false
		}
	}
	return true
}
