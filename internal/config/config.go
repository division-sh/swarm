package config

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	runtimesharding "github.com/division-sh/swarm/internal/runtime/core/sharding"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"gopkg.in/yaml.v3"
)

// Config contains platform-generic runtime configuration.
type Config struct {
	Runtime          RuntimeConfig          `yaml:"runtime"`
	Database         DatabaseConfig         `yaml:"database"`
	Store            StoreConfig            `yaml:"store"`
	Workspace        WorkspaceConfig        `yaml:"workspace"`
	LLM              LLMConfig              `yaml:"llm"`
	ProviderTriggers ProviderTriggersConfig `yaml:"provider_triggers"`
	Extensions       map[string]any         `yaml:",inline"`

	typedExtensions ExtensionsConfig `yaml:"-"`
	extensionsReady bool             `yaml:"-"`
	extensionsErr   error            `yaml:"-"`
}

type RuntimeConfig struct {
	MaxConcurrentAgents int           `yaml:"max_concurrent_agents"`
	EventPollInterval   time.Duration `yaml:"event_poll_interval"`
	RecoveryOnStartup   bool          `yaml:"recovery_on_startup"`
}

type ProviderTriggersConfig struct {
	Packs ProviderTriggerPacksConfig `yaml:"packs"`
}

type ProviderTriggerPacksConfig struct {
	PlatformDirs []string `yaml:"platform_dirs"`
	ExternalDirs []string `yaml:"external_dirs"`
}

type DatabaseConfig struct {
	Host              string `yaml:"host"`
	Port              int    `yaml:"port"`
	Name              string `yaml:"name"`
	User              string `yaml:"user"`
	Password          string `yaml:"password"`
	PasswordSecretKey string `yaml:"password_secret_key"`
	PasswordFile      string `yaml:"password_file"`
	PasswordEnv       string `yaml:"password_env"`
	SSLMode           string `yaml:"sslmode"`
	PoolSize          int    `yaml:"pool_size"`
}

type StoreConfig struct {
	Backend string            `yaml:"backend"`
	SQLite  StoreSQLiteConfig `yaml:"sqlite"`
}

type StoreSQLiteConfig struct {
	Path string `yaml:"path"`
}

type WorkspaceConfig struct {
	DataSource      string `yaml:"data_source"`
	Backend         string `yaml:"backend"`
	AllowExecOnHost bool   `yaml:"allow_exec_on_host"`
	Image           string `yaml:"image"`
	DockerBin       string `yaml:"docker_bin"`
	HostRoot        string `yaml:"host_root"`
	VolumesFrom     string `yaml:"volumes_from"`
	Network         string `yaml:"network"`

	dataSourceSet      bool
	backendSet         bool
	allowExecOnHostSet bool
	imageSet           bool
	dockerBinSet       bool
	hostRootSet        bool
	volumesFromSet     bool
	networkSet         bool
}

func (w *WorkspaceConfig) UnmarshalYAML(value *yaml.Node) error {
	type raw WorkspaceConfig
	var decoded raw
	if err := value.Decode(&decoded); err != nil {
		return err
	}
	*w = WorkspaceConfig(decoded)
	if value.Kind == yaml.MappingNode {
		for i := 0; i+1 < len(value.Content); i += 2 {
			switch value.Content[i].Value {
			case "data_source":
				w.dataSourceSet = true
			case "backend":
				w.backendSet = true
			case "allow_exec_on_host":
				w.allowExecOnHostSet = true
			case "image":
				w.imageSet = true
			case "docker_bin":
				w.dockerBinSet = true
			case "host_root":
				w.hostRootSet = true
			case "volumes_from":
				w.volumesFromSet = true
			case "network":
				w.networkSet = true
			}
		}
	}
	return nil
}

func (w WorkspaceConfig) DataSourceConfigured() bool {
	return w.dataSourceSet || strings.TrimSpace(w.DataSource) != ""
}

func (w WorkspaceConfig) BackendConfigured() bool {
	return w.backendSet || strings.TrimSpace(w.Backend) != ""
}

func (w WorkspaceConfig) AllowExecOnHostConfigured() bool {
	return w.allowExecOnHostSet || w.AllowExecOnHost
}

func (w WorkspaceConfig) ImageConfigured() bool {
	return w.imageSet || strings.TrimSpace(w.Image) != ""
}

func (w WorkspaceConfig) DockerBinConfigured() bool {
	return w.dockerBinSet || strings.TrimSpace(w.DockerBin) != ""
}

func (w WorkspaceConfig) HostRootConfigured() bool {
	return w.hostRootSet || strings.TrimSpace(w.HostRoot) != ""
}

func (w WorkspaceConfig) VolumesFromConfigured() bool {
	return w.volumesFromSet || strings.TrimSpace(w.VolumesFrom) != ""
}

func (w WorkspaceConfig) NetworkConfigured() bool {
	return w.networkSet || strings.TrimSpace(w.Network) != ""
}

type LLMConfig struct {
	Backend          string                            `yaml:"backend"`
	RuntimeMode      string                            `yaml:"runtime_mode"`
	Models           llmselection.ModelAliases         `yaml:"models"`
	Session          LLMSessionConfig                  `yaml:"session"`
	ProviderLimits   map[string]LLMProviderLimitPolicy `yaml:"provider_limits"`
	ClaudeAPI        ClaudeAPIConfig                   `yaml:"claude_api"`
	ClaudeCLI        ClaudeCLIConfig                   `yaml:"claude_cli"`
	OpenAICompatible OpenAICompatibleConfig            `yaml:"openai_compatible"`
	OpenAIResponses  OpenAIResponsesConfig             `yaml:"openai_responses"`
}

type LLMSessionConfig struct {
	LockTTL               time.Duration `yaml:"lock_ttl"`
	RotateAfterTurns      int           `yaml:"rotate_after_turns"`
	RotateOnParseFailures int           `yaml:"rotate_on_parse_failures"`
}

type LLMProviderLimitPolicy struct {
	RateLimit             string                            `yaml:"rate_limit"`
	RateLimitMaxWait      string                            `yaml:"rate_limit_max_wait"`
	MaxConcurrency        int                               `yaml:"max_concurrency"`
	MaxConcurrencyMaxWait string                            `yaml:"max_concurrency_max_wait"`
	Models                map[string]LLMProviderLimitPolicy `yaml:"models"`
}

type ClaudeAPIConfig struct {
	DefaultModel string        `yaml:"default_model"`
	HaikuModel   string        `yaml:"haiku_model"`
	MaxRetries   int           `yaml:"max_retries"`
	RetryBackoff time.Duration `yaml:"retry_backoff"`
}

type ClaudeCLIConfig struct {
	Command              string        `yaml:"command"`
	Timeout              time.Duration `yaml:"timeout"`
	OutputFormat         string        `yaml:"output_format"`
	Retries              int           `yaml:"retries"`
	NoSessionPersistence bool          `yaml:"no_session_persistence"`
	UseTMux              bool          `yaml:"use_tmux"`
}

type OpenAICompatibleConfig struct {
	BaseURL      string `yaml:"base_url"`
	DefaultModel string `yaml:"default_model"`
	LowCostModel string `yaml:"low_cost_model"`
}

type OpenAIResponsesConfig struct {
	BaseURL string `yaml:"base_url"`
}

type LoadOptions struct {
	BackendOverride string
}

func Load(path string) (*Config, error) {
	return LoadWithOptions(path, LoadOptions{})
}

func LoadWithOptions(path string, opts LoadOptions) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var cfg Config
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	backendOverride := strings.TrimSpace(opts.BackendOverride)
	if err := cfg.validate(backendOverride); err != nil {
		return nil, err
	}
	if backendOverride != "" {
		cfg.LLM.Backend = backendOverride
	}
	return &cfg, nil
}

func (c *Config) LLMBackendProfile() (llmselection.Profile, error) {
	if c == nil {
		return llmselection.Profile{}, errors.New("config is required")
	}
	if err := llmselection.RejectRetiredConfigRuntimeMode(c.LLM.RuntimeMode); err != nil {
		return llmselection.Profile{}, err
	}
	return llmselection.ResolveActiveBackend(c.LLM.Backend)
}

func (c *Config) Validate() error {
	return c.validate("")
}

func (c *Config) validate(backendOverride string) error {
	if err := c.ValidateOperationalControls(); err != nil {
		return err
	}
	if err := c.validateDatabasePasswordSource(); err != nil {
		return err
	}
	if err := c.validateProviderTriggerPacks(); err != nil {
		return err
	}
	if err := c.validateRetiredLLMModelConfig(); err != nil {
		return err
	}
	if err := llmselection.ValidateModelAliases(c.LLM.Models); err != nil {
		return err
	}
	if err := c.validateLLMProviderLimits(); err != nil {
		return err
	}
	profile, err := c.LLMBackendProfile()
	if err != nil {
		return err
	}
	if backendOverride = strings.TrimSpace(backendOverride); backendOverride != "" {
		profile, err = llmselection.ResolveActiveBackend(backendOverride)
		if err != nil {
			return err
		}
	}
	if profile.ID == llmselection.BackendClaudeCLI {
		if c.LLM.ClaudeCLI.Command == "" {
			return errors.New("llm.claude_cli.command is required for claude_cli backend")
		}
		switch strings.TrimSpace(c.LLM.ClaudeCLI.OutputFormat) {
		case "json", "stream-json":
		default:
			return errors.New("llm.claude_cli.output_format must be json or stream-json for claude_cli backend")
		}
		if c.LLM.ClaudeCLI.NoSessionPersistence {
			return errors.New("llm.claude_cli.no_session_persistence must be false for continuity")
		}
	}
	if profile.ID == llmselection.BackendOpenAICompatible {
		if _, err := llmselection.ResolveBaseURL(profile, c.LLM.OpenAICompatible.BaseURL); err != nil {
			return err
		}
	}
	if profile.ID == llmselection.BackendOpenAIResponses {
		if _, err := llmselection.ResolveBaseURL(profile, c.LLM.OpenAIResponses.BaseURL); err != nil {
			return err
		}
	}
	if c.LLM.Session.LockTTL <= 0 {
		return errors.New("llm.session.lock_ttl must be > 0")
	}
	if c.LLM.Session.RotateAfterTurns <= 0 {
		return errors.New("llm.session.rotate_after_turns must be > 0")
	}
	if c.LLM.Session.RotateOnParseFailures <= 0 {
		return errors.New("llm.session.rotate_on_parse_failures must be > 0")
	}
	return nil
}

func (c *Config) validateDatabasePasswordSource() error {
	if c == nil {
		return nil
	}
	return ValidateDatabasePasswordDeclaration(c.Database)
}

func (c *Config) validateProviderTriggerPacks() error {
	if c == nil {
		return nil
	}
	if err := validateProviderTriggerPackDirs("platform_dirs", c.ProviderTriggers.Packs.PlatformDirs); err != nil {
		return err
	}
	return validateProviderTriggerPackDirs("external_dirs", c.ProviderTriggers.Packs.ExternalDirs)
}

func validateProviderTriggerPackDirs(key string, dirs []string) error {
	seen := map[string]struct{}{}
	for i, dir := range dirs {
		dir = strings.TrimSpace(dir)
		if dir == "" {
			return fmt.Errorf("provider_triggers.packs.%s[%d] must be non-empty", key, i)
		}
		if _, exists := seen[dir]; exists {
			return fmt.Errorf("provider_triggers.packs.%s contains duplicate %q", key, dir)
		}
		seen[dir] = struct{}{}
	}
	return nil
}

func ValidateDatabasePasswordDeclaration(db DatabaseConfig) error {
	if strings.TrimSpace(db.Password) != "" {
		return errors.New("database.password is unsupported plaintext secret material; declare exactly one of database.password_secret_key, database.password_file, or database.password_env. SWARM_DB_PASSWORD and PGPASSWORD are not read unless explicitly named by database.password_env")
	}
	if count := DatabasePasswordSourceCount(db); count > 1 {
		return fmt.Errorf("ambiguous database password source: configure exactly one of database.password_secret_key, database.password_file, or database.password_env, got %s", strings.Join(DatabasePasswordSourceFields(db), ", "))
	}
	return nil
}

func ValidatePostgresDatabasePasswordSource(db DatabaseConfig) error {
	if err := ValidateDatabasePasswordDeclaration(db); err != nil {
		return err
	}
	if DatabasePasswordSourceCount(db) == 0 {
		return errors.New("postgres store requires exactly one database password source: database.password_secret_key, database.password_file, or database.password_env. database.password is unsupported; SWARM_DB_PASSWORD and PGPASSWORD are not read unless explicitly named by database.password_env")
	}
	return nil
}

func DatabasePasswordSourceCount(db DatabaseConfig) int {
	return len(DatabasePasswordSourceFields(db))
}

func DatabasePasswordSourceFields(db DatabaseConfig) []string {
	fields := make([]string, 0, 3)
	if strings.TrimSpace(db.PasswordSecretKey) != "" {
		fields = append(fields, "database.password_secret_key")
	}
	if strings.TrimSpace(db.PasswordFile) != "" {
		fields = append(fields, "database.password_file")
	}
	if strings.TrimSpace(db.PasswordEnv) != "" {
		fields = append(fields, "database.password_env")
	}
	return fields
}

func (c *Config) validateRetiredLLMModelConfig() error {
	retired := []struct {
		key   string
		value string
	}{
		{llmselection.ClaudeDefaultModelConfig, c.LLM.ClaudeAPI.DefaultModel},
		{llmselection.ClaudeHaikuModelConfig, c.LLM.ClaudeAPI.HaikuModel},
		{llmselection.OpenAICompatibleDefaultModelConfig, c.LLM.OpenAICompatible.DefaultModel},
		{llmselection.OpenAICompatibleLowCostModelConfig, c.LLM.OpenAICompatible.LowCostModel},
	}
	for _, item := range retired {
		if strings.TrimSpace(item.value) != "" {
			return fmt.Errorf("%s is retired for model selection; use llm.models", item.key)
		}
	}
	return nil
}

func (c *Config) ValidateOperationalControls() error {
	if c.Runtime.MaxConcurrentAgents != 0 {
		return errors.New("runtime.max_concurrent_agents is unsupported: no runtime path enforces it")
	}
	if c.Runtime.EventPollInterval != 0 {
		return errors.New("runtime.event_poll_interval is unsupported: no runtime path enforces it")
	}
	return c.ValidateExtensions()
}

func (c *Config) ValidateExtensions() error {
	if err := c.prepareTypedExtensions(); err != nil {
		return err
	}
	return nil
}

// Sharding returns the sharding extension config, or zero value if not configured.
func (c *Config) Sharding() runtimesharding.Config {
	if c == nil {
		return runtimesharding.Config{}
	}
	if err := c.prepareTypedExtensions(); err != nil {
		return runtimesharding.Config{}
	}
	return c.typedExtensions.Sharding
}

// Budget returns the budget extension config, or zero value if not configured.
func (c *Config) Budget() BudgetConfig {
	if c == nil {
		return BudgetConfig{}
	}
	if err := c.prepareTypedExtensions(); err != nil {
		return BudgetConfig{}
	}
	return c.typedExtensions.Budget
}

func (c *Config) DecodeExtensions(out any) error {
	if out == nil {
		return errors.New("extension target is required")
	}
	if len(c.Extensions) == 0 {
		return nil
	}
	raw, err := yaml.Marshal(c.Extensions)
	if err != nil {
		return fmt.Errorf("marshal extensions: %w", err)
	}
	if err := yaml.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("decode extensions: %w", err)
	}
	return nil
}

func (c *Config) prepareTypedExtensions() error {
	if c == nil {
		return nil
	}
	if c.extensionsReady {
		return nil
	}
	if c.extensionsErr != nil {
		return c.extensionsErr
	}
	var ext ExtensionsConfig
	if err := c.DecodeExtensions(&ext); err != nil {
		c.extensionsErr = err
		return err
	}
	if _, ok := c.Extensions["sharding"]; ok {
		c.extensionsErr = errors.New("sharding extension is unsupported: no runtime path consumes it")
		return c.extensionsErr
	}
	ext.ApplyDefaults()
	c.typedExtensions = ext
	c.extensionsReady = true
	return nil
}
