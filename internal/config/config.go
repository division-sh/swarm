package config

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
	runtimesharding "swarm/internal/runtime/core/sharding"
	llmselection "swarm/internal/runtime/llm/selection"
)

// Config contains platform-generic runtime configuration.
type Config struct {
	Runtime    RuntimeConfig  `yaml:"runtime"`
	Database   DatabaseConfig `yaml:"database"`
	Store      StoreConfig    `yaml:"store"`
	LLM        LLMConfig      `yaml:"llm"`
	Extensions map[string]any `yaml:",inline"`

	typedExtensions ExtensionsConfig `yaml:"-"`
	extensionsReady bool             `yaml:"-"`
	extensionsErr   error            `yaml:"-"`
}

type RuntimeConfig struct {
	MaxConcurrentAgents int           `yaml:"max_concurrent_agents"`
	EventPollInterval   time.Duration `yaml:"event_poll_interval"`
	RecoveryOnStartup   bool          `yaml:"recovery_on_startup"`
}

type DatabaseConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Name     string `yaml:"name"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	SSLMode  string `yaml:"sslmode"`
	PoolSize int    `yaml:"pool_size"`
}

type StoreConfig struct {
	Backend string            `yaml:"backend"`
	SQLite  StoreSQLiteConfig `yaml:"sqlite"`
}

type StoreSQLiteConfig struct {
	Path string `yaml:"path"`
}

type LLMConfig struct {
	Backend          string                 `yaml:"backend"`
	RuntimeMode      string                 `yaml:"runtime_mode"`
	Session          LLMSessionConfig       `yaml:"session"`
	ClaudeAPI        ClaudeAPIConfig        `yaml:"claude_api"`
	ClaudeCLI        ClaudeCLIConfig        `yaml:"claude_cli"`
	OpenAICompatible OpenAICompatibleConfig `yaml:"openai_compatible"`
}

type LLMSessionConfig struct {
	LockTTL               time.Duration `yaml:"lock_ttl"`
	RotateAfterTurns      int           `yaml:"rotate_after_turns"`
	RotateOnParseFailures int           `yaml:"rotate_on_parse_failures"`
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

func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var cfg Config
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
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
	if err := c.ValidateOperationalControls(); err != nil {
		return err
	}
	profile, err := c.LLMBackendProfile()
	if err != nil {
		return err
	}
	if profile.ID == llmselection.BackendCLITest {
		if c.LLM.ClaudeCLI.Command == "" {
			return errors.New("llm.claude_cli.command is required in cli_test mode")
		}
		switch strings.TrimSpace(c.LLM.ClaudeCLI.OutputFormat) {
		case "json", "stream-json":
		default:
			return errors.New("llm.claude_cli.output_format must be json or stream-json in cli_test mode")
		}
		if c.LLM.ClaudeCLI.NoSessionPersistence {
			return errors.New("llm.claude_cli.no_session_persistence must be false for continuity")
		}
	}
	if profile.ID == llmselection.BackendOpenAICompatible {
		if _, err := llmselection.ResolveBaseURL(profile, c.LLM.OpenAICompatible.BaseURL); err != nil {
			return err
		}
		if _, err := llmselection.ResolveModelName(profile, llmselection.ModelResolution{
			Models: llmselection.ModelMap{
				Default: c.LLM.OpenAICompatible.DefaultModel,
				LowCost: c.LLM.OpenAICompatible.LowCostModel,
			},
		}); err != nil {
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
