package config

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config mirrors the runtime-relevant subset of v1.7 empire.yaml.
type Config struct {
	Runtime     RuntimeConfig     `yaml:"runtime"`
	Database    DatabaseConfig    `yaml:"database"`
	LLM         LLMConfig         `yaml:"llm"`
	Mailbox     MailboxConfig     `yaml:"mailbox"`
	FounderMode FounderModeConfig `yaml:"founder_mode"`
	Hetzner     HetznerConfig     `yaml:"hetzner"`
	Registrar   RegistrarConfig   `yaml:"registrar"`
	WhatsApp    WhatsAppConfig    `yaml:"whatsapp"`
	Budget      BudgetConfig      `yaml:"budget"`
	Sharding    ShardingConfig    `yaml:"sharding"`
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

type LLMConfig struct {
	RuntimeMode string           `yaml:"runtime_mode"`
	Session     LLMSessionConfig `yaml:"session"`
	ClaudeAPI   ClaudeAPIConfig  `yaml:"claude_api"`
	ClaudeCLI   ClaudeCLIConfig  `yaml:"claude_cli"`
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

type MailboxConfig struct {
	PollInterval      time.Duration `yaml:"poll_interval"`
	StaleThreshold    time.Duration `yaml:"stale_threshold"`
	DigestMaxInterval time.Duration `yaml:"digest_max_interval"`
	DigestOnCEOReport bool          `yaml:"digest_on_ceo_report"`
}

type FounderModeConfig struct {
	SpecReview          bool          `yaml:"spec_review"`
	DeployReview        bool          `yaml:"deploy_review"`
	ReviewTimeout       time.Duration `yaml:"review_timeout"`
	FounderInput        bool          `yaml:"founder_input"`
	FounderInputTimeout time.Duration `yaml:"founder_input_timeout"`
}

type HetznerConfig struct {
	Host                 string `yaml:"host"`
	SSHKey               string `yaml:"ssh_key"`
	BaseDomain           string `yaml:"base_domain"`
	VerticalsDir         string `yaml:"verticals_dir"`
	PortRangeStart       int    `yaml:"port_range_start"`
	StagingPortRangeFrom int    `yaml:"staging_port_range_start"`
	GatewayPort          int    `yaml:"gateway_port"`
}

type RegistrarConfig struct {
	Provider string `yaml:"provider"`
	APIKey   string `yaml:"api_key"`
}

type WhatsAppConfig struct {
	Provider string `yaml:"provider"`
	APIKey   string `yaml:"api_key"`
}

type BudgetConfig struct {
	FactoryMonthlyCap     int              `yaml:"factory_monthly_cap"`
	PerVerticalMonthlyCap int              `yaml:"per_vertical_monthly_cap"`
	PortfolioMonthlyCap   int              `yaml:"portfolio_monthly_cap"`
	AutoApproveSpendBelow int              `yaml:"auto_approve_spend_below"`
	HumanTasks            HumanTasksConfig `yaml:"human_tasks"`
}

type HumanTasksConfig struct {
	MaxTasksPerWeek   int      `yaml:"max_tasks_per_week"`
	BudgetReset       string   `yaml:"budget_reset"`
	AutoExpireHours   int      `yaml:"auto_expire_hours"`
	CategoriesEnabled []string `yaml:"categories_enabled"`
}

type ShardingConfig struct {
	MaxShardsPerScan        int                  `yaml:"max_shards_per_scan"`
	MaxConcurrentShards     int                  `yaml:"max_concurrent_shards"`
	PerShardTimeout         time.Duration        `yaml:"per_shard_timeout"`
	PerShardBudgetCents     int                  `yaml:"per_shard_budget_cents"`
	MaxRetriesPerShard      int                  `yaml:"max_retries_per_shard"`
	CircuitBreakerThreshold float64              `yaml:"circuit_breaker_threshold"`
	Stages                  ShardingStagesConfig `yaml:"stages"`
}

type ShardingStagesConfig struct {
	MarketResearch ShardingStageConfig `yaml:"market_research"`
	TrendResearch  ShardingStageConfig `yaml:"trend_research"`
}

type ShardingStageConfig struct {
	TargetItemsPerShard int `yaml:"target_items_per_shard"`
	MaxShards           int `yaml:"max_shards"`
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

func (c *Config) Validate() error {
	if c.LLM.RuntimeMode != "api" && c.LLM.RuntimeMode != "cli_test" {
		return fmt.Errorf("invalid llm.runtime_mode: %q", c.LLM.RuntimeMode)
	}
	if c.LLM.RuntimeMode == "cli_test" {
		if c.LLM.ClaudeCLI.Command == "" {
			return errors.New("llm.claude_cli.command is required in cli_test mode")
		}
		if c.LLM.ClaudeCLI.OutputFormat != "json" {
			return errors.New("llm.claude_cli.output_format must be json in cli_test mode")
		}
		if c.LLM.ClaudeCLI.NoSessionPersistence {
			return errors.New("llm.claude_cli.no_session_persistence must be false for continuity")
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

	// Defaults for v2.0 human task system.
	if c.Budget.HumanTasks.AutoExpireHours <= 0 {
		c.Budget.HumanTasks.AutoExpireHours = 168
	}
	if strings.TrimSpace(c.Budget.HumanTasks.BudgetReset) == "" {
		c.Budget.HumanTasks.BudgetReset = "monday"
	}

	// Defaults for v2.0.16 sharded execution framework.
	c.applyShardingDefaults()
	return nil
}

func (c *Config) applyShardingDefaults() {
	if c.Sharding.MaxShardsPerScan <= 0 {
		c.Sharding.MaxShardsPerScan = 8
	}
	if c.Sharding.MaxConcurrentShards <= 0 {
		c.Sharding.MaxConcurrentShards = 12
	}
	if c.Sharding.PerShardTimeout <= 0 {
		c.Sharding.PerShardTimeout = 30 * time.Minute
	}
	if c.Sharding.PerShardBudgetCents <= 0 {
		c.Sharding.PerShardBudgetCents = 50
	}
	if c.Sharding.MaxRetriesPerShard <= 0 {
		c.Sharding.MaxRetriesPerShard = 2
	}
	if c.Sharding.CircuitBreakerThreshold <= 0 || c.Sharding.CircuitBreakerThreshold > 1 {
		c.Sharding.CircuitBreakerThreshold = 0.5
	}

	normalizeStage := func(stage *ShardingStageConfig, targetItems, maxShards int) {
		if stage.TargetItemsPerShard <= 0 {
			stage.TargetItemsPerShard = targetItems
		}
		if stage.MaxShards <= 0 {
			stage.MaxShards = maxShards
		}
		if stage.MaxShards > c.Sharding.MaxShardsPerScan {
			stage.MaxShards = c.Sharding.MaxShardsPerScan
		}
	}
	normalizeStage(&c.Sharding.Stages.MarketResearch, 13, 8)
	normalizeStage(&c.Sharding.Stages.TrendResearch, 3, 4)
}
