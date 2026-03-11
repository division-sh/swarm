package config

import (
	"strings"
	"time"
)

type EmpireConfig struct {
	Mailbox     MailboxConfig     `yaml:"mailbox"`
	FounderMode FounderModeConfig `yaml:"founder_mode"`
	Hetzner     HetznerConfig     `yaml:"hetzner"`
	Registrar   RegistrarConfig   `yaml:"registrar"`
	WhatsApp    WhatsAppConfig    `yaml:"whatsapp"`
	Budget      BudgetConfig      `yaml:"budget"`
	Sharding    ShardingConfig    `yaml:"sharding"`
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
	StartupGracePeriod      time.Duration        `yaml:"startup_grace_period"`
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

func (c *EmpireConfig) ApplyDefaults() {
	if c == nil {
		return
	}

	if c.Budget.HumanTasks.AutoExpireHours <= 0 {
		c.Budget.HumanTasks.AutoExpireHours = 168
	}
	if strings.TrimSpace(c.Budget.HumanTasks.BudgetReset) == "" {
		c.Budget.HumanTasks.BudgetReset = "monday"
	}

	c.applyShardingDefaults()
}

func (c *EmpireConfig) applyShardingDefaults() {
	if c.Sharding.MaxShardsPerScan <= 0 {
		c.Sharding.MaxShardsPerScan = 8
	}
	if c.Sharding.MaxConcurrentShards <= 0 {
		c.Sharding.MaxConcurrentShards = 12
	}
	if c.Sharding.PerShardTimeout <= 0 {
		c.Sharding.PerShardTimeout = 30 * time.Minute
	}
	if c.Sharding.StartupGracePeriod <= 0 {
		c.Sharding.StartupGracePeriod = 20 * time.Minute
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
