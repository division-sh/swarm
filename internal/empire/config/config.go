package config

import (
	"strings"
	"time"

	runtimesharding "empireai/internal/runtime/core/sharding"
)

type EmpireConfig struct {
	Mailbox     MailboxConfig     `yaml:"mailbox"`
	FounderMode FounderModeConfig `yaml:"founder_mode"`
	Hetzner     HetznerConfig     `yaml:"hetzner"`
	Registrar   RegistrarConfig   `yaml:"registrar"`
	WhatsApp    WhatsAppConfig    `yaml:"whatsapp"`
	Budget      BudgetConfig      `yaml:"budget"`
	Sharding    runtimesharding.Config `yaml:"sharding"`
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

	c.Sharding.ApplyDefaults()
}
