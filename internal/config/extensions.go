package config

import (
	"strings"

	runtimesharding "empireai/internal/runtime/core/sharding"
)

type ExtensionsConfig struct {
	Budget   BudgetConfig         `yaml:"budget"`
	Sharding runtimesharding.Config `yaml:"sharding"`
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

func (c *ExtensionsConfig) ApplyDefaults() {
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
