package config

import (
	"strings"

	runtimesharding "empireai/internal/runtime/core/sharding"
)

type ExtensionsConfig struct {
	Budget   BudgetConfig           `yaml:"budget"`
	Sharding runtimesharding.Config `yaml:"sharding"`
}

type BudgetConfig struct {
	GlobalMonthlyCap      int              `yaml:"global_monthly_cap"`
	PerEntityMonthlyCap   int              `yaml:"per_entity_monthly_cap"`
	SystemMonthlyCap      int              `yaml:"system_monthly_cap"`
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
