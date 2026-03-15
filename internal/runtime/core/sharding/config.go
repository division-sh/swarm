package sharding

import "time"

type Config struct {
	MaxShardsPerScan        int                    `yaml:"max_shards_per_scan"`
	MaxConcurrentShards     int                    `yaml:"max_concurrent_shards"`
	PerShardTimeout         time.Duration          `yaml:"per_shard_timeout"`
	StartupGracePeriod      time.Duration          `yaml:"startup_grace_period"`
	PerShardBudgetCents     int                    `yaml:"per_shard_budget_cents"`
	MaxRetriesPerShard      int                    `yaml:"max_retries_per_shard"`
	CircuitBreakerThreshold float64                `yaml:"circuit_breaker_threshold"`
	Stages                  map[string]StageConfig `yaml:"stages"`
}

type StageConfig struct {
	TargetItemsPerShard int `yaml:"target_items_per_shard"`
	MaxShards           int `yaml:"max_shards"`
}

func (c *Config) ApplyDefaults() {
	if c == nil {
		return
	}
	if c.MaxShardsPerScan <= 0 {
		c.MaxShardsPerScan = 8
	}
	if c.MaxConcurrentShards <= 0 {
		c.MaxConcurrentShards = 12
	}
	if c.PerShardTimeout <= 0 {
		c.PerShardTimeout = 30 * time.Minute
	}
	if c.StartupGracePeriod <= 0 {
		c.StartupGracePeriod = 20 * time.Minute
	}
	if c.PerShardBudgetCents <= 0 {
		c.PerShardBudgetCents = 50
	}
	if c.MaxRetriesPerShard <= 0 {
		c.MaxRetriesPerShard = 2
	}
	if c.CircuitBreakerThreshold <= 0 || c.CircuitBreakerThreshold > 1 {
		c.CircuitBreakerThreshold = 0.5
	}

	normalizeStage := func(stage *StageConfig, targetItems, maxShards int) {
		if stage.TargetItemsPerShard <= 0 {
			stage.TargetItemsPerShard = targetItems
		}
		if stage.MaxShards <= 0 {
			stage.MaxShards = maxShards
		}
		if stage.MaxShards > c.MaxShardsPerScan {
			stage.MaxShards = c.MaxShardsPerScan
		}
	}
	if c.Stages == nil {
		c.Stages = map[string]StageConfig{}
	}
	defaultStages := map[string]StageConfig{
		"primary":   {TargetItemsPerShard: 13, MaxShards: 8},
		"secondary": {TargetItemsPerShard: 3, MaxShards: 4},
	}
	for name, defaults := range defaultStages {
		stage := c.Stages[name]
		normalizeStage(&stage, defaults.TargetItemsPerShard, defaults.MaxShards)
		c.Stages[name] = stage
	}
}
