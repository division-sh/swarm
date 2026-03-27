package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	runtimesharding "swarm/internal/runtime/core/sharding"
)

func TestLoadAndValidate_CLI_TestMode(t *testing.T) {
	cfgText := strings.Join([]string{
		"runtime:",
		"  max_concurrent_agents: 10",
		"  event_poll_interval: 1s",
		"  recovery_on_startup: false",
		"database:",
		"  host: 127.0.0.1",
		"  port: 5432",
		"  name: swarm_test",
		"  user: postgres",
		"  password: postgres",
		"  sslmode: disable",
		"  pool_size: 5",
		"llm:",
		"  runtime_mode: cli_test",
		"  session:",
		"    lock_ttl: 10s",
		"    rotate_after_turns: 40",
		"    rotate_on_parse_failures: 3",
		"  claude_cli:",
		"    command: true",
		"    timeout: 2s",
		"    output_format: json",
		"    retries: 1",
		"    no_session_persistence: false",
		"    use_tmux: false",
		"budget:",
		"  global_monthly_cap: 50000",
		"  per_entity_monthly_cap: 20000",
		"  system_monthly_cap: 100000",
		"  human_tasks:",
		"    max_tasks_per_week: 3",
		"    budget_reset: ''",
		"    auto_expire_hours: 0",
		"    categories_enabled: [verification]",
	}, "\n") + "\n"
	p := filepath.Join(t.TempDir(), "swarm.yaml")
	if err := os.WriteFile(p, []byte(cfgText), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	cfg, err := Load(p)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.LLM.RuntimeMode != "cli_test" {
		t.Fatalf("unexpected runtime mode: %q", cfg.LLM.RuntimeMode)
	}
	if cfg.LLM.Session.LockTTL <= 0*time.Second {
		t.Fatalf("expected lock ttl > 0")
	}

	var ext ExtensionsConfig
	if err := cfg.DecodeExtensions(&ext); err != nil {
		t.Fatalf("DecodeExtensions: %v", err)
	}
	ext.ApplyDefaults()
	if ext.Budget.HumanTasks.AutoExpireHours != 168 {
		t.Fatalf("expected default auto_expire_hours 168, got %d", ext.Budget.HumanTasks.AutoExpireHours)
	}
	if ext.Budget.HumanTasks.BudgetReset != "monday" {
		t.Fatalf("expected default budget_reset monday, got %q", ext.Budget.HumanTasks.BudgetReset)
	}
	if ext.Sharding.MaxShardsPerJob != 8 {
		t.Fatalf("expected default sharding.max_shards_per_job=8, got %d", ext.Sharding.MaxShardsPerJob)
	}
	if ext.Sharding.Stages["default"].TargetItemsPerShard != 10 {
		t.Fatalf("expected default sharding.stages.default.target_items_per_shard=10, got %d", ext.Sharding.Stages["default"].TargetItemsPerShard)
	}
}

func TestValidate_RejectsInvalidRuntimeMode(t *testing.T) {
	c := &Config{}
	c.LLM.RuntimeMode = "bogus"
	c.LLM.Session.LockTTL = 1 * time.Second
	c.LLM.Session.RotateAfterTurns = 1
	c.LLM.Session.RotateOnParseFailures = 1
	if err := c.Validate(); err == nil {
		t.Fatal("expected error")
	}
}

func TestValidate_CLI_TestRequiresCommandAndJson(t *testing.T) {
	c := &Config{}
	c.LLM.RuntimeMode = "cli_test"
	c.LLM.Session.LockTTL = 1 * time.Second
	c.LLM.Session.RotateAfterTurns = 1
	c.LLM.Session.RotateOnParseFailures = 1
	c.LLM.ClaudeCLI.Command = ""
	c.LLM.ClaudeCLI.OutputFormat = "text"
	c.LLM.ClaudeCLI.NoSessionPersistence = true
	if err := c.Validate(); err == nil {
		t.Fatal("expected error for missing command")
	}
}

func TestExtensionsConfig_ApplyDefaultsAndClamps(t *testing.T) {
	c := &ExtensionsConfig{}
	c.Sharding.MaxShardsPerJob = 6
	c.Sharding.MaxConcurrentShards = 0
	c.Sharding.PerShardTimeout = 0
	c.Sharding.PerShardBudgetCents = 0
	c.Sharding.MaxRetriesPerShard = -1
	c.Sharding.CircuitBreakerThreshold = 2.0
	c.Sharding.Stages = map[string]runtimesharding.StageConfig{
		"default": {MaxShards: 99},
	}

	c.ApplyDefaults()
	if c.Sharding.MaxConcurrentShards != 12 {
		t.Fatalf("expected default max_concurrent_shards=12, got %d", c.Sharding.MaxConcurrentShards)
	}
	if c.Sharding.PerShardTimeout != 30*time.Minute {
		t.Fatalf("expected default per_shard_timeout=30m, got %s", c.Sharding.PerShardTimeout)
	}
	if c.Sharding.StartupGracePeriod != 20*time.Minute {
		t.Fatalf("expected default startup_grace_period=20m, got %s", c.Sharding.StartupGracePeriod)
	}
	if c.Sharding.PerShardBudgetCents != 50 {
		t.Fatalf("expected default per_shard_budget_cents=50, got %d", c.Sharding.PerShardBudgetCents)
	}
	if c.Sharding.MaxRetriesPerShard != 2 {
		t.Fatalf("expected default max_retries_per_shard=2, got %d", c.Sharding.MaxRetriesPerShard)
	}
	if c.Sharding.CircuitBreakerThreshold != 0.5 {
		t.Fatalf("expected default circuit_breaker_threshold=0.5, got %f", c.Sharding.CircuitBreakerThreshold)
	}
	if c.Sharding.Stages["default"].MaxShards != 6 {
		t.Fatalf("expected default.max_shards clamped to 6, got %d", c.Sharding.Stages["default"].MaxShards)
	}
}
