package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	runtimesharding "github.com/division-sh/swarm/internal/runtime/core/sharding"
)

func TestLoadAndValidate_CLI_TestMode(t *testing.T) {
	cfgText := strings.Join([]string{
		"runtime:",
		"  recovery_on_startup: false",
		"database:",
		"  host: 127.0.0.1",
		"  port: 5432",
		"  name: swarm_test",
		"  user: postgres",
		"  sslmode: disable",
		"  pool_size: 5",
		"workspace:",
		"  data_source: ./reference-data",
		"llm:",
		"  backend: claude_cli",
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
	if cfg.LLM.Backend != "claude_cli" {
		t.Fatalf("unexpected llm backend: %q", cfg.LLM.Backend)
	}
	if cfg.LLM.Session.LockTTL <= 0*time.Second {
		t.Fatalf("expected lock ttl > 0")
	}
	if cfg.Workspace.DataSource != "./reference-data" {
		t.Fatalf("workspace.data_source = %q, want ./reference-data", cfg.Workspace.DataSource)
	}
	if !cfg.Workspace.DataSourceConfigured() {
		t.Fatal("workspace.data_source presence was not preserved")
	}
	if cfg.Workspace.Backend != "" {
		t.Fatalf("workspace.backend = %q, want empty", cfg.Workspace.Backend)
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

func TestValidateProviderTriggerExternalPackDirs(t *testing.T) {
	base := strings.Join([]string{
		"runtime:",
		"  recovery_on_startup: false",
		"workspace:",
		"  data_source: ./reference-data",
		"llm:",
		"  backend: anthropic",
		"  session:",
		"    lock_ttl: 10s",
		"    rotate_after_turns: 40",
		"    rotate_on_parse_failures: 3",
	}, "\n")

	t.Run("accepts declared external dirs", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "swarm.yaml")
		body := base + "\n" + strings.Join([]string{
			"provider_triggers:",
			"  packs:",
			"    external_dirs:",
			"      - ./packs/linear",
		}, "\n") + "\n"
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatalf("write config: %v", err)
		}
		cfg, err := Load(path)
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if got := cfg.ProviderTriggers.Packs.ExternalDirs; len(got) != 1 || got[0] != "./packs/linear" {
			t.Fatalf("external dirs = %#v, want declared relative path", got)
		}
	})

	t.Run("rejects empty entries", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "swarm.yaml")
		body := base + "\n" + strings.Join([]string{
			"provider_triggers:",
			"  packs:",
			"    external_dirs:",
			"      - ''",
		}, "\n") + "\n"
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatalf("write config: %v", err)
		}
		_, err := Load(path)
		if err == nil || !strings.Contains(err.Error(), "provider_triggers.packs.external_dirs[0] must be non-empty") {
			t.Fatalf("Load error = %v, want empty external dir rejection", err)
		}
	})

	t.Run("rejects duplicates", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "swarm.yaml")
		body := base + "\n" + strings.Join([]string{
			"provider_triggers:",
			"  packs:",
			"    external_dirs:",
			"      - ./packs/linear",
			"      - ./packs/linear",
		}, "\n") + "\n"
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatalf("write config: %v", err)
		}
		_, err := Load(path)
		if err == nil || !strings.Contains(err.Error(), `provider_triggers.packs.external_dirs contains duplicate "./packs/linear"`) {
			t.Fatalf("Load error = %v, want duplicate external dir rejection", err)
		}
	})
}

func TestValidate_RejectsPlaintextDatabasePassword(t *testing.T) {
	c := validDatabasePasswordConfig()
	c.Database.Password = "postgres"

	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "database.password is unsupported plaintext secret material") {
		t.Fatalf("Validate error = %v, want plaintext database password rejection", err)
	}
}

func TestValidate_RejectsMultipleDatabasePasswordSources(t *testing.T) {
	c := validDatabasePasswordConfig()
	c.Database.PasswordSecretKey = "postgres_password"
	c.Database.PasswordFile = "/run/secrets/db-password"

	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "ambiguous database password source") {
		t.Fatalf("Validate error = %v, want ambiguous database password source rejection", err)
	}
}

func TestValidate_DoesNotRequirePostgresPasswordBeforeBackendSelection(t *testing.T) {
	c := validDatabasePasswordConfig()
	c.Store.Backend = "postgres"

	if err := c.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestValidatePostgresDatabasePasswordSourceRequiresSource(t *testing.T) {
	err := ValidatePostgresDatabasePasswordSource(DatabaseConfig{})
	if err == nil || !strings.Contains(err.Error(), "postgres store requires exactly one database password source") {
		t.Fatalf("ValidatePostgresDatabasePasswordSource error = %v, want missing postgres password source rejection", err)
	}
}

func TestValidatePostgresDatabasePasswordSourceAcceptsExplicitSource(t *testing.T) {
	c := validDatabasePasswordConfig()
	c.Database.PasswordFile = "/run/secrets/db-password"
	if err := ValidatePostgresDatabasePasswordSource(c.Database); err != nil {
		t.Fatalf("ValidatePostgresDatabasePasswordSource with password_file: %v", err)
	}
}

func validDatabasePasswordConfig() *Config {
	return &Config{
		LLM: LLMConfig{
			Backend: "claude_cli",
			Session: LLMSessionConfig{
				LockTTL:               time.Second,
				RotateAfterTurns:      1,
				RotateOnParseFailures: 1,
			},
			ClaudeCLI: ClaudeCLIConfig{
				Command:              "true",
				OutputFormat:         "json",
				NoSessionPersistence: false,
			},
		},
	}
}

func TestLoad_PreservesEmptyWorkspaceDataSourcePresence(t *testing.T) {
	cfgText := strings.Join([]string{
		"runtime:",
		"  recovery_on_startup: false",
		"workspace:",
		"  data_source: \"   \"",
		"llm:",
		"  backend: anthropic",
		"  session:",
		"    lock_ttl: 10s",
		"    rotate_after_turns: 40",
		"    rotate_on_parse_failures: 3",
	}, "\n") + "\n"
	p := filepath.Join(t.TempDir(), "swarm.yaml")
	if err := os.WriteFile(p, []byte(cfgText), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	cfg, err := Load(p)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !cfg.Workspace.DataSourceConfigured() {
		t.Fatal("empty workspace.data_source presence was not preserved")
	}
	if cfg.Workspace.DataSource != "   " {
		t.Fatalf("workspace.data_source = %q, want preserved whitespace", cfg.Workspace.DataSource)
	}
}

func TestLoad_PreservesWorkspaceBackendPresence(t *testing.T) {
	cfgText := strings.Join([]string{
		"runtime:",
		"  recovery_on_startup: false",
		"workspace:",
		"  backend: \"   \"",
		"llm:",
		"  backend: anthropic",
		"  session:",
		"    lock_ttl: 10s",
		"    rotate_after_turns: 40",
		"    rotate_on_parse_failures: 3",
	}, "\n") + "\n"
	p := filepath.Join(t.TempDir(), "swarm.yaml")
	if err := os.WriteFile(p, []byte(cfgText), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	cfg, err := Load(p)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !cfg.Workspace.BackendConfigured() {
		t.Fatal("empty workspace.backend presence was not preserved")
	}
	if cfg.Workspace.Backend != "   " {
		t.Fatalf("workspace.backend = %q, want preserved whitespace", cfg.Workspace.Backend)
	}
}

func TestValidate_RejectsInvalidBackend(t *testing.T) {
	c := &Config{}
	c.LLM.Backend = "bogus"
	c.LLM.Session.LockTTL = 1 * time.Second
	c.LLM.Session.RotateAfterTurns = 1
	c.LLM.Session.RotateOnParseFailures = 1
	if err := c.Validate(); err == nil {
		t.Fatal("expected error")
	}
}

func TestValidate_RejectsLegacyBackendIDsForNewConfig(t *testing.T) {
	for _, backend := range []string{"api", "cli_test"} {
		t.Run(backend, func(t *testing.T) {
			c := &Config{}
			c.LLM.Backend = backend
			c.LLM.Session.LockTTL = time.Second
			c.LLM.Session.RotateAfterTurns = 1
			c.LLM.Session.RotateOnParseFailures = 1
			if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "unsupported llm backend profile") {
				t.Fatalf("Validate error = %v, want legacy backend rejection", err)
			}
		})
	}
}

func TestValidate_RejectsReservedActiveBackend(t *testing.T) {
	c := &Config{}
	c.LLM.Backend = "local"
	c.LLM.Session.LockTTL = 1 * time.Second
	c.LLM.Session.RotateAfterTurns = 1
	c.LLM.Session.RotateOnParseFailures = 1
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("Validate error = %v, want reserved backend rejection", err)
	}
}

func TestValidate_OpenAICompatibleRequiresProfileOwnedConfig(t *testing.T) {
	c := &Config{}
	c.LLM.Backend = "openai_compatible"
	c.LLM.Session.LockTTL = 1 * time.Second
	c.LLM.Session.RotateAfterTurns = 1
	c.LLM.Session.RotateOnParseFailures = 1
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "llm.openai_compatible.base_url") {
		t.Fatalf("Validate error = %v, want openai-compatible base url requirement", err)
	}
	c.LLM.OpenAICompatible.BaseURL = "https://example.test/v1"
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	c.LLM.OpenAICompatible.DefaultModel = "gpt-compatible"
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "llm.models") {
		t.Fatalf("Validate error = %v, want retired model config guidance", err)
	}
}

func TestValidate_OpenAIResponsesUsesProfileOwnedDefaultAndOverride(t *testing.T) {
	c := &Config{}
	c.LLM.Backend = "openai_responses"
	c.LLM.Session.LockTTL = 1 * time.Second
	c.LLM.Session.RotateAfterTurns = 1
	c.LLM.Session.RotateOnParseFailures = 1
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate with built-in OpenAI Responses base URL: %v", err)
	}
	c.LLM.OpenAIResponses.BaseURL = "localhost:8080"
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "llm.openai_responses.base_url") {
		t.Fatalf("Validate invalid base_url error = %v, want openai_responses base URL guidance", err)
	}
	c.LLM.OpenAIResponses.BaseURL = "https://proxy.test/v1"
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate with OpenAI Responses override: %v", err)
	}
}

func TestValidate_RejectsRetiredRuntimeMode(t *testing.T) {
	c := &Config{}
	c.LLM.RuntimeMode = "api"
	c.LLM.Session.LockTTL = 1 * time.Second
	c.LLM.Session.RotateAfterTurns = 1
	c.LLM.Session.RotateOnParseFailures = 1
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "llm.backend") {
		t.Fatalf("Validate error = %v, want retired runtime_mode guidance", err)
	}
}

func TestValidate_CLI_TestRequiresCommandAndJson(t *testing.T) {
	c := &Config{}
	c.LLM.Backend = "claude_cli"
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

func TestLoad_FailsClosedOnMalformedBudgetExtension(t *testing.T) {
	cfgText := strings.Join([]string{
		"llm:",
		"  backend: anthropic",
		"  session:",
		"    lock_ttl: 10s",
		"    rotate_after_turns: 40",
		"    rotate_on_parse_failures: 3",
		"budget:",
		"  human_tasks: oops",
	}, "\n") + "\n"
	p := filepath.Join(t.TempDir(), "swarm.yaml")
	if err := os.WriteFile(p, []byte(cfgText), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := Load(p); err == nil || !strings.Contains(err.Error(), "decode extensions") {
		t.Fatalf("Load error = %v, want decode extensions failure", err)
	}
}

func TestValidate_RejectsUnsupportedRuntimeControls(t *testing.T) {
	c := &Config{}
	c.LLM.Backend = "anthropic"
	c.LLM.Session.LockTTL = 1 * time.Second
	c.LLM.Session.RotateAfterTurns = 1
	c.LLM.Session.RotateOnParseFailures = 1
	c.Runtime.MaxConcurrentAgents = 2
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "runtime.max_concurrent_agents") {
		t.Fatalf("Validate error = %v, want unsupported max_concurrent_agents", err)
	}

	c.Runtime.MaxConcurrentAgents = 0
	c.Runtime.EventPollInterval = time.Second
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "runtime.event_poll_interval") {
		t.Fatalf("Validate error = %v, want unsupported event_poll_interval", err)
	}
}

func TestValidateLLMProviderLimits(t *testing.T) {
	base := func() *Config {
		return &Config{
			LLM: LLMConfig{
				Backend: "anthropic",
				Session: LLMSessionConfig{
					LockTTL:               time.Second,
					RotateAfterTurns:      1,
					RotateOnParseFailures: 1,
				},
			},
		}
	}
	tests := []struct {
		name    string
		limits  map[string]LLMProviderLimitPolicy
		wantErr string
	}{
		{
			name: "valid profile and model limits",
			limits: map[string]LLMProviderLimitPolicy{
				"anthropic": {
					RateLimit:             "10/s",
					RateLimitMaxWait:      "1s",
					MaxConcurrency:        2,
					MaxConcurrencyMaxWait: "0s",
					Models: map[string]LLMProviderLimitPolicy{
						"regular": {
							RateLimit:        "20/m",
							RateLimitMaxWait: "5s",
						},
					},
				},
			},
		},
		{
			name: "default no limit",
		},
		{
			name: "missing rate wait",
			limits: map[string]LLMProviderLimitPolicy{
				"anthropic": {RateLimit: "1/s"},
			},
			wantErr: "rate_limit requires rate_limit_max_wait",
		},
		{
			name: "missing concurrency wait",
			limits: map[string]LLMProviderLimitPolicy{
				"anthropic": {MaxConcurrency: 1},
			},
			wantErr: "max_concurrency requires max_concurrency_max_wait",
		},
		{
			name: "reserved profile rejected",
			limits: map[string]LLMProviderLimitPolicy{
				"mock": {RateLimit: "1/s", RateLimitMaxWait: "1s"},
			},
			wantErr: "reserved",
		},
		{
			name: "profile key whitespace rejected",
			limits: map[string]LLMProviderLimitPolicy{
				"anthropic ": {RateLimit: "1/s", RateLimitMaxWait: "1s"},
			},
			wantErr: "profile key must not contain leading or trailing whitespace",
		},
		{
			name: "model key whitespace rejected",
			limits: map[string]LLMProviderLimitPolicy{
				"anthropic": {
					Models: map[string]LLMProviderLimitPolicy{
						"regular ": {RateLimit: "1/s", RateLimitMaxWait: "1s"},
					},
				},
			},
			wantErr: "model key must not contain leading or trailing whitespace",
		},
		{
			name: "model nested models rejected",
			limits: map[string]LLMProviderLimitPolicy{
				"anthropic": {
					Models: map[string]LLMProviderLimitPolicy{
						"regular": {Models: map[string]LLMProviderLimitPolicy{"cheap": {}}},
					},
				},
			},
			wantErr: "nested model limits are not supported",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base()
			cfg.LLM.ProviderLimits = tc.limits
			err := cfg.Validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("Validate: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("Validate error = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

func TestLoad_RejectsUnsupportedShardingExtension(t *testing.T) {
	cfgText := strings.Join([]string{
		"llm:",
		"  backend: anthropic",
		"  session:",
		"    lock_ttl: 10s",
		"    rotate_after_turns: 40",
		"    rotate_on_parse_failures: 3",
		"sharding:",
		"  max_shards_per_job: 4",
	}, "\n") + "\n"
	p := filepath.Join(t.TempDir(), "swarm.yaml")
	if err := os.WriteFile(p, []byte(cfgText), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := Load(p); err == nil || !strings.Contains(err.Error(), "sharding extension is unsupported") {
		t.Fatalf("Load error = %v, want unsupported sharding extension", err)
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
