package startupownership

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
)

func agentRecoveryCapacityConfig() *config.Config {
	return &config.Config{LLM: config.LLMConfig{
		Backend:        "anthropic",
		Session:        config.LLMSessionConfig{LockTTL: time.Second, RotateAfterTurns: 1, RotateOnParseFailures: 1},
		ProviderLimits: map[string]config.LLMProviderLimitPolicy{"anthropic": {MaxConcurrency: 1}},
	}}
}

// These are process-owner UNIT controls, not evidence of fenced native claims.
func TestAgentRecoveryCapacityPolicyIsImmutableAndOnceBound(t *testing.T) {
	p, session, _ := newAgentRecoverySeedCapability(t)
	cfg := agentRecoveryCapacityConfig()
	seed, epoch := p.agentRecoverySeed, session.writes.epoch
	if err := BindAgentRecoveryProfilePolicy(t.Context(), p, cfg); err != nil {
		t.Fatal(err)
	}
	bound := p.agentRecoveryPolicy
	if bound == nil || !reflect.DeepEqual(bound.limits, map[string]config.LLMProviderConcurrencyLimit{
		"anthropic": {Enabled: true, Limit: 1},
	}) {
		t.Fatalf("complete configured policy was not retained: %+v", bound)
	}
	cfg.LLM.ProviderLimits["anthropic"] = config.LLMProviderLimitPolicy{MaxConcurrency: 200}
	cfg.LLM.ProviderLimits["claude_cli"] = config.LLMProviderLimitPolicy{MaxConcurrency: 300}
	delete(cfg.LLM.ProviderLimits, "anthropic")
	if got := bound.limits; !reflect.DeepEqual(got, map[string]config.LLMProviderConcurrencyLimit{
		"anthropic": {Enabled: true, Limit: 1},
	}) {
		t.Fatalf("bound policy retained a mutable config alias: %+v", got)
	}
	if err := BindAgentRecoveryProfilePolicy(t.Context(), p, cfg); err == nil || p.agentRecoveryPolicy != bound {
		t.Fatalf("policy could be rebound: %v", err)
	}
	if p.agentRecoverySeed != seed || session.writes.epoch != epoch || session.writes.active != 0 {
		t.Fatal("policy binding changed the seed or pretended to start a native write")
	}
}

func TestAgentRecoveryCapacityPolicyDistinguishesUnconfigured(t *testing.T) {
	for _, empty := range []string{"nil_map", "empty_map", "unconfigured_entry"} {
		t.Run(empty, func(t *testing.T) {
			p, _, _ := newAgentRecoverySeedCapability(t)
			if p.agentRecoveryPolicy != nil {
				t.Fatal("new process inferred policy initialization")
			}
			cfg := agentRecoveryCapacityConfig()
			switch empty {
			case "nil_map":
				cfg.LLM.ProviderLimits = nil
			case "empty_map":
				cfg.LLM.ProviderLimits = map[string]config.LLMProviderLimitPolicy{}
			case "unconfigured_entry":
				cfg.LLM.ProviderLimits["anthropic"] = config.LLMProviderLimitPolicy{}
			}
			if err := BindAgentRecoveryProfilePolicy(t.Context(), p, cfg); err != nil {
				t.Fatal(err)
			}
			if p.agentRecoveryPolicy == nil || p.agentRecoveryPolicy.limits == nil {
				t.Fatal("explicit complete unconfigured policy became missing initialization")
			}
			limit := p.agentRecoveryPolicy.limits["anthropic"]
			if limit.Enabled || limit.Limit != 0 || limit.MaxWait != 0 {
				t.Fatalf("unconfigured policy acquired an inferred numeric default: %+v", limit)
			}
		})
	}
}

func TestAgentRecoveryCapacityPolicyRefusesInvalidOrForeignBinding(t *testing.T) {
	for _, fault := range []string{"nil_context", "canceled", "nil_process", "wrapped_process", "retired", "nil_config",
		"profile", "profile_whitespace", "negative", "models", "rate", "rate_wait", "concurrency_wait", "incomplete_config"} {
		t.Run(fault, func(t *testing.T) {
			p, session, _ := newAgentRecoverySeedCapability(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			cfg := agentRecoveryCapacityConfig()
			var capability ProcessCapability = p
			switch fault {
			case "nil_context":
				ctx = nil
			case "canceled":
				cancel()
			case "nil_process":
				capability = nil
			case "wrapped_process":
				capability = struct{ ProcessCapability }{p}
			case "retired":
				if err := p.Release(t.Context()); err != nil {
					t.Fatal(err)
				}
			case "nil_config":
				cfg = nil
			case "profile":
				cfg.LLM.ProviderLimits["not-a-profile"] = config.LLMProviderLimitPolicy{MaxConcurrency: 1}
			case "profile_whitespace":
				cfg.LLM.ProviderLimits[" anthropic"] = config.LLMProviderLimitPolicy{MaxConcurrency: 1}
			case "negative":
				cfg.LLM.ProviderLimits["anthropic"] = config.LLMProviderLimitPolicy{MaxConcurrency: -1}
			case "models":
				cfg.LLM.ProviderLimits["anthropic"] = config.LLMProviderLimitPolicy{Models: map[string]config.LLMProviderLimitPolicy{}}
			case "rate":
				cfg.LLM.ProviderLimits["anthropic"] = config.LLMProviderLimitPolicy{RateLimit: "1/s"}
			case "rate_wait":
				cfg.LLM.ProviderLimits["anthropic"] = config.LLMProviderLimitPolicy{RateLimitMaxWait: "1s"}
			case "concurrency_wait":
				cfg.LLM.ProviderLimits["anthropic"] = config.LLMProviderLimitPolicy{MaxConcurrency: 1, MaxConcurrencyMaxWait: "1s"}
			case "incomplete_config":
				cfg.LLM.Session.LockTTL = 0
			}
			seed, epoch := p.agentRecoverySeed, session.writes.epoch
			if err := BindAgentRecoveryProfilePolicy(ctx, capability, cfg); err == nil || p.agentRecoveryPolicy != nil {
				t.Fatalf("invalid binding installed capacity policy: %v", err)
			}
			if p.agentRecoverySeed != seed || session.writes.epoch != epoch || session.writes.active != 0 {
				t.Fatal("refused policy binding changed the private seed/native observer")
			}
		})
	}
}
