package startupownership

import (
	"context"
	"errors"
	"maps"

	"github.com/division-sh/swarm/internal/config"
)

type agentRecoveryProfilePolicy struct {
	limits map[string]config.LLMProviderConcurrencyLimit
}

// BindAgentRecoveryProfilePolicy installs only an immutable configuration view.
// It grants no native claim, provider admission or right to reuse a seed.
func BindAgentRecoveryProfilePolicy(ctx context.Context, capability ProcessCapability, cfg *config.Config) error {
	p, ok := capability.(*processCapability)
	if !ok || p == nil || ctx == nil || cfg == nil {
		return errors.New("agent recovery policy requires its original process, context and complete config")
	}
	if err := errors.Join(ctx.Err(), p.requireLive()); err != nil {
		return err
	}
	candidate := *cfg
	candidate.LLM.ProviderLimits = maps.Clone(cfg.LLM.ProviderLimits)
	if err := candidate.Validate(); err != nil {
		return err
	}
	policy := &agentRecoveryProfilePolicy{limits: make(map[string]config.LLMProviderConcurrencyLimit, len(candidate.LLM.ProviderLimits))}
	for profile, declared := range candidate.LLM.ProviderLimits {
		limit, _, err := declared.ParseConcurrencyLimit()
		if err != nil {
			return err
		}
		policy.limits[profile] = limit
	}
	p.opMu.Lock()
	defer p.opMu.Unlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := errors.Join(ctx.Err(), p.requireLive()); err != nil {
		return err
	}
	if p.terminal.Cause != "" || p.agentRecoveryPolicy != nil {
		return errors.New("agent recovery policy requires an unbound live original process")
	}
	p.agentRecoveryPolicy = policy
	return nil
}
