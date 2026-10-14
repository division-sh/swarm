package config

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
)

type LLMProviderRateLimit struct {
	Enabled bool
	Limit   int
	Period  time.Duration
	MaxWait time.Duration
}

type LLMProviderConcurrencyLimit struct {
	Enabled bool
	Limit   int
	MaxWait time.Duration
}

func (c *Config) validateLLMProviderLimits() error {
	if c == nil || len(c.LLM.ProviderLimits) == 0 {
		return nil
	}
	profiles := make([]string, 0, len(c.LLM.ProviderLimits))
	for profile := range c.LLM.ProviderLimits {
		profiles = append(profiles, profile)
	}
	sort.Strings(profiles)
	for _, profile := range profiles {
		trimmedProfile := strings.TrimSpace(profile)
		if trimmedProfile == "" {
			return fmt.Errorf("llm.provider_limits profile key is required")
		}
		if profile != trimmedProfile {
			return fmt.Errorf("llm.provider_limits.%q: profile key must not contain leading or trailing whitespace", profile)
		}
		if _, err := llmselection.ResolveActiveBackend(trimmedProfile); err != nil {
			return fmt.Errorf("llm.provider_limits.%s: %w", trimmedProfile, err)
		}
		if err := validateLLMProviderLimitPolicy("llm.provider_limits."+trimmedProfile, c.LLM.ProviderLimits[profile]); err != nil {
			return err
		}
	}
	return nil
}

func validateLLMProviderLimitPolicy(location string, policy LLMProviderLimitPolicy) error {
	for _, retired := range []struct {
		key     string
		present bool
	}{
		{"rate_limit", policy.RateLimit != ""},
		{"rate_limit_max_wait", policy.RateLimitMaxWait != ""},
		{"max_concurrency_max_wait", policy.MaxConcurrencyMaxWait != ""},
		{"models", policy.Models != nil},
	} {
		if retired.present {
			return runtimecontracts.NewUndefinedFieldDiagnostic(location, retired.key, map[string]struct{}{"max_concurrency": {}})
		}
	}
	if policy.MaxConcurrency < 0 {
		return fmt.Errorf("%s.max_concurrency must be a positive integer", location)
	}
	return nil
}

func (p LLMProviderLimitPolicy) ParseRateLimit() (LLMProviderRateLimit, bool, error) {
	rateLimitSet := strings.TrimSpace(p.RateLimit) != ""
	maxWaitSet := strings.TrimSpace(p.RateLimitMaxWait) != ""
	if !rateLimitSet && !maxWaitSet {
		return LLMProviderRateLimit{}, false, nil
	}
	if !rateLimitSet {
		return LLMProviderRateLimit{}, false, fmt.Errorf("rate_limit_max_wait requires rate_limit")
	}
	if !maxWaitSet {
		return LLMProviderRateLimit{}, false, fmt.Errorf("rate_limit requires rate_limit_max_wait")
	}
	limit, period, err := parseLLMProviderRate(p.RateLimit)
	if err != nil {
		return LLMProviderRateLimit{}, false, fmt.Errorf("rate_limit: %w", err)
	}
	wait, err := parseLLMProviderDuration(p.RateLimitMaxWait, true)
	if err != nil {
		return LLMProviderRateLimit{}, false, fmt.Errorf("rate_limit_max_wait: %w", err)
	}
	return LLMProviderRateLimit{
		Enabled: true,
		Limit:   limit,
		Period:  period,
		MaxWait: wait,
	}, true, nil
}

func (p LLMProviderLimitPolicy) ParseConcurrencyLimit() (LLMProviderConcurrencyLimit, bool, error) {
	maxWaitSet := strings.TrimSpace(p.MaxConcurrencyMaxWait) != ""
	if p.MaxConcurrency == 0 && !maxWaitSet {
		return LLMProviderConcurrencyLimit{}, false, nil
	}
	if p.MaxConcurrency <= 0 {
		return LLMProviderConcurrencyLimit{}, false, fmt.Errorf("max_concurrency must be a positive integer when max_concurrency_max_wait is present")
	}
	if !maxWaitSet {
		return LLMProviderConcurrencyLimit{Enabled: true, Limit: p.MaxConcurrency}, true, nil
	}
	wait, err := parseLLMProviderDuration(p.MaxConcurrencyMaxWait, true)
	if err != nil {
		return LLMProviderConcurrencyLimit{}, false, fmt.Errorf("max_concurrency_max_wait: %w", err)
	}
	return LLMProviderConcurrencyLimit{
		Enabled: true,
		Limit:   p.MaxConcurrency,
		MaxWait: wait,
	}, true, nil
}

func (p LLMProviderLimitPolicy) ModelPolicy(keys ...string) (LLMProviderLimitPolicy, string, bool) {
	if len(p.Models) == 0 {
		return LLMProviderLimitPolicy{}, "", false
	}
	for _, key := range keys {
		trimmed := strings.TrimSpace(key)
		if trimmed == "" {
			continue
		}
		if policy, ok := p.Models[trimmed]; ok {
			return policy, trimmed, true
		}
	}
	return LLMProviderLimitPolicy{}, "", false
}

func parseLLMProviderRate(raw string) (int, time.Duration, error) {
	if err := rejectLLMProviderLimitWhitespace(raw); err != nil {
		return 0, 0, err
	}
	countRaw, periodRaw, ok := strings.Cut(raw, "/")
	if !ok || countRaw == "" || periodRaw == "" || strings.Contains(periodRaw, "/") {
		return 0, 0, fmt.Errorf("must be <positive integer>/<period>")
	}
	count, err := strconv.Atoi(countRaw)
	if err != nil || count <= 0 {
		return 0, 0, fmt.Errorf("count must be a positive integer")
	}
	period, err := parseLLMProviderRatePeriod(periodRaw)
	if err != nil {
		return 0, 0, err
	}
	return count, period, nil
}

func parseLLMProviderRatePeriod(raw string) (time.Duration, error) {
	switch raw {
	case "ms", "s", "m", "h", "d":
		raw = "1" + raw
	}
	return parseLLMProviderDuration(raw, false)
}

func parseLLMProviderDuration(raw string, allowZero bool) (time.Duration, error) {
	if err := rejectLLMProviderLimitWhitespace(raw); err != nil {
		return 0, err
	}
	if raw == "" {
		return 0, fmt.Errorf("duration is required")
	}
	if len(raw) > 0 && raw[0] == '-' {
		return 0, fmt.Errorf("duration must be positive")
	}
	i := 0
	for i < len(raw) && raw[i] >= '0' && raw[i] <= '9' {
		i++
	}
	if i == 0 || i == len(raw) {
		return 0, fmt.Errorf("duration must be an integer with unit ms, s, m, h, or d")
	}
	value, err := strconv.Atoi(raw[:i])
	if err != nil {
		return 0, fmt.Errorf("duration value must be an integer")
	}
	if value == 0 && !allowZero {
		return 0, fmt.Errorf("duration must be positive")
	}
	if value < 0 {
		return 0, fmt.Errorf("duration must be positive")
	}
	switch raw[i:] {
	case "ms":
		return time.Duration(value) * time.Millisecond, nil
	case "s":
		return time.Duration(value) * time.Second, nil
	case "m":
		return time.Duration(value) * time.Minute, nil
	case "h":
		return time.Duration(value) * time.Hour, nil
	case "d":
		return time.Duration(value) * 24 * time.Hour, nil
	default:
		return 0, fmt.Errorf("duration unit must be ms, s, m, h, or d")
	}
}

func rejectLLMProviderLimitWhitespace(raw string) error {
	for _, r := range raw {
		if unicode.IsSpace(r) {
			return fmt.Errorf("must not contain whitespace")
		}
	}
	return nil
}
