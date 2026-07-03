package llm

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/config"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	runtimerterr "github.com/division-sh/swarm/internal/runtime/rterrors"
)

const (
	llmProviderRateLimitedCode = "rate_limited"

	llmProviderAdmissionComponent = "llm-provider"
	llmProviderAdmissionOperation = "provider_admission"

	llmProviderAdmissionModelAll = "*"
)

type ProviderAdmissionRegistry struct {
	cfg        *config.Config
	controller *llmProviderAdmissionController
}

type llmProviderAdmissionPolicy struct {
	Profile        llmselection.Profile
	ResolvedModel  llmselection.ResolvedModel
	BucketName     string
	RateBucketKey  string
	Rate           config.LLMProviderRateLimit
	ConcurrencyKey string
	Concurrency    config.LLMProviderConcurrencyLimit
}

type llmProviderAdmissionController struct {
	mu          sync.Mutex
	rateBuckets map[string]*llmProviderRateBucket
	concurrency map[string]*llmProviderConcurrencyBucket
}

type llmProviderRateBucket struct {
	mu        sync.Mutex
	scheduled []time.Time
}

type llmProviderConcurrencyBucket struct {
	tokens chan struct{}
}

func NewProviderAdmissionRegistry(cfg *config.Config) *ProviderAdmissionRegistry {
	return &ProviderAdmissionRegistry{
		cfg:        cfg,
		controller: newLLMProviderAdmissionController(),
	}
}

func newLLMProviderAdmissionController() *llmProviderAdmissionController {
	return &llmProviderAdmissionController{
		rateBuckets: map[string]*llmProviderRateBucket{},
		concurrency: map[string]*llmProviderConcurrencyBucket{},
	}
}

func (r *ProviderAdmissionRegistry) Admit(ctx context.Context, profile llmselection.Profile, resolvedModel llmselection.ResolvedModel) (func(), error) {
	if r == nil || r.controller == nil {
		return noopProviderAdmissionRelease, nil
	}
	policy, ok, err := r.policy(profile, resolvedModel)
	if err != nil {
		return noopProviderAdmissionRelease, err
	}
	if !ok {
		return noopProviderAdmissionRelease, nil
	}
	return r.controller.Admit(ctx, policy)
}

func admitProviderRequest(ctx context.Context, registry *ProviderAdmissionRegistry, profile llmselection.Profile, resolvedModel llmselection.ResolvedModel) (func(), error) {
	if registry == nil {
		return noopProviderAdmissionRelease, nil
	}
	return registry.Admit(ctx, profile, resolvedModel)
}

func resolveProviderAdmissionModel(ctx context.Context, cfg *config.Config, registry *ProviderAdmissionRegistry, profile llmselection.Profile) (llmselection.ResolvedModel, error) {
	if registry == nil || !registry.configuredFor(profile) {
		return llmselection.ResolvedModel{}, nil
	}
	return resolveAdmissionModel(ctx, cfg, profile)
}

func (r *ProviderAdmissionRegistry) configuredFor(profile llmselection.Profile) bool {
	if r == nil || r.cfg == nil || len(r.cfg.LLM.ProviderLimits) == 0 {
		return false
	}
	policy, ok := r.cfg.LLM.ProviderLimits[profile.ID]
	return ok && providerAdmissionPolicyDeclared(policy)
}

func providerAdmissionPolicyDeclared(policy config.LLMProviderLimitPolicy) bool {
	if strings.TrimSpace(policy.RateLimit) != "" || strings.TrimSpace(policy.RateLimitMaxWait) != "" ||
		policy.MaxConcurrency != 0 || strings.TrimSpace(policy.MaxConcurrencyMaxWait) != "" {
		return true
	}
	for _, modelPolicy := range policy.Models {
		if providerAdmissionPolicyDeclared(modelPolicy) {
			return true
		}
	}
	return false
}

func (r *ProviderAdmissionRegistry) policy(profile llmselection.Profile, resolvedModel llmselection.ResolvedModel) (llmProviderAdmissionPolicy, bool, error) {
	if r == nil || r.cfg == nil || len(r.cfg.LLM.ProviderLimits) == 0 {
		return llmProviderAdmissionPolicy{}, false, nil
	}
	base, ok := r.cfg.LLM.ProviderLimits[profile.ID]
	if !ok {
		return llmProviderAdmissionPolicy{}, false, nil
	}

	rate, rateBucketModel, err := ratePolicyForModel(base, resolvedModel)
	if err != nil {
		return llmProviderAdmissionPolicy{}, false, err
	}
	concurrency, concurrencyBucketModel, err := concurrencyPolicyForModel(base, resolvedModel)
	if err != nil {
		return llmProviderAdmissionPolicy{}, false, err
	}
	if !rate.Enabled && !concurrency.Enabled {
		return llmProviderAdmissionPolicy{}, false, nil
	}

	bucketName := strings.Join([]string{
		strings.TrimSpace(profile.ID),
		strings.TrimSpace(profile.Provider),
		strings.TrimSpace(profile.Transport),
		strings.TrimSpace(resolvedModel.ModelAlias),
		strings.TrimSpace(resolvedModel.ConcreteModel),
	}, "/")
	return llmProviderAdmissionPolicy{
		Profile:        profile,
		ResolvedModel:  resolvedModel,
		BucketName:     bucketName,
		RateBucketKey:  llmProviderAdmissionBucketKey(profile, rateBucketModel),
		Rate:           rate,
		ConcurrencyKey: llmProviderAdmissionBucketKey(profile, concurrencyBucketModel),
		Concurrency:    concurrency,
	}, true, nil
}

func ratePolicyForModel(base config.LLMProviderLimitPolicy, resolvedModel llmselection.ResolvedModel) (config.LLMProviderRateLimit, string, error) {
	rate, _, err := base.ParseRateLimit()
	if err != nil {
		return config.LLMProviderRateLimit{}, "", err
	}
	bucketModel := llmProviderAdmissionModelAll
	if model, key, ok := base.ModelPolicy(resolvedModel.ModelAlias, resolvedModel.ConcreteModel); ok {
		if modelRate, enabled, err := model.ParseRateLimit(); err != nil {
			return config.LLMProviderRateLimit{}, "", err
		} else if enabled {
			rate = modelRate
			bucketModel = key
		}
	}
	return rate, bucketModel, nil
}

func concurrencyPolicyForModel(base config.LLMProviderLimitPolicy, resolvedModel llmselection.ResolvedModel) (config.LLMProviderConcurrencyLimit, string, error) {
	limit, _, err := base.ParseConcurrencyLimit()
	if err != nil {
		return config.LLMProviderConcurrencyLimit{}, "", err
	}
	bucketModel := llmProviderAdmissionModelAll
	if model, key, ok := base.ModelPolicy(resolvedModel.ModelAlias, resolvedModel.ConcreteModel); ok {
		if modelLimit, enabled, err := model.ParseConcurrencyLimit(); err != nil {
			return config.LLMProviderConcurrencyLimit{}, "", err
		} else if enabled {
			limit = modelLimit
			bucketModel = key
		}
	}
	return limit, bucketModel, nil
}

func (c *llmProviderAdmissionController) Admit(ctx context.Context, policy llmProviderAdmissionPolicy) (func(), error) {
	if policy.Rate.Enabled {
		if err := c.admitRate(ctx, policy); err != nil {
			return noopProviderAdmissionRelease, err
		}
	}
	if policy.Concurrency.Enabled {
		concurrencyRelease, err := c.acquireConcurrency(ctx, policy)
		if err != nil {
			return noopProviderAdmissionRelease, err
		}
		return concurrencyRelease, nil
	}
	return noopProviderAdmissionRelease, nil
}

func (c *llmProviderAdmissionController) admitRate(ctx context.Context, policy llmProviderAdmissionPolicy) error {
	key := strings.TrimSpace(policy.RateBucketKey)
	if key == "" {
		return fmt.Errorf("llm provider rate admission bucket key is required")
	}
	bucket := c.rateBucket(key)
	wait, scheduled, timedOut := bucket.reserve(policy.Rate.Limit, policy.Rate.Period, policy.Rate.MaxWait)
	if timedOut {
		return newLLMProviderAdmissionRateLimitedError(policy)
	}
	if wait <= 0 {
		return nil
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		bucket.cancelReservation(scheduled)
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (c *llmProviderAdmissionController) acquireConcurrency(ctx context.Context, policy llmProviderAdmissionPolicy) (func(), error) {
	key := strings.TrimSpace(policy.ConcurrencyKey)
	if key == "" {
		return noopProviderAdmissionRelease, fmt.Errorf("llm provider concurrency admission bucket key is required")
	}
	bucket := c.concurrencyBucket(key, policy.Concurrency.Limit)
	select {
	case <-bucket.tokens:
		return func() {
			bucket.tokens <- struct{}{}
		}, nil
	default:
	}
	if policy.Concurrency.MaxWait <= 0 {
		return noopProviderAdmissionRelease, newLLMProviderAdmissionRateLimitedError(policy)
	}
	timer := time.NewTimer(policy.Concurrency.MaxWait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return noopProviderAdmissionRelease, ctx.Err()
	case <-timer.C:
		return noopProviderAdmissionRelease, newLLMProviderAdmissionRateLimitedError(policy)
	case <-bucket.tokens:
		return func() {
			bucket.tokens <- struct{}{}
		}, nil
	}
}

func (c *llmProviderAdmissionController) rateBucket(key string) *llmProviderRateBucket {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.rateBuckets == nil {
		c.rateBuckets = map[string]*llmProviderRateBucket{}
	}
	bucket := c.rateBuckets[key]
	if bucket == nil {
		bucket = &llmProviderRateBucket{}
		c.rateBuckets[key] = bucket
	}
	return bucket
}

func (c *llmProviderAdmissionController) concurrencyBucket(key string, limit int) *llmProviderConcurrencyBucket {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.concurrency == nil {
		c.concurrency = map[string]*llmProviderConcurrencyBucket{}
	}
	bucket := c.concurrency[key]
	if bucket == nil || cap(bucket.tokens) != limit {
		tokens := make(chan struct{}, limit)
		for i := 0; i < limit; i++ {
			tokens <- struct{}{}
		}
		bucket = &llmProviderConcurrencyBucket{tokens: tokens}
		c.concurrency[key] = bucket
	}
	return bucket
}

func (b *llmProviderRateBucket) reserve(limit int, period, maxWait time.Duration) (time.Duration, time.Time, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	cutoff := now.Add(-period)
	kept := b.scheduled[:0]
	for _, scheduled := range b.scheduled {
		if scheduled.After(cutoff) {
			kept = append(kept, scheduled)
		}
	}
	b.scheduled = kept
	scheduled := now
	if len(b.scheduled) >= limit {
		next := b.scheduled[len(b.scheduled)-limit].Add(period)
		if next.After(scheduled) {
			scheduled = next
		}
	}
	wait := scheduled.Sub(now)
	if wait < 0 {
		wait = 0
	}
	if wait > maxWait {
		return wait, scheduled, true
	}
	b.scheduled = append(b.scheduled, scheduled)
	return wait, scheduled, false
}

func (b *llmProviderRateBucket) cancelReservation(scheduled time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for i, item := range b.scheduled {
		if item.Equal(scheduled) {
			b.scheduled = append(b.scheduled[:i], b.scheduled[i+1:]...)
			return
		}
	}
}

func newLLMProviderAdmissionRateLimitedError(policy llmProviderAdmissionPolicy) error {
	return runtimerterr.NewRuntimeError(
		llmProviderRateLimitedCode,
		llmProviderAdmissionComponent,
		llmProviderAdmissionOperation,
		true,
		"llm provider admission limit exceeded for %s",
		strings.TrimSpace(policy.BucketName),
	)
}

func resolveAdmissionModel(ctx context.Context, cfg *config.Config, profile llmselection.Profile) (llmselection.ResolvedModel, error) {
	if cfg == nil {
		cfg = &config.Config{}
	}
	req := llmselection.ModelResolution{Models: cfg.LLM.Models}
	if actor, ok := runtimeactors.ActorFromContext(ctx); ok {
		req.Model = actor.Model
	}
	return llmselection.ResolveModel(profile, req)
}

func llmProviderAdmissionBucketKey(profile llmselection.Profile, modelKey string) string {
	return strings.Join([]string{
		"llm_provider",
		strings.TrimSpace(profile.ID),
		strings.TrimSpace(profile.Provider),
		strings.TrimSpace(profile.Transport),
		strings.TrimSpace(modelKey),
	}, ":")
}

func noopProviderAdmissionRelease() {}
