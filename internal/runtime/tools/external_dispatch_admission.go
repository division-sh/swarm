package tools

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const (
	externalDispatchRateLimitedCode = "rate_limited"

	externalDispatchScopeHTTPTool        = "http_tool"
	externalDispatchScopeMCPServer       = "mcp_server"
	externalDispatchScopeNativeWebSearch = "native_web_search"

	externalDispatchOutcomeImmediate = "admitted_immediate"
	externalDispatchOutcomeWaited    = "admitted_after_wait"
	externalDispatchOutcomeTimedOut  = "timed_out"
)

type externalDispatchRateLimitConfig struct {
	Enabled bool
	Limit   int
	Period  time.Duration
	MaxWait time.Duration
}

type externalDispatchAdmissionPolicy struct {
	Scope      string
	BucketName string
	BucketKey  string
	Config     externalDispatchRateLimitConfig
}

type externalDispatchAdmissionResult struct {
	Enabled    bool
	Scope      string
	BucketHash string
	Limit      int
	Period     time.Duration
	MaxWait    time.Duration
	Wait       time.Duration
	Outcome    string
	ErrorCode  string
	Retryable  bool
}

type externalDispatchAdmissionSummary struct {
	Enabled    bool
	Scope      string
	BucketHash string
	Limit      int
	Period     time.Duration
	MaxWait    time.Duration
	Wait       time.Duration
	Outcome    string
	ErrorCode  string
	Retryable  bool
	Attempts   int
}

type externalDispatchAdmissionCollector struct {
	mu      sync.Mutex
	summary externalDispatchAdmissionSummary
}

type externalDispatchAdmissionCollectorKey struct{}

func withExternalDispatchAdmissionCollector(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Value(externalDispatchAdmissionCollectorKey{}).(*externalDispatchAdmissionCollector); ok {
		return ctx
	}
	return context.WithValue(ctx, externalDispatchAdmissionCollectorKey{}, &externalDispatchAdmissionCollector{})
}

func recordExternalDispatchAdmission(ctx context.Context, result externalDispatchAdmissionResult) {
	if !result.Enabled {
		return
	}
	collector, ok := ctx.Value(externalDispatchAdmissionCollectorKey{}).(*externalDispatchAdmissionCollector)
	if !ok || collector == nil {
		return
	}
	collector.record(result)
}

func externalDispatchAdmissionDiagnosticDetail(ctx context.Context) (map[string]any, bool) {
	collector, ok := ctx.Value(externalDispatchAdmissionCollectorKey{}).(*externalDispatchAdmissionCollector)
	if !ok || collector == nil {
		return nil, false
	}
	summary, ok := collector.snapshot()
	if !ok {
		return nil, false
	}
	detail := map[string]any{
		"rate_limit_scope":       summary.Scope,
		"rate_limit_bucket_hash": summary.BucketHash,
		"rate_limit_limit":       summary.Limit,
		"rate_limit_period_ms":   summary.Period.Milliseconds(),
		"rate_limit_max_wait_ms": summary.MaxWait.Milliseconds(),
		"rate_limit_wait_ms":     summary.Wait.Milliseconds(),
		"rate_limit_outcome":     summary.Outcome,
		"rate_limit_attempts":    summary.Attempts,
	}
	if summary.ErrorCode != "" {
		detail["error_code"] = summary.ErrorCode
		detail["retryable"] = summary.Retryable
	}
	return detail, true
}

func (c *externalDispatchAdmissionCollector) record(result externalDispatchAdmissionResult) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.summary.Enabled {
		c.summary = externalDispatchAdmissionSummary{
			Enabled:    true,
			Scope:      result.Scope,
			BucketHash: result.BucketHash,
			Limit:      result.Limit,
			Period:     result.Period,
			MaxWait:    result.MaxWait,
			Outcome:    result.Outcome,
		}
	}
	c.summary.Attempts++
	c.summary.Wait += result.Wait
	switch result.Outcome {
	case externalDispatchOutcomeTimedOut:
		c.summary.Outcome = result.Outcome
		c.summary.ErrorCode = result.ErrorCode
		c.summary.Retryable = result.Retryable
	case externalDispatchOutcomeWaited:
		if c.summary.Outcome != externalDispatchOutcomeTimedOut {
			c.summary.Outcome = result.Outcome
		}
	case externalDispatchOutcomeImmediate:
		if c.summary.Outcome == "" {
			c.summary.Outcome = result.Outcome
		}
	}
}

func (c *externalDispatchAdmissionCollector) snapshot() (externalDispatchAdmissionSummary, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.summary.Enabled {
		return externalDispatchAdmissionSummary{}, false
	}
	return c.summary, true
}

type externalDispatchAdmissionController struct {
	mu      sync.Mutex
	buckets map[string]*externalDispatchAdmissionBucket
}

type externalDispatchAdmissionBucket struct {
	mu        sync.Mutex
	scheduled []time.Time
}

func newExternalDispatchAdmissionController() *externalDispatchAdmissionController {
	return &externalDispatchAdmissionController{buckets: map[string]*externalDispatchAdmissionBucket{}}
}

func (e *Executor) admitExternalDispatch(ctx context.Context, policy externalDispatchAdmissionPolicy) error {
	if e == nil || e.externalDispatchAdmission == nil || !policy.Config.Enabled {
		return nil
	}
	result, err := e.externalDispatchAdmission.Admit(ctx, policy)
	recordExternalDispatchAdmission(ctx, result)
	return err
}

func isExternalDispatchRateLimited(err error) bool {
	runtimeErr, ok := AsRuntimeError(err)
	return ok && runtimeErr != nil && runtimeErr.Code == externalDispatchRateLimitedCode
}

func (e *Executor) httpToolExternalDispatchPolicy(tool RegisteredTool) externalDispatchAdmissionPolicy {
	if !tool.RateLimit.Enabled {
		return externalDispatchAdmissionPolicy{}
	}
	source := e.workflowSourceForAdmission()
	toolName := strings.TrimSpace(tool.Name)
	bucketKey := strings.Join([]string{
		externalDispatchScopeHTTPTool,
		externalDispatchSourceKey(source),
		toolName,
	}, ":")
	return externalDispatchAdmissionPolicy{
		Scope:      externalDispatchScopeHTTPTool,
		BucketName: "http tool " + toolName,
		BucketKey:  bucketKey,
		Config:     tool.RateLimit,
	}
}

func (e *Executor) mcpToolExternalDispatchPolicy(tool RegisteredTool) (externalDispatchAdmissionPolicy, error) {
	if e == nil || e.mcpClient == nil {
		return externalDispatchAdmissionPolicy{}, nil
	}
	serverName := strings.TrimSpace(tool.MCPServerName)
	if serverName == "" {
		return externalDispatchAdmissionPolicy{}, nil
	}
	cfg, ok := e.mcpClient.ServerConfig(serverName)
	if !ok {
		return externalDispatchAdmissionPolicy{}, nil
	}
	rateLimit, _, err := parseExternalDispatchRateLimit(cfg.RateLimit, cfg.RateLimitMaxWait)
	if err != nil {
		return externalDispatchAdmissionPolicy{}, fmt.Errorf("mcp_servers.%s: %w", serverName, err)
	}
	if !rateLimit.Enabled {
		return externalDispatchAdmissionPolicy{}, nil
	}
	source := e.workflowSourceForAdmission()
	bucketKey := strings.Join([]string{
		externalDispatchScopeMCPServer,
		externalDispatchSourceKey(source),
		serverName,
	}, ":")
	return externalDispatchAdmissionPolicy{
		Scope:      externalDispatchScopeMCPServer,
		BucketName: "mcp server " + serverName,
		BucketKey:  bucketKey,
		Config:     rateLimit,
	}, nil
}

func (e *Executor) webSearchExternalDispatchPolicy(cfg webSearchProviderConfig) externalDispatchAdmissionPolicy {
	if !cfg.RateLimit.Enabled {
		return externalDispatchAdmissionPolicy{}
	}
	source := e.workflowSourceForAdmission()
	flowID := strings.TrimSpace(cfg.FlowID)
	if flowID == "" {
		flowID = "root"
	}
	provider := strings.TrimSpace(cfg.Provider)
	bucketKey := strings.Join([]string{
		externalDispatchScopeNativeWebSearch,
		externalDispatchSourceKey(source),
		flowID,
		provider,
	}, ":")
	return externalDispatchAdmissionPolicy{
		Scope:      externalDispatchScopeNativeWebSearch,
		BucketName: "native web_search provider " + provider,
		BucketKey:  bucketKey,
		Config:     cfg.RateLimit,
	}
}

func (e *Executor) workflowSourceForAdmission() semanticview.Source {
	if e == nil {
		return nil
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.workflowSource
}

func (c *externalDispatchAdmissionController) Admit(ctx context.Context, policy externalDispatchAdmissionPolicy) (externalDispatchAdmissionResult, error) {
	cfg := policy.Config
	if !cfg.Enabled {
		return externalDispatchAdmissionResult{}, nil
	}
	bucketKey := strings.TrimSpace(policy.BucketKey)
	if bucketKey == "" {
		return externalDispatchAdmissionResult{}, fmt.Errorf("external dispatch admission bucket key is required")
	}
	result := externalDispatchAdmissionResult{
		Enabled:    true,
		Scope:      strings.TrimSpace(policy.Scope),
		BucketHash: externalDispatchBucketHash(bucketKey),
		Limit:      cfg.Limit,
		Period:     cfg.Period,
		MaxWait:    cfg.MaxWait,
		Outcome:    externalDispatchOutcomeImmediate,
	}
	bucket := c.bucket(bucketKey)
	wait, scheduled, timedOut := bucket.reserve(cfg.Limit, cfg.Period, cfg.MaxWait)
	result.Wait = wait
	if timedOut {
		result.Outcome = externalDispatchOutcomeTimedOut
		result.ErrorCode = externalDispatchRateLimitedCode
		result.Retryable = true
		return result, NewRuntimeError(
			externalDispatchRateLimitedCode,
			"tool-executor",
			"external_dispatch_admission",
			true,
			"external dispatch rate limit exceeded for %s",
			strings.TrimSpace(policy.BucketName),
		)
	}
	if wait > 0 {
		result.Outcome = externalDispatchOutcomeWaited
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			bucket.cancelReservation(scheduled)
			return result, ctx.Err()
		case <-timer.C:
		}
	}
	return result, nil
}

func (c *externalDispatchAdmissionController) bucket(key string) *externalDispatchAdmissionBucket {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.buckets == nil {
		c.buckets = map[string]*externalDispatchAdmissionBucket{}
	}
	bucket := c.buckets[key]
	if bucket == nil {
		bucket = &externalDispatchAdmissionBucket{}
		c.buckets[key] = bucket
	}
	return bucket
}

func (b *externalDispatchAdmissionBucket) reserve(limit int, period, maxWait time.Duration) (time.Duration, time.Time, bool) {
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

func (b *externalDispatchAdmissionBucket) cancelReservation(scheduled time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for i, item := range b.scheduled {
		if item.Equal(scheduled) {
			b.scheduled = append(b.scheduled[:i], b.scheduled[i+1:]...)
			return
		}
	}
}

func externalDispatchBucketHash(bucketKey string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(bucketKey)))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func parseExternalDispatchRateLimit(rateLimit, maxWait string) (externalDispatchRateLimitConfig, bool, error) {
	rateLimitSet := rateLimit != ""
	maxWaitSet := maxWait != ""
	if !rateLimitSet && !maxWaitSet {
		return externalDispatchRateLimitConfig{}, false, nil
	}
	if !rateLimitSet {
		return externalDispatchRateLimitConfig{}, false, fmt.Errorf("rate_limit_max_wait requires rate_limit")
	}
	if !maxWaitSet {
		return externalDispatchRateLimitConfig{}, false, fmt.Errorf("rate_limit requires rate_limit_max_wait")
	}
	limit, period, err := parseExternalDispatchRate(rateLimit)
	if err != nil {
		return externalDispatchRateLimitConfig{}, false, fmt.Errorf("rate_limit: %w", err)
	}
	wait, err := parseExternalDispatchDuration(maxWait, true)
	if err != nil {
		return externalDispatchRateLimitConfig{}, false, fmt.Errorf("rate_limit_max_wait: %w", err)
	}
	return externalDispatchRateLimitConfig{
		Enabled: true,
		Limit:   limit,
		Period:  period,
		MaxWait: wait,
	}, true, nil
}

func ValidateExternalDispatchRateLimitDeclarations(source semanticview.Source) []error {
	if source == nil {
		return nil
	}
	errs := make([]error, 0)
	for name, entry := range source.ToolEntries() {
		location := fmt.Sprintf("tool %s", strings.TrimSpace(name))
		_, enabled, err := parseExternalDispatchRateLimit(entry.RateLimit, entry.RateLimitMaxWait)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", location, err))
			continue
		}
		if enabled && normalizeImplementationClass(name, entry) != implementationHTTP {
			errs = append(errs, fmt.Errorf("%s: rate_limit is only supported for handler_type http", location))
		}
	}
	errs = appendExternalDispatchPolicyValidationErrors(errs, "root policy", source.ResolvedPolicyForFlow("").Values)
	for _, scope := range source.ProjectScopes() {
		label := strings.TrimSpace(scope.Key)
		if label == "" {
			label = strings.TrimSpace(scope.Manifest.Name)
		}
		if label == "" {
			label = "project"
		}
		errs = appendExternalDispatchPolicyValidationErrors(errs, "project policy "+label, scope.Policy.Values)
	}
	for _, scope := range source.FlowScopes() {
		label := strings.TrimSpace(scope.ID)
		if label == "" {
			label = strings.TrimSpace(scope.Path)
		}
		if label == "" {
			label = "flow"
		}
		errs = appendExternalDispatchPolicyValidationErrors(errs, "flow policy "+label, scope.Policy.Values)
	}
	return dedupeExternalDispatchValidationErrors(errs)
}

func appendExternalDispatchPolicyValidationErrors(errs []error, location string, values map[string]runtimecontracts.PolicyValue) []error {
	if len(values) == 0 {
		return errs
	}
	if item, ok := values["mcp_servers"]; ok {
		errs = appendExternalDispatchMCPServerValidationErrors(errs, location+".mcp_servers", item.Value)
	}
	if item, ok := values["web_search_provider"]; ok {
		errs = appendExternalDispatchWebSearchValidationErrors(errs, location+".web_search_provider", item.Value)
	}
	return errs
}

func appendExternalDispatchMCPServerValidationErrors(errs []error, location string, raw any) []error {
	servers, ok := raw.(map[string]any)
	if !ok {
		return errs
	}
	names := make([]string, 0, len(servers))
	for name := range servers {
		if strings.TrimSpace(name) != "" {
			names = append(names, strings.TrimSpace(name))
		}
	}
	sort.Strings(names)
	for _, name := range names {
		item, ok := servers[name].(map[string]any)
		if !ok {
			continue
		}
		rateLimit, maxWait, err := externalDispatchRateLimitPairFromMap(item)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s.%s: %w", location, name, err))
			continue
		}
		if _, _, err := parseExternalDispatchRateLimit(rateLimit, maxWait); err != nil {
			errs = append(errs, fmt.Errorf("%s.%s: %w", location, name, err))
		}
	}
	return errs
}

func appendExternalDispatchWebSearchValidationErrors(errs []error, location string, raw any) []error {
	item, ok := raw.(map[string]any)
	if !ok {
		return errs
	}
	rateLimit, maxWait, err := externalDispatchRateLimitPairFromMap(item)
	if err != nil {
		return append(errs, fmt.Errorf("%s: %w", location, err))
	}
	if _, _, err := parseExternalDispatchRateLimit(rateLimit, maxWait); err != nil {
		errs = append(errs, fmt.Errorf("%s: %w", location, err))
	}
	return errs
}

func externalDispatchRateLimitPairFromMap(item map[string]any) (string, string, error) {
	rateLimit, _, err := externalDispatchOptionalString(item, "rate_limit")
	if err != nil {
		return "", "", err
	}
	maxWait, _, err := externalDispatchOptionalString(item, "rate_limit_max_wait")
	if err != nil {
		return "", "", err
	}
	return rateLimit, maxWait, nil
}

func externalDispatchOptionalString(item map[string]any, field string) (string, bool, error) {
	value, ok := item[field]
	if !ok || value == nil {
		return "", false, nil
	}
	text, ok := value.(string)
	if !ok {
		return "", true, fmt.Errorf("%s must be a string", field)
	}
	return text, true, nil
}

func dedupeExternalDispatchValidationErrors(errs []error) []error {
	if len(errs) < 2 {
		return errs
	}
	out := make([]error, 0, len(errs))
	seen := make(map[string]struct{}, len(errs))
	for _, err := range errs {
		if err == nil {
			continue
		}
		key := err.Error()
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, err)
	}
	return out
}

func parseExternalDispatchRate(raw string) (int, time.Duration, error) {
	if err := rejectRateLimitWhitespace(raw); err != nil {
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
	period, err := parseExternalDispatchDuration(periodRaw, false)
	if err != nil {
		return 0, 0, err
	}
	return count, period, nil
}

func parseExternalDispatchDuration(raw string, allowZero bool) (time.Duration, error) {
	if err := rejectRateLimitWhitespace(raw); err != nil {
		return 0, err
	}
	if raw == "" {
		return 0, fmt.Errorf("duration is required")
	}
	unit := ""
	numberRaw := raw
	for _, candidate := range []string{"ms", "s", "m", "h", "d"} {
		if strings.HasSuffix(raw, candidate) {
			unit = candidate
			numberRaw = strings.TrimSuffix(raw, candidate)
			break
		}
	}
	if unit == "" {
		return 0, fmt.Errorf("duration unit must be one of ms, s, m, h, d")
	}
	if numberRaw == "" {
		numberRaw = "1"
	}
	n, err := strconv.Atoi(numberRaw)
	if err != nil {
		return 0, fmt.Errorf("duration amount must be an integer")
	}
	if n < 0 || (!allowZero && n == 0) {
		return 0, fmt.Errorf("duration must be positive")
	}
	var unitDuration time.Duration
	switch unit {
	case "ms":
		unitDuration = time.Millisecond
	case "s":
		unitDuration = time.Second
	case "m":
		unitDuration = time.Minute
	case "h":
		unitDuration = time.Hour
	case "d":
		unitDuration = 24 * time.Hour
	}
	return time.Duration(n) * unitDuration, nil
}

func rejectRateLimitWhitespace(raw string) error {
	if raw == "" {
		return nil
	}
	if strings.ContainsFunc(raw, unicode.IsSpace) {
		return fmt.Errorf("must not contain whitespace")
	}
	return nil
}

func externalDispatchSourceKey(source semanticview.Source) string {
	if source == nil {
		return "source:unknown"
	}
	parts := []string{}
	if name := strings.TrimSpace(source.WorkflowName()); name != "" {
		parts = append(parts, name)
	}
	if version := strings.TrimSpace(source.WorkflowVersion()); version != "" {
		parts = append(parts, version)
	}
	if len(parts) == 0 {
		scopes := source.ProjectScopes()
		labels := make([]string, 0, len(scopes))
		for _, scope := range scopes {
			if key := strings.TrimSpace(scope.Key); key != "" {
				labels = append(labels, key)
			}
		}
		sort.Strings(labels)
		parts = labels
	}
	if len(parts) == 0 {
		return "source:unknown"
	}
	return "source:" + strings.Join(parts, "@")
}
