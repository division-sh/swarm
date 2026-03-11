package pipeline

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	empireconfig "empireai/internal/empire/config"
)

const (
	ShardStageMarketResearch = "market_research"
	ShardStageTrendResearch  = "trend_research"
)

type ShardAssignment struct {
	Stage          string         `json:"stage"`
	ShardIndex     int            `json:"shard_index"`
	ShardCount     int            `json:"shard_count"`
	ShardKey       string         `json:"shard_key"`
	Scope          map[string]any `json:"scope"`
	EstimatedItems int            `json:"estimated_items"`
	Timeout        time.Duration  `json:"timeout"`
	BudgetCents    int            `json:"budget_cents"`
}

type ShardPlanFunc func(payload map[string]any, cfg shardStageRuntimeConfig) []ShardAssignment

type ShardPlanner struct {
	stageCfg map[string]shardStageRuntimeConfig
	plans    map[string]ShardPlanFunc
}

type shardStageRuntimeConfig struct {
	TargetItemsPerShard int
	MaxShards           int
	PerShardTimeout     time.Duration
	PerShardBudgetCents int
}

func NewShardPlanner(cfg empireconfig.ShardingConfig) *ShardPlanner {
	stageCfg := map[string]shardStageRuntimeConfig{
		ShardStageMarketResearch: {
			TargetItemsPerShard: cfg.Stages.MarketResearch.TargetItemsPerShard,
			MaxShards:           cfg.Stages.MarketResearch.MaxShards,
			PerShardTimeout:     cfg.PerShardTimeout,
			PerShardBudgetCents: cfg.PerShardBudgetCents,
		},
		ShardStageTrendResearch: {
			TargetItemsPerShard: cfg.Stages.TrendResearch.TargetItemsPerShard,
			MaxShards:           cfg.Stages.TrendResearch.MaxShards,
			PerShardTimeout:     cfg.PerShardTimeout,
			PerShardBudgetCents: cfg.PerShardBudgetCents,
		},
	}
	return &ShardPlanner{
		stageCfg: stageCfg,
		plans: map[string]ShardPlanFunc{
			ShardStageMarketResearch: planMarketResearchShards,
			ShardStageTrendResearch:  planTrendResearchShards,
		},
	}
}

func (p *ShardPlanner) Plan(stage string, payload map[string]any) ([]ShardAssignment, error) {
	if p == nil {
		return nil, fmt.Errorf("shard planner is nil")
	}
	stage = strings.TrimSpace(stage)
	planFn, ok := p.plans[stage]
	if !ok {
		return nil, fmt.Errorf("unsupported shard stage: %s", stage)
	}
	cfg, ok := p.stageCfg[stage]
	if !ok {
		return nil, fmt.Errorf("missing shard config for stage: %s", stage)
	}
	assignments := planFn(payload, cfg)
	for i := range assignments {
		assignments[i].ShardCount = len(assignments)
		assignments[i].ShardIndex = i
	}
	return assignments, nil
}

var marketCategoryOrder = []string{
	"financial_ops",
	"commerce_payments",
	"customer_ops",
	"marketing_sales",
	"workforce_hr",
	"operations_productivity",
	"industry_specific",
	"compliance_governance",
}

var marketCategoryWeights = map[string]int{
	"financial_ops":           9,
	"commerce_payments":       6,
	"customer_ops":            6,
	"marketing_sales":         7,
	"workforce_hr":            6,
	"operations_productivity": 6,
	"industry_specific":       8,
	"compliance_governance":   4,
}

var marketCategoryAliases = map[string]string{
	"financial_operations":        "financial_ops",
	"commerce_and_payments":       "commerce_payments",
	"commerce_payments":           "commerce_payments",
	"customer_operations":         "customer_ops",
	"marketing_and_sales":         "marketing_sales",
	"workforce_and_hr":            "workforce_hr",
	"operations_and_productivity": "operations_productivity",
	"industry_specific_vertical":  "industry_specific",
}

var trendCategoryOrder = []string{
	"migration_relocation",
	"regulatory_changes",
	"technology_enablement",
	"demographic_shifts",
	"investment_signals",
	"community_growth",
}

var trendCategoryAliases = map[string]string{
	"migration_and_relocation": "migration_relocation",
}

func planMarketResearchShards(payload map[string]any, cfg shardStageRuntimeConfig) []ShardAssignment {
	categories := filteredOrderedCategories(payload, "taxonomy_categories", marketCategoryOrder, marketCategoryAliases)
	if len(categories) == 0 {
		return nil
	}
	buckets := splitWeightedCategories(categories, marketCategoryWeights, cfg.TargetItemsPerShard, cfg.MaxShards)
	return buildShardAssignments(ShardStageMarketResearch, "taxonomy_categories", buckets, cfg)
}

func planTrendResearchShards(payload map[string]any, cfg shardStageRuntimeConfig) []ShardAssignment {
	categories := filteredOrderedCategories(payload, "trend_categories", trendCategoryOrder, trendCategoryAliases)
	if len(categories) == 0 {
		return nil
	}
	weights := map[string]int{}
	for _, category := range trendCategoryOrder {
		weights[category] = 1
	}
	buckets := splitWeightedCategories(categories, weights, cfg.TargetItemsPerShard, cfg.MaxShards)
	return buildShardAssignments(ShardStageTrendResearch, "trend_categories", buckets, cfg)
}

func buildShardAssignments(stage, scopeField string, buckets [][]string, cfg shardStageRuntimeConfig) []ShardAssignment {
	out := make([]ShardAssignment, 0, len(buckets))
	for _, bucket := range buckets {
		estimated := 0
		for _, item := range bucket {
			if stage == ShardStageMarketResearch {
				estimated += weightFor(item, marketCategoryWeights)
				continue
			}
			estimated++
		}
		scope := map[string]any{
			scopeField: bucket,
		}
		out = append(out, ShardAssignment{
			Stage:          stage,
			ShardKey:       strings.Join(bucket, "+"),
			Scope:          scope,
			EstimatedItems: estimated,
			Timeout:        cfg.PerShardTimeout,
			BudgetCents:    cfg.PerShardBudgetCents,
		})
	}
	return out
}

func splitWeightedCategories(categories []string, weights map[string]int, targetItemsPerShard, maxShards int) [][]string {
	if len(categories) == 0 {
		return nil
	}
	if targetItemsPerShard <= 0 {
		targetItemsPerShard = 1
	}
	if maxShards <= 0 {
		maxShards = len(categories)
	}
	if maxShards > len(categories) {
		maxShards = len(categories)
	}

	totalWeight := 0
	for _, category := range categories {
		totalWeight += weightFor(category, weights)
	}
	plannedShards := int(math.Ceil(float64(totalWeight) / float64(targetItemsPerShard)))
	if len(categories) <= 2 {
		plannedShards = 1
	}
	if plannedShards < 1 {
		plannedShards = 1
	}
	if plannedShards > maxShards {
		plannedShards = maxShards
	}
	if plannedShards > len(categories) {
		plannedShards = len(categories)
	}

	buckets := make([][]string, 0, plannedShards)
	current := make([]string, 0, 8)
	currentWeight := 0

	for idx, category := range categories {
		weight := weightFor(category, weights)
		remainingCategories := len(categories) - idx
		remainingShards := plannedShards - len(buckets)
		canClose := len(current) > 0 && len(buckets)+1 < plannedShards
		if canClose && shouldCloseBeforeAdd(currentWeight, weight, targetItemsPerShard, remainingCategories, remainingShards) {
			buckets = append(buckets, current)
			current = make([]string, 0, 8)
			currentWeight = 0
		}

		current = append(current, category)
		currentWeight += weight

		remainingAfter := len(categories) - idx - 1
		remainingShardsAfter := plannedShards - (len(buckets) + 1)
		if len(current) > 0 && len(buckets)+1 < plannedShards && currentWeight >= targetItemsPerShard && remainingAfter >= remainingShardsAfter {
			buckets = append(buckets, current)
			current = make([]string, 0, 8)
			currentWeight = 0
		}
	}
	if len(current) > 0 {
		buckets = append(buckets, current)
	}
	return buckets
}

func shouldCloseBeforeAdd(currentWeight, nextWeight, targetItems, remainingCategories, remainingShards int) bool {
	if currentWeight+nextWeight <= targetItems {
		return false
	}
	// Keep at least one category available per remaining shard.
	if remainingCategories <= remainingShards-1 {
		return false
	}
	currentDelta := absInt(targetItems - currentWeight)
	nextDelta := absInt(targetItems - (currentWeight + nextWeight))
	return currentDelta <= nextDelta
}

func filteredOrderedCategories(payload map[string]any, field string, canonicalOrder []string, aliases map[string]string) []string {
	if len(canonicalOrder) == 0 {
		return nil
	}
	if payload == nil || len(payload) == 0 {
		return append([]string(nil), canonicalOrder...)
	}

	requested := parseCategoryFilter(payload[field], aliases)
	if len(requested) == 0 {
		// Backward compatibility: trend filter might still arrive via taxonomy_categories.
		if field == "trend_categories" {
			requested = parseCategoryFilter(payload["taxonomy_categories"], aliases)
		}
	}
	if len(requested) == 0 {
		return append([]string(nil), canonicalOrder...)
	}

	out := make([]string, 0, len(canonicalOrder))
	for _, category := range canonicalOrder {
		if _, ok := requested[category]; ok {
			out = append(out, category)
		}
	}
	return out
}

func parseCategoryFilter(raw any, aliases map[string]string) map[string]struct{} {
	values := toStringList(raw)
	if len(values) == 0 {
		return nil
	}
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		key := normalizeCategoryKey(value)
		if aliases != nil {
			if mapped, ok := aliases[key]; ok {
				key = mapped
			}
		}
		if key == "" {
			continue
		}
		out[key] = struct{}{}
	}
	return out
}

func toStringList(raw any) []string {
	switch typed := raw.(type) {
	case []string:
		out := make([]string, 0, len(typed))
		for _, v := range typed {
			v = strings.TrimSpace(v)
			if v != "" {
				out = append(out, v)
			}
		}
		return out
	case []any:
		out := make([]string, 0, len(typed))
		for _, v := range typed {
			s := strings.TrimSpace(fmt.Sprintf("%v", v))
			if s != "" {
				out = append(out, s)
			}
		}
		return out
	case string:
		s := strings.TrimSpace(typed)
		if s == "" {
			return nil
		}
		if strings.HasPrefix(s, "[") {
			var parsed []string
			if err := json.Unmarshal([]byte(s), &parsed); err == nil && len(parsed) > 0 {
				return parsed
			}
		}
		if strings.Contains(s, ",") {
			parts := strings.Split(s, ",")
			out := make([]string, 0, len(parts))
			for _, p := range parts {
				p = strings.TrimSpace(p)
				if p != "" {
					out = append(out, p)
				}
			}
			return out
		}
		return []string{s}
	default:
		return nil
	}
}

func normalizeCategoryKey(raw string) string {
	raw = strings.TrimSpace(strings.ToLower(raw))
	raw = strings.ReplaceAll(raw, "-", "_")
	raw = strings.ReplaceAll(raw, " ", "_")
	for strings.Contains(raw, "__") {
		raw = strings.ReplaceAll(raw, "__", "_")
	}
	return strings.Trim(raw, "_")
}

func weightFor(category string, weights map[string]int) int {
	if w, ok := weights[category]; ok && w > 0 {
		return w
	}
	return 1
}

func absInt(n int) int {
	if n < 0 {
		return -n
	}
	return n
}
