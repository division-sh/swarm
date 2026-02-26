package runtime

import (
	"reflect"
	"testing"
	"time"

	"empireai/internal/config"
)

func TestShardPlanner_MarketResearch_DefaultDeterministic(t *testing.T) {
	planner := newTestShardPlanner(t)
	payload := map[string]any{"geography": "Argentina"}

	first, err := planner.Plan(ShardStageMarketResearch, payload)
	if err != nil {
		t.Fatalf("Plan(first): %v", err)
	}
	second, err := planner.Plan(ShardStageMarketResearch, payload)
	if err != nil {
		t.Fatalf("Plan(second): %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("planner must be deterministic\nfirst=%#v\nsecond=%#v", first, second)
	}
	if len(first) != 4 {
		t.Fatalf("expected 4 market shards, got %d", len(first))
	}

	wantKeys := []string{
		"financial_ops+commerce_payments",
		"customer_ops+marketing_sales",
		"workforce_hr+operations_productivity",
		"industry_specific+compliance_governance",
	}
	for i := range first {
		if first[i].ShardKey != wantKeys[i] {
			t.Fatalf("shard[%d] key=%q want=%q", i, first[i].ShardKey, wantKeys[i])
		}
		if first[i].Timeout != 30*time.Minute {
			t.Fatalf("shard[%d] timeout=%s want=30m", i, first[i].Timeout)
		}
		if first[i].BudgetCents != 50 {
			t.Fatalf("shard[%d] budget=%d want=50", i, first[i].BudgetCents)
		}
	}
}

func TestShardPlanner_MarketResearch_FilteredScope(t *testing.T) {
	planner := newTestShardPlanner(t)
	payload := map[string]any{
		"taxonomy_categories": []string{"financial_ops", "commerce_payments"},
	}

	assignments, err := planner.Plan(ShardStageMarketResearch, payload)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if len(assignments) != 1 {
		t.Fatalf("expected 1 shard for two-category filter, got %d", len(assignments))
	}
	scope, _ := assignments[0].Scope["taxonomy_categories"].([]string)
	want := []string{"financial_ops", "commerce_payments"}
	if !reflect.DeepEqual(scope, want) {
		t.Fatalf("scope=%v want=%v", scope, want)
	}
}

func TestShardPlanner_TrendResearch_Default(t *testing.T) {
	planner := newTestShardPlanner(t)
	assignments, err := planner.Plan(ShardStageTrendResearch, map[string]any{"geography": "Chile"})
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if len(assignments) != 2 {
		t.Fatalf("expected 2 trend shards, got %d", len(assignments))
	}
	for i := range assignments {
		scope, _ := assignments[i].Scope["trend_categories"].([]string)
		if len(scope) != 3 {
			t.Fatalf("shard[%d] trend scope len=%d want=3", i, len(scope))
		}
	}
}

func TestShardPlanner_UnsupportedStage(t *testing.T) {
	planner := newTestShardPlanner(t)
	if _, err := planner.Plan("unknown", map[string]any{}); err == nil {
		t.Fatal("expected error for unsupported stage")
	}
}

func newTestShardPlanner(t *testing.T) *ShardPlanner {
	t.Helper()
	cfg := &config.Config{}
	cfg.LLM.RuntimeMode = "api"
	cfg.LLM.Session.LockTTL = time.Second
	cfg.LLM.Session.RotateAfterTurns = 1
	cfg.LLM.Session.RotateOnParseFailures = 1
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate config: %v", err)
	}
	return NewShardPlanner(cfg.Sharding)
}
