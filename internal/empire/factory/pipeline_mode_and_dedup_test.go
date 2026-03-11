package factory

import (
	"context"
	"testing"

	"empireai/internal/store"
	"empireai/internal/testutil"
)

func TestPipeline_RunScan_ModeSignalsAndDedup(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	p := NewPipeline(db, pg, pg)
	ctx := context.Background()

	first, err := p.RunScanWithMode(ctx, "Asuncion, Paraguay", "discovery", "saas_gap", []string{"crm", "billing"}, 1)
	if err != nil {
		t.Fatalf("RunScanWithMode first: %v", err)
	}
	if first.Discovered != 1 || len(first.VerticalIDs) != 1 {
		t.Fatalf("unexpected first summary: %+v", first)
	}

	second, err := p.RunScanWithMode(ctx, "Asuncion, Paraguay", "discovery", "saas_gap", []string{"crm", "billing"}, 1)
	if err != nil {
		t.Fatalf("RunScanWithMode second: %v", err)
	}
	if second.Discovered != 0 {
		t.Fatalf("expected dedup discovery=0, got %+v", second)
	}
	if len(second.VerticalIDs) != 1 || second.VerticalIDs[0] != first.VerticalIDs[0] {
		t.Fatalf("expected dedup to reuse existing vertical id, got %+v vs %+v", second.VerticalIDs, first.VerticalIDs)
	}

	var categoryEvents int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM events WHERE type='category.assessed'`).Scan(&categoryEvents); err != nil {
		t.Fatalf("count category.assessed: %v", err)
	}
	if categoryEvents < 1 {
		t.Fatalf("expected category.assessed events, got %d", categoryEvents)
	}
}

func TestPipeline_RunScan_ModeSignalsTrend(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	p := NewPipeline(db, pg, pg)
	ctx := context.Background()

	sum, err := p.RunScanWithMode(ctx, "Austin, TX", "discovery", "saas_trend", nil, 1)
	if err != nil {
		t.Fatalf("RunScanWithMode trend: %v", err)
	}
	if sum.Discovered != 1 {
		t.Fatalf("expected one discovery, got %+v", sum)
	}

	var trendEvents int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM events WHERE type='trend.identified'`).Scan(&trendEvents); err != nil {
		t.Fatalf("count trend.identified: %v", err)
	}
	if trendEvents < 1 {
		t.Fatalf("expected trend.identified events, got %d", trendEvents)
	}
}

func TestComputeScore_ModeRubrics(t *testing.T) {
	rubricLocal, localDims, localViability, localMarket, localTotal := computeScore("local_services", "HVAC Ops", "Austin, TX")
	if rubricLocal != "local_services_rubric" {
		t.Fatalf("expected local_services rubric, got %s", rubricLocal)
	}
	if localViability < 0 || localViability > 100 {
		t.Fatalf("local viability out of range: %d", localViability)
	}
	if localMarket < 0 || localMarket > 100 {
		t.Fatalf("local market out of range: %d", localMarket)
	}
	if localTotal < 0 || localTotal > 100 {
		t.Fatalf("local total out of range: %d", localTotal)
	}
	for _, dim := range []string{
		"willingness_to_pay",
		"retention_likelihood",
		"channel_access",
		"operational_friction",
		"business_density",
		"pain_severity",
		"competition_weakness",
		"revenue_per_business",
	} {
		if _, ok := localDims[dim]; !ok {
			t.Fatalf("expected %s dimension in local rubric", dim)
		}
	}

	rubricSaaS, saasDims, saasViability, saasMarket, saasTotal := computeScore("saas_gap", "AI Dispatch", "Asuncion, Paraguay")
	if rubricSaaS != "saas_gap_rubric" {
		t.Fatalf("expected saas rubric, got %s", rubricSaaS)
	}
	if saasViability < 0 || saasViability > 100 {
		t.Fatalf("saas viability out of range: %d", saasViability)
	}
	if saasMarket < 0 || saasMarket > 100 {
		t.Fatalf("saas market out of range: %d", saasMarket)
	}
	if saasTotal < 0 || saasTotal > 100 {
		t.Fatalf("saas total out of range: %d", saasTotal)
	}
	for _, dim := range []string{
		"willingness_to_pay",
		"retention_likelihood",
		"technical_feasibility",
		"distribution_access",
		"regulatory_moat",
		"competition_weakness",
		"pain_severity",
		"market_size",
		"localization_advantage",
	} {
		if _, ok := saasDims[dim]; !ok {
			t.Fatalf("expected %s dimension in saas rubric", dim)
		}
	}
}
