package factory

import (
	"context"
	"fmt"
	"testing"

	"empireai/internal/store"
	"empireai/internal/testutil"
)

type failingScanner struct {
	name string
}

func (s failingScanner) Name() string { return s.name }

func (s failingScanner) Scan(context.Context, string, string) ([]Signal, error) {
	return nil, fmt.Errorf("scanner failure")
}

type fixedScanner struct {
	name    string
	signals []Signal
}

func (s fixedScanner) Name() string { return s.name }

func (s fixedScanner) Scan(context.Context, string, string) ([]Signal, error) {
	out := make([]Signal, 0, len(s.signals))
	out = append(out, s.signals...)
	return out, nil
}

func TestPipeline_RunScan_ContinuesOnScannerFailure(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	p := NewPipeline(db, pg, pg)
	p.Scanners = []Scanner{
		failingScanner{name: "broken"},
		fixedScanner{name: "fallback", signals: []Signal{
			{Source: "instagram", Lead: "pet grooming in asuncion", Score: 78},
			{Source: "reviews", Lead: "manual scheduling pain", Score: 74},
		}},
	}

	ctx := context.Background()
	sum, err := p.RunScan(ctx, "Asuncion, Paraguay", "discovery", 1)
	if err != nil {
		t.Fatalf("RunScan: %v", err)
	}
	if sum.Discovered < 1 {
		t.Fatalf("expected discovery from healthy scanner, got %+v", sum)
	}

	var failed int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE type='scan.scanner_failed'`).Scan(&failed); err != nil {
		t.Fatalf("count scan.scanner_failed: %v", err)
	}
	if failed < 1 {
		t.Fatalf("expected scanner failure event, got %d", failed)
	}
}

func TestRulesScoringEngine_UsesSignalQuality(t *testing.T) {
	engine := RulesScoringEngine{}
	ctx := context.Background()

	low, err := engine.Score(ctx, "local_services", "Ops", "Asuncion", []Signal{
		{Source: "google_maps", Lead: "basic listing", Score: 35},
		{Source: "instagram", Lead: "few followers", Score: 30},
	})
	if err != nil {
		t.Fatalf("low score eval: %v", err)
	}
	high, err := engine.Score(ctx, "local_services", "Ops", "Asuncion", []Signal{
		{Source: "google_maps", Lead: "high demand in asuncion", Score: 88},
		{Source: "instagram", Lead: "strong channel traction", Score: 84},
		{Source: "reviews", Lead: "manual no-show pain", Score: 90},
	})
	if err != nil {
		t.Fatalf("high score eval: %v", err)
	}
	if high.Total <= low.Total {
		t.Fatalf("expected higher-quality signals to produce higher total: low=%d high=%d", low.Total, high.Total)
	}
	if high.Viability <= low.Viability {
		t.Fatalf("expected higher-quality signals to increase viability: low=%d high=%d", low.Viability, high.Viability)
	}
}
