package factory

import (
	"context"
	"testing"

	"empireai/internal/store"
	"empireai/internal/testutil"
)

func TestPipeline_RunScanAndPending(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}

	p := NewPipeline(db, pg, pg)
	ctx := context.Background()

	// No API keys set => scanners synthesize signals deterministically.
	sum, err := p.RunScan(ctx, "us", "full", 2)
	if err != nil {
		t.Fatalf("RunScan: %v", err)
	}
	if sum.Discovered == 0 || len(sum.VerticalIDs) == 0 {
		t.Fatalf("expected discovered verticals, got %+v", sum)
	}

	// Pending run should be safe even if nothing is pending; it should not error.
	_, err = p.RunPending(ctx, 10)
	if err != nil {
		t.Fatalf("RunPending: %v", err)
	}
}

