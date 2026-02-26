package factory

import (
	"context"
	"testing"

	"empireai/internal/store"
	"empireai/internal/testutil"
)

func TestFactoryPipeline_RunScan_RunPending_And_Transitions(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()

	// Seed holding agents so pipeline.emit can persist deliveries.
	for _, id := range []string{"empire-coordinator", "spec-auditor", "factory-cto"} {
		if _, err := db.ExecContext(ctx, `
			INSERT INTO agents (id, type, role, mode, status, config)
			VALUES ($1, 'stub', $1, 'holding', 'active', '{}'::jsonb)
			ON CONFLICT (id) DO NOTHING
		`, id); err != nil {
			t.Fatalf("seed agent %s: %v", id, err)
		}
	}

	p := NewPipeline(db, pg, pg)

	// Input validation branch.
	if _, err := p.RunScan(ctx, "", "discovery", 1); err == nil {
		t.Fatal("expected missing geography error")
	}

	// Discovery-only depth returns after inserting discovered verticals.
	sum, err := p.RunScan(ctx, "us", "discovery", 2)
	if err != nil {
		t.Fatalf("RunScan discovery: %v", err)
	}
	if sum.Discovered == 0 || len(sum.VerticalIDs) == 0 {
		t.Fatalf("expected discoveries, got %+v", sum)
	}

	// Full scan exercises scoring + validation + mailbox insertion.
	sum2, err := p.RunScan(ctx, "us", "full", 1)
	if err != nil {
		t.Fatalf("RunScan full: %v", err)
	}
	if sum2.Discovered != 0 || sum2.Scored != 1 {
		t.Fatalf("unexpected summary: %+v", sum2)
	}

	// RunPending should process remaining factory verticals.
	if _, err := p.RunPending(ctx, 10); err != nil {
		t.Fatalf("RunPending: %v", err)
	}

	// Stage transition helper coverage.
	if err := validateStageTransition("unknown", "discovered"); err == nil {
		t.Fatal("expected unknown stage error")
	}
	if err := validateStageTransition("discovered", "discovered"); err != nil {
		t.Fatalf("same-stage should be ok: %v", err)
	}
	if err := validateStageTransition("discovered", "operating"); err == nil {
		t.Fatal("expected invalid transition error")
	}
}
