package factory

import (
	"context"
	"testing"
	"time"

	"empireai/internal/store"
	"empireai/internal/testutil"
)

func TestPipeline_RunScan_DiscoveryOnlyAndFull(t *testing.T) {
	t.Setenv("GOOGLE_MAPS_API_KEY", "")
	t.Setenv("YELP_API_KEY", "")

	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	p := NewPipeline(db, pg, pg)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Discovery depth should stop after inserting discovered verticals.
	sum, err := p.RunScan(ctx, "Austin, TX", "discovery", 2)
	if err != nil {
		t.Fatalf("RunScan discovery: %v", err)
	}
	if sum.Discovered != 2 || sum.Scored != 0 || sum.ReadyForReview != 0 {
		t.Fatalf("unexpected discovery summary: %+v", sum)
	}

	// Full depth should score + validate and produce mailbox items.
	sum, err = p.RunScan(ctx, "Austin, TX", "full", 2)
	if err != nil {
		t.Fatalf("RunScan full: %v", err)
	}
	if sum.Discovered != 0 || sum.Scored != 2 {
		t.Fatalf("unexpected full summary: %+v", sum)
	}
	// Some may be killed depending on deterministic scoring hash; ensure no panic and at least one processed.
	if len(sum.VerticalIDs) != 2 {
		t.Fatalf("expected 2 vertical ids, got %d", len(sum.VerticalIDs))
	}
}

func TestPipeline_RunPending_ScoresDiscoveredThenValidates(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	p := NewPipeline(db, pg, pg)
	ctx := context.Background()

	// Seed a discovered vertical.
	var vID string
	if err := db.QueryRowContext(ctx, `
		INSERT INTO verticals (name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ('V', 'vslug', 'us', 'discovered', 'factory', now(), now())
		RETURNING id::text
	`).Scan(&vID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}

	sum, err := p.RunPending(ctx, 10)
	if err != nil {
		t.Fatalf("RunPending: %v", err)
	}
	if sum.Scored < 1 {
		t.Fatalf("expected scored >= 1, got %+v", sum)
	}
	if len(sum.VerticalIDs) != 1 || sum.VerticalIDs[0] != vID {
		t.Fatalf("unexpected pending ids: %+v", sum.VerticalIDs)
	}

	// validateVertical path should eventually write a validation kit or kill_reason.
	var stage string
	if err := db.QueryRowContext(ctx, `SELECT stage FROM verticals WHERE id=$1::uuid`, vID).Scan(&stage); err != nil {
		t.Fatalf("load stage: %v", err)
	}
	if stage == "" {
		t.Fatalf("expected stage updated")
	}
}

func TestPipeline_RunPending_IncludesSpecReviewStage(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	p := NewPipeline(db, pg, pg)
	ctx := context.Background()

	var vID string
	if err := db.QueryRowContext(ctx, `
		INSERT INTO verticals (name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ('Spec Review Vertical', 'spec-review-v', 'us', 'spec_review', 'factory', now(), now())
		RETURNING id::text
	`).Scan(&vID); err != nil {
		t.Fatalf("seed spec_review vertical: %v", err)
	}

	sum, err := p.RunPending(ctx, 10)
	if err != nil {
		t.Fatalf("RunPending: %v", err)
	}
	found := false
	for _, id := range sum.VerticalIDs {
		if id == vID {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected spec_review vertical to be processed, ids=%v", sum.VerticalIDs)
	}
}

func TestPipeline_ValidationMailboxHasTimeout(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	p := NewPipeline(db, pg, pg)
	ctx := context.Background()

	var vID string
	if err := db.QueryRowContext(ctx, `
		INSERT INTO verticals (name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ('Timeout Vertical', 'timeout-v', 'us', 'shortlisted', 'factory', now(), now())
		RETURNING id::text
	`).Scan(&vID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}

	ready, err := p.validateVertical(ctx, vID)
	if err != nil {
		t.Fatalf("validateVertical: %v", err)
	}
	if !ready {
		t.Fatalf("expected ready_for_review")
	}

	var timeoutAt time.Time
	if err := db.QueryRowContext(ctx, `
		SELECT timeout_at
		FROM mailbox
		WHERE vertical_id = $1::uuid
		  AND type = 'vertical_decision'
		ORDER BY created_at DESC
		LIMIT 1
	`, vID).Scan(&timeoutAt); err != nil {
		t.Fatalf("load mailbox timeout: %v", err)
	}
	if timeoutAt.IsZero() {
		t.Fatal("expected non-zero mailbox timeout")
	}
	if timeoutAt.Before(time.Now().Add(47*time.Hour)) || timeoutAt.After(time.Now().Add(49*time.Hour)) {
		t.Fatalf("unexpected timeout window: %s", timeoutAt.UTC().Format(time.RFC3339))
	}
}
