package store

import (
	"context"
	"testing"
	"time"

	"empireai/internal/runtime"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestScanCampaigns_CRUDAndTransitions(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	s := &PostgresStore{DB: db}
	ctx := context.Background()

	geoID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO geographies (id, name, country, region, created_at)
		VALUES ($1::uuid, 'San Francisco', 'US', 'CA', now())
	`, geoID); err != nil {
		t.Fatalf("seed geography: %v", err)
	}

	// Create (defaults + categories + next_rescan_at).
	next := time.Now().UTC().Add(2 * time.Hour).Truncate(time.Second)
	c1, err := s.CreateScanCampaign(ctx, runtime.CreateScanCampaignInput{
		GeographyID:     geoID,
		Mode:            "factory",
		Categories:      []string{"saas", "b2b"},
		RescanInterval:  "30d",
		NextRescanAt:    &next,
		Status:          "", // default queued
		Priority:        "", // default normal
	})
	if err != nil {
		t.Fatalf("CreateScanCampaign: %v", err)
	}
	if c1.ID == "" || c1.Status != "queued" || c1.Priority != "normal" {
		t.Fatalf("unexpected create output: %+v", c1)
	}
	if len(c1.Categories) != 2 || c1.Categories[0] != "saas" {
		t.Fatalf("unexpected categories: %#v", c1.Categories)
	}
	if c1.NextRescanAt == nil || !c1.NextRescanAt.Equal(next) {
		t.Fatalf("expected next_rescan_at=%s got=%v", next.Format(time.RFC3339), c1.NextRescanAt)
	}

	// Second campaign for priority ordering.
	c2, err := s.CreateScanCampaign(ctx, runtime.CreateScanCampaignInput{
		GeographyID: geoID,
		Mode:        "factory",
		Priority:    "high",
		Status:      "queued",
	})
	if err != nil {
		t.Fatalf("CreateScanCampaign2: %v", err)
	}
	if c2.Priority != "high" {
		t.Fatalf("expected high priority, got %q", c2.Priority)
	}

	// List filter.
	list, err := s.ListScanCampaigns(ctx, runtime.ScanCampaignFilter{Status: "queued", Limit: 10})
	if err != nil {
		t.Fatalf("ListScanCampaigns: %v", err)
	}
	if len(list) < 2 {
		t.Fatalf("expected 2 queued campaigns, got %d", len(list))
	}

	// If any campaign is active, claiming is blocked.
	if _, err := db.ExecContext(ctx, `UPDATE scan_campaigns SET status='active' WHERE id=$1::uuid`, c1.ID); err != nil {
		t.Fatalf("set active: %v", err)
	}
	_, ok, err := s.ClaimNextDueScanCampaign(ctx)
	if err != nil || ok {
		t.Fatalf("expected no claim when active exists ok=%v err=%v", ok, err)
	}

	// Remove active and claim; should pick high priority queued campaign.
	if _, err := db.ExecContext(ctx, `UPDATE scan_campaigns SET status='queued' WHERE id=$1::uuid`, c1.ID); err != nil {
		t.Fatalf("reset queued: %v", err)
	}
	claimed, ok, err := s.ClaimNextDueScanCampaign(ctx)
	if err != nil || !ok {
		t.Fatalf("ClaimNextDueScanCampaign ok=%v err=%v", ok, err)
	}
	if claimed.ID != c2.ID || claimed.Status != "active" {
		t.Fatalf("expected claimed=%s active got=%+v", c2.ID, claimed)
	}

	// Mark completed sets discoveries and next_rescan_at based on rescan_interval.
	if _, err := db.ExecContext(ctx, `UPDATE scan_campaigns SET rescan_interval='30d' WHERE id=$1::uuid`, claimed.ID); err != nil {
		t.Fatalf("set rescan_interval: %v", err)
	}
	if err := s.MarkScanCampaignCompleted(ctx, claimed.ID, 7); err != nil {
		t.Fatalf("MarkScanCampaignCompleted: %v", err)
	}
	var status string
	var discoveries int
	var nextAt *time.Time
	if err := db.QueryRowContext(ctx, `
		SELECT status, COALESCE(discoveries,0), next_rescan_at
		FROM scan_campaigns
		WHERE id=$1::uuid
	`, claimed.ID).Scan(&status, &discoveries, &nextAt); err != nil {
		t.Fatalf("load completed: %v", err)
	}
	if status != "completed" || discoveries != 7 || nextAt == nil {
		t.Fatalf("expected completed/discoveries/next_rescan_at got status=%s discoveries=%d next=%v", status, discoveries, nextAt)
	}

	// Requeue due rescans resets to queued.
	now := time.Now().UTC().Add(45 * 24 * time.Hour)
	if _, err := db.ExecContext(ctx, `UPDATE scan_campaigns SET next_rescan_at=$2 WHERE id=$1::uuid`, claimed.ID, now.Add(-time.Minute)); err != nil {
		t.Fatalf("set next_rescan_at past: %v", err)
	}
	n, err := s.RequeueDueRescans(ctx, now)
	if err != nil {
		t.Fatalf("RequeueDueRescans: %v", err)
	}
	if n < 1 {
		t.Fatalf("expected requeue >=1, got %d", n)
	}

	// Pause/resume.
	paused, err := s.PauseQueuedScanCampaigns(ctx)
	if err != nil || paused < 1 {
		t.Fatalf("PauseQueuedScanCampaigns paused=%d err=%v", paused, err)
	}
	resumed, err := s.ResumePausedScanCampaigns(ctx)
	if err != nil || resumed < 1 {
		t.Fatalf("ResumePausedScanCampaigns resumed=%d err=%v", resumed, err)
	}

	// Geography label joins the parts.
	label, err := s.LookupGeographyLabel(ctx, geoID)
	if err != nil {
		t.Fatalf("LookupGeographyLabel: %v", err)
	}
	if label != "San Francisco, CA, US" {
		t.Fatalf("unexpected label: %q", label)
	}
}

func TestParseRescanInterval(t *testing.T) {
	if d := parseRescanInterval(""); d != 0 {
		t.Fatalf("expected 0 for empty, got %v", d)
	}
	if d := parseRescanInterval("30d"); d != 30*24*time.Hour {
		t.Fatalf("expected 30d got %v", d)
	}
	if d := parseRescanInterval("1h"); d != time.Hour {
		t.Fatalf("expected 1h got %v", d)
	}
	if d := parseRescanInterval("bad"); d != 0 {
		t.Fatalf("expected 0 for bad got %v", d)
	}
	if d := parseRescanInterval("-1h"); d != 0 {
		t.Fatalf("expected 0 for negative got %v", d)
	}
	if d := parseRescanInterval("0d"); d != 0 {
		t.Fatalf("expected 0 for 0d got %v", d)
	}
}

func TestCreateScanCampaign_Validations(t *testing.T) {
	ctx := context.Background()
	if _, err := (*PostgresStore)(nil).CreateScanCampaign(ctx, runtime.CreateScanCampaignInput{}); err == nil {
		t.Fatalf("expected store required error")
	}
	_, db, _ := testutil.StartPostgres(t)
	s := &PostgresStore{DB: db}
	if _, err := s.CreateScanCampaign(ctx, runtime.CreateScanCampaignInput{GeographyID: "", Mode: ""}); err == nil {
		t.Fatalf("expected required fields error")
	}
}

func TestScanCampaigns_MoreBranches(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	s := &PostgresStore{DB: db}
	ctx := context.Background()

	geoID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO geographies (id, name, country, region, created_at)
		VALUES ($1::uuid, 'X', 'US', NULL, now())
	`, geoID); err != nil {
		t.Fatalf("seed geography: %v", err)
	}

	// Claim none queued -> ok=false.
	if _, ok, err := s.ClaimNextDueScanCampaign(ctx); err != nil || ok {
		t.Fatalf("expected no claim ok=%v err=%v", ok, err)
	}

	// List defaults (status empty, limit <=0).
	if _, err := s.CreateScanCampaign(ctx, runtime.CreateScanCampaignInput{GeographyID: geoID, Mode: "factory"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	list, err := s.ListScanCampaigns(ctx, runtime.ScanCampaignFilter{})
	if err != nil || len(list) == 0 {
		t.Fatalf("ListScanCampaigns default err=%v len=%d", err, len(list))
	}

	// Mark completed validations.
	if err := s.MarkScanCampaignCompleted(ctx, "", 1); err == nil {
		t.Fatalf("expected campaign_id required")
	}
	// Invalid interval -> no next_rescan_at.
	c, err := s.CreateScanCampaign(ctx, runtime.CreateScanCampaignInput{GeographyID: geoID, Mode: "factory", Status: "queued", RescanInterval: "bad"})
	if err != nil {
		t.Fatalf("create bad interval: %v", err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE scan_campaigns SET status='active' WHERE id=$1::uuid`, c.ID); err != nil {
		t.Fatalf("set active: %v", err)
	}
	if err := s.MarkScanCampaignCompleted(ctx, c.ID, -5); err != nil {
		t.Fatalf("MarkScanCampaignCompleted: %v", err)
	}
	var next *time.Time
	if err := db.QueryRowContext(ctx, `SELECT next_rescan_at FROM scan_campaigns WHERE id=$1::uuid`, c.ID).Scan(&next); err != nil {
		t.Fatalf("load next_rescan_at: %v", err)
	}
	if next != nil {
		t.Fatalf("expected next_rescan_at NULL for invalid interval")
	}

	// Lookup geography validations.
	if _, err := s.LookupGeographyLabel(ctx, ""); err == nil {
		t.Fatalf("expected geography_id required")
	}

	// RequeueDueRescans now.IsZero branch.
	if _, err := s.RequeueDueRescans(ctx, time.Time{}); err != nil {
		t.Fatalf("RequeueDueRescans: %v", err)
	}
}
