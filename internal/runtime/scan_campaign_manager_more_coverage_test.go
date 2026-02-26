package runtime

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"empireai/internal/events"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestScanCampaignManager_EmitCampaignCompletedIfDone(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	bus := NewEventBus(InMemoryEventStore{})
	store := &scanStoreStub{}
	manager := NewScanCampaignManager(bus, store, db)

	geoID := uuid.NewString()
	campaignID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO geographies (id, name, country, scan_config, created_at)
		VALUES ($1::uuid, 'Argentina', 'Argentina', '{}'::jsonb, now())
	`, geoID); err != nil {
		t.Fatalf("seed geography: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO scan_campaigns (
			id, geography_id, mode, priority, status, discoveries, directive_id, strategic_context, created_at, started_at, completed_at
		) VALUES (
			$1::uuid, $2::uuid, 'saas_gap', 'high', 'completed', 2, NULL, '{"directive_text":"SaaS in Argentina"}'::jsonb,
			now() - interval '2 hour', now() - interval '90 minute', now() - interval '80 minute'
		)
	`, campaignID, geoID); err != nil {
		t.Fatalf("seed campaign: %v", err)
	}

	ch := bus.Subscribe("watch-campaign-completed", events.EventType("campaign.completed"))
	emitted := manager.emitCampaignCompletedIfDone(ctx, campaignID, 3, "evt-source-1")
	if !emitted {
		t.Fatal("expected campaign.completed emission")
	}

	select {
	case evt := <-ch:
		var payload map[string]any
		if err := json.Unmarshal(evt.Payload, &payload); err != nil {
			t.Fatalf("decode campaign payload: %v", err)
		}
		if strings.TrimSpace(asString(payload["campaign_id"])) != campaignID {
			t.Fatalf("unexpected campaign_id in payload: %+v", payload)
		}
		if strings.TrimSpace(asString(payload["priority"])) != "high" {
			t.Fatalf("expected priority=high in payload: %+v", payload)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected campaign.completed event")
	}

	// If another active/queued campaign exists for the same geography, no summary event should emit.
	if _, err := db.ExecContext(ctx, `
		INSERT INTO scan_campaigns (id, geography_id, mode, priority, status, discoveries, created_at)
		VALUES ($1::uuid, $2::uuid, 'saas_trend', 'normal', 'queued', 0, now())
	`, uuid.NewString(), geoID); err != nil {
		t.Fatalf("seed follow-on campaign: %v", err)
	}
	if emitted := manager.emitCampaignCompletedIfDone(ctx, campaignID, 1, "evt-source-2"); emitted {
		t.Fatal("expected no campaign.completed while additional campaigns remain")
	}
}

func TestScanCampaignManager_BackpressurePauseResumeAndHelpers(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	store := &scanStoreStub{}
	manager := NewScanCampaignManager(NewEventBus(InMemoryEventStore{}), store, db)

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'V', 'v', 'us', 'ready_for_review', 'factory', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}
	// Exceed backpressure threshold (default: 5 pending vertical_approval items).
	for i := 0; i < 6; i++ {
		if _, err := db.ExecContext(ctx, `
			INSERT INTO mailbox (id, vertical_id, from_agent, type, priority, status, context, summary, created_at)
			VALUES ($1::uuid, $2::uuid, 'validation-coordinator', 'vertical_approval', 'normal', 'pending', '{}'::jsonb, 'review', now())
		`, uuid.NewString(), verticalID); err != nil {
			t.Fatalf("seed mailbox item %d: %v", i, err)
		}
	}

	if pending, err := manager.pendingMailboxCount(ctx); err != nil || pending != 6 {
		t.Fatalf("expected pending mailbox count=6, got pending=%d err=%v", pending, err)
	}
	if !manager.shouldPauseForBackpressure(ctx) {
		t.Fatal("expected manager to pause for mailbox backpressure")
	}
	if store.pauseCalls == 0 {
		t.Fatal("expected PauseQueuedScanCampaigns to be called")
	}

	if _, err := db.ExecContext(ctx, `UPDATE mailbox SET status = 'approved' WHERE status = 'pending'`); err != nil {
		t.Fatalf("clear mailbox pending status: %v", err)
	}
	if manager.shouldPauseForBackpressure(ctx) {
		t.Fatal("expected manager to resume after backpressure clears")
	}
	if store.resumeCalls == 0 {
		t.Fatal("expected ResumePausedScanCampaigns to be called")
	}

	manager.budgetPaused = true
	if !manager.shouldPauseForBackpressure(ctx) {
		t.Fatal("budget pause should short-circuit as paused")
	}
	manager.resetFlags()
	if manager.budgetPaused || manager.backpressurePaused {
		t.Fatalf("resetFlags should clear pause flags: budget=%v backpressure=%v", manager.budgetPaused, manager.backpressurePaused)
	}

	if got := sanitizeGeographyPhrase("argentina with complex filters and extras"); got != "Argentina" {
		t.Fatalf("unexpected sanitizeGeographyPhrase result: %q", got)
	}
}
