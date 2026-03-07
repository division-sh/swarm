package runtime

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"empireai/internal/events"
	runtimepipeline "empireai/internal/runtime/pipeline"
	"empireai/internal/testutil"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

type directiveCampaignStore struct {
	db      *sql.DB
	created []CreateScanCampaignInput
}

func (s *directiveCampaignStore) CreateScanCampaign(ctx context.Context, in CreateScanCampaignInput) (ScanCampaign, error) {
	s.created = append(s.created, in)
	id := uuid.NewString()
	priority := strings.TrimSpace(in.Priority)
	if priority == "" {
		priority = "normal"
	}
	status := strings.TrimSpace(in.Status)
	if status == "" {
		status = "queued"
	}
	strategic := strings.TrimSpace(string(in.StrategicContext))
	if strategic == "" {
		strategic = "{}"
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO scan_campaigns (
			id, geography_id, mode, categories, priority, status, strategic_context, deadline_at, created_at
		) VALUES (
			$1::uuid, $2::uuid, $3, $4::text[], $5, $6, $7::jsonb, $8, now()
		)
	`, id, in.GeographyID, in.Mode, pq.Array(in.Categories), priority, status, strategic, in.DeadlineAt)
	if err != nil {
		return ScanCampaign{}, err
	}
	return ScanCampaign{
		ID:               id,
		GeographyID:      in.GeographyID,
		DirectiveID:      "",
		Mode:             in.Mode,
		Categories:       append([]string(nil), in.Categories...),
		Priority:         priority,
		Status:           status,
		StrategicContext: in.StrategicContext,
		DeadlineAt:       in.DeadlineAt,
		CreatedAt:        time.Now().UTC(),
	}, nil
}

func (s *directiveCampaignStore) ListScanCampaigns(context.Context, ScanCampaignFilter) ([]ScanCampaign, error) {
	return nil, nil
}

func (s *directiveCampaignStore) ClaimNextDueScanCampaign(context.Context) (ScanCampaign, bool, error) {
	return ScanCampaign{}, false, nil
}

func (s *directiveCampaignStore) LookupGeographyLabel(context.Context, string) (string, error) {
	return "", nil
}

func (s *directiveCampaignStore) MarkScanCampaignCompleted(ctx context.Context, campaignID string, discoveries int) error {
	return s.markCompleted(ctx, campaignID, discoveries)
}

func (s *directiveCampaignStore) RequeueDueRescans(context.Context, time.Time) (int, error) {
	return 0, nil
}

func (s *directiveCampaignStore) PauseQueuedScanCampaigns(context.Context) (int, error) {
	return 0, nil
}

func (s *directiveCampaignStore) ResumePausedScanCampaigns(context.Context) (int, error) {
	return 0, nil
}

func (s *directiveCampaignStore) markCompleted(ctx context.Context, campaignID string, discoveries int) error {
	if s == nil || s.db == nil {
		return nil
	}
	campaignID = strings.TrimSpace(campaignID)
	if campaignID == "" {
		return nil
	}
	if discoveries < 0 {
		discoveries = 0
	}
	_, err := s.db.ExecContext(ctx, `
		UPDATE scan_campaigns
		SET status = 'completed',
		    discoveries = $2,
		    completed_at = now(),
		    started_at = COALESCE(started_at, now())
		WHERE id = $1::uuid
	`, campaignID, discoveries)
	return err
}

func TestScanCampaignManager_OnDirective_QueuesDeterministicModes(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	bus := NewEventBus(InMemoryEventStore{})
	store := &directiveCampaignStore{db: db}
	manager := runtimepipeline.NewScanCampaignManager(bus, store, newScanCampaignHooksForTest(), db)

	manager.OnDirectiveForTest(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("system.directive"),
		SourceAgent: "human",
		Payload: mustJSON(map[string]any{
			"directive_text": "SaaS in Argentina",
			"sent_by":        "dashboard",
		}),
		CreatedAt: time.Now().UTC(),
	})

	if len(store.created) != 3 {
		t.Fatalf("expected 3 queued campaigns, got %d", len(store.created))
	}
	gotModes := []string{
		store.created[0].Mode,
		store.created[1].Mode,
		store.created[2].Mode,
	}
	wantModes := []string{"saas_gap", "saas_trend", "local_services"}
	for i := range wantModes {
		if gotModes[i] != wantModes[i] {
			t.Fatalf("expected mode[%d]=%s, got %s (all=%v)", i, wantModes[i], gotModes[i], gotModes)
		}
	}

	var geographyName string
	if err := db.QueryRowContext(ctx, `SELECT name FROM geographies ORDER BY created_at DESC LIMIT 1`).Scan(&geographyName); err != nil {
		t.Fatalf("query geography: %v", err)
	}
	if strings.TrimSpace(geographyName) != "Argentina" {
		t.Fatalf("expected directive geography Argentina, got %q", geographyName)
	}

	rows, err := db.QueryContext(ctx, `
		SELECT mode, priority, status, COALESCE(strategic_context->>'directive_text', '')
		FROM scan_campaigns
	`)
	if err != nil {
		t.Fatalf("query scan campaigns: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var mode, priority, status, directiveText string
		if err := rows.Scan(&mode, &priority, &status, &directiveText); err != nil {
			t.Fatalf("scan campaign row: %v", err)
		}
		count++
		if strings.TrimSpace(priority) != "normal" {
			t.Fatalf("expected campaign priority=normal, got %q", priority)
		}
		if strings.TrimSpace(status) != "queued" {
			t.Fatalf("expected campaign status=queued, got %q", status)
		}
		if strings.TrimSpace(directiveText) != "SaaS in Argentina" {
			t.Fatalf("expected strategic_context.directive_text propagated, got %q", directiveText)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate scan campaigns: %v", err)
	}
	if count != 3 {
		t.Fatalf("expected 3 scan campaign rows, got %d", count)
	}
}

func TestScanCampaignManager_OnDirective_ComplexForwardsToCoordinator(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	bus := NewEventBus(InMemoryEventStore{})
	store := &directiveCampaignStore{db: db}
	manager := runtimepipeline.NewScanCampaignManager(bus, store, newScanCampaignHooksForTest(), db)

	ch := bus.Subscribe("empire-coordinator", events.EventType("system.directive"))
	manager.OnDirectiveForTest(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("system.directive"),
		SourceAgent: "human",
		Payload: mustJSON(map[string]any{
			"directive_text": "Focus on compliance-driven opportunities in LATAM countries with over 80 percent internet penetration",
		}),
		CreatedAt: time.Now().UTC(),
	})

	select {
	case forwarded := <-ch:
		if strings.TrimSpace(forwarded.SourceAgent) != "scan-campaign-manager" {
			t.Fatalf("expected forwarded directive source=scan-campaign-manager, got %q", forwarded.SourceAgent)
		}
		var payload map[string]any
		if err := json.Unmarshal(forwarded.Payload, &payload); err != nil {
			t.Fatalf("decode forwarded payload: %v", err)
		}
		if strings.TrimSpace(asString(payload["forwarded_by"])) != "scan-campaign-manager" {
			t.Fatalf("expected forwarded_by marker, got payload=%v", payload)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected complex directive to be forwarded directly to empire-coordinator")
	}

	var campaigns int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM scan_campaigns`).Scan(&campaigns); err != nil {
		t.Fatalf("count scan campaigns: %v", err)
	}
	if campaigns != 0 {
		t.Fatalf("expected no queued campaigns for complex directive, got %d", campaigns)
	}
}

func TestScanCampaignManager_OnDirective_CorpusQueuesWithPath(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	bus := NewEventBus(InMemoryEventStore{})
	store := &directiveCampaignStore{db: db}
	manager := runtimepipeline.NewScanCampaignManager(bus, store, newScanCampaignHooksForTest(), db)

	manager.OnDirectiveForTest(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("system.directive"),
		SourceAgent: "human",
		Payload: mustJSON(map[string]any{
			"directive_text": "US, corpus, corpus_path=/data/test-signals-25.jsonl",
		}),
		CreatedAt: time.Now().UTC(),
	})

	if len(store.created) != 1 {
		t.Fatalf("expected 1 corpus campaign, got %d", len(store.created))
	}
	if store.created[0].Mode != "corpus" {
		t.Fatalf("expected corpus mode queued, got %q", store.created[0].Mode)
	}
	var strategic map[string]any
	if err := json.Unmarshal(store.created[0].StrategicContext, &strategic); err != nil {
		t.Fatalf("decode strategic context: %v", err)
	}
	if got := strings.TrimSpace(asString(strategic["corpus_path"])); got != "/data/test-signals-25.jsonl" {
		t.Fatalf("expected corpus_path propagated, got %q", got)
	}
}

func TestScanCampaignManager_Tick_CompletesExpiredCampaignByTimeCap(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	bus := NewEventBus(&postgresEventStore{db: db})
	store := &directiveCampaignStore{db: db}
	manager := runtimepipeline.NewScanCampaignManager(bus, store, newScanCampaignHooksForTest(), db)

	geoID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO geographies (id, name, country, scan_config, created_at)
		VALUES ($1::uuid, 'Argentina', 'Argentina', '{}'::jsonb, now())
	`, geoID); err != nil {
		t.Fatalf("seed geography: %v", err)
	}

	campaignID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO scan_campaigns (
			id, geography_id, mode, categories, priority, status, discoveries, strategic_context, deadline_at, created_at, started_at
		) VALUES (
			$1::uuid, $2::uuid, 'saas_gap', '{}'::text[], 'normal', 'active', 2, '{}'::jsonb, now() - interval '2 minutes', now() - interval '2 hours', now() - interval '90 minutes'
		)
	`, campaignID, geoID); err != nil {
		t.Fatalf("seed expired scan campaign: %v", err)
	}

	campaignCompletedCh := bus.Subscribe("watch-expired-campaign", events.EventType("campaign.completed"))
	manager.TickForTest(ctx)

	select {
	case evt := <-campaignCompletedCh:
		payload := parsePayloadMap(evt.Payload)
		if got := strings.TrimSpace(asString(payload["campaign_id"])); got != campaignID {
			t.Fatalf("expected campaign.completed campaign_id=%s, got payload=%v", campaignID, payload)
		}
		if got := strings.TrimSpace(asString(payload["source_event_id"])); got != "campaign_time_cap_exceeded" {
			t.Fatalf("expected source_event_id=campaign_time_cap_exceeded, got payload=%v", payload)
		}
	case <-time.After(800 * time.Millisecond):
		t.Fatal("expected campaign.completed event for expired campaign")
	}

	var status string
	var discoveries int
	var completedAt sql.NullTime
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(status, ''), COALESCE(discoveries, 0), completed_at
		FROM scan_campaigns
		WHERE id = $1::uuid
	`, campaignID).Scan(&status, &discoveries, &completedAt); err != nil {
		t.Fatalf("query completed campaign: %v", err)
	}
	if strings.TrimSpace(status) != "completed" {
		t.Fatalf("expected expired campaign status=completed, got %q", status)
	}
	if discoveries != 2 {
		t.Fatalf("expected discoveries preserved at 2, got %d", discoveries)
	}
	if !completedAt.Valid {
		t.Fatal("expected completed_at to be set after campaign time cap completion")
	}
}
