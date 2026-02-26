package runtime

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"empireai/internal/events"
	"empireai/internal/testutil"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

type holdingFlowCampaignStore struct {
	db *sql.DB
}

func (s *holdingFlowCampaignStore) CreateScanCampaign(ctx context.Context, in CreateScanCampaignInput) (ScanCampaign, error) {
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
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO scan_campaigns (
			id, geography_id, directive_id, mode, categories, priority, status, strategic_context, deadline_at, created_at
		)
		VALUES ($1::uuid, $2::uuid, NULL::uuid, $3, $4::text[], $5, $6, $7::jsonb, $8, now())
	`, id, in.GeographyID, in.Mode, pq.Array(in.Categories), priority, status, strategic, in.DeadlineAt); err != nil {
		return ScanCampaign{}, err
	}
	return ScanCampaign{
		ID:               id,
		GeographyID:      in.GeographyID,
		DirectiveID:      strings.TrimSpace(in.DirectiveID),
		Mode:             in.Mode,
		Categories:       append([]string(nil), in.Categories...),
		Priority:         priority,
		Status:           status,
		StrategicContext: in.StrategicContext,
		DeadlineAt:       in.DeadlineAt,
		CreatedAt:        time.Now().UTC(),
	}, nil
}

func (s *holdingFlowCampaignStore) ListScanCampaigns(context.Context, ScanCampaignFilter) ([]ScanCampaign, error) {
	return nil, nil
}

func (s *holdingFlowCampaignStore) ClaimNextDueScanCampaign(ctx context.Context) (ScanCampaign, bool, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id::text, geography_id::text, mode, COALESCE(categories, ARRAY[]::text[]), COALESCE(priority,'normal')
		FROM scan_campaigns
		WHERE status = 'queued'
		ORDER BY created_at ASC
		LIMIT 1
	`)
	var c ScanCampaign
	if err := row.Scan(&c.ID, &c.GeographyID, &c.Mode, pq.Array(&c.Categories), &c.Priority); err != nil {
		if err == sql.ErrNoRows {
			return ScanCampaign{}, false, nil
		}
		return ScanCampaign{}, false, err
	}
	if _, err := s.db.ExecContext(ctx, `
		UPDATE scan_campaigns
		SET status = 'active', started_at = now()
		WHERE id = $1::uuid
	`, c.ID); err != nil {
		return ScanCampaign{}, false, err
	}
	return c, true, nil
}

func (s *holdingFlowCampaignStore) LookupGeographyLabel(ctx context.Context, geographyID string) (string, error) {
	var name string
	if err := s.db.QueryRowContext(ctx, `
		SELECT COALESCE(name, '')
		FROM geographies
		WHERE id = $1::uuid
	`, geographyID).Scan(&name); err != nil {
		return "", err
	}
	return strings.TrimSpace(name), nil
}

func (s *holdingFlowCampaignStore) MarkScanCampaignCompleted(ctx context.Context, campaignID string, discoveries int) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE scan_campaigns
		SET status = 'completed', discoveries = $2, completed_at = now()
		WHERE id = $1::uuid
	`, campaignID, discoveries)
	return err
}

func (s *holdingFlowCampaignStore) RequeueDueRescans(context.Context, time.Time) (int, error) {
	return 0, nil
}

func (s *holdingFlowCampaignStore) PauseQueuedScanCampaigns(ctx context.Context) (int, error) {
	res, err := s.db.ExecContext(ctx, `UPDATE scan_campaigns SET status = 'paused' WHERE status = 'queued'`)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

func (s *holdingFlowCampaignStore) ResumePausedScanCampaigns(ctx context.Context) (int, error) {
	res, err := s.db.ExecContext(ctx, `UPDATE scan_campaigns SET status = 'queued' WHERE status = 'paused'`)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

func waitForEventType(t *testing.T, ch <-chan events.Event, typ string) events.Event {
	t.Helper()
	return waitForEventTypes(t, ch, []string{typ})[typ]
}

func assertNoEventType(t *testing.T, ch <-chan events.Event, typ string, d time.Duration) {
	t.Helper()
	timer := time.NewTimer(d)
	defer timer.Stop()
	for {
		select {
		case evt := <-ch:
			if strings.TrimSpace(string(evt.Type)) == strings.TrimSpace(typ) {
				t.Fatalf("unexpected event %s", typ)
			}
		case <-timer.C:
			return
		}
	}
}

func insertTestVertical(t *testing.T, db *sql.DB, verticalID, name, geography string) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, $2, $3, $4, 'shortlisted', 'factory', now(), now())
	`, verticalID, name, buildVerticalSlug(name, verticalID), geography); err != nil {
		t.Fatalf("insert vertical: %v", err)
	}
}

func TestHoldingFlow_A1_DirectiveToCampaignCreation(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	bus := NewEventBus(InMemoryEventStore{})
	store := &holdingFlowCampaignStore{db: db}
	manager := NewScanCampaignManager(bus, store, db)
	ch := bus.Subscribe("watch-a1", events.EventType("scan.requested"))

	manager.onEvent(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("system.directive"),
		SourceAgent: "human",
		Payload: mustJSON(map[string]any{
			"directive_text": "SaaS in Argentina",
			"sent_by":        "dashboard",
		}),
		CreatedAt: time.Now().UTC(),
	})

	evt := waitForEventType(t, ch, "scan.requested")
	payload := parsePayloadMap(evt.Payload)
	if strings.TrimSpace(asString(payload["campaign_id"])) == "" {
		t.Fatal("expected scan.requested payload to include campaign_id")
	}
	if strings.TrimSpace(asString(payload["mode"])) != "automation_micro" {
		t.Fatalf("expected first mode automation_micro, got %v", payload["mode"])
	}
	if strings.TrimSpace(asString(payload["geography"])) != "Argentina" {
		t.Fatalf("expected geography Argentina, got %v", payload["geography"])
	}

	var campaigns int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM scan_campaigns`).Scan(&campaigns); err != nil {
		t.Fatalf("count campaigns: %v", err)
	}
	if campaigns != 4 {
		t.Fatalf("expected 4 queued campaigns, got %d", campaigns)
	}
}

func TestHoldingFlow_A2_ScanRequestedDispatchesAndAccumulates(t *testing.T) {
	ctx := context.Background()
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, nil)
	ch := bus.Subscribe("watch-a2", events.EventType("market_research.scan_assigned"))

	pc.handleScanRequested(ctx, events.Event{
		ID:   uuid.NewString(),
		Type: events.EventType("scan.requested"),
		Payload: mustJSON(map[string]any{
			"scan_id":     "scan-a2",
			"campaign_id": "campaign-a2",
			"mode":        "saas_gap",
			"geography":   "argentina",
		}),
	})

	evt := waitForEventType(t, ch, "market_research.scan_assigned")
	payload := parsePayloadMap(evt.Payload)
	if strings.TrimSpace(asString(payload["scan_id"])) != "scan-a2" {
		t.Fatalf("expected scan_id propagated, got %v", payload["scan_id"])
	}
	if strings.TrimSpace(asString(payload["geography"])) != "argentina" {
		t.Fatalf("expected geography propagated, got %v", payload["geography"])
	}
	snaps := pc.SnapshotScans()
	if len(snaps) != 1 {
		t.Fatalf("expected one scan accumulator, got %d", len(snaps))
	}
	if asInt(snaps[0]["expected"]) != 1 {
		t.Fatalf("expected expected_agents=1, got %v", snaps[0]["expected"])
	}
}

func TestHoldingFlow_A3_DiscoveryAccumulationEmitsVerticalForHighSignal(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	ch := bus.Subscribe("watch-a3", events.EventType("vertical.discovered"))

	pc.handleScanRequested(ctx, events.Event{
		ID:   uuid.NewString(),
		Type: events.EventType("scan.requested"),
		Payload: mustJSON(map[string]any{
			"scan_id":     "scan-a3",
			"mode":        "saas_gap",
			"geography":   "argentina",
			"campaign_id": "campaign-a3",
		}),
	})
	pc.handleDiscoveryReport(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("category.assessed"),
		SourceAgent: "market-research-agent",
		Payload: mustJSON(map[string]any{
			"scan_id":         "scan-a3",
			"vertical_name":   "Pet Grooming Scheduling",
			"signal_strength": 82,
			"geography":       "argentina",
			"mode":            "saas_gap",
		}),
	})

	evt := waitForEventType(t, ch, "vertical.discovered")
	payload := parsePayloadMap(evt.Payload)
	verticalID := strings.TrimSpace(asString(payload["vertical_id"]))
	if verticalID == "" {
		t.Fatal("expected vertical.discovered with vertical_id")
	}

	var name, geography string
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(name,''), COALESCE(geography,'')
		FROM verticals
		WHERE id = $1::uuid
	`, verticalID).Scan(&name, &geography); err != nil {
		t.Fatalf("load discovered vertical: %v", err)
	}
	if strings.TrimSpace(name) != "Pet Grooming Scheduling" || strings.TrimSpace(geography) != "argentina" {
		t.Fatalf("unexpected vertical row name=%q geography=%q", name, geography)
	}
}

func TestHoldingFlow_A4_LowSignalReportSkipsDiscovery(t *testing.T) {
	ctx := context.Background()
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, nil)
	ch := bus.Subscribe("watch-a4", events.EventType("vertical.discovered"))

	pc.handleScanRequested(ctx, events.Event{
		ID:   uuid.NewString(),
		Type: events.EventType("scan.requested"),
		Payload: mustJSON(map[string]any{
			"scan_id":   "scan-a4",
			"mode":      "saas_gap",
			"geography": "paraguay",
		}),
	})
	pc.handleDiscoveryReport(ctx, events.Event{
		ID:   uuid.NewString(),
		Type: events.EventType("category.assessed"),
		Payload: mustJSON(map[string]any{
			"scan_id":         "scan-a4",
			"vertical_name":   "Low Signal Vertical",
			"signal_strength": 30,
			"geography":       "paraguay",
		}),
	})
	assertNoEventType(t, ch, "vertical.discovered", 250*time.Millisecond)

	snaps := pc.SnapshotScans()
	if len(snaps) != 1 {
		t.Fatalf("expected one scan accumulator, got %d", len(snaps))
	}
	if asInt(snaps[0]["verticals_discovered"]) != 0 || asInt(snaps[0]["verticals_skipped"]) != 1 {
		t.Fatalf("expected discovered=0 skipped=1, got %+v", snaps[0])
	}
}

func TestHoldingFlow_A5_ScanCompleteEmitsAndClearsAccumulator(t *testing.T) {
	ctx := context.Background()
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, nil)
	ch := bus.Subscribe("watch-a5", events.EventType("scan.completed"))

	pc.handleScanRequested(ctx, events.Event{
		ID:   uuid.NewString(),
		Type: events.EventType("scan.requested"),
		Payload: mustJSON(map[string]any{
			"scan_id":     "scan-a5",
			"campaign_id": "campaign-a5",
			"mode":        "saas_gap",
			"geography":   "argentina",
		}),
	})
	pc.handleScanCompletion(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("market_research.scan_complete"),
		SourceAgent: "market-research-agent",
		Payload:     mustJSON(map[string]any{"scan_id": "scan-a5"}),
	})

	evt := waitForEventType(t, ch, "scan.completed")
	payload := parsePayloadMap(evt.Payload)
	if strings.TrimSpace(asString(payload["scan_id"])) != "scan-a5" {
		t.Fatalf("expected scan_id=scan-a5, got %v", payload["scan_id"])
	}
	if asInt(payload["agents_complete"]) != 1 || asInt(payload["agents_expected"]) != 1 {
		t.Fatalf("expected agents_complete/expected=1, got payload=%v", payload)
	}
	if strings.EqualFold(strings.TrimSpace(asString(payload["timed_out"])), "true") {
		t.Fatalf("expected timed_out=false, got payload=%v", payload)
	}
	if got := len(pc.SnapshotScans()); got != 0 {
		t.Fatalf("expected no active scans after completion, got %d", got)
	}
}

func TestHoldingFlow_B1_VerticalDiscoveredReachesScoringCoordinator(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	ch := bus.Subscribe("scoring-coordinator", events.EventType("vertical.discovered"))
	if err := bus.Publish(context.Background(), events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("vertical.discovered"),
		SourceAgent: "discovery-coordinator",
		VerticalID:  uuid.NewString(),
		Payload: mustJSON(map[string]any{
			"name":      "Dental Scheduling",
			"geography": "argentina",
		}),
		CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("publish vertical.discovered: %v", err)
	}
	waitForEventType(t, ch, "vertical.discovered")
}

func TestHoldingFlow_B2_HighScoreShortlistedTriggersValidationPipeline(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	bus.SetInterceptors(pc)

	verticalID := uuid.NewString()
	insertTestVertical(t, db, verticalID, "Clinic Ops SaaS", "argentina")
	ch := bus.Subscribe("watch-b2", events.EventType("validation.started"), events.EventType("brand.requested"))

	if err := bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("vertical.shortlisted"),
		SourceAgent: "scoring-coordinator",
		VerticalID:  verticalID,
		Payload:     mustJSON(map[string]any{"composite_score": 81, "viability_score": 72}),
		CreatedAt:   time.Now().UTC(),
	}); err != nil {
		t.Fatalf("publish vertical.shortlisted: %v", err)
	}
	waitForEventTypes(t, ch, []string{"validation.started", "brand.requested"})

	pc.mu.Lock()
	st := pc.validations[verticalID]
	pc.mu.Unlock()
	if st == nil || strings.TrimSpace(st.Status) != "active" {
		t.Fatalf("expected active validation pipeline state, got %+v", st)
	}
}

func TestHoldingFlow_B3_MarginalScoreRoutedToEmpireCoordinator(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	ch := bus.Subscribe("empire-coordinator", events.EventType("vertical.marginal"))
	if err := bus.Publish(context.Background(), events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("vertical.marginal"),
		SourceAgent: "scoring-coordinator",
		VerticalID:  uuid.NewString(),
		Payload: mustJSON(map[string]any{
			"composite_score": 62,
			"viability_score": 68,
		}),
		CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("publish vertical.marginal: %v", err)
	}
	waitForEventType(t, ch, "vertical.marginal")
}

func TestHoldingFlow_C1_ShortlistedCreatesValidationPipelineAndEnrichedPayloads(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	bus.SetInterceptors(pc)

	verticalID := uuid.NewString()
	insertTestVertical(t, db, verticalID, "Pet Grooming Operations", "argentina")
	ch := bus.Subscribe("watch-c1", events.EventType("validation.started"), events.EventType("brand.requested"))

	if err := bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("vertical.shortlisted"),
		SourceAgent: "scoring-coordinator",
		VerticalID:  verticalID,
		Payload:     mustJSON(map[string]any{"composite_score": 81.25}),
		CreatedAt:   time.Now().UTC(),
	}); err != nil {
		t.Fatalf("publish vertical.shortlisted: %v", err)
	}
	got := waitForEventTypes(t, ch, []string{"validation.started", "brand.requested"})

	pc.mu.Lock()
	st := pc.validations[verticalID]
	pc.mu.Unlock()
	if st == nil {
		t.Fatal("expected validation pipeline state")
	}
	if st.G1Research || st.G2Spec || st.G3CTO || st.G4Brand {
		t.Fatalf("expected all gates false on create, got %+v", st)
	}

	validationPayload := parsePayloadMap(got["validation.started"].Payload)
	if strings.TrimSpace(asString(validationPayload["vertical_name"])) != "Pet Grooming Operations" {
		t.Fatalf("expected enriched vertical_name, got payload=%v", validationPayload)
	}
	if strings.TrimSpace(asString(validationPayload["geography"])) != "argentina" {
		t.Fatalf("expected enriched geography, got payload=%v", validationPayload)
	}
	brandPayload := parsePayloadMap(got["brand.requested"].Payload)
	if strings.TrimSpace(asString(brandPayload["vertical_name"])) != "Pet Grooming Operations" {
		t.Fatalf("expected brand request vertical_name, got payload=%v", brandPayload)
	}
}

func TestHoldingFlow_C2_BusinessResearchReceivesValidationContextAndCanContinue(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	bus.SetInterceptors(pc)

	verticalID := uuid.NewString()
	insertTestVertical(t, db, verticalID, "Dental Clinic Scheduling", "paraguay")

	braCh := bus.Subscribe("business-research-agent", events.EventType("validation.started"))
	lsaCh := bus.Subscribe("lightweight-spec-agent", events.EventType("spec.requested"))
	errCh := make(chan error, 1)
	go func() {
		evt := waitForEventType(t, braCh, "validation.started")
		payload := parsePayloadMap(evt.Payload)
		if strings.TrimSpace(asString(payload["vertical_name"])) == "" || strings.TrimSpace(asString(payload["geography"])) == "" {
			errCh <- fmt.Errorf("validation.started missing required context fields: %v", payload)
			return
		}
		if err := bus.Publish(ctx, events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("research.completed"),
			SourceAgent: "business-research-agent",
			VerticalID:  verticalID,
			Payload:     mustJSON(map[string]any{"business_brief": map[string]any{"problem": "manual scheduling"}}),
			CreatedAt:   time.Now().UTC(),
		}); err != nil {
			errCh <- err
			return
		}
		if err := bus.Publish(ctx, events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("spec.requested"),
			SourceAgent: "business-research-agent",
			VerticalID:  verticalID,
			Payload:     mustJSON(map[string]any{"business_brief": map[string]any{"problem": "manual scheduling"}}),
			CreatedAt:   time.Now().UTC(),
		}); err != nil {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	if err := bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("vertical.shortlisted"),
		SourceAgent: "scoring-coordinator",
		VerticalID:  verticalID,
		Payload:     mustJSON(map[string]any{"composite_score": 80}),
		CreatedAt:   time.Now().UTC(),
	}); err != nil {
		t.Fatalf("publish shortlist: %v", err)
	}
	waitForEventType(t, lsaCh, "spec.requested")
	if err := <-errCh; err != nil {
		t.Fatalf("business research continuation failed: %v", err)
	}
	pc.mu.Lock()
	g1 := pc.validations[verticalID].G1Research
	pc.mu.Unlock()
	if !g1 {
		t.Fatal("expected G1 set after research.completed")
	}
}

func TestHoldingFlow_C3_ResearchCompletedSetsG1AndSpecRequestedPassthrough(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	bus.SetInterceptors(pc)

	verticalID := uuid.NewString()
	insertTestVertical(t, db, verticalID, "Payroll Ops", "paraguay")
	lsaCh := bus.Subscribe("lightweight-spec-agent", events.EventType("spec.requested"))

	_ = bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("vertical.shortlisted"),
		SourceAgent: "scoring-coordinator",
		VerticalID:  verticalID,
		Payload:     mustJSON(map[string]any{"composite_score": 79}),
		CreatedAt:   time.Now().UTC(),
	})
	_ = bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("research.completed"),
		SourceAgent: "business-research-agent",
		VerticalID:  verticalID,
		Payload:     mustJSON(map[string]any{"business_brief": map[string]any{"segment": "SMB"}}),
		CreatedAt:   time.Now().UTC(),
	})
	_ = bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("spec.requested"),
		SourceAgent: "business-research-agent",
		VerticalID:  verticalID,
		Payload:     mustJSON(map[string]any{"business_brief": map[string]any{"segment": "SMB"}}),
		CreatedAt:   time.Now().UTC(),
	})

	waitForEventType(t, lsaCh, "spec.requested")
	pc.mu.Lock()
	g1 := pc.validations[verticalID].G1Research
	pc.mu.Unlock()
	if !g1 {
		t.Fatal("expected G1 gate true after research.completed")
	}
}

func TestHoldingFlow_C4_SpecDraftToReviewRouting(t *testing.T) {
	ctx := context.Background()
	bus := NewEventBus(InMemoryEventStore{})
	braCh := bus.Subscribe("business-research-agent", events.EventType("spec.draft_ready"))
	reviewerCh := bus.Subscribe("spec-reviewer", events.EventType("spec.review_requested"))

	go func() {
		evt := waitForEventType(t, braCh, "spec.draft_ready")
		payload := parsePayloadMap(evt.Payload)
		_ = bus.Publish(ctx, events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("spec.review_requested"),
			SourceAgent: "business-research-agent",
			VerticalID:  evt.VerticalID,
			Payload:     mustJSON(map[string]any{"spec": payload["spec"], "review_checklist": "single-pass"}),
			CreatedAt:   time.Now().UTC(),
		})
	}()

	if err := bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("spec.draft_ready"),
		SourceAgent: "lightweight-spec-agent",
		VerticalID:  uuid.NewString(),
		Payload:     mustJSON(map[string]any{"spec": map[string]any{"workflow": "core"}}),
		CreatedAt:   time.Now().UTC(),
	}); err != nil {
		t.Fatalf("publish spec.draft_ready: %v", err)
	}
	waitForEventType(t, reviewerCh, "spec.review_requested")
}

func TestHoldingFlow_C5_SpecApprovedTriggersAuditorThenCTO(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	bus.SetInterceptors(pc)

	verticalID := uuid.NewString()
	insertTestVertical(t, db, verticalID, "E-Invoicing SaaS", "argentina")
	ch := bus.Subscribe("watch-c5", events.EventType("spec.validation_requested"), events.EventType("cto.spec_review_requested"))

	_ = bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("vertical.shortlisted"),
		SourceAgent: "scoring-coordinator",
		VerticalID:  verticalID,
		Payload:     mustJSON(map[string]any{"composite_score": 84}),
		CreatedAt:   time.Now().UTC(),
	})
	_ = bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("research.completed"),
		SourceAgent: "business-research-agent",
		VerticalID:  verticalID,
		Payload:     mustJSON(map[string]any{"brief": "ok"}),
		CreatedAt:   time.Now().UTC(),
	})
	_ = bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("spec.approved"),
		SourceAgent: "business-research-agent",
		VerticalID:  verticalID,
		Payload:     mustJSON(map[string]any{"spec": map[string]any{"features": []string{"f1"}}}),
		CreatedAt:   time.Now().UTC(),
	})

	specValidation := waitForEventType(t, ch, "spec.validation_requested")
	specPayload := parsePayloadMap(specValidation.Payload)
	specVersion := asInt(specPayload["spec_version"])
	if specVersion == 0 {
		specVersion = 1
	}
	_ = bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("spec.validation_passed"),
		SourceAgent: "spec-auditor",
		VerticalID:  verticalID,
		Payload:     mustJSON(map[string]any{"spec_version": specVersion, "status": "passed"}),
		CreatedAt:   time.Now().UTC(),
	})
	waitForEventType(t, ch, "cto.spec_review_requested")

	pc.mu.Lock()
	g2 := pc.validations[verticalID].G2Spec
	pc.mu.Unlock()
	if !g2 {
		t.Fatal("expected G2 gate true after spec.approved")
	}
}

func TestHoldingFlow_CatalogSmoke_EventPayloadJSONRoundTrip(t *testing.T) {
	raw := mustJSON(map[string]any{
		"mode":      "saas_gap",
		"geography": "argentina",
		"priority":  "normal",
	})
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("payload json unmarshal: %v", err)
	}
	if strings.TrimSpace(asString(out["mode"])) != "saas_gap" {
		t.Fatalf("expected saas_gap, got %v", out["mode"])
	}
}
