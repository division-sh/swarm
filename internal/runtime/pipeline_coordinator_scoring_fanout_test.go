package runtime

import (
	"context"
	"strings"
	"testing"
	"time"

	"empireai/internal/events"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestFactoryPipelineCoordinator_FinalizeScoring_RejectedDoesNotPublishVerticalScored(t *testing.T) {
	store := &captureStore{}
	bus := NewEventBus(store)
	pc := NewFactoryPipelineCoordinator(bus, nil)
	bus.SetInterceptors(pc)

	verticalID := uuid.NewString()
	pc.mu.Lock()
	pc.scoring[verticalID] = &scoringAccumulator{
		VerticalID:   verticalID,
		VerticalName: "Paraguay Ecommerce",
		Geography:    "paraguay",
		Mode:         "saas_gap",
		Rubric:       "saas",
		Expected:     []string{"willingness_to_pay", "market_size"},
		Received: map[string]scoreDimensionResult{
			"willingness_to_pay": {Score: 40, Evidence: "low"},
			"market_size":        {Score: 80, Evidence: "medium"},
		},
		Contested:       map[string]contestedDimension{},
		ContestNotified: map[string]bool{},
	}
	pc.mu.Unlock()

	ecCh := bus.Subscribe("empire-coordinator", events.EventType("vertical.scored"), events.EventType("vertical.rejected"))
	pc.finalizeScoringAccumulator(context.Background(), verticalID, false)

	received := collectEventTypes(ecCh, 250*time.Millisecond)
	if containsEventType(received, "vertical.scored") {
		t.Fatalf("rejected scoring must not emit vertical.scored, got=%v", received)
	}
	if !containsEventType(received, "vertical.rejected") {
		t.Fatalf("expected vertical.rejected for rejected scoring, got=%v", received)
	}
	if !hasPersistedEventType(store.events, "vertical.scored") {
		t.Fatalf("expected rejected scoring path to persist vertical.scored for audit, got %+v", store.events)
	}
}

func TestFactoryPipelineCoordinator_FinalizeScoring_MarginalDoesNotPublishVerticalScored(t *testing.T) {
	store := &captureStore{}
	bus := NewEventBus(store)
	pc := NewFactoryPipelineCoordinator(bus, nil)
	bus.SetInterceptors(pc)

	verticalID := uuid.NewString()
	pc.mu.Lock()
	pc.scoring[verticalID] = &scoringAccumulator{
		VerticalID:   verticalID,
		VerticalName: "Dental Ops",
		Geography:    "argentina",
		Mode:         "saas_gap",
		Rubric:       "saas",
		Expected:     []string{"willingness_to_pay", "market_size"},
		Received: map[string]scoreDimensionResult{
			"willingness_to_pay": {Score: 70, Evidence: "ok"},
			"market_size":        {Score: 55, Evidence: "ok"},
		},
		Contested:       map[string]contestedDimension{},
		ContestNotified: map[string]bool{},
	}
	pc.mu.Unlock()

	ecCh := bus.Subscribe("empire-coordinator", events.EventType("vertical.scored"), events.EventType("vertical.marginal"))
	pc.finalizeScoringAccumulator(context.Background(), verticalID, false)

	received := collectEventTypes(ecCh, 250*time.Millisecond)
	if containsEventType(received, "vertical.scored") {
		t.Fatalf("marginal scoring must not emit vertical.scored, got=%v", received)
	}
	if !containsEventType(received, "vertical.marginal") {
		t.Fatalf("expected vertical.marginal for marginal scoring, got=%v", received)
	}
	if !hasPersistedEventType(store.events, "vertical.scored") {
		t.Fatalf("expected marginal scoring path to persist vertical.scored for audit, got %+v", store.events)
	}
}

func TestFactoryPipelineCoordinator_FinalizeScoring_ShortlistedPublishesVerticalScored(t *testing.T) {
	store := &captureStore{}
	bus := NewEventBus(store)
	pc := NewFactoryPipelineCoordinator(bus, nil)
	bus.SetInterceptors(pc)

	verticalID := uuid.NewString()
	pc.mu.Lock()
	pc.scoring[verticalID] = &scoringAccumulator{
		VerticalID:   verticalID,
		VerticalName: "Clinic Scheduling",
		Geography:    "argentina",
		Mode:         "saas_gap",
		Rubric:       "saas",
		Expected:     []string{"willingness_to_pay", "market_size"},
		Received: map[string]scoreDimensionResult{
			"willingness_to_pay": {Score: 92, Evidence: "high"},
			"market_size":        {Score: 90, Evidence: "high"},
		},
		Contested:       map[string]contestedDimension{},
		ContestNotified: map[string]bool{},
	}
	pc.mu.Unlock()

	ecCh := bus.Subscribe("empire-coordinator", events.EventType("vertical.scored"), events.EventType("vertical.shortlisted"))
	pc.finalizeScoringAccumulator(context.Background(), verticalID, false)

	received := collectEventTypes(ecCh, 250*time.Millisecond)
	if !containsEventType(received, "vertical.scored") {
		t.Fatalf("shortlisted scoring must emit vertical.scored, got=%v", received)
	}
	if containsEventType(received, "vertical.shortlisted") {
		t.Fatalf("vertical.shortlisted should be interceptor-consumed, got=%v", received)
	}
}

func TestFactoryPipelineCoordinator_AutomationMicroGateRejectsLowAutomationLeverage(t *testing.T) {
	store := &captureStore{}
	bus := NewEventBus(store)
	pc := NewFactoryPipelineCoordinator(bus, nil)
	bus.SetInterceptors(pc)

	verticalID := uuid.NewString()
	pc.mu.Lock()
	pc.scoring[verticalID] = &scoringAccumulator{
		VerticalID:   verticalID,
		VerticalName: "Micro Vertical",
		Geography:    "argentina",
		Mode:         "automation_micro",
		Rubric:       "automation_micro",
		Expected: []string{
			"automation_leverage",
			"sales_cycle_simplicity",
			"channel_exploitability",
			"willingness_to_pay",
			"retention_likelihood",
			"pain_severity",
			"competition_weakness",
			"structural_cloneability",
			"compliance_lightness",
		},
		Received: map[string]scoreDimensionResult{
			"automation_leverage":     {Score: 65, Evidence: "requires heavy manual onboarding"},
			"sales_cycle_simplicity":  {Score: 80, Evidence: "single owner decision"},
			"channel_exploitability":  {Score: 80, Evidence: "clear local directories"},
			"willingness_to_pay":      {Score: 70, Evidence: "existing SaaS spend"},
			"retention_likelihood":    {Score: 75, Evidence: "daily usage"},
			"pain_severity":           {Score: 70, Evidence: "revenue leakage"},
			"competition_weakness":    {Score: 75, Evidence: "weak incumbents"},
			"structural_cloneability": {Score: 80, Evidence: "repeatable workflow"},
			"compliance_lightness":    {Score: 90, Evidence: "low regulation"},
		},
		Contested:       map[string]contestedDimension{},
		ContestNotified: map[string]bool{},
	}
	pc.mu.Unlock()

	pc.finalizeScoringAccumulator(context.Background(), verticalID, false)

	var rejected bool
	for _, evt := range store.events {
		if string(evt.Type) != "vertical.scored" {
			continue
		}
		payload := parsePayloadMap(evt.Payload)
		if strings.TrimSpace(asString(payload["result"])) == "rejected" &&
			strings.TrimSpace(asString(payload["reason"])) == "gate_automation_leverage" {
			rejected = true
			break
		}
	}
	if !rejected {
		t.Fatalf("expected automation_micro hard gate rejection, events=%+v", store.events)
	}
}

func TestFactoryPipelineCoordinator_SaaSGateRejectsLowAutomationCompleteness(t *testing.T) {
	store := &captureStore{}
	bus := NewEventBus(store)
	pc := NewFactoryPipelineCoordinator(bus, nil)
	bus.SetInterceptors(pc)

	verticalID := uuid.NewString()
	pc.mu.Lock()
	pc.scoring[verticalID] = &scoringAccumulator{
		VerticalID:   verticalID,
		VerticalName: "Helpdesk Ticketing",
		Geography:    "paraguay",
		Mode:         "saas_gap",
		Rubric:       "saas",
		Expected: []string{
			"automation_completeness",
			"build_complexity",
			"technical_feasibility",
			"distribution_access",
			"willingness_to_pay",
			"retention_likelihood",
			"regulatory_moat",
			"competition_weakness",
			"pain_severity",
			"market_size",
			"localization_advantage",
		},
		Received: map[string]scoreDimensionResult{
			"automation_completeness": {Score: 45, Evidence: "requires manual onboarding and human support"},
			"build_complexity":        {Score: 72, Evidence: "manageable MVP scope"},
			"technical_feasibility":   {Score: 80, Evidence: "standard SaaS stack"},
			"distribution_access":     {Score: 68, Evidence: "reachable SMB communities"},
			"willingness_to_pay":      {Score: 70, Evidence: "existing spend signals"},
			"retention_likelihood":    {Score: 66, Evidence: "moderate lock-in"},
			"regulatory_moat":         {Score: 60, Evidence: "some compliance pressure"},
			"competition_weakness":    {Score: 65, Evidence: "incumbent gaps"},
			"pain_severity":           {Score: 75, Evidence: "clear operational pain"},
			"market_size":             {Score: 58, Evidence: "mid-size market"},
			"localization_advantage":  {Score: 62, Evidence: "local requirements"},
		},
		Contested:       map[string]contestedDimension{},
		ContestNotified: map[string]bool{},
	}
	pc.mu.Unlock()

	pc.finalizeScoringAccumulator(context.Background(), verticalID, false)

	var rejected bool
	for _, evt := range store.events {
		if string(evt.Type) != "vertical.scored" {
			continue
		}
		payload := parsePayloadMap(evt.Payload)
		if strings.TrimSpace(asString(payload["result"])) == "rejected" &&
			strings.TrimSpace(asString(payload["reason"])) == "gate_automation_completeness" {
			rejected = true
			break
		}
	}
	if !rejected {
		t.Fatalf("expected saas hard gate rejection, events=%+v", store.events)
	}
}

func TestFactoryPipelineCoordinator_RejectedScoringBufferedAndInjectedIntoPortfolioDigest(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS scoring_digest_buffer (
			id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			vertical_id     UUID NOT NULL REFERENCES verticals(id),
			vertical_name   TEXT NOT NULL,
			geography       TEXT NOT NULL,
			composite       NUMERIC(5,2) NOT NULL,
			viability       NUMERIC(5,2),
			result          TEXT NOT NULL DEFAULT 'rejected',
			reason          TEXT NOT NULL,
			scored_at       TIMESTAMPTZ DEFAULT now()
		)
	`); err != nil {
		t.Fatalf("create scoring_digest_buffer: %v", err)
	}

	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	bus.SetInterceptors(pc)

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, $2, $3, $4, 'scoring', 'factory', now(), now())
	`, verticalID, "Ecommerce Logistics", "ecommerce-logistics", "paraguay"); err != nil {
		t.Fatalf("insert vertical: %v", err)
	}
	pc.mu.Lock()
	pc.scoring[verticalID] = &scoringAccumulator{
		VerticalID:   verticalID,
		VerticalName: "Ecommerce Logistics",
		Geography:    "paraguay",
		Mode:         "saas_gap",
		Rubric:       "saas",
		Expected:     []string{"willingness_to_pay", "market_size"},
		Received: map[string]scoreDimensionResult{
			"willingness_to_pay": {Score: 42, Evidence: "weak"},
			"market_size":        {Score: 78, Evidence: "ok"},
		},
		Contested:       map[string]contestedDimension{},
		ContestNotified: map[string]bool{},
	}
	pc.mu.Unlock()
	pc.finalizeScoringAccumulator(ctx, verticalID, false)

	var buffered int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM scoring_digest_buffer`).Scan(&buffered); err != nil {
		t.Fatalf("count scoring digest rows: %v", err)
	}
	if buffered != 1 {
		t.Fatalf("expected one scoring digest row, got %d", buffered)
	}

	ecCh := bus.Subscribe("empire-coordinator", events.EventType("timer.portfolio_digest"))
	if err := bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("timer.portfolio_digest"),
		SourceAgent: "runtime",
		Payload:     mustJSON(map[string]any{"window": "daily"}),
		CreatedAt:   time.Now().UTC(),
	}); err != nil {
		t.Fatalf("publish timer.portfolio_digest: %v", err)
	}

	received := collectEvents(ecCh, 350*time.Millisecond)
	if len(received) == 0 {
		t.Fatal("expected enriched timer.portfolio_digest event")
	}
	evt := received[len(received)-1]
	payload := parsePayloadMap(evt.Payload)
	if !boolFromAny(payload["scoring_rejections_injected"]) {
		t.Fatalf("expected scoring_rejections_injected=true, payload=%v", payload)
	}
	if asInt(payload["scoring_rejections_count"]) != 1 {
		t.Fatalf("expected scoring_rejections_count=1, payload=%v", payload)
	}
	if asInt(payload["rejection_count"]) != 1 {
		t.Fatalf("expected rejection_count=1, payload=%v", payload)
	}
	summaries, ok := payload["scoring_rejection_summaries"].([]any)
	if !ok || len(summaries) != 1 {
		t.Fatalf("expected one rejection summary, payload=%v", payload)
	}
	recent, ok := payload["recent_rejections"].([]any)
	if !ok || len(recent) != 1 {
		t.Fatalf("expected one recent_rejections entry, payload=%v", payload)
	}

	if err := bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("timer.portfolio_digest"),
		SourceAgent: "runtime",
		Payload:     mustJSON(map[string]any{"window": "daily"}),
		CreatedAt:   time.Now().UTC(),
	}); err != nil {
		t.Fatalf("publish second timer.portfolio_digest: %v", err)
	}
	received = collectEvents(ecCh, 350*time.Millisecond)
	if len(received) == 0 {
		t.Fatal("expected second timer.portfolio_digest event")
	}
	evt = received[len(received)-1]
	payload = parsePayloadMap(evt.Payload)
	if asInt(payload["rejection_count"]) != 0 {
		t.Fatalf("expected rejection_count=0 on next digest tick, payload=%v", payload)
	}
}

func hasPersistedEventType(eventsList []events.Event, eventType string) bool {
	for _, evt := range eventsList {
		if string(evt.Type) == eventType {
			return true
		}
	}
	return false
}

func collectEventTypes(ch <-chan events.Event, window time.Duration) []string {
	evts := collectEvents(ch, window)
	out := make([]string, 0, len(evts))
	for _, evt := range evts {
		out = append(out, string(evt.Type))
	}
	return out
}

func collectEvents(ch <-chan events.Event, window time.Duration) []events.Event {
	deadline := time.NewTimer(window)
	defer deadline.Stop()

	out := make([]events.Event, 0, 8)
	for {
		select {
		case evt := <-ch:
			out = append(out, evt)
		case <-deadline.C:
			return out
		}
	}
}

func containsEventType(types []string, want string) bool {
	for _, got := range types {
		if got == want {
			return true
		}
	}
	return false
}
