package pipeline

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
	acc := newUniversalAccumulator(verticalID, "Paraguay Ecommerce", "paraguay", "saas_gap")
	acc.Received["build_complexity"] = scoreDimensionResult{Score: 40, Evidence: "heavy enterprise integration required"}
	pc.mu.Lock()
	pc.scoringState.accumulators[verticalID] = acc
	pc.mu.Unlock()

	ecCh := bus.Subscribe("empire-coordinator", events.EventType("vertical.scored"), events.EventType("vertical.rejected"))
	pc.finalizeScoringAccumulator(context.Background(), verticalID, false)

	received := collectEventTypes(ecCh, 250*time.Millisecond)
	if containsEventType(received, "vertical.scored") {
		t.Fatalf("rejected scoring must not emit vertical.scored to EC, got=%v", received)
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
	acc := newUniversalAccumulator(verticalID, "Dental Ops", "argentina", "saas_gap")
	setScores(acc, map[string]int{
		"build_complexity":        80,
		"automation_completeness": 80,
		"icp_crispness":           72,
		"distribution_leverage":   72,
		"time_to_value":           60,
		"operational_drag":        60,
		"pain_severity":           60,
		"competition_gap":         60,
		"monetization_clarity":    60,
		"retention_architecture":  60,
		"expansion_potential":     60,
	})
	pc.mu.Lock()
	pc.scoringState.accumulators[verticalID] = acc
	pc.mu.Unlock()

	ecCh := bus.Subscribe("empire-coordinator", events.EventType("vertical.scored"), events.EventType("vertical.marginal"))
	pc.finalizeScoringAccumulator(context.Background(), verticalID, false)

	received := collectEventTypes(ecCh, 250*time.Millisecond)
	if containsEventType(received, "vertical.scored") {
		t.Fatalf("marginal scoring must not emit vertical.scored to EC, got=%v", received)
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
	acc := newUniversalAccumulator(verticalID, "Clinic Scheduling", "argentina", "saas_gap")
	setScores(acc, map[string]int{
		"build_complexity":        90,
		"automation_completeness": 90,
		"icp_crispness":           90,
		"distribution_leverage":   90,
		"time_to_value":           90,
		"operational_drag":        90,
		"pain_severity":           90,
		"competition_gap":         90,
		"monetization_clarity":    90,
		"retention_architecture":  90,
		"expansion_potential":     90,
	})
	pc.mu.Lock()
	pc.scoringState.accumulators[verticalID] = acc
	pc.mu.Unlock()

	ecCh := bus.Subscribe("empire-coordinator", events.EventType("vertical.scored"), events.EventType("vertical.shortlisted"))
	pc.finalizeScoringAccumulator(context.Background(), verticalID, false)

	received := collectEventTypes(ecCh, 250*time.Millisecond)
	if !containsEventType(received, "vertical.scored") {
		t.Fatalf("shortlisted scoring must emit vertical.scored to EC, got=%v", received)
	}
	if !containsEventType(received, "vertical.shortlisted") {
		t.Fatalf("vertical.shortlisted should remain visible downstream under dual_delivery, got=%v", received)
	}
}

func TestFactoryPipelineCoordinator_UniversalGateRejectsLowBuildComplexity(t *testing.T) {
	store := &captureStore{}
	bus := NewEventBus(store)
	pc := NewFactoryPipelineCoordinator(bus, nil)
	bus.SetInterceptors(pc)

	verticalID := uuid.NewString()
	acc := newUniversalAccumulator(verticalID, "Payment Gateway", "argentina", "saas_gap")
	setScores(acc, map[string]int{
		"build_complexity":        35,
		"automation_completeness": 85,
		"icp_crispness":           70,
		"distribution_leverage":   70,
		"time_to_value":           70,
		"operational_drag":        70,
		"pain_severity":           70,
		"competition_gap":         70,
		"monetization_clarity":    70,
		"retention_architecture":  70,
		"expansion_potential":     70,
	})
	pc.mu.Lock()
	pc.scoringState.accumulators[verticalID] = acc
	pc.mu.Unlock()

	pc.finalizeScoringAccumulator(context.Background(), verticalID, false)

	assertScoringReason(t, store.events, "gate_build_complexity")
}

func TestFactoryPipelineCoordinator_UniversalGateRejectsLowAutomationCompleteness(t *testing.T) {
	store := &captureStore{}
	bus := NewEventBus(store)
	pc := NewFactoryPipelineCoordinator(bus, nil)
	bus.SetInterceptors(pc)

	verticalID := uuid.NewString()
	acc := newUniversalAccumulator(verticalID, "Helpdesk Ticketing", "paraguay", "automation_micro")
	setScores(acc, map[string]int{
		"build_complexity":        75,
		"automation_completeness": 45,
		"icp_crispness":           70,
		"distribution_leverage":   70,
		"time_to_value":           70,
		"operational_drag":        70,
		"pain_severity":           70,
		"competition_gap":         70,
		"monetization_clarity":    70,
		"retention_architecture":  70,
		"expansion_potential":     70,
	})
	pc.mu.Lock()
	pc.scoringState.accumulators[verticalID] = acc
	pc.mu.Unlock()

	pc.finalizeScoringAccumulator(context.Background(), verticalID, false)

	assertScoringReason(t, store.events, "gate_automation_completeness")
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
	acc := newUniversalAccumulator(verticalID, "Ecommerce Logistics", "paraguay", "saas_gap")
	setScores(acc, map[string]int{
		"build_complexity":        80,
		"automation_completeness": 80,
		"icp_crispness":           40,
		"distribution_leverage":   72,
		"time_to_value":           72,
		"operational_drag":        72,
		"pain_severity":           70,
		"competition_gap":         70,
		"monetization_clarity":    70,
		"retention_architecture":  70,
		"expansion_potential":     70,
	})
	pc.mu.Lock()
	pc.scoringState.accumulators[verticalID] = acc
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

func newUniversalAccumulator(verticalID, name, geography, mode string) *scoringAccumulator {
	dims := ExpectedScoringDimensionsForTest("universal")
	received := make(map[string]scoreDimensionResult, len(dims))
	for _, dim := range dims {
		received[dim] = scoreDimensionResult{
			Score:    75,
			Evidence: "default",
		}
	}
	return &scoringAccumulator{
		VerticalID:      verticalID,
		VerticalName:    name,
		Geography:       geography,
		Mode:            mode,
		Rubric:          "universal",
		Expected:        dims,
		Received:        received,
		Contested:       map[string]contestedDimension{},
		ContestNotified: map[string]bool{},
	}
}

func setScores(acc *scoringAccumulator, scores map[string]int) {
	for dim, score := range scores {
		acc.Received[dim] = scoreDimensionResult{
			Score:    score,
			Evidence: "test",
		}
	}
}

func assertScoringReason(t *testing.T, eventsList []events.Event, reason string) {
	t.Helper()
	for _, evt := range eventsList {
		if string(evt.Type) != "vertical.scored" {
			continue
		}
		payload := parsePayloadMap(evt.Payload)
		if strings.TrimSpace(asString(payload["result"])) == "rejected" &&
			strings.TrimSpace(asString(payload["reason"])) == reason {
			return
		}
	}
	t.Fatalf("expected rejected vertical.scored reason=%s, events=%+v", reason, eventsList)
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
