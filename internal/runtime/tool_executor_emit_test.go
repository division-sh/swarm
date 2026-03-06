package runtime

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"empireai/internal/events"
	"empireai/internal/models"
)

func TestRuntimeToolExecutor_HandleEmitToolPublishesEvent(t *testing.T) {
	store := &captureStore{}
	bus := NewEventBus(store)
	exec := NewRuntimeToolExecutor(bus, nil, nil)
	actor := models.AgentConfig{ID: "empire-coordinator", Role: "empire-coordinator", Mode: "holding"}

	ctx := WithActor(context.Background(), actor)
	ctx = WithInboundEvent(ctx, events.Event{
		ID:          "dir-1",
		Type:        events.EventType("system.directive"),
		SourceAgent: "human",
		Payload:     mustJSON(map[string]any{"directive_text": "SaaS in Paraguay"}),
	})

	out, err := exec.Execute(ctx, "emit_scan_requested", map[string]any{
		"mode":      "saas_gap",
		"geography": "paraguay",
	})
	if err != nil {
		t.Fatalf("execute emit tool: %v", err)
	}
	if out == nil {
		t.Fatal("expected publish ack output")
	}
	if len(store.events) == 0 {
		t.Fatal("expected published event")
	}
	var last events.Event
	found := false
	for i := len(store.events) - 1; i >= 0; i-- {
		if string(store.events[i].Type) == "scan.requested" {
			last = store.events[i]
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected scan.requested event, got %+v", store.events)
	}
	var payload map[string]any
	if err := json.Unmarshal(last.Payload, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if strings.TrimSpace(asString(payload["mode"])) != "saas_gap" {
		t.Fatalf("expected mode preserved, got %+v", payload["mode"])
	}
	if _, ok := payload["priority"]; ok {
		t.Fatalf("expected legacy priority field to be trimmed by contract schema, got payload=%+v", payload)
	}
}

func TestRuntimeToolExecutor_HandleEmitToolTransitionGuardrail(t *testing.T) {
	store := &captureStore{}
	bus := NewEventBus(store)
	exec := NewRuntimeToolExecutor(bus, nil, nil)
	actor := models.AgentConfig{ID: "empire-coordinator", Role: "empire-coordinator", Mode: "holding"}

	ctx := WithActor(context.Background(), actor)
	ctx = WithInboundEvent(ctx, events.Event{
		ID:          "scan-1",
		Type:        events.EventType("scan.completed"),
		SourceAgent: "discovery-coordinator",
		Payload:     mustJSON(map[string]any{"discoveries_count": 3}),
	})
	_, err := exec.Execute(ctx, "emit_opco_spinup_requested", map[string]any{
		"vertical_id": "v1",
		"mandate":     map[string]any{"vertical_id": "v1"},
	})
	if err == nil || !strings.Contains(err.Error(), "guardrail_violation") {
		t.Fatalf("expected guardrail violation, got %v", err)
	}
}

func TestRuntimeToolExecutor_HandleEmitToolSchemaValidation(t *testing.T) {
	store := &captureStore{}
	bus := NewEventBus(store)
	exec := NewRuntimeToolExecutor(bus, nil, nil)
	actor := models.AgentConfig{ID: "empire-coordinator", Role: "empire-coordinator", Mode: "holding"}

	ctx := WithActor(context.Background(), actor)
	ctx = WithInboundEvent(ctx, events.Event{
		ID:          "dir-1",
		Type:        events.EventType("system.directive"),
		SourceAgent: "human",
		Payload:     mustJSON(map[string]any{"directive_text": "SaaS in Paraguay"}),
	})
	_, err := exec.Execute(ctx, "emit_scan_requested", map[string]any{
		"priority": "normal",
	})
	if err == nil {
		t.Fatal("expected schema validation error for missing required mode")
	}
	if !strings.Contains(err.Error(), "is required") {
		t.Fatalf("expected required-field schema error, got %v", err)
	}
}

func TestRuntimeToolExecutor_HandleEmitToolVerticalDerivedCoercesLegacyRationaleString(t *testing.T) {
	store := &captureStore{}
	bus := NewEventBus(store)
	exec := NewRuntimeToolExecutor(bus, nil, nil)
	actor := models.AgentConfig{ID: "analysis-agent", Role: "analysis-agent", Mode: "factory"}

	ctx := WithActor(context.Background(), actor)
	ctx = WithInboundEvent(ctx, events.Event{
		ID:          "score-req-legacy-rationale",
		Type:        events.EventType("scoring.requested"),
		SourceAgent: "scoring-node",
		VerticalID:  "v-parent-1",
		Payload:     mustJSON(map[string]any{"vertical_id": "v-parent-1"}),
	})
	_, err := exec.Execute(ctx, "emit_vertical_derived", map[string]any{
		"parent_id":            "v-parent-1",
		"generation_depth":     1,
		"generator_agent_id":   "analysis-agent",
		"derivation_rationale": "narrow ICP to owner-operated firms",
		"opportunity_name":     "Derived Opportunity",
		"signal_strength":      72,
		"discovery_context":    map[string]any{"mode": "derived"},
	})
	if err != nil {
		t.Fatalf("expected legacy derivation_rationale string to be normalized, got %v", err)
	}

	if len(store.events) == 0 {
		t.Fatal("expected published vertical.derived event")
	}
	var last events.Event
	found := false
	for i := len(store.events) - 1; i >= 0; i-- {
		if strings.TrimSpace(string(store.events[i].Type)) == "vertical.derived" {
			last = store.events[i]
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected vertical.derived event, got %+v", store.events)
	}
	var payload map[string]any
	if err := json.Unmarshal(last.Payload, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	rationale, ok := payload["derivation_rationale"].(map[string]any)
	if !ok {
		t.Fatalf("expected derivation_rationale object after normalization, got %T", payload["derivation_rationale"])
	}
	if strings.TrimSpace(asString(rationale["summary"])) == "" {
		t.Fatalf("expected derivation_rationale.summary to be populated, got %#v", rationale)
	}
}

func TestRuntimeToolExecutor_HandleEmitToolCoordinatorLegacyNestedPayload(t *testing.T) {
	store := &captureStore{}
	bus := NewEventBus(store)
	exec := NewRuntimeToolExecutor(bus, nil, nil)
	actor := models.AgentConfig{ID: "empire-coordinator", Role: "empire-coordinator", Mode: "holding"}

	ctx := WithActor(context.Background(), actor)
	ctx = WithInboundEvent(ctx, events.Event{
		ID:          "dir-legacy-1",
		Type:        events.EventType("system.directive"),
		SourceAgent: "human",
		Payload:     mustJSON(map[string]any{"directive_text": "SaaS in Paraguay"}),
	})
	_, err := exec.Execute(ctx, "emit_scan_requested", map[string]any{
		"payload": map[string]any{
			"mode":      "discovery",
			"priority":  "medium",
			"geography": "paraguay",
		},
	})
	if err != nil {
		t.Fatalf("expected legacy nested payload to be normalized, got %v", err)
	}

	if len(store.events) == 0 {
		t.Fatal("expected emitted event")
	}
	var last events.Event
	found := false
	for i := len(store.events) - 1; i >= 0; i-- {
		if string(store.events[i].Type) == "scan.requested" {
			last = store.events[i]
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected scan.requested event, got %+v", store.events)
	}
	var payload map[string]any
	if err := json.Unmarshal(last.Payload, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if got := strings.TrimSpace(asString(payload["mode"])); got != "saas_gap" {
		t.Fatalf("expected mode alias discovery->saas_gap, got %q", got)
	}
	if _, ok := payload["priority"]; ok {
		t.Fatalf("expected legacy priority field removed after normalization, got %+v", payload)
	}
	if _, hasNested := payload["payload"]; hasNested {
		t.Fatalf("expected nested payload key removed after normalization, got %+v", payload)
	}
}

func TestRuntimeToolExecutor_HandleEmitToolCoordinatorInvalidModeCoerced(t *testing.T) {
	store := &captureStore{}
	bus := NewEventBus(store)
	exec := NewRuntimeToolExecutor(bus, nil, nil)
	actor := models.AgentConfig{ID: "empire-coordinator", Role: "empire-coordinator", Mode: "holding"}

	ctx := WithActor(context.Background(), actor)
	ctx = WithInboundEvent(ctx, events.Event{
		ID:          "dir-invalid-mode-1",
		Type:        events.EventType("system.directive"),
		SourceAgent: "human",
		Payload:     mustJSON(map[string]any{"directive_text": "SaaS in Argentina"}),
	})
	_, err := exec.Execute(ctx, "emit_scan_requested", map[string]any{
		"mode":     "simple",
		"priority": "normal",
	})
	if err != nil {
		t.Fatalf("expected invalid mode to be coerced for coordinator scan.requested, got %v", err)
	}

	var last events.Event
	found := false
	for i := len(store.events) - 1; i >= 0; i-- {
		if string(store.events[i].Type) == "scan.requested" {
			last = store.events[i]
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected scan.requested event, got %+v", store.events)
	}
	var payload map[string]any
	if err := json.Unmarshal(last.Payload, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if got := strings.TrimSpace(asString(payload["mode"])); got != "saas_gap" {
		t.Fatalf("expected invalid mode coerced to saas_gap, got %q", got)
	}
}

func TestRuntimeToolExecutor_HandleEmitToolContextEnrichment(t *testing.T) {
	store := &captureStore{}
	bus := NewEventBus(store)
	exec := NewRuntimeToolExecutor(bus, nil, nil)
	actor := models.AgentConfig{
		ID:         "business-research-agent",
		Role:       "business-research-agent",
		Mode:       "factory",
		VerticalID: "v1",
	}

	ctx := WithActor(context.Background(), actor)
	ctx = WithInboundEvent(ctx, events.Event{
		ID:          "vs-1",
		Type:        events.EventType("validation.started"),
		SourceAgent: "pipeline-coordinator",
		VerticalID:  "v1",
		Payload:     mustJSON(map[string]any{"vertical_id": "v1"}),
	})

	if _, err := exec.Execute(ctx, "emit_spec_requested", map[string]any{}); err != nil {
		t.Fatalf("expected context-enriched emit to pass, got %v", err)
	}
	if len(store.events) == 0 {
		t.Fatal("expected emitted event")
	}
	last := store.events[len(store.events)-1]
	var payload map[string]any
	if err := json.Unmarshal(last.Payload, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if strings.TrimSpace(asString(payload["vertical_id"])) != "v1" {
		t.Fatalf("expected enriched vertical_id=v1, got %+v", payload["vertical_id"])
	}
}

func TestRuntimeToolExecutor_HandleEmitToolOneShotSpecApproved(t *testing.T) {
	store := &captureStore{}
	bus := NewEventBus(store)
	exec := NewRuntimeToolExecutor(bus, nil, nil)
	actor := models.AgentConfig{
		ID:         "business-research-agent",
		Role:       "business-research-agent",
		Mode:       "factory",
		VerticalID: "v1",
	}

	ctx := WithActor(context.Background(), actor)
	ctx = WithInboundEvent(ctx, events.Event{
		ID:          "spr-1",
		Type:        events.EventType("spec_review.passed"),
		SourceAgent: "spec-reviewer",
		VerticalID:  "v1",
		Payload:     mustJSON(map[string]any{"vertical_id": "v1"}),
	})
	if _, err := exec.Execute(ctx, "emit_spec_approved", map[string]any{"vertical_id": "v1"}); err != nil {
		t.Fatalf("first spec.approved should pass: %v", err)
	}
	if _, err := exec.Execute(ctx, "emit_spec_approved", map[string]any{"vertical_id": "v1"}); err == nil {
		t.Fatal("expected duplicate spec.approved to be blocked")
	}
}

func TestRuntimeToolExecutor_HandleEmitToolFlattensNestedCategoryAssessedPayload(t *testing.T) {
	store := &captureStore{}
	bus := NewEventBus(store)
	exec := NewRuntimeToolExecutor(bus, nil, nil)
	actor := models.AgentConfig{
		ID:         "market-research-agent-shard-0",
		Role:       "market-research-agent",
		Mode:       "factory",
		VerticalID: "",
	}

	ctx := WithActor(context.Background(), actor)
	ctx = WithInboundEvent(ctx, events.Event{
		ID:          "scan-assigned-1",
		Type:        events.EventType("market_research.scan_assigned"),
		SourceAgent: "pipeline-coordinator",
		Payload: mustJSON(map[string]any{
			"scan_id":     "scan-123",
			"campaign_id": "camp-1",
			"mode":        "saas_gap",
			"geography":   "argentina",
		}),
	})
	if _, err := exec.Execute(ctx, "emit_category_assessed", map[string]any{
		"payload": map[string]any{
			"scan_id":          "scan-123",
			"campaign_id":      "camp-1",
			"mode":             "saas_gap",
			"geography":        "argentina",
			"category":         "operations",
			"subcategory":      "clinic_scheduling",
			"signal_strength":  76,
			"opportunity_name": "Clinic Scheduling Automation",
			"preliminary_icp":  "Clinic operations manager",
			"build_sketch": map[string]any{
				"core_features":    []any{"calendar sync"},
				"key_integrations": []any{"whatsapp"},
				"red_flags":        []any{},
			},
			"opportunity_hypothesis": "Automate patient bookings and reminders.",
			"geographic_scope":       "local",
			"evidence": map[string]any{
				"competitors": []any{
					map[string]any{"name": "ClinicFlow", "pricing": "$49/mo", "source_url": "https://example.com/competitor"},
				},
				"pain_signals": []any{
					map[string]any{"signal": "Manual follow-up workflows are common", "source_url": "https://example.com/pain"},
				},
				"regulatory": []any{
					map[string]any{"detail": "Consent requirements apply", "source_url": "https://example.com/reg"},
				},
				"buyer_communities": []any{
					map[string]any{"name": "Clinic Ops LATAM", "source_url": "https://example.com/community"},
				},
			},
		},
	}); err != nil {
		t.Fatalf("expected nested payload normalization for category.assessed, got %v", err)
	}

	var last events.Event
	found := false
	for i := len(store.events) - 1; i >= 0; i-- {
		if string(store.events[i].Type) == "category.assessed" {
			last = store.events[i]
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected category.assessed event, got %+v", store.events)
	}
	var payload map[string]any
	if err := json.Unmarshal(last.Payload, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if strings.TrimSpace(asString(payload["scan_id"])) != "scan-123" {
		t.Fatalf("expected scan_id flattened into root, got payload=%v", payload)
	}
	if _, hasNested := payload["payload"]; hasNested {
		t.Fatalf("expected nested payload key removed, got payload=%v", payload)
	}
}

func TestRuntimeToolExecutor_HandleEmitToolFlattensNestedScanCompletePayload(t *testing.T) {
	store := &captureStore{}
	bus := NewEventBus(store)
	exec := NewRuntimeToolExecutor(bus, nil, nil)
	actor := models.AgentConfig{
		ID:   "market-research-agent-shard-1",
		Role: "market-research-agent",
		Mode: "factory",
	}

	ctx := WithActor(context.Background(), actor)
	ctx = WithInboundEvent(ctx, events.Event{
		ID:          "scan-assigned-2",
		Type:        events.EventType("market_research.scan_assigned"),
		SourceAgent: "pipeline-coordinator",
		Payload: mustJSON(map[string]any{
			"scan_id": "scan-456",
		}),
	})
	if _, err := exec.Execute(ctx, "emit_market_research_scan_complete", map[string]any{
		"payload": map[string]any{
			"scan_id": "scan-456",
		},
	}); err != nil {
		t.Fatalf("expected nested payload normalization for market_research.scan_complete, got %v", err)
	}

	var last events.Event
	found := false
	for i := len(store.events) - 1; i >= 0; i-- {
		if string(store.events[i].Type) == "market_research.scan_complete" {
			last = store.events[i]
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected market_research.scan_complete event, got %+v", store.events)
	}
	var payload map[string]any
	if err := json.Unmarshal(last.Payload, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if strings.TrimSpace(asString(payload["scan_id"])) != "scan-456" {
		t.Fatalf("expected scan_id flattened into root, got payload=%v", payload)
	}
	if _, hasNested := payload["payload"]; hasNested {
		t.Fatalf("expected nested payload key removed, got payload=%v", payload)
	}
}

func TestRuntimeToolExecutor_HandleEmitToolSourceScrapedBackfillsGeographyFromAssignment(t *testing.T) {
	store := &captureStore{}
	bus := NewEventBus(store)
	exec := NewRuntimeToolExecutor(bus, nil, nil)
	actor := models.AgentConfig{
		ID:   "scanner-agent-shard-0",
		Role: "scanner-agent",
		Mode: "factory",
	}

	ctx := WithActor(context.Background(), actor)
	ctx = WithInboundEvent(ctx, events.Event{
		ID:          "scanner-assigned-1",
		Type:        events.EventType("scanner.directories.scan_assigned"),
		SourceAgent: "pipeline-coordinator",
		Payload: mustJSON(map[string]any{
			"scan_id":     "scan-geo-1",
			"campaign_id": "camp-geo-1",
			"mode":        "local_services",
			"geography":   "United States",
		}),
	})
	if _, err := exec.Execute(ctx, "emit_source_scraped", map[string]any{
		"payload": map[string]any{
			"scan_id":         "scan-geo-1",
			"source":          "directories",
			"evidence":        "Signal from directory crawl",
			"signal_strength": 72,
		},
	}); err != nil {
		t.Fatalf("expected source.scraped payload to backfill geography from assignment, got %v", err)
	}

	var last events.Event
	found := false
	for i := len(store.events) - 1; i >= 0; i-- {
		if string(store.events[i].Type) == "source.scraped" {
			last = store.events[i]
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected source.scraped event, got %+v", store.events)
	}
	var payload map[string]any
	if err := json.Unmarshal(last.Payload, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if got := strings.TrimSpace(asString(payload["geography"])); got != "United States" {
		t.Fatalf("expected geography backfilled from assignment, got %q payload=%v", got, payload)
	}
}

func TestRuntimeToolExecutor_HandleEmitToolSourceScrapedPlaceholderGeographyReplaced(t *testing.T) {
	store := &captureStore{}
	bus := NewEventBus(store)
	exec := NewRuntimeToolExecutor(bus, nil, nil)
	actor := models.AgentConfig{
		ID:   "scanner-agent-shard-1",
		Role: "scanner-agent",
		Mode: "factory",
	}

	ctx := WithActor(context.Background(), actor)
	ctx = WithInboundEvent(ctx, events.Event{
		ID:          "scanner-assigned-2",
		Type:        events.EventType("scanner.google_maps.scan_assigned"),
		SourceAgent: "pipeline-coordinator",
		Payload: mustJSON(map[string]any{
			"scan_id":     "scan-geo-2",
			"campaign_id": "camp-geo-2",
			"mode":        "local_services",
			"geography":   "US",
		}),
	})
	if _, err := exec.Execute(ctx, "emit_source_scraped", map[string]any{
		"scan_id":         "scan-geo-2",
		"source":          "google_maps",
		"evidence":        "Signal from maps crawl",
		"signal_strength": 68,
		"geography":       "unspecified, unspecified",
	}); err != nil {
		t.Fatalf("expected placeholder geography to be normalized from assignment, got %v", err)
	}

	var last events.Event
	found := false
	for i := len(store.events) - 1; i >= 0; i-- {
		if string(store.events[i].Type) == "source.scraped" {
			last = store.events[i]
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected source.scraped event, got %+v", store.events)
	}
	var payload map[string]any
	if err := json.Unmarshal(last.Payload, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if got := strings.TrimSpace(asString(payload["geography"])); got != "US" {
		t.Fatalf("expected geography=US after placeholder replacement, got %q payload=%v", got, payload)
	}
}

func TestRuntimeToolExecutor_HandleEmitToolSourceScrapedRejectsMissingGeographyWithoutAssignment(t *testing.T) {
	store := &captureStore{}
	bus := NewEventBus(store)
	exec := NewRuntimeToolExecutor(bus, nil, nil)
	actor := models.AgentConfig{
		ID:   "scanner-agent-shard-2",
		Role: "scanner-agent",
		Mode: "factory",
	}

	ctx := WithActor(context.Background(), actor)
	ctx = WithInboundEvent(ctx, events.Event{
		ID:          "scanner-assigned-3",
		Type:        events.EventType("scanner.reviews.scan_assigned"),
		SourceAgent: "pipeline-coordinator",
		Payload: mustJSON(map[string]any{
			"scan_id": "scan-geo-3",
		}),
	})
	if _, err := exec.Execute(ctx, "emit_source_scraped", map[string]any{
		"scan_id":         "scan-geo-3",
		"source":          "reviews",
		"evidence":        "Signal from review crawl",
		"signal_strength": 65,
	}); err == nil || !strings.Contains(err.Error(), "geography is required") {
		t.Fatalf("expected source.scraped to reject missing geography without assignment fallback, got %v", err)
	}
}

func TestRuntimeToolExecutor_HandleEmitToolScoreDimensionDoesNotInjectTaskID(t *testing.T) {
	store := &captureStore{}
	bus := NewEventBus(store)
	exec := NewRuntimeToolExecutor(bus, nil, nil)
	actor := models.AgentConfig{
		ID:   "analysis-agent",
		Role: "analysis-agent",
		Mode: "factory",
	}

	ctx := WithActor(context.Background(), actor)
	ctx = WithInboundEvent(ctx, events.Event{
		ID:          "score-req-1",
		Type:        events.EventType("scoring.requested"),
		SourceAgent: "pipeline-coordinator",
		TaskID:      "task-score-1",
		VerticalID:  "vertical-1",
		Payload: mustJSON(map[string]any{
			"vertical_id": "vertical-1",
		}),
	})

	if _, err := exec.Execute(ctx, "emit_score_dimension_complete", map[string]any{
		"dimension": "market_size",
		"score":     73,
		"evidence":  "validated demand signal from sources",
	}); err != nil {
		t.Fatalf("expected emit_score_dimension_complete to pass without task_id injection, got %v", err)
	}
	if len(store.events) == 0 {
		t.Fatal("expected emitted event")
	}
	var last events.Event
	found := false
	for i := len(store.events) - 1; i >= 0; i-- {
		if string(store.events[i].Type) == "score.dimension_complete" {
			last = store.events[i]
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected score.dimension_complete event, got %+v", store.events)
	}
	var payload map[string]any
	if err := json.Unmarshal(last.Payload, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if _, ok := payload["task_id"]; ok {
		t.Fatalf("expected strict payload to omit task_id, got payload=%v", payload)
	}
}
