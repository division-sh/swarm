package runtime

import (
	"strings"
	"testing"

	"empireai/internal/commgraph"
)

func TestGenerateEmitToolsForRole(t *testing.T) {
	tools := GenerateEmitTools("empire-coordinator")
	if len(tools) == 0 {
		t.Fatal("expected emit tools for empire-coordinator")
	}
	foundScan := false
	for _, tool := range tools {
		if tool.Name != "emit_scan_requested" {
			continue
		}
		foundScan = true
		schema, ok := tool.Schema.(map[string]any)
		if !ok {
			t.Fatalf("expected scan tool schema object, got %#v", tool.Schema)
		}
		required, ok := schema["required"].([]string)
		if !ok || len(required) == 0 {
			t.Fatalf("expected required list, got %#v", schema["required"])
		}
		requiredSet := make(map[string]struct{}, len(required))
		for _, field := range required {
			requiredSet[field] = struct{}{}
		}
		for _, field := range []string{"mode", "geography", "campaign_context"} {
			if _, ok := requiredSet[field]; !ok {
				t.Fatalf("expected scan schema to require %q, got %#v", field, required)
			}
		}
	}
	if !foundScan {
		t.Fatalf("expected emit_scan_requested in tools: %+v", tools)
	}
}

func TestEmitToolNameRoundTrip(t *testing.T) {
	name := emitToolName("spec.validation_passed")
	if name != "emit_spec_validation_passed" {
		t.Fatalf("unexpected emit tool name: %s", name)
	}
	eventType, ok := eventTypeFromEmitToolName(name)
	if !ok || eventType != "spec.validation_passed" {
		t.Fatalf("unexpected round-trip event type: ok=%v event=%s", ok, eventType)
	}
}

func TestEmitToolNameRoundTrip_RejectsUnknown(t *testing.T) {
	if eventType, ok := eventTypeFromEmitToolName("emit_not_a_real_event"); ok || eventType != "" {
		t.Fatalf("expected unknown emit tool to be rejected, got ok=%v event=%q", ok, eventType)
	}
}

func TestIsEmitToolAllowedForRole(t *testing.T) {
	if !IsEmitToolAllowedForRole("empire-coordinator", "emit_scan_requested") {
		t.Fatal("expected emit_scan_requested to be allowed for empire-coordinator")
	}
	if IsEmitToolAllowedForRole("spec-reviewer", "emit_scan_requested") {
		t.Fatal("did not expect emit_scan_requested to be allowed for spec-reviewer")
	}
}

func TestGenerateEmitTools_SchemaPresentForAllProducedEvents(t *testing.T) {
	roles := []string{
		"empire-coordinator",
		"factory-cto",
		"spec-auditor",
		"discovery-coordinator",
		"analysis-agent",
		"validation-coordinator",
		"business-research-agent",
		"lightweight-spec-agent",
		"spec-reviewer",
		"market-research-agent",
		"trend-research-agent",
		"pre-brand-agent",
	}
	for _, role := range roles {
		allowed := commgraph.ProducerEventsForRole(role)
		tools := GenerateEmitTools(role)
		if len(allowed) != len(tools) {
			t.Fatalf("role %s expected %d emit tools, got %d", role, len(allowed), len(tools))
		}
		for _, tool := range tools {
			schema, ok := tool.Schema.(map[string]any)
			if !ok {
				t.Fatalf("role %s tool %s missing object schema: %#v", role, tool.Name, tool.Schema)
			}
			if schema["type"] != "object" {
				t.Fatalf("role %s tool %s expected object schema, got %#v", role, tool.Name, schema["type"])
			}
		}
	}
}

func TestEventSchemaRegistry_ExplicitForAllProducerEvents(t *testing.T) {
	ensureEventSchemaRegistry()
	missing := make([]string, 0, 16)
	for _, role := range commgraph.ProducerRoles() {
		for _, eventType := range commgraph.ProducerEventsForRole(role) {
			eventType = strings.TrimSpace(eventType)
			if eventType == "" {
				continue
			}
			if _, ok := EventSchemaRegistry[eventType]; ok {
				continue
			}
			missing = append(missing, eventType+" (role="+role+")")
		}
	}
	if len(missing) > 0 {
		t.Fatalf("missing explicit EventSchemaRegistry entries: %v", missing)
	}
}

func TestEventSchemaSnapshot_IncludesStrictDefaults(t *testing.T) {
	snapshot := EventSchemaSnapshot()
	if len(snapshot) == 0 {
		t.Fatalf("expected non-empty schema snapshot")
	}
	if _, ok := snapshot["spec.approved"]; !ok {
		t.Fatalf("expected strict default schema for spec.approved in snapshot")
	}
}

func TestGeneratedEmitSchemasForAgentRoles_SubsetAndSorted(t *testing.T) {
	all := GeneratedEmitSchemas()
	allSet := make(map[string]struct{}, len(all))
	for _, eventType := range all {
		allSet[eventType] = struct{}{}
	}

	agent := GeneratedEmitSchemasForAgentRoles()
	seen := make(map[string]struct{}, len(agent))
	for i, eventType := range agent {
		if _, ok := allSet[eventType]; !ok {
			t.Fatalf("expected agent generated schema %q to be in global generated set", eventType)
		}
		if _, dup := seen[eventType]; dup {
			t.Fatalf("duplicate generated schema entry: %q", eventType)
		}
		seen[eventType] = struct{}{}
		if i > 0 && agent[i-1] > eventType {
			t.Fatalf("expected sorted generated schemas, got %q before %q", agent[i-1], eventType)
		}
	}
}

func TestGeneratedEmitSchemasForAgentRoles_None(t *testing.T) {
	if got := GeneratedEmitSchemasForAgentRoles(); len(got) != 0 {
		t.Fatalf("expected zero generated schemas for agent roles, got %d: %v", len(got), got)
	}
}

func TestGenerateEmitTools_AllAgentSchemasStrictTopLevel(t *testing.T) {
	for _, role := range commgraph.ProducerRoles() {
		tools := GenerateEmitTools(role)
		for _, tool := range tools {
			schema, ok := tool.Schema.(map[string]any)
			if !ok {
				t.Fatalf("role %s tool %s schema not object: %#v", role, tool.Name, tool.Schema)
			}
			addl, ok := schema["additionalProperties"].(bool)
			if !ok {
				t.Fatalf("role %s tool %s missing additionalProperties", role, tool.Name)
			}
			if addl {
				t.Fatalf("role %s tool %s must be strict (additionalProperties=false)", role, tool.Name)
			}
		}
	}
}

func TestEventSchemaRegistry_ScoringRequestedExplicit(t *testing.T) {
	s := schemaForEventType("scoring.requested").Schema
	required, ok := s["required"].([]string)
	if !ok {
		t.Fatalf("expected required list, got %#v", s["required"])
	}
	for _, field := range []string{"vertical_id", "vertical_name", "geography", "mode", "rubric", "dimensions_requested", "discovery_context"} {
		found := false
		for _, got := range required {
			if got == field {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected required field %q in scoring.requested schema, got %#v", field, required)
		}
	}
}

func TestEventSchemaRegistry_ScoringRequestedRejectsLegacyTaskID(t *testing.T) {
	if err := ValidateEventPayloadAgainstSchema("scoring.requested", map[string]any{
		"vertical_id":          "v1",
		"vertical_name":        "Dental Clinic Scheduling",
		"geography":            "argentina",
		"mode":                 "saas_gap",
		"rubric":               "universal",
		"dimensions_requested": []any{"build_complexity", "automation_completeness", "icp_crispness"},
		"discovery_context": map[string]any{
			"opportunity_name": "Clinic scheduling optimization",
		},
		"task_id": "task-123",
	}); err == nil {
		t.Fatal("expected scoring.requested to reject legacy task_id")
	}
}

func TestEventSchemaRegistry_ScanAndScoringSupportAutomationMicro(t *testing.T) {
	if err := ValidateEventPayloadAgainstSchema("scan.requested", map[string]any{
		"mode":      "automation_micro",
		"geography": "Argentina",
		"campaign_context": map[string]any{
			"modes":             []any{"automation_micro", "saas_gap"},
			"strategic_context": "Automation-first micro opportunities.",
			"directive_id":      "dir-1",
		},
	}); err != nil {
		t.Fatalf("expected scan.requested automation_micro payload to validate, got %v", err)
	}

	if err := ValidateEventPayloadAgainstSchema("scoring.requested", map[string]any{
		"vertical_id":          "v-auto-1",
		"vertical_name":        "Appointment Recovery",
		"geography":            "argentina",
		"mode":                 "automation_micro",
		"rubric":               "universal",
		"dimensions_requested": []any{"build_complexity", "automation_completeness", "distribution_leverage"},
		"discovery_context": map[string]any{
			"opportunity_name": "Appointment recovery automation",
		},
	}); err != nil {
		t.Fatalf("expected scoring.requested automation_micro payload to validate, got %v", err)
	}
}

func TestEventSchemaRegistry_ScoringRequestedSupportsDerivedAndCorpusModes(t *testing.T) {
	base := map[string]any{
		"vertical_id":          "v-1",
		"vertical_name":        "Roofer Dispatch AI",
		"geography":            "us",
		"rubric":               "universal",
		"dimensions_requested": []any{"build_complexity", "automation_completeness"},
		"discovery_context": map[string]any{
			"opportunity_name": "Roofer Dispatch AI",
		},
	}
	derived := cloneMap(base)
	derived["mode"] = "derived"
	derived["assigned_analysis_agent_id"] = "analysis-agent-alt"
	derived["excluded_analysis_agent_id"] = "analysis-agent"
	if err := ValidateEventPayloadAgainstSchema("scoring.requested", derived); err != nil {
		t.Fatalf("expected scoring.requested derived payload to validate, got %v", err)
	}

	corpus := cloneMap(base)
	corpus["mode"] = "corpus"
	if err := ValidateEventPayloadAgainstSchema("scoring.requested", corpus); err != nil {
		t.Fatalf("expected scoring.requested corpus payload to validate, got %v", err)
	}
}

func TestEventSchemaRegistry_ScanCompletionRequiresScanID(t *testing.T) {
	eventTypes := []string{
		"market_research.scan_complete",
		"trend_research.scan_complete",
		"scanner.google_maps.scan_complete",
		"scanner.instagram.scan_complete",
		"scanner.reviews.scan_complete",
		"scanner.directories.scan_complete",
		"scanner.yelp.scan_complete",
	}
	for _, eventType := range eventTypes {
		eventType := eventType
		t.Run(eventType, func(t *testing.T) {
			if err := ValidateEventPayloadAgainstSchema(eventType, map[string]any{}); err == nil {
				t.Fatalf("expected %s schema to require scan_id", eventType)
			}
			if err := ValidateEventPayloadAgainstSchema(eventType, map[string]any{
				"scan_id": "scan-1",
			}); err != nil {
				t.Fatalf("expected %s with scan_id to be valid, got %v", eventType, err)
			}
		})
	}
}

func TestEventSchemaRegistry_ResearchSignalsRequireScanID(t *testing.T) {
	validSamples := map[string]map[string]any{
		"category.assessed": {
			"scan_id":                "scan-1",
			"category":               "operations",
			"subcategory":            "clinic_scheduling",
			"geography":              "argentina",
			"opportunity_name":       "Clinic Scheduling Automation",
			"preliminary_icp":        "Clinic operations manager",
			"build_sketch":           sampleBuildSketch(),
			"evidence":               sampleEvidence(),
			"opportunity_hypothesis": "Automate no-show prevention and slot optimization.",
			"geographic_scope":       "local",
			"signal_strength":        74,
		},
		"trend.identified": {
			"scan_id":                "scan-2",
			"trend_category":         "regulatory",
			"geography":              "argentina",
			"signal_strength":        79,
			"opportunity_name":       "E-invoicing compliance autopilot",
			"preliminary_icp":        "SMB finance operators",
			"build_sketch":           sampleBuildSketch(),
			"evidence":               sampleEvidence(),
			"trend_description":      "Digital invoicing mandates expanding across LATAM.",
			"opportunity_hypothesis": "Launch compliance-first invoicing workflow SaaS.",
			"geographic_scope":       "regional",
		},
		"source.scraped": {
			"scan_id":   "scan-3",
			"source":    "google_maps",
			"geography": "united states",
			"evidence":  "Top local businesses have sparse digital operations stack.",
		},
	}

	for eventType, payload := range validSamples {
		eventType := eventType
		payload := payload
		t.Run(eventType, func(t *testing.T) {
			if err := ValidateEventPayloadAgainstSchema(eventType, payload); err != nil {
				t.Fatalf("expected valid %s payload, got %v", eventType, err)
			}
			delete(payload, "scan_id")
			if err := ValidateEventPayloadAgainstSchema(eventType, payload); err == nil {
				t.Fatalf("expected %s schema to reject payload missing scan_id", eventType)
			}
		})
	}
}

func TestCategoryAssessedSchema(t *testing.T) {
	valid := map[string]any{
		"scan_id":                "scan-99",
		"category":               "operations",
		"subcategory":            "clinic_scheduling",
		"geography":              "argentina",
		"opportunity_name":       "Clinic Scheduling Automation",
		"preliminary_icp":        "Clinic operations manager",
		"build_sketch":           sampleBuildSketch(),
		"evidence":               sampleEvidence(),
		"opportunity_hypothesis": "Core SaaS wedge",
		"geographic_scope":       "local",
		"signal_strength":        70,
	}
	if err := ValidateEventPayloadAgainstSchema("category.assessed", valid); err != nil {
		t.Fatalf("expected valid structured category.assessed payload, got %v", err)
	}

	invalid := map[string]any{
		"scan_id":                "scan-99",
		"category":               "operations",
		"subcategory":            "clinic_scheduling",
		"geography":              "argentina",
		"opportunity_name":       "Clinic Scheduling Automation",
		"preliminary_icp":        "Clinic operations manager",
		"build_sketch":           sampleBuildSketch(),
		"opportunity_hypothesis": "Core SaaS wedge",
		"evidence":               sampleEvidence(),
		"signal_strength":        70,
		// geographic_scope is required.
	}
	if err := ValidateEventPayloadAgainstSchema("category.assessed", invalid); err == nil {
		t.Fatal("expected category.assessed to reject payload missing geographic_scope")
	}
}

func TestTrendIdentifiedSchema(t *testing.T) {
	valid := map[string]any{
		"scan_id":                "scan-2",
		"trend_category":         "regulatory",
		"geography":              "argentina",
		"signal_strength":        79,
		"opportunity_name":       "E-invoicing compliance autopilot",
		"preliminary_icp":        "SMB finance operators",
		"build_sketch":           sampleBuildSketch(),
		"evidence":               sampleEvidence(),
		"trend_description":      "Digital invoicing mandates expanding across LATAM.",
		"opportunity_hypothesis": "Launch compliance-first invoicing workflow SaaS.",
		"geographic_scope":       "regional",
	}
	if err := ValidateEventPayloadAgainstSchema("trend.identified", valid); err != nil {
		t.Fatalf("expected valid trend.identified payload, got %v", err)
	}
	delete(valid, "trend_category")
	if err := ValidateEventPayloadAgainstSchema("trend.identified", valid); err == nil {
		t.Fatal("expected trend.identified to reject payload missing trend_category")
	}
}

func TestPipelineDeadLetterSchema(t *testing.T) {
	valid := map[string]any{
		"event_id":    "evt-1",
		"node_id":     "scoring-node",
		"event_type":  "vertical.discovered",
		"retry_count": 5,
		"last_error":  "forced failure",
	}
	if err := ValidateEventPayloadAgainstSchema("pipeline.dead_letter", valid); err != nil {
		t.Fatalf("expected valid pipeline.dead_letter payload, got %v", err)
	}
	delete(valid, "last_error")
	if err := ValidateEventPayloadAgainstSchema("pipeline.dead_letter", valid); err == nil {
		t.Fatal("expected pipeline.dead_letter to reject payload missing last_error")
	}
}

func TestScoringRequestedDiscoveryContext(t *testing.T) {
	valid := map[string]any{
		"vertical_id":          "v-1",
		"vertical_name":        "Clinic Scheduling Automation",
		"geography":            "argentina",
		"mode":                 "saas_gap",
		"rubric":               "universal",
		"dimensions_requested": []any{"build_complexity", "automation_completeness"},
		"discovery_context": map[string]any{
			"opportunity_name": "Clinic Scheduling Automation",
			"preliminary_icp":  "Clinic operations manager",
		},
	}
	if err := ValidateEventPayloadAgainstSchema("scoring.requested", valid); err != nil {
		t.Fatalf("expected scoring.requested with discovery_context to validate, got %v", err)
	}
	delete(valid, "discovery_context")
	if err := ValidateEventPayloadAgainstSchema("scoring.requested", valid); err == nil {
		t.Fatal("expected scoring.requested to require discovery_context")
	}
}

func sampleBuildSketch() map[string]any {
	return map[string]any{
		"core_features":    []any{"calendar sync", "reminder automations"},
		"key_integrations": []any{"whatsapp", "google calendar"},
		"red_flags": []any{
			map[string]any{
				"type":  "one_time_setup",
				"notes": "Needs onboarding workflow",
			},
		},
	}
}

func sampleEvidence() map[string]any {
	return map[string]any{
		"competitors": []any{
			map[string]any{
				"name":       "ClinicFlow",
				"pricing":    "$49/mo",
				"source_url": "https://example.com/clinicflow",
			},
		},
		"pain_signals": []any{
			map[string]any{
				"signal":     "No-show rate remains high",
				"source_url": "https://example.com/pain",
			},
		},
		"regulatory": []any{
			map[string]any{
				"detail":     "Appointment reminders must include consent",
				"source_url": "https://example.com/reg",
			},
		},
		"buyer_communities": []any{
			map[string]any{
				"name":       "Clinic Ops LATAM",
				"source_url": "https://example.com/community",
			},
		},
	}
}

func TestEventSchemaRegistry_ScoreDimensionCompleteStrictAndBounded(t *testing.T) {
	s := schemaForEventType("score.dimension_complete").Schema
	if addl, ok := s["additionalProperties"].(bool); !ok || addl {
		t.Fatalf("expected strict score.dimension_complete schema, got additionalProperties=%#v", s["additionalProperties"])
	}
	valid := map[string]any{
		"vertical_id": "v1",
		"dimension":   "market_size",
		"score":       88,
		"evidence":    "source says demand is high",
	}
	if err := ValidateEventPayloadAgainstSchema("score.dimension_complete", valid); err != nil {
		t.Fatalf("expected valid payload, got %v", err)
	}
	withContext := map[string]any{
		"vertical_id": "v1",
		"task_id":     "task-1",
		"dimension":   "market_size",
		"score":       88,
		"evidence":    "source says demand is high",
	}
	if err := ValidateEventPayloadAgainstSchema("score.dimension_complete", withContext); err == nil {
		t.Fatal("expected task_id to be rejected by strict score.dimension_complete schema")
	}
	if err := ValidateEventPayloadAgainstSchema("score.dimension_complete", map[string]any{
		"vertical_id": "v1",
		"dimension":   "market_size",
		"score":       101,
		"evidence":    "too high",
	}); err == nil {
		t.Fatal("expected max bound violation for score > 100")
	}
	if err := ValidateEventPayloadAgainstSchema("score.dimension_complete", map[string]any{
		"vertical_id": "v1",
		"dimension":   "market_size",
		"score":       -1,
		"evidence":    "too low",
	}); err == nil {
		t.Fatal("expected min bound violation for score < 0")
	}
	if err := ValidateEventPayloadAgainstSchema("score.dimension_complete", map[string]any{
		"vertical_id": "v1",
		"dimension":   "market_size",
		"score":       70,
		"evidence":    "ok",
		"extra":       "nope",
	}); err == nil {
		t.Fatal("expected additional property rejection")
	}
}

func TestEventSchemaRegistry_VerticalShortlistedStrictPayload(t *testing.T) {
	valid := map[string]any{
		"vertical_id":     "v1",
		"composite_score": 81.2,
		"viability_score": 72.5,
		"scoring_payload": map[string]any{"dims": map[string]any{"market_size": 75}},
	}
	if err := ValidateEventPayloadAgainstSchema("vertical.shortlisted", valid); err != nil {
		t.Fatalf("expected valid shortlisted payload, got %v", err)
	}
	if err := ValidateEventPayloadAgainstSchema("vertical.shortlisted", map[string]any{
		"vertical_id":     "v1",
		"composite_score": 81.2,
		"viability_score": 72.5,
		"scoring_payload": map[string]any{},
		"reasoning":       "legacy field",
	}); err == nil {
		t.Fatal("expected legacy reasoning to be rejected")
	}
}

func TestEventSchemaRegistry_VerticalMarginalStrictPayload(t *testing.T) {
	if err := ValidateEventPayloadAgainstSchema("vertical.marginal", map[string]any{
		"vertical_id":        "v1",
		"composite_score":    63.0,
		"viability_score":    67.0,
		"dimensions":         map[string]any{"pain_severity": 70},
		"promotion_eligible": true,
	}); err != nil {
		t.Fatalf("expected valid marginal payload, got %v", err)
	}
	if err := ValidateEventPayloadAgainstSchema("vertical.marginal", map[string]any{
		"vertical_id":        "v1",
		"composite_score":    63.0,
		"viability_score":    67.0,
		"dimensions":         map[string]any{"pain_severity": 70},
		"promotion_eligible": true,
		"reasoning":          "legacy field",
	}); err == nil {
		t.Fatal("expected legacy reasoning to be rejected")
	}
}
