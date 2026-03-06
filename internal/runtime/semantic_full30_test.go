package runtime

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"empireai/internal/events"
	"empireai/internal/models"
	runtimetools "empireai/internal/runtime/tools"
	"empireai/internal/testutil"
	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

type semanticFullMatrix struct {
	Cases []semanticFullCase `yaml:"cases"`
}

type semanticFullCase struct {
	ID string `yaml:"id"`
}

func TestSemanticFull30Matrix(t *testing.T) {
	repoRoot := contractComplianceRepoRoot(t)
	raw, err := os.ReadFile(filepath.Join(repoRoot, "contracts", "test-vectors", "semantic-full-30.yaml"))
	if err != nil {
		t.Fatalf("read semantic full matrix: %v", err)
	}
	var matrix semanticFullMatrix
	if err := yaml.Unmarshal(raw, &matrix); err != nil {
		t.Fatalf("parse semantic full matrix: %v", err)
	}
	if got, want := len(matrix.Cases), 30; got != want {
		t.Fatalf("semantic matrix case count mismatch: got=%d want=%d", got, want)
	}

	checks := map[string]func(*testing.T){
		"prefilter_passthrough":                    checkPrefilterPassthrough,
		"prefilter_blocking_red_flag":              checkPrefilterBlockingRedFlag,
		"prefilter_co_occurrence":                  checkPrefilterCoOccurrence,
		"prefilter_signal_floor":                   checkPrefilterSignalFloor,
		"prefilter_evidence_gate":                  checkPrefilterEvidenceGate,
		"prefilter_retention_gate":                 checkPrefilterRetentionGate,
		"corpus_jsonl_batching":                    checkCorpusJSONLBatching,
		"scan_expected_agents_modes":               checkScanExpectedAgentsModes,
		"scan_completion_unique_keys":              checkScanCompletionUniqueKeys,
		"campaign_mode_cycling":                    checkCampaignModeCycling,
		"dedup_exact_and_fuzzy":                    checkDedupExactAndFuzzy,
		"dedup_resolution_drain":                   checkDedupResolutionDrain,
		"scoring_dispatch_rubric_dimensions":       checkScoringDispatchRubricDimensions,
		"derivation_anti_bias_exclusion":           checkDerivationAntiBiasExclusion,
		"derivation_anti_bias_fallback":            checkDerivationAntiBiasFallback,
		"derivation_depth_cap":                     checkDerivationDepthCap,
		"derivation_branch_cap":                    checkDerivationBranchCap,
		"composite_shortlisted":                    checkCompositeShortlisted,
		"rejection_cascade_order":                  checkRejectionCascadeOrder,
		"scoring_contest_emit_and_resolve":         checkScoringContestEmitAndResolve,
		"validation_gate_tracking":                 checkValidationGateTracking,
		"validation_staleness_drop":                checkValidationStalenessDrop,
		"revision_routing_on_validation_failed":    checkRevisionRoutingOnValidationFailed,
		"inner_revision_limit":                     checkInnerRevisionLimit,
		"validation_packaging_trigger":             checkValidationPackagingTrigger,
		"campaign_completion_requires_empty_queue": checkCampaignCompletionRequiresEmptyQueue,
		"opco_org_creation_13_agents":              checkOpCoOrgCreation13Agents,
		"opco_routes_and_template_version":         checkOpCoRoutesAndTemplateVersion,
		"cycle_counter_circuit_breaker":            checkCycleCounterCircuitBreaker,
		"budget_human_mailbox_contracts":           checkBudgetHumanMailboxContracts,
	}

	for _, tc := range matrix.Cases {
		tc := tc
		t.Run(tc.ID, func(t *testing.T) {
			check := checks[strings.TrimSpace(tc.ID)]
			if check == nil {
				t.Fatalf("missing semantic check for %q", tc.ID)
			}
			check(t)
		})
	}
}

func checkPrefilterPassthrough(t *testing.T) {
	tc := prefilterContractCase{
		Name:     "matrix_passthrough",
		Expected: "pass",
	}
	tc.Input.SignalStrength = 68
	tc.Input.RedFlags = []string{"accuracy_liability", "one_time_setup"}
	tc.Input.EvidenceURLs = 3
	tc.Input.RetentionPrimitives = []string{"workflow_embedding"}
	payload := buildPrefilterFixturePayload(tc)
	ok, _, reason := evaluateDiscoveryPreFilter(payload, tc.Input.SignalStrength)
	if !ok || reason != "" {
		t.Fatalf("expected pass, got ok=%v reason=%q", ok, reason)
	}
}

func checkPrefilterBlockingRedFlag(t *testing.T) {
	tc := prefilterContractCase{
		Name:           "matrix_blocking_flag",
		Expected:       "reject",
		ExpectedReason: "blocking_red_flag",
	}
	tc.Input.SignalStrength = 75
	tc.Input.RedFlags = []string{"phone_led_sales"}
	tc.Input.EvidenceURLs = 3
	tc.Input.RetentionPrimitives = []string{"recurring_data"}
	payload := buildPrefilterFixturePayload(tc)
	ok, _, reason := evaluateDiscoveryPreFilter(payload, tc.Input.SignalStrength)
	if ok || reason != "blocking_red_flag" {
		t.Fatalf("expected blocking red flag reject, got ok=%v reason=%q", ok, reason)
	}
}

func checkPrefilterCoOccurrence(t *testing.T) {
	tc := prefilterContractCase{Name: "matrix_co_occurrence"}
	tc.Input.SignalStrength = 80
	tc.Input.RedFlags = []string{"complex_integration", "high_feature_count"}
	tc.Input.EvidenceURLs = 4
	tc.Input.RetentionPrimitives = []string{"integration_lock_in"}
	payload := buildPrefilterFixturePayload(tc)
	ok, _, reason := evaluateDiscoveryPreFilter(payload, tc.Input.SignalStrength)
	if ok || reason != "co_occurrence_block" {
		t.Fatalf("expected co_occurrence_block reject, got ok=%v reason=%q", ok, reason)
	}
}

func checkPrefilterSignalFloor(t *testing.T) {
	tc := prefilterContractCase{Name: "matrix_signal_floor"}
	tc.Input.SignalStrength = 54
	tc.Input.EvidenceURLs = 3
	tc.Input.RetentionPrimitives = []string{"recurring_data"}
	payload := buildPrefilterFixturePayload(tc)
	ok, _, reason := evaluateDiscoveryPreFilter(payload, tc.Input.SignalStrength)
	if ok || reason != "signal_below_threshold" {
		t.Fatalf("expected signal_below_threshold reject, got ok=%v reason=%q", ok, reason)
	}
}

func checkPrefilterEvidenceGate(t *testing.T) {
	tc := prefilterContractCase{Name: "matrix_evidence_gate"}
	tc.Input.SignalStrength = 70
	tc.Input.EvidenceURLs = 1
	tc.Input.RetentionPrimitives = []string{"workflow_embedding"}
	payload := buildPrefilterFixturePayload(tc)
	ok, _, reason := evaluateDiscoveryPreFilter(payload, tc.Input.SignalStrength)
	if ok || reason != "evidence_insufficient" {
		t.Fatalf("expected evidence_insufficient reject, got ok=%v reason=%q", ok, reason)
	}
}

func checkPrefilterRetentionGate(t *testing.T) {
	tc := prefilterContractCase{Name: "matrix_retention_gate"}
	tc.Input.SignalStrength = 70
	tc.Input.EvidenceURLs = 3
	tc.Input.RetentionPrimitives = []string{}
	payload := buildPrefilterFixturePayload(tc)
	ok, _, reason := evaluateDiscoveryPreFilter(payload, tc.Input.SignalStrength)
	if ok || reason != "no_retention_primitive" {
		t.Fatalf("expected no_retention_primitive reject, got ok=%v reason=%q", ok, reason)
	}
}

func checkCorpusJSONLBatching(t *testing.T) {
	path := filepath.Join(t.TempDir(), "matrix-corpus.jsonl")
	lines := make([]string, 0, 52)
	for i := 0; i < 52; i++ {
		lines = append(lines, `{"signal":"s`+asString(i)+`","idx":`+asString(i)+`}`)
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatalf("write matrix corpus jsonl: %v", err)
	}
	batches, err := readJSONLFile(path, corpusBatchSize)
	if err != nil {
		t.Fatalf("readJSONLFile: %v", err)
	}
	if len(batches) != 3 {
		t.Fatalf("expected 3 batches, got %d", len(batches))
	}
	if len(batches[0]) != 25 || len(batches[1]) != 25 || len(batches[2]) != 2 {
		t.Fatalf("unexpected batch sizes: [%d %d %d]", len(batches[0]), len(batches[1]), len(batches[2]))
	}
}

func checkScanExpectedAgentsModes(t *testing.T) {
	if got := expectedAgents("local_services"); got != localServicesScannerExpected {
		t.Fatalf("local_services expected agents mismatch: got=%d want=%d", got, localServicesScannerExpected)
	}
	for _, mode := range []string{"saas_gap", "saas_trend", "automation_micro", "corpus"} {
		if got := expectedAgents(mode); got != 1 {
			t.Fatalf("mode=%s expected agents mismatch: got=%d want=1", mode, got)
		}
	}
}

func checkScanCompletionUniqueKeys(t *testing.T) {
	ctx := context.Background()
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, nil)
	scanID := "scan-" + strings.ReplaceAll(uuid.NewString(), "-", "")
	pc.handleScanRequested(ctx, events.Event{
		ID:   uuid.NewString(),
		Type: events.EventType("scan.requested"),
		Payload: mustJSON(map[string]any{
			"scan_id":     scanID,
			"campaign_id": uuid.NewString(),
			"mode":        "local_services",
			"geography":   "us",
		}),
	})
	for i := 0; i < 2; i++ {
		pc.handleScanCompletion(ctx, events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("scanner.google_maps.scan_complete"),
			SourceAgent: "scanner-agent",
			Payload:     mustJSON(map[string]any{"scan_id": scanID}),
		})
	}
	pc.mu.Lock()
	acc := pc.scans[scanID]
	pc.mu.Unlock()
	if acc == nil {
		t.Fatal("scan accumulator should still exist for local_services partial completion")
	}
	if got := len(acc.CompletedBy); got != 1 {
		t.Fatalf("expected unique completion key accounting, got completed_by=%d", got)
	}
}

func checkCampaignModeCycling(t *testing.T) {
	got := remainingCampaignModes("saas_gap")
	if len(got) != 2 || got[0] != "saas_trend" || got[1] != "local_services" {
		t.Fatalf("saas_gap cycle mismatch: %v", got)
	}
	if out := remainingCampaignModes("corpus"); len(out) != 0 {
		t.Fatalf("corpus should be terminal mode, got remaining=%v", out)
	}
}

func checkDedupExactAndFuzzy(t *testing.T) {
	if normalizeName("Pet Grooming Scheduling") != normalizeName("pet   grooming scheduling") {
		t.Fatal("expected exact normalized dedup match")
	}
	existing := []verticalCandidate{
		{ID: "v1", Name: "Pet Grooming Scheduling"},
		{ID: "v2", Name: "Dental Claims Copilot"},
	}
	best, score := fuzzyBestMatch("Pet Grooming Scheduling SaaS", existing)
	if best.ID != "v1" || score < 0.70 {
		t.Fatalf("expected fuzzy hold candidate v1 score>=0.70, got id=%s score=%.2f", best.ID, score)
	}
	_, low := fuzzyBestMatch("Warehouse Robotics", existing)
	if low >= 0.70 {
		t.Fatalf("expected unrelated fuzzy score < 0.70, got %.2f", low)
	}
}

func checkDedupResolutionDrain(t *testing.T) {
	ctx := context.Background()
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), nil)
	dedupID := "dedup-" + strings.ReplaceAll(uuid.NewString(), "-", "")
	pc.mu.Lock()
	pc.pendingDedup[dedupID] = pendingCandidate{
		DedupEventID: dedupID,
		ScanID:       "scan-1",
		CampaignID:   "campaign-1",
		Mode:         "saas_gap",
		Name:         "Candidate",
		Geography:    "us",
		Signal:       72,
		Payload:      map[string]any{"opportunity_name": "Candidate"},
		ExistingID:   "existing-1",
	}
	pc.mu.Unlock()
	pc.handleDedupResolved(ctx, events.Event{
		ID:   uuid.NewString(),
		Type: events.EventType("dedup.resolved"),
		Payload: mustJSON(map[string]any{
			"dedup_event_id": dedupID,
			"action":         "keep_existing",
		}),
	})
	pc.mu.Lock()
	_, exists := pc.pendingDedup[dedupID]
	pc.mu.Unlock()
	if exists {
		t.Fatalf("expected pending dedup candidate drained for %s", dedupID)
	}
}

func checkScoringDispatchRubricDimensions(t *testing.T) {
	ctx := context.Background()
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, nil)
	ch := bus.Subscribe("analysis-agent", events.EventType("scoring.requested"))
	verticalID := uuid.NewString()
	pc.handleScoringRequested(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("scoring.requested"),
		VerticalID: verticalID,
		Payload: mustJSON(map[string]any{
			"vertical_id":   verticalID,
			"vertical_name": "Dispatch Matrix Vertical",
			"geography":     "us",
			"mode":          "corpus",
		}),
	})
	out := waitForEventType(t, ch, "scoring.requested")
	payload := parsePayloadMap(out.Payload)
	if got := strings.TrimSpace(asString(payload["rubric"])); got != "universal" {
		t.Fatalf("expected rubric=universal, got %q", got)
	}
	dims, _ := payload["dimensions_requested"].([]any)
	if len(dims) != len(expectedScoringDimensions("universal")) {
		t.Fatalf("dimensions mismatch: got=%d want=%d", len(dims), len(expectedScoringDimensions("universal")))
	}
}

func checkDerivationAntiBiasExclusion(t *testing.T) {
	ctx := context.Background()
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, nil)
	generatorCh := bus.Subscribe("analysis-agent", events.EventType("scoring.requested"))
	alternateCh := bus.Subscribe("analysis-agent-alt", events.EventType("scoring.requested"))
	verticalID := uuid.NewString()
	pc.handleScoringRequested(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("scoring.requested"),
		VerticalID: verticalID,
		Payload: mustJSON(map[string]any{
			"vertical_id":   verticalID,
			"vertical_name": "Derived Anti Bias",
			"geography":     "us",
			"mode":          "derived",
			"discovery_context": map[string]any{
				"parent_id":          uuid.NewString(),
				"generation_depth":   1,
				"generator_agent_id": "analysis-agent",
			},
		}),
	})
	out := waitForEventType(t, alternateCh, "scoring.requested")
	payload := parsePayloadMap(out.Payload)
	if got := strings.TrimSpace(asString(payload["excluded_analysis_agent_id"])); got != "analysis-agent" {
		t.Fatalf("excluded_analysis_agent_id mismatch: %q", got)
	}
	if got := strings.TrimSpace(asString(payload["assigned_analysis_agent_id"])); got != "analysis-agent-alt" {
		t.Fatalf("assigned_analysis_agent_id mismatch: %q", got)
	}
	assertNoEventType(t, generatorCh, "scoring.requested", 200*time.Millisecond)
}

func checkDerivationAntiBiasFallback(t *testing.T) {
	ctx := context.Background()
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, nil)
	generatorCh := bus.Subscribe("analysis-agent", events.EventType("scoring.requested"))
	verticalID := uuid.NewString()
	pc.handleScoringRequested(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("scoring.requested"),
		VerticalID: verticalID,
		Payload: mustJSON(map[string]any{
			"vertical_id":   verticalID,
			"vertical_name": "Derived Fallback",
			"geography":     "us",
			"mode":          "derived",
			"discovery_context": map[string]any{
				"parent_id":          uuid.NewString(),
				"generation_depth":   1,
				"generator_agent_id": "analysis-agent",
			},
		}),
	})
	out := waitForEventType(t, generatorCh, "scoring.requested")
	payload := parsePayloadMap(out.Payload)
	if got := strings.TrimSpace(asString(payload["excluded_analysis_agent_id"])); got != "analysis-agent" {
		t.Fatalf("excluded_analysis_agent_id mismatch: %q", got)
	}
	if _, ok := payload["assigned_analysis_agent_id"]; ok {
		t.Fatalf("fallback should not include assigned_analysis_agent_id, payload=%v", payload)
	}
}

func checkDerivationDepthCap(t *testing.T) {
	ctx := context.Background()
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, nil)
	discovered := bus.Subscribe("matrix-depth-cap", events.EventType("vertical.discovered"))
	pc.handleVerticalDerived(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("vertical.derived"),
		SourceAgent: "analysis-agent",
		Payload: mustJSON(map[string]any{
			"parent_id":              uuid.NewString(),
			"generation_depth":       3,
			"generator_agent_id":     "analysis-agent",
			"derivation_rationale":   map[string]any{"reason": "matrix"},
			"opportunity_name":       "Depth Cap Candidate",
			"signal_strength":        72,
			"preliminary_icp":        "Owner at clinic invoice desk",
			"opportunity_hypothesis": "Automate invoice reporting",
		}),
	})
	assertNoEventType(t, discovered, "vertical.discovered", 150*time.Millisecond)
}

func checkDerivationBranchCap(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	discovered := bus.Subscribe("matrix-branch-cap", events.EventType("vertical.discovered"))

	parentID := uuid.NewString()
	insertTestVertical(t, db, parentID, "Parent Matrix", "us")
	for i := 0; i < 2; i++ {
		childID := uuid.NewString()
		if _, err := db.ExecContext(ctx, `
			INSERT INTO verticals (id, name, slug, geography, stage, mode, parent_id, created_at, updated_at)
			VALUES ($1::uuid, $2, $3, 'us', 'discovered', 'factory', $4::uuid, now(), now())
		`, childID, "Child Matrix "+asString(i), "child-matrix-"+asString(i), parentID); err != nil {
			t.Fatalf("seed child vertical: %v", err)
		}
	}
	pc.handleVerticalDerived(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("vertical.derived"),
		SourceAgent: "analysis-agent",
		Payload: mustJSON(map[string]any{
			"parent_id":              parentID,
			"generation_depth":       1,
			"generator_agent_id":     "analysis-agent",
			"derivation_rationale":   map[string]any{"reason": "matrix"},
			"opportunity_name":       "Branch Cap Candidate",
			"signal_strength":        72,
			"geography":              "us",
			"preliminary_icp":        "Owner at clinic invoice desk",
			"opportunity_hypothesis": "Automate invoice reporting",
			"retention_primitives":   []any{"workflow_embedding"},
			"build_sketch": map[string]any{
				"red_flags": []any{},
			},
			"evidence": map[string]any{
				"competitors":       []any{map[string]any{"name": "Comp", "pricing": "$99", "source_url": "https://example.com/c"}},
				"pain_signals":      []any{map[string]any{"signal": "Pain", "source_url": "https://example.com/p"}},
				"buyer_communities": []any{map[string]any{"name": "Community", "source_url": "https://example.com/b"}},
			},
		}),
	})
	assertNoEventType(t, discovered, "vertical.discovered", 150*time.Millisecond)
}

func checkCompositeShortlisted(t *testing.T) {
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), nil)
	acc := newUniversalAccumulator(uuid.NewString(), "Composite Matrix", "us", "saas_gap")
	setScores(acc, map[string]int{
		"build_complexity":        78,
		"automation_completeness": 78,
		"icp_crispness":           78,
		"distribution_leverage":   78,
		"time_to_value":           78,
		"operational_drag":        78,
		"pain_severity":           78,
		"competition_gap":         78,
		"monetization_clarity":    78,
		"retention_architecture":  78,
		"expansion_potential":     78,
	})
	res := pc.computeComposite(acc, false)
	if res.Result != "shortlisted" {
		t.Fatalf("expected shortlisted, got %+v", res)
	}
	if res.CompositeScore < 77.9 || res.CompositeScore > 78.1 {
		t.Fatalf("expected composite ~78, got %.3f", res.CompositeScore)
	}
}

func checkRejectionCascadeOrder(t *testing.T) {
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), nil)

	gate := newUniversalAccumulator(uuid.NewString(), "Gate", "us", "saas_gap")
	setScores(gate, map[string]int{
		"build_complexity":        40,
		"automation_completeness": 80,
		"icp_crispness":           80,
		"distribution_leverage":   80,
		"time_to_value":           80,
		"operational_drag":        80,
		"pain_severity":           80,
		"competition_gap":         80,
		"monetization_clarity":    80,
		"retention_architecture":  80,
		"expansion_potential":     80,
	})
	if got := pc.computeComposite(gate, false).Reason; got != "gate_build_complexity" {
		t.Fatalf("expected first cascade reject gate_build_complexity, got %q", got)
	}

	tier1 := newUniversalAccumulator(uuid.NewString(), "Tier1", "us", "saas_gap")
	setScores(tier1, map[string]int{
		"build_complexity":        80,
		"automation_completeness": 80,
		"icp_crispness":           40,
		"distribution_leverage":   80,
		"time_to_value":           80,
		"operational_drag":        80,
		"pain_severity":           80,
		"competition_gap":         80,
		"monetization_clarity":    80,
		"retention_architecture":  80,
		"expansion_potential":     80,
	})
	if got := pc.computeComposite(tier1, false).Reason; got != "tier1_dimension_floor_icp_crispness" {
		t.Fatalf("expected tier1 floor reject, got %q", got)
	}
}

func checkScoringContestEmitAndResolve(t *testing.T) {
	ctx := context.Background()
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, nil)
	verticalID := uuid.NewString()
	acc := newUniversalAccumulator(verticalID, "Contest Matrix", "us", "saas_gap")
	setScores(acc, map[string]int{"icp_crispness": 40})
	pc.mu.Lock()
	pc.scoring[verticalID] = acc
	pc.mu.Unlock()

	contested := bus.Subscribe("matrix-contested", events.EventType("scoring.contested"))
	scored := bus.Subscribe("matrix-scored", events.EventType("vertical.scored"))

	pc.handleScoreDimensionComplete(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("score.dimension_complete"),
		VerticalID: verticalID,
		Payload: mustJSON(map[string]any{
			"vertical_id": verticalID,
			"dimension":   "icp_crispness",
			"score":       90,
			"evidence":    "conflicting scorer",
		}),
	})
	waitForEventType(t, contested, "scoring.contested")

	pc.handleScoringContestResolved(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("scoring.contest_resolved"),
		VerticalID: verticalID,
		Payload: mustJSON(map[string]any{
			"vertical_id":    verticalID,
			"dimension":      "icp_crispness",
			"resolved_score": 75,
			"reasoning":      "tie-break",
		}),
	})
	waitForEventType(t, scored, "vertical.scored")
}

func checkValidationGateTracking(t *testing.T) {
	ctx := context.Background()
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), nil)
	verticalID := uuid.NewString()
	pc.handleValidationStarted(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("vertical.shortlisted"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"composite_score": 81}),
	})
	pc.handleValidationGate(ctx, events.Event{ID: uuid.NewString(), Type: events.EventType("research.completed"), VerticalID: verticalID, Payload: mustJSON(map[string]any{"ok": true})}, "g1")
	pc.handleValidationGate(ctx, events.Event{ID: uuid.NewString(), Type: events.EventType("spec.approved"), VerticalID: verticalID, Payload: mustJSON(map[string]any{"ok": true})}, "g2")
	pc.handleValidationGate(ctx, events.Event{ID: uuid.NewString(), Type: events.EventType("cto.spec_approved"), VerticalID: verticalID, Payload: mustJSON(map[string]any{"ok": true})}, "g3")
	pc.handleValidationGate(ctx, events.Event{ID: uuid.NewString(), Type: events.EventType("brand.candidates_ready"), VerticalID: verticalID, Payload: mustJSON(map[string]any{"ok": true})}, "g4")
	pc.mu.Lock()
	st := pc.validations[verticalID]
	pc.mu.Unlock()
	if st == nil || !(st.G1Research && st.G2Spec && st.G3CTO && st.G4Brand) {
		t.Fatalf("expected all validation gates true, state=%+v", st)
	}
}

func checkValidationStalenessDrop(t *testing.T) {
	ctx := context.Background()
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), nil)
	verticalID := uuid.NewString()
	pc.handleValidationStarted(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("vertical.shortlisted"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"composite_score": 81}),
	})
	pc.handleValidationGate(ctx, events.Event{ID: uuid.NewString(), Type: events.EventType("research.completed"), VerticalID: verticalID, Payload: mustJSON(map[string]any{"ok": true})}, "g1")
	pc.handleValidationGate(ctx, events.Event{ID: uuid.NewString(), Type: events.EventType("spec.approved"), VerticalID: verticalID, Payload: mustJSON(map[string]any{"ok": true})}, "g2")
	pc.handleValidationGate(ctx, events.Event{ID: uuid.NewString(), Type: events.EventType("cto.spec_approved"), VerticalID: verticalID, Payload: mustJSON(map[string]any{"ok": true})}, "g3")
	pc.handleValidationGate(ctx, events.Event{ID: uuid.NewString(), Type: events.EventType("brand.candidates_ready"), VerticalID: verticalID, Payload: mustJSON(map[string]any{"ok": true})}, "g4")
	pc.handleValidationPackaged(ctx, events.Event{ID: uuid.NewString(), Type: events.EventType("vertical.ready_for_review"), VerticalID: verticalID})
	if reason := pc.interceptStateDropReason("research.completed", events.Event{VerticalID: verticalID}); strings.TrimSpace(reason) == "" {
		t.Fatal("expected stale research drop reason after packaged state")
	}
}

func checkRevisionRoutingOnValidationFailed(t *testing.T) {
	ctx := context.Background()
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, nil)
	ch := bus.Subscribe("matrix-revision", events.EventType("spec.revision_requested"))
	verticalID := uuid.NewString()
	pc.handleValidationStarted(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("vertical.shortlisted"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"composite_score": 81}),
	})
	pc.handleSpecValidationFailed(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("spec.validation_failed"),
		VerticalID: verticalID,
		Payload: mustJSON(map[string]any{
			"status": "blocker",
		}),
	})
	waitForEventType(t, ch, "spec.revision_requested")
	pc.mu.Lock()
	st := pc.validations[verticalID]
	pc.mu.Unlock()
	if st == nil || st.RevisionCount != 1 {
		t.Fatalf("expected revision_count=1, state=%+v", st)
	}
}

func checkInnerRevisionLimit(t *testing.T) {
	ctx := context.Background()
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), nil)
	verticalID := uuid.NewString()
	pc.handleValidationStarted(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("vertical.shortlisted"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"composite_score": 81}),
	})
	for i := 0; i < maxInnerRevisions; i++ {
		if escalated := pc.handleInnerSpecRevision(ctx, events.Event{
			ID:         uuid.NewString(),
			Type:       events.EventType("spec.revision_needed"),
			VerticalID: verticalID,
			Payload:    mustJSON(map[string]any{"cycle": i + 1}),
		}); escalated {
			t.Fatalf("did not expect escalation at cycle %d", i+1)
		}
	}
	if escalated := pc.handleInnerSpecRevision(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("spec.revision_needed"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"cycle": maxInnerRevisions + 1}),
	}); !escalated {
		t.Fatal("expected escalation after max inner revision cycles")
	}
	pc.mu.Lock()
	st := pc.validations[verticalID]
	pc.mu.Unlock()
	if st == nil || st.Status != "parked" {
		t.Fatalf("expected parked validation state after inner revision limit, state=%+v", st)
	}
}

func checkValidationPackagingTrigger(t *testing.T) {
	ctx := context.Background()
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, nil)
	ch := bus.Subscribe("matrix-packaging", events.EventType("validation.package_ready"))
	verticalID := uuid.NewString()
	pc.handleValidationStarted(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("vertical.shortlisted"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"composite_score": 81}),
	})
	pc.handleValidationGate(ctx, events.Event{ID: uuid.NewString(), Type: events.EventType("research.completed"), VerticalID: verticalID, Payload: mustJSON(map[string]any{"ok": true})}, "g1")
	pc.handleValidationGate(ctx, events.Event{ID: uuid.NewString(), Type: events.EventType("spec.approved"), VerticalID: verticalID, Payload: mustJSON(map[string]any{"ok": true})}, "g2")
	pc.handleValidationGate(ctx, events.Event{ID: uuid.NewString(), Type: events.EventType("cto.spec_approved"), VerticalID: verticalID, Payload: mustJSON(map[string]any{"ok": true})}, "g3")
	pc.handleValidationGate(ctx, events.Event{ID: uuid.NewString(), Type: events.EventType("brand.candidates_ready"), VerticalID: verticalID, Payload: mustJSON(map[string]any{"ok": true})}, "g4")
	waitForEventType(t, ch, "validation.package_ready")
}

func checkCampaignCompletionRequiresEmptyQueue(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	bus := NewEventBus(InMemoryEventStore{})
	store := &directiveCampaignStore{db: db}
	manager := NewScanCampaignManager(bus, store, db)

	geoID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO geographies (id, name, country, created_at)
		VALUES ($1::uuid, 'Matrix Geography', 'US', now())
	`, geoID); err != nil {
		t.Fatalf("insert geography: %v", err)
	}
	completed, err := store.CreateScanCampaign(ctx, CreateScanCampaignInput{
		GeographyID: geoID,
		Mode:        "corpus",
		Categories:  []string{"ops"},
		Priority:    "normal",
		Status:      "completed",
	})
	if err != nil {
		t.Fatalf("create completed campaign: %v", err)
	}
	active, err := store.CreateScanCampaign(ctx, CreateScanCampaignInput{
		GeographyID: geoID,
		Mode:        "saas_trend",
		Categories:  []string{"ops"},
		Priority:    "normal",
		Status:      "queued",
	})
	if err != nil {
		t.Fatalf("create active campaign: %v", err)
	}
	if emitted := manager.emitCampaignCompletedIfDone(ctx, completed.ID, 2, uuid.NewString()); emitted {
		t.Fatal("campaign.completed should not emit while queued campaigns remain")
	}
	if _, err := db.ExecContext(ctx, `UPDATE scan_campaigns SET status='completed' WHERE id=$1::uuid`, active.ID); err != nil {
		t.Fatalf("complete active campaign: %v", err)
	}
	ch := bus.Subscribe("matrix-campaign-completed", events.EventType("campaign.completed"))
	if emitted := manager.emitCampaignCompletedIfDone(ctx, completed.ID, 2, uuid.NewString()); !emitted {
		t.Fatal("campaign.completed should emit once queue is empty")
	}
	waitForEventType(t, ch, "campaign.completed")
}

func checkOpCoOrgCreation13Agents(t *testing.T) {
	roster := defaultOpCoRoster("v1")
	if len(roster) != 13 {
		t.Fatalf("expected 13-agent default opco roster, got %d", len(roster))
	}
	foundCEO := false
	for _, spec := range roster {
		if strings.TrimSpace(spec.Config.Role) == "opco-ceo" {
			foundCEO = true
			break
		}
	}
	if !foundCEO {
		t.Fatal("expected opco-ceo in default roster")
	}
}

func checkOpCoRoutesAndTemplateVersion(t *testing.T) {
	routes := defaultOpCoRoutes("v1")
	if len(routes) != 20 {
		t.Fatalf("expected 20 default opco routes, got %d", len(routes))
	}
	bootstrap := 0
	seeded := 0
	for _, rt := range routes {
		switch strings.TrimSpace(rt.Source) {
		case "bootstrap":
			bootstrap++
		case "seeded":
			seeded++
		}
	}
	if bootstrap != 20 || seeded != 0 {
		t.Fatalf("expected bootstrap=20 and seeded=0 routes, got bootstrap=%d seeded=%d", bootstrap, seeded)
	}

	bus := NewEventBus(InMemoryEventStore{})
	am := NewAgentManager(bus, nil)
	store := &templateStoreStub{
		bootstrapVersion: 7,
		info: VerticalInfo{
			ID:        "v1",
			Name:      "Acme Vertical",
			Slug:      "acme",
			Geography: "US",
		},
	}
	am.store = store
	if err := am.SpawnOpCo("v1", models.MandateDocument{VerticalID: "v1"}); err != nil {
		t.Fatalf("SpawnOpCo: %v", err)
	}
	if store.setTplCalls != 1 || strings.TrimSpace(store.lastTplVersion) == "" {
		t.Fatalf("expected template version tracking call, calls=%d version=%q", store.setTplCalls, store.lastTplVersion)
	}
}

func checkCycleCounterCircuitBreaker(t *testing.T) {
	ctx := context.Background()
	tracker := NewOpCoCycleTracker(nil)
	verticalID := uuid.NewString()
	var escalated bool
	var escalation *events.Event
	for i := 0; i < defaultOpCoCycleLimit; i++ {
		escalated, escalation = tracker.Check(ctx, events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("qa.validation_failed"),
			VerticalID:  verticalID,
			SourceAgent: "opco-qa-" + verticalID,
			Payload:     mustJSON(map[string]any{"cycle": i + 1}),
		})
	}
	if !escalated || escalation == nil || strings.TrimSpace(string(escalation.Type)) != "cycle_limit_reached" {
		t.Fatalf("expected cycle_limit_reached escalation, got escalated=%v event=%+v", escalated, escalation)
	}
}

func checkBudgetHumanMailboxContracts(t *testing.T) {
	cases := map[string]events.EventType{
		"warning":   events.EventType("budget.warning"),
		"throttle":  events.EventType("budget.throttle"),
		"emergency": events.EventType("budget.emergency"),
		"ok":        events.EventType("budget.resumed"),
	}
	for state, want := range cases {
		if got := budgetEventTypeForState(state); got != want {
			t.Fatalf("budget state mapping mismatch state=%s got=%s want=%s", state, got, want)
		}
	}
	for _, evt := range []string{"human_task.requested", "human_task.approved", "human_task.rejected", "human_task.deferred", "mailbox.item_decided"} {
		if _, ok := contractEventPayloadFields[evt]; !ok {
			t.Fatalf("missing contract payload fields for %s", evt)
		}
	}
	if mt, err := runtimetools.NormalizeMailboxType("vertical_approval"); err != nil || mt != "vertical_approval" {
		t.Fatalf("mailbox type normalization mismatch type=%q err=%v", mt, err)
	}
	if mp, err := runtimetools.NormalizeMailboxPriority("critical"); err != nil || mp != "critical" {
		t.Fatalf("mailbox priority normalization mismatch priority=%q err=%v", mp, err)
	}
}
