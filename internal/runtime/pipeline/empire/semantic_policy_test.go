package empire

import "testing"

func TestSemanticPolicy_PrefilterVectors(t *testing.T) {
	t.Run("passthrough", func(t *testing.T) {
		payload := buildPrefilterPayload(68, []string{"accuracy_liability", "one_time_setup"}, 3, []string{"workflow_embedding"})
		ok, _, reason := EvaluateDiscoveryPreFilter(payload, 68)
		if !ok || reason != "" {
			t.Fatalf("expected pass, got ok=%v reason=%q", ok, reason)
		}
	})

	t.Run("blocking_red_flag", func(t *testing.T) {
		payload := buildPrefilterPayload(75, []string{"phone_led_sales"}, 3, []string{"recurring_data"})
		ok, _, reason := EvaluateDiscoveryPreFilter(payload, 75)
		if ok || reason != "blocking_red_flag" {
			t.Fatalf("expected blocking red flag reject, got ok=%v reason=%q", ok, reason)
		}
	})

	t.Run("co_occurrence", func(t *testing.T) {
		payload := buildPrefilterPayload(80, []string{"complex_integration", "high_feature_count"}, 4, []string{"integration_lock_in"})
		ok, _, reason := EvaluateDiscoveryPreFilter(payload, 80)
		if ok || reason != "co_occurrence_block" {
			t.Fatalf("expected co_occurrence_block reject, got ok=%v reason=%q", ok, reason)
		}
	})

	t.Run("signal_floor", func(t *testing.T) {
		payload := buildPrefilterPayload(54, nil, 3, []string{"recurring_data"})
		ok, _, reason := EvaluateDiscoveryPreFilter(payload, 54)
		if ok || reason != "signal_below_threshold" {
			t.Fatalf("expected signal_below_threshold reject, got ok=%v reason=%q", ok, reason)
		}
	})

	t.Run("evidence_gate", func(t *testing.T) {
		payload := buildInsufficientEvidencePayload(70, []string{"workflow_embedding"})
		ok, _, reason := EvaluateDiscoveryPreFilter(payload, 70)
		if ok || reason != "evidence_insufficient" {
			t.Fatalf("expected evidence_insufficient reject, got ok=%v reason=%q", ok, reason)
		}
	})

	t.Run("retention_gate", func(t *testing.T) {
		payload := buildPrefilterPayload(70, nil, 3, nil)
		ok, _, reason := EvaluateDiscoveryPreFilter(payload, 70)
		if ok || reason != "no_retention_primitive" {
			t.Fatalf("expected no_retention_primitive reject, got ok=%v reason=%q", ok, reason)
		}
	})
}

func TestSemanticPolicy_ScoringComposite(t *testing.T) {
	t.Run("shortlisted", func(t *testing.T) {
		res := ComputeComposite(newScoringInput(map[string]int{
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
		}))
		if res.Result != "shortlisted" {
			t.Fatalf("expected shortlisted, got %+v", res)
		}
		if res.CompositeScore < 77.9 || res.CompositeScore > 78.1 {
			t.Fatalf("expected composite ~78, got %.3f", res.CompositeScore)
		}
	})

	t.Run("rejection_cascade_order", func(t *testing.T) {
		gate := ComputeComposite(newScoringInput(map[string]int{
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
		}))
		if gate.Reason != "gate_build_complexity" {
			t.Fatalf("expected first cascade reject gate_build_complexity, got %q", gate.Reason)
		}

		tier1 := ComputeComposite(newScoringInput(map[string]int{
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
		}))
		if tier1.Reason != "tier1_dimension_floor_icp_crispness" {
			t.Fatalf("expected tier1 floor reject, got %q", tier1.Reason)
		}
	})
}

func buildPrefilterPayload(signal float64, redFlags []string, evidenceURLs int, retention []string) map[string]any {
	flagList := make([]any, 0, len(redFlags))
	for _, flag := range redFlags {
		flagList = append(flagList, map[string]any{"type": flag})
	}

	competitors := buildEvidenceURLs("c", evidenceURLs)
	painSignals := buildEvidenceURLs("p", evidenceURLs)
	communities := buildEvidenceURLs("b", evidenceURLs)
	retentionList := make([]any, 0, len(retention))
	for _, item := range retention {
		retentionList = append(retentionList, item)
	}

	return map[string]any{
		"signal_strength": signal,
		"opportunity_name": "Policy Candidate",
		"preliminary_icp":  "Owner at clinic invoice desk",
		"opportunity_hypothesis": "Automate invoice reporting",
		"build_sketch": map[string]any{
			"red_flags": flagList,
		},
		"evidence": map[string]any{
			"competitors":       competitors,
			"pain_signals":      painSignals,
			"buyer_communities": communities,
		},
		"retention_primitives": retentionList,
	}
}

func buildInsufficientEvidencePayload(signal float64, retention []string) map[string]any {
	payload := buildPrefilterPayload(signal, nil, 1, retention)
	evidence := payload["evidence"].(map[string]any)
	same := []any{
		map[string]any{"name": "source", "pricing": "$99", "source_url": "https://example.com/shared"},
	}
	evidence["competitors"] = same
	evidence["pain_signals"] = []any{
		map[string]any{"name": "pain", "source_url": "https://example.com/shared"},
	}
	evidence["buyer_communities"] = []any{
		map[string]any{"name": "buyers", "source_url": "https://example.com/shared"},
	}
	return payload
}

func buildEvidenceURLs(prefix string, count int) []any {
	out := make([]any, 0, count)
	for i := 0; i < count; i++ {
		entry := map[string]any{
			"name":       "source",
			"source_url": "https://example.com/" + prefix + string(rune('a'+i)),
		}
		if prefix == "c" {
			entry["pricing"] = "$99"
		}
		out = append(out, entry)
	}
	return out
}

func newScoringInput(scores map[string]int) ScoringAccumulatorInput {
	dims := ExpectedScoringDimensions("universal")
	received := make(map[string]ScoreDimensionResult, len(dims))
	for _, dim := range dims {
		received[dim] = ScoreDimensionResult{
			Score:    scores[dim],
			Evidence: "evidence",
		}
	}
	return ScoringAccumulatorInput{
		Rubric:   "universal",
		Expected: dims,
		Received: received,
	}
}
