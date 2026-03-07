package pipeline

import "testing"

func TestPreFilter(t *testing.T) {
	legacy := map[string]any{
		"scan_id":         "scan-1",
		"category":        "operations",
		"subcategory":     "clinic_scheduling",
		"signal_strength": 72.0,
	}
	ok, score, reason := evaluateDiscoveryPreFilter(legacy, 72)
	if !ok || reason != "" {
		t.Fatalf("expected legacy payload to pass compatibility path, ok=%v score=%v reason=%q", ok, score, reason)
	}

	structuredPass := map[string]any{
		"signal_strength":        78.0,
		"preliminary_icp":        "Clinic operations manager at SMB dental clinics",
		"opportunity_hypothesis": "Automate booking and reminder workflow",
		"build_sketch": map[string]any{
			"core_features":      []any{"calendar sync"},
			"key_integrations":   []any{"whatsapp"},
			"workflow_embedding": true,
			"red_flags":          []any{},
		},
		"evidence": map[string]any{
			"competitors": []any{
				map[string]any{"name": "ClinicFlow", "pricing": "$49", "source_url": "https://example.com/c1"},
			},
			"pain_signals": []any{
				map[string]any{"signal": "No-show rate is high", "source_url": "https://example.com/p1"},
			},
			"regulatory": []any{
				map[string]any{"detail": "Consent required", "source_url": "https://example.com/r1"},
			},
			"buyer_communities": []any{
				map[string]any{"name": "Clinic Ops LATAM", "source_url": "https://example.com/b1"},
			},
		},
	}
	ok, score, reason = evaluateDiscoveryPreFilter(structuredPass, 78)
	if !ok || reason != "" || score < 50 {
		t.Fatalf("expected structured payload to pass, ok=%v score=%v reason=%q", ok, score, reason)
	}

	structuredFail := map[string]any{
		"signal_strength":        82.0,
		"preliminary_icp":        "Clinic operations manager",
		"opportunity_hypothesis": "Automate booking workflow",
		"build_sketch": map[string]any{
			"core_features":      []any{"calendar sync"},
			"key_integrations":   []any{"whatsapp"},
			"workflow_embedding": true,
			"red_flags": []any{
				map[string]any{"type": "phone_led_sales"},
			},
		},
		"evidence": map[string]any{
			"competitors": []any{
				map[string]any{"name": "ClinicFlow", "pricing": "$49", "source_url": "https://example.com/c1"},
			},
			"pain_signals": []any{
				map[string]any{"signal": "No-show rate is high", "source_url": "https://example.com/p1"},
			},
			"regulatory": []any{
				map[string]any{"detail": "Consent required", "source_url": "https://example.com/r1"},
			},
			"buyer_communities": []any{
				map[string]any{"name": "Clinic Ops LATAM", "source_url": "https://example.com/b1"},
			},
		},
	}
	ok, _, reason = evaluateDiscoveryPreFilter(structuredFail, 82)
	if ok || reason != "blocking_red_flag" {
		t.Fatalf("expected blocking red flag rejection, ok=%v reason=%q", ok, reason)
	}
}

func TestPreFilter_PassthroughAndComplexIntegrationRules(t *testing.T) {
	passthrough := map[string]any{
		"signal_strength":        68.0,
		"preliminary_icp":        "Billing coordinator at mid-size law firm",
		"opportunity_hypothesis": "Scrub prebills for OCG compliance before submission",
		"build_sketch": map[string]any{
			"core_features": []any{
				"Prebill scrub queue",
				"OCG parser and rule matcher",
				"Rejection reason dashboard",
			},
			"key_integrations": []any{
				"Clio API sync",
			},
			"red_flags": []any{
				map[string]any{"type": "accuracy_liability"},
				map[string]any{"type": "one_time_setup"},
			},
		},
		"evidence": map[string]any{
			"competitors": []any{
				map[string]any{"name": "ALB", "pricing": "custom", "source_url": "https://example.com/c1"},
			},
			"pain_signals": []any{
				map[string]any{"signal": "Manual guideline reviews", "source_url": "https://example.com/p1"},
			},
			"buyer_communities": []any{
				map[string]any{"name": "Legal billing ops", "source_url": "https://example.com/b1"},
			},
			"regulatory": []any{
				map[string]any{"detail": "LEDES requirements", "source_url": "https://example.com/r1"},
			},
		},
		"required_capabilities": map[string]any{
			"current": []any{"Web UI", "Clio API integration"},
		},
	}
	ok, score, reason := evaluateDiscoveryPreFilter(passthrough, 68)
	if !ok || reason != "" || score < 55 {
		t.Fatalf("expected passthrough red flags to pass, ok=%v score=%v reason=%q", ok, score, reason)
	}

	complexOnly := map[string]any{
		"signal_strength":        80.0,
		"preliminary_icp":        "Operations manager at regional contractor",
		"opportunity_hypothesis": "Automate invoice compliance reporting for AP teams",
		"build_sketch": map[string]any{
			"core_features":      []any{"Invoice matching", "Approval workflow"},
			"key_integrations":   []any{"QuickBooks API"},
			"workflow_embedding": true,
			"red_flags": []any{
				map[string]any{"type": "complex_integration"},
			},
		},
		"evidence": map[string]any{
			"competitors": []any{
				map[string]any{"name": "Stampli", "pricing": "$500", "source_url": "https://example.com/c2"},
			},
			"pain_signals": []any{
				map[string]any{"signal": "Manual 3-way matching", "source_url": "https://example.com/p2"},
			},
			"buyer_communities": []any{
				map[string]any{"name": "CFMA", "source_url": "https://example.com/b2"},
			},
			"regulatory": []any{
				map[string]any{"detail": "Lien waiver tracking", "source_url": "https://example.com/r2"},
			},
		},
	}
	ok, _, reason = evaluateDiscoveryPreFilter(complexOnly, 80)
	if !ok || reason != "" {
		t.Fatalf("expected complex_integration alone to pass, ok=%v reason=%q", ok, reason)
	}

	complexCoOccurrence := map[string]any{
		"signal_strength":        80.0,
		"preliminary_icp":        "Operations manager at regional contractor",
		"opportunity_hypothesis": "Automate invoice compliance reporting for AP teams",
		"build_sketch": map[string]any{
			"core_features":      []any{"Invoice matching", "Approval workflow"},
			"key_integrations":   []any{"QuickBooks API"},
			"workflow_embedding": true,
			"red_flags": []any{
				map[string]any{"type": "complex_integration"},
				map[string]any{"type": "high_feature_count"},
			},
		},
		"evidence": map[string]any{
			"competitors": []any{
				map[string]any{"name": "Stampli", "pricing": "$500", "source_url": "https://example.com/c3"},
			},
			"pain_signals": []any{
				map[string]any{"signal": "Manual 3-way matching", "source_url": "https://example.com/p3"},
			},
			"buyer_communities": []any{
				map[string]any{"name": "CFMA", "source_url": "https://example.com/b3"},
			},
			"regulatory": []any{
				map[string]any{"detail": "Lien waiver tracking", "source_url": "https://example.com/r3"},
			},
		},
	}
	ok, _, reason = evaluateDiscoveryPreFilter(complexCoOccurrence, 80)
	if ok || reason != "co_occurrence_block" {
		t.Fatalf("expected complex+high_feature_count to block, ok=%v reason=%q", ok, reason)
	}
}

func TestBuildPrefilterSkipDetail_IncludesDebugFields(t *testing.T) {
	payload := map[string]any{
		"opportunity_name":    "Test opportunity",
		"opportunity_pattern": "ai_wrapper",
		"build_sketch": map[string]any{
			"red_flags": []any{
				map[string]any{"type": "phone_led_sales"},
			},
		},
		"evidence": map[string]any{
			"competitors": []any{
				map[string]any{"name": "X", "pricing": "$99", "source_url": "https://example.com/c"},
			},
			"pain_signals": []any{
				map[string]any{"signal": "pain", "source_url": "https://example.com/p"},
			},
			"buyer_communities": []any{
				map[string]any{"name": "buyers", "source_url": "https://example.com/b"},
			},
			"regulatory": []any{
				map[string]any{"detail": "reg", "source_url": "https://example.com/r"},
			},
		},
	}
	detail := buildPrefilterSkipDetail(payload, 72, 62, "blocking_red_flag", "saas_gap")
	for _, key := range []string{
		"skip_reason",
		"red_flags",
		"signal_strength",
		"evidence_urls",
		"retention_primitive",
		"passes_icp_gate",
		"passes_evidence_gate",
		"passes_retention_gate",
	} {
		if _, ok := detail[key]; !ok {
			t.Fatalf("expected key %q in prefilter skip detail", key)
		}
	}
	if got := asString(detail["skip_reason"]); got != "blocking_red_flag" {
		t.Fatalf("unexpected skip_reason: %q", got)
	}
}
