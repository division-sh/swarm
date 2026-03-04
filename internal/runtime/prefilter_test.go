package runtime

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
			"core_features":    []any{"calendar sync"},
			"key_integrations": []any{"whatsapp"},
			"red_flags": []any{
				map[string]any{"type": "regulatory_license"},
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
	if ok || reason != "blocking_red_flags" {
		t.Fatalf("expected blocking red flag rejection, ok=%v reason=%q", ok, reason)
	}
}
