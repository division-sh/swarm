package productpolicy

func NewGenericTestPolicy() Policy {
	return NewStaticPolicy(map[string]any{
		"control_plane_agent_id": "coordinator",
		"default_scan_mode":      "saas_gap",
		"scan_modes": map[string]any{
			"local_services": map[string]any{
				"rubric":                 "local_services_rubric",
				"emits_category_signals": true,
				"emits_trend_signals":    false,
			},
			"saas_gap": map[string]any{
				"rubric":                 "saas_gap_rubric",
				"emits_category_signals": true,
				"emits_trend_signals":    false,
			},
			"saas_trend": map[string]any{
				"rubric":                 "saas_trend_rubric",
				"emits_category_signals": false,
				"emits_trend_signals":    true,
			},
			"corpus": map[string]any{
				"rubric":                 "corpus_rubric",
				"emits_category_signals": true,
				"emits_trend_signals":    false,
				"expected_scanner_count": 1,
				"dispatch_kind":          "jsonl",
				"shard_stage":            "corpus_scan",
			},
		},
	})
}
