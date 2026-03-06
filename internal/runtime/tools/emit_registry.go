package tools

import (
	"sort"
	"strings"
)

// LegacyEventSchemaRegistry preserves hand-written schemas that still act as
// strict overrides for a small set of events. The generated catalog-backed
// registry is the default source of truth.
var LegacyEventSchemaRegistry = map[string]EmitSchema{
	"scan.requested": {
		Description: "Request a market scan for a geography.",
		Schema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"mode": map[string]any{
					"type": "string",
					"enum": []string{"automation_micro", "saas_gap", "saas_trend", "local_services", "corpus"},
				},
				"geography_id": map[string]any{"type": "string"},
				"geography":    map[string]any{"type": "string"},
				"categories": map[string]any{
					"type":  "array",
					"items": map[string]any{"type": "string"},
				},
				"taxonomy_categories": map[string]any{
					"type":  "array",
					"items": map[string]any{"type": "string"},
				},
				"sources": map[string]any{
					"type":  "array",
					"items": map[string]any{"type": "string"},
				},
				"depth": map[string]any{
					"type": "string",
					"enum": []string{"quick", "standard", "deep"},
				},
				"campaign_id": map[string]any{"type": "string"},
				"corpus_path": map[string]any{"type": "string"},
				"priority": map[string]any{
					"type": "string",
					"enum": []string{"low", "normal", "high", "critical"},
				},
				"campaign_context": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"modes": map[string]any{
							"type":  "array",
							"items": map[string]any{"type": "string", "enum": []string{"automation_micro", "saas_gap", "saas_trend", "local_services", "corpus"}},
						},
						"strategic_context": map[string]any{"type": "string"},
						"directive_id":      map[string]any{"type": "string"},
					},
					"required":             []string{"modes", "strategic_context", "directive_id"},
					"additionalProperties": true,
				},
				"directive_id": map[string]any{"type": "string"},
				"strategic_context": map[string]any{
					"type":                 "object",
					"additionalProperties": true,
				},
				"requested_at": map[string]any{"type": "string"},
				"task_id":      map[string]any{"type": "string"},
				"vertical_id":  map[string]any{"type": "string"},
			},
			"required":             []string{"mode", "geography", "campaign_context"},
			"additionalProperties": false,
		},
	},
	"pipeline.dead_letter": {
		Description: "System node exhausted retries for an event and emitted dead-letter escalation.",
		Schema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"event_id":    map[string]any{"type": "string"},
				"node_id":     map[string]any{"type": "string"},
				"event_type":  map[string]any{"type": "string"},
				"retry_count": map[string]any{"type": "integer", "minimum": 1},
				"last_error":  map[string]any{"type": "string"},
				"vertical_id": map[string]any{"type": "string"},
			},
			"required":             []string{"event_id", "node_id", "event_type", "retry_count", "last_error"},
			"additionalProperties": false,
		},
	},
	"category.assessed": {
		Description: "Market Research Agent reports one assessed category for a scan shard.",
		Schema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"scan_id":          map[string]any{"type": "string"},
				"campaign_id":      map[string]any{"type": "string"},
				"mode":             map[string]any{"type": "string"},
				"geography":        map[string]any{"type": "string"},
				"category":         map[string]any{"type": "string"},
				"subcategory":      map[string]any{"type": "string"},
				"opportunity_name": map[string]any{"type": "string"},
				"preliminary_icp":  map[string]any{"type": "string"},
				"build_sketch": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"core_features": map[string]any{
							"type":  "array",
							"items": map[string]any{"type": "string"},
						},
						"key_integrations": map[string]any{
							"type":  "array",
							"items": map[string]any{"type": "string"},
						},
						"red_flags": map[string]any{
							"type": "array",
							"items": map[string]any{
								"type": "object",
								"properties": map[string]any{
									"type": map[string]any{
										"type": "string",
										"enum": []string{
											"regulatory_license",
											"enterprise_contract",
											"certification",
											"two_sided_marketplace",
											"funds_custody",
											"requires_human_review",
											"data_residency_requirement",
											"complex_integration",
											"high_feature_count",
											"phone_led_sales",
											"enterprise_procurement",
											"relationship_networking",
											"physical_presence_required",
											"support_mode_phone_video",
											"one_time_setup",
											"accuracy_liability",
										},
									},
									"notes":      map[string]any{"type": "string"},
									"source_url": map[string]any{"type": "string"},
								},
								"required":             []string{"type"},
								"additionalProperties": false,
							},
						},
					},
					"required":             []string{"core_features", "key_integrations", "red_flags"},
					"additionalProperties": false,
				},
				"evidence": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"competitors": map[string]any{
							"type": "array",
							"items": map[string]any{
								"type": "object",
								"properties": map[string]any{
									"name":       map[string]any{"type": "string"},
									"pricing":    map[string]any{"type": "string"},
									"source_url": map[string]any{"type": "string"},
								},
								"required":             []string{"name", "pricing", "source_url"},
								"additionalProperties": false,
							},
						},
						"pain_signals": map[string]any{
							"type": "array",
							"items": map[string]any{
								"type": "object",
								"properties": map[string]any{
									"signal":     map[string]any{"type": "string"},
									"source_url": map[string]any{"type": "string"},
								},
								"required":             []string{"signal", "source_url"},
								"additionalProperties": false,
							},
						},
						"regulatory": map[string]any{
							"type": "array",
							"items": map[string]any{
								"type": "object",
								"properties": map[string]any{
									"detail":     map[string]any{"type": "string"},
									"source_url": map[string]any{"type": "string"},
								},
								"required":             []string{"detail", "source_url"},
								"additionalProperties": false,
							},
						},
						"buyer_communities": map[string]any{
							"type": "array",
							"items": map[string]any{
								"type": "object",
								"properties": map[string]any{
									"name":       map[string]any{"type": "string"},
									"source_url": map[string]any{"type": "string"},
								},
								"required":             []string{"name", "source_url"},
								"additionalProperties": false,
							},
						},
					},
					"required":             []string{"competitors", "pain_signals", "regulatory", "buyer_communities"},
					"additionalProperties": false,
				},
				"opportunity_hypothesis": map[string]any{"type": "string"},
				"geographic_scope": map[string]any{
					"type": "string",
					"enum": []string{"global", "regional", "local"},
				},
				"signal_strength": map[string]any{
					"type":    "integer",
					"minimum": 0,
					"maximum": 100,
				},
				"opportunity_pattern": map[string]any{
					"type": "string",
					"enum": []string{
						"platform_parasitic",
						"freelancer_replacement",
						"data_asymmetry",
						"api_middleware",
						"compliance_regulatory",
						"ai_wrapper",
						"workflow_automation",
						"unknown",
					},
				},
				"signal_sources": map[string]any{
					"type": "array",
					"items": map[string]any{
						"type": "object",
						"properties": map[string]any{
							"source":      map[string]any{"type": "string"},
							"url":         map[string]any{"type": "string"},
							"signal_type": map[string]any{"type": "string"},
						},
						"additionalProperties": true,
					},
				},
				"required_capabilities": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"current": map[string]any{
							"type":  "array",
							"items": map[string]any{"type": "string"},
						},
						"would_unlock":           map[string]any{"type": "string"},
						"automation_with_unlock": map[string]any{"type": "number"},
					},
					"additionalProperties": true,
				},
				"task_id":     map[string]any{"type": "string"},
				"vertical_id": map[string]any{"type": "string"},
			},
			"required":             []string{"scan_id", "category", "subcategory", "geography", "signal_strength", "opportunity_name", "preliminary_icp", "build_sketch", "evidence", "opportunity_hypothesis", "geographic_scope"},
			"additionalProperties": false,
		},
	},
	"trend.identified": {
		Description: "Trend Research Agent reports one trend finding for a scan shard.",
		Schema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"scan_id":          map[string]any{"type": "string"},
				"campaign_id":      map[string]any{"type": "string"},
				"mode":             map[string]any{"type": "string"},
				"geography":        map[string]any{"type": "string"},
				"trend_category":   map[string]any{"type": "string"},
				"opportunity_name": map[string]any{"type": "string"},
				"preliminary_icp":  map[string]any{"type": "string"},
				"build_sketch": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"core_features": map[string]any{
							"type":  "array",
							"items": map[string]any{"type": "string"},
						},
						"key_integrations": map[string]any{
							"type":  "array",
							"items": map[string]any{"type": "string"},
						},
						"red_flags": map[string]any{
							"type": "array",
							"items": map[string]any{
								"type": "object",
								"properties": map[string]any{
									"type": map[string]any{
										"type": "string",
										"enum": []string{
											"regulatory_license",
											"enterprise_contract",
											"certification",
											"two_sided_marketplace",
											"funds_custody",
											"requires_human_review",
											"data_residency_requirement",
											"complex_integration",
											"high_feature_count",
											"phone_led_sales",
											"enterprise_procurement",
											"relationship_networking",
											"physical_presence_required",
											"support_mode_phone_video",
											"one_time_setup",
											"accuracy_liability",
										},
									},
									"notes":      map[string]any{"type": "string"},
									"source_url": map[string]any{"type": "string"},
								},
								"required":             []string{"type"},
								"additionalProperties": false,
							},
						},
					},
					"required":             []string{"core_features", "key_integrations", "red_flags"},
					"additionalProperties": false,
				},
				"evidence": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"competitors": map[string]any{
							"type": "array",
							"items": map[string]any{
								"type": "object",
								"properties": map[string]any{
									"name":       map[string]any{"type": "string"},
									"pricing":    map[string]any{"type": "string"},
									"source_url": map[string]any{"type": "string"},
								},
								"required":             []string{"name", "pricing", "source_url"},
								"additionalProperties": false,
							},
						},
						"pain_signals": map[string]any{
							"type": "array",
							"items": map[string]any{
								"type": "object",
								"properties": map[string]any{
									"signal":     map[string]any{"type": "string"},
									"source_url": map[string]any{"type": "string"},
								},
								"required":             []string{"signal", "source_url"},
								"additionalProperties": false,
							},
						},
						"regulatory": map[string]any{
							"type": "array",
							"items": map[string]any{
								"type": "object",
								"properties": map[string]any{
									"detail":     map[string]any{"type": "string"},
									"source_url": map[string]any{"type": "string"},
								},
								"required":             []string{"detail", "source_url"},
								"additionalProperties": false,
							},
						},
						"buyer_communities": map[string]any{
							"type": "array",
							"items": map[string]any{
								"type": "object",
								"properties": map[string]any{
									"name":       map[string]any{"type": "string"},
									"source_url": map[string]any{"type": "string"},
								},
								"required":             []string{"name", "source_url"},
								"additionalProperties": false,
							},
						},
					},
					"required":             []string{"competitors", "pain_signals", "regulatory", "buyer_communities"},
					"additionalProperties": false,
				},
				"trend_description":      map[string]any{"type": "string"},
				"opportunity_hypothesis": map[string]any{"type": "string"},
				"geographic_scope": map[string]any{
					"type": "string",
					"enum": []string{"global", "regional", "local"},
				},
				"signal_strength": map[string]any{
					"type":    "integer",
					"minimum": 0,
					"maximum": 100,
				},
				"task_id":     map[string]any{"type": "string"},
				"vertical_id": map[string]any{"type": "string"},
			},
			"required":             []string{"scan_id", "trend_category", "geography", "signal_strength", "opportunity_name", "preliminary_icp", "build_sketch", "evidence", "trend_description", "opportunity_hypothesis", "geographic_scope"},
			"additionalProperties": false,
		},
	},
	"source.scraped": {
		Description: "Scanner agent reports one scraped source item for discovery synthesis.",
		Schema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"scan_id":                map[string]any{"type": "string"},
				"campaign_id":            map[string]any{"type": "string"},
				"mode":                   map[string]any{"type": "string"},
				"geography":              map[string]any{"type": "string"},
				"geography_id":           map[string]any{"type": "string"},
				"category":               map[string]any{"type": "string"},
				"subcategory":            map[string]any{"type": "string"},
				"source":                 map[string]any{"type": "string"},
				"url":                    map[string]any{"type": "string"},
				"title":                  map[string]any{"type": "string"},
				"snippet":                map[string]any{"type": "string"},
				"opportunity_hypothesis": map[string]any{"type": "string"},
				"evidence":               map[string]any{"type": "string"},
				"signal_strength": map[string]any{
					"type":    "integer",
					"minimum": 0,
					"maximum": 100,
				},
				"task_id":     map[string]any{"type": "string"},
				"vertical_id": map[string]any{"type": "string"},
			},
			"required":             []string{"scan_id", "source", "evidence", "signal_strength", "geography"},
			"additionalProperties": false,
		},
	},
	"market_research.scan_complete": {
		Description: "Market research scan shard is complete.",
		Schema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"scan_id":     map[string]any{"type": "string"},
				"campaign_id": map[string]any{"type": "string"},
				"mode":        map[string]any{"type": "string"},
				"geography":   map[string]any{"type": "string"},
				"categories_assessed": map[string]any{
					"type":    "integer",
					"minimum": 0,
				},
				"high_signal_count": map[string]any{
					"type":    "integer",
					"minimum": 0,
				},
				"task_id":      map[string]any{"type": "string"},
				"vertical_id":  map[string]any{"type": "string"},
				"shard":        map[string]any{"type": "object", "additionalProperties": true},
				"completion":   map[string]any{"type": "object", "additionalProperties": true},
				"report_count": map[string]any{"type": "integer", "minimum": 0},
			},
			"required":             []string{"scan_id"},
			"additionalProperties": false,
		},
	},
	"trend_research.scan_complete": {
		Description: "Trend research scan shard is complete.",
		Schema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"scan_id":      map[string]any{"type": "string"},
				"campaign_id":  map[string]any{"type": "string"},
				"mode":         map[string]any{"type": "string"},
				"geography":    map[string]any{"type": "string"},
				"task_id":      map[string]any{"type": "string"},
				"vertical_id":  map[string]any{"type": "string"},
				"shard":        map[string]any{"type": "object", "additionalProperties": true},
				"completion":   map[string]any{"type": "object", "additionalProperties": true},
				"report_count": map[string]any{"type": "integer", "minimum": 0},
			},
			"required":             []string{"scan_id"},
			"additionalProperties": false,
		},
	},
	"scanner.google_maps.scan_complete": {
		Description: "Google Maps scanner shard is complete.",
		Schema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"scan_id":      map[string]any{"type": "string"},
				"campaign_id":  map[string]any{"type": "string"},
				"mode":         map[string]any{"type": "string"},
				"geography":    map[string]any{"type": "string"},
				"task_id":      map[string]any{"type": "string"},
				"vertical_id":  map[string]any{"type": "string"},
				"shard":        map[string]any{"type": "object", "additionalProperties": true},
				"completion":   map[string]any{"type": "object", "additionalProperties": true},
				"report_count": map[string]any{"type": "integer", "minimum": 0},
			},
			"required":             []string{"scan_id"},
			"additionalProperties": false,
		},
	},
	"scanner.instagram.scan_complete": {
		Description: "Instagram scanner shard is complete.",
		Schema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"scan_id":      map[string]any{"type": "string"},
				"campaign_id":  map[string]any{"type": "string"},
				"mode":         map[string]any{"type": "string"},
				"geography":    map[string]any{"type": "string"},
				"task_id":      map[string]any{"type": "string"},
				"vertical_id":  map[string]any{"type": "string"},
				"shard":        map[string]any{"type": "object", "additionalProperties": true},
				"completion":   map[string]any{"type": "object", "additionalProperties": true},
				"report_count": map[string]any{"type": "integer", "minimum": 0},
			},
			"required":             []string{"scan_id"},
			"additionalProperties": false,
		},
	},
	"scanner.reviews.scan_complete": {
		Description: "Reviews scanner shard is complete.",
		Schema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"scan_id":      map[string]any{"type": "string"},
				"campaign_id":  map[string]any{"type": "string"},
				"mode":         map[string]any{"type": "string"},
				"geography":    map[string]any{"type": "string"},
				"task_id":      map[string]any{"type": "string"},
				"vertical_id":  map[string]any{"type": "string"},
				"shard":        map[string]any{"type": "object", "additionalProperties": true},
				"completion":   map[string]any{"type": "object", "additionalProperties": true},
				"report_count": map[string]any{"type": "integer", "minimum": 0},
			},
			"required":             []string{"scan_id"},
			"additionalProperties": false,
		},
	},
	"scanner.directories.scan_complete": {
		Description: "Directories scanner shard is complete.",
		Schema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"scan_id":      map[string]any{"type": "string"},
				"campaign_id":  map[string]any{"type": "string"},
				"mode":         map[string]any{"type": "string"},
				"geography":    map[string]any{"type": "string"},
				"task_id":      map[string]any{"type": "string"},
				"vertical_id":  map[string]any{"type": "string"},
				"shard":        map[string]any{"type": "object", "additionalProperties": true},
				"completion":   map[string]any{"type": "object", "additionalProperties": true},
				"report_count": map[string]any{"type": "integer", "minimum": 0},
			},
			"required":             []string{"scan_id"},
			"additionalProperties": false,
		},
	},
	"scanner.yelp.scan_complete": {
		Description: "Yelp scanner shard is complete.",
		Schema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"scan_id":      map[string]any{"type": "string"},
				"campaign_id":  map[string]any{"type": "string"},
				"mode":         map[string]any{"type": "string"},
				"geography":    map[string]any{"type": "string"},
				"task_id":      map[string]any{"type": "string"},
				"vertical_id":  map[string]any{"type": "string"},
				"shard":        map[string]any{"type": "object", "additionalProperties": true},
				"completion":   map[string]any{"type": "object", "additionalProperties": true},
				"report_count": map[string]any{"type": "integer", "minimum": 0},
			},
			"required":             []string{"scan_id"},
			"additionalProperties": false,
		},
	},
	"scoring.requested": {
		Description: "Runtime requests dimension scoring from Analysis Agent.",
		Schema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"vertical_id":   map[string]any{"type": "string"},
				"vertical_name": map[string]any{"type": "string"},
				"geography":     map[string]any{"type": "string"},
				"mode": map[string]any{
					"type": "string",
					"enum": []string{"automation_micro", "saas_gap", "saas_trend", "local_services", "corpus", "derived"},
				},
				"signal_strength": map[string]any{"type": "integer"},
				"campaign_id":     map[string]any{"type": "string"},
				"rubric": map[string]any{
					"type": "string",
					"enum": []string{"universal"},
				},
				"dimensions_requested": map[string]any{
					"type":  "array",
					"items": map[string]any{"type": "string"},
				},
				"discovery_context": map[string]any{
					"type":                 "object",
					"additionalProperties": true,
				},
				"assigned_analysis_agent_id": map[string]any{"type": "string"},
				"excluded_analysis_agent_id": map[string]any{"type": "string"},
				"task_id":                    map[string]any{"type": "string"},
			},
			"required":             []string{"vertical_id", "vertical_name", "geography", "mode", "rubric", "dimensions_requested", "discovery_context"},
			"additionalProperties": false,
		},
	},
	"score.dimension_complete": {
		Description: "Analysis Agent reports score for one dimension of one vertical.",
		Schema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"vertical_id": map[string]any{"type": "string"},
				"dimension":   map[string]any{"type": "string"},
				"score": map[string]any{
					"type":    "integer",
					"minimum": 0,
					"maximum": 100,
				},
				"evidence": map[string]any{"type": "string"},
				"confidence": map[string]any{
					"type": "string",
					"enum": []string{"high", "medium", "low"},
				},
			},
			"required":             []string{"vertical_id", "dimension", "score", "evidence"},
			"additionalProperties": false,
		},
	},
	"vertical.shortlisted": {
		Description: "Promote a marginal/scored vertical into the validation pipeline.",
		Schema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"vertical_id":     map[string]any{"type": "string"},
				"composite_score": map[string]any{"type": "number"},
				"viability_score": map[string]any{"type": "number"},
				"scoring_payload": map[string]any{"type": "object", "additionalProperties": true},
				"reasoning":       map[string]any{"type": "string"},
				"task_id":         map[string]any{"type": "string"},
			},
			"required":             []string{"vertical_id", "composite_score", "scoring_payload"},
			"additionalProperties": false,
		},
	},
	"vertical.marginal": {
		Description: "Vertical scored 50-74 composite and needs Empire Coordinator judgment.",
		Schema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"vertical_id":        map[string]any{"type": "string"},
				"composite_score":    map[string]any{"type": "number"},
				"viability_score":    map[string]any{"type": "number"},
				"dimensions":         map[string]any{"type": "object", "additionalProperties": true},
				"promotion_eligible": map[string]any{"type": "boolean"},
				"reasoning":          map[string]any{"type": "string"},
				"reconsideration_triggers": map[string]any{
					"type":  "array",
					"items": map[string]any{"type": "string"},
				},
				"task_id": map[string]any{"type": "string"},
			},
			"required":             []string{"vertical_id", "composite_score", "viability_score", "dimensions"},
			"additionalProperties": false,
		},
	},
}

// StrictDefaultEventSchemas enumerates produced events that currently use the
// default agent schema contract until catalog-authored payloads are added.
var StrictDefaultEventSchemas = map[string]EmitSchema{
	"analyst.anti_pattern_advisory":      DefaultAgentEventSchema("analyst.anti_pattern_advisory"),
	"analyst.bootstrap_upgrade_proposal": DefaultAgentEventSchema("analyst.bootstrap_upgrade_proposal"),
	"analyst.prompt_refinement_proposal": DefaultAgentEventSchema("analyst.prompt_refinement_proposal"),
	"brand.candidates_ready":             DefaultAgentEventSchema("brand.candidates_ready"),
	"budget.emergency":                   DefaultAgentEventSchema("budget.emergency"),
	"budget.resumed":                     DefaultAgentEventSchema("budget.resumed"),
	"budget.throttle":                    DefaultAgentEventSchema("budget.throttle"),
	"budget.warning":                     DefaultAgentEventSchema("budget.warning"),
	"bug_fix_deployed":                   DefaultAgentEventSchema("bug_fix_deployed"),
	"bug_reported":                       DefaultAgentEventSchema("bug_reported"),
	"build_blocked":                      DefaultAgentEventSchema("build_blocked"),
	"build_complete":                     DefaultAgentEventSchema("build_complete"),
	"build_progress":                     DefaultAgentEventSchema("build_progress"),
	"channel_blocked":                    DefaultAgentEventSchema("channel_blocked"),
	"churn_risk":                         DefaultAgentEventSchema("churn_risk"),
	"cross_domain_report":                DefaultAgentEventSchema("cross_domain_report"),
	"cto.architecture_directive":         DefaultAgentEventSchema("cto.architecture_directive"),
	"cto.extraction_recommended":         DefaultAgentEventSchema("cto.extraction_recommended"),
	"cto.pattern_detected":               DefaultAgentEventSchema("cto.pattern_detected"),
	"cto.spec_approved":                  DefaultAgentEventSchema("cto.spec_approved"),
	"cto.spec_revision_needed":           DefaultAgentEventSchema("cto.spec_revision_needed"),
	"cto.spec_vetoed":                    DefaultAgentEventSchema("cto.spec_vetoed"),
	"cto.tech_spec_feedback":             DefaultAgentEventSchema("cto.tech_spec_feedback"),
	"cto.tech_spec_review_requested":     DefaultAgentEventSchema("cto.tech_spec_review_requested"),
	"dedup.resolved":                     DefaultAgentEventSchema("dedup.resolved"),
	"deploy_requested":                   DefaultAgentEventSchema("deploy_requested"),
	"devops.capacity_warning":            DefaultAgentEventSchema("devops.capacity_warning"),
	"devops.deploy_complete":             DefaultAgentEventSchema("devops.deploy_complete"),
	"devops.deploy_failed":               DefaultAgentEventSchema("devops.deploy_failed"),
	"devops.deploy_requested":            DefaultAgentEventSchema("devops.deploy_requested"),
	"devops.health_check_failed":         DefaultAgentEventSchema("devops.health_check_failed"),
	"devops.infra_change_needed":         DefaultAgentEventSchema("devops.infra_change_needed"),
	"devops.rollback_complete":           DefaultAgentEventSchema("devops.rollback_complete"),
	"devops.rollback_failed":             DefaultAgentEventSchema("devops.rollback_failed"),
	"devops.rollback_requested":          DefaultAgentEventSchema("devops.rollback_requested"),
	"devops.ssl_provisioned":             DefaultAgentEventSchema("devops.ssl_provisioned"),
	"feature_deployed":                   DefaultAgentEventSchema("feature_deployed"),
	"feature_request":                    DefaultAgentEventSchema("feature_request"),
	"growth_escalation":                  DefaultAgentEventSchema("growth_escalation"),
	"growth_report":                      DefaultAgentEventSchema("growth_report"),
	"human_task.approved":                DefaultAgentEventSchema("human_task.approved"),
	"human_task.deferred":                DefaultAgentEventSchema("human_task.deferred"),
	"human_task.rejected":                DefaultAgentEventSchema("human_task.rejected"),
	"market_signals":                     DefaultAgentEventSchema("market_signals"),
	"opco.ceo_report":                    DefaultAgentEventSchema("opco.ceo_report"),
	"opco.deploy_review":                 DefaultAgentEventSchema("opco.deploy_review"),
	"opco.escalation":                    DefaultAgentEventSchema("opco.escalation"),
	"opco.founder_input":                 DefaultAgentEventSchema("opco.founder_input"),
	"opco.launched":                      DefaultAgentEventSchema("opco.launched"),
	"opco.product_spec_review":           DefaultAgentEventSchema("opco.product_spec_review"),
	"opco.spend_request":                 DefaultAgentEventSchema("opco.spend_request"),
	"opco.steady_state_reached":          DefaultAgentEventSchema("opco.steady_state_reached"),
	"outreach_digest":                    DefaultAgentEventSchema("outreach_digest"),
	"prelaunch_ready":                    DefaultAgentEventSchema("prelaunch_ready"),
	"product_escalation":                 DefaultAgentEventSchema("product_escalation"),
	"product_report":                     DefaultAgentEventSchema("product_report"),
	"product_spec_ready":                 DefaultAgentEventSchema("product_spec_ready"),
	"qa.validation_failed":               DefaultAgentEventSchema("qa.validation_failed"),
	"qa.validation_passed":               DefaultAgentEventSchema("qa.validation_passed"),
	"research.completed":                 DefaultAgentEventSchema("research.completed"),
	"research.vertical_rejected":         DefaultAgentEventSchema("research.vertical_rejected"),
	"runtime.reset":                      DefaultAgentEventSchema("runtime.reset"),
	"spec.approved":                      DefaultAgentEventSchema("spec.approved"),
	"spec.draft_ready":                   DefaultAgentEventSchema("spec.draft_ready"),
	"spec.requested":                     DefaultAgentEventSchema("spec.requested"),
	"spec.revision_needed":               DefaultAgentEventSchema("spec.revision_needed"),
	"spec.validation_requested":          DefaultAgentEventSchema("spec.validation_requested"),
	"spec_review.issues_found":           DefaultAgentEventSchema("spec_review.issues_found"),
	"spec_review.passed":                 DefaultAgentEventSchema("spec_review.passed"),
	"spec_review.requested":              DefaultAgentEventSchema("spec_review.requested"),
	"spend_needed":                       DefaultAgentEventSchema("spend_needed"),
	"spend_request":                      DefaultAgentEventSchema("spend_request"),
	"support_critical":                   DefaultAgentEventSchema("support_critical"),
	"support_digest":                     DefaultAgentEventSchema("support_digest"),
	"synthesis.resolved":                 DefaultAgentEventSchema("synthesis.resolved"),
	"technical_spec_ready":               DefaultAgentEventSchema("technical_spec_ready"),
	"template.version_published":         DefaultAgentEventSchema("template.version_published"),
	"user_onboarded":                     DefaultAgentEventSchema("user_onboarded"),
}

func MissingProducerEventSchemas(producerRoles func() []string, producerEvents func(string) []string, registry map[string]EmitSchema) []string {
	missing := make([]string, 0, 16)
	for _, role := range producerRoles() {
		for _, eventType := range producerEvents(role) {
			eventType = strings.TrimSpace(eventType)
			if eventType == "" {
				continue
			}
			if _, ok := registry[eventType]; ok {
				continue
			}
			missing = append(missing, eventType)
		}
	}
	return UniqueNonEmpty(missing)
}

func EnsureSchemaContextFields(registry map[string]EmitSchema) {
	for eventType, entry := range registry {
		root := entry.Schema
		if root == nil {
			continue
		}
		rootType := strings.TrimSpace(AsString(root["type"]))
		if rootType != "" && rootType != "object" {
			continue
		}
		props, ok := root["properties"].(map[string]any)
		if !ok || props == nil {
			props = map[string]any{}
		}
		root["properties"] = props
		entry.Schema = root
		registry[eventType] = entry
	}
}

// EnsureSchemaPayloadParity aligns registry properties with the contract event
// payload field list (exhaustive-exact keys). Existing field schema definitions
// are preserved; missing fields are backfilled as strings.
func EnsureSchemaPayloadParity(registry map[string]EmitSchema, contractEventPayloadFields map[string][]string) {
	for eventType, payloadFields := range contractEventPayloadFields {
		entry, ok := registry[eventType]
		if !ok {
			continue
		}
		root := entry.Schema
		if root == nil {
			root = map[string]any{}
		}
		if strings.TrimSpace(AsString(root["type"])) == "" {
			root["type"] = "object"
		}
		props := schemaProperties(root["properties"])
		aligned := make(map[string]any, len(payloadFields))
		allowed := make(map[string]struct{}, len(payloadFields))
		for _, field := range payloadFields {
			field = strings.TrimSpace(field)
			if field == "" {
				continue
			}
			allowed[field] = struct{}{}
			if existing, ok := props[field]; ok && existing != nil {
				aligned[field] = existing
				continue
			}
			aligned[field] = map[string]any{"type": "string"}
		}
		root["properties"] = aligned

		existingRequired := requiredList(root["required"])
		filteredRequired := make([]string, 0, len(existingRequired))
		for _, field := range existingRequired {
			field = strings.TrimSpace(AsString(field))
			if field == "" {
				continue
			}
			if _, ok := allowed[field]; !ok {
				continue
			}
			filteredRequired = append(filteredRequired, field)
		}
		filteredRequired = UniqueNonEmpty(filteredRequired)
		if len(filteredRequired) > 0 {
			root["required"] = filteredRequired
		} else {
			delete(root, "required")
		}

		entry.Schema = root
		registry[eventType] = entry
	}
}

func SeedAgentEventSchemaDefaults(producerRoles func() []string, producerEvents func(string) []string, registry map[string]EmitSchema) {
	for _, role := range producerRoles() {
		for _, eventType := range producerEvents(role) {
			eventType = strings.TrimSpace(eventType)
			if eventType == "" {
				continue
			}
			if _, ok := registry[eventType]; ok {
				continue
			}
			registry[eventType] = DefaultAgentEventSchema(eventType)
		}
	}
}

func DefaultAgentEventSchema(eventType string) EmitSchema {
	props := map[string]any{
		"vertical_id":     map[string]any{"type": "string"},
		"task_id":         map[string]any{"type": "string"},
		"scan_id":         map[string]any{"type": "string"},
		"campaign_id":     map[string]any{"type": "string"},
		"mode":            map[string]any{"type": "string"},
		"geography":       map[string]any{"type": "string"},
		"geography_id":    map[string]any{"type": "string"},
		"name":            map[string]any{"type": "string"},
		"vertical_name":   map[string]any{"type": "string"},
		"priority":        map[string]any{"type": "string"},
		"status":          map[string]any{"type": "string"},
		"severity":        map[string]any{"type": "string"},
		"action":          map[string]any{"type": "string"},
		"reason":          map[string]any{"type": "string"},
		"notes":           map[string]any{"type": "string"},
		"summary":         map[string]any{"type": "string"},
		"message":         map[string]any{"type": "string"},
		"evidence":        map[string]any{"type": "string"},
		"score":           map[string]any{"type": "number"},
		"composite_score": map[string]any{"type": "number"},
		"viability_score": map[string]any{"type": "number"},
		"signal_strength": map[string]any{"type": "number"},
		"confidence":      map[string]any{"type": "string"},
		"passed":          map[string]any{"type": "boolean"},
		"version":         map[string]any{"type": "string"},
		"from_version":    map[string]any{"type": "string"},
		"to_version":      map[string]any{"type": "string"},
		"migration_id":    map[string]any{"type": "string"},
		"error":           map[string]any{"type": "string"},
		"requested_by":    map[string]any{"type": "string"},
		"requested_at":    map[string]any{"type": "string"},
		"completed_at":    map[string]any{"type": "string"},
		"failed_at":       map[string]any{"type": "string"},
		"digest_text":     map[string]any{"type": "string"},
		"recommendation":  map[string]any{"type": "string"},
		"snapshot":        map[string]any{"type": "object", "additionalProperties": true},
		"payload":         map[string]any{"type": "object", "additionalProperties": true},
		"metadata":        map[string]any{"type": "object", "additionalProperties": true},
		"context":         map[string]any{"type": "object", "additionalProperties": true},
		"details":         map[string]any{"type": "object", "additionalProperties": true},
		"trend_data":      map[string]any{"type": "object", "additionalProperties": true},
		"mandate":         map[string]any{"type": "object", "additionalProperties": true},
		"spec":            map[string]any{"type": "object", "additionalProperties": true},
		"business_brief":  map[string]any{"type": "object", "additionalProperties": true},
		"scoring_payload": map[string]any{"type": "object", "additionalProperties": true},
		"dimensions":      map[string]any{"type": "object", "additionalProperties": true},
		"template_diff":   map[string]any{"type": "object", "additionalProperties": true},
		"issues":          map[string]any{"type": "array", "items": map[string]any{"type": "object"}},
		"items":           map[string]any{"type": "array", "items": map[string]any{"type": "object"}},
		"candidates":      map[string]any{"type": "array", "items": map[string]any{"type": "object"}},
		"events":          map[string]any{"type": "array", "items": map[string]any{"type": "object"}},
	}
	required := []string{}

	switch eventType {
	case "dedup.resolved":
		props["dedup_event_id"] = map[string]any{"type": "string"}
		props["action"] = map[string]any{"type": "string", "enum": []string{"merge", "keep_both"}}
		required = append(required, "dedup_event_id", "action")
	case "synthesis.resolved":
		props["resolution"] = map[string]any{"type": "string"}
		props["rationale"] = map[string]any{"type": "string"}
	case "portfolio.digest_compiled":
		props["message"] = map[string]any{"type": "string"}
		props["digest_text"] = map[string]any{"type": "string"}
		props["trigger_reason"] = map[string]any{"type": "string"}
		props["snapshot"] = map[string]any{"type": "object", "additionalProperties": true}
		required = append(required, "message")
	case "template.version_published":
		props["version"] = map[string]any{"type": "string"}
		required = append(required, "version")
	case "template.migration_planned", "template.migration_completed", "template.migration_failed":
		props["migration_id"] = map[string]any{"type": "string"}
		props["from_version"] = map[string]any{"type": "string"}
		props["to_version"] = map[string]any{"type": "string"}
		props["error"] = map[string]any{"type": "string"}
	case "human_task.requested", "human_task.approved", "human_task.rejected", "human_task.deferred", "human_task.completed", "human_task.expired":
		props["task_id"] = map[string]any{"type": "string"}
		required = append(required, "task_id")
	case "brand.candidates_ready":
		props["candidates"] = map[string]any{"type": "array", "items": map[string]any{"type": "object"}}
		required = append(required, "vertical_id", "candidates")
	}
	if strings.Contains(eventType, ".scan_complete") {
		required = append(required, "scan_id")
	}
	if strings.Contains(eventType, ".scan_assigned") {
		required = append(required, "scan_id")
	}
	if strings.HasPrefix(eventType, "opco.") && !strings.Contains(eventType, "teardown_complete") {
		required = append(required, "vertical_id")
	}
	if strings.HasPrefix(eventType, "spec.") || strings.HasPrefix(eventType, "cto.spec_") {
		required = append(required, "vertical_id")
	}
	if strings.HasPrefix(eventType, "vertical.") {
		required = append(required, "vertical_id")
	}
	if strings.HasPrefix(eventType, "budget.") {
		props["state"] = map[string]any{"type": "string"}
		props["next_event_type"] = map[string]any{"type": "string"}
	}
	required = UniqueNonEmpty(required)

	schema := map[string]any{
		"type":                 "object",
		"properties":           props,
		"additionalProperties": false,
	}
	if len(required) > 0 {
		schema["required"] = required
	}
	return EmitSchema{
		Description: "Emit " + eventType + " event",
		Schema:      schema,
	}
}

func SnapshotEmitSchemas(registry map[string]EmitSchema) map[string]EmitSchema {
	out := make(map[string]EmitSchema, len(registry))
	for eventType, entry := range registry {
		schemaCopy, _ := deepCloneJSONValue(entry.Schema).(map[string]any)
		if schemaCopy == nil {
			schemaCopy = map[string]any{}
		}
		out[eventType] = EmitSchema{Description: entry.Description, Schema: schemaCopy}
	}
	return out
}

func UniqueNonEmpty(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, v := range values {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}
