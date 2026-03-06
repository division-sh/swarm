package runtime

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"empireai/internal/commgraph"
)

var (
	emitToolIndexOnce sync.Once
	emitToolToEvent   map[string]string
	generatedSchemas  map[string]struct{}
)

type EventSchema struct {
	Description string
	Schema      map[string]any
}

func ValidateEventPayloadAgainstSchema(eventType string, payload map[string]any) error {
	s := schemaForEventType(eventType)
	root := s.Schema
	if root == nil {
		return nil
	}
	return validateSchemaObject("$", root, payload)
}

func validateSchemaObject(path string, schema map[string]any, payload map[string]any) error {
	if schemaType := strings.TrimSpace(asString(schema["type"])); schemaType != "" && schemaType != "object" {
		return nil
	}
	required := requiredList(schema["required"])
	for _, key := range required {
		if _, ok := payload[key]; !ok {
			return fmt.Errorf("schema validation failed: %s.%s is required", path, key)
		}
	}
	props := schemaProperties(schema["properties"])
	allowAdditional := schemaAdditionalProps(schema["additionalProperties"])
	for k, v := range payload {
		propSchema, known := props[k]
		if !known {
			if allowAdditional {
				continue
			}
			return fmt.Errorf("schema validation failed: %s.%s is not allowed", path, k)
		}
		if err := validateValue(path+"."+k, propSchema, v); err != nil {
			return err
		}
	}
	return nil
}

func validateValue(path string, schema map[string]any, value any) error {
	st := strings.TrimSpace(asString(schema["type"]))
	if st == "" {
		props := schemaProperties(schema["properties"])
		switch {
		case len(props) > 0 || len(requiredList(schema["required"])) > 0:
			st = "object"
		case schema["items"] != nil:
			st = "array"
		default:
			return nil
		}
	}
	if enumRaw, ok := schema["enum"]; ok {
		if !valueInEnum(value, enumRaw) {
			return fmt.Errorf("schema validation failed: %s has invalid enum value %v", path, value)
		}
	}
	switch st {
	case "string":
		if _, ok := value.(string); !ok {
			return fmt.Errorf("schema validation failed: %s must be string", path)
		}
	case "boolean":
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("schema validation failed: %s must be boolean", path)
		}
	case "number":
		if !isNumeric(value) {
			return fmt.Errorf("schema validation failed: %s must be number", path)
		}
		if err := validateNumericBounds(path, schema, value); err != nil {
			return err
		}
	case "integer":
		if !isInteger(value) {
			return fmt.Errorf("schema validation failed: %s must be integer", path)
		}
		if err := validateNumericBounds(path, schema, value); err != nil {
			return err
		}
	case "array":
		arr, ok := asArray(value)
		if !ok {
			return fmt.Errorf("schema validation failed: %s must be array", path)
		}
		if itemsRaw, ok := schema["items"]; ok {
			if itemSchema, ok := itemsRaw.(map[string]any); ok {
				for i, it := range arr {
					if err := validateValue(fmt.Sprintf("%s[%d]", path, i), itemSchema, it); err != nil {
						return err
					}
				}
			}
		}
	case "object":
		obj, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("schema validation failed: %s must be object", path)
		}
		if err := validateSchemaObject(path, schema, obj); err != nil {
			return err
		}
	case "null":
		if value != nil {
			return fmt.Errorf("schema validation failed: %s must be null", path)
		}
	}
	return nil
}

func validateNumericBounds(path string, schema map[string]any, value any) error {
	n, ok := asFloat64(value)
	if !ok {
		return fmt.Errorf("schema validation failed: %s must be numeric", path)
	}
	if minRaw, ok := schema["minimum"]; ok {
		min, ok := asFloat64(minRaw)
		if ok && n < min {
			return fmt.Errorf("schema validation failed: %s must be >= %v", path, min)
		}
	}
	if maxRaw, ok := schema["maximum"]; ok {
		max, ok := asFloat64(maxRaw)
		if ok && n > max {
			return fmt.Errorf("schema validation failed: %s must be <= %v", path, max)
		}
	}
	return nil
}

func asFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint8:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	default:
		return 0, false
	}
}

func schemaProperties(raw any) map[string]map[string]any {
	out := map[string]map[string]any{}
	switch t := raw.(type) {
	case map[string]any:
		for k, v := range t {
			if s, ok := v.(map[string]any); ok {
				out[k] = s
			}
		}
	}
	return out
}

func schemaAdditionalProps(raw any) bool {
	if raw == nil {
		return true
	}
	if b, ok := raw.(bool); ok {
		return b
	}
	return true
}

func requiredList(raw any) []string {
	switch t := raw.(type) {
	case []string:
		return t
	case []any:
		out := make([]string, 0, len(t))
		for _, v := range t {
			if s := strings.TrimSpace(asString(v)); s != "" {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}

func valueInEnum(value any, enumRaw any) bool {
	enum, ok := enumRaw.([]any)
	if !ok {
		switch t := enumRaw.(type) {
		case []string:
			for _, v := range t {
				if strings.EqualFold(strings.TrimSpace(asString(value)), strings.TrimSpace(v)) {
					return true
				}
			}
			return false
		default:
			return true
		}
	}
	for _, v := range enum {
		if strings.EqualFold(strings.TrimSpace(asString(value)), strings.TrimSpace(asString(v))) {
			return true
		}
	}
	return false
}

func isNumeric(v any) bool {
	switch v.(type) {
	case float64, float32, int, int64, int32, uint, uint64, uint32:
		return true
	default:
		return false
	}
}

func isInteger(v any) bool {
	switch v.(type) {
	case int, int64, int32, uint, uint64, uint32:
		return true
	case float64:
		return v.(float64) == float64(int64(v.(float64)))
	case float32:
		return v.(float32) == float32(int64(v.(float32)))
	default:
		return false
	}
}

func asArray(v any) ([]any, bool) {
	switch t := v.(type) {
	case []any:
		return t, true
	case []string:
		out := make([]any, 0, len(t))
		for _, s := range t {
			out = append(out, s)
		}
		return out, true
	default:
		return nil, false
	}
}

// legacyEventSchemaRegistry preserves hand-written schemas that still act as
// strict overrides for a small set of events. The generated catalog-backed
// registry is the default source of truth.
var legacyEventSchemaRegistry = map[string]EventSchema{
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

// strictDefaultEventSchemas enumerates produced events that currently use the
// strict baseline schema contract. Entries here are explicit and deterministic,
// allowing strict startup checks to pass without runtime-generated schemas.
var strictDefaultEventSchemas = map[string]EventSchema{
	"analyst.anti_pattern_advisory":      defaultAgentEventSchema("analyst.anti_pattern_advisory"),
	"analyst.bootstrap_upgrade_proposal": defaultAgentEventSchema("analyst.bootstrap_upgrade_proposal"),
	"analyst.prompt_refinement_proposal": defaultAgentEventSchema("analyst.prompt_refinement_proposal"),
	"brand.candidates_ready":             defaultAgentEventSchema("brand.candidates_ready"),
	"budget.emergency":                   defaultAgentEventSchema("budget.emergency"),
	"budget.resumed":                     defaultAgentEventSchema("budget.resumed"),
	"budget.throttle":                    defaultAgentEventSchema("budget.throttle"),
	"budget.warning":                     defaultAgentEventSchema("budget.warning"),
	"bug_fix_deployed":                   defaultAgentEventSchema("bug_fix_deployed"),
	"bug_reported":                       defaultAgentEventSchema("bug_reported"),
	"build_blocked":                      defaultAgentEventSchema("build_blocked"),
	"build_complete":                     defaultAgentEventSchema("build_complete"),
	"build_progress":                     defaultAgentEventSchema("build_progress"),
	"channel_blocked":                    defaultAgentEventSchema("channel_blocked"),
	"churn_risk":                         defaultAgentEventSchema("churn_risk"),
	"cross_domain_report":                defaultAgentEventSchema("cross_domain_report"),
	"cto.architecture_directive":         defaultAgentEventSchema("cto.architecture_directive"),
	"cto.extraction_recommended":         defaultAgentEventSchema("cto.extraction_recommended"),
	"cto.pattern_detected":               defaultAgentEventSchema("cto.pattern_detected"),
	"cto.spec_approved":                  defaultAgentEventSchema("cto.spec_approved"),
	"cto.spec_revision_needed":           defaultAgentEventSchema("cto.spec_revision_needed"),
	"cto.spec_vetoed":                    defaultAgentEventSchema("cto.spec_vetoed"),
	"cto.tech_spec_feedback":             defaultAgentEventSchema("cto.tech_spec_feedback"),
	"cto.tech_spec_review_requested":     defaultAgentEventSchema("cto.tech_spec_review_requested"),
	"dedup.resolved":                     defaultAgentEventSchema("dedup.resolved"),
	"deploy_requested":                   defaultAgentEventSchema("deploy_requested"),
	"devops.capacity_warning":            defaultAgentEventSchema("devops.capacity_warning"),
	"devops.deploy_complete":             defaultAgentEventSchema("devops.deploy_complete"),
	"devops.deploy_failed":               defaultAgentEventSchema("devops.deploy_failed"),
	"devops.deploy_requested":            defaultAgentEventSchema("devops.deploy_requested"),
	"devops.health_check_failed":         defaultAgentEventSchema("devops.health_check_failed"),
	"devops.infra_change_needed":         defaultAgentEventSchema("devops.infra_change_needed"),
	"devops.rollback_complete":           defaultAgentEventSchema("devops.rollback_complete"),
	"devops.rollback_failed":             defaultAgentEventSchema("devops.rollback_failed"),
	"devops.rollback_requested":          defaultAgentEventSchema("devops.rollback_requested"),
	"devops.ssl_provisioned":             defaultAgentEventSchema("devops.ssl_provisioned"),
	"feature_deployed":                   defaultAgentEventSchema("feature_deployed"),
	"feature_request":                    defaultAgentEventSchema("feature_request"),
	"growth_escalation":                  defaultAgentEventSchema("growth_escalation"),
	"growth_report":                      defaultAgentEventSchema("growth_report"),
	"human_task.approved":                defaultAgentEventSchema("human_task.approved"),
	"human_task.deferred":                defaultAgentEventSchema("human_task.deferred"),
	"human_task.rejected":                defaultAgentEventSchema("human_task.rejected"),
	"market_signals":                     defaultAgentEventSchema("market_signals"),
	"opco.ceo_report":                    defaultAgentEventSchema("opco.ceo_report"),
	"opco.deploy_review":                 defaultAgentEventSchema("opco.deploy_review"),
	"opco.escalation":                    defaultAgentEventSchema("opco.escalation"),
	"opco.founder_input":                 defaultAgentEventSchema("opco.founder_input"),
	"opco.launched":                      defaultAgentEventSchema("opco.launched"),
	"opco.product_spec_review":           defaultAgentEventSchema("opco.product_spec_review"),
	"opco.spend_request":                 defaultAgentEventSchema("opco.spend_request"),
	"opco.steady_state_reached":          defaultAgentEventSchema("opco.steady_state_reached"),
	"outreach_digest":                    defaultAgentEventSchema("outreach_digest"),
	"prelaunch_ready":                    defaultAgentEventSchema("prelaunch_ready"),
	"product_escalation":                 defaultAgentEventSchema("product_escalation"),
	"product_report":                     defaultAgentEventSchema("product_report"),
	"product_spec_ready":                 defaultAgentEventSchema("product_spec_ready"),
	"qa.validation_failed":               defaultAgentEventSchema("qa.validation_failed"),
	"qa.validation_passed":               defaultAgentEventSchema("qa.validation_passed"),
	"research.completed":                 defaultAgentEventSchema("research.completed"),
	"research.vertical_rejected":         defaultAgentEventSchema("research.vertical_rejected"),
	"runtime.reset":                      defaultAgentEventSchema("runtime.reset"),
	"spec.approved":                      defaultAgentEventSchema("spec.approved"),
	"spec.draft_ready":                   defaultAgentEventSchema("spec.draft_ready"),
	"spec.requested":                     defaultAgentEventSchema("spec.requested"),
	"spec.revision_needed":               defaultAgentEventSchema("spec.revision_needed"),
	"spec.validation_requested":          defaultAgentEventSchema("spec.validation_requested"),
	"spec_review.issues_found":           defaultAgentEventSchema("spec_review.issues_found"),
	"spec_review.passed":                 defaultAgentEventSchema("spec_review.passed"),
	"spec_review.requested":              defaultAgentEventSchema("spec_review.requested"),
	"spend_needed":                       defaultAgentEventSchema("spend_needed"),
	"spend_request":                      defaultAgentEventSchema("spend_request"),
	"support_critical":                   defaultAgentEventSchema("support_critical"),
	"support_digest":                     defaultAgentEventSchema("support_digest"),
	"synthesis.resolved":                 defaultAgentEventSchema("synthesis.resolved"),
	"technical_spec_ready":               defaultAgentEventSchema("technical_spec_ready"),
	"template.version_published":         defaultAgentEventSchema("template.version_published"),
	"user_onboarded":                     defaultAgentEventSchema("user_onboarded"),
}

func ensureEventSchemaRegistry() {
	emitToolIndexOnce.Do(func() {
		generatedSchemas = make(map[string]struct{})
		for eventType, schema := range strictDefaultEventSchemas {
			if _, ok := EventSchemaRegistry[eventType]; ok {
				continue
			}
			EventSchemaRegistry[eventType] = schema
		}
		missing := missingAgentEmitSchemas()
		if len(missing) > 0 {
			for _, eventType := range missing {
				generatedSchemas[eventType] = struct{}{}
			}
			runtimeWarnOnce(
				"event-schema-missing-explicit",
				"event-schema-registry",
				"missing explicit schemas for %d known produced events: %s",
				len(missing),
				summarizeLogList(missing, 20),
			)
		}
		ensureSchemaContextFields()
		ensureSchemaPayloadParity()
		emitToolToEvent = make(map[string]string, len(EventSchemaRegistry))
		for eventType := range EventSchemaRegistry {
			emitToolToEvent[emitToolName(eventType)] = eventType
		}
	})
}

func missingAgentEmitSchemas() []string {
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
			missing = append(missing, eventType)
		}
	}
	return uniqueNonEmpty(missing)
}

func ensureSchemaContextFields() {
	for eventType, entry := range EventSchemaRegistry {
		root := entry.Schema
		if root == nil {
			continue
		}
		rootType := strings.TrimSpace(asString(root["type"]))
		if rootType != "" && rootType != "object" {
			continue
		}
		props, ok := root["properties"].(map[string]any)
		if !ok || props == nil {
			props = map[string]any{}
		}
		root["properties"] = props
		entry.Schema = root
		EventSchemaRegistry[eventType] = entry
	}
}

// ensureSchemaPayloadParity aligns EventSchemaRegistry properties with the
// contract event payload field list (exhaustive-exact keys). Existing field
// schema definitions are preserved; missing fields are backfilled as strings.
func ensureSchemaPayloadParity() {
	for eventType, payloadFields := range contractEventPayloadFields {
		entry, ok := EventSchemaRegistry[eventType]
		if !ok {
			continue
		}
		root := entry.Schema
		if root == nil {
			root = map[string]any{}
		}
		if strings.TrimSpace(asString(root["type"])) == "" {
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

		// Keep existing required semantics only for fields still present in payload.
		existingRequired := requiredList(root["required"])
		filteredRequired := make([]string, 0, len(existingRequired))
		for _, field := range existingRequired {
			field = strings.TrimSpace(field)
			if field == "" {
				continue
			}
			if _, ok := allowed[field]; !ok {
				continue
			}
			filteredRequired = append(filteredRequired, field)
		}
		filteredRequired = uniqueNonEmpty(filteredRequired)
		if len(filteredRequired) > 0 {
			root["required"] = filteredRequired
		} else {
			delete(root, "required")
		}

		entry.Schema = root
		EventSchemaRegistry[eventType] = entry
	}
}

func seedAgentEventSchemaDefaults() {
	for _, role := range commgraph.ProducerRoles() {
		for _, eventType := range commgraph.ProducerEventsForRole(role) {
			eventType = strings.TrimSpace(eventType)
			if eventType == "" {
				continue
			}
			if _, ok := EventSchemaRegistry[eventType]; ok {
				continue
			}
			EventSchemaRegistry[eventType] = defaultAgentEventSchema(eventType)
		}
	}
}

func defaultAgentEventSchema(eventType string) EventSchema {
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
	required = uniqueNonEmpty(required)

	schema := map[string]any{
		"type":                 "object",
		"properties":           props,
		"additionalProperties": false,
	}
	if len(required) > 0 {
		schema["required"] = required
	}
	return EventSchema{
		Description: "Emit " + eventType + " event",
		Schema:      schema,
	}
}

func GenerateEmitTools(role string) []ToolDefinition {
	allowed := commgraph.ProducerEventsForRole(role)
	if len(allowed) == 0 {
		return nil
	}
	ensureEventSchemaRegistry()
	tools := make([]ToolDefinition, 0, len(allowed))
	for _, eventType := range allowed {
		eventType = strings.TrimSpace(eventType)
		if eventType == "" {
			continue
		}
		if _, ok := EventSchemaRegistry[eventType]; !ok {
			runtimeWarnOnce(
				"emit-tool-missing-schema-"+eventType,
				"event-schema-registry",
				"skipping emit tool generation for %q because no explicit schema exists",
				eventType,
			)
			continue
		}
		schema := schemaForEventType(eventType)
		tools = append(tools, ToolDefinition{
			Name:        emitToolName(eventType),
			Description: schema.Description,
			Schema:      schema.Schema,
		})
	}
	sort.Slice(tools, func(i, j int) bool { return tools[i].Name < tools[j].Name })
	return tools
}

// GeneratedEmitSchemas returns event types that currently rely on permissive,
// auto-generated schemas rather than explicit spec-authored definitions.
func GeneratedEmitSchemas() []string {
	ensureEventSchemaRegistry()
	out := make([]string, 0, len(generatedSchemas))
	for eventType := range generatedSchemas {
		out = append(out, eventType)
	}
	sort.Strings(out)
	return out
}

// GeneratedEmitSchemasForAgentRoles returns generated/permissive schemas that
// are reachable through at least one agent role's emit tool allowlist.
func GeneratedEmitSchemasForAgentRoles() []string {
	ensureEventSchemaRegistry()
	out := make([]string, 0, 64)
	seen := make(map[string]struct{}, 128)
	for _, role := range commgraph.ProducerRoles() {
		for _, eventType := range commgraph.ProducerEventsForRole(role) {
			if _, ok := generatedSchemas[eventType]; !ok {
				continue
			}
			if _, dup := seen[eventType]; dup {
				continue
			}
			seen[eventType] = struct{}{}
			out = append(out, eventType)
		}
	}
	sort.Strings(out)
	return out
}

func IsEmitToolAllowedForRole(role, toolName string) bool {
	eventType, ok := eventTypeFromEmitToolName(toolName)
	if !ok {
		return false
	}
	for _, evt := range commgraph.ProducerEventsForRole(role) {
		if strings.TrimSpace(evt) == eventType {
			return true
		}
	}
	return false
}

// EventSchemaSnapshot returns a copy of the current event schema registry.
// Used by diagnostics and dashboard tooling.
func EventSchemaSnapshot() map[string]EventSchema {
	ensureEventSchemaRegistry()
	out := make(map[string]EventSchema, len(EventSchemaRegistry))
	for eventType, entry := range EventSchemaRegistry {
		schemaCopy := map[string]any{}
		if entry.Schema != nil {
			schemaCopy = parsePayloadMap(mustJSON(entry.Schema))
		}
		out[eventType] = EventSchema{
			Description: entry.Description,
			Schema:      schemaCopy,
		}
	}
	return out
}

func emitToolName(eventType string) string {
	return "emit_" + strings.ReplaceAll(strings.TrimSpace(eventType), ".", "_")
}

func eventTypeFromEmitToolName(toolName string) (string, bool) {
	toolName = strings.TrimSpace(toolName)
	if !strings.HasPrefix(toolName, "emit_") {
		return "", false
	}
	ensureEventSchemaRegistry()
	if eventType, ok := emitToolToEvent[toolName]; ok {
		return eventType, true
	}
	return "", false
}

func schemaForEventType(eventType string) EventSchema {
	ensureEventSchemaRegistry()
	eventType = strings.TrimSpace(eventType)
	if eventType == "" {
		runtimeWarnOnce("schema-for-empty-event-type", "event-schema-registry", "schema requested for empty event type")
		return EventSchema{
			Description: "Emit event",
			Schema: map[string]any{
				"type":                 "object",
				"properties":           map[string]any{},
				"additionalProperties": false,
			},
		}
	}
	if s, ok := EventSchemaRegistry[eventType]; ok {
		return s
	}
	// Unknown event types are blocked before execution; keep a defensive schema.
	runtimeWarnOnce(
		"schema-for-unknown-"+eventType,
		"event-schema-registry",
		"schema requested for unknown event type %q; returning strict defensive empty schema",
		eventType,
	)
	return EventSchema{
		Description: "Emit " + eventType + " event",
		Schema: map[string]any{
			"type":                 "object",
			"properties":           map[string]any{},
			"additionalProperties": false,
		},
	}
}

func uniqueNonEmpty(values []string) []string {
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
